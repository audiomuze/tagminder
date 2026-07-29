#!/usr/bin/env python3
""" 
Purpose:
    Canonicalize and de-duplicate Tagminder's year/date fields in `alib`.

    Canonical fields:
        - `date` (current release date)
        - `originalreleasedate` (original debut date)

    Aliases:
        - `releasedate` is treated as an alias for `date`
        - `originaldate` is treated as an alias for `originalreleasedate`

    Policy (safe, non-lossy):
        - Normalize common date formats to either `YYYY` or `YYYY-MM-DD`.
        - Fill missing canonical values from their alias (after normalization).
        - If canonical and alias are both present and normalize to the same value,
          NULL the alias (deduplication).
        - If canonical and alias are both present and normalize to different values,
          do NOT overwrite anything; record an exception.
        - Normalize `year`/`originalyear` to `YYYY` when possible.
        - Derive missing `year` from canonical `date` when `date` is valid.
                - Derive missing `originalyear` from canonical `originalreleasedate`
                    when valid.
                - Only derive missing `originalyear` from `year` when both
                    `originalyear` and `originalreleasedate` are missing.

    Exception reporting:
        - Writes exception rows into `_INF_year_date_exceptions`.
        - If there are no exceptions, the table will be empty.

    All field-level changes are logged to `changelog` and `__sqlmodded` is
    incremented by the number of fields changed per row.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog
    - _INF_year_date_exceptions

Author: audiomuze
Last updated: 2026-04-16
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db
from tagminder.core import tm_run

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


FIELDS = [
    "year",
    "date",
    "releasedate",
    "originalyear",
    "originaldate",
    "originalreleasedate",
]

# Tagminder multi-value delimiter (stored in SQLite text as two literal backslashes).
# Source of truth: tagminder.toml [strings].multivalue_delimiter
DELIMITER = tm_config.get_multivalue_delimiter()


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _clean_text(expr: pl.Expr) -> pl.Expr:
    """Trim and convert empty strings to NULL."""

    s = expr.cast(pl.Utf8).str.strip_chars()
    return pl.when(s.is_null() | (s == "")).then(pl.lit(None, dtype=pl.Utf8)).otherwise(s)


def _contains_delimiter(expr: pl.Expr) -> pl.Expr:
    """True if the value contains Tagminder's multi-value delimiter."""

    s = _clean_text(expr)
    return s.is_not_null() & s.str.contains(DELIMITER, literal=True)


def _normalize_date(expr: pl.Expr) -> pl.Expr:
    """Normalize to `YYYY` or `YYYY-MM-DD`.

    If the cell contains Tagminder's multi-value delimiter, split and de-duplicate
    segments (preserving order). If multiple distinct values remain, they are
    re-joined using the delimiter and later surfaced via exception reporting.
    """

    s = _clean_text(expr)
    parts = s.str.split(DELIMITER)

    e0 = pl.element().cast(pl.Utf8).str.strip_chars()
    e = (
        pl.when(e0.is_null() | (e0 == ""))
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(e0)
    )

    # Clean/normalize each segment.
    norm_parts0 = (
        parts.list.eval(
            pl.coalesce(
                [
                    # YYYY-MM-DD (or YYYY-MM-DDTHH:MM:SS...) -> YYYY-MM-DD
                    pl.when(e.str.contains(r"^\d{4}-\d{2}-\d{2}")).then(e.str.slice(0, 10)),
                    # YYYYMMDD -> YYYY-MM-DD
                    pl.when(e.str.contains(r"^\d{8}$")).then(
                        e.str.replace(r"^(\d{4})(\d{2})(\d{2})$", r"$1-$2-$3")
                    ),
                    # YYYY/MM/DD -> YYYY-MM-DD
                    pl.when(e.str.contains(r"^\d{4}/\d{2}/\d{2}")).then(
                        e.str.replace_all("/", "-").str.slice(0, 10)
                    ),
                    # YYYY.MM.DD -> YYYY-MM-DD
                    pl.when(e.str.contains(r"^\d{4}\.\d{2}\.\d{2}")).then(
                        e.str.replace_all("\\.", "-").str.slice(0, 10)
                    ),
                    # YYYY -> YYYY
                    pl.when(e.str.contains(r"^\d{4}$")).then(e),
                    # YYYY-MM -> YYYY
                    pl.when(e.str.contains(r"^\d{4}-\d{2}$")).then(e.str.slice(0, 4)),
                    e,
                ]
            )
        )
        .list.drop_nulls()
        .list.unique(maintain_order=True)
    )

    # If we have both a full date (YYYY-MM-DD) and the matching year (YYYY),
    # drop the redundant year-only value.
    years_from_full_dates = (
        norm_parts0.list.eval(
            pl.when(pl.element().str.contains(r"^\d{4}-\d{2}-\d{2}$"))
            .then(pl.element().str.slice(0, 4))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
        .list.drop_nulls()
        .list.unique(maintain_order=True)
    )

    norm_parts = norm_parts0.list.set_difference(years_from_full_dates)

    return (
        pl.when(norm_parts.is_null() | (norm_parts.list.len() == 0))
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(norm_parts.list.len() == 1)
        .then(norm_parts.list.get(0))
        .otherwise(norm_parts.list.join(DELIMITER))
    )


def _is_valid_date(expr: pl.Expr) -> pl.Expr:
    """Expected format: `YYYY` or `YYYY-MM-DD`."""

    s = _clean_text(expr)
    return s.is_not_null() & s.str.contains(r"^\d{4}(-\d{2}-\d{2})?$")


def _normalize_year(expr: pl.Expr) -> pl.Expr:
    """Normalize to `YYYY`.

    If the cell contains Tagminder's multi-value delimiter, split and de-duplicate
    segments (preserving order). If multiple distinct values remain, they are
    re-joined using the delimiter and later surfaced via exception reporting.
    """

    s = _clean_text(expr)
    parts = s.str.split(DELIMITER)

    e0 = pl.element().cast(pl.Utf8).str.strip_chars()
    e = (
        pl.when(e0.is_null() | (e0 == ""))
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(e0)
    )

    norm_parts = (
        parts.list.eval(
            pl.coalesce(
                [
                    # YYYY-MM-DD (or YYYY-MM-DD...) -> YYYY
                    pl.when(e.str.contains(r"^\d{4}-\d{2}-\d{2}")).then(e.str.slice(0, 4)),
                    # YYYYMMDD -> YYYY
                    pl.when(e.str.contains(r"^\d{8}$")).then(e.str.slice(0, 4)),
                    # YYYY -> YYYY
                    pl.when(e.str.contains(r"^\d{4}$")).then(e),
                    # YYYY-MM -> YYYY
                    pl.when(e.str.contains(r"^\d{4}-\d{2}$")).then(e.str.slice(0, 4)),
                    # YYYY/MM/DD -> YYYY
                    pl.when(e.str.contains(r"^\d{4}/\d{2}/\d{2}")).then(e.str.slice(0, 4)),
                    # YYYY.MM.DD -> YYYY
                    pl.when(e.str.contains(r"^\d{4}\.\d{2}\.\d{2}")).then(e.str.slice(0, 4)),
                    e,
                ]
            )
        )
        .list.drop_nulls()
        .list.unique(maintain_order=True)
    )

    return (
        pl.when(norm_parts.is_null() | (norm_parts.list.len() == 0))
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(norm_parts.list.len() == 1)
        .then(norm_parts.list.get(0))
        .otherwise(norm_parts.list.join(DELIMITER))
    )


def _is_valid_year(expr: pl.Expr) -> pl.Expr:
    s = _clean_text(expr)
    return s.is_not_null() & s.str.contains(r"^\d{4}$")


def _changed(old: pl.Expr, new: pl.Expr) -> pl.Expr:
    """Null-safe change detector."""

    return (
        (old.is_null() & new.is_not_null())
        | (old.is_not_null() & new.is_null())
        | (old.is_not_null() & new.is_not_null() & (old != new))
    )


def _ensure_exceptions_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {tm_db.quote_ident(table)} (
            alib_path TEXT,
            issue TEXT,
            field TEXT,
            value1 TEXT,
            value2 TEXT,
            timestamp TEXT,
            script TEXT
        )
        """.strip()
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        prog="20-normalize-dates-and-years.py",
        description="Canonicalize year/date fields in alib and write exception-only findings.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )

    args = parser.parse_args(argv)

    conn, db_path, script, timestamp = tm_run.open_db(
        db_path=args.db,
        require_exists=True,
        ensure_changelog=True,
    )

    exceptions_table = "_INF_year_date_exceptions"

    try:
        logging.info("DB: %s", db_path)

        cfg = tm_config.load_config()
        db_cfg = cfg.get("db", {}) if isinstance(cfg, dict) else {}
        alib_table = str(db_cfg.get("alib_table") or "alib") if isinstance(db_cfg, dict) else "alib"

        if not tm_db.table_exists(conn, alib_table):
            raise RuntimeError(
                f"Missing required table {alib_table!r}. "
                "Run tags2db import to create/populate the staging database before running this step."
            )

        # Read only the required columns.
        df = tm_polars_db.sqlite_to_polars(
            conn,
            f"""
            SELECT
                rowid,
                __path,
                COALESCE(__sqlmodded, 0) AS __sqlmodded,
                year,
                date,
                releasedate,
                originalyear,
                originaldate,
                originalreleasedate
            FROM {tm_db.quote_ident(alib_table)}
            """.strip(),
        )

        if df.is_empty():
            logging.info("No rows found in alib; nothing to do.")
            return 0

        # Normalize fields (without yet deciding precedence/merges).
        df = df.with_columns(
            [
                _normalize_date(pl.col("date")).alias("_date_n"),
                _normalize_date(pl.col("releasedate")).alias("_releasedate_n"),
                _normalize_date(pl.col("originalreleasedate")).alias("_originalreleasedate_n"),
                _normalize_date(pl.col("originaldate")).alias("_originaldate_n"),
                _normalize_date(pl.col("year")).alias("_year_as_date_n"),
                _normalize_year(pl.col("year")).alias("_year_n"),
                _normalize_year(pl.col("originalyear")).alias("_originalyear_n"),
            ]
        )

        # Final canonical fields:
        # - date is canonical; fill from releasedate only when date is NULL.
        # - if `year` contains a full date (YYYY-MM-DD) and both date/releasedate are NULL,
        #   preserve the full date by promoting it into canonical `date`.
        # - originalreleasedate is canonical; fill from originaldate only when originalreleasedate is NULL.
        year_is_long_date = (
            pl.col("_year_as_date_n").is_not_null()
            & pl.col("_year_as_date_n").str.contains(r"^\d{4}-\d{2}-\d{2}$")
        )
        date_missing = pl.col("_date_n").is_null() & pl.col("_releasedate_n").is_null()
        year_date_fill = (
            pl.when(date_missing & year_is_long_date)
            .then(pl.col("_year_as_date_n"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )

        df = df.with_columns(
            [
                pl.coalesce([pl.col("_date_n"), pl.col("_releasedate_n")]).alias("_date_existing_n"),
                pl.coalesce([pl.col("_date_n"), pl.col("_releasedate_n"), year_date_fill]).alias(
                    "_date_final"
                ),
                pl.coalesce(
                    [pl.col("_originalreleasedate_n"), pl.col("_originaldate_n")]
                ).alias("_originalreleasedate_final"),
            ]
        )

        # Alias deduplication:
        # NULL the alias if it matches the canonical (after normalization).
        df = df.with_columns(
            [
                pl.when(
                    pl.col("_releasedate_n").is_not_null()
                    & pl.col("_date_final").is_not_null()
                    & (pl.col("_releasedate_n") == pl.col("_date_final"))
                )
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("_releasedate_n"))
                .alias("_releasedate_final"),
                pl.when(
                    pl.col("_originaldate_n").is_not_null()
                    & pl.col("_originalreleasedate_final").is_not_null()
                    & (pl.col("_originaldate_n") == pl.col("_originalreleasedate_final"))
                )
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("_originaldate_n"))
                .alias("_originaldate_final"),
            ]
        )

        year_vs_date_conflict = (
            year_is_long_date
            & pl.col("_date_existing_n").is_not_null()
            & _is_valid_date(pl.col("_date_existing_n"))
            & (pl.col("_year_as_date_n") != pl.col("_date_existing_n"))
        )

        year_base = (
            pl.when(year_vs_date_conflict)
            .then(pl.col("_year_as_date_n"))
            .otherwise(pl.col("_year_n"))
        )

        # Derive missing year/originalyear (only if the year field itself is empty).
        # For originalyear, prefer originalreleasedate first; only use year if
        # originalyear and originalreleasedate are both empty.
        originalyear_from_originalreleasedate = (
            pl.when(_is_valid_date(pl.col("_originalreleasedate_final")))
            .then(pl.col("_originalreleasedate_final").str.slice(0, 4))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
        originalyear_from_year_if_no_originalreleasedate = (
            pl.when(pl.col("_originalreleasedate_final").is_null())
            .then(pl.col("_year_n"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        )
        df = df.with_columns(
            [
                pl.when(year_base.is_null() & _is_valid_date(pl.col("_date_final")))
                .then(pl.col("_date_final").str.slice(0, 4))
                .otherwise(year_base)
                .alias("_year_final"),
                pl.coalesce(
                    [
                        pl.col("_originalyear_n"),
                        originalyear_from_originalreleasedate,
                        originalyear_from_year_if_no_originalreleasedate,
                    ]
                )
                .alias("_originalyear_final"),
            ]
        )

        # Exception-only findings (DB table).
        # We treat conflicts as: both canonical+alias present AND both are valid AND they disagree.
        date_conflict = (
            pl.col("_date_n").is_not_null()
            & pl.col("_releasedate_n").is_not_null()
            & _is_valid_date(pl.col("_date_n"))
            & _is_valid_date(pl.col("_releasedate_n"))
            & (pl.col("_date_n") != pl.col("_releasedate_n"))
        )
        orig_conflict = (
            pl.col("_originalreleasedate_n").is_not_null()
            & pl.col("_originaldate_n").is_not_null()
            & _is_valid_date(pl.col("_originalreleasedate_n"))
            & _is_valid_date(pl.col("_originaldate_n"))
            & (pl.col("_originalreleasedate_n") != pl.col("_originaldate_n"))
        )

        multi_date = _contains_delimiter(pl.col("_date_final"))
        multi_releasedate = _contains_delimiter(pl.col("_releasedate_final"))
        multi_originalreleasedate = _contains_delimiter(pl.col("_originalreleasedate_final"))
        multi_originaldate = _contains_delimiter(pl.col("_originaldate_final"))
        multi_year = _contains_delimiter(pl.col("_year_final"))
        multi_originalyear = _contains_delimiter(pl.col("_originalyear_final"))

        invalid_date = (
            pl.col("_date_final").is_not_null()
            & ~_is_valid_date(pl.col("_date_final"))
            & ~multi_date
        )
        invalid_originalreleasedate = (
            pl.col("_originalreleasedate_final").is_not_null()
            & ~_is_valid_date(pl.col("_originalreleasedate_final"))
            & ~multi_originalreleasedate
        )

        invalid_year = (
            pl.col("_year_final").is_not_null()
            & ~_is_valid_year(pl.col("_year_final"))
            & ~multi_year
            & ~year_vs_date_conflict
        )
        missing_year = pl.col("_year_final").is_null()

        invalid_originalyear = (
            pl.col("_originalyear_final").is_not_null()
            & ~_is_valid_year(pl.col("_originalyear_final"))
            & ~multi_originalyear
        )
        missing_originalyear = pl.col("_originalyear_final").is_null()

        exceptions_frames: list[pl.DataFrame] = []

        def _exc_df(mask: pl.Expr, *, issue: str, field: str, v1: str, v2: str | None = None) -> pl.DataFrame:
            cols = [
                pl.col("__path").alias("alib_path"),
                pl.lit(issue, dtype=pl.Utf8).alias("issue"),
                pl.lit(field, dtype=pl.Utf8).alias("field"),
                pl.col(v1).alias("value1"),
            ]
            if v2 is not None:
                cols.append(pl.col(v2).alias("value2"))
            else:
                cols.append(pl.lit(None, dtype=pl.Utf8).alias("value2"))

            cols.extend(
                [
                    pl.lit(timestamp, dtype=pl.Utf8).alias("timestamp"),
                    pl.lit(script, dtype=pl.Utf8).alias("script"),
                ]
            )

            return df.filter(mask).select(cols)

        exceptions_frames.append(
            _exc_df(
                date_conflict,
                issue="conflict",
                field="date_vs_releasedate",
                v1="_date_n",
                v2="_releasedate_n",
            )
        )
        exceptions_frames.append(
            _exc_df(
                orig_conflict,
                issue="conflict",
                field="originalreleasedate_vs_originaldate",
                v1="_originalreleasedate_n",
                v2="_originaldate_n",
            )
        )
        exceptions_frames.append(
            _exc_df(
                year_vs_date_conflict,
                issue="conflict",
                field="year_long_date_vs_date",
                v1="_year_as_date_n",
                v2="_date_existing_n",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_date,
                issue="multi_value",
                field="date",
                v1="_date_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_releasedate,
                issue="multi_value",
                field="releasedate",
                v1="_releasedate_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_originalreleasedate,
                issue="multi_value",
                field="originalreleasedate",
                v1="_originalreleasedate_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_originaldate,
                issue="multi_value",
                field="originaldate",
                v1="_originaldate_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_year,
                issue="multi_value",
                field="year",
                v1="_year_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                multi_originalyear,
                issue="multi_value",
                field="originalyear",
                v1="_originalyear_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                invalid_date,
                issue="invalid_format",
                field="date",
                v1="_date_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                invalid_originalreleasedate,
                issue="invalid_format",
                field="originalreleasedate",
                v1="_originalreleasedate_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                missing_year,
                issue="missing",
                field="year",
                v1="_year_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                invalid_year,
                issue="invalid_format",
                field="year",
                v1="_year_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                missing_originalyear,
                issue="missing",
                field="originalyear",
                v1="_originalyear_final",
            )
        )
        exceptions_frames.append(
            _exc_df(
                invalid_originalyear,
                issue="invalid_format",
                field="originalyear",
                v1="_originalyear_final",
            )
        )

        exceptions_df = pl.concat(exceptions_frames, how="vertical")
        exceptions_count = int(exceptions_df.height)

        # Compute update masks for the six tracked fields.
        df = df.with_columns(
            [
                pl.col("_year_final").alias("_new_year"),
                pl.col("_date_final").alias("_new_date"),
                pl.col("_releasedate_final").alias("_new_releasedate"),
                pl.col("_originalyear_final").alias("_new_originalyear"),
                pl.col("_originaldate_final").alias("_new_originaldate"),
                pl.col("_originalreleasedate_final").alias("_new_originalreleasedate"),
            ]
        )

        change_exprs = [
            _changed(pl.col("year"), pl.col("_new_year")).alias("_chg_year"),
            _changed(pl.col("date"), pl.col("_new_date")).alias("_chg_date"),
            _changed(pl.col("releasedate"), pl.col("_new_releasedate")).alias("_chg_releasedate"),
            _changed(pl.col("originalyear"), pl.col("_new_originalyear")).alias("_chg_originalyear"),
            _changed(pl.col("originaldate"), pl.col("_new_originaldate")).alias("_chg_originaldate"),
            _changed(pl.col("originalreleasedate"), pl.col("_new_originalreleasedate")).alias("_chg_originalreleasedate"),
        ]

        df = df.with_columns(change_exprs)

        df = df.with_columns(
            pl.sum_horizontal([
                pl.col("_chg_year").cast(pl.Int16),
                pl.col("_chg_date").cast(pl.Int16),
                pl.col("_chg_releasedate").cast(pl.Int16),
                pl.col("_chg_originalyear").cast(pl.Int16),
                pl.col("_chg_originaldate").cast(pl.Int16),
                pl.col("_chg_originalreleasedate").cast(pl.Int16),
            ]).alias("_change_count")
        )

        changed_df = df.filter(pl.col("_change_count") > 0)
        changed_rows = int(changed_df.height)

        logging.info("Detected %d row(s) with date/year changes", changed_rows)
        logging.info("Detected %d exception row(s)", exceptions_count)

        # Apply DB updates + changelog + exception table in one transaction.
        with tm_db.transaction(conn):
            cur = conn.cursor()

            tm_db.ensure_changelog_table(conn)

            _ensure_exceptions_table(conn, exceptions_table)
            cur.execute(f"DELETE FROM {tm_db.quote_ident(exceptions_table)}")

            if exceptions_count:
                cur.executemany(
                    f"""
                    INSERT INTO {tm_db.quote_ident(exceptions_table)}
                        (alib_path, issue, field, value1, value2, timestamp, script)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """.strip(),
                    [
                        (
                            r.get("alib_path"),
                            r.get("issue"),
                            r.get("field"),
                            r.get("value1"),
                            r.get("value2"),
                            r.get("timestamp"),
                            r.get("script"),
                        )
                        for r in exceptions_df.to_dicts()
                    ],
                )

            if changed_rows:
                set_cols = [
                    "year",
                    "date",
                    "releasedate",
                    "originalyear",
                    "originaldate",
                    "originalreleasedate",
                ]

                update_sql = tm_db.build_update_sql(table=alib_table, set_cols=set_cols)

                changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)

                for r in changed_df.iter_rows(named=True):
                    rowid = int(r["rowid"])
                    alib_path = str(r.get("__path") or "")

                    changes: list[tuple[str, object, object]] = []

                    def _maybe(field: str, new_key: str, chg_key: str) -> None:
                        if not r.get(chg_key):
                            return
                        changes.append((field, r.get(field), r.get(new_key)))

                    _maybe("year", "_new_year", "_chg_year")
                    _maybe("date", "_new_date", "_chg_date")
                    _maybe("releasedate", "_new_releasedate", "_chg_releasedate")
                    _maybe("originalyear", "_new_originalyear", "_chg_originalyear")
                    _maybe("originaldate", "_new_originaldate", "_chg_originaldate")
                    _maybe("originalreleasedate", "_new_originalreleasedate", "_chg_originalreleasedate")

                    if changes:
                        changelog.add(alib_path=alib_path, changes=changes)

                    new_values = (
                        r.get("_new_year"),
                        r.get("_new_date"),
                        r.get("_new_releasedate"),
                        r.get("_new_originalyear"),
                        r.get("_new_originaldate"),
                        r.get("_new_originalreleasedate"),
                        int(r.get("__sqlmodded") or 0) + int(r.get("_change_count") or 0),
                        rowid,
                    )
                    cur.execute(update_sql, new_values)

                changelog.flush(cur)

        if exceptions_count == 0:
            logging.info("No exceptions detected.")
        else:
            logging.warning(
                "Exceptions detected: %d row(s) written to %s", exceptions_count, exceptions_table
            )

        logging.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
