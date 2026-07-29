#!/usr/bin/env python3
""" 
Purpose:
    Exception-only report: flag albums (distinct __dirpath) where one or more
    tracks are missing values for critical tag fields.

Policy:
        - Album is defined as an album-root folder derived from `__dirpath`:
                - If `__dirpath` ends in a disc subfolder like "CD1", "Disc 02", "disc003"
                    (case-insensitive; optional space; 1-3 digits), the album root is the
                    parent directory of that disc subfolder.
                - Otherwise the album root is `__dirpath`.
    - A field is "missing" if it is NULL or empty after trimming.
    - If any track in an album is missing a critical field, emit a report row per album.
    - The report table is wide: each critical field is a column containing the missing
      track count for that album (NULL if not missing for that album).
    - Report is written to a single SQLite table (dropped + recreated each run).

Config:
    - tagminder.toml [reports.missing_critical_tags_by_album].critical_columns
    - tagminder.toml [reports.missing_critical_tags_by_album].table

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - _INF_missing_critical_tags_by_album (configurable)

Author: audiomuze
Last updated: 2026-04-16
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import polars as pl

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db
from tagminder.core import tm_run

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _get_report_config() -> tuple[list[str], str]:
    cfg = tm_config.load_config()
    reports = cfg.get("reports", {}) if isinstance(cfg, dict) else {}
    report_cfg = reports.get("missing_critical_tags_by_album", {}) if isinstance(reports, dict) else {}

    if not isinstance(report_cfg, dict):
        raise RuntimeError(
            "Missing config table [reports.missing_critical_tags_by_album] in tagminder.toml"
        )

    cols = report_cfg.get("critical_columns")
    if not isinstance(cols, list) or not cols or not all(isinstance(x, str) and x for x in cols):
        raise RuntimeError(
            "Invalid or missing [reports.missing_critical_tags_by_album].critical_columns in tagminder.toml"
        )

    table = report_cfg.get("table")
    if not isinstance(table, str) or not table:
        raise RuntimeError(
            "Invalid or missing [reports.missing_critical_tags_by_album].table in tagminder.toml"
        )

    # De-dupe while preserving order.
    seen: set[str] = set()
    out_cols: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out_cols.append(c)

    return out_cols, table


def _ensure_report_table(conn: sqlite3.Connection, table: str, *, critical_cols: list[str]) -> None:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(table)}")

    # Wide table: one row per album, one column per critical field.
    crit_cols_sql = ",\n            ".join(
        f"{tm_db.quote_ident(c)} INTEGER" for c in critical_cols
    )

    cur.execute(
        f"""
        CREATE TABLE {tm_db.quote_ident(table)} (
            album_dirpath TEXT,
            album_dirname TEXT,
            total_tracks INTEGER,
            {crit_cols_sql},
            timestamp TEXT,
            script TEXT
        )
        """.strip()
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        prog="94-report-missing-critical-tags-by-album.py",
        description="Exception-only report: albums with missing critical tag fields.",
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
        ensure_changelog=False,
    )

    try:
        logging.info("DB: %s", db_path)

        cfg = tm_config.load_config()
        db_cfg = cfg.get("db", {}) if isinstance(cfg, dict) else {}
        alib_table = str(db_cfg.get("alib_table") or "alib") if isinstance(db_cfg, dict) else "alib"

        critical_cols, report_table = _get_report_config()

        existing_cols = tm_db.table_columns(conn, alib_table)
        has_compilation = "compilation" in existing_cols

        tm_db.require_table_columns(
            conn,
            alib_table,
            ["__dirpath", *critical_cols],
            hint="Ensure your staging DB is populated (tags2db import) and your config matches the alib schema.",
        )

        # Read only required columns (+ compilation if present, for conditional albumartist logic).
        select_cols = ["__dirpath", *critical_cols]
        if has_compilation:
            select_cols.append("compilation")

        quoted_cols = ", ".join(tm_db.quote_ident(c) for c in select_cols)

        df = tm_polars_db.sqlite_to_polars(
            conn,
            f"SELECT {quoted_cols} FROM {tm_db.quote_ident(alib_table)}",
        )

        if df.is_empty():
            logging.info("No rows found in alib; report will be empty.")
            with tm_db.transaction(conn):
                _ensure_report_table(conn, report_table, critical_cols=critical_cols)
            logging.info("Done.")
            return 0

        def _missing(col: str) -> pl.Expr:
            s = pl.col(col).cast(pl.Utf8).str.strip_chars()
            return s.is_null() | (s == "")

        # Derive an album-root folder for grouping (shared policy).
        df = df.with_columns(tm_album.album_root_polars_expr("__dirpath", out_col="_album_dirpath"))

        missing_aggs: list[pl.Expr] = [
            _missing(c).cast(pl.Int32).sum().alias(c)
            for c in critical_cols
        ]

        aggs: list[pl.Expr] = [
            pl.len().alias("total_tracks"),
            *missing_aggs,
        ]

        if has_compilation:
            aggs.append(
                pl.col("compilation")
                .cast(pl.Int32, strict=False)
                .fill_null(0)
                .max()
                .alias("_album_is_compilation")
            )

        grouped = df.group_by("_album_dirpath").agg(aggs)

        # If an album is a compilation (compilation=1), albumartist is not required.
        # Zero it out so it won't trigger the album flag (and later becomes NULL for readability).
        if has_compilation and "albumartist" in critical_cols:
            grouped = grouped.with_columns(
                pl.when(pl.col("_album_is_compilation") == 1)
                .then(pl.lit(0, dtype=pl.Int32))
                .otherwise(pl.col("albumartist"))
                .alias("albumartist")
            )

        any_missing = pl.any_horizontal([pl.col(c) > 0 for c in critical_cols]).alias("_any_missing")

        wide_df = (
            grouped.with_columns(any_missing)
            .filter(pl.col("_any_missing"))
            .drop("_any_missing")
        )

        # Convert 0 -> NULL for report readability.
        wide_df = wide_df.with_columns(
            [
                pl.when(pl.col(c) == 0)
                .then(pl.lit(None, dtype=pl.Int32))
                .otherwise(pl.col(c))
                .alias(c)
                for c in critical_cols
            ]
        )

        wide_df = (
            wide_df.with_columns(
                [
                    pl.col("_album_dirpath").alias("album_dirpath"),
                    pl.col("_album_dirpath")
                    .cast(pl.Utf8)
                    .str.replace(r"/$", "")
                    .str.replace(r"^.*/", "")
                    .alias("album_dirname"),
                    pl.lit(timestamp, dtype=pl.Utf8).alias("timestamp"),
                    pl.lit(script, dtype=pl.Utf8).alias("script"),
                ]
            )
            .select(
                [
                    "album_dirpath",
                    "album_dirname",
                    "total_tracks",
                    *critical_cols,
                    "timestamp",
                    "script",
                ]
            )
        )

        # Stable insertion order in SQLite.
        wide_df = wide_df.sort("album_dirpath")

        report_albums = int(wide_df.height)

        logging.info("Flagged %d album(s) with missing critical fields", report_albums)

        insert_cols = ["album_dirpath", "album_dirname", "total_tracks", *critical_cols, "timestamp", "script"]
        insert_cols_sql = ", ".join(tm_db.quote_ident(c) for c in insert_cols)
        placeholders = ", ".join(["?"] * len(insert_cols))

        with tm_db.transaction(conn):
            _ensure_report_table(conn, report_table, critical_cols=critical_cols)

            if report_albums:
                conn.executemany(
                    f"""
                    INSERT INTO {tm_db.quote_ident(report_table)}
                        ({insert_cols_sql})
                    VALUES ({placeholders})
                    """.strip(),
                    [
                        (
                            r.get("album_dirpath"),
                            r.get("album_dirname"),
                            int(r.get("total_tracks") or 0),
                            *[
                                (None if (v := r.get(c)) is None else int(v))
                                for c in critical_cols
                            ],
                            r.get("timestamp"),
                            r.get("script"),
                        )
                        for r in wide_df.to_dicts()
                    ],
                )

        if report_albums == 0:
            logging.info("No missing critical tags detected.")
        else:
            logging.warning(
                "Missing critical tags detected: %d album(s) written to %s",
                report_albums,
                report_table,
            )

        logging.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
