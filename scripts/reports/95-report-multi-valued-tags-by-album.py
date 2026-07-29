#!/usr/bin/env python3
""" 
Purpose:
    Exception-only report: flag albums (distinct album-root derived from __dirpath)
    where one or more nominated tags have more than one distinct value across
    tracks in that album folder.

Policy:
    - Album is defined as an album-root folder derived from `__dirpath`:
        - If `__dirpath` ends in a disc subfolder like "CD1", "Disc 02", "disc003"
          (case-insensitive; optional space; 1-3 digits), the album root is the
          parent directory of that disc subfolder.
        - Otherwise the album root is `__dirpath`.
    - A tag value is considered for distinctness if it is non-NULL and non-empty
      after trimming.
    - An album is flagged when any nominated tag has > 1 distinct non-empty value.
    - The report table is wide: each nominated tag is a column containing the
      distinct value count for that album (NULL unless the count is > 1).
    - Report is written to a single SQLite table (dropped + recreated each run).

Notes:
    - For multi-value text tags (genre/style/mood/theme), values are canonicalized
      by splitting on Tagminder's configured delimiter, trimming, de-duping,
      lowercasing, sorting, then re-joining. This avoids false positives from
      token ordering differences.

Config:
    - tagminder.toml [reports.multi_value_tags_by_album].tags
    - tagminder.toml [reports.multi_value_tags_by_album].table

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - _INF_multi_value_tags_by_album (created by this script)

Author: audiomuze
Last updated: 2026-04-25
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import polars as pl

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_polars_db
from tagminder.core import tm_run

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _get_report_config() -> tuple[list[str], str]:
    cfg = tm_config.load_config()
    reports = cfg.get("reports", {}) if isinstance(cfg, dict) else {}
    report_cfg = (
        reports.get("multi_value_tags_by_album", {}) if isinstance(reports, dict) else {}
    )

    if not isinstance(report_cfg, dict):
        raise RuntimeError(
            "Missing config table [reports.multi_value_tags_by_album] in tagminder.toml"
        )

    tags = report_cfg.get("tags")
    if (
        not isinstance(tags, list)
        or not tags
        or not all(isinstance(x, str) and x for x in tags)
    ):
        raise RuntimeError(
            "Invalid or missing [reports.multi_value_tags_by_album].tags in tagminder.toml"
        )

    table = report_cfg.get("table")
    if not isinstance(table, str) or not table:
        raise RuntimeError(
            "Invalid or missing [reports.multi_value_tags_by_album].table in tagminder.toml"
        )

    # De-dupe while preserving order.
    seen: set[str] = set()
    out_tags: list[str] = []
    for t in tags:
        if t in seen:
            continue
        seen.add(t)
        out_tags.append(t)

    return out_tags, table

# Tags treated as Tagminder multi-value delimited fields.
MULTIVALUE_TOKEN_TAGS = {"genre", "style", "mood", "theme"}


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _ensure_report_table(conn: sqlite3.Connection, table: str, *, tag_cols: list[str]) -> None:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(table)}")

    cols_sql = ",\n            ".join(f"{tm_db.quote_ident(c)} INTEGER" for c in tag_cols)

    cur.execute(
        f"""
        CREATE TABLE {tm_db.quote_ident(table)} (
            album_dirpath TEXT,
            album_dirname TEXT,
            total_tracks INTEGER,
            {cols_sql},
            timestamp TEXT,
            script TEXT
        )
        """.strip()
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        prog="95-report-multi-valued-tags-by-album.py",
        description="Exception-only report: albums with >1 distinct value in nominated tags.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )
    parser.add_argument(
        "--table",
        metavar="NAME",
        default=None,
        help="SQLite table name to write (default: tagminder.toml [reports.multi_value_tags_by_album].table)",
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
        alib_table = (
            str(db_cfg.get("alib_table") or "alib")
            if isinstance(db_cfg, dict)
            else "alib"
        )

        tags, default_table = _get_report_config()
        report_table = str(args.table or default_table)

        # Ensure __dirpath exists; tag columns are optional (we'll fill NULLs for missing).
        tm_db.require_table_columns(
            conn,
            alib_table,
            ["__dirpath"],
            hint="Ensure your staging DB is populated (tags2db import) and your config matches the alib schema.",
        )

        existing_cols = tm_db.table_columns(conn, alib_table)
        present = [t for t in tags if t in existing_cols]
        missing = [t for t in tags if t not in existing_cols]
        if missing:
            logging.warning(
                "Skipping %d nominated tag(s) not present in %s: %s",
                len(missing),
                alib_table,
                ", ".join(missing),
            )

        select_cols = ["__dirpath", *present]
        quoted_cols = ", ".join(tm_db.quote_ident(c) for c in select_cols)

        df = tm_polars_db.sqlite_to_polars(
            conn,
            f"SELECT {quoted_cols} FROM {tm_db.quote_ident(alib_table)}",
        )

        if df.is_empty():
            logging.info("No rows found in alib; report will be empty.")
            with tm_db.transaction(conn):
                _ensure_report_table(conn, report_table, tag_cols=tags)
            logging.info("Done.")
            return 0

        # Add missing nominated columns as NULLs so we can compute a stable wide report.
        for t in tags:
            if t not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(t))

        delimiter = tm_config.get_multivalue_delimiter()

        def _norm_expr(col: str) -> pl.Expr:
            if col in MULTIVALUE_TOKEN_TAGS:
                return (
                    tm_polars.expr_tokens(pl.col(col), delimiter=delimiter)
                    .list.eval(pl.element().cast(pl.Utf8, strict=False).str.to_lowercase())
                    .list.sort()
                    .list.join(delimiter)
                )
            return pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars()

        def _distinct_count(col: str) -> pl.Expr:
            s = _norm_expr(col)
            return (
                s.filter(s.is_not_null() & (s != ""))
                .n_unique()
                .cast(pl.Int32)
                .alias(col)
            )

        # Derive an album-root folder for grouping (shared policy).
        df = df.with_columns(tm_album.album_root_polars_expr("__dirpath", out_col="_album_dirpath"))

        aggs: list[pl.Expr] = [
            pl.len().alias("total_tracks"),
            *[_distinct_count(c) for c in tags],
        ]

        grouped = df.group_by("_album_dirpath").agg(aggs)

        any_multi = pl.any_horizontal([pl.col(c) > 1 for c in tags]).alias("_any_multi")

        wide_df = grouped.with_columns(any_multi).filter(pl.col("_any_multi")).drop("_any_multi")

        # Convert <=1 -> NULL for readability.
        wide_df = wide_df.with_columns(
            [
                pl.when(pl.col(c) <= 1)
                .then(pl.lit(None, dtype=pl.Int32))
                .otherwise(pl.col(c))
                .alias(c)
                for c in tags
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
                    *tags,
                    "timestamp",
                    "script",
                ]
            )
        )

        wide_df = wide_df.sort("album_dirpath")

        report_albums = int(wide_df.height)
        logging.info("Flagged %d album(s) with multi-valued nominated tags", report_albums)

        insert_cols = ["album_dirpath", "album_dirname", "total_tracks", *tags, "timestamp", "script"]
        insert_cols_sql = ", ".join(tm_db.quote_ident(c) for c in insert_cols)
        placeholders = ", ".join(["?"] * len(insert_cols))

        with tm_db.transaction(conn):
            _ensure_report_table(conn, report_table, tag_cols=tags)

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
                            *[(None if (v := r.get(c)) is None else int(v)) for c in tags],
                            r.get("timestamp"),
                            r.get("script"),
                        )
                        for r in wide_df.to_dicts()
                    ],
                )

        if report_albums == 0:
            logging.info("No multi-valued nominated tags detected.")
        else:
            logging.warning(
                "Multi-valued nominated tags detected: %d album(s) written to %s",
                report_albums,
                report_table,
            )

        logging.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
