#!/usr/bin/env python3
""" 
Purpose:
    Exception-only report: flag albums (distinct __dirpath) where track numbers are
    missing or out-of-sequence.

Policy:
        - Album is defined as an album-root folder derived from `__dirpath`:
                - If `__dirpath` ends in a disc subfolder like "CD1", "Disc 02", "disc003"
                    (case-insensitive; optional space; 1-3 digits), the album root is the
                    parent directory of that disc subfolder.
                - Otherwise the album root is `__dirpath`.
    - Disc grouping is applied when a disc column is available/configured.
      If not, all tracks are treated as disc 1.
    - Track number parsing is numeric-leading (e.g. "01" -> 1, "1/10" -> 1).
    - A track number is "missing" if NULL or empty after trimming.
    - A track number is "invalid" if non-empty but cannot be parsed as an int.
    - A disc is flagged if any of these are true:
        - missing track numbers
        - invalid track numbers
        - duplicate parsed track numbers
        - gaps in the numeric sequence 1..max(track)

Config:
    - tagminder.toml [reports.track_sequence_by_album].track_column
    - tagminder.toml [reports.track_sequence_by_album].disc_column
    - tagminder.toml [reports.track_sequence_by_album].table

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - _INF_track_sequence_by_album (configurable)

Author: audiomuze
Last updated: 2026-04-17
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


def _get_report_config() -> tuple[str, str | None, str]:
    cfg = tm_config.load_config()
    reports = cfg.get("reports", {}) if isinstance(cfg, dict) else {}
    report_cfg = reports.get("track_sequence_by_album", {}) if isinstance(reports, dict) else {}

    if not isinstance(report_cfg, dict):
        raise RuntimeError(
            "Missing config table [reports.track_sequence_by_album] in tagminder.toml"
        )

    track_col = report_cfg.get("track_column")
    if not isinstance(track_col, str) or not track_col:
        raise RuntimeError(
            "Invalid or missing [reports.track_sequence_by_album].track_column in tagminder.toml"
        )

    disc_col = report_cfg.get("disc_column")
    if disc_col is not None and (not isinstance(disc_col, str) or not disc_col.strip()):
        disc_col = None

    table = report_cfg.get("table")
    if not isinstance(table, str) or not table:
        raise RuntimeError(
            "Invalid or missing [reports.track_sequence_by_album].table in tagminder.toml"
        )

    return track_col, disc_col, table


def _ensure_report_table(conn: sqlite3.Connection, table: str) -> None:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(table)}")
    cur.execute(
        f"""
        CREATE TABLE {tm_db.quote_ident(table)} (
            album_dirpath TEXT,
            album_dirname TEXT,
            disc_number INTEGER,
            total_tracks INTEGER,
            parsed_track_count INTEGER,
            unique_track_count INTEGER,
            max_track INTEGER,
            missing_track_count INTEGER,
            invalid_track_count INTEGER,
            duplicate_track_count INTEGER,
            duplicate_track_numbers TEXT,
            missing_seq_count INTEGER,
            missing_seq TEXT,
            timestamp TEXT,
            script TEXT
        )
        """.strip()
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        prog="93-report-track-sequence-anomalies-by-album.py",
        description="Exception-only report: albums with out-of-sequence track numbers.",
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

        track_col, disc_col, report_table = _get_report_config()

        existing_cols = tm_db.table_columns(conn, alib_table)

        has_disc = bool(disc_col) and disc_col in existing_cols
        if disc_col and not has_disc:
            logging.warning("Configured disc column %r not found in %r; treating all tracks as disc 1", disc_col, alib_table)

        required = ["__dirpath", track_col]
        if has_disc and disc_col:
            required.append(disc_col)

        tm_db.require_table_columns(
            conn,
            alib_table,
            required,
            hint="Ensure your staging DB is populated (tags2db import) and your config matches the alib schema.",
        )

        # Read only required columns.
        select_cols = ["__dirpath", track_col]
        if has_disc and disc_col:
            select_cols.append(disc_col)

        quoted_cols = ", ".join(tm_db.quote_ident(c) for c in select_cols)
        df = tm_polars_db.sqlite_to_polars(
            conn,
            f"SELECT {quoted_cols} FROM {tm_db.quote_ident(alib_table)}",
        )

        if df.is_empty():
            logging.info("No rows found in alib; report will be empty.")
            with tm_db.transaction(conn):
                _ensure_report_table(conn, report_table)
            logging.info("Done.")
            return 0

        def _clean(col: str) -> pl.Expr:
            s = pl.col(col).cast(pl.Utf8).str.strip_chars()
            return pl.when(s.is_null() | (s == "")).then(pl.lit(None, dtype=pl.Utf8)).otherwise(s)

        track_s = _clean(track_col)
        track_n = track_s.str.extract(r"^(\d+)", 1).cast(pl.Int32, strict=False)

        if has_disc and disc_col:
            disc_s = _clean(disc_col)
            disc_n = (
                disc_s.str.extract(r"^(\d+)", 1)
                .cast(pl.Int32, strict=False)
                .fill_null(1)
            )
        else:
            disc_n = pl.lit(1, dtype=pl.Int32)

        df = df.with_columns(
            [
                pl.col("__dirpath"),
                disc_n.alias("_disc_n"),
                track_s.alias("_track_s"),
                track_n.alias("_track_n"),
            ]
        )

        # Derive an album-root folder for grouping (shared policy).
        df = df.with_columns(tm_album.album_root_polars_expr("__dirpath", out_col="_album_dirpath"))

        missing_track = pl.col("_track_s").is_null()
        invalid_track = pl.col("_track_s").is_not_null() & pl.col("_track_n").is_null()

        grouped = (
            df.group_by(["_album_dirpath", "_disc_n"])
            .agg(
                [
                    pl.len().alias("total_tracks"),
                    missing_track.cast(pl.Int32).sum().alias("missing_track_count"),
                    invalid_track.cast(pl.Int32).sum().alias("invalid_track_count"),
                    pl.col("_track_n").is_not_null().cast(pl.Int32).sum().alias("parsed_track_count"),
                    pl.col("_track_n").drop_nulls().n_unique().alias("unique_track_count"),
                    pl.col("_track_n").max().alias("max_track"),
                    pl.col("_track_n").drop_nulls().unique().sort().alias("_tracks"),
                    pl.col("_track_n").drop_nulls().value_counts().alias("_track_counts"),
                ]
            )
            .with_columns(
                [
                    (pl.col("parsed_track_count") - pl.col("unique_track_count")).alias("duplicate_track_count"),
                    pl.col("_track_counts")
                    .list.eval(
                        pl.when(pl.element().struct.field("count") > 1)
                        .then(pl.element().struct.field("_track_n"))
                        .otherwise(pl.lit(None, dtype=pl.Int64))
                    )
                    .list.drop_nulls()
                    .list.sort()
                    .alias("_duplicate_track_numbers"),
                ]
            )
            .with_columns(
                pl.when(pl.col("_duplicate_track_numbers").list.len() == 0)
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("_duplicate_track_numbers").cast(pl.List(pl.Utf8)).list.join(","))
                .alias("duplicate_track_numbers")
            )
        )

        # Missing sequence numbers within 1..max(track).
        missing_seq_list = (
            pl.when(pl.col("max_track").is_not_null() & (pl.col("max_track") > 0))
            .then(
                pl.int_ranges(1, pl.col("max_track") + 1)
                .list.set_difference(pl.col("_tracks"))
                .list.sort()
            )
            .otherwise(pl.lit([], dtype=pl.List(pl.Int64)))
            .alias("_missing_seq")
        )

        grouped = grouped.with_columns(missing_seq_list)

        grouped = grouped.with_columns(
            [
                pl.col("_missing_seq").list.len().cast(pl.Int32).alias("missing_seq_count"),
                pl.when(pl.col("_missing_seq").list.len() == 0)
                .then(pl.lit(None, dtype=pl.Utf8))
                .otherwise(pl.col("_missing_seq").cast(pl.List(pl.Utf8)).list.join(","))
                .alias("missing_seq"),
            ]
        )

        any_issue = pl.any_horizontal(
            [
                pl.col("missing_track_count") > 0,
                pl.col("invalid_track_count") > 0,
                pl.col("duplicate_track_count") > 0,
                pl.col("missing_seq_count") > 0,
            ]
        ).alias("_any_issue")

        out_df = (
            grouped.with_columns(any_issue)
            .filter(pl.col("_any_issue"))
            .drop(["_any_issue", "_tracks", "_track_counts", "_duplicate_track_numbers", "_missing_seq"])
            .with_columns(
                [
                    pl.col("_album_dirpath").alias("album_dirpath"),
                    pl.col("_album_dirpath")
                    .cast(pl.Utf8)
                    .str.replace(r"/$", "")
                    .str.replace(r"^.*/", "")
                    .alias("album_dirname"),
                    pl.col("_disc_n").alias("disc_number"),
                    pl.lit(timestamp, dtype=pl.Utf8).alias("timestamp"),
                    pl.lit(script, dtype=pl.Utf8).alias("script"),
                ]
            )
            .select(
                [
                    "album_dirpath",
                    "album_dirname",
                    "disc_number",
                    "total_tracks",
                    "parsed_track_count",
                    "unique_track_count",
                    "max_track",
                    "missing_track_count",
                    "invalid_track_count",
                    "duplicate_track_count",
                    "duplicate_track_numbers",
                    "missing_seq_count",
                    "missing_seq",
                    "timestamp",
                    "script",
                ]
            )
        )

        # Convert 0 -> NULL for report readability.
        count_cols = [
            "missing_track_count",
            "invalid_track_count",
            "duplicate_track_count",
            "missing_seq_count",
        ]
        out_df = out_df.with_columns(
            [
                pl.when(pl.col(c) == 0)
                .then(pl.lit(None, dtype=pl.Int32))
                .otherwise(pl.col(c))
                .alias(c)
                for c in count_cols
            ]
        )

        out_df = out_df.sort(["album_dirpath", "disc_number"])

        report_rows = int(out_df.height)
        logging.info("Flagged %d album-disc group(s) with track sequence issues", report_rows)

        insert_cols = [
            "album_dirpath",
            "album_dirname",
            "disc_number",
            "total_tracks",
            "parsed_track_count",
            "unique_track_count",
            "max_track",
            "missing_track_count",
            "invalid_track_count",
            "duplicate_track_count",
            "duplicate_track_numbers",
            "missing_seq_count",
            "missing_seq",
            "timestamp",
            "script",
        ]

        insert_cols_sql = ", ".join(tm_db.quote_ident(c) for c in insert_cols)
        placeholders = ", ".join(["?"] * len(insert_cols))

        with tm_db.transaction(conn):
            _ensure_report_table(conn, report_table)

            if report_rows:
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
                            int(r.get("disc_number") or 1),
                            int(r.get("total_tracks") or 0),
                            int(r.get("parsed_track_count") or 0),
                            int(r.get("unique_track_count") or 0),
                            (None if (v := r.get("max_track")) is None else int(v)),
                            (None if (v := r.get("missing_track_count")) is None else int(v)),
                            (None if (v := r.get("invalid_track_count")) is None else int(v)),
                            (None if (v := r.get("duplicate_track_count")) is None else int(v)),
                            r.get("duplicate_track_numbers"),
                            (None if (v := r.get("missing_seq_count")) is None else int(v)),
                            r.get("missing_seq"),
                            r.get("timestamp"),
                            r.get("script"),
                        )
                        for r in out_df.to_dicts()
                    ],
                )

        if report_rows == 0:
            logging.info("No track sequence issues detected.")
        else:
            logging.warning(
                "Track sequence issues detected: %d row(s) written to %s",
                report_rows,
                report_table,
            )

        logging.info("Done.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
