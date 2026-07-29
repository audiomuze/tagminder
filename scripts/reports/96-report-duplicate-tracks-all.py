"""
Purpose:
    Detect duplicate tracks (duplicate files) both globally and within folders
    by comparing per-file `__md5sig` values in `alib`.

    This is an offline, deterministic report step. It does not modify `alib` and
    does not write to `changelog`.

    This script generates both reports in a single pass for efficiency:
    1) Global duplicates: tracks with identical __md5sig anywhere in library
    2) Intra-folder duplicates: tracks with identical __md5sig within same __dirpath

Detection logic:
    1) Consider only rows where `__filetype` is "FLAC" or "WavPack".
    2) Exclude rows with invalid `__md5sig` from duplicate detection.
       Invalid criteria (per user guidance), evaluated after internal trimming
       and removing "-" for the all-zeros check only:
         - NULL
         - empty string
         - "0" (or numeric 0, after casting)
         - all-zero (e.g., "0000..." or "0000-0000-..." after removing "-")
    3) Group by __md5sig for global duplicates, and by __dirpath/__md5sig for intra-folder.
    4) Emit detail tables for every duplicate occurrence in both modes.

Outputs (SQLite tables created/replaced):
    Global Duplicates:
    - report_duplicate_tracks_by_md5sig
        Columns: __dirpath, __filename, __md5sig
        (Rows where `__md5sig` occurs more than once globally.)
    - report_duplicate_tracks_md5sig_summary
        Columns: __md5sig, file_count
        (One row per globally duplicated `__md5sig`.)

    Intra-Folder Duplicates:
    - report_duplicate_tracks_in_folder_by_md5sig
        Columns: __dirpath, __filename, __md5sig
        (Rows where `__md5sig` + `__dirpath` occurs more than once.)
    - report_duplicate_tracks_in_folder_md5sig_summary
        Columns: __dirpath, __md5sig, file_count
        (One row per duplicated `__md5sig` within each folder.)

    Invalid MD5 (shared):
    - report_duplicate_tracks_invalid_md5sig
        Columns: __dirpath, __filename, __md5sig
        (Rows excluded due to invalid `__md5sig`.)

This script is part of Tagminder.

SQLite tables referenced:
    - alib

Author: audiomuze
Last updated: 2026-06-25
"""

from __future__ import annotations

import argparse
import logging
import sqlite3

import polars as pl

from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_run

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

FILETYPES = ("FLAC", "WavPack")

# Global report table names
T_DUPLICATES_GLOBAL = "report_duplicate_tracks_by_md5sig"
T_SUMMARY_GLOBAL = "report_duplicate_tracks_md5sig_summary"

# Intra-folder report table names
T_DUPLICATES_FOLDER = "report_duplicate_tracks_in_folder_by_md5sig"
T_SUMMARY_FOLDER = "report_duplicate_tracks_in_folder_md5sig_summary"

# Shared invalid MD5 table
T_INVALID = "report_duplicate_tracks_invalid_md5sig"

# All tables for cleanup
ALL_TABLES = (
    T_DUPLICATES_GLOBAL,
    T_SUMMARY_GLOBAL,
    T_DUPLICATES_FOLDER,
    T_SUMMARY_FOLDER,
    T_INVALID,
)


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, force=True)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Report duplicate tracks (global and intra-folder) by FLAC/WavPack __md5sig values",
    )
    p.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )
    return p.parse_args()


def _write_rows(
    cur: sqlite3.Cursor,
    *,
    table: str,
    columns: list[str],
    rows: object,
) -> None:
    cols_sql = ", ".join(tm_db.quote_ident(c) for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT INTO {tm_db.quote_ident(table)} ({cols_sql}) VALUES ({placeholders})"
    cur.executemany(sql, rows)


def _load_flac_wavpack_rows(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT __dirpath, __filename, __md5sig, __filetype
        FROM alib
        WHERE __dirpath IS NOT NULL
          AND __filename IS NOT NULL
          AND __filetype IN ('FLAC', 'WavPack')
    """.strip()

    # Some columns are stored as TEXT; be explicit.
    return pl.read_database(
        query,
        conn,
        schema_overrides={
            "__dirpath": pl.Utf8,
            "__filename": pl.Utf8,
            "__md5sig": pl.Utf8,
            "__filetype": pl.Utf8,
        },
    )


def _build_reports(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Build both global and intra-folder duplicate reports in a single pass."""

    empty_detail = pl.DataFrame(
        {"__dirpath": [], "__filename": [], "__md5sig": []},
        schema={"__dirpath": pl.Utf8, "__filename": pl.Utf8, "__md5sig": pl.Utf8},
    )
    empty_summary_global = pl.DataFrame(
        {"__md5sig": [], "file_count": []},
        schema={"__md5sig": pl.Utf8, "file_count": pl.Int64},
    )
    empty_summary_folder = pl.DataFrame(
        {"__dirpath": [], "__md5sig": [], "file_count": []},
        schema={"__dirpath": pl.Utf8, "__md5sig": pl.Utf8, "file_count": pl.Int64},
    )

    if df.is_empty():
        return {
            T_DUPLICATES_GLOBAL: empty_detail,
            T_SUMMARY_GLOBAL: empty_summary_global,
            T_DUPLICATES_FOLDER: empty_detail,
            T_SUMMARY_FOLDER: empty_summary_folder,
            T_INVALID: empty_detail,
        }

    # Evaluate invalid criteria using shared logic (see tm_polars.expr_md5sig_is_invalid).
    invalid_md5 = tm_polars.expr_md5sig_is_invalid(pl.col("__md5sig"))
    valid_md5 = ~invalid_md5

    invalid_rows = (
        df.lazy()
        .filter(~valid_md5)
        .select(["__dirpath", "__filename", "__md5sig"])
        .collect()
        .sort(["__dirpath", "__filename"])
    )

    valid_rows = df.filter(valid_md5).select(["__dirpath", "__filename", "__md5sig"])

    # === GLOBAL DUPLICATES ===
    global_dup_counts = (
        valid_rows.lazy()
        .group_by("__md5sig")
        .agg(pl.len().alias("file_count"))
        .filter(pl.col("file_count") > 1)
        .collect()
    )

    detail_global = (
        valid_rows.join(global_dup_counts.select(["__md5sig"]), on="__md5sig", how="inner")
        .sort(["__md5sig", "__dirpath", "__filename"])
    )

    summary_global = global_dup_counts.sort(["file_count", "__md5sig"], descending=[True, False])

    # === INTRA-FOLDER DUPLICATES ===
    folder_dup_counts = (
        valid_rows.lazy()
        .group_by(["__dirpath", "__md5sig"])
        .agg(pl.len().alias("file_count"))
        .filter(pl.col("file_count") > 1)
        .collect()
    )

    detail_folder = (
        valid_rows.join(
            folder_dup_counts.select(["__dirpath", "__md5sig"]),
            on=["__dirpath", "__md5sig"],
            how="inner",
        )
        .sort(["__dirpath", "__md5sig", "__filename"])
    )

    summary_folder = folder_dup_counts.sort(
        ["__dirpath", "file_count", "__md5sig"], descending=[False, True, False]
    )

    return {
        T_DUPLICATES_GLOBAL: detail_global,
        T_SUMMARY_GLOBAL: summary_global,
        T_DUPLICATES_FOLDER: detail_folder,
        T_SUMMARY_FOLDER: summary_folder,
        T_INVALID: invalid_rows,
    }


def _write_reports(conn: sqlite3.Connection, reports: dict[str, pl.DataFrame]) -> list[str]:
    """Write all report tables in a single transaction; drop empty ones to prevent stale results."""

    detail_global = reports[T_DUPLICATES_GLOBAL]
    summary_global = reports[T_SUMMARY_GLOBAL]
    detail_folder = reports[T_DUPLICATES_FOLDER]
    summary_folder = reports[T_SUMMARY_FOLDER]
    invalid = reports[T_INVALID]

    created: list[str] = []
    cur = conn.cursor()

    with tm_db.transaction(conn):
        # Always drop prior tables so empty results don't leave stale data.
        for t in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(t)}")

        # Write global duplicates detail
        if detail_global.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_DUPLICATES_GLOBAL)} ("
                    "__dirpath TEXT, __filename TEXT, __md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_DUPLICATES_GLOBAL,
                columns=["__dirpath", "__filename", "__md5sig"],
                rows=detail_global.iter_rows(),
            )
            created.append(T_DUPLICATES_GLOBAL)

        # Write global duplicates summary
        if summary_global.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_SUMMARY_GLOBAL)} ("
                    "__md5sig TEXT, file_count INTEGER"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_SUMMARY_GLOBAL,
                columns=["__md5sig", "file_count"],
                rows=summary_global.iter_rows(),
            )
            created.append(T_SUMMARY_GLOBAL)

        # Write intra-folder duplicates detail
        if detail_folder.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_DUPLICATES_FOLDER)} ("
                    "__dirpath TEXT, __filename TEXT, __md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_DUPLICATES_FOLDER,
                columns=["__dirpath", "__filename", "__md5sig"],
                rows=detail_folder.iter_rows(),
            )
            created.append(T_DUPLICATES_FOLDER)

        # Write intra-folder duplicates summary
        if summary_folder.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_SUMMARY_FOLDER)} ("
                    "__dirpath TEXT, __md5sig TEXT, file_count INTEGER"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_SUMMARY_FOLDER,
                columns=["__dirpath", "__md5sig", "file_count"],
                rows=summary_folder.iter_rows(),
            )
            created.append(T_SUMMARY_FOLDER)

        # Write invalid MD5s (shared for both modes)
        if invalid.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_INVALID)} ("
                    "__dirpath TEXT, __filename TEXT, __md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_INVALID,
                columns=["__dirpath", "__filename", "__md5sig"],
                rows=invalid.iter_rows(),
            )
            created.append(T_INVALID)

    return created


def main() -> None:
    _configure_logging()
    args = _parse_args()

    conn, db_path, _, _ = tm_run.open_db(
        db_path=args.db,
        ensure_changelog=False,
        log_connect=True,
    )

    try:
        tm_db.optimize_for_etl(conn)

        df = _load_flac_wavpack_rows(conn)
        logging.info(
            f"Loaded {df.height} FLAC/WavPack rows for duplicate evaluation (from {db_path})"
        )

        reports = _build_reports(df)

        invalid_rows = reports[T_INVALID].height
        duplicated_tracks_global = reports[T_SUMMARY_GLOBAL].height
        duplicated_rows_global = reports[T_DUPLICATES_GLOBAL].height
        duplicated_tracks_folder = reports[T_SUMMARY_FOLDER].height
        duplicated_rows_folder = reports[T_DUPLICATES_FOLDER].height

        logging.info(f"Invalid __md5sig rows (excluded): {invalid_rows}")
        logging.info(f"")
        logging.info(f"GLOBAL DUPLICATES:")
        logging.info(f"  Duplicated tracks (unique __md5sig): {duplicated_tracks_global}")
        logging.info(f"  Duplicate file occurrences (rows): {duplicated_rows_global}")
        logging.info(f"")
        logging.info(f"INTRA-FOLDER DUPLICATES:")
        logging.info(f"  Duplicated tracks (unique __dirpath/__md5sig): {duplicated_tracks_folder}")
        logging.info(f"  Duplicate file occurrences in folders (rows): {duplicated_rows_folder}")

        created = _write_reports(conn, reports)
        if created:
            logging.info(f"")
            logging.info("Report tables written:")
            for t in created:
                logging.info(f"  - {t}")
        else:
            logging.info("No report tables written (no duplicates/invalids detected).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
