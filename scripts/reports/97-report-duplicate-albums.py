"""
Purpose:
    Detect duplicate albums (duplicate folders) by comparing folder-level content
    signatures derived from FLAC/WavPack `__md5sig` values.

    This is an offline, deterministic report step. It does not modify `alib` and
    does not write to `changelog`.

Detection logic (matches the original SQL approach):
    1) Consider only rows where `__filetype` is "FLAC" or "WavPack".
    2) For each `__dirpath`, concatenate the sorted `__md5sig` values using the
       separator " | " to produce `concat__md5sig`.
    3) `__dirpath` folders with identical `concat__md5sig` are duplicates.

Additional safety rule (to avoid false positives):
    If ANY FLAC/WavPack row in a folder has an invalid `__md5sig`, the entire
    folder is excluded from duplicate evaluation and listed in a skipped report.

Keeper selection:
    For each duplicate signature group, the keeper is the folder whose FLAC/
    WavPack files have the oldest (minimum) `__file_mod_datetime_raw` epoch.
    Kill candidates are pre-populated with `kill = '1'`; keeper rows have NULL.

Outputs (SQLite tables created/replaced):
    - report_folder_content_concat_md5sig
        Columns: __dirpath, concat__md5sig

    - report_folders_with_same_album
        Columns: kill, __dirpath, concat__md5sig

    - report_folders_with_same_album_to_kill
        Columns: __dirpath, concat__md5sig

    - report_folders_skipped_invalid_md5sig
        Columns: __dirpath, total_files, invalid_md5sig_files

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - sqlite_master (for DROP/CREATE)

Author: audiomuze
Last updated: 2026-04-15
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

# Report table names (user preference: prefix 'report_')
T_SIGNATURES = "report_folder_content_concat_md5sig"
T_DUPES = "report_folders_with_same_album"
T_KILL = "report_folders_with_same_album_to_kill"
T_SKIPPED = "report_folders_skipped_invalid_md5sig"


# MD5 validity rules (per user guidance): invalid if any of the following apply
# after trimming whitespace ("-" is removed only for the all-zeros check):
# - NULL
# - empty string
# - "0" (or numeric 0, after casting)
# - all-zero (e.g., "0000..." or "0000-0000-...")


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, force=True)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Report duplicate album folders by FLAC/WavPack __md5sig signatures",
    )
    p.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )
    return p.parse_args()


def _drop_and_create(cur: sqlite3.Cursor, *, table: str, ddl: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(table)}")
    cur.execute(ddl)


def _write_rows(
    cur: sqlite3.Cursor,
    *,
    table: str,
    columns: list[str],
    rows: object,
) -> None:
    cols_sql = ", ".join(tm_db.quote_ident(c) for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    sql = (
        f"INSERT INTO {tm_db.quote_ident(table)} ({cols_sql}) VALUES ({placeholders})"
    )
    cur.executemany(sql, rows)


def _load_flac_wavpack_rows(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT __dirpath, __md5sig, __filetype, __file_mod_datetime_raw
        FROM alib
        WHERE __dirpath IS NOT NULL
          AND __filetype IN ('FLAC', 'WavPack')
    """.strip()

    # Some columns are stored as TEXT; be explicit.
    return pl.read_database(
        query,
        conn,
        schema_overrides={
            "__dirpath": pl.Utf8,
            "__md5sig": pl.Utf8,
            "__filetype": pl.Utf8,
            "__file_mod_datetime_raw": pl.Utf8,
        },
    )


def _build_reports(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    if df.is_empty():
        empty_sig = pl.DataFrame({"__dirpath": [], "concat__md5sig": []}, schema={"__dirpath": pl.Utf8, "concat__md5sig": pl.Utf8})
        empty_dupes = pl.DataFrame({"kill": [], "__dirpath": [], "concat__md5sig": []}, schema={"kill": pl.Utf8, "__dirpath": pl.Utf8, "concat__md5sig": pl.Utf8})
        empty_kill = pl.DataFrame({"__dirpath": [], "concat__md5sig": []}, schema={"__dirpath": pl.Utf8, "concat__md5sig": pl.Utf8})
        empty_skipped = pl.DataFrame({"__dirpath": [], "total_files": [], "invalid_md5sig_files": []}, schema={"__dirpath": pl.Utf8, "total_files": pl.Int64, "invalid_md5sig_files": pl.Int64})
        return {
            T_SIGNATURES: empty_sig,
            T_DUPES: empty_dupes,
            T_KILL: empty_kill,
            T_SKIPPED: empty_skipped,
        }

    # Normalize strings used for validation and signatures.
    # Keep grouping/signatures on the raw __md5sig (trimmed for stability).
    # Remove "-" only for the all-zeros check.
    df2 = df.with_columns(
        pl.col("__md5sig").cast(pl.Utf8).str.strip_chars().alias("md5"),
        pl.col("__file_mod_datetime_raw")
        .cast(pl.Utf8)
        .str.strip_chars()
        .cast(pl.Float64, strict=False)
        .alias("mod_epoch"),
    )

    # Invalid md5 (shared logic; see tm_polars.expr_md5sig_is_invalid).
    invalid_md5 = tm_polars.expr_md5sig_is_invalid(pl.col("__md5sig"))
    valid_md5 = ~invalid_md5

    # Per-folder stats and skip list (skip if any invalid md5 in folder)
    per_folder = (
        df2.lazy()
        .group_by("__dirpath")
        .agg(
            pl.len().alias("total_files"),
            invalid_md5.cast(pl.Int64).sum().alias("invalid_md5sig_files"),
            pl.col("mod_epoch").min().alias("oldest_mod_epoch"),
        )
        .with_columns((pl.col("invalid_md5sig_files") > 0).alias("skip"))
        .collect()
    )

    skipped = per_folder.filter(pl.col("skip")).select(
        ["__dirpath", "total_files", "invalid_md5sig_files"]
    )

    eligible = per_folder.filter(~pl.col("skip")).select(
        ["__dirpath", "oldest_mod_epoch"]
    )

    # Signatures for eligible folders
    sigs = (
        df2.join(eligible.select(["__dirpath"]), on="__dirpath", how="inner")
        .group_by("__dirpath")
        .agg(pl.col("md5").sort().implode().list.join(" | ").alias("concat__md5sig"))
    )

    # Duplicate signature groups
    dupe_sigs = (
        sigs.group_by("concat__md5sig")
        .agg(pl.len().alias("dir_count"))
        .filter(pl.col("dir_count") > 1)
        .select(["concat__md5sig"])
    )

    dupe_dirs = (
        sigs.join(dupe_sigs, on="concat__md5sig", how="inner")
        .join(eligible, on="__dirpath", how="left")
    )

    # Choose keeper: oldest oldest_mod_epoch (min), tie-breaker: lexicographically smallest __dirpath
    keepers = (
        dupe_dirs.sort(["concat__md5sig", "oldest_mod_epoch", "__dirpath"])
        .group_by("concat__md5sig")
        .agg(
            pl.first("__dirpath").alias("keeper_dirpath"),
            pl.first("oldest_mod_epoch").alias("keeper_oldest_mod_epoch"),
        )
    )

    dupes_with_kill = (
        dupe_dirs.join(keepers, on="concat__md5sig", how="left")
        .with_columns(
            pl.when(pl.col("__dirpath") != pl.col("keeper_dirpath"))
            .then(pl.lit("1"))
            .otherwise(pl.lit(None))
            .alias("kill")
        )
        .select(["kill", "__dirpath", "concat__md5sig"])
        .sort(["concat__md5sig", "__dirpath"])
    )

    kill_list = dupes_with_kill.filter(pl.col("kill") == "1").select(
        ["__dirpath", "concat__md5sig"]
    )

    sigs_out = sigs.sort(["concat__md5sig", "__dirpath"])

    return {
        T_SIGNATURES: sigs_out,
        T_DUPES: dupes_with_kill,
        T_KILL: kill_list,
        T_SKIPPED: skipped.sort(["__dirpath"]),
    }


def _write_reports(conn: sqlite3.Connection, reports: dict[str, pl.DataFrame]) -> list[str]:
    """Write non-empty report tables; drop empty ones to prevent stale results."""

    # Inserts.
    sigs = reports[T_SIGNATURES]
    dupes = reports[T_DUPES]
    kill = reports[T_KILL]
    skipped = reports[T_SKIPPED]

    created: list[str] = []
    cur = conn.cursor()

    with tm_db.transaction(conn):
        # Always drop prior tables so empty results don't leave stale data.
        for t in (T_SIGNATURES, T_DUPES, T_KILL, T_SKIPPED):
            cur.execute(f"DROP TABLE IF EXISTS {tm_db.quote_ident(t)}")

        if sigs.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_SIGNATURES)} ("
                    "__dirpath TEXT, concat__md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_SIGNATURES,
                columns=["__dirpath", "concat__md5sig"],
                rows=sigs.iter_rows(),
            )
            created.append(T_SIGNATURES)

        if dupes.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_DUPES)} ("
                    "kill TEXT, __dirpath TEXT, concat__md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_DUPES,
                columns=["kill", "__dirpath", "concat__md5sig"],
                rows=dupes.iter_rows(),
            )
            created.append(T_DUPES)

        if kill.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_KILL)} ("
                    "__dirpath TEXT, concat__md5sig TEXT"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_KILL,
                columns=["__dirpath", "concat__md5sig"],
                rows=kill.iter_rows(),
            )
            created.append(T_KILL)

        if skipped.height:
            cur.execute(
                (
                    f"CREATE TABLE {tm_db.quote_ident(T_SKIPPED)} ("
                    "__dirpath TEXT, total_files INTEGER, invalid_md5sig_files INTEGER"
                    ")"
                )
            )
            _write_rows(
                cur,
                table=T_SKIPPED,
                columns=[
                    "__dirpath",
                    "total_files",
                    "invalid_md5sig_files",
                ],
                rows=skipped.iter_rows(),
            )
            created.append(T_SKIPPED)

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

        logging.info(
            f"Skipped folders (invalid __md5sig): {reports[T_SKIPPED].height}"
        )
        logging.info(
            f"Eligible folder signatures: {reports[T_SIGNATURES].height}"
        )
        logging.info(
            f"Duplicate folders (rows): {reports[T_DUPES].height}"
        )
        logging.info(
            f"Kill candidates (rows): {reports[T_KILL].height}"
        )

        created = _write_reports(conn, reports)
        if created:
            logging.info("Report tables written:")
            for t in created:
                logging.info(f"  - {t}")
        else:
            logging.info("No report tables written (no eligible FLAC/WavPack rows).")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
