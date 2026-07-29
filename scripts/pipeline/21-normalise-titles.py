#!/usr/bin/env python3
"""
Purpose:
    Normalize title-like capitalization in `alib` for title-bearing fields.

    This script applies conservative English title-case rules to title-bearing
    columns (currently `title`, `album`, `discsubtitle`, and `version` when present), using a reusable helper
    module so the same policy can be reused elsewhere later.

    The script writes only changed rows back to `alib`, increments
    `__sqlmodded`, and logs per-field changes to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-06-13
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_titlecase

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
_DEFAULT_COLUMNS = ["title", "album", "discsubtitle", "version"]


def _configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, force=True)


def _get_existing_columns(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.execute("PRAGMA table_info(alib)")
    return {str(row[1]) for row in cursor.fetchall()}


def _resolve_columns(conn: sqlite3.Connection, requested: list[str]) -> list[str]:
    existing = _get_existing_columns(conn)
    columns = [column for column in requested if column in existing]
    missing = [column for column in requested if column not in existing]

    if missing:
        logging.warning("Skipping missing column(s): %s", ", ".join(missing))
    if not columns:
        raise ValueError("No requested title columns exist in alib.")

    return columns


def _fetch_data(conn: sqlite3.Connection, columns: list[str]) -> pl.DataFrame:
    select_columns = ", ".join(tm_db.quote_ident(column) for column in columns)
    where_clause = " OR ".join(
        f"{tm_db.quote_ident(column)} IS NOT NULL AND TRIM({tm_db.quote_ident(column)}) != ''"
        for column in columns
    )

    query = f"""
        SELECT rowid, COALESCE(__sqlmodded, 0) AS __sqlmodded, {select_columns}
        FROM alib
        WHERE {where_clause}
    """

    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data: dict[str, pl.Series] = {}
    for i, name in enumerate(col_names):
        values = [row[i] for row in rows]
        if name == "rowid":
            data[name] = pl.Series(name=name, values=[int(x or 0) for x in values], dtype=pl.Int64)
        elif name == "__sqlmodded":
            data[name] = pl.Series(name=name, values=[int(x or 0) for x in values], dtype=pl.Int16)
        else:
            data[name] = pl.Series(
                name=name,
                values=[str(x) if x is not None else None for x in values],
                dtype=pl.Utf8,
            )

    return pl.DataFrame(data)


def _normalize_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    updated_df = df.clone()
    sqlmodded_increments = pl.lit(0)

    for column in columns:
        original = df[column].cast(pl.Utf8)
        normalized = df[column].map_elements(
            tm_titlecase.normalize_title_case,
            return_dtype=pl.Utf8,
        )

        changed = original.fill_null("") != normalized.fill_null("")
        sqlmodded_increments += changed.cast(pl.Int16)
        updated_df = updated_df.with_columns(normalized.alias(column))

    return updated_df.with_columns(
        (pl.col("__sqlmodded") + sqlmodded_increments).cast(pl.Int16).alias("__sqlmodded")
    )


def _write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No title changes to write.")
        return 0

    logging.info("Writing %s changed rows to database", changed.height)
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info("Sample changed rowids: %s", sample_ids)

    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()
    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)

    changed_rowids = changed["rowid"].to_list()
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    updates = 0
    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)
        for record in changed.to_dicts():
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            changes: list[tuple[str, object, object]] = []
            changed_cols: list[str] = []
            for col in [key for key in record.keys() if key not in {"rowid", "__sqlmodded"}]:
                old_v = original_row.get(col)
                new_v = record.get(col)
                if (old_v if old_v is not None else None) == (new_v if new_v is not None else None):
                    continue
                changes.append((col, old_v, new_v))
                changed_cols.append(col)

            if changes:
                changelog.add(alib_path=alib_path, changes=changes)

                sql = tm_db.build_update_sql(table="alib", set_cols=changed_cols)
                values = [record[col] for col in changed_cols] + [int(record["__sqlmodded"] or 0), rowid]
                cursor.execute(sql, values)
                updates += 1

            changelog.flush(cursor)

    logging.info("Updated %s rows and logged all changes.", updates)
    return updates


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="21-normalise-titles.py",
        description="Normalize capitalization for title-like fields in alib.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite database path (defaults to tagminder.toml [db].path).",
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=list(_DEFAULT_COLUMNS),
        help="Columns to normalize (default: title album discsubtitle version).",
    )
    return parser.parse_args()


def main() -> None:
    _configure_logging()
    args = _parse_args()

    db_path = tm_config.get_db_path(argv=sys.argv[1:], default=args.db)
    logging.info("Connecting to database: %s", db_path)

    if not Path(db_path).exists():
        logging.error(f"Database file does not exist: {db_path}")
        return

    conn = tm_db.connect(db_path)

    if not tm_db.table_exists(conn, "alib"):
        logging.error("Required table 'alib' not found in database")
        conn.close()
        return

    try:
        columns = _resolve_columns(conn, args.columns)
        logging.info("Normalizing columns: %s", ", ".join(columns))

        df = _fetch_data(conn, columns)
        logging.info("Loaded %s rows for processing", df.height)

        original_df = df.clone()
        updated_df = _normalize_columns(df, columns)

        changed_rows = updated_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info("Detected %s modified rows", changed_rows)

        if changed_rows > 0:
            _write_updates(conn, original_df, updated_df)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
