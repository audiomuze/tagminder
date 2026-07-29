"""
Purpose:
    Clean text fields in the `alib` SQLite table by:
    - removing CR/LF artifacts
    - converting empty strings to NULL
    - normalizing a small set of problematic apostrophe characters

    Only changed rows are written back. The script increments `__sqlmodded`
    for modified rows and logs per-field changes to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import sqlite3
import polars as pl
import logging
from typing import List

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_polars_db
from tagminder.core import tm_run

# ---------- Logging setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Config ----------
TABLE_NAME = "alib"
EXCLUDED_COLUMNS = {"discogs_artist_url", "lyrics", "review", "unsyncedlyrics"}

# ---------- Helpers ----------

def get_filtered_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """Get column names, excluding specified columns and system columns."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [
        row[1] for row in cursor.fetchall()
        if not row[1].startswith("__") and row[1] not in EXCLUDED_COLUMNS
    ]
    logging.info(f"Discovered {len(columns)} usable columns (excluded: {', '.join(EXCLUDED_COLUMNS)})")
    return columns

def sqlite_to_polars(conn: sqlite3.Connection, table: str, columns: List[str]) -> pl.DataFrame:
    """
    Load data from SQLite table into Polars DataFrame.
    Properly quotes column names to handle spaces and special characters.
    """
    # Quote column names with square brackets to handle spaces
    quoted_columns = [f'[{col}]' for col in columns]
    col_query = ", ".join(quoted_columns)
    query = f"SELECT rowid, {col_query}, COALESCE(__sqlmodded, 0) as __sqlmodded FROM [{table}]"

    return tm_polars_db.sqlite_to_polars(conn, query)

def clean_text(val: str) -> str:
    """
    Clean text by removing CRLF/LF, normalizing apostrophes, and converting empty strings to None.
    """
    if val is None:
        return None

    cleaned = val.replace("\r\n", "").replace("\n", "").strip()

    # Handle specific problematic apostrophe encodings
    if cleaned in {"â€™", "Ì"}:
        cleaned = "'"

    return cleaned if cleaned else None

def apply_cleaning(df: pl.DataFrame, text_columns: List[str]) -> pl.DataFrame:
    """
    Apply text cleaning to specified columns and track changes for __sqlmodded increment.
    Uses vectorized operations for optimal performance.
    """
    updated_df = df.clone()
    sqlmodded_increments = pl.lit(0)

    for col in text_columns:
        original = df[col]
        cleaned = df[col].map_elements(clean_text, return_dtype=pl.Utf8)

        # Track which rows changed for this column
        changed = (original != cleaned) & original.is_not_null()
        sqlmodded_increments += changed.cast(pl.Int32())

        # Update the column with cleaned values
        updated_df = updated_df.with_columns(cleaned.alias(col))

    # Update __sqlmodded counter
    updated_df = updated_df.with_columns(
        (pl.col("__sqlmodded").fill_null(0) + sqlmodded_increments)
        .cast(pl.Int16)
        .alias("__sqlmodded")
    )
    return updated_df

def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame, columns: List[str]) -> int:
    """
    Write only changed rows back to the database and log all changes.
    Uses proper SQL quoting for column names with spaces.
    """
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_rowids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_rowids}")

    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)

    changed_rowids = changed["rowid"].to_list()
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    updates = 0
    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for record in changed.to_dicts():
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            def _norm(v: object) -> str | None:
                if v is None:
                    return None
                return v if isinstance(v, str) else str(v)

            changes: list[tuple[str, object, object]] = []
            update_cols: list[str] = []
            for col in columns:
                old_v = original_row.get(col)
                new_v = record.get(col)
                if _norm(old_v) == _norm(new_v):
                    continue
                changes.append((col, old_v, new_v))
                update_cols.append(col)

            if changes:
                changelog.add(alib_path=alib_path, changes=changes)

                # Update the alib table with proper identifier quoting
                sql = tm_db.build_update_sql(table="alib", set_cols=update_cols)
                values = [record[col] for col in update_cols] + [int(record.get("__sqlmodded") or 0), rowid]
                cursor.execute(sql, values)

                updates += 1
                logging.debug(f"Updated rowid={rowid}, cols={update_cols}")

            changelog.flush(cursor)

    logging.info(f"Successfully updated {updates} rows in the database and logged changes.")
    return updates

# ---------- Main entry ----------

def main():
    """Main execution function."""
    try:
        conn, _, _, _ = tm_run.open_db(ensure_changelog=True, require_exists=True)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    if not tm_db.table_exists(conn, TABLE_NAME):
        logging.error(f"Required table '{TABLE_NAME}' not found in database")
        conn.close()
        return

    try:
        target_cols = get_filtered_columns(conn, TABLE_NAME)

        logging.info(f"Fetching rows from '{TABLE_NAME}'...")
        df = sqlite_to_polars(conn, TABLE_NAME, target_cols)
        logging.info(f"Loaded {df.height} rows with {len(df.columns)} columns")

        original_df = df.clone()
        logging.info("Cleaning text data across columns...")
        cleaned_df = apply_cleaning(df, target_cols)

        num_changed = cleaned_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info(f"Detected {num_changed} rows with changes")

        if num_changed > 0:
            write_updates(conn, original_df, cleaned_df, target_cols)
        else:
            logging.info("No changes detected - database update skipped.")

    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
