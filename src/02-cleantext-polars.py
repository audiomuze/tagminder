"""
Script Name: cleantext-polars.py

Purpose:
    This script processes all records in alib and strips tag entries of spurious characters:
    CR
    CRLF
    '' (empty strings)
    wrong_apostrophes: r'’', r' ́', replace with "\'"

    It is part of tagminder.

Usage:
    python cleantext-polars.py
    uv run cleantext-polars.py

Author: audiomuze
Created: 2025-04-13


Script Name: cleantext-polars.py

Purpose:
    Load all text columns from the 'alib' table, clean each field by:
        - Removing CRLF and LF
        - Converting empty strings to null
        - Normalizing apostrophes ONLY if they appear standalone
    Increment `sqlmodded` per column change
    Write only changed rows back to the SQLite DB
    Log all changed fields into a new 'changes' table for review

Author: audiomuze
"""

import sqlite3
import polars as pl
import logging
from typing import List, Dict
from datetime import datetime, timezone


# ---------- Logging setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
TABLE_NAME = "alib"
EXCLUDED_COLUMNS = {"discogs_artist_url", "lyrics", "review", "sqlmodded", "unsyncedlyrics"}

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
    query = f"SELECT rowid, {col_query}, COALESCE(sqlmodded, 0) as sqlmodded FROM [{table}]"

    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    data = {}

    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]
        if name == "rowid":
            data[name] = pl.Series(name=name, values=col_data, dtype=pl.Int64)
        elif name == "sqlmodded":
            data[name] = pl.Series(name=name, values=[int(x) if x is not None else 0 for x in col_data], dtype=pl.Int64)
        else:
            coerced = [str(x) if x is not None else None for x in col_data]
            data[name] = pl.Series(name=name, values=coerced, dtype=pl.Utf8, strict=False)

    return pl.DataFrame(data)

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
    Apply text cleaning to specified columns and track changes for sqlmodded increment.
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

    # Update sqlmodded counter
    updated_df = updated_df.with_columns(
        (pl.col("sqlmodded").fill_null(0) + sqlmodded_increments).alias("sqlmodded")
    )
    return updated_df

def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame, columns: List[str]) -> int:
    """
    Write only changed rows back to the database and log all changes.
    Uses proper SQL quoting for column names with spaces.
    """
    changed = updated.filter(pl.col("sqlmodded") > original["sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_rowids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_rowids}")

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    # Create changelog table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    updates = 0
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "cleantext-polars.py"

    try:
        for record in changed.to_dicts():
            rowid = record["rowid"]
            original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)

            # Find columns that actually changed
            update_cols = [
                col for col in columns
                if record.get(col) != original_row.get(col)
            ]

            if update_cols:
                # 1. Log changes to changelog table
                for col in update_cols:
                    cursor.execute(
                        "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                        (rowid, col, original_row.get(col), record.get(col), timestamp, script_name)
                    )

                # 2. Update the alib table with proper column quoting
                quoted_cols = [f'[{col}]' for col in update_cols]
                set_clause = ", ".join(f"{quoted_col} = ?" for quoted_col in quoted_cols) + ", sqlmodded = ?"
                values = [record[col] for col in update_cols] + [record["sqlmodded"], rowid]

                cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

                updates += 1
                logging.debug(f"Updated rowid={rowid}, cols={update_cols}")

        conn.commit()
        logging.info(f"Successfully updated {updates} rows in the database and logged changes.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Write failed: {e}")
        raise

    return updates

# ---------- Main entry ----------

def main():
    """Main execution function."""
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        target_cols = get_filtered_columns(conn, TABLE_NAME)

        logging.info(f"Fetching rows from '{TABLE_NAME}'...")
        df = sqlite_to_polars(conn, TABLE_NAME, target_cols)
        logging.info(f"Loaded {df.height} rows with {len(df.columns)} columns")

        original_df = df.clone()
        logging.info("Cleaning text data across columns...")
        cleaned_df = apply_cleaning(df, target_cols)

        num_changed = cleaned_df.filter(pl.col("sqlmodded") > original_df["sqlmodded"]).height
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
