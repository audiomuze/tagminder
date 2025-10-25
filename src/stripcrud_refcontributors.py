"""
Script Name: cleantext-ref-polars.py

Purpose:
    This script processes entity and lentity columns in the '_*REF*mb_disambiguated' table,
    cleaning spurious characters:
    - CR/CRLF removal
    - Empty string to null conversion
    - Apostrophe normalization (standalone ' and ́ to ')

    It is part of tagminder, adapted from cleantext-polars.py

Usage:
    python cleantext-ref-polars.py
    uv run cleantext-ref-polars.py

Author: audiomuze
Created: 2025-06-28
"""

import sqlite3
import polars as pl
import logging
from typing import List


# ---------- Logging setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
TABLE_NAME = "_REF_mb_disambiguated"
TARGET_COLUMNS = ["entity", "lentity"]

# ---------- Helpers ----------

def verify_table_and_columns(conn: sqlite3.Connection, table: str, columns: List[str]) -> List[str]:
    """Verify table exists and return available target columns."""
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table,))

    if not cursor.fetchone():
        raise ValueError(f"Table '{table}' not found in database")

    # Get table schema and verify columns exist
    cursor.execute(f"PRAGMA table_info({table})")
    available_columns = {row[1] for row in cursor.fetchall()}

    valid_columns = [col for col in columns if col in available_columns]
    missing_columns = [col for col in columns if col not in available_columns]

    if missing_columns:
        logging.warning(f"Missing columns: {', '.join(missing_columns)}")

    if not valid_columns:
        raise ValueError(f"None of the target columns {columns} found in table '{table}'")

    logging.info(f"Found {len(valid_columns)} target columns: {', '.join(valid_columns)}")
    return valid_columns

def sqlite_to_polars(conn: sqlite3.Connection, table: str, columns: List[str]) -> pl.DataFrame:
    """Load data from SQLite into Polars DataFrame."""
    col_query = ", ".join(columns)
    query = f"SELECT rowid, {col_query} FROM {table}"

    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    data = {}

    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]

        if name == "rowid":
            data[name] = pl.Series(name=name, values=col_data, dtype=pl.Int64)
        else:
            # Text columns - coerce to string but preserve nulls
            coerced = [str(x) if x is not None else None for x in col_data]
            data[name] = pl.Series(name=name, values=coerced, dtype=pl.Utf8, strict=False)

    return pl.DataFrame(data)

def clean_text(val: str) -> str:
    """Clean individual text values according to business rules."""
    if val is None:
        return None

    # Remove CR/CRLF and strip whitespace
    cleaned = val.replace("\r\n", "").replace("\n", "").strip()

    # Normalize standalone apostrophes
    if cleaned in {"'", "́"}:
        cleaned = "'"

    # Convert empty strings to null
    return cleaned if cleaned else None

def apply_cleaning(df: pl.DataFrame, text_columns: List[str]) -> tuple[pl.DataFrame, int, int]:
    """Apply text cleaning to specified columns and return stats."""
    updated_df = df.clone()
    total_changes = 0
    changed_rows = set()

    for col in text_columns:
        original = df[col]
        cleaned = df[col].map_elements(clean_text, return_dtype=pl.Utf8)

        # Count changes in this column (handling nulls properly)
        changes_mask = (
            (original != cleaned) & 
            (original.is_not_null() | cleaned.is_not_null())
        )
        
        column_changes = changes_mask.sum()
        total_changes += column_changes
        
        # Track which rows changed
        if column_changes > 0:
            changed_row_indices = df.with_row_index().filter(changes_mask)["index"].to_list()
            changed_rows.update(changed_row_indices)

        updated_df = updated_df.with_columns(cleaned.alias(col))

    return updated_df, total_changes, len(changed_rows)

def write_updates(conn: sqlite3.Connection, updated: pl.DataFrame, columns: List[str]) -> int:
    """Write all cleaned data back to the database."""
    if updated.is_empty():
        logging.info("No data to write.")
        return 0

    logging.info(f"Writing {updated.height} rows to database")

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    updates = 0

    try:
        for record in updated.to_dicts():
            rowid = record["rowid"]

            # Update all target columns
            set_clause = ", ".join(f"{col} = ?" for col in columns)
            values = [record[col] for col in columns] + [rowid]
            cursor.execute(f"UPDATE `{TABLE_NAME}` SET {set_clause} WHERE rowid = ?", values)

            updates += 1

        conn.commit()
        logging.info(f"Successfully updated {updates} rows in the database.")

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
        # Verify table and columns exist
        valid_columns = verify_table_and_columns(conn, TABLE_NAME, TARGET_COLUMNS)

        logging.info(f"Fetching rows from '{TABLE_NAME}'...")
        df = sqlite_to_polars(conn, TABLE_NAME, valid_columns)
        logging.info(f"Loaded {df.height} rows with {len(df.columns)} columns")

        if df.height == 0:
            logging.info("No rows found in table. Exiting.")
            return

        logging.info("Cleaning text data in entity columns...")
        cleaned_df, total_field_changes, changed_row_count = apply_cleaning(df, valid_columns)

        if total_field_changes > 0:
            logging.info(f"Made {total_field_changes} field changes across {changed_row_count} rows")
            # Write all cleaned data back
            write_updates(conn, cleaned_df, valid_columns)
        else:
            logging.info("No changes detected. Database remains unchanged.")

    except Exception as e:
        logging.error(f"Script failed: {e}")
        raise

    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()