"""
Script Name: compilation-polars.py

Purpose:
    Detect and set compilation flags based on directory path patterns:
        - Check if "__dirpath" contains "/VA - " or "/OST - " after the 2nd last "/"
        - Set compilation = '1' where pattern matches, '0' otherwise
        - Track and write only modified rows
        - Log all changes to a 'changelog' table

Optimized for speed using Polars vectorized expressions.

It is part of tagminder.

Usage:
    python compilation-polars.py
    uv run compilation-polars.py

Author: audiomuze
Created: 2025-06-01

"""

import sqlite3
import polars as pl
import logging
from datetime import datetime, timezone

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["__dirpath", "compilation"]

# ---------- Fetch data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch data from database including __dirpath and compilation columns."""
    query = """
        SELECT rowid, __dirpath, compilation, COALESCE(sqlmodded, 0) as sqlmodded
        FROM alib
    """
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
        elif name == "compilation":
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "0" for x in col_data], dtype=pl.Utf8)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "" for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Apply compilation detection ----------
def apply_compilation_detection(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detect compilation albums based on directory path patterns.
    
    Logic: Extract the segment after the last "/" and check if it starts with "VA - " or "OST - "
    """
    # Split the dirpath by "/" and get the last segment (final directory/folder name)
    # This handles the pattern: /some/path/[VA - Album Name]
    df = df.with_columns([
        # Split by "/" and get the last element (index -1)
        # Handle cases where path might end with "/" or be empty
        pl.col("__dirpath")
        .str.split("/")
        .list.get(-1, null_on_oob=True)
        .alias("last_segment")
    ])
    
    # Check if the last segment starts with "VA - " or "OST - "
    compilation_pattern = pl.col("last_segment").str.starts_with("VA - ") | \
                         pl.col("last_segment").str.starts_with("Various Artists - ") | \
                         pl.col("last_segment").str.starts_with("OST - ")
    
    # Set new compilation value based on pattern match
    df = df.with_columns([
        pl.when(compilation_pattern)
        .then(pl.lit("1"))
        .otherwise(pl.lit("0"))
        .alias("new_compilation")
    ])
    
    # Detect changes
    compilation_changed = pl.col("compilation") != pl.col("new_compilation")
    
    # Calculate sqlmodded delta
    sqlmodded_delta = compilation_changed.cast(pl.Int32())
    
    # Update the dataframe with new values
    df = df.with_columns([
        pl.col("new_compilation").alias("compilation"),
        (pl.col("sqlmodded") + sqlmodded_delta).alias("sqlmodded")
    ])
    
    # Drop temporary columns
    return df.drop(["last_segment", "new_compilation"])

# ---------- Write updates ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    """Write updates to database and log changes."""
    changed = updated.filter(pl.col("sqlmodded") > original["sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
    
    # Ensure changelog table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    updates = 0
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "compilation-polars.py"

    for record in changed.to_dicts():
        rowid = record["rowid"]
        original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)

        changed_cols = []
        for col in COLUMNS:
            if col in record and record[col] != original_row[col]:
                changed_cols.append(col)
                cursor.execute(
                    "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, col, original_row[col], record[col], timestamp, script_name)
                )

        if changed_cols:
            set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
            values = [record[col] for col in changed_cols] + [int(record["sqlmodded"]), rowid]
            cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)
            updates += 1

    conn.commit()
    logging.info(f"Updated {updates} rows and logged all changes.")
    return updates

# ---------- Analyze distinct dirpaths ----------
def analyze_distinct_dirpaths(df: pl.DataFrame) -> None:
    """Log analysis of distinct dirpath patterns for debugging."""
    # Get distinct dirpaths and their compilation status
    distinct_paths = df.select([
        "__dirpath",
        "compilation"
    ]).unique().sort("__dirpath")
    
    logging.info(f"Found {distinct_paths.height} distinct directory paths")
    
    # Show sample compilation matches
    compilations = distinct_paths.filter(pl.col("compilation") == "1")
    if not compilations.is_empty():
        logging.info(f"Found {compilations.height} compilation paths")
        sample_compilations = compilations["__dirpath"].to_list()[:10]
        for path in sample_compilations:
            logging.info(f"  Compilation: {path}")
    
    # Show sample non-compilation paths for comparison
    non_compilations = distinct_paths.filter(pl.col("compilation") == "0")
    if not non_compilations.is_empty():
        sample_non_compilations = non_compilations["__dirpath"].to_list()[:5]
        logging.info("Sample non-compilation paths:")
        for path in sample_non_compilations:
            logging.info(f"  Regular: {path}")

# ---------- Main ----------
def main():
    """Main execution function."""
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} rows")

        original_df = df.clone()
        updated_df = apply_compilation_detection(df)

        # Analyze the results
        analyze_distinct_dirpaths(updated_df)

        changed_rows = updated_df.filter(pl.col("sqlmodded") > original_df["sqlmodded"]).height
        logging.info(f"Detected {changed_rows} rows with changes")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
        else:
            logging.info("No compilation flags needed updating.")
            
    except Exception as e:
        logging.error(f"Error during processing: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
