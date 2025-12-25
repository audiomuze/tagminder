"""
Script Name: discnumber-cleanup-polars.py

Purpose:
    Clean up disc number data based on directory path analysis:
        1. Import "__dirpath", "discnumber" and "sqlmodded" from database
        2. Filter out rows where discnumber is None/empty for entire "__dirpath" groups
        3. Filter out paths matching "cdxx", "discxx", "cd xx", "disc xx" patterns
        4. For remaining paths, if all discnumber entries are identical, set to None
        5. Track and write only modified rows
        6. Log all changes to 'changelog' table

Optimized for speed using Polars vectorized expressions.

It is part of tagminder.

Usage:
    python discnumber-cleanup-polars.py
    uv run discnumber-cleanup-polars.py

Author: audiomuze
Created: 2025-06-01

"""

import sqlite3
import polars as pl
import logging
import re
from datetime import datetime, timezone

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["discnumber"]

# ---------- Fetch data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch data from database including __dirpath, discnumber and sqlmodded columns."""
    query = """
        SELECT rowid, __dirpath, discnumber, COALESCE(sqlmodded, 0) as sqlmodded
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
            # Ensure sqlmodded is never None - default to 0
            data[name] = pl.Series(name=name, values=[int(x) if x is not None else 0 for x in col_data], dtype=pl.Int64)
        elif name == "discnumber":
            # Keep discnumber as nullable string to preserve None values
            data[name] = pl.Series(name=name, values=[str(x) if x is not None and str(x).strip() != "" else None for x in col_data], dtype=pl.Utf8)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "" for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Apply disc number cleanup ----------
def apply_discnumber_cleanup(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean up disc numbers based on directory path analysis.

    Steps:
    1. Filter out paths where all discnumber entries are None/empty
    2. Filter out paths matching disc/cd pattern with numbers
    3. Set discnumber to None where all entries for a path are identical
    """
    logging.info("Starting disc number cleanup process...")

    # Step 1: Filter out paths where ALL discnumber entries are None/empty for that path
    logging.info("Step 1: Filtering paths with no disc number data...")

    # Count non-null discnumbers per path
    path_discnumber_counts = df.group_by("__dirpath").agg([
        pl.col("discnumber").is_not_null().sum().alias("non_null_count"),
        pl.len().alias("total_count")
    ])

    # Keep only paths that have at least one non-null discnumber
    paths_with_data = path_discnumber_counts.filter(pl.col("non_null_count") > 0)
    valid_paths = paths_with_data["__dirpath"].to_list()

    df_filtered = df.filter(pl.col("__dirpath").is_in(valid_paths))

    removed_empty_paths = len(df) - len(df_filtered)
    logging.info(f"Removed {removed_empty_paths} rows from paths with no disc number data")

    # Step 2: Filter out paths matching disc/cd patterns
    logging.info("Step 2: Filtering paths matching disc/cd patterns...")

    def matches_disc_pattern(path: str) -> bool:
        """Check if path matches cd/disc number patterns."""
        if not path:
            return False

        # Extract the last segment of the path (final directory name)
        last_segment = path.split("/")[-1].lower()

        # Patterns to match: cdxx, discxx, cd xx, disc xx (case insensitive)
        patterns = [
            r'\bcd\s*\d+\b',      # cd1, cd 1, cd2, etc.
            r'\bdisc\s*\d+\b',    # disc1, disc 1, disc2, etc.
        ]

        for pattern in patterns:
            if re.search(pattern, last_segment):
                return True
        return False

    # Apply pattern filtering
    valid_paths_after_pattern = []
    for path in valid_paths:
        if not matches_disc_pattern(path):
            valid_paths_after_pattern.append(path)

    df_pattern_filtered = df_filtered.filter(pl.col("__dirpath").is_in(valid_paths_after_pattern))

    removed_pattern_paths = len(df_filtered) - len(df_pattern_filtered)
    logging.info(f"Removed {removed_pattern_paths} rows from paths matching disc/cd patterns")

    # Step 3: Set discnumber to None where all entries for a path are identical
    logging.info("Step 3: Processing paths with identical disc numbers...")

    # For each path, check if all non-null discnumbers are the same
    path_analysis = df_pattern_filtered.group_by("__dirpath").agg([
        pl.col("discnumber").drop_nulls().n_unique().alias("unique_discnumbers"),
        pl.col("discnumber").drop_nulls().first().alias("sample_discnumber"),
        pl.col("discnumber").is_not_null().sum().alias("non_null_count")
    ])

    # Identify paths where all discnumbers are identical (unique count = 1)
    identical_disc_paths = path_analysis.filter(
        (pl.col("unique_discnumbers") == 1) & (pl.col("non_null_count") > 0)
    )["__dirpath"].to_list()

    logging.info(f"Found {len(identical_disc_paths)} paths with identical disc numbers that will be cleared")

    # Create new discnumber column
    df_updated = df_pattern_filtered.with_columns([
        pl.when(pl.col("__dirpath").is_in(identical_disc_paths))
        .then(None)
        .otherwise(pl.col("discnumber"))
        .alias("new_discnumber")
    ])

    # Detect changes
    discnumber_changed = (
        (pl.col("discnumber").is_null() != pl.col("new_discnumber").is_null()) |
        (pl.col("discnumber") != pl.col("new_discnumber"))
    )

    # Calculate sqlmodded delta
    sqlmodded_delta = discnumber_changed.cast(pl.Int64())

    # Update the dataframe with new values - ensure sqlmodded is never None
    df_updated = df_updated.with_columns([
        pl.col("new_discnumber").alias("discnumber"),
        (pl.col("sqlmodded").fill_null(0) + sqlmodded_delta).alias("sqlmodded")
    ])

    # Drop temporary column
    df_final = df_updated.drop(["new_discnumber"])

    return df_final

# ---------- Write updates ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    """Write updates to database and log changes."""
    # Find changed rows by comparing the original data with updated data
    # We need to match on rowid since some rows may have been filtered out
    original_dict = {row["rowid"]: row for row in original.to_dicts()}

    changed_records = []
    for record in updated.to_dicts():
        rowid = record["rowid"]
        if rowid in original_dict:
            original_record = original_dict[rowid]
            # Safe comparison with None handling
            original_sqlmodded = original_record.get("sqlmodded", 0) or 0
            updated_sqlmodded = record.get("sqlmodded", 0) or 0
            if updated_sqlmodded > original_sqlmodded:
                changed_records.append(record)

    if not changed_records:
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {len(changed_records)} changed rows to database")
    sample_ids = [r["rowid"] for r in changed_records[:5]]
    logging.info(f"Sample changed rowids: {sample_ids}")

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    # Ensure changelog table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            alib_column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    updates = 0
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "discnumber-cleanup-polars.py"

    for record in changed_records:
        rowid = record["rowid"]
        original_record = original_dict[rowid]

        changed_cols = []
        for col in COLUMNS:
            if col in record:
                old_val = original_record.get(col)
                new_val = record[col]
                # Handle None comparisons properly
                if (old_val is None) != (new_val is None) or (old_val != new_val and old_val is not None and new_val is not None):
                    changed_cols.append(col)
                    cursor.execute(
                        "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                        (rowid, col, str(old_val) if old_val is not None else None,
                         str(new_val) if new_val is not None else None, timestamp, script_name)
                    )

        if changed_cols:
            set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
            # Ensure sqlmodded is never None when writing to DB
            sqlmodded_value = record.get("sqlmodded", 0) or 0
            values = [record[col] for col in changed_cols] + [int(sqlmodded_value), rowid]
            cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)
            updates += 1

    conn.commit()
    logging.info(f"Updated {updates} rows and logged all changes.")
    return updates

# ---------- Analyze distinct dirpaths ----------
def analyze_distinct_dirpaths(original_df: pl.DataFrame, updated_df: pl.DataFrame) -> None:
    """Log analysis of distinct dirpath patterns and changes for debugging."""

    # Analysis of original data
    original_paths = original_df.select("__dirpath").n_unique()
    logging.info(f"Original dataset: {original_df.height} rows across {original_paths} distinct paths")

    # Analysis of updated data
    updated_paths = updated_df.select("__dirpath").n_unique()
    logging.info(f"After filtering: {updated_df.height} rows across {updated_paths} distinct paths")

    # Show sample paths that had disc numbers cleared
    cleared_paths = []
    original_dict = {row["rowid"]: row for row in original_df.to_dicts()}

    for record in updated_df.to_dicts():
        rowid = record["rowid"]
        if rowid in original_dict:
            original_record = original_dict[rowid]
            if (original_record.get("discnumber") is not None and
                record.get("discnumber") is None):
                cleared_paths.append((record["__dirpath"], original_record.get("discnumber")))

    if cleared_paths:
        unique_cleared = list(set(cleared_paths))[:10]  # Show up to 10 examples
        logging.info(f"Sample paths with disc numbers cleared:")
        for path, old_disc in unique_cleared:
            logging.info(f"  Path: {path} (was disc {old_disc})")

    # Count changes by type - use safe comparison
    changes = updated_df.filter(
        pl.col("sqlmodded").fill_null(0) > 0
    ).height
    logging.info(f"Total rows with changes: {changes}")

# ---------- Main ----------
def main():
    """Main execution function."""
    logging.info("Starting disc number cleanup process...")
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} rows")

        original_df = df.clone()
        updated_df = apply_discnumber_cleanup(df)

        # Analyze the results
        analyze_distinct_dirpaths(original_df, updated_df)

        # Count changes in the updated dataset with safe None handling
        original_dict = {row["rowid"]: row for row in original_df.to_dicts()}
        changed_rows = 0
        for record in updated_df.to_dicts():
            rowid = record["rowid"]
            if rowid in original_dict:
                original_sqlmodded = original_dict[rowid].get("sqlmodded", 0) or 0
                updated_sqlmodded = record.get("sqlmodded", 0) or 0
                if updated_sqlmodded > original_sqlmodded:
                    changed_rows += 1

        logging.info(f"Detected {changed_rows} rows with changes")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
        else:
            logging.info("No disc numbers needed updating.")

    except Exception as e:
        logging.error(f"Error during processing: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
