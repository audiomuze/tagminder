"""
Script Name: merge-composers-polars.py

Purpose:
    This script merges composer, arranger, lyricist, and writer columns into the
    composer column, preserving order and deduplicating (case-insensitive).
    Only composer is modified; other columns remain unchanged.
    Logs changes to changelog table and increments sqlmodded.

Usage:
    python merge-composers-polars.py
    uv run merge-composers-polars.py

Author: audiomuze
Created: 2025-10-11
"""

import polars as pl
import sqlite3
import logging
import sys
import os
from typing import List, Tuple
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def get_script_name(with_extension=True):
    """
    Returns the name of the currently executing script.
    
    Args:
        with_extension (bool): If True, returns the script name with its file extension.
                               If False, returns the name without the extension.
    """
    script_path = sys.argv[0]
    script_name = os.path.basename(script_path)
    if not with_extension:
        script_name = os.path.splitext(script_name)[0]
    return script_name


def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    """
    Execute a SQLite query and return results as a Polars DataFrame.
    
    Args:
        conn: SQLite database connection
        query: SQL query to execute
        id_column: Name of column to cast to Int64 (typically 'rowid')
    
    Returns:
        Polars DataFrame with query results
    """
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if id_column and col_name == id_column:
            data[col_name] = pl.Series(col_data, dtype=pl.Int64)
        elif col_name == "sqlmodded":
            # Convert string numbers like "3" to integers
            cleaned = [int(x) if isinstance(x, str) and x.isdigit() else x for x in col_data]
            data[col_name] = pl.Series(cleaned, dtype=pl.Int64)
        else:
            data[col_name] = [x if x is not None else None for x in col_data]

    return pl.DataFrame(data)


def merge_and_dedupe_values(composer: str, arranger: str, lyricist: str, writer: str, delimiter: str = "\\\\") -> str:
    """
    Merge and deduplicate values from four fields, preserving order.
    Case-insensitive deduplication.
    
    Args:
        composer, arranger, lyricist, writer: Field values to merge
        delimiter: Delimiter used in fields and for output
    
    Returns:
        Merged and deduplicated string
    """
    all_values = []
    seen_lower = set()
    
    # Process each field in order
    for field_value in [composer, arranger, lyricist, writer]:
        if field_value is None or str(field_value).strip() == "":
            continue
        
        # Split by delimiter and process each component
        components = [v.strip() for v in str(field_value).split(delimiter) if v.strip()]
        
        for component in components:
            component_lower = component.lower()
            if component_lower not in seen_lower:
                seen_lower.add(component_lower)
                all_values.append(component)
    
    return delimiter.join(all_values) if all_values else None


def process_composer_merge(df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge composer, arranger, lyricist, and writer into composer column using vectorization.
    Preserves order, deduplicates case-insensitively.
    
    Args:
        df: Input DataFrame with composer, arranger, lyricist, writer columns
    
    Returns:
        DataFrame with new_composer column added
    """
    # Use map_elements (formerly apply) to vectorize the merge operation
    df_with_merged = df.with_columns(
        pl.struct(["composer", "arranger", "lyricist", "writer"])
        .map_elements(
            lambda row: merge_and_dedupe_values(
                row["composer"],
                row["arranger"],
                row["lyricist"],
                row["writer"]
            ),
            return_dtype=pl.String
        )
        .alias("new_composer")
    )
    
    return df_with_merged


def identify_changes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identify rows where composer has changed (case-insensitive comparison).
    
    Args:
        df: DataFrame with composer and new_composer columns
    
    Returns:
        DataFrame filtered to only changed rows
    """
    # Fill nulls with empty string for comparison
    df_comparison = df.with_columns([
        pl.col("composer").fill_null("").str.strip_chars().str.to_lowercase().alias("orig_lower"),
        pl.col("new_composer").fill_null("").str.strip_chars().str.to_lowercase().alias("new_lower")
    ])
    
    # Filter to rows where values differ
    changed_df = df_comparison.filter(
        pl.col("orig_lower") != pl.col("new_lower")
    ).select(
        pl.exclude(["orig_lower", "new_lower"])
    )
    
    return changed_df


def write_updates_to_db(
    conn: sqlite3.Connection,
    changed_df: pl.DataFrame
) -> int:
    """
    Write composer updates and changelog entries to database.
    
    Args:
        conn: SQLite database connection
        changed_df: DataFrame containing only rows with changes
    
    Returns:
        Number of rows updated
    """
    cursor = conn.cursor()
    
    if changed_df.height == 0:
        return 0
    
    # Ensure changelog table exists
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
    
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = get_script_name()
    
    conn.execute("BEGIN TRANSACTION")
    try:
        # Prepare update data as list of tuples
        update_data = [
            (row["new_composer"], row["rowid"])
            for row in changed_df.iter_rows(named=True)
        ]
        
        # Update composer column for changed rows
        cursor.executemany(
            'UPDATE alib SET composer = ? WHERE rowid = ?',
            update_data
        )
        
        # Increment sqlmodded for all changed rows
        rowids = [(row["rowid"],) for row in changed_df.select("rowid").iter_rows(named=True)]
        cursor.executemany(
            "UPDATE alib SET sqlmodded = COALESCE(sqlmodded, 0) + 1 WHERE rowid = ?",
            rowids
        )
        
        # Write to changelog
        changelog_entries = [
            (
                row["rowid"],
                "composer",
                row["composer"] if row["composer"] else None,
                row["new_composer"] if row["new_composer"] else None,
                timestamp,
                script_name
            )
            for row in changed_df.iter_rows(named=True)
        ]
        
        cursor.executemany(
            "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            changelog_entries
        )
        
        conn.commit()
        return changed_df.height
    
    except Exception as e:
        conn.rollback()
        logging.error(f"Error writing updates to database: {str(e)}")
        raise


def main():
    """Main execution function."""
    db_path = '/tmp/amg/dbtemplate.db'
    conn = sqlite3.connect(db_path)
    
    try:
        logging.info("Fetching composer-related columns from alib table...")
        
        query = """
            SELECT 
                rowid,
                COALESCE(sqlmodded, 0) AS sqlmodded,
                composer,
                arranger,
                lyricist,
                writer
            FROM alib
        """
        
        tracks_df = sqlite_to_polars(conn, query, id_column="rowid")
        logging.info(f"Loaded DataFrame with {tracks_df.height} rows")
        
        logging.info("Processing composer merges...")
        merged_df = process_composer_merge(tracks_df)
        
        logging.info("Identifying changes...")
        changed_df = identify_changes(merged_df)
        
        if changed_df.height > 0:
            logging.info(f"Detected {changed_df.height} rows with composer changes")
            logging.info("Writing updates to database...")
            
            updated_count = write_updates_to_db(conn, changed_df)
            logging.info(f"Successfully updated {updated_count} rows in the database")
        else:
            logging.info("No changes detected, database not updated.")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
