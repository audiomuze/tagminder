#!/usr/bin/env python3
"""
Script Name: va_track_randomiser.py

Purpose:
    Handles VA albums without track numbers by generating sequential track numbers
    in the range 1...n where n is the number of tracks in the directory.
    Uses Polars for vectorized operations.

Author: audiomuze
Created: 2025
"""

import sqlite3
import logging
from datetime import datetime, timezone
import argparse
import random
import polars as pl

# ---------- Configuration ----------
DB_PATH = '/tmp/amg/dbtemplate.db'
SCRIPT_NAME = "va_track_randomiser.py"

# ---------- Command Line Arguments ----------
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Fix missing track numbers for VA albums")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the changes (default is dry-run mode)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    return parser.parse_args()

# ---------- Logging Setup ----------
def setup_logging(verbose=False):
    """Configure logging based on command line arguments."""
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('track_randomiser.log')]
    )

def sqlite_to_polars(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """
    Convert SQLite query results to a Polars DataFrame with proper type handling.

    Args:
        conn: SQLite database connection
        query: SQL query to execute

    Returns:
        Polars DataFrame with appropriate data types
    """
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        # Handle different data types
        if col_name in ['rowid']:
            # Integer columns
            data[col_name] = pl.Series(
                name=col_name,
                values=[int(x) if x is not None else None for x in col_data],
                dtype=pl.Int64
            )
        else:
            # String columns with null handling
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )

    return pl.DataFrame(data)

def get_va_albums_without_tracknumbers(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Find VA albums (compilation='1') that have missing track numbers using sqlite_to_polars.

    Args:
        conn: SQLite database connection

    Returns:
        Polars DataFrame with rowid, __dirpath, and current track values
    """
    query = """
        SELECT rowid, __dirpath, track
        FROM alib
        WHERE compilation = '1'
          AND (track IS NULL OR track = '' OR track = '0')
          AND __dirpath IS NOT NULL
    """

    return sqlite_to_polars(conn, query)

def update_track_numbers_vectorized(conn: sqlite3.Connection, df: pl.DataFrame, dry_run: bool = True) -> int:
    """
    Update database with new track numbers using vectorized operations.

    Args:
        conn: SQLite database connection
        df: DataFrame with files needing track numbers
        dry_run: If True, only log what would be updated without making changes

    Returns:
        Number of updates performed
    """
    if df.is_empty():
        return 0

    # Group by directory and generate sequential track numbers
    updates_df = (
        df
        .group_by("__dirpath")
        .agg(pl.col("rowid").alias("rowids"))
        .with_columns(
            track_count=pl.col("rowids").list.len()
        )
        .with_columns(
            # Generate shuffled track numbers for each directory
            track_numbers=pl.col("track_count").map_elements(
                lambda n: random.sample(range(1, n + 1), n) if n > 0 else [],
                return_dtype=pl.List(pl.Int64)
            )
        )
        .explode(["rowids", "track_numbers"])  # This should create individual rows
        .with_columns(
            track_numbers=pl.col("track_numbers").cast(pl.Utf8)
        )
        .select(["rowids", "track_numbers", "__dirpath"])
    )

    # Convert to dictionary for efficient processing
    updates_dict = updates_df.to_dict(as_series=False)
    rowids = updates_dict["rowids"]
    new_tracks = updates_dict["track_numbers"]  # This should now be individual values
    dirpaths = updates_dict["__dirpath"]

    cursor = conn.cursor()

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

    timestamp = datetime.now(timezone.utc).isoformat()
    total_updates = 0

    # Get current track values for changelog in bulk
    if not dry_run and rowids:
        placeholders = ",".join("?" for _ in rowids)
        cursor.execute(f"SELECT rowid, track FROM alib WHERE rowid IN ({placeholders})", rowids)
        current_tracks = {rowid: track or "NULL" for rowid, track in cursor.fetchall()}

    # Process updates
    processed_dirs = set()
    for rowid, new_track, dirpath in zip(rowids, new_tracks, dirpaths):
        # Log directory information once per directory
        if dirpath not in processed_dirs:
            # Get all track numbers for this directory
            dir_track_numbers = [t for r, t, d in zip(rowids, new_tracks, dirpaths) if d == dirpath]
            logging.info(f"Directory: {dirpath}")
            logging.info(f"  Files without track numbers: {len(dir_track_numbers)}")
            # Ensure we're working with individual values, not lists
            try:
                track_nums = [int(t) if not isinstance(t, list) else int(t[0]) for t in dir_track_numbers]
                logging.info(f"  Assigned track numbers: {sorted(track_nums)}")
            except (TypeError, ValueError) as e:
                logging.warning(f"  Could not parse track numbers: {dir_track_numbers} - {e}")
            processed_dirs.add(dirpath)

        if dry_run:
            logging.info(f"DRY RUN: Would update row {rowid}: track -> {new_track}")
        else:
            try:
                # Ensure new_track is a string, not a list
                if isinstance(new_track, list):
                    new_track = str(new_track[0]) if new_track else "1"

                # Get current track value for changelog
                old_track = current_tracks.get(rowid, "NULL")

                # Update the main table
                cursor.execute(
                    "UPDATE alib SET track = ? WHERE rowid = ?",
                    (new_track, rowid)
                )

                # Log to changelog
                cursor.execute(
                    "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, "track", old_track, new_track, timestamp, SCRIPT_NAME)
                )

                logging.info(f"Updated row {rowid}: track {old_track} -> {new_track}")
                total_updates += 1

            except Exception as e:
                logging.error(f"Error updating row {rowid}: {str(e)}")
                conn.rollback()
                raise

    if not dry_run:
        conn.commit()

    return total_updates

def main():
    """
    Main execution function that finds VA albums without track numbers
    and assigns sequential random track numbers using Polars.
    """
    args = parse_arguments()
    setup_logging(args.verbose)

    # Dry-run is the default mode unless --execute is specified
    dry_run = not args.execute

    logging.info("Starting track number randomization process")
    if dry_run:
        logging.info("DRY RUN MODE: No changes will be made to database")
    else:
        logging.info("EXECUTE MODE: Changes will be made to database")

    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Connected to database: {DB_PATH}")

        # Find VA albums without track numbers using sqlite_to_polars
        logging.info("Searching for VA albums without track numbers...")
        df = get_va_albums_without_tracknumbers(conn)

        if df.is_empty():
            logging.info("No VA albums found without track numbers")
            return

        logging.info(f"Found {df.height} files without track numbers in {df['__dirpath'].n_unique()} directories")

        # Show sample of what we found
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            sample_dirs = df['__dirpath'].unique().slice(0, 5).to_list()
            logging.debug(f"Sample directories: {sample_dirs}")

        # Update track numbers using vectorized operations
        total_updates = update_track_numbers_vectorized(conn, df, dry_run)

        if dry_run:
            logging.info(f"DRY RUN: Would update {total_updates} track numbers")
            logging.info("Use --execute flag to actually perform the changes.")
        else:
            logging.info(f"Successfully updated {total_updates} track numbers")

    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()
