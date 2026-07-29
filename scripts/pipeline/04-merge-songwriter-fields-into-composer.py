"""
Purpose:
    Merge `arranger`, `lyricist`, and `writer` into `composer`, preserving
    order and de-duplicating (case-insensitive).

    Only `composer` is modified; other songwriter fields remain unchanged.
    Logs changes to `changelog` and increments `__sqlmodded`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import polars as pl
import sqlite3
import logging
from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
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
    
    tm_db.ensure_changelog_table(conn)

    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()

    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_df.select("rowid").to_series().to_list())

    with tm_db.transaction(conn):
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
        
        # Increment __sqlmodded for all changed rows
        rowids = [(row["rowid"],) for row in changed_df.select("rowid").iter_rows(named=True)]
        cursor.executemany(
            "UPDATE alib SET __sqlmodded = COALESCE(__sqlmodded, 0) + 1 WHERE rowid = ?",
            rowids
        )

        # Write to changelog
        entries: list[tm_db.ChangelogEntry] = []
        for row in changed_df.iter_rows(named=True):
            alib_path = path_by_rowid.get(int(row["rowid"]), str(row["rowid"]))
            old_v = row["composer"] if row["composer"] else None
            new_v = row["new_composer"] if row["new_composer"] else None
            entries.extend(
                tm_changes.entries_from_changes(
                    alib_path=alib_path,
                    changes=[("composer", old_v, new_v)],
                    timestamp=timestamp,
                    script=script_name,
                )
            )

        tm_db.insert_changelog_entries(cursor, entries)

    return changed_df.height


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

    if not tm_db.table_exists(conn, "alib"):
        logging.error("Required table 'alib' not found in database")
        conn.close()
        return
    
    try:
        logging.info("Fetching composer-related columns from alib table...")
        
        query = """
            SELECT 
                rowid,
                COALESCE(__sqlmodded, 0) AS __sqlmodded,
                composer,
                arranger,
                lyricist,
                writer
            FROM alib
        """
        
        tracks_df = tm_polars_db.sqlite_to_polars(conn, query)
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
