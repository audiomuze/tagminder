"""
Script Name: update_contributors.py

Purpose:
    Reads data from two SQLite tables (allmusic_reference_data and musicbrainz_source)
    using the exact same methods as pullmbids.py (no type inference, all columns as strings),
    performs case-insensitive comparison between allmusic_artist and contributor fields,
    and updates contributor values when they don't match exactly (but match case-insensitively).

    The script performs the following operations:
    1. Reads both SQLite tables into Polars DataFrames with no type inference (all as strings)
    2. Filters allmusic_reference_data to only rows where name_similarity = 1
    3. Performs case-insensitive comparison between allmusic_artist and contributor
    4. Updates contributor in musicbrainz_source when they match case-insensitively but not exactly
    5. Sets updated_from_allmusic to 1 for updated rows
    6. Tracks and reports on the number of changes made

Author: audiomuze
Created: 2025-09-17
Modified: 2025-09-17
"""

import polars as pl
import sqlite3
import logging
import os
import sys
import re

def read_sqlite_table_as_strings(db_name: str, table_name: str) -> pl.DataFrame:
    """
    Read a SQLite table into a Polars DataFrame with NO TYPE INFERENCE - all columns as strings.
    This matches the approach used in pullmbids.py by manually reading all data as strings.
    
    Args:
        db_name: SQLite database file name/path
        table_name: Name of the table to read
        
    Returns:
        Polars DataFrame with all columns as strings
    """
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        # Get all data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Convert all values to strings (no type inference)
        string_rows = []
        for row in rows:
            string_row = [str(value) if value is not None else None for value in row]
            string_rows.append(string_row)
        
        # Create DataFrame with all columns as strings
        df = pl.DataFrame(string_rows, schema=columns, orient="row")
        
        # Ensure all columns are treated as strings
        df = df.cast({col: pl.String for col in df.columns})
        
        conn.close()
        return df
        
    except Exception as e:
        logging.error(f"Error reading table {table_name}: {e}")
        print(f"Error reading table {table_name}: {e}")
        return None

def update_musicbrainz_contributors(db_name: str = "dbtemplate.db"):
    """
    Main function to update contributor values in musicbrainz_source table.
    
    Args:
        db_name: SQLite database file name/path
        
    Returns:
        Number of changes made
    """
    try:
        # Read both tables with NO TYPE INFERENCE - all columns as strings
        allmusic_df = read_sqlite_table_as_strings(db_name, "allmusic_reference_data")
        musicbrainz_df = read_sqlite_table_as_strings(db_name, "musicbrainz_source")
        
        if allmusic_df is None or musicbrainz_df is None:
            print("Error reading one or both tables from database")
            return 0
        
        print(f"Loaded {len(allmusic_df)} rows from allmusic_reference_data")
        print(f"Loaded {len(musicbrainz_df)} rows from musicbrainz_source")
        
        # Filter allmusic_reference_data to only rows where name_similarity = "1" (as string)
        similarity_filtered = allmusic_df.filter(
            pl.col("name_similarity").str.strip_chars() == "1"
        )
        
        print(f"Found {len(similarity_filtered)} rows with name_similarity = 1")
        
        if len(similarity_filtered) == 0:
            print("No rows with name_similarity = 1 found. Exiting.")
            return 0
        
        # Create a mapping of lowercase allmusic_artist to original allmusic_artist
        # for case-insensitive matching but exact replacement
        artist_mapping = {}
        for row in similarity_filtered.iter_rows(named=True):
            if row["allmusic_artist"] and row["allmusic_artist"].strip():  # Only add if not null/empty
                lowercase_key = row["allmusic_artist"].lower().strip()
                artist_mapping[lowercase_key] = row["allmusic_artist"]
        
        print(f"Created mapping for {len(artist_mapping)} unique artists")
        
        # Vectorized operation to check and update contributors
        changes_count = 0
        changes_to_make = []
        
        # Iterate through musicbrainz rows and find matches
        for row in musicbrainz_df.iter_rows(named=True):
            if row["contributor"] and row["contributor"].strip():  # Only process if contributor exists
                lowercase_contributor = row["contributor"].lower().strip()
                
                # Check if this lowercase contributor exists in our mapping
                if lowercase_contributor in artist_mapping:
                    mapped_artist = artist_mapping[lowercase_contributor]
                    
                    # If the contributor doesn't exactly match the allmusic_artist
                    if row["contributor"] != mapped_artist:
                        changes_to_make.append({
                            "csv_row_number": row["csv_row_number"],
                            "old_contributor": row["contributor"],
                            "new_contributor": mapped_artist
                        })
                        changes_count += 1
        
        print(f"Found {changes_count} contributors to update")
        
        # Update the database with the changes
        if changes_count > 0:
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            
            # Prepare update statement - update both contributor and updated_from_allmusic
            update_sql = """
            UPDATE musicbrainz_source 
            SET contributor = ?, updated_from_allmusic = '1'
            WHERE csv_row_number = ?
            """
            
            # Execute updates
            for change in changes_to_make:
                cursor.execute(update_sql, (change["new_contributor"], change["csv_row_number"]))
            
            conn.commit()
            conn.close()
            
            # Print some examples of changes
            print("\nSample changes:")
            for i, change in enumerate(changes_to_make[:5]):  # Show first 5 changes
                print(f"  Row {change['csv_row_number']}: '{change['old_contributor']}' -> '{change['new_contributor']}'")
            if changes_count > 5:
                print(f"  ... and {changes_count - 5} more changes")
            
            print(f"Updated {changes_count} rows in the database (set updated_from_allmusic = 1)")
        
        return changes_count
        
    except Exception as e:
        logging.error(f"Error updating contributors: {e}")
        print(f"Error updating contributors: {e}")
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    db_name = "dbtemplate.db"
    
    print("Starting contributor update process...")
    print("Reading tables with NO TYPE INFERENCE (all columns as strings)...")
    print("Comparing allmusic_artist (where name_similarity=1) with contributor...")
    print("Updating contributors to match allmusic_artist exactly when they match case-insensitively...")
    print("Setting updated_from_allmusic = 1 for updated rows...")
    
    changes_made = update_musicbrainz_contributors(db_name)
    
    print(f"\nProcessing completed. Made {changes_made} contributor updates.")
    print("Database has been updated with the corrected contributor values.")