"""
Script Name: contributors-polars.py

Purpose:
    This script processes all records in alib containing one or more contributor entries.  
    For each track it checks if any contributor names match entries in the reference dictionary (case-insensitive)
    and replaces them with the standardized versions from _REF_vetted_contributors if they differ.

    It is the de-facto way of ensuring artist name consistency throughout your music collection.
    To update MBIDs in alib you should run mbids-pandas.py afterward.
    It is part of tagminder.

Usage:
    python contributors-polars.py
    uv run contributors-polars.py

Author: audiomuze
Created: 2025-04-18
"""



import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    """
    Execute a query and convert the results to a Polars DataFrame.
    
    Args:
        conn: SQLite database connection
        query: SQL query to execute
        id_column: Name of ID column to preserve type (all others are converted to pl.Utf8)
        
    Returns:
        Polars DataFrame with results
    """
    # Execute the query and get the results
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Get column names
    column_names = [description[0] for description in cursor.description]
    
    # Fetch all rows
    rows = cursor.fetchall()
    
    # Convert data and handle types
    data = {}
    for i, col_name in enumerate(column_names):
        # Extract column data
        col_data = [row[i] for row in rows]
        
        # Apply type conversion for string columns
        if id_column and col_name != id_column:
            data[col_name] = pl.Series(col_name, col_data, dtype=pl.Utf8)
        else:
            data[col_name] = col_data
    
    # Create a Polars DataFrame
    return pl.DataFrame(data)

def write_updates_to_db(conn: sqlite3.Connection, updated_df: pl.DataFrame, 
                        changed_rowids: List[int], columns_to_update: List[str]) -> int:
    """
    Write updates back to the database
    
    Args:
        conn: SQLite database connection
        updated_df: DataFrame with updated values
        changed_rowids: List of rowids that were changed
        columns_to_update: List of column names to update
        
    Returns:
        Number of rows updated
    """
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0
    
    cursor = conn.cursor()
    
    # Start transaction
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Filter dataframe to only changed rows and required columns
        update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids)).select(
            ["rowid"] + columns_to_update
        )
        
        # Convert to records for SQL
        records = update_df.to_dicts()
        
        # Update each record
        updated_count = 0
        for record in records:
            rowid = record["rowid"]
            
            # Build SET clause and parameters 
            set_params = []
            set_values = []
            
            for col in columns_to_update:
                if col in record and record[col] is not None:
                    set_params.append(f"{col} = ?")
                    set_values.append(record[col])
            
            # Skip if no columns to update
            if not set_params:
                continue
                
            # Build and execute update query
            update_query = f"UPDATE alib SET {', '.join(set_params)} WHERE rowid = ?"
            cursor.execute(update_query, set_values + [rowid])
            updated_count += 1
        
        # Also mark sqlmodded for these records
        cursor.execute(
            "UPDATE alib SET sqlmodded = 1 WHERE rowid IN ({})".format(
                ",".join("?" for _ in changed_rowids)
            ),
            changed_rowids
        )
        
        # Commit the transaction
        conn.commit()
        
        return updated_count
        
    except Exception as e:
        # Rollback on error
        conn.rollback()
        logging.error(f"Error writing updates to database: {str(e)}")
        raise e

def main():
    # Connect to the SQLite database
    db_path = '/tmp/amg/dbtemplate.db'
    logging.info(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    
    try:
        # Fetch contributors data
        logging.info("Fetching contributors data...")
        contributors = sqlite_to_polars(
            conn, 
            "SELECT current_val, replacement_val FROM _REF_vetted_contributors"
        )
        
        # Convert to a dictionary for faster lookups
        contributors_dict = dict(zip(
            contributors["current_val"].str.to_lowercase(),
            contributors["replacement_val"]
        ))
        
        # Fetch tracks data with all required columns
        logging.info("Fetching tracks data...")
        tracks = sqlite_to_polars(
            conn, 
            "SELECT rowid, artist, composer, arranger, lyricist, writer, albumartist, ensemble, performer, personnel, conductor, producer, engineer, mixer, remixer, , COALESCE(sqlmodded, 0) AS sqlmodded FROM alib ORDER BY rowid", 
            id_column="rowid"
        )
        
        # Filter only tracks that have at least one of the columns populated
        columns_to_replace = ["artist", "composer", "arranger", "lyricist", "writer", "albumartist", "ensemble", "performer", "personnel", "conductor", "producer", "engineer", "mixer", "remixer"]
        
        # Create a filter expression to select rows that have at least one populated column
        filter_expr = None
        for col in columns_to_replace:
            if filter_expr is None:
                filter_expr = pl.col(col).is_not_null()
            else:
                filter_expr = filter_expr | pl.col(col).is_not_null()
                
        tracks_filtered = tracks.filter(filter_expr)
        
        logging.info(f"Processing {tracks_filtered.height} tracks with data...")
        
        # Create a copy for updates
        updated_tracks = tracks_filtered.clone()
        
        # Apply replacement to all columns
        for col in columns_to_replace:
            updated_tracks = updated_tracks.with_columns(
                pl.col(col).map_elements(
                    lambda x: contributors_dict.get(x.lower(), x) if x is not None else None,
                    return_dtype=pl.Utf8
                ).alias(col)
            )
        
        # Identify changed rows by comparing original to updated values
        logging.info("Detecting changes...")
        
        # Create masks for each column and combine
        change_masks = []
        for col in columns_to_replace:
            # Create a mask for this column where values changed
            mask = (
                (tracks_filtered[col].is_not_null()) & 
                (tracks_filtered[col] != updated_tracks[col])
            )
            change_masks.append(mask)
        
        # Combine all masks (a track is changed if any column changed)
        final_change_mask = None
        for mask in change_masks:
            if final_change_mask is None:
                final_change_mask = mask
            else:
                final_change_mask = final_change_mask | mask
            
        # Get rowids of changed rows
        changed_rowids = updated_tracks.filter(final_change_mask)["rowid"].to_list()
        
        logging.info(f"Found {len(changed_rowids)} tracks with changes")
        
        # Write changes back to database
        if changed_rowids:
            num_updated = write_updates_to_db(
                conn, 
                updated_tracks, 
                changed_rowids, 
                columns_to_replace
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
    finally:
        # Close the database connection when done
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()