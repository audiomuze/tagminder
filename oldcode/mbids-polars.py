"""
Script Name: mbid-polars.py

Purpose:
    This script processes all records in alib and updates mbid's related to 

        'artist': 'musicbrainz_artistid',
        'albumartist': 'musicbrainz_albumartistid',
        'composer': 'musicbrainz_composerid',
        'engineer': 'musicbrainz_engineerid',
        'producer': 'musicbrainz_producerid'

    adding mbid's where missing and ensuring that mbid's appear in the same order as the urelated contributor tag.
    Where there is no matching MBID it writes the value _NO_MBID into the relevant mbid tag.
    This provides the ability to easily identify contributors that need to be added to MusicBrainz to generate a MBID

    It is the de-facto way of ensuring contributors and associated mbid's are accurately recods in your tags throughout
    your music collection.

    It is part of tagminder.

Usage:
    python mbid-polars.py
    uv run mbid-polars.py

Author: audiomuze
Created: 2025-04-18
"""


import polars as pl
import sqlite3
from typing import List, Dict, Set, Optional, Tuple, Any
import numpy as np

def load_dataframes(conn: sqlite3.Connection) -> Tuple[Dict[str, str], int]:
    """
    Load contributors dictionary and count rows in alib
    
    Args:
        conn: SQLite database connection
        
    Returns:
        Tuple of (contributors dictionary, total alib rows)
    """
    # Use polars to load contributors
    query = "SELECT lentity, mbid FROM _REF_mb_disambiguated"
    df_contributors = pl.read_database(query, conn, schema_overrides={
        "lentity": pl.Utf8,
        "mbid": pl.Utf8
    })
    
    # Convert to dictionary for faster lookups
    contributors_dict = dict(zip(df_contributors['lentity'].to_list(), df_contributors['mbid'].to_list()))
    
    # Get total count of alib rows
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alib")
    total_rows = cursor.fetchone()[0]
    
    return contributors_dict, total_rows

def process_chunk(conn: sqlite3.Connection, contributors_dict: Dict[str, str], 
                 offset: int, chunk_size: int) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    """
    Process a chunk of the database using Polars
    
    Args:
        conn: SQLite database connection
        contributors_dict: Dictionary mapping lowercase entity names to MusicBrainz IDs
        offset: Starting offset for this chunk
        chunk_size: Size of the chunk
        
    Returns:
        Tuple of (updates list, statistics dictionary)
    """
    # Define field mappings
    fields = {
        'artist': 'musicbrainz_artistid',
        'albumartist': 'musicbrainz_albumartistid',
        'composer': 'musicbrainz_composerid',
        'engineer': 'musicbrainz_engineerid',
        'producer': 'musicbrainz_producerid'
    }
    
    # Initialize statistics counters
    stats = {
        'additions': {},  # Empty to non-empty
        'corrections': {}  # Non-empty to different non-empty
    }
    
    # Get chunk with pre-filtering and include sqlmodded field
    query = f"""
        SELECT rowid, artist, albumartist, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid, 
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid,
               COALESCE(sqlmodded, 0) AS sqlmodded
        FROM alib 
        WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR 
              composer IS NOT NULL OR engineer IS NOT NULL OR 
              producer IS NOT NULL)
        ORDER BY rowid
        LIMIT {chunk_size} OFFSET {offset}
    """
    
    # Define schema with explicit types
    schema = {
        "rowid": pl.Int64,
        "artist": pl.Utf8,
        "albumartist": pl.Utf8,
        "composer": pl.Utf8,
        "engineer": pl.Utf8,
        "producer": pl.Utf8,
        "musicbrainz_artistid": pl.Utf8,
        "musicbrainz_albumartistid": pl.Utf8,
        "musicbrainz_composerid": pl.Utf8,
        "musicbrainz_engineerid": pl.Utf8,
        "musicbrainz_producerid": pl.Utf8,
        "sqlmodded": pl.Int64
    }
    
    df_chunk = pl.read_database(query, conn, schema_overrides=schema)
    
    # Process each row and collect updates
    updates_by_rowid = {}
    
    # Convert to dicts for row processing
    for row in df_chunk.iter_rows(named=True):
        changes_in_row = 0  # Track changes for this row to increment sqlmodded
        
        for field, mbid_field in fields.items():
            value = row[field]
            if value is None:
                continue
                
            # Split the value and match entities
            entities = [e.strip().lower() for e in str(value).split('\\\\')]
            matched_mbids = []
            
            for entity in entities:
                if entity in contributors_dict:
                    matched_mbids.append(contributors_dict[entity])
                else:
                    matched_mbids.append('_NO_MBID_FOUND')
            
            if not matched_mbids:
                continue
                
            new_value = '\\\\'.join(matched_mbids)
            
            # Check current value
            current_mbid = row[mbid_field]
            is_current_empty = current_mbid is None or (isinstance(current_mbid, str) and current_mbid.strip() == '')
            
            # Determine if this is an addition or correction
            update_needed = False
            
            if is_current_empty and new_value:
                # Empty to non-empty = addition
                field_type = field
                stats['additions'][field_type] = stats['additions'].get(field_type, 0) + 1
                update_needed = True
                changes_in_row += 1
            elif not is_current_empty and new_value != str(current_mbid).strip():
                # Non-empty to different = correction
                field_type = field
                stats['corrections'][field_type] = stats['corrections'].get(field_type, 0) + 1
                update_needed = True
                changes_in_row += 1
                
            if update_needed:
                rowid = row['rowid']
                if rowid not in updates_by_rowid:
                    updates_by_rowid[rowid] = {'rowid': rowid}
                updates_by_rowid[rowid][mbid_field] = new_value
        
        # If there were changes in this row, increment sqlmodded
        if changes_in_row > 0:
            rowid = row['rowid']
            current_sqlmodded = row['sqlmodded']
            new_sqlmodded = current_sqlmodded + changes_in_row
            
            if rowid not in updates_by_rowid:
                updates_by_rowid[rowid] = {'rowid': rowid}
                
            # Only include sqlmodded if it's > 0
            if new_sqlmodded > 0:
                updates_by_rowid[rowid]['sqlmodded'] = new_sqlmodded
            else:
                # Set to NULL explicitly if 0
                updates_by_rowid[rowid]['sqlmodded'] = None
    
    return list(updates_by_rowid.values()), stats

def write_updates_to_db(updates: List[Dict[str, Any]], conn: sqlite3.Connection, 
                       stats: Dict[str, Dict[str, int]], batch_size: int = 1000):
    """
    Write updates to both temporary table and main table using batching
    
    Args:
        updates: List of update dictionaries
        conn: SQLite database connection
        stats: Statistics dictionary
        batch_size: Size of batches for processing
    """
    if not updates:
        print("No updates to write to database")
        return
    
    cursor = conn.cursor()
    
    # Create temporary table - now includes sqlmodded
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS _TMP_alib_updates (
        rowid INTEGER,
        musicbrainz_artistid TEXT,
        musicbrainz_albumartistid TEXT,
        musicbrainz_composerid TEXT,
        musicbrainz_engineerid TEXT,
        musicbrainz_producerid TEXT,
        sqlmodded INTEGER
    )
    """)
    
    # Clear any existing data in temporary table
    cursor.execute("DELETE FROM _TMP_alib_updates")
    
    # Get column names - now includes sqlmodded
    columns = ['rowid', 'musicbrainz_artistid', 'musicbrainz_albumartistid',
              'musicbrainz_composerid', 'musicbrainz_engineerid', 'musicbrainz_producerid',
              'sqlmodded']
    
    # Check if a transaction is already active
    cursor.execute("SELECT * FROM sqlite_master LIMIT 0")
    transaction_active = conn.in_transaction
        
    # Only begin a transaction if one isn't already active
    if not transaction_active:
        conn.execute("BEGIN TRANSACTION")
    
    try:
        # Prepare statements
        insert_placeholders = ', '.join(['?' for _ in columns])
        insert_query = f"INSERT INTO _TMP_alib_updates ({', '.join(columns)}) VALUES ({insert_placeholders})"
        
        # Process in batches
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            
            # Insert into temporary table
            for update in batch:
                # Prepare data for insertion with None for missing columns
                insert_data = [update.get('rowid')]
                for col in columns[1:]:  # Skip rowid
                    insert_data.append(update.get(col))
                
                # Insert into temporary table
                cursor.execute(insert_query, insert_data)
            
            # Update main table - only update non-null values
            for update in batch:
                update_cols = []
                update_vals = []
                rowid = update['rowid']
                
                # Only include fields that exist in the update and are not None
                for col in columns[1:]:  # Skip rowid
                    if col in update:
                        if update[col] is not None:
                            update_cols.append(f"{col} = ?")
                            update_vals.append(update[col])
                        elif col == 'sqlmodded':  # Special handling for sqlmodded to set NULL when 0
                            update_cols.append(f"{col} = NULL")
                
                if update_cols:  # Only proceed if there are columns to update
                    update_query = f"UPDATE alib SET {', '.join(update_cols)} WHERE rowid = ?"
                    cursor.execute(update_query, update_vals + [rowid])
        
        # Only commit if we started the transaction
        if not transaction_active:
            conn.commit()
        
    except Exception as e:
        # Rollback on error only if we started the transaction
        if not transaction_active:
            conn.rollback()
        raise e

def process_database(conn: sqlite3.Connection, chunk_size: int = 50000):
    """
    Process the entire database in chunks
    
    Args:
        conn: SQLite database connection
        chunk_size: Size of each chunk (increased for Polars)
    """
    # Get contributors dictionary and total rows once
    contributors_dict, total_rows = load_dataframes(conn)
    
    # Initialize statistics
    all_stats = {
        'additions': {},
        'corrections': {}
    }
    
    # Start a transaction for all operations
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Process in chunks and write updates for each chunk immediately
        for offset in range(0, total_rows, chunk_size):
            print(f"Processing rows {offset} to {offset + chunk_size}...")
            
            # Process the chunk
            chunk_updates, chunk_stats = process_chunk(conn, contributors_dict, offset, chunk_size)
            
            # Combine statistics
            for category in ['additions', 'corrections']:
                for field, count in chunk_stats[category].items():
                    all_stats[category][field] = all_stats[category].get(field, 0) + count
            
            # Write updates for this chunk immediately
            if chunk_updates:
                # Pass conn in transaction mode
                write_updates_to_db(chunk_updates, conn, all_stats)
        
        # Commit the transaction at the end
        conn.commit()
        
        # Display final statistics
        display_statistics(all_stats)
    
    except Exception as e:
        # Rollback on error
        conn.rollback()
        raise e

def process_full_database(conn: sqlite3.Connection):
    """
    Process the entire database in one go with Polars
    
    Args:
        conn: SQLite database connection
    """
    # Get contributors dictionary
    contributors_dict, _ = load_dataframes(conn)
    
    # Define field mappings
    fields = {
        'artist': 'musicbrainz_artistid',
        'albumartist': 'musicbrainz_albumartistid',
        'composer': 'musicbrainz_composerid',
        'engineer': 'musicbrainz_engineerid',
        'producer': 'musicbrainz_producerid'
    }
    
    # Initialize statistics
    all_stats = {
        'additions': {},
        'corrections': {}
    }
    
    # Define schema with explicit types
    schema = {
        "rowid": pl.Int64,
        "artist": pl.Utf8,
        "albumartist": pl.Utf8,
        "composer": pl.Utf8,
        "engineer": pl.Utf8,
        "producer": pl.Utf8,
        "musicbrainz_artistid": pl.Utf8,
        "musicbrainz_albumartistid": pl.Utf8,
        "musicbrainz_composerid": pl.Utf8,
        "musicbrainz_engineerid": pl.Utf8,
        "musicbrainz_producerid": pl.Utf8,
        "sqlmodded": pl.Int64
    }
    
    # Get all relevant data at once - include sqlmodded field
    query = """
        SELECT rowid, artist, albumartist, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid, 
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid,
               COALESCE(sqlmodded, 0) AS sqlmodded
        FROM alib 
        WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR 
              composer IS NOT NULL OR engineer IS NOT NULL OR 
              producer IS NOT NULL)
    """
    
    try:
        print("Loading entire database with Polars...")
        df = pl.read_database(query, conn, schema_overrides=schema)
        print(f"Loaded {df.height} rows for processing")
        
        # Process full dataset
        updates_by_rowid = {}
        
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")
        
        # Process each row
        for row in df.iter_rows(named=True):
            changes_in_row = 0  # Track changes for this row to increment sqlmodded
            
            for field, mbid_field in fields.items():
                value = row[field]
                if value is None:
                    continue
                    
                # Split the value and match entities
                entities = [e.strip().lower() for e in str(value).split('\\\\')]
                matched_mbids = []
                
                for entity in entities:
                    if entity in contributors_dict:
                        matched_mbids.append(contributors_dict[entity])
                    else:
                        matched_mbids.append('_NO_MBID_FOUND')
                
                if not matched_mbids:
                    continue
                    
                new_value = '\\\\'.join(matched_mbids)
                
                # Check current value
                current_mbid = row[mbid_field]
                is_current_empty = current_mbid is None or (isinstance(current_mbid, str) and current_mbid.strip() == '')
                
                # Determine if this is an addition or correction
                update_needed = False
                
                if is_current_empty and new_value:
                    # Empty to non-empty = addition
                    field_type = field
                    all_stats['additions'][field_type] = all_stats['additions'].get(field_type, 0) + 1
                    update_needed = True
                    changes_in_row += 1
                elif not is_current_empty and new_value != str(current_mbid).strip():
                    # Non-empty to different = correction
                    field_type = field
                    all_stats['corrections'][field_type] = all_stats['corrections'].get(field_type, 0) + 1
                    update_needed = True
                    changes_in_row += 1
                    
                if update_needed:
                    rowid = row['rowid']
                    if rowid not in updates_by_rowid:
                        updates_by_rowid[rowid] = {'rowid': rowid}
                    updates_by_rowid[rowid][mbid_field] = new_value
            
            # If there were changes in this row, increment sqlmodded
            if changes_in_row > 0:
                rowid = row['rowid']
                current_sqlmodded = row['sqlmodded']
                new_sqlmodded = current_sqlmodded + changes_in_row
                
                if rowid not in updates_by_rowid:
                    updates_by_rowid[rowid] = {'rowid': rowid}
                
                # Only include sqlmodded if it's > 0
                if new_sqlmodded > 0:
                    updates_by_rowid[rowid]['sqlmodded'] = new_sqlmodded
                else:
                    # Set to NULL explicitly if 0
                    updates_by_rowid[rowid]['sqlmodded'] = None
        
        # Write all updates at once
        if updates_by_rowid:
            print(f"Writing {len(updates_by_rowid)} updates to database...")
            write_updates_to_db(list(updates_by_rowid.values()), conn, all_stats, batch_size=5000)
        
        # Commit the transaction
        conn.commit()
        
        # Display statistics
        display_statistics(all_stats)
    
    except Exception as e:
        conn.rollback()
        print(f"Error processing database: {e}")
        raise e

def display_statistics(stats: Dict[str, Dict[str, int]]):
    """Display statistics about the updates"""
    total_additions = sum(stats['additions'].values())
    total_corrections = sum(stats['corrections'].values())
    total_changes = total_additions + total_corrections
    
    print(f"\nMusicBrainz ID Update Summary:")
    print(f"==============================")
    print(f"Total changes: {total_changes}")
    print(f"  - New IDs added: {total_additions}")
    print(f"  - Existing IDs corrected: {total_corrections}")
    
    # Print detailed statistics by field type
    print("\nAdditions by field type:")
    for field, count in sorted(stats['additions'].items()):
        print(f"  {field}: {count}")
    
    print("\nCorrections by field type:")
    for field, count in sorted(stats['corrections'].items()):
        print(f"  {field}: {count}")

def update_with_polars(file_path: str, use_chunking: bool = False):
    """
    Optimized implementation using Polars for data processing
    
    Args:
        file_path: Path to the SQLite database
        use_chunking: Whether to use chunking or process entire database at once
    """
    # Open a single connection
    conn = sqlite3.connect(file_path)
    
    try:
        if use_chunking:
            # Process in chunks (useful for very large databases or limited memory)
            process_database(conn, chunk_size=50000)  # Increased chunk size for Polars
        else:
            # Process entire database at once (preferred with Polars if memory allows)
            process_full_database(conn)
        
    finally:
        # Close the connection when done
        conn.close()

def main():
    """Main function to run the script"""
    db_path = '/tmp/amg/dbtemplate.db'
    # Set use_chunking=True if memory constraints are an issue
    update_with_polars(db_path, use_chunking=False)

if __name__ == "__main__":
    main()