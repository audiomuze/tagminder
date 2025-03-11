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
    # Use pandas for SQLite compatibility to load contributors
    import pandas as pd
    
    # Load contributors table
    df_contributors = pd.read_sql("""
        SELECT lentity, mbid 
        FROM _REF_mb_disambiguated
    """, conn)
    
    # Convert to dictionary for faster lookups
    contributors_dict = dict(zip(df_contributors['lentity'], df_contributors['mbid']))
    
    # Get total count of alib rows
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM alib")
    total_rows = cursor.fetchone()[0]
    
    return contributors_dict, total_rows

def process_chunk(conn: sqlite3.Connection, contributors_dict: Dict[str, str], 
                 offset: int, chunk_size: int) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    """
    Process a chunk of the database using direct SQL and dictionaries
    
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
    
    # Use pandas for SQLite compatibility
    import pandas as pd
    
    # Get chunk with pre-filtering
    df_chunk = pd.read_sql(f"""
        SELECT rowid, artist, albumartist, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid, 
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid
        FROM alib 
        WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR 
              composer IS NOT NULL OR engineer IS NOT NULL OR 
              producer IS NOT NULL)
        ORDER BY rowid
        LIMIT {chunk_size} OFFSET {offset}
    """, conn)
    
    # Process each row and collect updates
    updates_by_rowid = {}
    
    # Process each row
    for _, row in df_chunk.iterrows():
        for field, mbid_field in fields.items():
            value = row[field]
            if pd.isna(value):
                continue
                
            # Split the value and match entities
            entities = [e.strip().lower() for e in str(value).split('\\\\')]
            matched_mbids = []
            
            for entity in entities:
                if entity in contributors_dict:
                    matched_mbids.append(contributors_dict[entity])
            
            if not matched_mbids:
                continue
                
            new_value = '\\\\'.join(matched_mbids)
            
            # Check current value
            current_mbid = row[mbid_field]
            is_current_empty = pd.isna(current_mbid) or (isinstance(current_mbid, str) and current_mbid.strip() == '')
            
            # Determine if this is an addition or correction
            update_needed = False
            
            if is_current_empty and new_value:
                # Empty to non-empty = addition
                field_type = field
                stats['additions'][field_type] = stats['additions'].get(field_type, 0) + 1
                update_needed = True
            elif not is_current_empty and new_value != str(current_mbid).strip():
                # Non-empty to different = correction
                field_type = field
                stats['corrections'][field_type] = stats['corrections'].get(field_type, 0) + 1
                update_needed = True
                
            if update_needed:
                rowid = row['rowid']
                if rowid not in updates_by_rowid:
                    updates_by_rowid[rowid] = {'rowid': rowid}
                updates_by_rowid[rowid][mbid_field] = new_value
    
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
    
    # Create temporary table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS _TMP_alib_updates (
        rowid INTEGER,
        musicbrainz_artistid TEXT,
        musicbrainz_albumartistid TEXT,
        musicbrainz_composerid TEXT,
        musicbrainz_engineerid TEXT,
        musicbrainz_producerid TEXT
    )
    """)
    
    # Clear any existing data in temporary table
    cursor.execute("DELETE FROM _TMP_alib_updates")
    
    # Get column names
    columns = ['rowid', 'musicbrainz_artistid', 'musicbrainz_albumartistid',
              'musicbrainz_composerid', 'musicbrainz_engineerid', 'musicbrainz_producerid']
    
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
                
                # Only include fields that exist in the update
                for col in columns[1:]:  # Skip rowid
                    if col in update and update[col] is not None:
                        update_cols.append(f"{col} = ?")
                        update_vals.append(update[col])
                
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
    
    # Display summary statistics
    total_additions = sum(stats['additions'].values())
    total_corrections = sum(stats['corrections'].values())
    total_changes = total_additions + total_corrections
    
    print(f"\nMusicBrainz ID Update Summary:")
    print(f"==============================")
    print(f"Total rows updated: {len(updates)}")
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

def process_database(conn: sqlite3.Connection, chunk_size: int = 10000):
    """
    Process the entire database in chunks
    
    Args:
        conn: SQLite database connection
        chunk_size: Size of each chunk
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
        total_additions = sum(all_stats['additions'].values())
        total_corrections = sum(all_stats['corrections'].values())
        total_changes = total_additions + total_corrections
        
        print(f"\nMusicBrainz ID Update Summary:")
        print(f"==============================")
        print(f"Total changes: {total_changes}")
        print(f"  - New IDs added: {total_additions}")
        print(f"  - Existing IDs corrected: {total_corrections}")
        
        # Print detailed statistics by field type
        print("\nAdditions by field type:")
        for field, count in sorted(all_stats['additions'].items()):
            print(f"  {field}: {count}")
        
        print("\nCorrections by field type:")
        for field, count in sorted(all_stats['corrections'].items()):
            print(f"  {field}: {count}")
    
    except Exception as e:
        # Rollback on error
        conn.rollback()
        raise e

def update_with_polars(file_path: str):
    """
    Optimized implementation using Polars for data processing but pandas for database operations
    
    Args:
        file_path: Path to the SQLite database
    """
    # Open a single connection
    conn = sqlite3.connect(file_path)
    
    try:
        # Load data using pandas for SQLite compatibility
        import pandas as pd
        
        # Load contributors
        df_contributors = pd.read_sql("SELECT lentity, mbid FROM _REF_mb_disambiguated", conn)
        contributors_dict = dict(zip(df_contributors['lentity'], df_contributors['mbid']))
        
        # Now process the database in chunks
        process_database(conn, chunk_size=10000)
        
    finally:
        # Close the connection when done
        conn.close()

def main():
    """Main function to run the script"""
    db_path = '/tmp/amg/dbtemplate.db'
    update_with_polars(db_path)

if __name__ == "__main__":
    main()