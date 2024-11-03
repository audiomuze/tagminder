import pandas as pd
import sqlite3
from typing import List, Dict, Set
import numpy as np

def load_dataframes(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load dataframes from SQLite database"""
    conn = sqlite3.connect(db_path)
    
    # Load alib table
    df_alib = pd.read_sql("""
        SELECT rowid, artist, albumartist, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid, 
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid
        FROM alib ORDER BY rowid
    """, conn)
    
    # Load contributors table
    df_contributors = pd.read_sql("""
        SELECT entity, lentity, mbid 
        FROM _REF_mb_disambiguated
    """, conn)
    
    conn.close()
    return df_alib, df_contributors

def split_and_match(value: str, contributors_dict: Dict[str, str]) -> tuple[List[str], List[str]]:
    """Split delimited string and find matches in contributors"""
    if pd.isna(value):
        return [], []
    
    entities = [e.strip().lower() for e in value.split('\\\\')]
    matched_mbids = []
    
    for entity in entities:
        if entity in contributors_dict:
            matched_mbids.append(contributors_dict[entity])
    
    return entities, matched_mbids

def process_row(row: pd.Series, contributors_dict: Dict[str, str]) -> Dict[str, List]:
    """Process a single row and return original and updated MusicBrainz IDs"""
    fields = {
        'artist': 'musicbrainz_artistid',
        'albumartist': 'musicbrainz_albumartistid',
        'composer': 'musicbrainz_composerid',
        'engineer': 'musicbrainz_engineerid',
        'producer': 'musicbrainz_producerid'
    }
    
    changes = {}
    
    for field, mbid_field in fields.items():
        if pd.notna(row[field]):
            entities, matched_mbids = split_and_match(row[field], contributors_dict)
            if matched_mbids:  # Only include if we found matches
                current_value = str(row[mbid_field]) if pd.notna(row[mbid_field]) else ''
                new_value = '\\\\'.join(matched_mbids)
                if new_value != current_value:
                    changes[mbid_field] = new_value
    
    if changes:  # Only return if there are changes
        return {
            'rowid': row['rowid'],
            **changes
        }
    return None

def update_musicbrainz_ids(df_alib: pd.DataFrame, df_contributors: pd.DataFrame) -> pd.DataFrame:
    """Update MusicBrainz IDs based on contributor matches"""
    # Create contributors dictionary for faster lookup
    contributors_dict = dict(zip(df_contributors['lentity'], df_contributors['mbid']))
    
    # Process each row and collect updates
    updates_list = []
    for idx, row in df_alib.iterrows():
        update_dict = process_row(row, contributors_dict)
        if update_dict:  # Only add if there are changes
            updates_list.append(update_dict)
    
    if not updates_list:
        return pd.DataFrame()
    
    # Create DataFrame with updates
    df_updates = pd.DataFrame(updates_list)
    
    return df_updates

def write_updates_to_db(df_updates: pd.DataFrame, db_path: str):
    """Write updates to both temporary table and main table"""
    if df_updates.empty:
        print("No updates to write to database")
        return
    
    conn = sqlite3.connect(db_path)
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
    
    # Prepare data for database operations
    columns = ['rowid', 'musicbrainz_artistid', 'musicbrainz_albumartistid',
              'musicbrainz_composerid', 'musicbrainz_engineerid', 'musicbrainz_producerid']
    
    # Fill missing columns with None
    for col in columns:
        if col not in df_updates.columns:
            df_updates[col] = None
    
    update_data = df_updates[columns].values.tolist()
    
    # Insert into temporary table
    placeholders = ','.join(['?' for _ in columns])
    insert_query = f"""
    INSERT INTO _TMP_alib_updates (
        {', '.join(columns)}
    ) VALUES ({placeholders})
    """
    
    cursor.executemany(insert_query, update_data)
    
    # Update main table - only update non-null values
    for row in update_data:
        update_cols = []
        update_vals = []
        rowid = row[0]  # First column is always rowid
        
        # Only include non-None values in the update
        for col, val in zip(columns[1:], row[1:]):  # Skip rowid
            if val is not None:
                update_cols.append(f"{col} = ?")
                update_vals.append(val)
        
        if update_cols:  # Only proceed if there are columns to update
            update_query = f"""
            UPDATE alib 
            SET {', '.join(update_cols)}
            WHERE rowid = ?
            """
            cursor.execute(update_query, update_vals + [rowid])
    
    conn.commit()
    conn.close()
    
    print(f"Updated {len(df_updates)} rows in the database")

# Main execution
def main():
    db_path = '/tmp/amg/dbtemplate.db'
    
    # Load data
    df_alib, df_contributors = load_dataframes(db_path)
    
    # Process updates
    df_updates = update_musicbrainz_ids(df_alib, df_contributors)
    
    # Write updates to database
    write_updates_to_db(df_updates, db_path)

if __name__ == "__main__":
    main()