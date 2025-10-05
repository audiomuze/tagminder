"""
Script Name: update_contributors.py

Purpose:
    Reads specific columns from two SQLite tables and updates entity names in musicbrainz_contributors
    where they match case-insensitively with allmusic_artist but have different exact spelling.

    The script performs the following operations:
    1. Reads allmusic_artist, name_similarity from allmusic_reference_data
    2. Reads rowid, entity, lentity from musicbrainz_contributors  
    3. Filters allmusic_reference_data to only rows where name_similarity = 1
    4. Updates musicbrainz_contributors DataFrame where lentity matches but entity differs
    5. Writes only changed rows back to database using rowid

Author: audiomuze
Created: 2025-09-17
Modified: 2025-09-17
"""

import polars as pl
import sqlite3
import logging

def update_musicbrainz_contributors(db_name: str = "dbtemplate.db"):
    """
    Main function to update entity values in musicbrainz_contributors table.
    
    Args:
        db_name: SQLite database file name/path
        
    Returns:
        Number of changes made
    """
    try:
        print("Reading specific columns from tables...")
        
        # Read only required columns from allmusic_reference_data WHERE name_similarity = 1
        allmusic_query = "SELECT allmusic_artist, name_similarity FROM allmusic_reference_data WHERE name_similarity = '1'"
        allmusic_df = pl.read_database(allmusic_query, sqlite3.connect(db_name))
        
        # Read only required columns from musicbrainz_contributors
        musicbrainz_query = "SELECT rowid, entity, lentity FROM musicbrainz_contributors"
        musicbrainz_df = pl.read_database(musicbrainz_query, sqlite3.connect(db_name))
        
        print(f"AllMusic DataFrame (name_similarity=1): {allmusic_df.shape}")
        print(f"MusicBrainz DataFrame: {musicbrainz_df.shape}")
        
        if len(allmusic_df) == 0:
            print("No rows with name_similarity = 1 found. Exiting.")
            return 0
        
        # Create a mapping of lowercase allmusic_artist to original allmusic_artist
        artist_mapping = {}
        for row in allmusic_df.iter_rows(named=True):
            allmusic_artist = row["allmusic_artist"]
            if allmusic_artist and str(allmusic_artist).strip():
                lowercase_key = str(allmusic_artist).lower().strip()
                artist_mapping[lowercase_key] = str(allmusic_artist)
        
        print(f"Created mapping for {len(artist_mapping)} unique artists")
        
        # Find rows that need updating using vectorized operations
        # Add a column indicating if this row needs updating
        musicbrainz_df = musicbrainz_df.with_columns([
            # Check if lentity exists in our artist mapping
            pl.col("lentity").map_elements(
                lambda x: x in artist_mapping if x else False,
                return_dtype=pl.Boolean
            ).alias("has_match"),
            
            # Get the canonical artist name for matched entries
            pl.col("lentity").map_elements(
                lambda x: artist_mapping.get(x, None) if x else None,
                return_dtype=pl.String
            ).alias("canonical_name")
        ])
        
        # Filter to rows that have matches AND need updating (entity != canonical_name)
        rows_to_update = musicbrainz_df.filter(
            pl.col("has_match") & 
            (pl.col("entity") != pl.col("canonical_name"))
        )
        
        changes_count = len(rows_to_update)
        print(f"Found {changes_count} entities to update")
        
        if changes_count == 0:
            print("No changes needed.")
            return 0
        
        # Update the DataFrame - replace entity with canonical_name for matched rows
        musicbrainz_df = musicbrainz_df.with_columns(
            pl.when(pl.col("has_match") & (pl.col("entity") != pl.col("canonical_name")))
            .then(pl.col("canonical_name"))
            .otherwise(pl.col("entity"))
            .alias("entity")
        )
        
        # Show sample changes
        print("\nSample changes:")
        sample_changes = rows_to_update.select(["rowid", "entity", "canonical_name"]).head(5)
        for row in sample_changes.iter_rows(named=True):
            print(f"  Row {row['rowid']}: '{row['entity']}' -> '{row['canonical_name']}'")
        if changes_count > 5:
            print(f"  ... and {changes_count - 5} more changes")
        
        # Write only changed rows back to database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        update_sql = "UPDATE musicbrainz_contributors SET entity = ? WHERE rowid = ?"
        
        # Execute updates for only the changed rows
        for row in rows_to_update.iter_rows(named=True):
            cursor.execute(update_sql, (row["canonical_name"], row["rowid"]))
        
        conn.commit()
        conn.close()
        
        print(f"Updated {changes_count} rows in the database")
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
    print("Reading only records with name_similarity = 1 from allmusic_reference_data")
    print("Reading only required columns: rowid, entity, lentity from musicbrainz_contributors")
    print("Updating entities to match allmusic_artist exactly when lentity matches...")
    print("Writing only changed rows back to database using rowid...")
    
    changes_made = update_musicbrainz_contributors(db_name)
    
    print(f"\nProcessing completed. Made {changes_made} entity updates.")
