"""
Script Name: update_contributors.py

Purpose:
    Reads specific columns from two SQLite tables and updates entity names, genres, and styles
    in _REF_mb_disambiguated where they match case-insensitively with allmusic_artist.

    The script performs the following operations:
    1. Reads allmusic_artist, name_similarity, genre, styles from allmusic_reference_data
    2. Reads rowid, entity, lentity, genre, styles, updated_from_allmusic from _REF_mb_disambiguated
    3. Optionally filters allmusic_reference_data to only rows where name_similarity = 1
    4. Updates _REF_mb_disambiguated DataFrame where lentity matches:
       - Updates entity name if different
       - Copies genre and styles (including when NULL in target)
       - Sets updated_from_allmusic = 1
    5. Writes only changed rows back to database using rowid

Author: audiomuze
Created: 2025-09-17
Modified: 2025-10-15
"""

import polars as pl
import sqlite3
import logging
import argparse


def update__REF_mb_disambiguated(
    db_name: str = "dbtemplate.db", require_similarity_1: bool = True
):
    """
    Main function to update entity values, genres, and styles in _REF_mb_disambiguated table.

    Args:
        db_name: SQLite database file name/path
        require_similarity_1: If True, only process records where name_similarity = 1

    Returns:
        Number of changes made
    """
    try:
        print("Reading specific columns from tables...")

        # Build query with optional similarity filter
        # FIX: Use CAST to handle both '1' and '1.0' values
        if require_similarity_1:
            allmusic_query = """
                SELECT allmusic_artist, name_similarity, genre, styles
                FROM allmusic_reference_data
                WHERE CAST(name_similarity AS REAL) = 1.0
            """
            print("Filtering to records with name_similarity = 1.0")
        else:
            allmusic_query = """
                SELECT allmusic_artist, name_similarity, genre, styles
                FROM allmusic_reference_data
            """
            print("Processing all records regardless of name_similarity score")

        # Define explicit schemas to avoid Polars' flawed type inference
        allmusic_schema = {
            "allmusic_artist": pl.Utf8,
            "name_similarity": pl.Utf8,
            "genre": pl.Utf8,
            "styles": pl.Utf8,
        }
        
        allmusic_df = pl.read_database(
            allmusic_query, 
            sqlite3.connect(db_name),
            schema_overrides=allmusic_schema
        )

        # Read required columns from _REF_mb_disambiguated
        musicbrainz_query = """
            SELECT rowid, entity, lentity, genre, styles, updated_from_allmusic
            FROM _REF_mb_disambiguated
        """
        
        musicbrainz_schema = {
            "rowid": pl.Int64,
            "entity": pl.Utf8,
            "lentity": pl.Utf8,
            "genre": pl.Utf8,
            "styles": pl.Utf8,
            "updated_from_allmusic": pl.Int64,
        }
        
        musicbrainz_df = pl.read_database(
            musicbrainz_query, 
            sqlite3.connect(db_name),
            schema_overrides=musicbrainz_schema
        )

        print(f"AllMusic DataFrame: {allmusic_df.shape}")
        print(f"MusicBrainz DataFrame: {musicbrainz_df.shape}")

        if len(allmusic_df) == 0:
            print("No matching rows found in allmusic_reference_data. Exiting.")
            return 0

        # DEBUG: Check if "Idles" exists in the AllMusic data
        print("\n=== DEBUG: Searching for 'Idles' in AllMusic data ===")
        idles_in_allmusic = allmusic_df.filter(
            pl.col("allmusic_artist").str.to_lowercase() == "idles"
        )
        print(f"Found {len(idles_in_allmusic)} records with 'idles' (case-insensitive) in AllMusic:")
        if len(idles_in_allmusic) > 0:
            for row in idles_in_allmusic.iter_rows(named=True):
                print(f"  allmusic_artist: '{row['allmusic_artist']}', name_similarity: '{row['name_similarity']}'")

        # Prepare the allmusic data with lowercase key and remove duplicates
        allmusic_prepared = allmusic_df.with_columns([
            pl.col("allmusic_artist").str.to_lowercase().alias("lartist")
        ]).unique(subset=["lartist"])  # Remove duplicates

        print(f"AllMusic prepared: {allmusic_prepared.shape}")

        # DEBUG: Check if "idles" exists in the prepared data
        idles_in_prepared = allmusic_prepared.filter(pl.col("lartist") == "idles")
        print(f"Found {len(idles_in_prepared)} records with lartist='idles' in prepared data:")
        if len(idles_in_prepared) > 0:
            for row in idles_in_prepared.iter_rows(named=True):
                print(f"  lartist: '{row['lartist']}', allmusic_artist: '{row['allmusic_artist']}'")

        # Join instead of manual mapping - rename columns before join to avoid conflicts
        allmusic_prepared = allmusic_prepared.rename({
            "genre": "amg_genre",
            "styles": "amg_styles"
        })

        musicbrainz_df = musicbrainz_df.join(
            allmusic_prepared, 
            left_on="lentity", 
            right_on="lartist", 
            how="left"
        )

        # Add the has_match column
        musicbrainz_df = musicbrainz_df.with_columns([
            pl.col("allmusic_artist").is_not_null().alias("has_match")
        ])

        print(f"After join - MusicBrainz DataFrame: {musicbrainz_df.shape}")

        # DEBUG: Check the joined data for 'idles'
        print("\n=== DEBUG: Joined data for 'idles' ===")
        idles_joined = musicbrainz_df.filter(pl.col("lentity") == "idles")
        if len(idles_joined) > 0:
            for row in idles_joined.select(["rowid", "entity", "lentity", "allmusic_artist", "has_match"]).iter_rows(named=True):
                print(f"  rowid: {row['rowid']}, entity: '{row['entity']}', allmusic_artist: '{row['allmusic_artist']}', has_match: {row['has_match']}")

        # Filter to rows that have matches AND need updating
        # A row needs updating if: name differs OR genre differs OR styles differ
        # IMPORTANT: Handle NULL comparisons explicitly since NULL != value returns NULL (not True)
        rows_to_update = musicbrainz_df.filter(
            pl.col("has_match")
            & (
                # Name is different
                (pl.col("entity") != pl.col("allmusic_artist"))
                # Genre is NULL in target but has value in source
                | (pl.col("genre").is_null() & pl.col("amg_genre").is_not_null())
                # Genre has value in both but they differ
                | (
                    (pl.col("genre").is_not_null())
                    & (pl.col("amg_genre").is_not_null())
                    & (pl.col("genre") != pl.col("amg_genre"))
                )
                # Styles is NULL in target but has value in source
                | (pl.col("styles").is_null() & pl.col("amg_styles").is_not_null())
                # Styles has value in both but they differ
                | (
                    (pl.col("styles").is_not_null())
                    & (pl.col("amg_styles").is_not_null())
                    & (pl.col("styles") != pl.col("amg_styles"))
                )
            )
        )

        changes_count = len(rows_to_update)
        print(f"Found {changes_count} entities to update")

        if changes_count == 0:
            print("No changes needed.")
            return 0

        # Show sample changes
        print("\nSample changes:")
        sample_changes = rows_to_update.select(
            [
                "rowid",
                "entity",
                "allmusic_artist",
                "genre",
                "amg_genre",
                "styles",
                "amg_styles",
            ]
        ).head(5)
        for row in sample_changes.iter_rows(named=True):
            changes = []
            if row["entity"] != row["allmusic_artist"]:
                changes.append(f"name: '{row['entity']}' -> '{row['allmusic_artist']}'")
            if row["genre"] != row["amg_genre"]:
                changes.append(f"genre: '{row['genre']}' -> '{row['amg_genre']}'")
            if row["styles"] != row["amg_styles"]:
                changes.append(
                    f"styles: '{row['styles']}' -> '{row['amg_styles']}'"
                )
            print(f"  Row {row['rowid']}: {', '.join(changes)}")
        if changes_count > 5:
            print(f"  ... and {changes_count - 5} more changes")

        # Write only changed rows back to database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        update_sql = """
            UPDATE _REF_mb_disambiguated
            SET entity = ?, genre = ?, styles = ?, updated_from_allmusic = 1
            WHERE rowid = ?
        """

        # Execute updates for only the changed rows
        for row in rows_to_update.iter_rows(named=True):
            cursor.execute(
                update_sql,
                (
                    row["allmusic_artist"],
                    row["amg_genre"],
                    row["amg_styles"],
                    row["rowid"],
                ),
            )

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
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Update entity names, genres, and styles from AllMusic reference data"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="dbtemplate.db",
        help="SQLite database file name/path (default: dbtemplate.db)",
    )
    parser.add_argument(
        "--ignore-similarity",
        action="store_true",
        help="Process all records regardless of name_similarity score (default: only process similarity=1)",
    )

    args = parser.parse_args()

    print("Starting contributor update process...")
    print(f"Database: {args.db}")
    print(
        f"Similarity filter: {'DISABLED (processing all records)' if args.ignore_similarity else 'ENABLED (only name_similarity=1.0)'}"
    )
    print("Reading allmusic_artist, genre, styles from allmusic_reference_data")
    print(
        "Reading rowid, entity, lentity, genre, styles, updated_from_allmusic from _REF_mb_disambiguated"
    )
    print(
        "Updating entities, genres, styles and setting updated_from_allmusic=1 where lentity matches..."
    )
    print("Writing only changed rows back to database using rowid...")

    changes_made = update__REF_mb_disambiguated(
        db_name=args.db, require_similarity_1=not args.ignore_similarity
    )

    print(f"\nProcessing completed. Made {changes_made} entity updates.")