"""
Script Name: droptags-polars.py

Purpose:
    This script processes all records in alib and sets tag values to null for all unauthorised tags
    (i.e. any tagnames that don't appear in fixed_columns defined in main()

    It is the de-facto way of getting rid of tags you don't want in your music collection.
    It is part of tagminder.

Usage:
    python droptags-polars.py
    uv run droptags-polars.py

Author: audiomuze
Created: 2025-04-13
"""


import polars as pl
import sqlite3
import logging
from typing import List, Dict

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
        id_column: Name of ID column to preserve type

    Returns:
        Polars DataFrame with results
    """
    cursor = conn.cursor()
    cursor.execute(query)

    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if id_column and col_name == id_column:
            data[col_name] = col_data
        elif col_name == "sqlmodded":
            data[col_name] = [int(x) if x is not None else 0 for x in col_data]
        else:
            data[col_name] = [x if x is not None else None for x in col_data]

    df = pl.DataFrame(data)
    return df

def cleanup_dataframe(df: pl.DataFrame, fixed_columns: List[str]) -> tuple[pl.DataFrame, Dict[str, int]]:
    columns_to_drop = [col for col in df.columns if col not in fixed_columns and col != "rowid" and col != "sqlmodded"]
    updated_df = df.clone()
    changes_by_column: Dict[str, int] = {col: 0 for col in columns_to_drop}

    update_exprs = []
    sqlmodded_updates = pl.lit(0)

    for col in columns_to_drop:
        mask = df[col].is_not_null()
        count = mask.sum()
        if count > 0:
            updated_df = updated_df.with_columns(
                pl.when(mask).then(pl.lit(None)).otherwise(pl.col(col)).alias(col)
            )
            changes_by_column[col] = count
            sqlmodded_updates = sqlmodded_updates.add(mask.cast(pl.Int32())) # Increment sqlmodded for each nullification

    if "sqlmodded" in updated_df.columns:
        updated_df = updated_df.with_columns(
            (pl.col("sqlmodded").fill_null(0) + sqlmodded_updates).alias("sqlmodded")
        )
    else:
        updated_df = updated_df.with_columns(sqlmodded_updates.alias("sqlmodded"))

    return updated_df, changes_by_column

def write_updates_to_db(conn: sqlite3.Connection, updated_df: pl.DataFrame, original_df: pl.DataFrame) -> int:
    """
    Write updates (nullified values) back to the database for rows with sqlmodded > 0.

    Args:
        conn: SQLite database connection
        updated_df: DataFrame with nullified values and updated sqlmodded
        original_df: The original DataFrame fetched from the database

    Returns:
        Number of rows updated
    """
    cursor = conn.cursor()
    updated_rows_count = 0

    changed_df = updated_df.filter(pl.col("sqlmodded") > 0)

    if changed_df.is_empty():
        return 0

    conn.execute("BEGIN TRANSACTION")
    try:
        for record in changed_df.to_dicts():
            rowid = record.get("rowid")
            if rowid is not None:
                original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)
                updated_row = record
                update_columns = [
                    col for col in updated_row
                    if col not in ["rowid", "sqlmodded"]
                    and updated_row[col] is None
                    and original_row.get(col) is not None
                ]
                if update_columns:
                    set_clause = ", ".join(f"\"{col}\" = ?" for col in update_columns)
                    update_query = f"UPDATE alib SET {set_clause}, sqlmodded = ? WHERE rowid = ?"
                    update_values = [updated_row[col] for col in update_columns] + [updated_row["sqlmodded"], rowid]
                    cursor.execute(update_query, update_values)
                    updated_rows_count += cursor.rowcount
        conn.commit()
        return updated_rows_count
    except Exception as e:
        conn.rollback()
        logging.error(f"Error writing updates to database: {str(e)}")
        raise e

def main():
    # Define fixed columns
    fixed_columns = [
        "__path", "__dirpath", "__filename", "__filename_no_ext", "__ext", "__accessed",
        "__app", "__bitrate", "__bitspersample", "__bitrate_num", "__frequency_num",
        "__frequency", "__channels", "__created", "__dirname", "__file_access_date",
        "__file_access_datetime", "__file_access_datetime_raw", "__file_create_date",
        "__file_create_datetime", "__file_create_datetime_raw", "__file_mod_date",
        "__file_mod_datetime", "__file_mod_datetime_raw", "__file_size", "__file_size_bytes",
        "__file_size_kb", "__file_size_mb", "__filetype", "__image_mimetype", "__image_type",
        "__layer", "__length", "__length_seconds", "__mode", "__modified", "__num_images",
        "__parent_dir", "__size", "__tag", "__tag_read", "__version", "__vendorstring",
        "__md5sig", "tagminder_uuid", "sqlmodded", "reflac", "discnumber", "track", "title",
        "subtitle", "artist", "composer", "arranger", "lyricist", "writer", "albumartist",
        "discsubtitle", "album", "live", "version", "_releasecomment", "work", "movement",
        "part", "ensemble", "performer", "personnel", "conductor", "engineer", "producer",
        "mixer", "remixer", "releasetype", "year", "originaldate", "originalreleasedate",
        "originalyear", "genre", "style", "mood", "theme", "rating", "compilation", "bootleg",
        "label", "amgtagged", "amg_album_id", "amg_boxset_url", "amg_url",
        "musicbrainz_albumartistid", "musicbrainz_albumid", "musicbrainz_artistid",
        "musicbrainz_composerid", "musicbrainz_discid", "musicbrainz_engineerid",
        "musicbrainz_producerid", "musicbrainz_releasegroupid", "musicbrainz_releasetrackid",
        "musicbrainz_trackid", "musicbrainz_workid", "lyrics", "unsyncedlyrics",
        "performancedate", "acousticbrainz_mood", "acoustid_fingerprint", "acoustid_id",
        "analysis", "asin", "barcode", "catalog", "catalognumber", "isrc", "media", "country",
        "discogs_artist_url", "discogs_release_url", "fingerprint", "recordinglocation",
        "recordingstartdate", "replaygain_album_gain", "replaygain_album_peak",
        "replaygain_track_gain", "replaygain_track_peak", "review", "roonalbumtag", "roonid",
        "roonradioban", "roontracktag", "upc", "__albumgain"
    ]

    # Database path
    db_path = '/tmp/amg/dbtemplate.db'

    # Logging setup
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Connect to the database
    conn = sqlite3.connect(db_path)

    try:
        # Fetch all data from the table
        logging.info("Fetching all data from alib table...")
        tracks_df = sqlite_to_polars(
            conn,
            "SELECT rowid, *, 0 AS sqlmodded FROM alib",
            id_column="rowid"
        )
        original_tracks_df = tracks_df.clone() # Keep a copy of the original DataFrame

        # Log initial dataframe info
        logging.info(f"Loaded DataFrame with {tracks_df.height} rows and {len(tracks_df.columns)} columns")

        # Clean up the dataframe
        logging.info("Cleaning up dataframe...")
        updated_tracks, changes_by_column = cleanup_dataframe(tracks_df, fixed_columns)

        # Report on changes
        total_rows_changed = updated_tracks.filter(pl.col("sqlmodded") > 0).height
        logging.info(f"Total number of rows with changes: {total_rows_changed}")
        logging.info("Number of changes by column:")
        for col, count in changes_by_column.items():
            logging.info(f"  - {col}: {count}")

        # Write updates back to database
        if total_rows_changed > 0:
            logging.info("Writing updates back to database...")
            updated_count = write_updates_to_db(conn, updated_tracks, original_tracks_df) # Corrected function call
            logging.info(f"Successfully updated {updated_count} rows in the database")
        else:
            logging.info("No changes detected (no non-fixed columns with non-null values), database not updated")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        # Always close the connection
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
