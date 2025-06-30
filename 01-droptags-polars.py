"""
Script Name: droptags-polars.py

Purpose:
    This script processes all records in alib and sets tag values to null for all unauthorised tags
    (i.e. any tagnames that don't appear in fixed_columns defined in main()
    Logs changes to changes table.

    It is the de-facto way of getting rid of tags you don't want in your music collection.

    It is part of tagminder.

Usage:
    python droptags-polars.py
    uv run droptags-polars.py

Author: audiomuze
Created: 2025-04-13
Updated: 2025-4-21
"""

import polars as pl
import sqlite3
import logging
from typing import List, Dict, Tuple
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if id_column and col_name == id_column:
            data[col_name] = pl.Series(col_data, dtype=pl.Int64)
        elif col_name == "sqlmodded":
            # Convert string numbers like "3" to integers
            cleaned = [int(x) if isinstance(x, str) and x.isdigit() else x for x in col_data]
            data[col_name] = pl.Series(cleaned, dtype=pl.Int64)
        else:
            data[col_name] = [x if x is not None else None for x in col_data]

    return pl.DataFrame(data)



def cleanup_dataframe(
    df: pl.DataFrame,
    fixed_columns: List[str]
) -> Tuple[Dict[int, int], List[Tuple[int, str, str, None]], List[Tuple[str, int]], Dict[str, int]]:
    columns_to_drop = [col for col in df.columns if col not in fixed_columns and col not in ("rowid", "sqlmodded")]

    change_log: List[Tuple[int, str, str, None]] = []
    null_updates: List[Tuple[str, int]] = []
    rowid_mod_map: Dict[int, int] = {}
    changes_by_column: Dict[str, int] = {}

    for col in columns_to_drop:
        series = df[col]
        for rowid, value in zip(df["rowid"], series):
            if value is not None:
                rowid_int = int(rowid)
                change_log.append((rowid_int, col, value, None))
                null_updates.append((col, rowid_int))
                rowid_mod_map[rowid_int] = rowid_mod_map.get(rowid_int, 0) + 1
                changes_by_column[col] = changes_by_column.get(col, 0) + 1

    return rowid_mod_map, change_log, null_updates, changes_by_column



def write_updates_to_db(
    conn: sqlite3.Connection,
    rowid_mod_map: Dict[int, int],
    change_log: List[Tuple[int, str, str, None]],
    null_updates: List[Tuple[str, int]]
) -> int:
    cursor = conn.cursor()
    updated_rows_count = len(rowid_mod_map)

    if updated_rows_count == 0:
        return 0

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)
    timestamp = datetime.now(timezone.utc).isoformat()

    conn.execute("BEGIN TRANSACTION")
    try:
        # 1. Nullify unwanted values
        updates_by_column: Dict[str, List[int]] = {}
        for col, rowid in null_updates:
            updates_by_column.setdefault(col, []).append(rowid)

        for col, rowids in updates_by_column.items():
            cursor.executemany(
                f'UPDATE alib SET "{col}" = NULL WHERE rowid = ?',
                [(rowid,) for rowid in rowids]
            )

        # 2. Update sqlmodded
        cursor.executemany(
            "UPDATE alib SET sqlmodded = COALESCE(sqlmodded, 0) + ? WHERE rowid = ?",
            [(mod_count, rowid) for rowid, mod_count in rowid_mod_map.items()]
        )

        # 3. Write changelog
        cursor.executemany(
            "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            [(rowid, col, old, new, timestamp, "droptags-polars.py") for (rowid, col, old, new) in change_log]
        )

        conn.commit()
        return updated_rows_count
    except Exception as e:
        conn.rollback()
        logging.error(f"Error writing updates to database: {str(e)}")
        raise



def main():
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
        "__md5sig", "tagminder_uuid", "sqlmodded", "reflac", "disc", "discnumber", "track", "title",
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
        "roonradioban", "roontracktag", "upc", "__albumgain", "album_dr", "bliss_analysis"
    ]

    db_path = '/tmp/amg/dbtemplate.db'
    conn = sqlite3.connect(db_path)

    try:
        logging.info("Fetching all data from alib table...")

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(alib)")
        all_columns = [row[1] for row in cursor.fetchall()]
        data_columns = [col for col in all_columns if not col.startswith("__") and col != "sqlmodded"]

        column_clause = ", ".join(f'"{col}"' for col in data_columns)
        query = f"""
            SELECT rowid, COALESCE(sqlmodded, 0) AS sqlmodded, {column_clause}
            FROM alib
        """

        tracks_df = sqlite_to_polars(conn, query, id_column="rowid")
        logging.info(f"Loaded DataFrame with {tracks_df.height} rows and {len(tracks_df.columns)} columns")

        logging.info("Cleaning up dataframe...")
        rowid_mod_map, change_log, null_updates, changes_by_column = cleanup_dataframe(tracks_df, fixed_columns)
        total_rows_changed = len(rowid_mod_map)
        logging.info(f"Total number of rows with changes: {total_rows_changed}")
        logging.info("Number of changes by column:")
        for col, count in changes_by_column.items():
            logging.info(f"  - {col}: {count}")

        if total_rows_changed > 0:
            logging.info("Writing updates back to database...")
            logging.info(f"Rows flagged for update: {total_rows_changed}")
            updated_count = write_updates_to_db(conn, rowid_mod_map, change_log, null_updates)
            logging.info(f"Successfully updated {updated_count} rows in the database and logged {len(change_log)} changes.")
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
