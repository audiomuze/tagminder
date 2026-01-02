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

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_script_name(with_extension=True):
    """
    Returns the name of the currently executing script.

    Args:
        with_extension (bool): If True, returns the script name with its file extension.
                               If False, returns the name without the extension.
    """
    script_path = sys.argv[0]
    script_name = os.path.basename(script_path)
    if not with_extension:
        script_name = os.path.splitext(script_name)[0]
    return script_name


def sqlite_to_polars(
    conn: sqlite3.Connection, query: str, id_column: str = None
) -> pl.DataFrame:
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
            cleaned = [
                int(x) if isinstance(x, str) and x.isdigit() else x for x in col_data
            ]
            data[col_name] = pl.Series(cleaned, dtype=pl.Int64)
        else:
            data[col_name] = [x if x is not None else None for x in col_data]

    return pl.DataFrame(data)


def merge_columns_before_cleanup(
    df: pl.DataFrame,
) -> Tuple[pl.DataFrame, List[Tuple[int, str, str, str]]]:
    """
    Merge involvedpeople with personnel and author with composer before cleanup.
    Also copy itunesadvisory to explicit if itunesadvisory = '1'
    Returns updated dataframe and list of merge changes for logging.
    """
    merge_changes: List[Tuple[int, str, str, str]] = []
    df_updated = df.clone()

    # Handle iTunes advisory -> explicit mapping
    if "itunesadvisory" in df.columns:
        # Ensure explicit column exists
        if "explicit" not in df.columns:
            df_updated = df_updated.with_columns(pl.lit(None).alias("explicit"))

        for i in range(df.height):
            rowid = int(df_updated["rowid"][i])
            itunesadvisory_val = df_updated["itunesadvisory"][i]
            explicit_val = df_updated["explicit"][i]

            # Convert iTunes advisory to explicit flag
            if (
                itunesadvisory_val is not None
                and str(itunesadvisory_val).strip() == "1"
            ):
                new_explicit = "1"  # Set to '1' to match iTunes convention

                # Only update if the value is different
                if str(explicit_val or "").lower() != new_explicit:
                    df_updated = df_updated.with_columns(
                        pl.when(pl.col("rowid") == rowid)
                        .then(pl.lit(new_explicit))
                        .otherwise(pl.col("explicit"))
                        .alias("explicit")
                    )
                    merge_changes.append(
                        (rowid, "explicit", str(explicit_val or ""), new_explicit)
                    )

    # Handle involvedpeople -> personnel merge
    if "involvedpeople" in df.columns:
        # Ensure personnel column exists
        if "personnel" not in df.columns:
            df_updated = df_updated.with_columns(pl.lit(None).alias("personnel"))

        for i in range(df.height):
            rowid = int(df_updated["rowid"][i])
            involvedpeople_val = df_updated["involvedpeople"][i]
            personnel_val = df_updated["personnel"][i]

            if involvedpeople_val is not None and str(involvedpeople_val).strip() != "":
                if personnel_val is None or str(personnel_val).strip() == "":
                    new_personnel = str(involvedpeople_val).strip()
                else:
                    personnel_str = str(personnel_val).strip()
                    involvedpeople_str = str(involvedpeople_val).strip()
                    if involvedpeople_str not in personnel_str:
                        new_personnel = f"{personnel_str}\\\\{involvedpeople_str}"
                    else:
                        new_personnel = personnel_str

                if new_personnel != str(personnel_val or ""):
                    df_updated = df_updated.with_columns(
                        pl.when(pl.col("rowid") == rowid)
                        .then(pl.lit(new_personnel))
                        .otherwise(pl.col("personnel"))
                        .alias("personnel")
                    )
                    merge_changes.append(
                        (rowid, "personnel", str(personnel_val or ""), new_personnel)
                    )

    # Handle author -> composer merge
    if "author" in df.columns:
        # Ensure composer column exists
        if "composer" not in df.columns:
            df_updated = df_updated.with_columns(pl.lit(None).alias("composer"))

        for i in range(df.height):
            rowid = int(df_updated["rowid"][i])
            author_val = df_updated["author"][i]
            composer_val = df_updated["composer"][i]

            if author_val is not None and str(author_val).strip() != "":
                if composer_val is None or str(composer_val).strip() == "":
                    new_composer = str(author_val).strip()
                else:
                    composer_str = str(composer_val).strip()
                    author_str = str(author_val).strip()
                    if author_str not in composer_str:
                        new_composer = f"{composer_str}\\\\{author_str}"
                    else:
                        new_composer = composer_str

                if new_composer != str(composer_val or ""):
                    df_updated = df_updated.with_columns(
                        pl.when(pl.col("rowid") == rowid)
                        .then(pl.lit(new_composer))
                        .otherwise(pl.col("composer"))
                        .alias("composer")
                    )
                    merge_changes.append(
                        (rowid, "composer", str(composer_val or ""), new_composer)
                    )

    # Handle "musicbrainz album type" -> releasetype merge
    if "musicbrainz album type" in df.columns:
        # Ensure releasetype column exists
        if "releasetype" not in df.columns:
            df_updated = df_updated.with_columns(pl.lit(None).alias("releasetype"))

        for i in range(df.height):
            rowid = int(df_updated["rowid"][i])
            musicbrainz_album_type_val = df_updated["musicbrainz album type"][i]
            releasetype_val = df_updated["releasetype"][i]

            if (
                musicbrainz_album_type_val is not None
                and str(musicbrainz_album_type_val).strip() != ""
            ):
                if releasetype_val is None or str(releasetype_val).strip() == "":
                    new_releasetype = str(musicbrainz_album_type_val).strip()
                else:
                    releasetype_str = str(releasetype_val).strip()
                    musicbrainz_album_type_str = str(musicbrainz_album_type_val).strip()
                    if musicbrainz_album_type_str not in releasetype_str:
                        new_releasetype = (
                            f"{releasetype_str}\\\\{musicbrainz_album_type_str}"
                        )
                    else:
                        new_releasetype = releasetype_str

                if new_releasetype != str(releasetype_val or ""):
                    df_updated = df_updated.with_columns(
                        pl.when(pl.col("rowid") == rowid)
                        .then(pl.lit(new_releasetype))
                        .otherwise(pl.col("releasetype"))
                        .alias("releasetype")
                    )
                    merge_changes.append(
                        (
                            rowid,
                            "releasetype",
                            str(releasetype_val or ""),
                            new_releasetype,
                        )
                    )

    # Handle description -> review merge
    if "description" in df.columns:
        # Ensure review column exists
        if "review" not in df.columns:
            df_updated = df_updated.with_columns(pl.lit(None).alias("review"))

        for i in range(df.height):
            rowid = int(df_updated["rowid"][i])
            description_val = df_updated["description"][i]
            review_val = df_updated["review"][i]

            if description_val is not None and str(description_val).strip() != "":
                if review_val is None or str(review_val).strip() == "":
                    new_review = str(description_val).strip()
                else:
                    review_str = str(review_val).strip()
                    description_str = str(description_val).strip()
                    if description_str not in review_str:
                        new_review = f"{review_str}\\\\{description_str}"
                    else:
                        new_review = review_str

                if new_review != str(review_val or ""):
                    df_updated = df_updated.with_columns(
                        pl.when(pl.col("rowid") == rowid)
                        .then(pl.lit(new_review))
                        .otherwise(pl.col("review"))
                        .alias("review")
                    )
                    merge_changes.append(
                        (rowid, "review", str(review_val or ""), new_review)
                    )

    return df_updated, merge_changes


def cleanup_dataframe(
    df: pl.DataFrame, fixed_columns: List[str]
) -> Tuple[
    Dict[int, int],
    List[Tuple[int, str, str, None]],
    List[Tuple[str, int]],
    Dict[str, int],
]:
    columns_to_drop = [
        col
        for col in df.columns
        if col not in fixed_columns and col not in ("rowid", "sqlmodded")
    ]

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
    null_updates: List[Tuple[str, int]],
    merge_changes: List[Tuple[int, str, str, str]],
) -> int:
    cursor = conn.cursor()
    updated_rows_count = len(rowid_mod_map)

    # Include merge changes in the total updated rows count
    all_updated_rowids = set(rowid_mod_map.keys())
    all_updated_rowids.update(rowid for rowid, _, _, _ in merge_changes)
    total_updated_rows = len(all_updated_rowids)

    if total_updated_rows == 0:
        return 0

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            alib_column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)
    timestamp = datetime.now(timezone.utc).isoformat()

    conn.execute("BEGIN TRANSACTION")
    try:
        # 1. Apply merge changes first
        for rowid, column, old_value, new_value in merge_changes:
            cursor.execute(
                f'UPDATE alib SET "{column}" = ? WHERE rowid = ?', (new_value, rowid)
            )

        # 2. Nullify unwanted values
        updates_by_column: Dict[str, List[int]] = {}
        for col, rowid in null_updates:
            updates_by_column.setdefault(col, []).append(rowid)

        for col, rowids in updates_by_column.items():
            cursor.executemany(
                f'UPDATE alib SET "{col}" = NULL WHERE rowid = ?',
                [(rowid,) for rowid in rowids],
            )

        # 3. Update sqlmodded for drop changes
        if rowid_mod_map:
            cursor.executemany(
                "UPDATE alib SET sqlmodded = COALESCE(sqlmodded, 0) + ? WHERE rowid = ?",
                [(mod_count, rowid) for rowid, mod_count in rowid_mod_map.items()],
            )

        # 4. Update sqlmodded for merge changes (increment by 1 for each merge)
        merge_mod_counts = {}
        for rowid, _, _, _ in merge_changes:
            merge_mod_counts[rowid] = merge_mod_counts.get(rowid, 0) + 1

        if merge_mod_counts:
            cursor.executemany(
                "UPDATE alib SET sqlmodded = COALESCE(sqlmodded, 0) + ? WHERE rowid = ?",
                [(mod_count, rowid) for rowid, mod_count in merge_mod_counts.items()],
            )

        # 5. Write changelog for both merge and drop changes
        all_changes = []

        # Add merge changes to changelog
        for rowid, column, old_value, new_value in merge_changes:
            all_changes.append(
                (rowid, column, old_value, new_value, timestamp, get_script_name())
            )

        # Add drop changes to changelog
        for rowid, col, old, new in change_log:
            all_changes.append((rowid, col, old, new, timestamp, get_script_name()))

        if all_changes:
            cursor.executemany(
                "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                all_changes,
            )

        conn.commit()
        return total_updated_rows
    except Exception as e:
        conn.rollback()
        logging.error(f"Error writing updates to database: {str(e)}")
        raise


def main():
    fixed_columns = [
        "__path",
        "__dirpath",
        "__filename",
        "__filename_no_ext",
        "__ext",
        "__accessed",
        "__app",
        "__bitrate",
        "__bitspersample",
        "__bitrate_num",
        "__frequency_num",
        "__frequency",
        "__channels",
        "__created",
        "__dirname",
        "__file_access_date",
        "__file_access_datetime",
        "__file_access_datetime_raw",
        "__file_create_date",
        "__file_create_datetime",
        "__file_create_datetime_raw",
        "__file_mod_date",
        "__file_mod_datetime",
        "__file_mod_datetime_raw",
        "__file_size",
        "__file_size_bytes",
        "__file_size_kb",
        "__file_size_mb",
        "__filetype",
        "__image_mimetype",
        "__image_type",
        "__layer",
        "__length",
        "__length_seconds",
        "__mode",
        "__modified",
        "__num_images",
        "__parent_dir",
        "__size",
        "__tag",
        "__tag_read",
        "__version",
        "__vendorstring",
        "__md5sig",
        "songkong_id",
        "tagminder_uuid",
        "sqlmodded",
        "reflac",
        "disc",
        "discnumber",
        "track",
        "title",
        "subtitle",
        "explicit",
        "artist",
        "composer",
        "arranger",
        "lyricist",
        "writer",
        "albumartist",
        "discsubtitle",
        "album",
        "live",
        "version",
        "_releasecomment",
        "work",
        "movement",
        "part",
        "ensemble",
        "performer",
        "personnel",
        "conductor",
        "orchestra",
        "engineer",
        "producer",
        "mixer",
        "remixer",
        "releasetype",
        "year",
        "originaldate",
        "originalreleasedate",
        "originalyear",
        "genre",
        "style",
        "mood",
        "theme",
        "rating",
        "compilation",
        "bootleg",
        "label",
        "amgtagged",
        "amg_album_id",
        "amg_boxset_url",
        "amg_url",
        "musicbrainz_albumartistid",
        "musicbrainz_albumid",
        "musicbrainz_artistid",
        "musicbrainz_composerid",
        "musicbrainz_discid",
        "musicbrainz_engineerid",
        "musicbrainz_producerid",
        "musicbrainz_releasegroupid",
        "musicbrainz_releasetrackid",
        "musicbrainz_trackid",
        "musicbrainz_workid",
        "lyrics",
        "unsyncedlyrics",
        "performancedate",
        "acousticbrainz_mood",
        "acoustid_fingerprint",
        "acoustid_id",
        "analysis",
        "asin",
        "barcode",
        "catalog",
        "catalognumber",
        "isrc",
        "media",
        "country",
        "discogs_artist_url",
        "discogs_release_url",
        "fingerprint",
        "recordinglocation",
        "recordingstartdate",
        "replaygain_album_gain",
        "replaygain_album_peak",
        "replaygain_track_gain",
        "replaygain_track_peak",
        "review",
        "roonalbumtag",
        "roonid",
        "roonradioban",
        "roontracktag",
        "upc",
        "__albumgain",
        "album_dr",
        "bliss_analysis",
    ]

    db_path = "/tmp/amg/dbtemplate.db"
    conn = sqlite3.connect(db_path)

    try:
        logging.info("Fetching all data from alib table...")

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(alib)")
        all_columns = [row[1] for row in cursor.fetchall()]
        data_columns = [
            col
            for col in all_columns
            if not col.startswith("__") and col != "sqlmodded"
        ]

        column_clause = ", ".join(f'"{col}"' for col in data_columns)
        query = f"""
            SELECT rowid, COALESCE(sqlmodded, 0) AS sqlmodded, {column_clause}
            FROM alib
        """

        tracks_df = sqlite_to_polars(conn, query, id_column="rowid")
        logging.info(
            f"Loaded DataFrame with {tracks_df.height} rows and {len(tracks_df.columns)} columns"
        )

        logging.info("Processing column merges...")
        tracks_df, merge_changes = merge_columns_before_cleanup(tracks_df)
        if merge_changes:
            merge_counts = {}
            for _, column, _, _ in merge_changes:
                merge_counts[column] = merge_counts.get(column, 0) + 1
            logging.info(f"Merged {len(merge_changes)} values:")
            for column, count in merge_counts.items():
                logging.info(f"  - {column}: {count} merges")

        logging.info("Cleaning up dataframe...")
        rowid_mod_map, change_log, null_updates, changes_by_column = cleanup_dataframe(
            tracks_df, fixed_columns
        )
        total_rows_changed = len(rowid_mod_map)
        logging.info(f"Total number of rows with drop changes: {total_rows_changed}")
        logging.info("Number of drop changes by column:")
        for col, count in changes_by_column.items():
            logging.info(f"  - {col}: {count}")

        if total_rows_changed > 0 or merge_changes:
            logging.info("Writing updates back to database...")
            all_updated_rowids = set(rowid_mod_map.keys())
            all_updated_rowids.update(rowid for rowid, _, _, _ in merge_changes)
            logging.info(f"Rows flagged for update: {len(all_updated_rowids)}")
            updated_count = write_updates_to_db(
                conn, rowid_mod_map, change_log, null_updates, merge_changes
            )
            total_changes = len(change_log) + len(merge_changes)
            logging.info(
                f"Successfully updated {updated_count} rows in the database and logged {total_changes} changes."
            )
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
