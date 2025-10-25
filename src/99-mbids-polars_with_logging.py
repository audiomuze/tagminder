"""
Script Name: 99-mbids-polars_with_logging.py

Purpose:
    This script processes all records in alib and updates mbid's related to

        'artist': 'musicbrainz_artistid',
        'albumartist': 'musicbrainz_albumartistid',
        'composer': 'musicbrainz_composerid',
        'engineer': 'musicbrainz_engineerid',
        'producer': 'musicbrainz_producerid'

    adding mbid's where missing and ensuring that mbid's appear in the same order as the urelated contributor tag.
    Where there is no matching MBID it writes the value '' (add '' so Mutagen writes an empty tag and Lyrion inteprets it as a blank delimited value) into the relevant mbid tag.
    This provides the ability to easily identify contributors that need to be added to MusicBrainz to generate a MBID

    It is the de-facto way of ensuring contributors and associated mbid's are accurately recorded in your tags throughout
    your music collection.

    It is part of tagminder.

Usage:
    python 99-mbids-polars_with_logging.py
    uv run 99-mbids-polars_with_logging.py

Author: audiomuze
Created: 2025-04-18
Updated: 2025-06-01 - Added comprehensive logging and changelog support
"""

import polars as pl
import sqlite3
import logging
from collections import defaultdict
from typing import List, Dict, Set, Optional, Tuple, Any
from datetime import datetime, timezone
import numpy as np
import unicodedata

# ---------- Logging ----------
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# def normalize_entity_series(series: pl.Series) -> pl.Series:
#     """Consistent vectorized normalization for all text processing"""
#     return (
#         series.str.to_lowercase()
#         .str.replace_all(r"^\s+|\s+$", "")
#         .str.replace('"', '')
#         .map_elements(
#             lambda x: unicodedata.normalize('NFKD', x) if x else None,
#             return_dtype=pl.Utf8
#         )
#     )


# def normalize_text_series(series: pl.Series) -> pl.Series:
#     """Vectorized normalization with type guards"""
#     if series.dtype != pl.Utf8:
#         raise TypeError(f"Expected pl.Utf8, got {series.dtype}")

#     return (
#         series.str.to_lowercase()
#         .str.strip()
#         .str.replace_all(r"\s+", " ")  # Normalize internal whitespace
#         .map_elements(
#             lambda x: unicodedata.normalize('NFKD', x) if x else None,
#             return_dtype=pl.Utf8  # Preserve type
#         )
#     )


def normalize_string(text: str) -> str:
    """
    A single, consistent normalization function for all entity names.
    Handles case, whitespace, quotes, and diacritics.
    """
    if not isinstance(text, str):
        return ""
    # 1. Normalize unicode characters (diacritics)
    text = unicodedata.normalize("NFKD", text)
    # 2. Convert to lowercase
    text = text.lower()
    # 3. Remove double quotes
    text = text.replace('"', "")
    # 4. Normalize all whitespace (leading, trailing, and internal)
    text = " ".join(text.split())
    return text


def load_dataframes(conn: sqlite3.Connection) -> Tuple[Dict[str, str], int]:
    """
    Load and normalize MusicBrainz reference data with strict typing
    Modified to use the centralized normalize_entity_series function
    """
    logging.info("Loading contributors dictionary with vectorized normalization...")

    # 1. Load reference data with strict UTF-8 typing
    df_contributors = pl.read_database(
        "SELECT entity, mbid FROM _REF_mb_disambiguated",
        conn,
        schema_overrides={"entity": pl.Utf8, "mbid": pl.Utf8},
    )

    # 2. Apply consistent vectorized normalization
    df_contributors = df_contributors.with_columns(
        pl.col("entity")
        .map_elements(normalize_string, return_dtype=pl.Utf8)
        .alias("norm_entity")
    )
    # 3. Create optimized lookup dictionary
    contributors_dict = dict(
        zip(df_contributors["norm_entity"].to_list(), df_contributors["mbid"].to_list())
    )
    logging.info(f"Loaded {len(contributors_dict)} normalized contributors")

    # 4. Get total rows count
    total_rows = conn.execute("SELECT COUNT(*) FROM alib").fetchone()[0]
    logging.info(f"Total alib rows: {total_rows}")

    return contributors_dict, total_rows


def setup_changelog_table(conn: sqlite3.Connection):
    """
    Ensure changelog table exists for tracking changes

    Args:
        conn: SQLite database connection
    """
    cursor = conn.cursor()
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
    logging.info("Changelog table ready")


def log_change_to_changelog(
    cursor: sqlite3.Cursor,
    rowid: int,
    column: str,
    old_value: Optional[str],
    new_value: str,
    timestamp: str,
    script_name: str,
):
    """
    Log a single change to the changelog table

    Args:
        cursor: SQLite cursor
        rowid: Row ID being changed
        column: Column name being changed
        old_value: Previous value (can be None)
        new_value: New value
        timestamp: Timestamp of change
        script_name: Name of script making change
    """
    cursor.execute(
        "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
        (rowid, column, old_value, new_value, timestamp, script_name),
    )


# def process_chunk(
#     conn: sqlite3.Connection,
#     contributors_dict: Dict[str, str],
#     offset: int,
#     chunk_size: int,
# ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
#     """
#     Process a chunk of records with vectorized MBID matching

#     Args:
#         conn: Database connection
#         contributors_dict: Normalized {entity: mbid} mapping
#         offset: Chunk starting position
#         chunk_size: Number of rows to process

#     Returns:
#         Tuple of (updates, statistics)
#     """
#     logging.info(
#         f"Processing chunk {offset}-{offset + chunk_size} with vectorized operations"
#     )

#     # 1. Define field mappings and schema
#     fields = {
#         "artist": "musicbrainz_artistid",
#         "albumartist": "musicbrainz_albumartistid",
#         "composer": "musicbrainz_composerid",
#         "engineer": "musicbrainz_engineerid",
#         "producer": "musicbrainz_producerid",
#     }

#     schema = {
#         "rowid": pl.Int64,
#         "artist": pl.Utf8,
#         "albumartist": pl.Utf8,
#         "composer": pl.Utf8,
#         "engineer": pl.Utf8,
#         "producer": pl.Utf8,
#         "musicbrainz_artistid": pl.Utf8,
#         "musicbrainz_albumartistid": pl.Utf8,
#         "musicbrainz_composerid": pl.Utf8,
#         "musicbrainz_engineerid": pl.Utf8,
#         "musicbrainz_producerid": pl.Utf8,
#         "sqlmodded": pl.Int64,
#     }

#     # 2. Load chunk with consistent vectorized normalization
#     query = f"""
#         SELECT rowid, artist, albumartist, composer, engineer, producer,
#                musicbrainz_artistid, musicbrainz_albumartistid,
#                musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid,
#                COALESCE(sqlmodded, 0) AS sqlmodded
#         FROM alib
#         WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR
#               composer IS NOT NULL OR engineer IS NOT NULL OR
#               producer IS NOT NULL)
#         ORDER BY rowid
#         LIMIT {chunk_size} OFFSET {offset}
#     """

#     df = pl.read_database(query, conn, schema_overrides=schema).with_columns(
#         *[
#             normalize_entity_series(pl.col(field)).alias(f"norm_{field}")
#             for field in fields.keys()
#         ]
#     )

#     # 3. Vectorized MBID matching
#     updates = []
#     stats = {"additions": defaultdict(int), "corrections": defaultdict(int)}

#     for field, mbid_field in fields.items():
#         norm_col = f"norm_{field}"
#         current_col = mbid_field

#         # Split, lookup, and join MBIDs
#         # df = df.with_columns(
#         #             pl.col(norm_col)
#         #             .str.split("\\\\")
#         #             .list.eval(
#         #                 pl.element()
#         #                 .map_elements(
#         #                     lambda x: contributors_dict.get(
#         #                         unicodedata.normalize('NFKD', x.lower().strip().replace('"', '')),
#         #                         ""  # Default if not found
#         #                     ),
#         #                     return_dtype=pl.Utf8
#         #                 )
#         #             )
#         #             .list.join("\\\\")
#         #             .alias(f"new_{mbid_field}")
#         #         )
#         df = df.with_columns(
#             pl.col(norm_col)
#             .str.split("\\\\")
#             .list.eval(
#                 pl.element().map_elements(
#                     lambda x: contributors_dict.get(
#                         unicodedata.normalize(
#                             "NFKD", x.lower().strip().replace('"', "")
#                         ),
#                         "",  # Default if not found
#                     ),
#                     return_dtype=pl.Utf8,
#                 )
#             )
#             .list.join("\\\\")
#             .map_elements(
#                 lambda x: None
#                 if x.replace("\\\\", "") == ""
#                 else x,  # Set to None if all empty
#                 return_dtype=pl.Utf8,
#             )
#             .alias(f"new_{mbid_field}")
#         )
#         # Enhanced change detection to handle empty/quote cases
#         df = df.with_columns(
#             (pl.col(f"new_{mbid_field}") != pl.col(current_col))
#             & (pl.col(f"new_{mbid_field}").is_not_null())
#             & ~(  # Not the case where current is just quotes
#                 pl.col(current_col).str.replace('"', "").is_null()
#                 & pl.col(f"new_{mbid_field}").eq("")
#             ).alias(f"change_{mbid_field}")
#         )

#         # Update stats - handle both NULL and "" cases as additions
#         change_counts = (
#             df.filter(pl.col(f"change_{mbid_field}"))
#             .group_by(
#                 (
#                     pl.col(current_col).is_null()
#                     | (pl.col(current_col).str.replace('"', "").is_null())
#                 ).alias("was_empty")
#             )
#             .agg(pl.count().alias("count"))
#             .to_dicts()
#         )

#         for count in change_counts:
#             if count["was_empty"]:
#                 stats["additions"][field] += count["count"]
#             else:
#                 stats["corrections"][field] += count["count"]

#     # 4. Prepare updates for changed rows
#     changed_rows = df.filter(
#         sum(
#             pl.col(f"change_{mbid_field}").cast(pl.Int8)
#             for mbid_field in fields.values()
#         )
#         > 0
#     )

#     if changed_rows.height > 0:
#         updates = changed_rows.select(
#             [
#                 "rowid",
#                 *[f"new_{mbid_field}" for mbid_field in fields.values()],
#                 (
#                     pl.col("sqlmodded")
#                     + pl.sum_horizontal(
#                         [
#                             pl.col(f"change_{mbid_field}").cast(pl.Int8)
#                             for mbid_field in fields.values()
#                         ]
#                     )
#                 ).alias("sqlmodded"),
#             ]
#         ).to_dicts()

#     logging.info(f"Chunk complete: {len(updates)} updates, {stats}")
#     return updates, stats


def process_chunk(
    conn: sqlite3.Connection,
    contributors_dict: Dict[str, str],
    offset: int,
    chunk_size: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    """
    Process a chunk of records with vectorized MBID matching

    Args:
        conn: Database connection
        contributors_dict: Normalized {entity: mbid} mapping
        offset: Chunk starting position
        chunk_size: Number of rows to process

    Returns:
        Tuple of (updates, statistics)
    """
    logging.info(
        f"Processing chunk {offset}-{offset + chunk_size} with vectorized operations"
    )

    # 1. Define field mappings and schema
    fields = {
        "artist": "musicbrainz_artistid",
        "albumartist": "musicbrainz_albumartistid",
        "composer": "musicbrainz_composerid",
        "engineer": "musicbrainz_engineerid",
        "producer": "musicbrainz_producerid",
    }

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
        "sqlmodded": pl.Int64,
    }

    # 2. Load chunk with consistent vectorized normalization
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

    df = pl.read_database(query, conn, schema_overrides=schema).with_columns(
        *[
            pl.col(field)
            .map_elements(normalize_string, return_dtype=pl.Utf8)
            .alias(f"norm_{field}")
            for field in fields.keys()
        ]
    )

    # 3. Vectorized MBID matching - FIXED VERSION
    updates = []
    stats = {"additions": defaultdict(int), "corrections": defaultdict(int)}

    for field, mbid_field in fields.items():
        norm_col = f"norm_{field}"
        current_col = mbid_field

        # FIXED: Proper MBID matching with correct backslash handling
        df = df.with_columns(
            pl.col(norm_col)
            .str.split("\\\\")
            .list.eval(
                pl.element().map_elements(
                    lambda x: contributors_dict.get(
                        unicodedata.normalize(
                            "NFKD", x.lower().strip().replace('"', "")
                        ),
                        "",  # Explicit empty string for unmatched contributors
                    ),
                    return_dtype=pl.Utf8,
                )
            )
            .map_elements(
                lambda mbids: "\\\\".join(mbids) if mbids else None,
                return_dtype=pl.Utf8,
            )
            .alias(f"new_{mbid_field}")
        )

        # Enhanced change detection to handle empty/quote cases
        df = df.with_columns(
            (pl.col(f"new_{mbid_field}") != pl.col(current_col))
            & (pl.col(f"new_{mbid_field}").is_not_null())
            & ~(  # Not the case where current is just quotes
                pl.col(current_col).str.replace('"', "").is_null()
                & pl.col(f"new_{mbid_field}").eq("")
            ).alias(f"change_{mbid_field}")
        )

        # Update stats - handle both NULL and "" cases as additions
        change_counts = (
            df.filter(pl.col(f"change_{mbid_field}"))
            .group_by(
                (
                    pl.col(current_col).is_null()
                    | (pl.col(current_col).str.replace('"', "").is_null())
                ).alias("was_empty")
            )
            .agg(pl.count().alias("count"))
            .to_dicts()
        )

        for count in change_counts:
            if count["was_empty"]:
                stats["additions"][field] += count["count"]
            else:
                stats["corrections"][field] += count["count"]

    # 4. Prepare updates for changed rows
    changed_rows = df.filter(
        sum(
            pl.col(f"change_{mbid_field}").cast(pl.Int8)
            for mbid_field in fields.values()
        )
        > 0
    )

    if changed_rows.height > 0:
        updates = changed_rows.select(
            [
                "rowid",
                *[f"new_{mbid_field}" for mbid_field in fields.values()],
                (
                    pl.col("sqlmodded")
                    + pl.sum_horizontal(
                        [
                            pl.col(f"change_{mbid_field}").cast(pl.Int8)
                            for mbid_field in fields.values()
                        ]
                    )
                ).alias("sqlmodded"),
            ]
        ).to_dicts()

    logging.info(f"Chunk complete: {len(updates)} updates, {stats}")
    return updates, stats


def write_updates_to_db(
    updates: List[Dict[str, Any]],
    conn: sqlite3.Connection,
    stats: Dict[str, Dict[str, int]],
    batch_size: int = 1000,
):
    """
    Write updates to both temporary table and main table using batching, with changelog logging

    Args:
        updates: List of update dictionaries
        conn: SQLite database connection
        stats: Statistics dictionary
        batch_size: Size of batches for processing
    """
    if not updates:
        logging.info("No updates to write to database")
        return

    logging.info(
        f"Writing {len(updates)} updates to database in batches of {batch_size}"
    )

    cursor = conn.cursor()
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "99-mbids-polars_with_logging.py"

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
    columns = [
        "rowid",
        "musicbrainz_artistid",
        "musicbrainz_albumartistid",
        "musicbrainz_composerid",
        "musicbrainz_engineerid",
        "musicbrainz_producerid",
        "sqlmodded",
    ]

    # Check if a transaction is already active
    cursor.execute("SELECT * FROM sqlite_master LIMIT 0")
    transaction_active = conn.in_transaction

    # Only begin a transaction if one isn't already active
    if not transaction_active:
        conn.execute("BEGIN TRANSACTION")

    try:
        # Prepare statements
        insert_placeholders = ", ".join(["?" for _ in columns])
        insert_query = f"INSERT INTO _TMP_alib_updates ({', '.join(columns)}) VALUES ({insert_placeholders})"

        updates_written = 0

        # Process in batches
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            logging.info(
                f"Processing batch {i // batch_size + 1}: {len(batch)} updates"
            )

            # Insert into temporary table
            for update in batch:
                # Prepare data for insertion with None for missing columns
                insert_data = [update.get("rowid")]
                for col in columns[1:]:  # Skip rowid
                    insert_data.append(update.get(col))

                # Insert into temporary table
                cursor.execute(insert_query, insert_data)

            # Update main table and log changes - only update non-null values
            for update in batch:
                update_cols = []
                update_vals = []
                rowid = update["rowid"]
                old_values = update.get("old_values", {})

                # Only include fields that exist in the update and are not None
                for col in columns[1:]:  # Skip rowid
                    if col in update:
                        if update[col] is not None:
                            update_cols.append(f"{col} = ?")
                            update_vals.append(update[col])

                            # Log change to changelog if it's an MBID field
                            if col.startswith("musicbrainz_"):
                                old_val = old_values.get(col)
                                log_change_to_changelog(
                                    cursor,
                                    rowid,
                                    col,
                                    old_val,
                                    update[col],
                                    timestamp,
                                    script_name,
                                )

                        elif (
                            col == "sqlmodded"
                        ):  # Special handling for sqlmodded to set NULL when 0
                            update_cols.append(f"{col} = NULL")

                if update_cols:  # Only proceed if there are columns to update
                    update_query = (
                        f"UPDATE alib SET {', '.join(update_cols)} WHERE rowid = ?"
                    )
                    cursor.execute(update_query, update_vals + [rowid])
                    updates_written += 1

        logging.info(f"Successfully wrote {updates_written} updates to main table")

        # Only commit if we started the transaction
        if not transaction_active:
            conn.commit()
            logging.info("Transaction committed successfully")

    except Exception as e:
        logging.error(f"Error writing updates: {e}")
        # Rollback on error only if we started the transaction
        if not transaction_active:
            conn.rollback()
            logging.info("Transaction rolled back due to error")
        raise e


def process_database(conn: sqlite3.Connection, chunk_size: int = 50000):
    """
    Process the entire database in chunks

    Args:
        conn: SQLite database connection
        chunk_size: Size of each chunk (increased for Polars)
    """
    logging.info(f"Starting chunked database processing with chunk size: {chunk_size}")

    # Get contributors dictionary and total rows once
    contributors_dict, total_rows = load_dataframes(conn)

    # Setup changelog table
    setup_changelog_table(conn)

    # Initialize statistics
    all_stats = {"additions": {}, "corrections": {}}

    # Start a transaction for all operations
    conn.execute("BEGIN TRANSACTION")
    logging.info("Started database transaction")

    try:
        # Process in chunks and write updates for each chunk immediately
        chunks_processed = 0
        for offset in range(0, total_rows, chunk_size):
            chunks_processed += 1
            logging.info(
                f"Processing chunk {chunks_processed}: rows {offset} to {offset + chunk_size}..."
            )

            # Process the chunk
            chunk_updates, chunk_stats = process_chunk(
                conn, contributors_dict, offset, chunk_size
            )

            # Combine statistics
            for category in ["additions", "corrections"]:
                for field, count in chunk_stats[category].items():
                    all_stats[category][field] = (
                        all_stats[category].get(field, 0) + count
                    )

            # Write updates for this chunk immediately
            if chunk_updates:
                # Pass conn in transaction mode
                write_updates_to_db(chunk_updates, conn, all_stats)
            else:
                logging.info("No updates needed for this chunk")

        # Commit the transaction at the end
        conn.commit()
        logging.info(
            f"All {chunks_processed} chunks processed successfully. Transaction committed."
        )

        # Display final statistics
        display_statistics(all_stats)

    except Exception as e:
        # Rollback on error
        logging.error(f"Error during chunked processing: {e}")
        conn.rollback()
        logging.info("Transaction rolled back due to error")
        raise e


def process_full_database(conn: sqlite3.Connection):
    """
    Process the entire database in one go with Polars

    Args:
        conn: SQLite database connection
    """
    logging.info("Starting full database processing (non-chunked)")

    # Get contributors dictionary
    contributors_dict, _ = load_dataframes(conn)

    # Setup changelog table
    setup_changelog_table(conn)

    # Define field mappings
    fields = {
        "artist": "musicbrainz_artistid",
        "albumartist": "musicbrainz_albumartistid",
        "composer": "musicbrainz_composerid",
        "engineer": "musicbrainz_engineerid",
        "producer": "musicbrainz_producerid",
    }

    # Initialize statistics
    all_stats = {"additions": {}, "corrections": {}}

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
        "sqlmodded": pl.Int64,
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
        logging.info("Loading entire database with Polars...")
        df = pl.read_database(query, conn, schema_overrides=schema)
        logging.info(f"Loaded {df.height} rows for processing")

        # Process full dataset
        updates_by_rowid = {}

        # Start a transaction
        conn.execute("BEGIN TRANSACTION")
        logging.info("Started database transaction")

        # Process each row
        processed_rows = 0
        for row in df.iter_rows(named=True):
            processed_rows += 1
            if processed_rows % 50000 == 0:
                logging.info(f"Processed {processed_rows} rows...")

            changes_in_row = 0  # Track changes for this row to increment sqlmodded

            for field, mbid_field in fields.items():
                value = row[field]
                if value is None:
                    continue

                # Split the value and match entities
                entities = [normalize_string(e) for e in str(value).split("\\\\")]

                # matched_mbids = []
                # for entity in entities:
                #     if entity in contributors_dict:
                #         matched_mbids.append(contributors_dict[entity])
                #     else:
                #         matched_mbids.append("")  # Explicit empty string

                # new_value = "\\\\".join(matched_mbids)
                # # Set to None if all MBIDs are empty strings
                # if new_value.replace("\\\\", "") == "":
                #     new_value = None

                matched_mbids = []
                for entity in entities:
                    if entity in contributors_dict:
                        matched_mbids.append(contributors_dict[entity])
                    else:
                        matched_mbids.append("")  # Explicit empty string

                # Always join with single backslash, never set to None for partial matches
                new_value = "\\\\".join(matched_mbids)

                # Only set to None if there are NO contributors (empty array)
                if not matched_mbids:
                    new_value = None

                # Check current value
                current_mbid = row[mbid_field]
                is_current_empty = current_mbid is None or (
                    isinstance(current_mbid, str)
                    and (current_mbid.strip() == "" or current_mbid.strip() == '""')
                )
                # Determine if this is an addition or correction
                update_needed = False

                if is_current_empty and new_value:
                    # Empty to non-empty = addition
                    field_type = field
                    all_stats["additions"][field_type] = (
                        all_stats["additions"].get(field_type, 0) + 1
                    )
                    update_needed = True
                    changes_in_row += 1
                elif not is_current_empty and new_value != str(current_mbid).strip():
                    # Non-empty to different = correction
                    field_type = field
                    all_stats["corrections"][field_type] = (
                        all_stats["corrections"].get(field_type, 0) + 1
                    )
                    update_needed = True
                    changes_in_row += 1

                if update_needed:
                    rowid = row["rowid"]
                    if rowid not in updates_by_rowid:
                        updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}
                    updates_by_rowid[rowid][mbid_field] = new_value
                    # Store old value for changelog
                    updates_by_rowid[rowid]["old_values"][mbid_field] = current_mbid

            # If there were changes in this row, increment sqlmodded
            if changes_in_row > 0:
                rowid = row["rowid"]
                current_sqlmodded = row["sqlmodded"]
                new_sqlmodded = current_sqlmodded + changes_in_row

                if rowid not in updates_by_rowid:
                    updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}

                # Only include sqlmodded if it's > 0
                if new_sqlmodded > 0:
                    updates_by_rowid[rowid]["sqlmodded"] = new_sqlmodded
                else:
                    # Set to NULL explicitly if 0
                    updates_by_rowid[rowid]["sqlmodded"] = None

        logging.info(f"Completed processing {processed_rows} rows")

        # Write all updates at once
        if updates_by_rowid:
            logging.info(f"Writing {len(updates_by_rowid)} updates to database...")
            write_updates_to_db(
                list(updates_by_rowid.values()), conn, all_stats, batch_size=5000
            )
        else:
            logging.info("No updates needed")

        # Commit the transaction
        conn.commit()
        logging.info("Transaction committed successfully")

        # Display statistics
        display_statistics(all_stats)

    except Exception as e:
        conn.rollback()
        logging.error(f"Error processing database: {e}")
        logging.info("Transaction rolled back due to error")
        raise e


def display_statistics(stats: Dict[str, Dict[str, int]]):
    """Display statistics about the updates"""
    total_additions = sum(stats["additions"].values())
    total_corrections = sum(stats["corrections"].values())
    total_changes = total_additions + total_corrections

    logging.info(f"MusicBrainz ID Update Summary:")
    logging.info(f"==============================")
    logging.info(f"Total changes: {total_changes}")
    logging.info(f"  - New IDs added: {total_additions}")
    logging.info(f"  - Existing IDs corrected: {total_corrections}")

    # Print detailed statistics by field type
    if stats["additions"]:
        logging.info("Additions by field type:")
        for field, count in sorted(stats["additions"].items()):
            logging.info(f"  {field}: {count}")

    if stats["corrections"]:
        logging.info("Corrections by field type:")
        for field, count in sorted(stats["corrections"].items()):
            logging.info(f"  {field}: {count}")


def update_with_polars(file_path: str, use_chunking: bool = False):
    """
    Optimized implementation using Polars for data processing

    Args:
        file_path: Path to the SQLite database
        use_chunking: Whether to use chunking or process entire database at once
    """
    logging.info(f"Starting MBID processing with database: {file_path}")
    logging.info(f"Using chunking: {use_chunking}")

    # Open a single connection
    conn = sqlite3.connect(file_path)

    try:
        if use_chunking:
            # Process in chunks (useful for very large databases or limited memory)
            process_database(conn, chunk_size=50000)  # Increased chunk size for Polars
        else:
            # Process entire database at once (preferred with Polars if memory allows)
            process_full_database(conn)

        logging.info("MBID processing completed successfully")

    except Exception as e:
        logging.error(f"Fatal error during MBID processing: {e}")
        raise
    finally:
        # Close the connection when done
        conn.close()
        logging.info("Database connection closed")


def main():
    """Main function to run the script"""
    db_path = "/tmp/amg/dbtemplate.db"
    logging.info("=== MBID Processing Script Started ===")

    try:
        # Set use_chunking=True if memory constraints are an issue
        update_with_polars(db_path, use_chunking=False)
        logging.info("=== MBID Processing Script Completed Successfully ===")
    except Exception as e:
        logging.error(f"=== MBID Processing Script Failed: {e} ===")
        raise


if __name__ == "__main__":
    main()
