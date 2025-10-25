"""
Script Name: 12-release_type_normalizer.py

Purpose:
    This script processes all records in alib and normalises releaseetype tag values based on the mapping table set out in
    RELEASE_TYPE_MAPPING.

    The mapping table is derived from releastype metadata populated into tags via Picard (left side) and maps to user preference (right side)

    Logs changes to changelog table.

    Ensures all music in your library has releasetype assigned in a consistent manner enabling a much cleaner discography browse
    in releastype aware music servers like Lyrion.

    It is part of tagminder.

Usage:
    python 12-release_type_normalizer.py
    uv run 12-release_type_normalizer.py

Author: audiomuze
Created: 2025-06-11
Updated: 2025-06-22
"""

import polars as pl
import sqlite3
from typing import Dict, List, Union
import logging
from datetime import datetime, timezone

# ---------- Config ----------
SCRIPT_NAME = "release-type-normalizer.py"
DB_PATH = '/tmp/amg/dbtemplate.db'

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Release Type Mapping ----------
# query to inspect outcomes:
# select distinct old_value,new_value from changelog order by old_value;
# inspect changelog:
# select changelog.rowid, alib.albumartist, alib.album, column, old_value, alib.releasetype from changelog inner join alib on alib.rowid == changelog.rowid;

RELEASE_TYPE_MAPPING = {
    "album\\\\audiobook": "Studio Album\\\\Audiobook",
    "album\\\\audio drama": "Studio Album",
    "album\\\\bootleg": "Demos, Soundboards & Bootlegs",
    "album\\\\bootleg\\\\live": "Demos, Soundboards & Bootlegs\\\\Live Album",
    "album\\\\compilation\\\\dj-mix": "Remix",
    "album\\\\compilation": "Greatest Hits & Anthologies",
    "album\\\\compilation\\\\live": "Greatest Hits & Anthologies\\\\Live Album",
    "album\\\\compilation\\\\soundtrack": "Soundtrack",
    "album\\\\demo": "Demos, Soundboards & Bootlegs",
    "album\\\\dj-mix": "Remix\\\\DJ-Mix",
    "album\\\\interview": "Studio Album\\\\Interview",
    "album\\\\live": "Live Album",
    "album\\\\mixtape/street": "Mixtape/Street",
    "album\\\\remix": "Remix",
    "album\\\\live\\\\soundtrack": "Soundtrack\\\\Live Album",
    "album\\\\soundtrack": "Soundtrack\\\\Studio Album",
    "album": "Studio Album",
    "anthology": "Greatest Hits & Anthologies",
    "audio drama\\\\broadcast": "Live Album\\\\Broadcast",
    "bootleg\\\\soundboard": "Demos, Soundboards & Bootlegs",
    "box set": "Box Set",
    "box set\\\\live album": "Box Set\\\\Live Album",
    "broadcast\\\\live": "Live Album\\\\Broadcast",
    "compilation\\\\album": "Greatest Hits & Anthologies",
    "compilation\\\\demo\\\\ep": "Demos, Soundboards & Bootlegs\\\\Extended Play",
    "compilation\\\\ep": "Greatest Hits & Anthologies\\\\Extended Play",
    "compilation\\\\live": "Greatest Hits & Anthologies\\\\Live Album",
    "compilation\\\\live album": "Greatest Hits & Anthologies\\\\Live Album",
    "compilation": "Greatest Hits & Anthologies",
    "compilation\\\\single": "Single\\\\Compilation",
    "composite reissue": "Studio Album",
    "demo": "Demos, Soundboards & Bootlegs",
    "demo\\\\ep": "Demos, Soundboards & Bootlegs\\\\Extended Play",
    "ep": "Extended Play",
    "ep\\\\live": "Extended Play\\\\Live Album",
    "ep\\\\mixtape/street": "Mixtape/Street\\\\Extended Play",
    "ep\\\\remix": "Remix\\\\Extended Play",
    "ep\\\\soundtrack": "Soundtrack\\\\Extended Play",
    "extended play": "Extended Play",
    "extended play\\\\remix": "Remix\\\\Extended Play",
    "interview\\\\single": "Single\\\\Interview",
    "live album": "Live Album",
    "live\\\\album": "Live Album",
    "live\\\\ep": "Extended Play\\\\Live Album",
    "live": "Live Album",
    "live\\\\single": "Single\\\\Live Album",
    "mixtape/street": "Mixtape/Street",
    "other": "Studio Album",
    "remix": "Remix",
    "remix\\\\single": "Remix\\\\Single",
    "single\\\\live": "Single\\\\Live Album",
    "single": "Single",
    "single\\\\soundtrack": "Single\\\\Soundtrack",
    "soundtrack\\\\album": "Soundtrack\\\\Studio Album",
    "soundtrack": "Soundtrack",
    "studio album\\\\compilation": "Greatest Hits & Anthologies",
    "studio album\\\\compilation\\\\remix": "Remix",
    "studio album\\\\demo": "Demos, Soundboards & Bootlegs",
    "studio album\\\\remix": "Remix",
    "studio album": "Studio Album",
    "various artists": "Various Artists Compilation"
}

DELIMITER = '\\\\'

# ---------- Helpers ----------
def sqlite_to_polars(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """
    Convert SQLite query results to Polars DataFrame with proper type handling.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if col_name in ("rowid", "sqlmodded"):
            data[col_name] = pl.Series(
                name=col_name,
                values=[int(x or 0) for x in col_data],
                dtype=pl.Int64
            )
        else:
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )

    return pl.DataFrame(data)


def apply_multi_value_mappings(x: Union[str, None], mapping: Dict[str, str]) -> Union[str, None]:
    """
    Apply mappings for entries that contain the delimiter (multi-value mappings).
    These are applied as direct string replacements without splitting.

    Args:
        x: The release type string to normalize (can be None)
        mapping: Dictionary mapping old multi-value strings to new values

    Returns:
        String with multi-value mappings applied or None
    """
    if x is None:
        return None

    # Apply direct string replacement for multi-value mappings (case-insensitive exact match)
    lowercase_x = x.lower()
    if lowercase_x in mapping:
        return mapping[lowercase_x]

    return x


def normalize_single_value_entry(x: Union[str, None], mapping: Dict[str, str]) -> Union[str, None]:
    """
    Normalize a release type entry by direct mapping lookup.
    No splitting logic needed since all keys in single-value mappings contain no delimiters.

    Args:
        x: The release type string to normalize (can be None)
        mapping: Dictionary mapping old single values to new values

    Returns:
        Normalized release type string or None
    """
    if x is None:
        return None

    # Single value direct mapping (case insensitive)
    stripped_x = x.strip() if x else x
    lowercase_x = stripped_x.lower() if stripped_x else stripped_x
    return mapping.get(lowercase_x, stripped_x)


def batch_normalize_release_types(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """
    Apply release type normalization to the releasetype column using two-stage vectorized operations.
    Stage 1: Apply only mappings where left side contains delimiter (no splitting) to ALL rows
    Stage 2: Apply only mappings where left side doesn't contain delimiter (with splitting) to rows NOT processed in Stage 1

    Args:
        df: Input DataFrame
        mapping: Dictionary mapping old values to new values

    Returns:
        DataFrame with normalized releasetype column
    """
    # Separate mappings into multi-value and single-value based on left side containing delimiter
    multi_value_mapping = {k: v for k, v in mapping.items() if DELIMITER in k}
    single_value_mapping = {k: v for k, v in mapping.items() if DELIMITER not in k}

    result_df = df
    stage1_processed_rowids = []

    # Stage 1: Apply multi-value mappings (direct string replacement) to ALL rows
    if multi_value_mapping:
        logging.info(f"Stage 1: Applying {len(multi_value_mapping)} multi-value mappings to all rows...")

        # Track which rows were actually changed in stage 1
        original_releasetype = result_df["releasetype"]

        multi_expr = pl.col("releasetype").map_elements(
            lambda x: apply_multi_value_mappings(x, multi_value_mapping),
            return_dtype=pl.Utf8
        ).alias("releasetype")
        result_df = result_df.with_columns(multi_expr)

        # Identify rows that were changed in stage 1
        stage1_changed_mask = (
            (original_releasetype.is_not_null()) &
            (original_releasetype != result_df["releasetype"])
        )
        stage1_processed_rowids = result_df.filter(stage1_changed_mask)["rowid"].to_list()
        logging.info(f"Stage 1 processed {len(stage1_processed_rowids)} rows")

    # Stage 2: Apply single-value mappings (with splitting and deduplication) to rows NOT processed in stage 1
    if single_value_mapping:
        logging.info(f"Stage 2: Applying {len(single_value_mapping)} single-value mappings to unprocessed rows...")

        if stage1_processed_rowids:
            # Only process rows that were NOT changed in stage 1
            unprocessed_mask = ~pl.col("rowid").is_in(stage1_processed_rowids)

            single_expr = pl.when(unprocessed_mask).then(
                pl.col("releasetype").map_elements(
                    lambda x: normalize_single_value_entry(x, single_value_mapping),
                    return_dtype=pl.Utf8
                )
            ).otherwise(pl.col("releasetype")).alias("releasetype")

            result_df = result_df.with_columns(single_expr)

            # Count how many additional rows were processed in stage 2
            stage2_count = result_df.filter(unprocessed_mask).height
            logging.info(f"Stage 2 processed {stage2_count} rows")
        else:
            # No rows were processed in stage 1, so process all rows in stage 2
            single_expr = pl.col("releasetype").map_elements(
                lambda x: normalize_single_value_entry(x, single_value_mapping),
                return_dtype=pl.Utf8
            ).alias("releasetype")
            result_df = result_df.with_columns(single_expr)
            logging.info(f"Stage 2 processed all {result_df.height} rows")

    return result_df


def assign_release_types_for_null_values(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign release types to albums that currently have null release types.
    Implements the logic from the legacy add_releasetype() function using vectorized operations.

    Logic:
    1. Singles: ≤3 tracks per __dirpath (excluding Classical/Jazz genres)
    2. Extended Play: 4-6 tracks per __dirpath (excluding Classical/Jazz genres)
    3. Soundtrack: __dirpath contains '/OST'
    4. Studio Album: >6 tracks per __dirpath OR Classical/Jazz genres (for remaining null values)

    Args:
        df: DataFrame with album data including __dirpath, releasetype, and genre columns

    Returns:
        DataFrame with release types assigned to previously null values
    """
    logging.info("Starting release type assignment for null values...")

    # Create a copy to work with
    result_df = df.clone()

    # Get track counts per directory for null release types, excluding Classical/Jazz
    track_counts = (
        result_df
        .filter(
            (pl.col("releasetype").is_null()) &
            (~pl.col("genre").str.contains("(?i)classical", literal=False)) &
            (~pl.col("genre").str.contains("(?i)jazz", literal=False))
        )
        .group_by("__dirpath")
        .agg(pl.len().alias("track_count"))
    )

    logging.info(f"Found {track_counts.height} directories with null release types (excluding Classical/Jazz)")

    # Assign Singles (≤3 tracks)
    singles_dirs = track_counts.filter(pl.col("track_count") <= 3)["__dirpath"].to_list()
    if singles_dirs:
        singles_mask = (
            (pl.col("releasetype").is_null()) &
            (pl.col("__dirpath").is_in(singles_dirs)) &
            (~pl.col("genre").str.contains("(?i)classical", literal=False)) &
            (~pl.col("genre").str.contains("(?i)jazz", literal=False))
        )
        result_df = result_df.with_columns(
            pl.when(singles_mask)
            .then(pl.lit("Single"))
            .otherwise(pl.col("releasetype"))
            .alias("releasetype")
        )
        singles_count = result_df.filter(singles_mask).height
        logging.info(f"Assigned 'Single' to {singles_count} tracks in {len(singles_dirs)} directories")

    # Assign Extended Play (4-6 tracks)
    ep_dirs = track_counts.filter((pl.col("track_count") > 3) & (pl.col("track_count") <= 6))["__dirpath"].to_list()
    if ep_dirs:
        ep_mask = (
            (pl.col("releasetype").is_null()) &
            (pl.col("__dirpath").is_in(ep_dirs)) &
            (~pl.col("genre").str.contains("(?i)classical", literal=False)) &
            (~pl.col("genre").str.contains("(?i)jazz", literal=False))
        )
        result_df = result_df.with_columns(
            pl.when(ep_mask)
            .then(pl.lit("Extended Play"))
            .otherwise(pl.col("releasetype"))
            .alias("releasetype")
        )
        ep_count = result_df.filter(ep_mask).height
        logging.info(f"Assigned 'Extended Play' to {ep_count} tracks in {len(ep_dirs)} directories")

    # Assign Soundtrack (directories containing '/OST')
    ost_mask = (
        (pl.col("releasetype").is_null()) &
        (pl.col("__dirpath").str.contains("/OST"))
    )
    if result_df.filter(ost_mask).height > 0:
        result_df = result_df.with_columns(
            pl.when(ost_mask)
            .then(pl.lit("Soundtrack"))
            .otherwise(pl.col("releasetype"))
            .alias("releasetype")
        )
        ost_count = result_df.filter(ost_mask).height
        logging.info(f"Assigned 'Soundtrack' to {ost_count} tracks in OST directories")

    # Final step: Assign Studio Album to all remaining null values
    remaining_null_count = result_df.filter(pl.col("releasetype").is_null()).height
    if remaining_null_count > 0:
        logging.info(f"Assigning 'Studio Album' to remaining {remaining_null_count} tracks with null release types...")

        result_df = result_df.with_columns(
            pl.when(pl.col("releasetype").is_null())
            .then(pl.lit("Studio Album"))
            .otherwise(pl.col("releasetype"))
            .alias("releasetype")
        )

        logging.info(f"Assigned 'Studio Album' to {remaining_null_count} tracks")

    return result_df


def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int]
) -> int:
    """
    Write updates to the database and log changes.

    Args:
        conn: SQLite database connection
        updated_df: DataFrame with updated values
        original_df: DataFrame with original values
        changed_rowids: List of rowids that have changes

    Returns:
        Number of updated rows
    """
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    # Ensure changelog table exists
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
    updated_count = 0

    # Filter to only changed rows
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)

        # Check if releasetype actually changed and is not None
        new_value = record["releasetype"]
        old_value = original_row["releasetype"]

        if new_value != old_value and new_value is not None:
            # Increment sqlmodded counter
            new_sqlmodded = int(original_row["sqlmodded"] or 0) + 1

            # Update the database
            cursor.execute(
                "UPDATE alib SET releasetype = ?, sqlmodded = ? WHERE rowid = ?",
                (new_value, new_sqlmodded, rowid)
            )

            # Log the change
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, "releasetype", old_value, new_value, timestamp, SCRIPT_NAME)
            )

            updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


# ---------- Main ----------
def main():
    """
    Main function to normalize release types in the database.
    """
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Fetch data - now including __dirpath and genre for assignment logic
        logging.info("Fetching release type data...")
        tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid, releasetype, __dirpath, genre, COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            ORDER BY rowid
            """
        )

        logging.info(f"Processing {tracks.height} total tracks...")

        # Step 1: Normalize existing release types (only for non-null values)
        tracks_with_releasetype = tracks.filter(pl.col("releasetype").is_not_null())
        if tracks_with_releasetype.height > 0:
            logging.info(f"Normalizing {tracks_with_releasetype.height} tracks with existing release types...")
            normalized_tracks = batch_normalize_release_types(tracks_with_releasetype, RELEASE_TYPE_MAPPING)

            # Update the main dataframe with normalized values
            tracks = tracks.update(
                normalized_tracks.select(["rowid", "releasetype"]),
                on="rowid"
            )

        # Step 2: Assign release types to null values
        tracks_with_null = tracks.filter(pl.col("releasetype").is_null())
        if tracks_with_null.height > 0:
            logging.info(f"Assigning release types to {tracks_with_null.height} tracks with null values...")
            tracks = assign_release_types_for_null_values(tracks)

        # Detect all changes using vectorized comparison with original data
        original_tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid, releasetype, __dirpath, genre, COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            ORDER BY rowid
            """
        )

        # Compare original vs updated, accounting for null values
        change_expr = (
            (original_tracks["releasetype"] != tracks["releasetype"]) |
            (original_tracks["releasetype"].is_null() & tracks["releasetype"].is_not_null()) |
            (original_tracks["releasetype"].is_not_null() & tracks["releasetype"].is_null())
        )

        changed_rowids = tracks.filter(change_expr)["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes total")

        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=tracks,
                original_df=original_tracks,
                changed_rowids=changed_rowids
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
