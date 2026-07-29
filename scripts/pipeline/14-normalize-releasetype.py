"""
Purpose:
    Normalize `releasetype` values in `alib` using the mapping in
    `RELEASE_TYPE_MAPPING`.

    Ensures consistent releasetype tagging for cleaner browsing in releasetype-
    aware music servers.

    Logs changes to `changelog` and increments `__sqlmodded`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import polars as pl
import sqlite3
from typing import Dict, List, Union
import logging

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# ---------- Config ----------


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

# Multi-value delimiter used by Tagminder (written to SQLite as two literal backslashes).
# Source of truth: tagminder.toml [strings].multivalue_delimiter
DELIMITER = tm_config.get_multivalue_delimiter()

# ---------- Helpers ----------
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


def apply_isgreatesthits_logic(df: pl.DataFrame, delimiter: str) -> pl.DataFrame:
    """
    Apply isgreatesthits boolean flag to enhance releasetype.

    If isgreatesthits = 1 or 'true', ensure releasetype includes "Greatest Hits & Anthologies".
    - If releasetype is NULL, set to "Greatest Hits & Anthologies"
    - If releasetype already contains the value, leave unchanged
    - If releasetype exists but doesn't contain it, append via delimiter

    Args:
        df: DataFrame with releasetype and isgreatesthits columns
        delimiter: Multi-value delimiter from tagminder.toml

    Returns:
        DataFrame with releasetype enhanced by isgreatesthits logic
    """
    if "isgreatesthits" not in df.columns:
        return df

    logging.info("Applying isgreatesthits logic...")

    result_df = df.clone()
    target_value = "Greatest Hits & Anthologies"

    # Identify rows where isgreatesthits indicates true (1 or 'true' or 'True')
    is_greatest_hits_mask = (
        (pl.col("isgreatesthits").is_not_null()) & (
            (pl.col("isgreatesthits").cast(pl.Utf8).str.to_lowercase() == "true") |
            (pl.col("isgreatesthits").cast(pl.Utf8) == "1")
        )
    )

    rowids = result_df.get_column("rowid").to_list()
    releasetypes = result_df.get_column("releasetype").to_list()

    new_releasetypes: list[str | None] = []
    for i in range(len(rowids)):
        rowid = rowids[i]
        current_rt = releasetypes[i]
        is_greatest = False

        # Check if this row has isgreatesthits=true
        if "isgreatesthits" in result_df.columns:
            is_greatest_val = result_df.filter(pl.col("rowid") == rowid)["isgreatesthits"][0]
            if is_greatest_val is not None:
                val_str = str(is_greatest_val).strip().lower()
                is_greatest = val_str in ("true", "1")

        if not is_greatest:
            new_releasetypes.append(current_rt)
            continue

        # isgreatesthits is true; ensure Greatest Hits & Anthologies is in releasetype
        if current_rt is None or current_rt == "":
            # Set to target value
            new_releasetypes.append(target_value)
        elif target_value in (current_rt or "").split(delimiter):
            # Already contains the value
            new_releasetypes.append(current_rt)
        else:
            # Append target value
            merged = f"{current_rt}{delimiter}{target_value}"
            new_releasetypes.append(merged)

    result_df = result_df.with_columns(
        pl.Series(name="releasetype", values=new_releasetypes, dtype=pl.Utf8)
    )

    updated_count = sum(
        1 for i in range(len(rowids))
        if new_releasetypes[i] != releasetypes[i]
    )
    if updated_count > 0:
        logging.info(f"Applied isgreatesthits logic to {updated_count} tracks")

    return result_df


def apply_issoundtrack_logic(df: pl.DataFrame, delimiter: str) -> pl.DataFrame:
    """
    Apply issoundtrack boolean flag to enhance releasetype.

    If issoundtrack = 1 or 'true', ensure releasetype includes "Soundtrack".
    - If releasetype is NULL, set to "Soundtrack"
    - If releasetype already contains the value, leave unchanged
    - If releasetype exists but doesn't contain it, append via delimiter

    Args:
        df: DataFrame with releasetype and issoundtrack columns
        delimiter: Multi-value delimiter from tagminder.toml

    Returns:
        DataFrame with releasetype enhanced by issoundtrack logic
    """
    if "issoundtrack" not in df.columns:
        return df

    logging.info("Applying issoundtrack logic...")

    result_df = df.clone()
    target_value = "Soundtrack"

    rowids = result_df.get_column("rowid").to_list()
    releasetypes = result_df.get_column("releasetype").to_list()

    new_releasetypes: list[str | None] = []
    for i in range(len(rowids)):
        rowid = rowids[i]
        current_rt = releasetypes[i]
        is_soundtrack = False

        # Check if this row has issoundtrack=true
        if "issoundtrack" in result_df.columns:
            is_soundtrack_val = result_df.filter(pl.col("rowid") == rowid)["issoundtrack"][0]
            if is_soundtrack_val is not None:
                val_str = str(is_soundtrack_val).strip().lower()
                is_soundtrack = val_str in ("true", "1")

        if not is_soundtrack:
            new_releasetypes.append(current_rt)
            continue

        # issoundtrack is true; ensure Soundtrack is in releasetype
        if current_rt is None or current_rt == "":
            # Set to target value
            new_releasetypes.append(target_value)
        elif target_value in (current_rt or "").split(delimiter):
            # Already contains the value
            new_releasetypes.append(current_rt)
        else:
            # Append target value
            merged = f"{current_rt}{delimiter}{target_value}"
            new_releasetypes.append(merged)

    result_df = result_df.with_columns(
        pl.Series(name="releasetype", values=new_releasetypes, dtype=pl.Utf8)
    )

    updated_count = sum(
        1 for i in range(len(rowids))
        if new_releasetypes[i] != releasetypes[i]
    )
    if updated_count > 0:
        logging.info(f"Applied issoundtrack logic to {updated_count} tracks")

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
    tm_db.ensure_changelog_table(conn)
    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()
    updated_count = 0

    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    update_sql = tm_db.build_update_sql(table="alib", set_cols=["releasetype"])

    # Filter to only changed rows
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original_df.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for record in records:
            rowid = record["rowid"]
            original_row = original_by_rowid[int(rowid)]

            # Check if releasetype actually changed and is not None
            new_value = record["releasetype"]
            old_value = original_row["releasetype"]

            if new_value != old_value and new_value is not None:
                # Increment __sqlmodded counter
                new_sqlmodded = int(original_row["__sqlmodded"] or 0) + 1

                # Update the database
                cursor.execute(
                    update_sql,
                    (new_value, int(new_sqlmodded or 0), rowid),
                )

                alib_path = path_by_rowid.get(int(rowid), str(rowid))
                changelog.add(
                    alib_path=alib_path,
                    changes=[("releasetype", old_value, new_value)],
                )

                updated_count += 1

            changelog.flush(cursor)

    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


# ---------- Main ----------
def main():
    """
    Main function to normalize release types in the database.
    """
    try:
        conn, _, _, _ = tm_run.open_db(ensure_changelog=True, require_exists=True)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    if not tm_db.table_exists(conn, "alib"):
        logging.error("Required table 'alib' not found in database")
        conn.close()
        return

    try:
        # Fetch data - now including __dirpath, genre, isgreatesthits, issoundtrack for logic
        logging.info("Fetching release type data...")
        tracks = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid, releasetype, __dirpath, genre, isgreatesthits, issoundtrack, COALESCE(__sqlmodded, 0) AS __sqlmodded
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

        # Step 3: Apply isgreatesthits logic to enhance releasetype
        tracks = apply_isgreatesthits_logic(tracks, DELIMITER)

        # Step 4: Apply issoundtrack logic to enhance releasetype
        tracks = apply_issoundtrack_logic(tracks, DELIMITER)

        # Detect all changes using vectorized comparison with original data
        original_tracks = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid, releasetype, __dirpath, genre, isgreatesthits, issoundtrack, COALESCE(__sqlmodded, 0) AS __sqlmodded
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
