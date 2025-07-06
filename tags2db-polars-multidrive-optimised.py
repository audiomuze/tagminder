"""
Script Name: tags2db-polars-multidrive-optimised.py

Purpose:
    This script imports/exports all metadata tags from specified folder trees and writes them to a SQLite database table 'alib'.
    It is designed for multi-drive setups, optimizing tag reading by assigning dedicated worker pools per physical drive
    to maximize concurrent I/O operations and reduce disk contention.

    It is part of tagminder.

Usage (Import):
    python tags2db-polars-multidrive-optimised.py import /path/to/db.sqlite /qnap/qnap1 /qnap/qnap2 /qnap/qnap3 --workers 8 --chunk-size 4000
    - 'import': Specifies the action to import tags.
    - '/path/to/db.sqlite': Path to the SQLite database file.
    - '/qnap/qnap1 /qnap/qnap2 /qnap/qnap3': Space-separated paths to music directories (mount points of different drives).
    - '--workers N': (Optional) Number of worker processes for tag processing *per drive*. Defaults to CPU count // number of active drives.
    - '--chunk-size N': (Optional) Number of files to process per chunk during tag reading. Default is 4000.

Usage (Export):
    python tags2db-polars-multidrive-optimised.py export /path/to/db.sqlite /path/to/music_directory
    - 'export': Specifies the action to export tags.
    - '/path/to/db.sqlite': Path to the SQLite database file.
    - '/path/to/music_directory': A single directory path to filter exported tags by (e.g., to export tags only from files under this path).

Author: audiomuze
Created: 2025-03-17
Optimized: 2025-06-22
"""

#!/usr/bin/env python3

import argparse
import os
import sqlite3
import sys
import logging
from typing import Dict, List, Optional, Any, Tuple, Iterator
import multiprocessing
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import time

try:
    from os import scandir
except ImportError:
    from scandir import scandir  # use scandir PyPI module on Python < 3.5

# Required dependencies
try:
    # import puddlestuff
    from puddlestuff import audioinfo # Reverted: Import audioinfo module
except ImportError:
    print("Error: puddlestuff module or audioinfo submodule not found. Please ensure puddlestuff is installed correctly.", file=sys.stderr)
    sys.exit(1)

try:
    import polars as pl
except ImportError:
    print("Error: polars module not found. Please install it with: pip install polars", file=sys.stderr)
    sys.exit(1)

# --- Constants ---
AUDIO_EXTENSIONS = {'.mp3', '.flac', '.wv', '.ogg', '.m4a', '.aiff', '.ape'}
DATABASE_PATH = 'alib.db' # Default database name
TABLE_NAME = 'alib'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# --- Polars Schema ---
# Defines the schema for the Polars DataFrame, mapping column names to Polars data types.
# This ensures consistent data types when building the DataFrame and inserting into the database.
ALBUM_INFO_SCHEMA = {
    "__path": pl.Utf8, # <-- Primary Key (full file path)
    "__dirpath": pl.Utf8,
    "__filename": pl.Utf8,
    "__filename_no_ext": pl.Utf8,
    "__ext": pl.Utf8,
    "__accessed": pl.Utf8,
    "__app": pl.Utf8,
    "__bitrate": pl.Utf8,
    "__bitspersample": pl.Utf8,
    "__bitrate_num": pl.Utf8,
    "__frequency_num": pl.Utf8,
    "__frequency": pl.Utf8,
    "__channels": pl.Utf8,
    "__created": pl.Utf8,
    "__dirname": pl.Utf8,
    "__file_access_date": pl.Utf8,
    "__file_access_datetime": pl.Utf8,
    "__file_access_datetime_raw": pl.Utf8,
    "__file_create_date": pl.Utf8,
    "__file_create_datetime": pl.Utf8,
    "__file_create_datetime_raw": pl.Utf8,
    "__file_mod_date": pl.Utf8,
    "__file_mod_datetime": pl.Utf8,
    "__file_mod_datetime_raw": pl.Utf8,
    "__file_size": pl.Utf8,
    "__file_size_bytes": pl.Utf8,
    "__file_size_kb": pl.Utf8,
    "__file_size_mb": pl.Utf8,
    "__filetype": pl.Utf8,
    "__image_mimetype": pl.Utf8,
    "__image_type": pl.Utf8,
    "__layer": pl.Utf8,
    "__length": pl.Utf8,
    "__length_seconds": pl.Utf8,
    "__mode": pl.Utf8,
    "__modified": pl.Utf8,
    "__num_images": pl.Utf8,
    "__parent_dir": pl.Utf8,
    "__size": pl.Utf8,
    "__tag": pl.Utf8,
    "__tag_read": pl.Utf8,
    "__version": pl.Utf8,
    "__vendorstring": pl.Utf8,
    "__md5sig": pl.Utf8,
    "bliss_analysis": pl.Utf8,
    "tagminder_uuid": pl.Utf8,
    "sqlmodded": pl.Int64,
    "reflac": pl.Utf8,
    "disc": pl.Utf8,
    "discnumber": pl.Utf8,
    "track": pl.Utf8,
    "title": pl.Utf8,
    "subtitle": pl.Utf8,
    "artist": pl.Utf8,
    "composer": pl.Utf8,
    "arranger": pl.Utf8,
    "lyricist": pl.Utf8,
    "writer": pl.Utf8,
    "albumartist": pl.Utf8,
    "discsubtitle": pl.Utf8,
    "album": pl.Utf8,
    "live": pl.Utf8,
    "version": pl.Utf8,
    "work": pl.Utf8,
    "movement": pl.Utf8,
    "part": pl.Utf8,
    "ensemble": pl.Utf8,
    "performer": pl.Utf8,
    "personnel": pl.Utf8,
    "conductor": pl.Utf8,
    "engineer": pl.Utf8,
    "producer": pl.Utf8,
    "mixer": pl.Utf8,
    "remixer": pl.Utf8,
    "releasetype": pl.Utf8,
    "year": pl.Utf8,
    "originaldate": pl.Utf8,
    "originalreleasedate": pl.Utf8,
    "originalyear": pl.Utf8,
    "genre": pl.Utf8,
    "style": pl.Utf8,
    "mood": pl.Utf8,
    "theme": pl.Utf8,
    "rating": pl.Utf8,
    "compilation": pl.Utf8,
    "bootleg": pl.Utf8,
    "label": pl.Utf8,
    "amgtagged": pl.Utf8,
    "amg_album_id": pl.Utf8,
    "amg_boxset_url": pl.Utf8,
    "amg_url": pl.Utf8,
    "musicbrainz_albumartistid": pl.Utf8,
    "musicbrainz_albumid": pl.Utf8,
    "musicbrainz_artistid": pl.Utf8,
    "musicbrainz_composerid": pl.Utf8,
    "musicbrainz_discid": pl.Utf8,
    "musicbrainz_engineerid": pl.Utf8,
    "musicbrainz_producerid": pl.Utf8,
    "musicbrainz_releasegroupid": pl.Utf8,
    "musicbrainz_releasetrackid": pl.Utf8,
    "musicbrainz_trackid": pl.Utf8,
    "musicbrainz_workid": pl.Utf8,
    "lyrics": pl.Utf8,
    "unsyncedlyrics": pl.Utf8,
    "performancedate": pl.Utf8,
    "acousticbrainz_mood": pl.Utf8,
    "acoustid_fingerprint": pl.Utf8,
    "acoustid_id": pl.Utf8,
    "analysis": pl.Utf8,
    "asin": pl.Utf8,
    "barcode": pl.Utf8,
    "catalog": pl.Utf8,
    "catalognumber": pl.Utf8,
    "isrc": pl.Utf8,
    "media": pl.Utf8,
    "country": pl.Utf8,
    "discogs_artist_url": pl.Utf8,
    "discogs_release_url": pl.Utf8,
    "fingerprint": pl.Utf8,
    "recordinglocation": pl.Utf8,
    "recordingstartdate": pl.Utf8,
    "replaygain_album_gain": pl.Utf8,
    "replaygain_album_peak": pl.Utf8,
    "replaygain_track_gain": pl.Utf8,
    "replaygain_track_peak": pl.Utf8,
    "review": pl.Utf8,
    "roonalbumtag": pl.Utf8,
    "roonid": pl.Utf8,
    "roonradioban": pl.Utf8,
    "roontracktag": pl.Utf8,
    "upc": pl.Utf8,
    "__albumgain": pl.Utf8
}

# --- Helper Functions ---
def sanitize_value(value: Any) -> str:
    """
    Converts a tag value to a sanitized string. Handles lists by joining them.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(map(str, value))
    return str(value)

def tag_to_dict_raw(tag: Any) -> Dict[str, Any]:
    """
    Convert a puddlestuff Tag object to a dictionary,
    keeping raw values and extracting direct audio properties.
    """
    tag_dict = dict(tag) # Gets textual tags from the Tag object

    # Add direct audio properties from the Tag object using getattr for safety
    tag_dict['__path'] = tag.filepath # Use 'filename' to match schema
    tag_dict['length'] = getattr(tag, 'length', None)
    tag_dict['bitrate'] = getattr(tag, 'bitrate', None)
    tag_dict['samplerate'] = getattr(tag, 'samplerate', None)
    tag_dict['channels'] = getattr(tag, 'channels', None)
    tag_dict['bitdepth'] = getattr(tag, 'bitdepth', None)
    tag_dict['replaygain_track_gain'] = getattr(tag, 'replaygain_track_gain', None)
    tag_dict['replaygain_album_gain'] = getattr(tag, 'replaygain_album_gain', None)

    cleaned_dict = {}
    for k, v in tag_dict.items():
        # Remove quotes from tag names (they're illegal in SQLite column names)
        safe_k = k.replace('"', '') if isinstance(k, str) and '"' in k else k
        cleaned_dict[safe_k] = v
    return cleaned_dict


def scantree(path: str) -> Iterator[str]:
    """
    Recursively yields file paths matching AUDIO_EXTENSIONS from a directory tree.
    Uses os.scandir for efficient directory listing.
    """
    for entry in scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)
        elif entry.is_file(follow_symlinks=False):
            if os.path.splitext(entry.name)[1].lower() in AUDIO_EXTENSIONS:
                yield entry.path

def scan_single(path: str) -> Tuple[str, List[str]]:
    """
    Scans a single directory path and returns the path itself and a list of found audio files.
    This function is designed to be run in a separate thread.
    """
    logging.info(f"Scanning {path}...")
    files = list(scantree(path))
    logging.info(f"Finished scanning {path}. Found {len(files)} files.")
    return path, files

def parallel_scantree(dirpaths: List[str], workers: int) -> Dict[str, List[str]]:
    """
    Scans multiple directory paths in parallel using a ThreadPoolExecutor.
    Returns a dictionary mapping each directory path to its list of audio files.
    """
    drive_files: Dict[str, List[str]] = {}
    # Using ThreadPoolExecutor for I/O-bound scanning is efficient as threads wait for disk.
    with ThreadPoolExecutor(max_workers=workers) as executor: # Corrected: use the 'workers' argument
        futures = {executor.submit(scan_single, path): path for path in dirpaths}
        for future in concurrent.futures.as_completed(futures):
            drive_path, files = future.result()
            drive_files[drive_path] = files
    return drive_files

def process_chunk_optimized(filepaths: List[str]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Processes a chunk of filepaths, parses their tags using audioinfo.Tag,
    and returns the list of parsed tags and statistics (processed/failed files) for the chunk.
    This function is designed to be run in a separate process.
    """
    tags_in_chunk = []
    chunk_stats = {"processed": 0, "failed": 0}
    for filepath in filepaths:
        try:
            info = audioinfo.Tag(filepath) # Reverted: Use audioinfo.Tag
            parsed_tags = tag_to_dict_raw(info)
            tags_in_chunk.append(parsed_tags)
            chunk_stats["processed"] += 1
        except Exception as e:
            logging.warning(f"Failed to parse tags for {filepath}: {e}")
            chunk_stats["failed"] += 1
    return tags_in_chunk, chunk_stats

def clean_and_normalize_tags_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """Apply vectorized data cleaning and normalization to tag DataFrame.

    This replaces the individual tag cleaning that was done during dictionary creation.

    Args:
        df: DataFrame with raw tag values

    Returns:
        DataFrame with cleaned and normalized values
    """
    expressions = []

    for col in df.columns:
        col_dtype = df[col].dtype

        if col_dtype == pl.List:
            # Handle list columns - convert to string with double backslash delimiter
            cleaned_expr = (
                pl.when(pl.col(col).is_null())
                .then(None)
                .when(pl.col(col).list.len() == 0)
                .then(None)
                .otherwise(
                    pl.col(col)
                    .list.eval(pl.element().cast(pl.Utf8))
                    .list.join("\\\\") # Use double backslash as a deliberate separator
                )
                .alias(col)
            )
        else:
            # Handle non-list columns - ensure they're strings and clean
            cleaned_expr = (
                pl.when(pl.col(col).is_null())
                .then(None)
                .otherwise(pl.col(col).cast(pl.Utf8))
                .alias(col)
            )

            # Additional cleaning for string columns to remove empty strings
            if col_dtype in [pl.Utf8, pl.String]:
                cleaned_expr = (
                    pl.when(pl.col(col).is_null())
                    .then(None)
                    .when(pl.col(col).cast(pl.Utf8).str.strip_chars() == "")
                    .then(None)
                    .otherwise(pl.col(col).cast(pl.Utf8))
                    .alias(col)
                )

        expressions.append(cleaned_expr)

    return df.with_columns(expressions)


def build_dataframe_with_schema(all_tags: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Builds a Polars DataFrame from a list of tag dictionaries, enforcing a dynamic schema
    where all tag fields are treated as Utf8 to prevent type conflicts during creation.
    """
    if not all_tags:
        # Return an empty DataFrame with the correct schema if no tags are processed
        return pl.DataFrame({}, schema=ALBUM_INFO_SCHEMA)

    # 1. Discover all unique keys from all tags to prevent dropping unknown tags.
    #    Also, ensure all keys from the static schema are included for consistency.
    all_keys = set(ALBUM_INFO_SCHEMA.keys())
    for tag_dict in all_tags:
        all_keys.update(tag_dict.keys())

    # 2. Per the request, create an ingestion schema where all columns are treated as pl.Utf8
    #    to prevent type errors on creation. The original schema's `Int64` for `sqlmodded`
    #    is respected as it's an internal field, not a "tag".
    ingestion_schema = {key: pl.Utf8 for key in all_keys}
    if 'sqlmodded' in ingestion_schema:
        ingestion_schema['sqlmodded'] = pl.Int64

    # 3. Pre-process the raw tag data. The primary goal is to convert list values
    #    into strings BEFORE they are passed to the DataFrame constructor. This avoids
    #    the `ComputeError: could not append value: [...] of type: list[str]`.
    pre_processed_tags = []
    for tag_dict in all_tags:
        processed_dict = {}
        # Iterate over all possible keys to ensure dictionaries have a consistent structure
        for key in all_keys:
            if key in tag_dict:
                value = tag_dict[key]
                if isinstance(value, list):
                    # Convert list to a delimited string.
                    processed_dict[key] = "\\\\".join(map(str, value))
                else:
                    processed_dict[key] = value
            else:
                processed_dict[key] = None

        # Ensure 'sqlmodded' has a default value if missing/None.
        if processed_dict.get('sqlmodded') is None:
            processed_dict['sqlmodded'] = 0

        pre_processed_tags.append(processed_dict)

    # 4. Create the DataFrame using the pre-processed data and the dynamically generated
    #    schema. This single step now correctly handles all known and unknown tags without type errors.
    df = pl.DataFrame(pre_processed_tags, schema=ingestion_schema)

    # 5. The original call to `clean_and_normalize_tags_vectorized` is no longer necessary
    #    as its work (list conversion, ensuring columns) is now done *before* DataFrame creation.
    return df

# def create_and_migrate_db(dbpath: str, conn: sqlite3.Connection, df_columns: List[str] = None):
#     cursor = conn.cursor()

#     # Start with the predefined schema columns in their original order
#     ordered_columns = list(ALBUM_INFO_SCHEMA.keys())

#     # Add any new columns from the DataFrame that aren't in the schema
#     if df_columns:
#         new_columns = [col for col in df_columns if col not in ordered_columns]
#         for column in new_columns:
#             print(f"Adding column '{column}'")
#         print(f"Adding {len(new_columns)} additional columns to alib")
#         ordered_columns.extend(new_columns)

#     # Build the schema mapping
#     schema_to_use = {}
#     for col in ordered_columns:
#         if col in ALBUM_INFO_SCHEMA:
#             schema_to_use[col] = ALBUM_INFO_SCHEMA[col]
#         else:
#             schema_to_use[col] = pl.Utf8

#     # Define table creation SQL with consistent quoting
#     columns_sql = []
#     for col in ordered_columns:
#         dtype = schema_to_use[col]
#         sql_type = "TEXT"
#         if dtype == pl.Int64:
#             sql_type = "INTEGER"
#         elif dtype == pl.Float64:
#             sql_type = "REAL"

#         # Quote all column names consistently
#         quoted_col = f'"{col}"'
#         if col == "__path":
#             columns_sql.append(f"{quoted_col} {sql_type} PRIMARY KEY")
#         else:
#             columns_sql.append(f"{quoted_col} {sql_type}")

#     cursor.execute(f'''
#         CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (
#             {", ".join(columns_sql)}
#         )
#     ''')
#     conn.commit()
#     logging.info(f"Database table '{TABLE_NAME}' ensured to exist with {len(ordered_columns)} columns.")


def create_and_migrate_db(dbpath: str, conn: sqlite3.Connection, df_columns: List[str] = None) -> None:
    cursor = conn.cursor()

    # 1. Get current columns (primary key first, then others in existing order)
    cursor.execute(f'SELECT name FROM pragma_table_info("{TABLE_NAME}") ORDER BY cid')
    existing_columns = [row[0] for row in cursor.fetchall()]

    # 2. Define required columns (schema columns first, then new ones)
    required_columns = list(ALBUM_INFO_SCHEMA.keys())  # Preserves order
    if df_columns:
        required_columns += [col for col in df_columns
                           if col not in ALBUM_INFO_SCHEMA]  # New tags appended

    # 3. Create table if missing (with perfect schema order)
    if not existing_columns:
        columns_sql = [
            f'"{col}" TEXT{" PRIMARY KEY" if col == "__path" else ""}'
            if ALBUM_INFO_SCHEMA.get(col, pl.Utf8) == pl.Utf8
            else f'"{col}" INTEGER'
            for col in required_columns
        ]

        cursor.execute(f'''
            CREATE TABLE "{TABLE_NAME}" (
                {", ".join(columns_sql)}
            )
        ''')
        conn.commit()
        return

    # 4. Just add missing columns (all as TEXT since new tags are pl.Utf8)
    missing_columns = [col for col in required_columns
                      if col not in existing_columns]

    for col in missing_columns:
        try:
            cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN "{col}" TEXT')
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e):  # Ignore harmless duplicates
                raise

    conn.commit()


def process_single_drive(
    drive_path: str,
    files: List[str],
    chunk_size: int,
    workers_per_drive: int
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Processes all files for a single drive in parallel using a dedicated ProcessPoolExecutor.
    This function is designed to be run by an outer ThreadPoolExecutor (e.g., drive_manager_executor)
    to manage concurrent processing of multiple drives.

    Args:
        drive_path (str): The path to the current drive/mount point.
        files (List[str]): A list of all audio file paths found on this drive.
        chunk_size (int): The number of files to process per chunk.
        workers_per_drive (int): The maximum number of worker processes to use for this specific drive's pool.

    Returns:
        Tuple[List[Dict[str, Any]], Dict[str, Any]]: A tuple containing:
            - A list of all successfully parsed tag dictionaries from this drive.
            - A dictionary of total statistics for this drive (e.g., "processed_files", "failed_files").
    """
    logging.info(f"Starting parallel processing for drive: {drive_path} with {len(files)} files")

    # --- START OF CHANGE ---
    # Sort the files list for the current drive to improve locality of reference
    files.sort()
    # --- END OF CHANGE ---

    drive_all_tags = []
    drive_total_stats = {"processed_files": 0, "failed_files": 0}

    # Calculate total chunks for progress tracking
    total_chunks = (len(files) + chunk_size - 1) // chunk_size
    completed_chunks = 0

    # Each drive gets its own ProcessPoolExecutor, ensuring dedicated workers
    # that focus their I/O on that specific physical disk.
    with ProcessPoolExecutor(max_workers=workers_per_drive) as executor:
        futures = []
        # Submit chunks of files from this drive to its dedicated pool
        for i in range(0, len(files), chunk_size):
            chunk = files[i:i + chunk_size]
            futures.append(executor.submit(process_chunk_optimized, chunk))

        # Collect results from this drive's chunks as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                chunk_tags, chunk_stats = future.result()
                drive_all_tags.extend(chunk_tags)
                drive_total_stats["processed_files"] += chunk_stats["processed"]
                drive_total_stats["failed_files"] += chunk_stats["failed"]

                # Progress tracking
                completed_chunks += 1
                progress_percent = (completed_chunks / total_chunks) * 100
                logging.info(f"Drive {drive_path}: {completed_chunks}/{total_chunks} chunks completed ({progress_percent:.1f}%)")

            except Exception as e:
                logging.error(f"Error processing chunk for drive {drive_path}: {e}")
                # A rough estimate: if a chunk fails completely, assume all files in it failed
                drive_total_stats["failed_files"] += len(chunk)
                # Still increment completed chunks for progress tracking
                completed_chunks += 1
                progress_percent = (completed_chunks / total_chunks) * 100
                logging.info(f"Drive {drive_path}: {completed_chunks}/{total_chunks} chunks completed ({progress_percent:.1f}%) - chunk failed")

    logging.info(f"Finished processing drive: {drive_path}. Processed {drive_total_stats['processed_files']} files, failed {drive_total_stats['failed_files']}.")
    return drive_all_tags, drive_total_stats


def import_dir_optimized(
    dbpath: str,
    dirpaths: List[str],
    workers: Optional[int] = None, # Renamed to 'workers' to indicate workers PER DRIVE
    chunk_size: int = 4000
) -> None:
    """
    Optimized function to import audio metadata tags from multiple directories into a SQLite database.
    This version uses dedicated worker pools for each drive to enhance concurrent disk I/O.

    Args:
        dbpath (str): Path to the SQLite database file.
        dirpaths (List[str]): List of paths to the music directories (mount points).
        workers (Optional[int]): Number of worker processes to dedicate to each drive's processing pool.
                                 If None, it calculates workers as (total CPU cores) // (number of active drives).
        chunk_size (int): Number of files to process per chunk.
    """
    logging.info("Starting optimized import process...")
    start_time = time.time()

    # Phase 1: Parallel Scan all directories to identify audio files on each drive.
    # This phase uses a ThreadPoolExecutor as scanning is I/O-bound.
    logging.info("Phase 1: Scanning directories in parallel...")
    # Determine the number of worker threads for scanning. Max out at 16 (common CPU core count) or number of drives.
    scan_threads = min(len(dirpaths), multiprocessing.cpu_count(), 16)
    drive_files = parallel_scantree(dirpaths, scan_threads) # Pass scan_threads
    logging.info("Phase 1 Complete.")

    total_files_to_process = sum(len(files) for files in drive_files.values())
    if total_files_to_process == 0:
        logging.info("No audio files found across all specified directories. Exiting.")
        return

    # Determine the number of worker processes to assign PER DRIVE.
    # This calculation aims to distribute available CPU cores efficiently among the active drives.
    num_cpu_cores = multiprocessing.cpu_count()
    active_drives_count = len(drive_files)

    if workers is None:
        # Default strategy: distribute CPU cores as evenly as possible among active drives.
        # Ensure at least 1 worker process per drive.
        workers_per_drive = max(1, num_cpu_cores // active_drives_count)
        logging.info(f"Auto-determining workers: {num_cpu_cores} CPU cores / {active_drives_count} drives = {workers_per_drive} workers per drive.")
    else:
        # If user explicitly specified workers, use that number for each drive's pool.
        workers_per_drive = max(1, workers)
        logging.info(f"Using user-specified {workers_per_drive} worker processes per drive.")


    # Phase 2: Process tags for each drive using its own dedicated ProcessPoolExecutor.
    # An outer ThreadPoolExecutor (drive_manager_executor) manages the concurrent launch
    # and monitoring of these per-drive ProcessPoolExecutors.
    logging.info("Phase 2: Processing tags in parallel (dedicated pool per drive)...")
    all_tags: List[Dict[str, Any]] = []
    total_processed_files = 0
    total_failed_files = 0

    # `drive_manager_executor` allows simultaneous execution of `process_single_drive` for multiple drives.
    # Its `max_workers` is set to the number of drives, enabling concurrent drive processing.
    with ThreadPoolExecutor(max_workers=active_drives_count) as drive_manager_executor:
        drive_processing_futures = []
        for drive_path, files in drive_files.items():
            if files: # Only submit processing for drives that actually have files
                drive_processing_futures.append(
                    drive_manager_executor.submit(
                        process_single_drive, drive_path, files, chunk_size, workers_per_drive
                    )
                )

        # Collect results from each drive's processing as they complete.
        for future in concurrent.futures.as_completed(drive_processing_futures):
            try:
                drive_tags, drive_stats = future.result()
                all_tags.extend(drive_tags) # Consolidate all tags into one list
                total_processed_files += drive_stats["processed_files"]
                total_failed_files += drive_stats["failed_files"]
            except Exception as e:
                logging.error(f"An error occurred in a drive's processing task: {e}")
                # Note: Exact failed file count from inner process might be lost on critical failure here.

    logging.info("Phase 2 Complete.")
    logging.info(f"Summary: Total files processed successfully: {total_processed_files}")
    if total_failed_files > 0:
        logging.warning(f"Summary: Total files failed to process: {total_failed_files}")

    if not all_tags:
        logging.info("No tags successfully processed across all drives. Exiting.")
        return

    # Phase 3: Build a single Polars DataFrame from all collected tags.
    logging.info("Phase 3: Building Polars DataFrame from consolidated tags...")
    df = build_dataframe_with_schema(all_tags)
    logging.info(f"DataFrame built with {len(df)} records.")
    logging.info("Phase 3 Complete.")

    # Phase 4: Write the Polars DataFrame to the SQLite Database.
    # This phase is sequential as it involves a single database connection.
    logging.info("Phase 4: Writing to SQLite database...")
    try:
        with sqlite3.connect(dbpath) as conn:
            create_and_migrate_db(dbpath, conn, df.columns)

            # Convert Polars DataFrame to a list of lists for sqlite3 executemany.
            # Ensure the order of columns matches the database schema for correct insertion.
            # column_order = list(ALBUM_INFO_SCHEMA.keys())
            column_order = list(df.columns)
            data_to_insert = df.select(column_order).to_numpy().tolist()

            # Using INSERT OR REPLACE for UPSERT functionality:
            # New records are inserted, existing records (identified by 'filename' PRIMARY KEY) are updated.
            # placeholders = ', '.join(['?'] * len(column_order))
            # columns_str = ', '.join(column_order)
            # insert_sql = f"INSERT OR REPLACE INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

            quoted_columns = [f'"{col}"' for col in column_order]
            placeholders = ', '.join(['?'] * len(column_order))
            columns_str = ', '.join(quoted_columns)
            insert_sql = f'INSERT OR REPLACE INTO "{TABLE_NAME}" ({columns_str}) VALUES ({placeholders})'


            cursor = conn.cursor()
            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()
            logging.info(f"Successfully inserted/updated {len(data_to_insert)} records into {dbpath}")

    except sqlite3.Error as e:
        logging.error(f"SQLite database error during write operation: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during database write: {e}", exc_info=True)
        sys.exit(1)
    logging.info("Phase 4 Complete.")

    end_time = time.time()
    logging.info(f"Import process finished in {end_time - start_time:.2f} seconds.")

def clean_values_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """Clean all values in DataFrame using vectorized operations."""
    string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

    # Process each column individually
    for col in string_cols:
        # First handle empty strings by converting to None
        df = df.with_columns(
            pl.when(
                pl.col(col).is_null() | (pl.col(col).str.strip_chars() == "")
            )
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )

        # Then handle multi-value tags - but keep as String type
        # Split by double backslash and rejoin with single backslash or other delimiter
        df = df.with_columns(
            pl.when(pl.col(col).str.contains(r"\\\\"))
            .then(
                pl.col(col)
                .str.split(r"\\\\")
                .list.join("\\")  # Join with single backslash
            )
            .otherwise(pl.col(col))
            .alias(col)
        )
    return df


def build_path_filter_condition(dirpath: str) -> str:
    """Build SQL condition for path filtering.

    Args:
        dirpath: Directory path to filter by

    Returns:
        SQL WHERE condition string
    """
    # Normalize path and ensure it ends with separator
    normalized_path = os.path.normpath(dirpath)
    if not normalized_path.endswith(os.path.sep):
        normalized_path += os.path.sep

    # Use GLOB for efficient path matching
    # GLOB is case-sensitive and works well for path prefixes
    return f"__path GLOB '{normalized_path}*'"

def process_files(df: pl.DataFrame, preserve_mtime: bool) -> dict:
    stats = {"processed": 0, "errors": 0, "skipped": 0}
    tag_cols = [col for col in df.columns if not col.startswith('__')]

    for row in df.iter_rows(named=True):
        try:
            if not os.path.exists(row["__path"]):
                stats["skipped"] += 1
                continue

            tag = audioinfo.Tag(row["__path"])
            for col in tag_cols:
                if (val := row[col]) is not None:
                    tag[col] = val.split('\\\\') if '\\\\' in str(val) else val

            tag.save()

            if preserve_mtime:
                try:
                    mtime = float(row["__file_mod_datetime_raw"])
                    os.utime(row["__path"], times=(mtime, mtime))
                except Exception as e:
                        logging.warning(f"Could not preserve mtime for {row['__path']}: {str(e)}")

            stats["processed"] += 1

        except Exception as e:
            stats["errors"] += 1
            logging.error(f"Failed {row['__path']}: {str(e)}")

    return stats


def export_db(dbpath: str, dirpath: str, preserve_mtime: bool = True) -> None:
    """Export database to audio files using optimized DataFrame operations with improved path handling."""
    try:
        # Connect to database
        logging.info(f"Reading database from {dbpath}...")
        conn = sqlite3.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES)

        # Build path filter condition
        path_condition = build_path_filter_condition(dirpath)

        # Query schema to build explicit schema for Polars
        schema_query = f"PRAGMA table_info({TABLE_NAME})"
        schema_df = pl.read_database(query=schema_query, connection=conn)
        table_schema = {col_name: pl.Utf8 for col_name in schema_df["name"]}

        # First, get just the paths to validate file existence
        path_query = f"""
        SELECT __path FROM {TABLE_NAME}
        WHERE {path_condition}
        ORDER BY __path
        """

        logging.info("Querying candidate file paths...")
        path_df = pl.read_database(
            query=path_query,
            connection=conn,
            schema_overrides={"__path": pl.Utf8}
        )

        if path_df.is_empty():
            logging.warning(f"No database records found for directory: {dirpath}")
            conn.close()
            return

        total_candidates = len(path_df)
        logging.info(f"Found {total_candidates} database records matching path filter")

        # Pre-filter for existing files only
        logging.info("Validating file existence for candidate records...")
        candidate_paths = path_df.get_column("__path").to_list()
        existing_paths = []

        for filepath in candidate_paths:
            if os.path.exists(filepath):
                existing_paths.append(filepath)

        existing_count = len(existing_paths)
        skipped_count = total_candidates - existing_count

        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} database entries - files not found on disk")

        if existing_count == 0:
            logging.warning(f"No files found on disk for any database records under: {dirpath}")
            conn.close()
            return

        logging.info(f"Will process {existing_count} files that exist on disk")

        # Query records in batches to avoid SQL variable limit
        batch_size = 500  # Well below SQLite's default 999 variable limit
        total_batches = (existing_count + batch_size - 1) // batch_size
        all_dfs = []

        logging.info(f"Querying database in batches of {batch_size}...")
        for i in range(0, existing_count, batch_size):
            batch_paths = existing_paths[i:i + batch_size]
            placeholders = ','.join(['?'] * len(batch_paths))
            final_query = f"""
            SELECT * FROM {TABLE_NAME}
            WHERE __path IN ({placeholders})
            ORDER BY __path
            """

            batch_df = pl.read_database(
                query=final_query,
                connection=conn,
                execute_options={"parameters": batch_paths},
                schema_overrides=table_schema
            )
            all_dfs.append(batch_df)

            # Log progress every 10 batches or on last batch
            if (i // batch_size) % 10 == 0 or (i + batch_size >= existing_count):
                logging.info(f"Processed batch {i//batch_size + 1}/{total_batches}")

        # Combine all batches into single DataFrame - already sorted by __path
        df = pl.concat(all_dfs)
        conn.close()

        logging.info(f"Loaded {len(df)} records for processing (sorted by __path for locality)")

        # Vectorized data cleaning
        logging.info("Applying vectorized data cleaning...")
        df_cleaned = clean_values_vectorized(df)

        # Memory layout optimization with columnar processing
        logging.info("Processing files with optimized path handling...")
        stats = process_files_with_directory_grouping(df_cleaned, batch_size=1000)

        logging.info(f'Export complete. Processed: {stats["processed"]}, '
                    f'Errors: {stats["errors"]}, Skipped: {stats["skipped"]}')

    except Exception as e:
        logging.error(f"Error during export: {str(e)}", exc_info=True)
        if 'conn' in locals():
            conn.close()
        raise


def process_files_with_directory_grouping(df: pl.DataFrame, batch_size: int = 1000) -> Dict[str, int]:
    """Alternative approach: Group by directory for even better locality.

    This version processes files directory by directory, which can be even more
    efficient for disk I/O patterns.
    """
    stats = {"processed": 0, "errors": 0, "skipped": 0}

    # Get exportable columns
    exportable_columns = [col for col in df.columns if not col.startswith('__')]
    logging.info(f"Will export {len(exportable_columns)} tag fields (excluding __ fields)")

    # Group by __dirpath if available, otherwise extract from __path
    if "__dirpath" in df.columns:
        # Use the existing __dirpath column for grouping
        grouped = df.group_by("__dirpath", maintain_order=True)
    else:
        # Fallback: extract directory from __path
        df = df.with_columns(
            pl.col("__path").map_elements(lambda x: os.path.dirname(x), return_dtype=pl.Utf8).alias("__dirpath")
        )
        grouped = df.group_by("__dirpath", maintain_order=True)

    # Get the groups as a dictionary
    dir_groups = dict(grouped)

    total_directories = len(dir_groups)
    processed_directories = 0

    logging.info(f"Processing {len(df)} files across {total_directories} directories")

    # Process each directory group
    for dirpath, dir_df in dir_groups.items():
        try:
            # Process files in this directory
            filepaths = dir_df.get_column("__path").to_list()

            # Pre-extract tag data for this directory
            tag_columns = {}
            for col in exportable_columns:
                tag_columns[col] = dir_df.get_column(col).to_list()

            # Process each file in the directory
            for i, filepath in enumerate(filepaths):
                try:
                    if not os.path.exists(filepath):
                        stats["skipped"] += 1
                        logging.warning(f'File not found, skipping: {filepath}')
                        continue

                    # Build tag dictionary
                    tag_values = {col: tag_columns[col][i] for col in exportable_columns}

                    # Process the file using full path
                    tag = audioinfo.Tag(filepath)

                    # Update tags
                    for key, value in tag_values.items():
                        if value is None or (isinstance(value, str) and value.strip() == ""):
                            if key in tag:
                                del tag[key]
                        elif isinstance(value, str) and '\\\\' in value:
                            tag[key] = value.split('\\\\')
                        elif isinstance(value, str) and '\\' in value and not key.endswith('path'):
                            tag[key] = value.split('\\')
                        else:
                            tag[key] = value


                    tag.save()
                    stats["processed"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    logging.error(f'Could not update {filepath}: {str(e)}')

            processed_directories += 1

            # Progress logging
            if processed_directories % 100 == 0 or processed_directories == total_directories:
                logging.info(f'Processed {processed_directories}/{total_directories} directories '
                           f'({stats["processed"]} files so far)...')

        except Exception as e:
            logging.error(f'Error processing directory {dirpath}: {str(e)}')
            continue

    return stats


def setup_logging(level: str) -> None:
    """Set up logging configuration.

    Args:
        level: Logging level
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    try:
        level = getattr(logging, level.upper(), logging.INFO)
    except AttributeError:
        level = logging.INFO
        print(f"Invalid log level: {level}, defaulting to INFO", file=sys.stderr)

    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[logging.StreamHandler()]
    )


def main() -> None:
    """Main entry point with support for parallel processing and multiple directories."""
    parser = argparse.ArgumentParser(
        description='Import/Export audio file tags to/from SQLite database.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')
    subparsers.required = True

    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import audio files to database')
    import_parser.add_argument('dbpath', help='Path to SQLite database')
    import_parser.add_argument(
        'musicdirs',
        nargs='+',
        help='Paths to music directories to import (can specify multiple)'
    )
    import_parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of worker processes for tag processing PER DRIVE. '
             'If not specified, defaults to CPU count // number of active drives.'
    )
    import_parser.add_argument(
        '--chunk-size',
        type=int,
        default=4000,
        help='Number of files to process per chunk (for tag reading). Default is 4000.'
    )

    # Export subcommand
    export_parser = subparsers.add_parser('export', help='Export database to audio files')
    export_parser.add_argument('--ignore-lastmodded',action='store_true',help="Don't preserve last modification timestamps when writing tags to file")
    export_parser.add_argument('dbpath', help='Path to SQLite database')
    export_parser.add_argument('musicdir', help='Path to music directory to export to')

    # Common arguments
    for p in [import_parser, export_parser]:
        p.add_argument(
            '--log',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            default='INFO',
            help='Log level'
        )

    try:
        args = parser.parse_args()
        setup_logging(args.log)

        # Validate paths
        if args.action == 'import':
            invalid_paths = [p for p in args.musicdirs if not os.path.exists(p)]
            if invalid_paths:
                logging.error(f"Error: One or more specified music directories do not exist: {', '.join(invalid_paths)}")
                sys.exit(1)

            dbpath = os.path.realpath(args.dbpath)
            musicdirs = [os.path.realpath(p) for p in args.musicdirs]

            logging.info(f"Starting import operation on {len(musicdirs)} directories:")
            for i, path in enumerate(musicdirs, 1):
                logging.info(f"  {i}. {path}")

            if args.workers is not None and args.workers < 1:
                logging.warning("Warning: Worker count must be 1 or greater. Using default calculation for workers per drive.")
                args.workers = None # Reset to None to trigger default calculation

            import_dir_optimized(
                dbpath=dbpath,
                dirpaths=musicdirs,
                workers=args.workers,
                chunk_size=args.chunk_size
            )

        else:  # export
            musicdir_for_export = args.musicdir

            if not os.path.exists(musicdir_for_export):
                logging.error(f"Error: Music directory for export does not exist: {musicdir_for_export}")
                sys.exit(1)

            dbpath = os.path.realpath(args.dbpath)
            musicdir_resolved = os.path.realpath(musicdir_for_export)

            logging.info(f"Starting export operation filtered by music directory: {musicdir_resolved}")
            export_db(dbpath, musicdir_resolved, preserve_mtime=not args.ignore_lastmodded)

    except Exception as e:
        logging.error(f"An unhandled error occurred during script execution: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
