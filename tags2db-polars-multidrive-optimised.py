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

def create_and_migrate_db(dbpath: str, conn: sqlite3.Connection):
    """
    Creates the main database table if it doesn't exist.
    """
    cursor = conn.cursor()
    # Define table creation SQL based on the Polars schema
    columns_sql = []
    for col, dtype in ALBUM_INFO_SCHEMA.items():
        sql_type = "TEXT" # Default to TEXT for simplicity and flexibility
        if dtype == pl.Int64:
            sql_type = "INTEGER"
        elif dtype == pl.Float64:
            sql_type = "REAL"

        # filename is the primary key
        if col == "__path":
            columns_sql.append(f"{col} {sql_type} PRIMARY KEY")
        else:
            columns_sql.append(f"{col} {sql_type}")

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {", ".join(columns_sql)}
        )
    ''')
    conn.commit()
    logging.info(f"Database table '{TABLE_NAME}' ensured to exist.")


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
            create_and_migrate_db(dbpath, conn)

            # Convert Polars DataFrame to a list of lists for sqlite3 executemany.
            # Ensure the order of columns matches the database schema for correct insertion.
            column_order = list(ALBUM_INFO_SCHEMA.keys())
            data_to_insert = df.select(column_order).to_numpy().tolist()

            # Using INSERT OR REPLACE for UPSERT functionality:
            # New records are inserted, existing records (identified by 'filename' PRIMARY KEY) are updated.
            placeholders = ', '.join(['?'] * len(column_order))
            columns_str = ', '.join(column_order)
            insert_sql = f"INSERT OR REPLACE INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

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


def process_files_columnar(df: pl.DataFrame, batch_size: int = 1000) -> Dict[str, int]:
    """Process files using columnar operations where possible, with batched row operations.
    Only exports tags that don't begin with '__'.

    Args:
        df: DataFrame with cleaned tag data
        batch_size: Number of files to process per batch

    Returns:
        Dictionary with processing statistics
    """
    stats = {"processed": 0, "errors": 0, "skipped": 0}

    # Pre-filter to only get non-__ tag columns for export
    exportable_columns = [col for col in ALBUM_INFO_SCHEMA.keys()
                         if not col.startswith('__')]

    logging.info(f"Will export {len(exportable_columns)} tag fields (excluding __ fields)")

    # Process in batches to balance memory usage and performance
    total_rows = len(df)
    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)

        # Extract batch data using columnar operations
        batch_df = df.slice(batch_start, batch_end - batch_start)

        # Pre-extract filepaths
        filepaths = batch_df.get_column("__path").to_list()

        # Pre-extract ONLY exportable tag columns (no __ fields)
        tag_columns = {}
        for col in exportable_columns:
            if col in batch_df.columns:
                tag_columns[col] = batch_df.get_column(col).to_list()
            else:
                tag_columns[col] = [None] * len(batch_df)

        # Process each file in the batch
        for i, filepath in enumerate(filepaths):
            try:
                # Verify the file exists at the database path
                if not os.path.exists(filepath):
                    stats["skipped"] += 1
                    logging.warning(f'File not found, skipping: {filepath}')
                    continue

                # Build tag dictionary from ONLY exportable columns (no __ fields)
                tag_values = {col: tag_columns[col][i] for col in exportable_columns}

                # WORKAROUND: Change working directory to file's directory to avoid filepath issues
                original_cwd = os.getcwd()
                file_dir = os.path.dirname(filepath)
                filename = os.path.basename(filepath)

                try:
                    os.chdir(file_dir)

                    # Load the file using just the filename to avoid path issues
                    tag = audioinfo.Tag(filename)

                    # Clear existing NON-__ tags (preserve internal audioinfo fields)
                    tag_keys_to_remove = [key for key in tag.keys() if not key.startswith('__')]
                    for key in tag_keys_to_remove:
                        del tag[key]

                    # Set new values from database (all are already non-__ fields)
                    for key, value in tag_values.items():
                        if value is None or value == "":
                            # Skip empty/null values (already cleared above)
                            continue
                        else:
                            # Handle multi-value fields (those with backslash separators)
                            if isinstance(value, str) and '\\' in value and not key.endswith('path'):
                                # Split on single backslash and create list
                                tag[key] = value.split('\\')
                            else:
                                tag[key] = value

                    # Save the file
                    tag.save()

                finally:
                    # Always restore original working directory
                    os.chdir(original_cwd)

                stats["processed"] += 1

            except Exception as e:
                stats["errors"] += 1
                logging.error(f'Could not update {filepath}: {str(e)}')

        # Progress logging per batch
        if batch_end % 5000 == 0 or batch_end == total_rows:
            logging.info(f'Processed {batch_end}/{total_rows} files...')

    return stats


def export_db(dbpath: str, dirpath: str) -> None:
    """Export database to audio files using optimized DataFrame operations with file existence pre-filtering."""
    try:
        # Connect to database
        logging.info(f"Reading database from {dbpath}...")
        conn = sqlite3.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES)

        # OPTIMIZATION 1: Get candidate records with path filtering
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

        # OPTIMIZATION 2: Pre-filter for existing files only
        logging.info("Validating file existence for candidate records...")
        candidate_paths = path_df.get_column("__path").to_list()
        existing_paths = []

        for filepath in candidate_paths:
            if os.path.exists(filepath):
                existing_paths.append(filepath)

        existing_count = len(existing_paths)
        skipped_count = total_candidates - existing_count

        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} database entries - records do not match the specified export destination: {dirpath}")

        if existing_count == 0:
            logging.warning(f"No files found on disk for any database records under: {dirpath}")
            conn.close()
            return

        logging.info(f"Will process {existing_count} files that exist on disk")

        # OPTIMIZATION 3: Query only records for existing files
        # Build IN clause for existing paths (using parameterized query for safety)
        placeholders = ','.join(['?' for _ in existing_paths])
        final_query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE __path IN ({placeholders})
        ORDER BY __path
        """

        logging.info("Executing optimized database query for existing files only...")
        df = pl.read_database(
            query=final_query,
            connection=conn,
            execute_options={"parameters": existing_paths},
            schema_overrides=table_schema
        )
        conn.close()

        logging.info(f"Loaded {len(df)} records for processing")

        # OPTIMIZATION 4: Vectorized data cleaning
        logging.info("Applying vectorized data cleaning...")
        df_cleaned = clean_values_vectorized(df)

        # OPTIMIZATION 5: Memory layout optimization with columnar processing
        logging.info("Processing files with memory-optimized columnar operations...")
        stats = process_files_columnar(df_cleaned, batch_size=1000)

        logging.info(f'Export complete. Processed: {stats["processed"]}, '
                    f'Errors: {stats["errors"]}, Skipped: {stats["skipped"]}')

    except Exception as e:
        logging.error(f"Error during export: {str(e)}")
        raise


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
            export_db(dbpath, musicdir_resolved)

    except Exception as e:
        logging.error(f"An unhandled error occurred during script execution: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
