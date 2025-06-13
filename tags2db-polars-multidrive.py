"""
Script Name: tags2db3-polarsv2.py

Purpose:
    This script imports/exports all metadata tags from the folder tree it is run in or passed as an argument and writes it to a SQLite database table alib.
    
    It is part of tagminder.

Usage:
    python tags2db3-polarsv2
    uv run tags2db3-polarsv2


    python tags2db-polars-multidrive.py import /path/to/db.sqlite /qnap/qnap1 /qnap/qnap2 /qnap/qnap3 --workers 24 --chunk-size 2000


Author: audiomuze
Created: 2025-03-17
"""

#!/usr/bin/env python3

import argparse
import os
import sqlite3
import sys
import logging
import re
from typing import Dict, List, Union, Set, Optional, Any, Tuple, Iterator
import multiprocessing
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import time

try:
    from os import scandir
except ImportError:
    from scandir import scandir  # use scandir PyPI module on Python < 3.5

# Required dependencies
try:
    import puddlestuff
    from puddlestuff import audioinfo
except ImportError:
    print("Error: puddlestuff module not found. Please install it first.", file=sys.stderr)
    sys.exit(1)

try:
    import polars as pl
except ImportError:
    print("Error: polars module not found. Please install it with: pip install polars", file=sys.stderr)
    sys.exit(1)


# Define baseline of columns one might expect to see in the tags and set them in the order you want them to appear in the db
# (module-level constant)
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
    "bliss_analysis",
    "tagminder_uuid",
    "sqlmodded",
    "reflac",
    "discnumber",
    "track",
    "title",
    "subtitle",
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
    "__albumgain"
    ]

def process_file(filepath: str) -> Optional[Dict[str, Any]]:
    """Process a single file and return its tag dictionary."""
    try:
        logging.debug(f"Reading tags from: {filepath}")
        tag = audioinfo.Tag(filepath)
        return tag_to_dict(tag) if tag is not None else None
    except Exception as e:
        logging.error(f"Could not read tags from: {filepath}: {str(e)}")
        return None


def issubfolder(parent: str, child: str) -> bool:
    """Check if child is a subfolder of parent.

    Args:
        parent: Parent path
        child: Child path

    Returns:
        True if child is a subfolder of parent
    """
    # Normalize paths for cross-platform compatibility
    parent = os.path.normpath(parent)
    child = os.path.normpath(child)
    
    # Ensure paths end with path separator for proper subfolder checking
    if not parent.endswith(os.path.sep):
        parent += os.path.sep
        
    return child.startswith(parent)

def parallel_scantree(dirpaths: List[str], workers: int) -> Dict[str, List[str]]:
    """Scan multiple directories in parallel, returning {path: [filepaths]}."""
    from concurrent.futures import ThreadPoolExecutor

    def scan_single(path: str) -> Tuple[str, List[str]]:
        return (path, list(scantree(path)))

    drive_files = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scan_single, path): path for path in dirpaths}
        for future in concurrent.futures.as_completed(futures):
            path, files = future.result()
            drive_files[path] = files
    return drive_files


def scantree(path: str) -> Iterator[str]:
    """Recursively yield file paths for given directory."""
    audio_extensions = {'.flac', '.wv', '.mp3', '.m4a', '.ape'}
    
    try:
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                try:
                    yield from scantree(entry.path)
                except (PermissionError, OSError) as e:
                    logging.warning(f"Could not scan directory {entry.path}: {str(e)}")
            else:
                ext = os.path.splitext(entry.name)[1].lower()
                if ext in audio_extensions:
                    yield entry.path  # Return path instead of DirEntry
    except (PermissionError, OSError) as e:
        logging.error(f"Could not scan directory {path}: {str(e)}")


def tag_to_dict(tag: Any) -> Dict[str, Any]:
    """Convert a puddlestuff Tag object to a dictionary suitable for DataFrame.
    
    Args:
        tag: The Tag object
        
    Returns:
        Dictionary representation of the tag
    """
    tag_dict = dict(tag)
    tag_dict['__path'] = tag.filepath
    
    # Handle list values
    for k, v in list(tag_dict.items()):
        # Remove quotes from tag names (they're illegal in SQLite column names and shouldn't appear in tag names either)
        if '"' in k:
            safe_k = k.replace('"', '')
            tag_dict[safe_k] = tag_dict.pop(k)
            k = safe_k

        # Convert lists to string with \\ delimiter
        if not isinstance(v, (int, float, str)):
            tag_dict[k] = "\\\\".join(str(item) for item in v)
            
    return tag_dict


def get_unique_keys(list_of_dicts):
    """Memory-efficient version for very large lists"""
    keys_set = set()
    for d in list_of_dicts:
        keys_set.update(d.keys())
    return sorted(keys_set)


def import_dir(
    dbpath: str,
    dirpaths: Union[str, List[str]],
    workers: Optional[int] = None,
    chunk_size: int = 2000,
    max_total_workers: int = 32
) -> None:
    """Import directory/directories of audio files into database using parallel processing.
    
    Args:
        dbpath: Path to SQLite database
        dirpaths: Single directory path or list of paths to import
        workers: Total workers to use (None for automatic calculation)
        chunk_size: Number of files to process per worker chunk (default: 2000)
        max_total_workers: Maximum total workers across all drives (default: 32)
        
    Raises:
        ValueError: If invalid parameters are provided
        sqlite3.Error: If database operations fail
        OSError: If directory scanning fails
    """
    # Validate inputs
    if not dbpath or not dirpaths:
        raise ValueError("dbpath and dirpaths must be specified")
    if chunk_size < 1:
        raise ValueError("chunk_size must be ≥ 1")
    
    # Convert single path to list for uniform handling
    if isinstance(dirpaths, str):
        dirpaths = [dirpaths]
    
    # Calculate total workers
    if workers is not None:
        if workers < 1:
            raise ValueError("workers must be ≥ 1")
        total_workers = min(workers, max_total_workers)
        workers_info = f"{total_workers} total workers"
    else:
        # Default behavior: 8 workers per drive, capped at max_total_workers
        workers_per_drive = 8
        total_workers = min(workers_per_drive * len(dirpaths), max_total_workers)
        workers_info = f"{total_workers} workers ({workers_per_drive} per drive)"
    
    try:
        # --- PARALLEL SCANNING SECTION ---
        logging.info(f"Scanning {len(dirpaths)} directories in parallel...")
        scan_workers = min(len(dirpaths), max_total_workers)
        drive_files = parallel_scantree(dirpaths, scan_workers)
        total_files = sum(len(files) for files in drive_files.values())
        
        # Validate scan results
        missing_dirs = [p for p in dirpaths if p not in drive_files]
        if missing_dirs:
            raise OSError(f"Failed to scan directories: {missing_dirs}")

        if total_files == 0:
            logging.warning("No audio files found to import.")
            return
        
        # --- PARALLEL PROCESSING SECTION ---
        logging.info(f"Processing {total_files} files with {workers_info}...")
        start_time = time.time()
        stats = {"processed": 0, "errors": 0}
        all_tags: List[Dict[str, Any]] = []
        
        with ProcessPoolExecutor(max_workers=total_workers) as executor:
            futures = []
            
            # Submit chunks per drive
            for drive_path, files in drive_files.items():
                for i in range(0, len(files), chunk_size):
                    chunk = files[i:i + chunk_size]
                    futures.append(executor.submit(process_chunk, chunk))
            
            # Process completed futures
            for future in concurrent.futures.as_completed(futures):
                try:
                    chunk_tags, chunk_stats = future.result()
                    all_tags.extend(chunk_tags)
                    stats["processed"] += chunk_stats["processed"]
                    stats["errors"] += chunk_stats["errors"]
                    
                    # Progress reporting
                    elapsed = time.time() - start_time
                    rate = stats["processed"] / elapsed if elapsed > 0 else 0
                    if stats["processed"] % 10000 == 0:
                        logging.info(
                            f"Processed {stats['processed']}/{total_files} files "
                            f"({rate:.1f} files/sec, {len(all_tags)} valid tags)"
                        )
                except Exception as e:
                    logging.error(f"Chunk processing failed: {str(e)}")
                    stats["errors"] += chunk_size
        
        if not all_tags:
            logging.warning("No valid tags found to import.")
            return

        # --- DATABASE WRITING SECTION ---
        logging.info(f"Creating DataFrame with {len(all_tags)} records...")

        # Pander to Polars foibles ... it cracks the shits if the list from which you create a dataframe doesn't have all keys present in all list items, so get got to populate nulls...
        # Get all possible keys from all dictionaries in every list item
        all_keys = get_unique_keys(all_tags)
        # Convert all to lowercase and get rid of any duplicates arising from case differences
        all_keys = list(set([i.lower() for i in all_keys]))

        # identify and isolate whatever other keys happen to be present in the tags that are not included in fixed_columns
        extra_columns = [col for col in all_keys if col not in fixed_columns]

        # Create final set of columns in the order they are to be written to DF and database
        final_columns = fixed_columns + extra_columns

        # Ensure each dictionary has all keys and set value to None if the dictionary is missing a key
        for item in all_tags:
            for key in final_columns:
                if key not in item:
                    item[key] = None

        # Then create DataFrame with EXACTLY these columns
        df = pl.DataFrame(all_tags, schema_overrides={key: pl.Utf8 for key in all_keys})
        df = df.select(final_columns)  # Force the exact column order we want

        
        logging.info("Writing to SQLite database...")
        conn = sqlite3.connect(dbpath)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("DROP TABLE IF EXISTS alib")
            
            # Create table with dynamic schema
            columns_sql = [
                f'"{col}" blob PRIMARY KEY' if col == "__path" else f'"{col}" text'
                for col in df.columns
            ]
            conn.execute(f"CREATE TABLE alib ({', '.join(columns_sql)})")
            
            # Batch insert
            conn.executemany(
                f"INSERT OR REPLACE INTO alib ({', '.join(f'"{col}"' for col in df.columns)}) "
                f"VALUES ({', '.join(['?'] * len(df.columns))})",
                df.to_numpy().tolist()
            )
            conn.commit()
            
        except sqlite3.Error as e:
            conn.rollback()
            raise
        finally:
            conn.close()
        
        # --- FINAL STATS ---
        elapsed_total = time.time() - start_time
        logging.info(
            f"Import completed. Processed: {stats['processed']}, "
            f"Errors: {stats['errors']}, "
            f"Time: {elapsed_total:.1f} seconds, "
            f"Rate: {stats['processed']/elapsed_total:.1f} files/sec"
        )
        
    except OSError as e:
        logging.error(f"Directory operation failed: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Fatal error during import: {str(e)}", exc_info=True)
        raise

def process_chunk(chunk: List[str]) -> Tuple[List[Dict], Dict]:
    """Process a chunk of file paths and return tags and stats."""
    chunk_tags = []
    chunk_stats = {"processed": 0, "errors": 0}
    
    for filepath in chunk:
        try:
            tag = audioinfo.Tag(filepath)
            if tag is not None:
                chunk_tags.append(tag_to_dict(tag))
                chunk_stats["processed"] += 1
            else:
                logging.warning(f"Invalid file: {filepath}")
                chunk_stats["errors"] += 1
        except Exception as e:
            logging.error(f"Could not read tags from: {filepath}: {str(e)}")
            chunk_stats["errors"] += 1
    
    return chunk_tags, chunk_stats

def clean_value_for_export(value: Any) -> Any:
    """Clean values for export to audio files.
    
    Args:
        value: Value to clean
        
    Returns:
        Cleaned value
    """
    if not value:
        return value
        
    if isinstance(value, memoryview):
        return str(value)
    elif isinstance(value, str) and '\\\\' in value:
        # Split by double backslash which is how we stored the multi-value tags
        items = value.split('\\\\')
        return list(filter(None, items))
    
    return value


def export_db(dbpath: str, dirpath: str) -> None:
    """Export database to audio files using DataFrame."""
    try:
        # Connect to database
        logging.info(f"Reading database from {dbpath}...")
        conn = sqlite3.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES)
        
        # First query just the paths
        query = "SELECT __path FROM alib ORDER BY __path"
        paths_df = pl.read_database(query=query, connection=conn)
        
        if paths_df.is_empty():
            logging.warning("No records found in database.")
            return
            
        # Filter for paths in the target directory
        paths = paths_df["__path"].to_list()
        target_paths = [p for p in paths if issubfolder(dirpath, p)]
        if not target_paths:
            logging.warning(f"No files in database are located in {dirpath}.")
            return

        # Query schema information to get column names
        schema_query = "PRAGMA table_info(alib)"
        schema_df = pl.read_database(query=schema_query, connection=conn)
        
        # Build schema dict where every column is pl.Utf8
        table_schema = {col_name: pl.Utf8 for col_name in schema_df["name"]}
        
        # Query the actual data with our explicit schema
        placeholders = ",".join(["?"] * len(target_paths))
        query = f"SELECT * FROM alib WHERE __path IN ({placeholders}) ORDER BY __path"
        df = pl.read_database(
            query=query, 
            connection=conn, 
            execute_options={"parameters": target_paths},
            schema_overrides=table_schema  # Use our explicit schema
        )
        conn.close()
        
        # Process files
        stats = {"processed": 0, "errors": 0, "skipped": 0}
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dicts()
        
        for record in records:
            filepath = record['__path']
            
            try:
                # logging.info(f'Updating {filepath}')
                
                # Clean all values
                for key, value in record.items():
                    record[key] = clean_value_for_export(value)
                
                # Extract non-system values
                new_values = {k: v for k, v in record.items() if not k.startswith('__')}
                
                try:
                    tag = audioinfo.Tag(filepath)
                    
                    # Update tag values
                    for key, value in new_values.items():
                        if value is None or value == "":
                            if key in tag:
                                del tag[key]
                        else:
                            tag[key] = value
                    
                    # Save tag back to file
                    tag.save()
                    audioinfo.setmodtime(tag.filepath, tag.accessed, tag.modified)
                    stats["processed"] += 1
                    logging.info(f'Updated tag for {filepath}')
                    
                except Exception as e:
                    stats["errors"] += 1
                    logging.error(f'Could not save tag to {filepath}: {str(e)}')
            except Exception as e:
                stats["errors"] += 1
                logging.error(f'Error processing {filepath}: {str(e)}')
        
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
        help='Number of parallel workers (default: drives × 2, max 32)'
    )
    import_parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Number of files processed per worker chunk'
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
                logging.error(f"Directory does not exist: {', '.join(invalid_paths)}")
                sys.exit(1)
                
            dbpath = os.path.realpath(args.dbpath)
            musicdirs = [os.path.realpath(p) for p in args.musicdirs]
            
            logging.info(f"Starting import operation on {len(musicdirs)} directories:")
            for i, path in enumerate(musicdirs, 1):
                logging.info(f"  {i}. {path}")
            
            if args.workers is not None and args.workers < 1:
                logging.warning("Worker count must be ≥1, using default")
                args.workers = None
                
            import_dir(
                dbpath=dbpath,
                dirpaths=musicdirs,
                workers=args.workers,
                chunk_size=args.chunk_size
            )
            
        else:  # export
            if not os.path.exists(args.musicdir):
                logging.error(f"Directory does not exist: {args.musicdir}")
                sys.exit(1)
                
            dbpath = os.path.realpath(args.dbpath)
            musicdir = os.path.realpath(args.musicdir)
            
            logging.info(f"Starting export operation on {musicdir}")
            export_db(dbpath, musicdir)
            
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)
    
if __name__ == '__main__':
    main()
