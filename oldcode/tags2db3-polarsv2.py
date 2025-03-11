#!/usr/bin/env python3

import argparse
import os
import sqlite3
import sys
import logging
import re
from typing import Dict, List, Union, Set, Optional, Any, Tuple, Iterator

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


def scantree(path: str) -> Iterator[os.DirEntry]:
    """Recursively yield DirEntry objects for given directory.
    
    Args:
        path: Directory path to scan
        
    Yields:
        DirEntry objects for audio files
    """
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
                    yield entry
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
    # print("\n".join(f"{k}: {v}" for k, v in tag_dict.items()))
    # input()
    
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


def import_dir(dbpath: str, dirpath: str) -> None:
    """Import directory of audio files into database using DataFrame.
    
    Args:
        dbpath: Path to SQLite database
        dirpath: Directory path to import
    """

    # Define baseline of columns one might expect to see in the tags and set them in the order you want them to appear in the db
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
        "album",
        "version",
        "_releasecomment",
        "discsubtitle",
        "work",
        "movement",
        "part",
        "live",
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


    try:
        all_tags = []
        stats = {"processed": 0, "errors": 0}
        
        # First pass: collect all tag dictionaries
        logging.info("Ingesting tags from all files...")
        for entry in scantree(dirpath):
            try:
                logging.debug(f"Reading tags from: {entry.path}")
                tag = audioinfo.Tag(entry.path)
                
                if tag is not None:
                    all_tags.append(tag_to_dict(tag))
                    stats["processed"] += 1
                    if stats["processed"] % 5000 == 0:
                        logging.info(f"Processed {stats['processed']} files...")
                else:
                    logging.warning(f"Invalid file: {entry.path}")
            except Exception as e:
                stats["errors"] += 1
                logging.error(f"Could not read tags from: {entry.path}: {str(e)}")
        
        if not all_tags:
            logging.warning("No valid tags found to import.")
            return

        # # Test to see if discnumber has survived unaltered to this point
        # for item in all_tags:
        #     if 'discnumber' in item:
        #         print(f"Discnumber present")

        #     else:
        #         print(f"\nDiscnumber not present")

        #     print(item)


        # Pander to Polars foibles ... it cracks the shits if the list from which you create a dataframe doesn't have all keys present in all list items, so get got to populate nulls...
        # Get all possible keys from all dictionaries in every list item
        all_keys = get_unique_keys(all_tags)
        # Convert all to lowercase and get rid of any duplicates arising from case differences
        all_keys = list(set([i.lower() for i in all_keys]))

        # identify and isolate whatever other keys happen to be present in the tags that are not included in fixed_columns
        # extra_columns = [col.lower() for col in df.columns if col.lower() not in fixed_columns]
        extra_columns = [col for col in all_keys if col not in fixed_columns]

        # # Print them for posterity
        # for col in extra_columns:
        #     print(f"Extra column: {col}")

        # Create final set of columns in the order they are to be written to DF and database
        final_columns = fixed_columns + extra_columns

        # Ensure each dictionary has all keys and set value to None if the dictionary is missing a key
        for item in all_tags:
            for key in final_columns:
                if key not in item:
                    item[key] = None

        # for col in final_columns:
        #     print(f"Final column: {col}")

        # Create DataFrame from all collected tags
        logging.info(f"Creating DataFrame with {len(all_tags)} records.")

        df = pl.DataFrame(all_tags, schema_overrides={key: pl.Utf8 for key in all_keys}) # Treat all as strings initially. This gets around Polars' idiotic col string length being determined by the first item in a df col.


        # # Test to see if discnumber has made it to the dataframe
        # for col in df.columns:
        #     if col == 'discnumber':
        #         print(f"Discnumber in df")
        #     else:
        #         print(f"Discnumber not in df")

        # for col in final_columns:
        #     print(col)
        # input()

        # Reorder the DataFrame
        df = df.select(final_columns)
    
        logging.info(f"{len(final_columns)} tag names and associated values ingested from {df.height} files.")
    
      
        # Write to SQLite in one operation
        logging.info("Writing to SQLite database...")
        
        # Initialize database with just the path column to ensure it exists
        conn = sqlite3.connect(dbpath)
        
        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS alib")
        
        # Create table with all columns from DataFrame
        # Ensure __path is the primary key
        columns = df.columns
        columns_sql = []
        
        for col in columns:
            if col == "__path":
                columns_sql.append(f'"{col}" blob PRIMARY KEY')
            else:
                columns_sql.append(f'"{col}" text')
        # for x in columns_sql:
        #     print(x)
        # input()
        create_table_sql = f"CREATE TABLE alib ({', '.join(columns_sql)})"
        conn.execute(create_table_sql)
        
        # Convert DataFrame to records and insert all at once
        logging.info("Inserting records into database...")
        
        # Prepare column names and placeholders for SQL
        column_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["?"] * len(columns))
        insert_sql = f"INSERT INTO alib ({column_names}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples for executemany
        records = df.to_numpy().tolist()
        
        # Use executemany for bulk insert
        conn.executemany(insert_sql, records)
        conn.commit()
        conn.close()
        
        logging.info(f"Import completed. Processed: {stats['processed']}, Errors: {stats['errors']}")
        
    except Exception as e:
        logging.error(f"Error during import: {str(e)}")
        raise


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
        # Convert filter object to list before passing to remove_dupes
        # return remove_dupes(list(filter(None, items)))
        return list(filter(None, items))
    
    return value


def export_db(dbpath: str, dirpath: str) -> None:
    """Export database to audio files using DataFrame."""
    try:
        # Connect to database
        logging.info(f"Reading database from {dbpath}...")
        conn = sqlite3.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES)
        
        # First query just the paths
        query = "SELECT __path FROM alib"
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
        query = f"SELECT * FROM alib WHERE __path IN ({placeholders})"
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
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Import/Export audio file tags to/from SQLite database.')
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')
    subparsers.required = True
    
    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import audio files to database')
    import_parser.add_argument('dbpath', help='Path to SQLite database')
    import_parser.add_argument('musicdir', help='Path to music directory to import')
    
    # Export subcommand
    export_parser = subparsers.add_parser('export', help='Export database to audio files')
    export_parser.add_argument('dbpath', help='Path to SQLite database')
    export_parser.add_argument('musicdir', help='Path to music directory to export to')
    
    # Common arguments
    for p in [import_parser, export_parser]:
        p.add_argument('--log', 
                      help='Log level (DEBUG, INFO, WARNING, ERROR)',
                      default='INFO')
    
    try:
        args = parser.parse_args()
        setup_logging(args.log)
        
        # Validate paths
        if not os.path.exists(args.musicdir):
            logging.error(f"Directory does not exist: {args.musicdir}")
            sys.exit(1)
            
        dbpath = os.path.realpath(args.dbpath)
        musicdir = os.path.realpath(args.musicdir)
        
        logging.info(f"Starting {args.action} operation on {musicdir}")
        
        if args.action == 'import':
            import_dir(dbpath, musicdir)
        else:  # export
            export_db(dbpath, musicdir)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    
    
if __name__ == '__main__':
    main()
