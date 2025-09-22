"""
Script Name: rename_files_and_folders.py

Purpose:
    Renames audio files and their parent directories based on compilation status and album metadata.
    - For VA compilations (compilation='1'): {discnumber}-{track} - {artist} - {title}.{ext}
    - For albumartist albums (compilation='0'): {discnumber}-{track} - {title}.{ext}
    - For directories: VA compilations → 'VA - {year} {album}', consistent discnumber → 'cd{discnumber}', otherwise '{albumartist} - {year} {album}'
    - Appends audio quality info when appropriate
    - Zero-pads track numbers with at least 2 digits
    - Extracts only year component from date fields
    - Requires --year parameter to include year in directory names
    - Uses [Mixed Res] for directories with varying audio quality

    Updates the database with new paths and logs changes to changelog.
    Dry-run is the default mode to preview changes without executing them.

Author: audiomuze
Created: 2025
"""

import polars as pl
import sqlite3
import os
import logging
from pathlib import Path
import shutil
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime, timezone
import argparse

# ---------- Configuration ----------
DB_PATH = '/tmp/amg/dbtemplate.db'
LOG_LEVEL = logging.INFO
SCRIPT_NAME = "rename_files_and_folders.py"

# ---------- Command Line Arguments ----------
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Rename audio files and directories based on metadata")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the changes (default is dry-run mode)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--year",
        action="store_true",
        help="Include year in directory names"
    )
    return parser.parse_args()

# ---------- Logging Setup ----------
def setup_logging(verbose=False, execute=False):
    """Configure logging based on command line arguments."""
    log_level = logging.DEBUG if verbose else LOG_LEVEL
    
    handlers = [logging.StreamHandler()]
    if execute:
        handlers.append(logging.FileHandler('rename_files_and_folders.log'))
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

# ---------- Database Helper Functions ----------

def sqlite_to_polars(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """
    Convert SQLite query results to a Polars DataFrame with proper type handling.

    Args:
        conn: SQLite database connection
        query: SQL query to execute

    Returns:
        Polars DataFrame with appropriate data types
    """
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        # Handle different data types
        if col_name in ['__bitspersample', '__frequency_num']:
            # Numeric columns
            data[col_name] = pl.Series(
                name=col_name,
                values=[float(x) if x is not None else None for x in col_data],
                dtype=pl.Float64
            )
        else:
            # String columns with null handling
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )

    return pl.DataFrame(data)

def get_track_count_by_directory(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Get the maximum track number for each directory to determine padding needs.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        Dictionary mapping directory paths to maximum track number
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT __dirpath, MAX(CAST(track AS INTEGER)) as max_track
        FROM alib 
        WHERE track IS NOT NULL AND track != '' 
        GROUP BY __dirpath
    """)
    
    result = {}
    for dirpath, max_track in cursor.fetchall():
        try:
            result[dirpath] = int(max_track)
        except (ValueError, TypeError):
            result[dirpath] = 0
    
    return result

def update_database_and_changelog(
    conn: sqlite3.Connection, 
    updates: List[Tuple[str, str, int]],
    dry_run: bool = True
):
    """
    Update database with new paths and log changes to changelog.
    
    Args:
        conn: SQLite database connection
        updates: List of tuples (old_path, new_path, rowid)
        dry_run: If True, only log what would be updated without making changes
    """
    if dry_run:
        logging.info("DRY RUN: Would update database with the following changes:")
        for old_path, new_path, rowid in updates:
            logging.info(f"  Row {rowid}: {old_path} -> {new_path}")
        return
    
    cursor = conn.cursor()
    
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
    
    for old_path, new_path, rowid in updates:
        try:
            # Update the main table
            cursor.execute(
                "UPDATE alib SET __path = ?, __dirpath = ?, __dirname = ?, __filename = ? WHERE rowid = ?",
                (
                    new_path,
                    str(Path(new_path).parent),
                    Path(new_path).parent.name,
                    Path(new_path).name,
                    rowid
                )
            )
            
            # Log to changelog
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, "__path", old_path, new_path, timestamp, SCRIPT_NAME)
            )
            
        except Exception as e:
            logging.error(f"Error updating database for rowid {rowid}: {str(e)}")
            conn.rollback()
            raise
    
    conn.commit()

# ---------- Text Processing Functions ----------

def extract_year(date_string: str) -> str:
    """
    Extract only the year component from a date string.
    
    Args:
        date_string: Date string that may contain full date or just year
        
    Returns:
        Year string or empty string if no year found
    """
    if not date_string:
        return ""
    
    # Try to extract year using regex (matches 4-digit years)
    year_match = re.search(r'\b(19|20)\d{2}\b', date_string)
    if year_match:
        return year_match.group(0)
    
    return ""

def zero_pad_track(track: str, max_track: int) -> str:
    """
    Zero-pad track numbers based on the maximum track number in the directory.
    
    Args:
        track: Original track number string
        max_track: Maximum track number in the directory
        
    Returns:
        Zero-padded track number string
    """
    if not track or not track.strip():
        return track
    
    try:
        track_num = int(track)
        # Always pad with at least 2 digits, even for less than 10 tracks
        if max_track >= 100:
            return f"{track_num:03d}"  # 3-digit padding
        else:
            return f"{track_num:02d}"  # 2-digit padding (always)
    except (ValueError, TypeError):
        return track  # Return original if not a number

def sanitize_filename(name: str) -> str:
    """
    Remove or replace characters that are not safe for filenames.
    
    Args:
        name: Original filename string
        
    Returns:
        Sanitized filename string
    """
    if not name:
        return name
        
    # Replace problematic characters with alternatives
    replacements = {
        '/': '-',
        '\\': '-',
        ':': '-',
        '*': '',
        '?': '',
        '"': "'",
        '<': '',
        '>': '',
        '|': '-'
    }
    
    for old_char, new_char in replacements.items():
        name = name.replace(old_char, new_char)
    
    # Remove leading/trailing spaces and dots
    name = name.strip().strip('.')
    
    # Replace multiple spaces with single space
    name = re.sub(r'\s+', ' ', name)
    
    return name

def build_new_filename(row: Dict, ext: str, max_track: int) -> str:
    """
    Build the new filename based on compilation status and row data.
    
    Args:
        row: Dictionary containing file metadata
        ext: File extension
        max_track: Maximum track number in the directory for padding
        
    Returns:
        New filename string
    """
    compilation = row.get('compilation', '0')
    discnumber = row.get('discnumber')
    track = row.get('track', '')
    artist = row.get('artist', '')
    title = row.get('title', '')
    
    # Zero-pad track number
    track = zero_pad_track(track, max_track)
    
    # Sanitize all components
    track = sanitize_filename(track) if track else ''
    artist = sanitize_filename(artist) if artist else ''
    title = sanitize_filename(title) if title else ''
    
    # Build filename components
    parts = []
    
    # Add discnumber if present
    if discnumber and discnumber.strip():
        parts.append(f"{discnumber.strip()}-")
    
    # Add track number if present
    if track and track.strip():
        parts.append(f"{track.strip()} - ")
    
    # For compilations, add artist
    if compilation == '1' and artist and artist.strip():
        parts.append(f"{artist} - ")
    
    # Always add title
    parts.append(title)
    
    # Join parts and add extension
    filename = ''.join(parts).strip()
    if filename and not filename.endswith('.' + ext):
        filename += '.' + ext
    
    return filename

def rename_file(old_path: str, new_filename: str, dry_run: bool = True) -> Tuple[bool, str]:
    """
    Rename a file with error handling and logging.
    
    Args:
        old_path: Current full path to the file
        new_filename: New filename (without path)
        dry_run: If True, only log what would be renamed without making changes
        
    Returns:
        Tuple of (success boolean, new_full_path)
    """
    try:
        old_file = Path(old_path)
        if not old_file.exists():
            logging.error(f"File not found: {old_path}")
            return False, ""
        
        new_path = old_file.parent / new_filename
        
        # Check if new filename would be different
        if old_file.name == new_filename:
            logging.info(f"Filename unchanged: {old_path}")
            return True, str(new_path)
        
        # Check if target file already exists
        if new_path.exists():
            logging.error(f"Target file already exists: {new_path}")
            return False, ""
        
        if dry_run:
            logging.info(f"DRY RUN: Would rename {old_path} -> {new_path}")
            return True, str(new_path)
        
        # Perform the rename
        old_file.rename(new_path)
        logging.info(f"Renamed: {old_path} -> {new_path}")
        return True, str(new_path)
        
    except Exception as e:
        logging.error(f"Error renaming {old_path}: {str(e)}")
        return False, ""

def process_files(df: pl.DataFrame, track_counts: Dict[str, int], dry_run: bool = True) -> Tuple[Dict[str, List[Dict]], List[Tuple[str, str, int]]]:
    """
    Process all files in the DataFrame and rename them.
    
    Args:
        df: DataFrame containing file metadata
        track_counts: Dictionary mapping directories to max track numbers
        dry_run: If True, only log what would be renamed without making changes
        
    Returns:
        Tuple of (dir_files mapping, database_updates list)
    """
    dir_files = {}  # Track files by directory for later folder renaming
    database_updates = []  # Track changes for database update
    
    if dry_run:
        logging.info("=== DRY RUN MODE: No files will actually be renamed ===")
    
    for row in df.to_dicts():
        try:
            old_path = row['__path']
            dir_path = row['__dirpath']
            filename = row['__filename']
            rowid = row['rowid']
            
            # Track this file for directory processing
            if dir_path not in dir_files:
                dir_files[dir_path] = []
            dir_files[dir_path].append(row)
            
            # Get file extension
            ext = Path(filename).suffix.lstrip('.') if '.' in filename else ''
            
            # Get max track number for this directory for padding
            max_track = track_counts.get(dir_path, 0)
            
            # Build new filename
            new_filename = build_new_filename(row, ext, max_track)
            
            if not new_filename:
                logging.warning(f"Could not generate new filename for: {old_path}")
                continue
            
            # Rename the file
            success, new_path = rename_file(old_path, new_filename, dry_run)
            
            if success:
                logging.info(f"Successfully processed: {old_path} -> {new_path}")
                database_updates.append((old_path, new_path, rowid))
            else:
                logging.warning(f"Failed to process: {old_path}")
                
        except Exception as e:
            logging.error(f"Error processing file {row.get('__path', 'unknown')}: {str(e)}")
    
    return dir_files, database_updates

def determine_new_dirname(files_in_dir: List[Dict], include_year: bool = False) -> str:
    """
    Determine the new directory name based on files in the directory.
    
    Args:
        files_in_dir: List of file metadata dictionaries for files in the directory
        include_year: Whether to include year in directory names
        
    Returns:
        New directory name or empty string if cannot be determined
    """
    if not files_in_dir:
        return ""
    
    # Check if this is a compilation (VA)
    is_compilation = any(file_data.get('compilation') == '1' for file_data in files_in_dir)
    
    # Check if all files have the same discnumber (for non-VA)
    discnumbers = set()
    albumartist = None
    year = None
    album = None
    needs_quality_suffix = False
    bitspersample = None
    frequency_num = None
    unique_qualities = set()
    
    for file_data in files_in_dir:
        discnum = file_data.get('discnumber')
        if discnum and discnum.strip():
            discnumbers.add(discnum.strip())
        
        # Get album metadata from first file that has it
        if not albumartist and file_data.get('albumartist'):
            albumartist = file_data.get('albumartist')
            # Remove \\ delimiters completely for albumartist in folder names
            albumartist = albumartist.replace('\\\\', '')
        
        # Extract year from date field, but only use it if include_year is True
        if include_year and not year and file_data.get('year'):
            year = extract_year(file_data.get('year'))
            
        if not album and file_data.get('album'):
            album = file_data.get('album')
        
        # Check if we need quality suffix and track unique qualities
        try:
            bits = float(file_data.get('__bitspersample', 0)) if file_data.get('__bitspersample') else 0
            freq = float(file_data.get('__frequency_num', 0)) if file_data.get('__frequency_num') else 0
            
            if bits > 16 or freq > 44.1:
                needs_quality_suffix = True
                # Store first file's values for consistent suffix
                if bitspersample is None:
                    bitspersample = int(bits) if bits else None
                if frequency_num is None:
                    frequency_num = freq
                # Track all unique qualities for mixed resolution detection
                unique_qualities.add((bits, freq))
        except (ValueError, TypeError):
            pass
    
    # For VA compilations, use "VA - {year} {album}" format (year only if included)
    if is_compilation:
        parts = []
        parts.append("VA")
        if include_year and year and year.strip():
            parts.append(sanitize_filename(year))
        if album and album.strip():
            parts.append(sanitize_filename(album))
        
        dirname = " - ".join(parts) if parts else ""
    
    # If all files have the same discnumber, use cd{discnumber} format
    elif len(discnumbers) == 1:
        discnumber = discnumbers.pop()
        dirname = f"cd{discnumber}"
    
    else:
        # Otherwise use albumartist - year album format (year only if included)
        parts = []
        if albumartist and albumartist.strip():
            parts.append(sanitize_filename(albumartist))
        if include_year and year and year.strip():
            parts.append(sanitize_filename(year))
        if album and album.strip():
            parts.append(sanitize_filename(album))
        
        dirname = " - ".join(parts) if parts else ""
    
    # Add quality suffix if needed - Use [Mixed Res] for directories with varying quality
    if needs_quality_suffix and dirname:
        if len(unique_qualities) > 1:
            # Multiple different high-res configurations found
            dirname += " [Mixed Res]"
        else:
            # All high-res files have the same quality, use specific format
            if bitspersample and frequency_num:
                # Convert frequency to string and split on decimal
                freq_str = str(frequency_num)
                if '.' in freq_str:
                    integer_part, decimal_part = freq_str.split('.')
                    # Pad decimal part with zeros if needed
                    decimal_part = decimal_part.ljust(1, '0')
                    formatted = f"{int(bitspersample)}{integer_part}.{decimal_part}"
                else:
                    formatted = f"{int(bitspersample)}{freq_str}.0"
                dirname += f" [{formatted} kHz]"
    
    return dirname

def rename_directory(old_dirpath: str, new_dirname: str, dry_run: bool = True) -> Tuple[bool, str]:
    """
    Rename a directory with error handling and logging.
    
    Args:
        old_dirpath: Current directory path
        new_dirname: New directory name
        dry_run: If True, only log what would be renamed without making changes
        
    Returns:
        Tuple of (success boolean, new_dirpath)
    """
    try:
        old_dir = Path(old_dirpath)
        if not old_dir.exists() or not old_dir.is_dir():
            logging.error(f"Directory not found: {old_dirpath}")
            return False, ""
        
        # Check if directory name would change
        if old_dir.name == new_dirname:
            logging.info(f"Directory name unchanged: {old_dirpath}")
            return True, str(old_dir)
        
        new_dirpath = old_dir.parent / new_dirname
        
        # Check if target directory already exists
        if new_dirpath.exists():
            logging.error(f"Target directory already exists: {new_dirpath}")
            return False, ""
        
        if dry_run:
            logging.info(f"DRY RUN: Would rename directory {old_dirpath} -> {new_dirpath}")
            return True, str(new_dirpath)
        
        # Perform the rename
        old_dir.rename(new_dirpath)
        logging.info(f"Renamed directory: {old_dirpath} -> {new_dirpath}")
        return True, str(new_dirpath)
        
    except Exception as e:
        logging.error(f"Error renaming directory {old_dirpath}: {str(e)}")
        return False, ""

def process_directories(
    dir_files: Dict[str, List[Dict]], 
    file_updates: List[Tuple[str, str, int]],
    dry_run: bool = True,
    include_year: bool = False
) -> List[Tuple[str, str, int]]:
    """
    Process all directories and rename them based on their contents.
    
    Args:
        dir_files: Dictionary mapping directory paths to lists of file metadata
        file_updates: List of file updates (old_path, new_path, rowid)
        dry_run: If True, only log what would be renamed without making changes
        include_year: Whether to include year in directory names
        
    Returns:
        Updated list of file updates with directory changes
    """
    dir_updates = {}  # Track directory renames
    updated_file_updates = []
    
    if dry_run:
        logging.info("=== DRY RUN MODE: No directories will actually be renamed ===")
    
    for old_dirpath, files_in_dir in dir_files.items():
        try:
            new_dirname = determine_new_dirname(files_in_dir, include_year)
            if not new_dirname:
                logging.warning(f"Could not determine new name for directory: {old_dirpath}")
                continue
            
            success, new_dirpath = rename_directory(old_dirpath, new_dirname, dry_run)
            
            if success:
                logging.info(f"Successfully renamed directory: {old_dirpath} -> {new_dirpath}")
                dir_updates[old_dirpath] = new_dirpath
            else:
                logging.warning(f"Failed to rename directory: {old_dirpath}")
                
        except Exception as e:
            logging.error(f"Error processing directory {old_dirpath}: {str(e)}")
    
    # Update file paths with new directory paths
    for old_path, new_path, rowid in file_updates:
        old_dir = str(Path(old_path).parent)
        if old_dir in dir_updates:
            # This file is in a renamed directory, update its path
            new_dir = dir_updates[old_dir]
            filename = Path(new_path).name
            updated_path = str(Path(new_dir) / filename)
            updated_file_updates.append((old_path, updated_path, rowid))
            
            if dry_run:
                logging.info(f"DRY RUN: Would update file path due to directory rename: {new_path} -> {updated_path}")
        else:
            # Directory wasn't renamed, keep original update
            updated_file_updates.append((old_path, new_path, rowid))
    
    return updated_file_updates

# ---------- Summary Functions ----------

def print_summary(file_updates: List[Tuple[str, str, int]], dry_run: bool = True):
    """Print a summary of changes that would be made."""
    if not file_updates:
        logging.info("No changes would be made.")
        return
    
    mode = "DRY RUN: Would make" if dry_run else "Will make"
    logging.info(f"=== SUMMARY ===")
    logging.info(f"{mode} {len(file_updates)} changes:")
    
    # Group by directory
    dir_changes = {}
    for old_path, new_path, rowid in file_updates:
        old_dir = str(Path(old_path).parent)
        new_dir = str(Path(new_path).parent)
        
        if old_dir not in dir_changes:
            dir_changes[old_dir] = {"old_files": [], "new_dir": new_dir, "file_changes": 0}
        
        dir_changes[old_dir]["file_changes"] += 1
        dir_changes[old_dir]["old_files"].append(Path(old_path).name)
    
    # Print directory-level summary
    for old_dir, info in dir_changes.items():
        new_dir = info["new_dir"]
        if old_dir == new_dir:
            logging.info(f"  Directory: {old_dir} (unchanged)")
        else:
            logging.info(f"  Directory: {old_dir} -> {new_dir}")
        logging.info(f"    Files to rename: {info['file_changes']}")
        
        # Show first few filenames as examples
        if info["old_files"]:
            sample_files = info["old_files"][:3]
            file_list = ", ".join(sample_files)
            if len(info["old_files"]) > 3:
                file_list += f" ... and {len(info['old_files']) - 3} more"
            logging.info(f"    Example files: {file_list}")

# ---------- Main Execution Function ----------

def main():
    """
    Main execution function that orchestrates the file and directory renaming process.
    """
    args = parse_arguments()
    setup_logging(args.verbose, args.execute)
    
    # Dry-run is the default mode unless --execute is specified
    dry_run = not args.execute
    
    logging.info("Starting file and directory renaming process")
    if dry_run:
        logging.info("DRY RUN MODE: No changes will be made to files or database")
    else:
        logging.info("EXECUTE MODE: Changes will be made to files and database")
    
    if args.year:
        logging.info("Year will be included in directory names")
    else:
        logging.info("Year will NOT be included in directory names (use --year to include)")
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Connected to database: {DB_PATH}")
        
        # Get track counts for zero-padding
        logging.info("Calculating track counts for zero-padding...")
        track_counts = get_track_count_by_directory(conn)
        logging.info(f"Found {len(track_counts)} directories with track information")
        
        # Load required data from database
        logging.info("Loading file metadata from database...")
        df = sqlite_to_polars(
            conn,
            """
            SELECT rowid, __path, __dirpath, __dirname, __filename, __file_mod_datetime,
                   __bitspersample, __frequency_num, albumartist, discnumber, track,
                   title, subtitle, compilation, year, album, artist
            FROM alib
            """
        )
        
        logging.info(f"Loaded {df.height} files from database")
        
        if df.height == 0:
            logging.warning("No files found in database")
            return
        
        # Process files first
        logging.info("Starting file renaming...")
        dir_files, file_updates = process_files(df, track_counts, dry_run)
        
        # Then process directories
        logging.info("Starting directory renaming...")
        all_updates = process_directories(dir_files, file_updates, dry_run, args.year)
        
        # Print summary of changes
        print_summary(all_updates, dry_run)
        
        # Update database with changes (unless dry run)
        if all_updates and not dry_run:
            logging.info("Updating database with new paths...")
            update_database_and_changelog(conn, all_updates, dry_run)
            logging.info(f"Updated {len(all_updates)} records in database")
        elif all_updates and dry_run:
            logging.info("DRY RUN: Database would be updated with the above changes")
        else:
            logging.info("No changes to update in database")
        
        if dry_run:
            logging.info("Dry run completed successfully. No changes were made.")
            logging.info("Use --execute flag to actually perform the changes.")
        else:
            logging.info("File and directory renaming completed successfully")
        
    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()