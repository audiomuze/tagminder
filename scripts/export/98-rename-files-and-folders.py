"""
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

        Updates the database with new paths and logs changes to `changelog`.

        Rollback connectivity:
                This step is intentionally "late" in the pipeline and can change `alib.__path`.
                To preserve the ability to roll back earlier Tagminder metadata changes after
                files are renamed/moved, it also:
                - logs explicit path-field changes (`__path`, `__dirpath`, `__filename`, and
                    `__filename_no_ext` when available)
                - rewrites existing `changelog.alib_path` values from old paths to new paths
                    so historical changelog entries still point at the current on-disk file

        Dry-run is the default mode to preview changes without executing them.

    This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import polars as pl
import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Tuple
import re
import argparse

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_polars_db
# ---------- Configuration ----------
LOG_LEVEL = logging.INFO
MAX_FILENAME_LENGTH = 255  # ext4 limit
MAX_PATH_LENGTH = 4096     # ext4 limit

# ---------- Command Line Arguments ----------
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Rename audio files and directories based on metadata")
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=tm_config.db_path_from_toml(default=None),
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )
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
        log_filename = f"{Path(tm_db.script_name()).stem}.log"
        handlers.append(logging.FileHandler(log_filename))
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

# ---------- Validation Functions ----------

def validate_filename_length(filename: str) -> Tuple[bool, str]:
    """
    Validate that filename doesn't exceed filesystem limitations.
    
    Args:
        filename: Proposed filename to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not filename:
        return False, "Filename is empty"
    
    # Check filename length (without path)
    if len(filename) > MAX_FILENAME_LENGTH:
        return False, f"Filename exceeds {MAX_FILENAME_LENGTH} character limit: {len(filename)} characters"
    
    return True, ""

def truncate_filename(filename: str, max_length: int = MAX_FILENAME_LENGTH) -> str:
    """
    Truncate filename to fit within filesystem limits while preserving extension.
    
    Args:
        filename: Original filename to truncate
        max_length: Maximum allowed length
        
    Returns:
        Truncated filename that fits within limits
    """
    if len(filename) <= max_length:
        return filename
    
    # Preserve extension if possible
    name_parts = filename.rsplit('.', 1)
    if len(name_parts) == 2:
        name, ext = name_parts
        # Reserve space for extension + dot
        max_name_length = max_length - len(ext) - 1
        if max_name_length > 10:  # Ensure we have reasonable space for name
            truncated_name = name[:max_name_length]
            return f"{truncated_name}.{ext}"
    
    # Fallback: simple truncation
    return filename[:max_length]

# ---------- Database Helper Functions ----------

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
            if old_path != new_path:
                logging.info(f"  Row {rowid}: {old_path} -> {new_path}")
            else:
                logging.info(f"  Row {rowid}: UNCHANGED (path remains the same)")
        return
    
    cursor = conn.cursor()

    # Canonical changelog schema (with best-effort migration for older tables)
    tm_db.ensure_changelog_table(conn)

    alib_cols = {row[1] for row in conn.execute("PRAGMA table_info(alib)").fetchall()}
    set_cols = ["__path", "__dirpath", "__dirname", "__filename"]
    if "__filename_no_ext" in alib_cols:
        set_cols.append("__filename_no_ext")

    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()

    # Only process rows where a rename actually happened.
    changed = [(o, n, int(r)) for (o, n, r) in updates if o and n and o != n]
    if not changed:
        logging.info("No path changes detected; nothing to update.")
        return

    # Create a temp mapping table for set-based rewrites.
    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS tmp_path_map (old_path TEXT PRIMARY KEY, new_path TEXT NOT NULL)"
    )
    cursor.execute("DELETE FROM tmp_path_map")
    cursor.executemany(
        "INSERT INTO tmp_path_map (old_path, new_path) VALUES (?, ?)",
        [(old_path, new_path) for old_path, new_path, _ in changed],
    )

    # Apply all DB + changelog changes in a single transaction.
    actual_updates = 0
    with tm_db.transaction(conn):
        # 1) Rewrite historical changelog rows to keep connectivity.
        cursor.execute(
            """
            UPDATE changelog
            SET alib_path = (
                SELECT new_path FROM tmp_path_map WHERE tmp_path_map.old_path = changelog.alib_path
            )
            WHERE alib_path IN (SELECT old_path FROM tmp_path_map)
            """
        )

        # 2) Update alib system columns and insert explicit rename entries.
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)
        for old_path, new_path, rowid in changed:
            old_p = Path(old_path)
            new_p = Path(new_path)

            old_dirpath = str(old_p.parent)
            new_dirpath = str(new_p.parent)
            old_filename = old_p.name
            new_filename = new_p.name
            old_filename_no_ext = old_p.stem
            new_filename_no_ext = new_p.stem

            updates_for_row: list[tuple[str, object]] = []
            updates_for_row.append(("__path", new_path))
            updates_for_row.append(("__dirpath", new_dirpath))
            updates_for_row.append(("__dirname", new_p.parent.name))
            updates_for_row.append(("__filename", new_filename))
            if "__filename_no_ext" in alib_cols:
                updates_for_row.append(("__filename_no_ext", new_filename_no_ext))

            set_clause = ", ".join([f"{tm_db.quote_ident(col)} = ?" for col, _ in updates_for_row])
            values = [val for _, val in updates_for_row]
            values.append(rowid)
            cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

            changes: list[tuple[str, object, object]] = []
            changes.append(("__path", old_path, new_path))
            if old_dirpath != new_dirpath:
                changes.append(("__dirpath", old_dirpath, new_dirpath))
            if old_filename != new_filename:
                changes.append(("__filename", old_filename, new_filename))
            if "__filename_no_ext" in alib_cols and old_filename_no_ext != new_filename_no_ext:
                changes.append(("__filename_no_ext", old_filename_no_ext, new_filename_no_ext))

            changelog.add(alib_path=new_path, changes=changes)
            actual_updates += 1
            logging.info(f"Updated database and changelog for rowid {rowid}: {old_path} -> {new_path}")

        changelog.flush(cursor)

    logging.info(
        f"Database update completed: {actual_updates} records updated out of {len(updates)} processed"
    )

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
    
    # Validate and truncate if necessary
    is_valid, error_msg = validate_filename_length(filename)
    if not is_valid:
        logging.warning(f"Filename too long, will truncate: {error_msg}")
        filename = truncate_filename(filename)
        logging.info(f"Truncated filename: {filename}")
    
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
            return True, str(old_path)  # Return old path since no change
        
        # Check if target file already exists
        if new_path.exists():
            logging.error(f"Target file already exists: {new_path}")
            return False, ""
        
        # Validate path length before attempting rename
        try:
            new_path_str = str(new_path)
            if len(new_path_str) > MAX_PATH_LENGTH:
                logging.error(f"Path exceeds {MAX_PATH_LENGTH} character limit: {len(new_path_str)} characters")
                logging.error(f"Path: {new_path_str}")
                return False, ""
        except Exception as e:
            logging.error(f"Error validating path length: {str(e)}")
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
            
            # Only proceed if filename would actually change
            if Path(old_path).name == new_filename:
                logging.debug(f"Filename unchanged, skipping: {old_path}")
                database_updates.append((old_path, old_path, rowid))  # No change
                continue
            
            # Rename the file
            success, new_path = rename_file(old_path, new_filename, dry_run)
            
            if success:
                if old_path != new_path:
                    logging.info(f"Successfully renamed: {old_path} -> {new_path}")
                else:
                    logging.debug(f"No change needed: {old_path}")
                database_updates.append((old_path, new_path, rowid))
            else:
                logging.warning(f"Failed to process: {old_path}")
                # Even if rename failed, track the original path
                database_updates.append((old_path, old_path, rowid))
                
        except Exception as e:
            logging.error(f"Error processing file {row.get('__path', 'unknown')}: {str(e)}")
            # On error, preserve the original path
            database_updates.append((old_path, old_path, rowid))
    
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
            return True, str(old_dir)  # Return old path since no change
        
        new_dirpath = old_dir.parent / new_dirname
        
        # Check if target directory already exists
        if new_dirpath.exists():
            logging.error(f"Target directory already exists: {new_dirpath}")
            return False, ""
        
        # Validate directory name length
        is_valid, error_msg = validate_filename_length(new_dirname)
        if not is_valid:
            logging.error(f"Directory name too long: {error_msg}")
            return False, ""
        
        # Validate full path length
        try:
            new_dirpath_str = str(new_dirpath)
            if len(new_dirpath_str) > MAX_PATH_LENGTH:
                logging.error(f"Directory path exceeds {MAX_PATH_LENGTH} character limit: {len(new_dirpath_str)} characters")
                logging.error(f"Path: {new_dirpath_str}")
                return False, ""
        except Exception as e:
            logging.error(f"Error validating directory path length: {str(e)}")
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
                # Directory won't be renamed, so files stay in same directory
                continue
            
            # Check if directory name would actually change
            if Path(old_dirpath).name == new_dirname:
                logging.debug(f"Directory name unchanged, skipping: {old_dirpath}")
                continue
            
            success, new_dirpath = rename_directory(old_dirpath, new_dirname, dry_run)
            
            if success:
                if old_dirpath != new_dirpath:
                    logging.info(f"Successfully renamed directory: {old_dirpath} -> {new_dirpath}")
                    dir_updates[old_dirpath] = new_dirpath
                else:
                    logging.debug(f"No directory change needed: {old_dirpath}")
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
            
            if dry_run and old_path != updated_path:
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
    
    # Count actual changes (where old_path != new_path)
    actual_changes = sum(1 for old_path, new_path, _ in file_updates if old_path != new_path)
    
    mode = "DRY RUN: Would make" if dry_run else "Will make"
    logging.info("=== SUMMARY ===")
    logging.info(f"{mode} {actual_changes} actual changes out of {len(file_updates)} files processed:")
    
    if actual_changes == 0:
        logging.info("No files need to be renamed - all filenames are already in the correct format.")
        return
    
    # Group by directory
    dir_changes = {}
    for old_path, new_path, rowid in file_updates:
        if old_path == new_path:
            continue  # Skip unchanged files
            
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
            logging.info(f"  Directory: {old_dir} (files renamed but directory unchanged)")
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

    if not args.db:
        raise SystemExit(
            "No DB path resolved: set tagminder.toml [db].path or pass --db PATH"
        )
    setup_logging(args.verbose, args.execute)
    
    # Dry-run is the default mode unless --execute is specified
    dry_run = not args.execute
    
    # Get script name for logging and changelog
    script_name = Path(tm_db.script_name()).stem
    actual_script_name = tm_db.script_name()
    logging.info(f"Starting {script_name} - file and directory renaming process")
    logging.debug(f"Script file: {actual_script_name}")
    
    if dry_run:
        logging.info("DRY RUN MODE: No changes will be made to files or database")
    else:
        logging.info("EXECUTE MODE: Changes will be made to files and database")
    
    if args.year:
        logging.info("Year will be included in directory names")
    else:
        logging.info("Year will NOT be included in directory names (use --year to include)")
    
    conn = None
    db_path = args.db
    try:
        conn = tm_db.connect(db_path)
        logging.info(f"Connected to database: {db_path}")
        
        # Get track counts for zero-padding
        logging.info("Calculating track counts for zero-padding...")
        track_counts = get_track_count_by_directory(conn)
        logging.info(f"Found {len(track_counts)} directories with track information")
        
        # Load required data from database
        logging.info("Loading file metadata from database...")
        df = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid, __path, __dirpath, __dirname, __filename, __file_mod_datetime,
                   __bitspersample, __frequency_num, albumartist, discnumber, track,
                   title, subtitle, compilation, year, album, artist
            FROM alib
            """,
            dtype_overrides={
                "__bitspersample": pl.Float64,
                "__frequency_num": pl.Float64,
            },
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