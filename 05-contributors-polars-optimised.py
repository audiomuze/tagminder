"""
Script Name: 05-contributors-polars-optimised.py

Purpose:
    Connects to a SQLite music library database (alib table) containing track metadata with various contributor fields (artist, composer, arranger, lyricist, etc.)
    Loads a reference dictionary of contributor names from a disambiguation table (_REF_mb_disambiguated) that maps lowercase variants to canonical/proper forms

    SURGICAL FILTERING ENHANCEMENT:
    The script now surgically filters out contributors that already exist in the disambiguation dictionary,
    eliminating them from further processing to improve efficiency and prevent unnecessary changes.

    Processes and normalizes contributor names across 13 different contributor fields by:
    - Handling multiple contributors separated by delimiters (double backslash \\)
    - Applying smart title case formatting
    - Looking up canonical forms from the reference dictionary
    - Deduplicating entries
    - ONLY processing entries that are NOT already in the disambiguation dictionary

    Detects changes by comparing original vs normalized data using Polars vectorized operations
    Updates the database only for rows that actually changed, while:
    - Incrementing a modification counter (sqlmodded)
    - Logging all changes to a changelog table with timestamps
    - Using database transactions for data integrity

    The script is optimized for performance using Polars DataFrames and vectorized operations, with
    comprehensive logging and error handling. It's designed to clean up inconsistent contributor name
    formatting in a music library database while maintaining a full audit trail of changes.

It is part of taglib.

Usage:
    python 05-contributors-polars-optimised.py

Author: audiomuze
Created: 2025-06-11
Modified: 2025-06-28 - Added surgical filtering for disambiguation dictionary entries
"""

import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any, Union
import logging
from datetime import datetime, timezone
import re

# ---------- Configuration ----------
SCRIPT_NAME = "contributors-polars.py"
DB_PATH = '/tmp/amg/dbtemplate.db'

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Global Constants ----------
# Main delimiter for joining multiple contributors
DELIMITER = '\\\\'  # Double backslash for splitting and joining

# Regex pattern for splitting on various delimiters, but not commas followed by suffixes
SPLIT_PATTERN = re.compile(r'(?:\\\\|;|/|,(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b))')

# ---------- Database Helper Functions ----------

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: Union[str, Tuple[str, ...]] = None) -> pl.DataFrame:
    """
    Convert SQLite query results to a Polars DataFrame with proper type handling.

    Args:
        conn: SQLite database connection
        query: SQL query to execute
        id_column: Column(s) to treat as integer IDs (unused but kept for compatibility)

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
        if col_name in ("rowid", "sqlmodded"):
            # Ensure integer columns are properly typed
            data[col_name] = pl.Series(
                name=col_name,
                values=[int(x or 0) for x in col_data],
                dtype=pl.Int64
            )
        else:
            # String columns with null handling
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )

    return pl.DataFrame(data)

# ---------- Text Processing Functions ----------

def smart_title(text):
    """
    Apply intelligent title casing that preserves certain patterns and handles special cases.

    This function handles:
    - Articles and prepositions (keeping them lowercase unless first word)
    - Roman numerals (converting to uppercase)
    - Possessives (maintaining correct apostrophe forms)
    - Names with prefixes (Mc, O')
    - Hyphenated words
    - Words with periods (initials)
    - Already capitalized words (preserving them)

    Args:
        text: String to apply smart title casing to

    Returns:
        String with smart title casing applied
    """
    if not text:
        return text

    def fix_caps_word(word, is_first_word=False, follows_bracket=False):
        """Apply capitalization rules to a single word."""
        lower_words = ["of", "a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by"]

        if is_first_word:
            # First word is always capitalized unless already has uppercase
            if any(c.isupper() for c in word):
                return word
            else:
                return word.capitalize()
        elif follows_bracket:
            return word.capitalize()
        elif any(c.isupper() for c in word):
            # Preserve existing capitalization
            return word
        elif re.match(r"^[IVXLCDM]+$", word.upper()):
            # Roman numerals
            return word.upper()
        elif "." in word:
            # Handle initials like "J.R.R."
            parts = word.split('.')
            return '.'.join(part.capitalize() for part in parts)
        elif "'" in word or "'" in word:
            # Handle possessives and contractions
            apos_pos = max(word.find("'"), word.find("'"))
            if 0 < apos_pos < len(word) - 1:
                return word[:apos_pos].capitalize() + word[apos_pos:]
            else:
                return word.capitalize()
        elif "-" in word:
            # Handle hyphenated words
            parts = word.split('-')
            return '-'.join(part.capitalize() for part in parts)
        elif word.lower() in lower_words:
            # Keep articles and prepositions lowercase
            return word
        else:
            return word.capitalize()

    # Regex to capture words (including McNames, O'Names, possessives)
    word_pattern = r"\b(?:Mc\w+|O'\w+|\w+(?:['']\w+)?)\b"
    # Regex to capture non-word parts (spaces, punctuation)
    non_word_pattern = r"[^\w\s]+"

    # Combine the patterns to capture words and non-word parts
    combined_pattern = rf"({word_pattern})|({non_word_pattern})|\s+"

    parts = re.findall(combined_pattern, text)
    result = []
    capitalize_next = True

    for part_tuple in parts:
        word = part_tuple[0] or part_tuple[1]
        if word:
            if re.match(word_pattern, word):  # It's a word
                processed_word = fix_caps_word(word, is_first_word=capitalize_next, follows_bracket=False)
                # Handle possessive 's (moved from fix_caps_word)
                if processed_word.lower().endswith("'s"):
                    processed_word = processed_word[:-2] + "'s"
                elif processed_word.lower().endswith("'s"):
                    processed_word = processed_word[:-2] + "'s"
                # Special rule for "O'" (applied directly)
                elif word.lower().startswith("o'") and len(word) > 2 and word[2].lower() != 's' and word[2] != ' ':
                    processed_word = "O'" + fix_caps_word(word[2:], is_first_word=False, follows_bracket=False)
                elif word.lower().startswith("o'") and len(word) > 2 and word[2].lower() != 's' and word[2] != ' ':
                    processed_word = "O'" + fix_caps_word(word[2:], is_first_word=False, follows_bracket=False)
                result.append(processed_word)
                capitalize_next = False
            else:  # It's a non-word part
                result.append(word)
                capitalize_next = word in "({[<"
        else:
            result.append(" ")  # It's whitespace

    processed_text = "".join(result)
    # Final pass to ensure possessive 's is lowercase
    processed_text = re.sub(r"(\w)['']S\b", r"\1's", processed_text)

    return processed_text

def normalize_contributor_entry(x: Union[str, None], contributors_dict: Dict[str, str]) -> Union[str, None]:
    """
    Normalize a single contributor entry by splitting on delimiters, looking up canonical forms,
    and applying smart title casing where needed.

    This function handles:
    - Multiple contributors separated by the main delimiter (\\\\)
    - Additional splitting on secondary delimiters (;, /, comma)
    - Dictionary lookup for canonical forms
    - Smart title casing fallback
    - Deduplication of normalized names

    Args:
        x: Contributor string to normalize (can be None)
        contributors_dict: Dictionary mapping lowercase names to canonical forms

    Returns:
        Normalized contributor string with proper formatting and canonical names
    """
    if x is None:
        return None

    if DELIMITER in x:
        # Handle multiple contributors separated by main delimiter
        items = x.split(DELIMITER)
        normalized_items = []
        for item in items:
            stripped_item = item.strip()
            lowered_item = stripped_item.lower()
            if lowered_item in contributors_dict:
                # Use canonical form from dictionary
                normalized_items.append(contributors_dict[lowered_item])
            else:
                # Further split on secondary delimiters and normalize each part
                parts = SPLIT_PATTERN.split(stripped_item)
                for part in parts:
                    stripped = part.strip()
                    lowered = stripped.lower()
                    normalized = contributors_dict.get(lowered, smart_title(stripped))
                    normalized_items.append(normalized)

        # Deduplicate the entire normalized_items list
        final_normalized_items = []
        seen = set()
        for item in normalized_items:
            if item not in seen:
                final_normalized_items.append(item)
                seen.add(item)

        return DELIMITER.join(final_normalized_items)
    else:
        # Handle single contributor or contributors with secondary delimiters only
        lowered_x = x.lower()
        if lowered_x in contributors_dict:
            return contributors_dict[lowered_x]

        parts = SPLIT_PATTERN.split(x)
        normalized_parts = []
        seen = set()

        for part in parts:
            stripped = part.strip()
            lowered = stripped.lower()
            normalized = contributors_dict.get(lowered, smart_title(stripped))

            if normalized not in seen:
                normalized_parts.append(normalized)
                seen.add(normalized)

        return DELIMITER.join(normalized_parts) if normalized_parts else None

# ---------- Surgical Filtering Functions ----------

def create_contributor_masks(df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]) -> pl.DataFrame:
    """
    Create boolean masks for each contributor column indicating which entries
    exist in the disambiguation dictionary (and should be skipped from processing).

    This function uses Polars vectorization to efficiently check all contributor
    fields against the disambiguation dictionary, creating mask columns that
    identify entries that don't need further processing.

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names to check
        contributors_dict: Dictionary mapping lowercase names to canonical forms

    Returns:
        DataFrame with original data plus boolean mask columns (named {column}_in_dict)
    """
    mask_expressions = []

    for column in columns:
        # Create mask: True if the lowercased contributor exists in dictionary
        mask_expr = (
            pl.col(column)
            .str.to_lowercase()
            .is_in(list(contributors_dict.keys()))
            .alias(f"{column}_in_dict")
        )
        mask_expressions.append(mask_expr)

    return df.with_columns(mask_expressions)

def filter_processable_tracks(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    """
    Filter to only tracks that have at least one contributor field that needs processing
    (i.e., not null and not already in the disambiguation dictionary).

    This optimizes performance by eliminating tracks where all contributor fields
    are either empty or already properly disambiguated.

    Args:
        df: DataFrame with contributor columns and mask columns
        columns: List of contributor column names

    Returns:
        Filtered DataFrame containing only tracks that need processing
    """
    # Create expression to check if any contributor field needs processing
    # (is not null AND not in dictionary)
    needs_processing_expr = pl.any_horizontal([
        pl.col(col).is_not_null() & ~pl.col(f"{col}_in_dict")
        for col in columns
    ])

    return df.filter(needs_processing_expr)

def selective_normalize_contributors(df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]) -> pl.DataFrame:
    """
    Selectively normalize contributor columns, only processing entries that are not already
    in the disambiguation dictionary.

    This function uses Polars conditional logic to surgically avoid processing
    contributor entries that are already in their canonical form, improving
    efficiency and preventing unnecessary database updates.

    Args:
        df: DataFrame with contributor columns and mask columns
        columns: List of contributor column names to normalize
        contributors_dict: Dictionary mapping lowercase names to canonical forms

    Returns:
        DataFrame with selectively normalized contributor columns
    """
    expressions = []

    for column in columns:
        # Only normalize if the entry is not in the dictionary
        expr = (
            pl.when(pl.col(f"{column}_in_dict"))
            .then(pl.col(column))  # Keep original if in dictionary
            .otherwise(
                pl.col(column).map_elements(
                    lambda x: normalize_contributor_entry(x, contributors_dict),
                    return_dtype=pl.Utf8
                )
            )
            .alias(column)
        )
        expressions.append(expr)

    return df.with_columns(expressions)

def detect_changes_with_masks(original_df: pl.DataFrame, updated_df: pl.DataFrame, columns: List[str]) -> List[int]:
    """
    Detect changes between original and updated DataFrames, but only for columns
    that were actually processed (not skipped due to dictionary matches).

    This prevents false positive change detection for entries that were already
    in the disambiguation dictionary and didn't need processing.

    Args:
        original_df: Original DataFrame with mask columns
        updated_df: Updated DataFrame after selective normalization
        columns: List of contributor column names

    Returns:
        List of rowids that have actual changes requiring database updates
    """
    # Create change detection expressions only for columns that were processed
    change_expressions = []

    for col in columns:
        # Only consider it a change if:
        # 1. The column was not in dictionary (was processed)
        # 2. The original value was not null
        # 3. The values actually differ
        change_expr = (
            ~original_df[f"{col}_in_dict"] &  # Was not in dictionary
            original_df[col].is_not_null() &  # Original value exists
            (original_df[col] != updated_df[col])  # Values differ
        )
        change_expressions.append(change_expr)

    # Any row with at least one qualifying change
    any_change_expr = pl.any_horizontal(change_expressions)

    return updated_df.filter(any_change_expr)["rowid"].to_list()

# ---------- Database Update Functions ----------

def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int],
    columns_to_update: List[str]
) -> int:
    """
    Write normalized contributor updates to the database with full changelog tracking.

    This function:
    - Updates only the rows that actually changed
    - Increments the sqlmodded counter for each field changed
    - Logs all changes to the changelog table with timestamps
    - Uses database transactions for data integrity

    Args:
        conn: SQLite database connection
        updated_df: DataFrame with normalized contributor data
        original_df: DataFrame with original contributor data
        changed_rowids: List of rowids that need updating
        columns_to_update: List of contributor column names to update

    Returns:
        Number of rows actually updated in the database
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

    # Process only the rows that changed
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)

        # Identify which columns actually changed and have new values
        changed_cols = [
            col for col in columns_to_update
            if record[col] != original_row[col] and record[col] is not None
        ]

        if not changed_cols:
            continue

        # Increment sqlmodded counter by number of fields changed
        new_sqlmodded = int(original_row["sqlmodded"] or 0) + len(changed_cols)
        set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
        values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

        # Update the main table
        cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

        # Log each field change to changelog
        for col in changed_cols:
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, col, original_row[col], record[col], timestamp, SCRIPT_NAME)
            )

        updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count

# ---------- Main Execution Function ----------

def main():
    """
    Main execution function that orchestrates the contributor normalization process.

    Process flow:
    1. Load disambiguation dictionary from database
    2. Load track data with contributor fields
    3. Create masks to identify entries already in dictionary
    4. Filter to tracks that need processing
    5. Selectively normalize only unmatched contributors
    6. Detect actual changes using mask-aware comparison
    7. Update database with changes and log to changelog
    """
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Load disambiguation dictionary
        logging.info("Fetching contributors dictionary...")
        contributors = sqlite_to_polars(
            conn,
            "SELECT entity, lentity FROM _REF_mb_disambiguated"
        ).with_columns([
            pl.col("entity").str.strip_chars(),
            pl.col("lentity").str.strip_chars()
        ])

        contributors_dict = dict(zip(
            contributors["lentity"].to_list(),
            contributors["entity"].to_list()
        ))
        logging.info(f"Loaded {len(contributors_dict)} disambiguated contributor entries")

        # Load track data
        logging.info("Fetching tracks data...")
        tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid,
                   artist, composer, arranger, lyricist, writer,
                   albumartist, ensemble, performer,
                   conductor, producer, engineer, mixer, remixer,
                   COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            ORDER BY rowid
            """,
            id_column=("rowid", "sqlmodded")
        )

        columns_to_replace = [
            "artist", "composer", "arranger", "lyricist", "writer",
            "albumartist", "ensemble", "performer",
            "conductor", "producer", "engineer", "mixer", "remixer"
        ]

        # Create surgical filtering masks
        logging.info("Creating contributor masks for surgical filtering...")
        tracks_with_masks = create_contributor_masks(tracks, columns_to_replace, contributors_dict)

        # Filter to only tracks that need processing
        tracks_filtered = filter_processable_tracks(tracks_with_masks, columns_to_replace)
        logging.info(f"Processing {tracks_filtered.height} tracks to validate...")

        if tracks_filtered.height == 0:
            logging.info("No tracks need processing - all contributors already properly disambiguated")
            return

        # Store original data before normalization (for change detection)
        original_tracks = tracks_filtered.clone()

        # Selectively normalize contributors (skip those already in dictionary)
        logging.info("Performing selective contributor normalization...")
        updated_tracks = selective_normalize_contributors(tracks_filtered, columns_to_replace, contributors_dict)

        # Detect changes using mask-aware comparison
        changed_rowids = detect_changes_with_masks(original_tracks, updated_tracks, columns_to_replace)
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            # Remove mask columns before writing to database
            updated_tracks_clean = updated_tracks.drop([f"{col}_in_dict" for col in columns_to_replace])
            original_tracks_clean = original_tracks.drop([f"{col}_in_dict" for col in columns_to_replace])

            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks_clean,
                original_df=original_tracks_clean,
                changed_rowids=changed_rowids,
                columns_to_update=columns_to_replace
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
