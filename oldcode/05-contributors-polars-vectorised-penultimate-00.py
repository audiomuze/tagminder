"""
Script Name: 05-contributors-polars-optimised-final.py

Purpose:
    Connects to a SQLite music library database (alib table) containing track metadata with various contributor fields (artist, composer, arranger, lyricist, etc.)
    Loads a reference dictionary of contributor names from a disambiguation table (_REF_mb_disambiguated) that maps lowercase variants to canonical/proper forms

    SURGICAL FILTERING ENHANCEMENT:
    The script surgically filters out contributors that already exist in the disambiguation dictionary,
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

    OPTIMIZED VERSION: Merges surgical filtering with vectorized Polars operations for maximum efficiency.

Usage:
    python 05-contributors-polars-optimised-final.py

Author: audiomuze
Created: 2025-06-11
Modified: 2025-06-28 - Merged surgical filtering with vectorized Polars operations
"""

import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any, Union
import logging
from datetime import datetime, timezone
import re

# ---------- Configuration ----------
SCRIPT_NAME = "05-contributors-polars-optimised-final.py"
DB_PATH = "/tmp/amg/dbtemplate.db"

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- Global Constants ----------
# Main delimiter for joining multiple contributors
DELIMITER = "\\\\"  # Double backslash for splitting and joining

# Regex pattern for splitting on various delimiters, but not commas followed by suffixes
SPLIT_PATTERN = re.compile(
    r"(?:\\\\|;|/|,(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b))"
)

# Surname dictionary for transforming names not matched to an entry in _REF_mb_disambiguated table
SURNAME_DICT = {
    # Mac Surnames
    "macintyre": "MacIntyre",
    "macallister": "MacAllister",
    "mackenzie": "MacKenzie",
    "macdonald": "MacDonald",
    "maclachlan": "MacLachlan",
    "macgregor": "MacGregor",
    "macpherson": "MacPherson",
    "maclean": "MacLean",
    "macleod": "MacLeod",
    "macneil": "MacNeil",
    # Mc Surnames
    "mcdaniel": "McDaniel",
    "mcdonald": "McDonald",
    "mcintyre": "McIntyre",
    "mckenzie": "McKenzie",
    "mcallister": "McAllister",
    "mcfarland": "McFarland",
    "mcgregor": "McGregor",
    "mcnamara": "McNamara",
    "mcguire": "McGuire",
    "mcgrath": "McGrath",
    "mcguirk": "McGuirk",
    "mcpherson": "McPherson",
    "mcleod": "McLeod",
    "mcvey": "McVey",
    # O' Surnames
    "obrien": "O'Brien",
    "odonnell": "O'Donnell",
    "oconnor": "O'Connor",
    "oneill": "O'Neill",
    "omally": "O'Malley",
    "ohara": "O'Hara",
    "okeeffe": "O'Keeffe",
    "oreilly": "O'Reilly",
    "osullivan": "O'Sullivan",
    # Fitz Surnames
    "fitzgibbon": "FitzGibbon",
    "fitzhenry": "FitzHenry",
    # De / De La Surnames
    "desantis": "DeSantis",
    "delorean": "DeLorean",
    "delacruz": "De La Cruz",
    "delarosa": "De La Rosa",
    "deguzman": "De Guzman",
    "degaulle": "de Gaulle",
    "demedici": "de Medici",
    "devito": "DeVito",
    "depalma": "DePalma",
    "donatello": "Donatello",
    # Van Surnames (Dutch)
    "vanpelt": "Van Pelt",
    "vandamme": "Van Damme",
    "vanhalen": "Van Halen",
    "vanderbilt": "Vanderbilt",
    "vanderveer": "Vanderveer",
    "vanburen": "Van Buren",
    "vanhouten": "Van Houten",
    "vangogh": "van Gogh",
    # Von Surnames (German)
    "vonbeethoven": "von Beethoven",
    "vontrapp": "von Trapp",
    "vonbraun": "von Braun",
    "vondoom": "Von Doom",
}

# ---------- Database Helper Functions ----------

def sqlite_to_polars(
    conn: sqlite3.Connection, query: str, id_column: Union[str, Tuple[str, ...]] = None
) -> pl.DataFrame:
    """
    Convert SQLite query results to a Polars DataFrame with proper type handling.

    Args:
        conn: SQLite database connection
        query: SQL query to execute
        id_column: Column(s) to treat as integer IDs

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
                name=col_name, values=[int(x or 0) for x in col_data], dtype=pl.Int64
            )
        else:
            # String columns with null handling
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8,
            )

    return pl.DataFrame(data)

# ---------- Text Processing Functions ----------

def smart_title(text):
    """
    Apply intelligent title casing that preserves certain patterns and handles special cases.
    Includes surname dictionary lookup and preserves uppercase initials.
    """
    if not text:
        return text

    # First check if the entire text matches a surname pattern
    lowered = text.lower()
    if lowered in SURNAME_DICT:
        return SURNAME_DICT[lowered]

    def fix_caps_word(word, is_first_word=False, follows_bracket=False):
        """Apply capitalization rules to a single word."""
        # Check if this word matches a surname pattern
        lowered_word = word.lower()
        if lowered_word in SURNAME_DICT:
            return SURNAME_DICT[lowered_word]

        # Check if this is an initial with a period (like "A." or "J.R.")
        if re.match(r"^[A-Z]\.$", word) or re.match(r"^[A-Z]\.[A-Z]\.$", word):
            return word  # Preserve as-is if it's already an uppercase initial

        lower_words = [
            "of", "a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by",
        ]

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
            # Handle initials like "J.R.R." - ensure they're uppercase
            parts = word.split(".")
            processed_parts = []
            for part in parts:
                if part and len(part) == 1:  # Single character initial
                    processed_parts.append(part.upper())
                else:
                    processed_parts.append(part.capitalize())
            return ".".join(processed_parts)
        elif "'" in word or "'" in word:
            # Handle possessives and contractions
            apos_pos = max(word.find("'"), word.find("'"))
            if 0 < apos_pos < len(word) - 1:
                return word[:apos_pos].capitalize() + word[apos_pos:]
            else:
                return word.capitalize()
        elif "-" in word:
            # Handle hyphenated words
            parts = word.split("-")
            return "-".join(part.capitalize() for part in parts)
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
                processed_word = fix_caps_word(
                    word, is_first_word=capitalize_next, follows_bracket=False
                )
                # Handle possessive 's
                if processed_word.lower().endswith("'s"):
                    processed_word = processed_word[:-2] + "'s"
                elif processed_word.lower().endswith("'s"):
                    processed_word = processed_word[:-2] + "'s"
                # Special rule for "O'"
                elif (
                    word.lower().startswith("o'")
                    and len(word) > 2
                    and word[2].lower() != "s"
                    and word[2] != " "
                ):
                    processed_word = "O'" + fix_caps_word(
                        word[2:], is_first_word=False, follows_bracket=False
                    )
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

# ---------- Vectorized Processing Functions ----------

def vectorized_normalize_contributors(
    df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Vectorized normalization of contributor columns using Polars native operations.

    This function handles:
    - Dictionary lookup for canonical forms
    - Splitting on delimiters
    - Deduplication
    - Smart title casing fallback

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names to normalize
        contributors_dict: Dictionary mapping lowercase names to canonical forms

    Returns:
        DataFrame with normalized contributor columns
    """
    # Create Polars Series for efficient mapping
    lookup_keys = pl.Series(list(contributors_dict.keys()))
    canonical_values = pl.Series(list(contributors_dict.values()))

    expressions = []

    for column in columns:
        current_col = pl.col(column)

        # Step 1: Handle main delimiter splitting and dictionary lookup
        split_parts = current_col.str.split(DELIMITER)

        # Process each split part: lookup canonical form or apply smart title
        processed_parts = split_parts.list.eval(
            pl.element()
            .str.to_lowercase()
            .replace(lookup_keys, canonical_values)
            .map_elements(
                lambda x: smart_title(x)
                if x and x.lower() not in contributors_dict
                else x,
                return_dtype=pl.Utf8,
            )
        )

        # Step 2: Further split on secondary delimiters with comma preservation
        final_expr = (
            pl.when(processed_parts.is_not_null())
            .then(
                processed_parts.map_elements(
                    lambda parts: _process_parts_vectorized(parts, contributors_dict),
                    return_dtype=pl.Utf8,
                )
            )
            .otherwise(None)
        )

        expressions.append(final_expr.alias(column))

    return df.with_columns(expressions)

def _process_parts_vectorized(parts: Any, contributors_dict: Dict[str, str]) -> str:
    """
    Helper function to process individual parts with secondary splitting and deduplication.
    """
    if isinstance(parts, pl.Series):
        if parts.is_null().any():
            return None
        parts = parts.to_list()

    if not parts:
        return None

    processed_items = []
    seen = set()

    for part in parts:
        if not part:
            continue

        # Check if this part contains commas and exists in dictionary
        if "," in part and part.lower() in contributors_dict:
            normalized = contributors_dict[part.lower()]
            if normalized not in seen:
                processed_items.append(normalized)
                seen.add(normalized)
            continue

        # Split on secondary delimiters
        sub_parts = SPLIT_PATTERN.split(part)
        for sub_part in sub_parts:
            stripped = sub_part.strip()
            if not stripped:
                continue

            # Lookup or apply smart title
            lowered = stripped.lower()
            if lowered in contributors_dict:
                normalized = contributors_dict[lowered]
            else:
                normalized = smart_title(stripped)

            if normalized not in seen:
                processed_items.append(normalized)
                seen.add(normalized)

    return DELIMITER.join(processed_items) if processed_items else None

# ---------- Surgical Filtering Functions ----------

def create_and_filter_tracks(
    df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Create boolean masks and filter to only tracks that need processing in a single operation.

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names to check
        contributors_dict: Dictionary mapping lowercase names to canonical forms

    Returns:
        Filtered DataFrame containing only tracks that need processing, with mask columns
    """
    # Create lookup series for efficient membership testing
    dict_keys = pl.Series(list(contributors_dict.keys()))
    canonical_values = pl.Series(list(contributors_dict.values()))

    # Create mask expressions for each column
    mask_exprs = [
        pl.col(col)
        .str.to_lowercase()
        .is_in(dict_keys)
        .alias(f"{col}_in_dict")
        for col in columns
    ]

    # Add mask columns and filter in one operation
    df_with_masks = df.with_columns(mask_exprs)

    # Check if values need normalization (even if in dictionary, case might differ)
    needs_processing_expr = pl.any_horizontal(
        [
            pl.col(col).is_not_null()
            & (
                ~pl.col(f"{col}_in_dict")  # Not in dictionary OR
                | (
                    pl.col(col)
                    != pl.col(col)
                    .str.to_lowercase()
                    .replace(dict_keys, canonical_values)  # Case mismatch
                )
            )
            for col in columns
        ]
    )
    
    return df_with_masks.filter(needs_processing_expr)

def selective_normalize_contributors(
    df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Selectively normalize contributor columns using vectorized Polars operations.
    Only processes entries that need normalization.
    """
    # First, handle the easy case: entries already in dictionary
    lookup_keys = pl.Series(list(contributors_dict.keys()))
    canonical_values = pl.Series(list(contributors_dict.values()))

    expressions = []

    for column in columns:
        current_col = pl.col(column)

        # Check if value is already canonical before processing
        is_already_canonical = (
            current_col.str.to_lowercase().replace(lookup_keys, canonical_values)
            == current_col
        )

        # Only process non-canonical values
        needs_processing = ~is_already_canonical

        # Apply canonical forms to entries that are in dictionary but not canonical
        canonical_expr = (
            pl.when(pl.col(f"{column}_in_dict") & needs_processing)
            .then(current_col.str.to_lowercase().replace(lookup_keys, canonical_values))
            .otherwise(current_col)
        )

        expressions.append(canonical_expr.alias(column))

    df_with_canonical = df.with_columns(expressions)

    # Now normalize the remaining entries using the vectorized approach
    return vectorized_normalize_contributors(
        df_with_canonical, columns, contributors_dict
    )

def detect_changes_vectorized(
    original_df: pl.DataFrame, updated_df: pl.DataFrame, columns: List[str]
) -> pl.DataFrame:
    """
    Vectorized change detection using Polars native operations.

    Args:
        original_df: Original DataFrame
        updated_df: Updated DataFrame after normalization
        columns: List of contributor column names

    Returns:
        DataFrame with changed rowids
    """
    # Create change detection expressions
    change_exprs = [
        (original_df[col] != updated_df[col]) & original_df[col].is_not_null()
        for col in columns
    ]

    # Find rows with any changes
    changed_rows = updated_df.filter(pl.any_horizontal(change_exprs)).select("rowid")

    return changed_rows

# ---------- Database Update Functions ----------

def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int],
    columns_to_update: List[str],
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
            col
            for col in columns_to_update
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
                (rowid, col, original_row[col], record[col], timestamp, SCRIPT_NAME),
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
    3. Create masks and filter to tracks that need processing
    4. Selectively normalize only unmatched contributors
    5. Detect actual changes using vectorized comparison
    6. Update database with changes and log to changelog
    """
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Load disambiguation dictionary
        logging.info("Fetching contributors dictionary...")
        contributors = sqlite_to_polars(
            conn, "SELECT entity, lentity FROM _REF_mb_disambiguated"
        ).with_columns(
            [pl.col("entity").str.strip_chars(), pl.col("lentity").str.strip_chars()]
        )

        contributors_dict = dict(
            zip(contributors["lentity"].to_list(), contributors["entity"].to_list())
        )
        logging.info(
            f"Loaded {len(contributors_dict)} disambiguated contributor entries"
        )

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
            id_column=("rowid", "sqlmodded"),
        )

        columns_to_replace = [
            "artist",
            "composer",
            "arranger",
            "lyricist",
            "writer",
            "albumartist",
            "ensemble",
            "performer",
            "conductor",
            "producer",
            "engineer",
            "mixer",
            "remixer",
        ]

        # Create masks and filter to only tracks that need processing
        logging.info("Creating contributor masks and filtering tracks...")
        tracks_filtered = create_and_filter_tracks(
            tracks, columns_to_replace, contributors_dict
        )
        logging.info(f"Processing {tracks_filtered.height} tracks to validate...")

        if tracks_filtered.height == 0:
            logging.info(
                "No tracks need processing - all contributors already properly disambiguated"
            )
            return

        # Store original data before normalization (for change detection)
        original_tracks = tracks_filtered.clone()

        # Selectively normalize contributors (skip those already in dictionary)
        logging.info("Performing selective contributor normalization...")
        updated_tracks = selective_normalize_contributors(
            tracks_filtered, columns_to_replace, contributors_dict
        )

        # Detect changes using vectorized comparison
        logging.info("Detecting changes...")
        changed_rows = detect_changes_vectorized(
            original_tracks, updated_tracks, columns_to_replace
        )
        changed_rowids = changed_rows["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            # Remove mask columns before writing to database
            mask_columns = [f"{col}_in_dict" for col in columns_to_replace]
            updated_tracks_clean = updated_tracks.drop(mask_columns)
            original_tracks_clean = original_tracks.drop(mask_columns)

            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks_clean,
                original_df=original_tracks_clean,
                changed_rowids=changed_rowids,
                columns_to_update=columns_to_replace,
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
