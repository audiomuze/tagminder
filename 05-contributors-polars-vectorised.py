"""
Script Name: 05-contributors-polars-optimised_fixed.py

Purpose:
    Connects to a SQLite music library database (alib table) containing track metadata with various contributor fields.
    Loads a reference dictionary of contributor names from a disambiguation table that maps lowercase variants to canonical forms.

    FULLY VECTORIZED APPROACH:
    - Processes ALL records using Polars vectorization for maximum performance
    - Handles multiple contributors separated by delimiters (double backslash \\)
    - Applies smart title case formatting with surname dictionary
    - Looks up canonical forms from the reference dictionary
    - Deduplicates entries
    - Only updates records that actually changed after processing

    Uses pure vectorized operations throughout for optimal performance while maintaining
    comprehensive logging and error handling.

Author: audiomuze
Created: 2025-06-11
Modified: 2025-09-27 - Full vectorization refactor for performance and accuracy
"""

import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any, Union
import logging
from datetime import datetime, timezone
import re

# ---------- Configuration ----------
SCRIPT_NAME = "contributors-polars.py"
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
            "of",
            "a",
            "an",
            "the",
            "and",
            "but",
            "or",
            "for",
            "nor",
            "on",
            "at",
            "to",
            "from",
            "by",
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


# def _vectorized_process_part(part: str, contributors_dict: Dict[str, str]) -> str:
#     """
#     Process a single part with full normalization logic.
#     Returns None for empty results to be filtered out.
#     """
#     if not part or not part.strip():
#         return None

#     part = part.strip()

#     # Check direct dictionary lookup first
#     lowered = part.lower()
#     if lowered in contributors_dict:
#         return contributors_dict[lowered]

#     # Handle comma-containing entries that might be in dictionary
#     if "," in part and lowered in contributors_dict:
#         return contributors_dict[lowered]

#     # Split on secondary delimiters
#     sub_parts = SPLIT_PATTERN.split(part)
#     processed_items = []
#     seen = set()

#     for sub_part in sub_parts:
#         stripped = sub_part.strip()
#         if not stripped:
#             continue

#         sub_lowered = stripped.lower()
#         if sub_lowered in contributors_dict:
#             normalized = contributors_dict[sub_lowered]
#         else:
#             normalized = smart_title(stripped)

#         if normalized and normalized not in seen:
#             processed_items.append(normalized)
#             seen.add(normalized)

#     return DELIMITER.join(processed_items) if processed_items else None


# def optimized_vectorized_normalize_contributors(
#     df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]
# ) -> pl.DataFrame:
#     """
#     Optimized vectorized contributor normalization using efficient Polars operations.
#     Fixed to handle null dtype issues with list.join operations.
#     """
#     expressions = []

#     for column in columns:
#         current_col = pl.col(column)

#         # Create the normalization pipeline with proper null and dtype handling
#         processed_list = (
#             current_col.str.split(DELIMITER)
#             .list.eval(
#                 pl.element().map_elements(
#                     lambda x: _vectorized_process_part(x, contributors_dict),
#                     return_dtype=pl.Utf8,
#                 )
#             )
#             .list.drop_nulls()
#             .list.unique()
#         )

#         # Handle the join operation with explicit dtype casting and null safety
#         normalized_expr = (
#             pl.when(current_col.is_null())
#             .then(None)
#             .otherwise(
#                 pl.when(processed_list.is_null() | (processed_list.list.len() == 0))
#                 .then(None)
#                 .otherwise(
#                     # Ensure we have a string list before joining
#                     # Cast to string explicitly to avoid dtype null issues
#                     processed_list.list.eval(
#                         pl.when(pl.element().is_null())
#                         .then(pl.lit(""))  # Convert nulls to empty strings
#                         .otherwise(pl.element().cast(pl.Utf8))
#                     )
#                     .list.filter(pl.element() != "")  # Remove empty strings
#                     .list.join(DELIMITER)
#                 )
#             )
#         )

#         # Handle case where result is empty string and ensure final null handling
#         final_expr = (
#             pl.when((normalized_expr == "") | normalized_expr.is_null())
#             .then(None)
#             .otherwise(normalized_expr)
#         )

#         expressions.append(final_expr.alias(column))

#     return df.with_columns(expressions)


def _vectorized_process_part(part: str, contributors_dict: Dict[str, str]) -> str:
    """
    Process a single part with full normalization logic and order-preserving deduplication.
    Returns None for empty results to be filtered out.
    """
    if not part or not part.strip():
        return None

    part = part.strip()

    # Check direct dictionary lookup first
    lowered = part.lower()
    if lowered in contributors_dict:
        return contributors_dict[lowered]

    # Handle comma-containing entries that might be in dictionary
    if "," in part and lowered in contributors_dict:
        return contributors_dict[lowered]

    # Split on secondary delimiters
    sub_parts = SPLIT_PATTERN.split(part)
    processed_items = []

    for sub_part in sub_parts:
        stripped = sub_part.strip()
        if not stripped:
            continue

        sub_lowered = stripped.lower()
        if sub_lowered in contributors_dict:
            normalized = contributors_dict[sub_lowered]
        else:
            normalized = smart_title(stripped)

        if normalized:
            processed_items.append(normalized)

    # Fast order-preserving deduplication using dict.fromkeys()
    if processed_items:
        deduplicated = list(dict.fromkeys(processed_items))
        return DELIMITER.join(deduplicated)

    return None


def optimized_vectorized_normalize_contributors(
    df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Optimized vectorized contributor normalization using efficient Polars operations.
    Fixed to handle null dtype issues with list.join operations and preserve order.
    """
    expressions = []

    for column in columns:
        current_col = pl.col(column)

        # Create the normalization pipeline with proper null and dtype handling
        processed_list = (
            current_col.str.split(DELIMITER)
            .list.eval(
                pl.element().map_elements(
                    lambda x: _vectorized_process_part(x, contributors_dict),
                    return_dtype=pl.Utf8,
                )
            )
            .list.drop_nulls()
            .list.unique(maintain_order=True)  # ORDER-PRESERVING deduplication
        )

        # Handle the join operation with explicit dtype casting and null safety
        normalized_expr = (
            pl.when(current_col.is_null())
            .then(None)
            .otherwise(
                pl.when(processed_list.is_null() | (processed_list.list.len() == 0))
                .then(None)
                .otherwise(
                    # Ensure we have a string list before joining
                    # Cast to string explicitly to avoid dtype null issues
                    processed_list.list.eval(
                        pl.when(pl.element().is_null())
                        .then(pl.lit(""))  # Convert nulls to empty strings
                        .otherwise(pl.element().cast(pl.Utf8))
                    )
                    .list.filter(pl.element() != "")  # Remove empty strings
                    .list.join(DELIMITER)
                )
            )
        )

        # Handle case where result is empty string and ensure final null handling
        final_expr = (
            pl.when((normalized_expr == "") | normalized_expr.is_null())
            .then(None)
            .otherwise(normalized_expr)
        )

        expressions.append(final_expr.alias(column))

    return df.with_columns(expressions)


# ---------- Change Detection ----------


def detect_changes_vectorized(
    original_df: pl.DataFrame, updated_df: pl.DataFrame, columns: List[str]
) -> pl.DataFrame:
    """
    Vectorized change detection using Polars native operations.
    Only considers rows where the original value was not null.
    """
    # Create change detection expressions
    change_exprs = [
        (original_df[col] != updated_df[col]) & original_df[col].is_not_null()
        for col in columns
    ]

    # Find rows with any changes
    changed_mask = pl.any_horizontal(change_exprs)
    changed_rows = updated_df.filter(changed_mask).select("rowid")

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
    """
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    try:
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
        original_df_filtered = original_df.filter(pl.col("rowid").is_in(changed_rowids))

        # Convert to dictionaries for processing
        update_records = {row["rowid"]: row for row in update_df.to_dicts()}
        original_records = {
            row["rowid"]: row for row in original_df_filtered.to_dicts()
        }

        for rowid in changed_rowids:
            record = update_records[rowid]
            original_row = original_records[rowid]

            # Identify which columns actually changed and have new values
            changed_cols = [
                col
                for col in columns_to_update
                if record[col] != original_row[col] and original_row[col] is not None
            ]

            if not changed_cols:
                continue

            # Increment sqlmodded counter by number of fields changed
            new_sqlmodded = int(original_row["sqlmodded"] or 0) + len(changed_cols)
            set_clause = (
                ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
            )
            values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

            # Update the main table
            cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

            # Log each field change to changelog
            for col in changed_cols:
                cursor.execute(
                    "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        rowid,
                        col,
                        original_row[col],
                        record[col],
                        timestamp,
                        SCRIPT_NAME,
                    ),
                )

            updated_count += 1

        conn.commit()
        logging.info(f"Updated {updated_count} rows and logged all changes.")
        return updated_count

    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating database: {e}")
        raise


# ---------- Main Execution Function ----------


def main():
    """
    Main execution function with fully vectorized processing approach.
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

        # Load ALL track data - no pre-filtering
        logging.info("Fetching all tracks data...")
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

        logging.info(f"Processing {tracks.height} total tracks...")

        # Store original data before normalization
        original_tracks = tracks.clone()

        # Apply full vectorized normalization to ALL records
        logging.info("Performing vectorized contributor normalization...")
        updated_tracks = optimized_vectorized_normalize_contributors(
            tracks, columns_to_replace, contributors_dict
        )

        # Detect changes using vectorized comparison
        logging.info("Detecting changes...")
        changed_rows = detect_changes_vectorized(
            original_tracks, updated_tracks, columns_to_replace
        )
        changed_rowids = changed_rows["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks,
                original_df=original_tracks,
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
