"""
Purpose:
    Normalize contributor fields in `alib` using a disambiguation reference
    mapping of lowercase variants to canonical forms.

    Uses a fully vectorized Polars approach, de-duplicates contributor lists,
    and applies shared contributor-case fallback rules for unresolved names
    through `tm_contributor_case.smart_title`.

    Canonical mappings are sourced from `contributors_unified_disambiguated` and, when
    present, supplemented with `contributors_unified_namesakes` entries.

    updates only changed rows, and logs per-field changes to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - contributors_unified_disambiguated
    - contributors_unified_namesakes (optional)
    - changelog

Author: audiomuze
Last updated: 2026-07-05
"""

import polars as pl
import sqlite3
from typing import Dict, List
import logging
import re

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_contributor_case
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# ---------- Configuration ----------


# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- Global Constants ----------
# Multi-value delimiter used by Tagminder (written to SQLite as two literal backslashes).
# Source of truth: tagminder.toml [strings].multivalue_delimiter
DELIMITER = tm_config.get_multivalue_delimiter()

# Regex patterns for splitting contributor strings.
# Stage 1 splits on explicit multi-value delimiters except '/'.
# Stage 2 optionally splits on '/'.
# Stage 3 splits on commas only when they are likely list separators.
_DELIM_REGEX = re.escape(DELIMITER)
PRIMARY_SPLIT_PATTERN = re.compile(rf"(?:{_DELIM_REGEX}|;)")
SLASH_SPLIT_PATTERN = re.compile(r"/")
AMPERSAND_SPLIT_PATTERN = re.compile(r"\s*&\s*")
COMMA_SPLIT_PATTERN = re.compile(
    r",(?!\d{3}(?:\D|$))(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b)"
)

# ---------- Text Processing Functions ----------


# def smart_title(text):
#     """
#     Apply intelligent title casing that preserves certain patterns and handles special cases.
#     Includes surname dictionary lookup and preserves uppercase initials.
#     """
#     if not text:
#         return text

#     # First check if the entire text matches a surname pattern
#     lowered = text.lower()
#     if lowered in SURNAME_DICT:
#         return SURNAME_DICT[lowered]

#     def fix_caps_word(word, is_first_word=False, follows_bracket=False):
#         """Apply capitalization rules to a single word."""
#         # Check if this word matches a surname pattern
#         lowered_word = word.lower()
#         if lowered_word in SURNAME_DICT:
#             return SURNAME_DICT[lowered_word]

#         # Check if this is an initial with a period (like "A." or "J.R.")
#         if re.match(r"^[A-Z]\.$", word) or re.match(r"^[A-Z]\.[A-Z]\.$", word):
#             return word  # Preserve as-is if it's already an uppercase initial

#         lower_words = [
#             "of",
#             "a",
#             "an",
#             "the",
#             "and",
#             "but",
#             "or",
#             "for",
#             "nor",
#             "on",
#             "at",
#             "to",
#             "from",
#             "by",
#         ]

#         if is_first_word:
#             # First word is always capitalized unless already has uppercase
#             if any(c.isupper() for c in word):
#                 return word
#             else:
#                 return word.capitalize()
#         elif follows_bracket:
#             return word.capitalize()
#         elif any(c.isupper() for c in word):
#             # Preserve existing capitalization
#             return word
#         elif re.match(r"^[IVXLCDM]+$", word.upper()):
#             # Roman numerals
#             return word.upper()
#         elif "." in word:
#             # Handle initials like "J.R.R." - ensure they're uppercase
#             parts = word.split(".")
#             processed_parts = []
#             for part in parts:
#                 if part and len(part) == 1:  # Single character initial
#                     processed_parts.append(part.upper())
#                 else:
#                     processed_parts.append(part.capitalize())
#             return ".".join(processed_parts)
#         elif "'" in word or "'" in word:
#             # Handle possessives and contractions
#             apos_pos = max(word.find("'"), word.find("'"))
#             if 0 < apos_pos < len(word) - 1:
#                 return word[:apos_pos].capitalize() + word[apos_pos:]
#             else:
#                 return word.capitalize()
#         elif "-" in word:
#             # Handle hyphenated words
#             parts = word.split("-")
#             return "-".join(part.capitalize() for part in parts)
#         elif word.lower() in lower_words:
#             # Keep articles and prepositions lowercase
#             return word
#         else:
#             return word.capitalize()

#     # Regex to capture words (including McNames, O'Names, possessives)
#     word_pattern = r"\b(?:Mc\w+|O'\w+|\w+(?:['']\w+)?)\b"
#     # Regex to capture non-word parts (spaces, punctuation)
#     non_word_pattern = r"[^\w\s]+"

#     # Combine the patterns to capture words and non-word parts
#     combined_pattern = rf"({word_pattern})|({non_word_pattern})|\s+"

#     parts = re.findall(combined_pattern, text)
#     result = []
#     capitalize_next = True

#     for part_tuple in parts:
#         word = part_tuple[0] or part_tuple[1]
#         if word:
#             if re.match(word_pattern, word):  # It's a word
#                 processed_word = fix_caps_word(
#                     word, is_first_word=capitalize_next, follows_bracket=False
#                 )
#                 # Handle possessive 's
#                 if processed_word.lower().endswith("'s"):
#                     processed_word = processed_word[:-2] + "'s"
#                 elif processed_word.lower().endswith("'s"):
#                     processed_word = processed_word[:-2] + "'s"
#                 # Special rule for "O'"
#                 elif (
#                     word.lower().startswith("o'")
#                     and len(word) > 2
#                     and word[2].lower() != "s"
#                     and word[2] != " "
#                 ):
#                     processed_word = "O'" + fix_caps_word(
#                         word[2:], is_first_word=False, follows_bracket=False
#                     )
#                 result.append(processed_word)
#                 capitalize_next = False
#             else:  # It's a non-word part
#                 result.append(word)
#                 capitalize_next = word in "({[<"
#         else:
#             result.append(" ")  # It's whitespace

#     processed_text = "".join(result)
#     # Final pass to ensure possessive 's is lowercase
#     processed_text = re.sub(r"(\w)['']S\b", r"\1's", processed_text)

#     return processed_text


# def smart_title(text):
#     """
#     Apply intelligent title casing that preserves certain patterns and handles special cases.
#     Includes surname dictionary lookup. Normalizes all-caps and mixed-case to proper title case.
#     """
#     if not text:
#         return text

#     # First check if the entire text matches a surname pattern
#     lowered = text.lower()
#     if lowered in SURNAME_DICT:
#         return SURNAME_DICT[lowered]

#     def fix_caps_word(word, is_first_word=False, follows_bracket=False):
#         """Apply capitalization rules to a single word."""
#         # Check if this word matches a surname pattern
#         lowered_word = word.lower()
#         if lowered_word in SURNAME_DICT:
#             return SURNAME_DICT[lowered_word]

#         # Check if this is initials with periods (like "A." or "J.R." or "A.D.")
#         # Pattern: one or more single letters followed by periods
#         if re.match(r"^([A-Za-z]\.)+$", word, re.IGNORECASE):
#             return word.upper()

#         lower_words = [
#             "of",
#             "a",
#             "an",
#             "the",
#             "and",
#             "but",
#             "or",
#             "for",
#             "nor",
#             "on",
#             "at",
#             "to",
#             "from",
#             "by",
#         ]

#         if is_first_word:
#             # First word is always capitalized
#             return word.capitalize()
#         elif follows_bracket:
#             return word.capitalize()
#         elif re.match(r"^[IVXLCDM]+$", word.upper()):
#             # Roman numerals - check if it's likely a roman numeral
#             return word.upper()
#         elif "." in word:
#             # Handle initials like "J.R.R." - normalize to uppercase
#             parts = word.split(".")
#             processed_parts = []
#             for part in parts:
#                 if part and len(part) == 1:  # Single character initial
#                     processed_parts.append(part.upper())
#                 else:
#                     processed_parts.append(part.capitalize())
#             return ".".join(processed_parts)
#         elif "'" in word or "'" in word:
#             # Handle possessives and contractions
#             apos_pos = max(word.find("'"), word.find("'"))
#             if 0 < apos_pos < len(word) - 1:
#                 return word[:apos_pos].capitalize() + word[apos_pos:]
#             else:
#                 return word.capitalize()
#         elif "-" in word:
#             # Handle hyphenated words
#             parts = word.split("-")
#             return "-".join(part.capitalize() for part in parts)
#         elif word.lower() in lower_words:
#             # Keep articles and prepositions lowercase
#             return word.lower()
#         else:
#             return word.capitalize()

#     # Regex to capture words including initials with periods (A.D., J.R.R., etc.)
#     # Put initials pattern first so it matches greedily
#     word_pattern = r"(?:[A-Za-z]\.){2,}|[A-Za-z]\.|Mc\w+|O'\w+|\w+(?:['']\w+)?"
#     # Regex to capture non-word parts (spaces, punctuation)
#     non_word_pattern = r"[^\w\s]+"

#     # Combine the patterns to capture words and non-word parts
#     combined_pattern = rf"({word_pattern})|({non_word_pattern})|\s+"

#     parts = re.findall(combined_pattern, text)
#     result = []
#     capitalize_next = True

#     for part_tuple in parts:
#         word = part_tuple[0] or part_tuple[1]
#         if word:
#             if re.match(word_pattern, word):  # It's a word
#                 # DEBUG
#                 if "fairfield" in text.lower():
#                     print(f"  Processing word: '{word}'")
#                 processed_word = fix_caps_word(
#                     word, is_first_word=capitalize_next, follows_bracket=False
#                 )
#                 # DEBUG
#                 if "fairfield" in text.lower():
#                     print(f"  Result: '{processed_word}'")
#                 # Handle possessive 's
#                 if processed_word.lower().endswith("'s"):
#                     processed_word = processed_word[:-2] + "'s"
#                 elif processed_word.lower().endswith("'s"):
#                     processed_word = processed_word[:-2] + "'s"
#                 # Special rule for "O'"
#                 elif (
#                     word.lower().startswith("o'")
#                     and len(word) > 2
#                     and word[2].lower() != "s"
#                     and word[2] != " "
#                 ):
#                     processed_word = "O'" + fix_caps_word(
#                         word[2:], is_first_word=False, follows_bracket=False
#                     )
#                 result.append(processed_word)
#                 capitalize_next = False
#             else:  # It's a non-word part
#                 result.append(word)
#                 capitalize_next = word in "({[<"
#         else:
#             result.append(" ")  # It's whitespace

#     processed_text = "".join(result)
#     # Final pass to ensure possessive 's is lowercase
#     processed_text = re.sub(r"(\w)['']S\b", r"\1's", processed_text)

#     return processed_text


def smart_title(text):
    """
    Backward-compatible wrapper around shared contributor casing helper.
    """
    return tm_contributor_case.smart_title(text)


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


def _vectorized_process_part(part: str, contributors_dict: Dict[str, str]) -> str | None:
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

    # Stage 1: split on explicit multi-value delimiters first.
    # Keep comma-containing chunks intact so we can try dictionary matches before comma fallback.
    primary_parts = PRIMARY_SPLIT_PATTERN.split(part)
    processed_items = []

    for primary_part in primary_parts:
        stripped_primary = primary_part.strip()
        if not stripped_primary:
            continue

        # Prefer exact dictionary mapping for the whole chunk, including commas.
        primary_lowered = stripped_primary.lower()
        if primary_lowered in contributors_dict:
            processed_items.append(contributors_dict[primary_lowered])
            continue

        # Stage 2: comma fallback for unresolved chunks.
        comma_parts = COMMA_SPLIT_PATTERN.split(stripped_primary)

        for comma_part in comma_parts:
            stripped_comma = comma_part.strip()
            if not stripped_comma:
                continue

            # Re-check dictionary before slash fallback so names like "20/20" can be preserved.
            comma_lowered = stripped_comma.lower()
            if comma_lowered in contributors_dict:
                processed_items.append(contributors_dict[comma_lowered])
                continue

            # Stage 3: slash fallback for remaining unresolved chunks.
            slash_parts = SLASH_SPLIT_PATTERN.split(stripped_comma)

            for slash_part in slash_parts:
                stripped = slash_part.strip()
                if not stripped:
                    continue

                # Conservative '&' splitting: only split when all sides resolve in refs
                # AND the full string does not itself resolve as a single entity.
                # This avoids breaking band names like "Fathers & Sons" when both words
                # happen to appear independently in the reference table.
                if "&" in stripped:
                    amp_parts = [p.strip() for p in AMPERSAND_SPLIT_PATTERN.split(stripped)]
                    if len(amp_parts) > 1 and stripped.lower() not in contributors_dict and all(
                        part and part.lower() in contributors_dict for part in amp_parts
                    ):
                        for amp_part in amp_parts:
                            processed_items.append(contributors_dict[amp_part.lower()])
                        continue

                sub_lowered = stripped.lower()
                if sub_lowered in contributors_dict:
                    normalized = contributors_dict[sub_lowered]
                else:
                    # Preserve unresolved hyphenated names as-is (e.g. AC-DC)
                    # instead of applying fallback word-level hyphen splitting.
                    if "-" in stripped:
                        normalized = stripped
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
    tm_db.ensure_changelog_table(conn)

    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()
    updated_count = 0

    # Process only the rows that changed
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    original_df_filtered = original_df.filter(pl.col("rowid").is_in(changed_rowids))

    # Convert to dictionaries for processing
    update_records = {row["rowid"]: row for row in update_df.to_dicts()}
    original_records = {
        row["rowid"]: row for row in original_df_filtered.to_dicts()
    }

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for rowid in changed_rowids:
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            record = update_records[rowid]
            original_row = original_records[rowid]

            eligible_fields = [
                col for col in columns_to_update if original_row.get(col) is not None
            ]

            def _norm(v: object) -> str | None:
                if v is None:
                    return None
                return v if isinstance(v, str) else str(v)

            changes: list[tuple[str, object, object]] = []
            changed_cols: list[str] = []
            for col in eligible_fields:
                old_v = original_row.get(col)
                new_v = record.get(col)
                if _norm(old_v) == _norm(new_v):
                    continue
                changes.append((col, old_v, new_v))
                changed_cols.append(col)

            if not changes:
                continue

            # Increment __sqlmodded counter by number of fields changed
            new_sqlmodded = int(original_row["__sqlmodded"] or 0) + len(changed_cols)
            sql = tm_db.build_update_sql(table="alib", set_cols=changed_cols)
            values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

            # Update the main table
            cursor.execute(sql, values)

            # Log each field change to changelog
            changelog.add(alib_path=alib_path, changes=changes)

            updated_count += 1

        changelog.flush(cursor)

    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


# ---------- Main Execution Function ----------


def main():
    """
    Main execution function with fully vectorized processing approach.
    """
    try:
        conn, db_path, _, _ = tm_run.open_db(ensure_changelog=True, require_exists=True)
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

    master_db_path = tm_config.get_master_data_db_path(default=db_path)
    master_conn = conn if master_db_path == db_path else tm_db.connect(master_db_path, read_only=True)

    try:
        tm_db.require_table_columns(
            master_conn,
            "contributors_unified_disambiguated",
            ("preferred__artist_name", "lpreferred__artist_name"),
            hint="Run emit_contributors.py first so contributors_unified_disambiguated is available.",
        )

        # Load disambiguation dictionary
        logging.info("Fetching contributors dictionary from contributors_unified_disambiguated...")
        contributors_ref = tm_polars_db.sqlite_to_polars(
            master_conn,
            "SELECT preferred__artist_name AS contributor, "
            "lpreferred__artist_name "
            "FROM contributors_unified_disambiguated",
        ).with_columns(
            [
                pl.col("contributor").str.strip_chars(),
                pl.col("lpreferred__artist_name").str.strip_chars(),
            ]
        )

        contributors_dict = dict(
            zip(
                contributors_ref["lpreferred__artist_name"].to_list(),
                contributors_ref["contributor"].to_list(),
            )
        )
        logging.info(
            f"Loaded {len(contributors_dict)} disambiguated contributor entries"
        )

        # Also include namesakes so canonical names like "20/20" are recognized before fallback splitting.
        namesakes_exists = master_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='contributors_unified_namesakes' LIMIT 1"
        ).fetchone()
        if namesakes_exists:
            logging.info("Fetching additional namesake entries from contributors_unified_namesakes...")
            contributors_namesakes = tm_polars_db.sqlite_to_polars(
                master_conn,
                "SELECT preferred__artist_name AS contributor, "
                "lpreferred__artist_name "
                "FROM contributors_unified_namesakes",
            ).with_columns(
                [pl.col("contributor").str.strip_chars(), pl.col("lpreferred__artist_name").str.strip_chars()]
            )

            namesakes_added = 0
            for lentity, entity in zip(
                contributors_namesakes["lpreferred__artist_name"].to_list(),
                contributors_namesakes["contributor"].to_list(),
            ):
                if lentity not in contributors_dict:
                    contributors_dict[lentity] = entity
                    namesakes_added += 1

            logging.info(
                f"Added {namesakes_added} namesake entries; total contributor lookup size is {len(contributors_dict)}"
            )
        else:
            logging.info("contributors_unified_namesakes table not found; proceeding with contributors_unified_disambiguated only")

        # Load ALL track data - no pre-filtering
        logging.info("Fetching all tracks data...")
        tracks = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid,
                   artist, composer, arranger, lyricist, writer,
                   albumartist, ensemble,
                   conductor, producer, engineer, mixer, remixer,
                   COALESCE(__sqlmodded, 0) AS __sqlmodded
            FROM alib
            ORDER BY rowid
            """,
        )

        columns_to_replace = [
            "artist",
            "composer",
            "arranger",
            "lyricist",
            "writer",
            "albumartist",
            "ensemble",
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
        if master_conn is not conn:
            master_conn.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
