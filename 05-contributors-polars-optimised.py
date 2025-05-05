import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any, Union
import logging
from datetime import datetime, timezone
import re

# ---------- Config ----------
SCRIPT_NAME = "contributors-polars.py"
DB_PATH = '/tmp/amg/dbtemplate.db'

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Helpers ----------
SPLIT_PATTERN = re.compile(r'(?:\\\\|;|/|,(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b))')

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: Union[str, Tuple[str, ...]] = None) -> pl.DataFrame:
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if col_name in ("rowid", "sqlmodded"):
            data[col_name] = pl.Series(
                name=col_name,
                values=[int(x or 0) for x in col_data],
                dtype=pl.Int64
            )
        else:
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )

    return pl.DataFrame(data)


DELIMITER = '\\\\'  # <- double backslash for splitting and joining

#------------------------------------

def smart_title(text):
    """
    A more direct approach to smart title that uses regex to capture words and
    applies capitalization rules directly, preserving non-word parts and correctly handling possessives.
    """
    if not text:
        return text

    def fix_caps_word(word, is_first_word=False, follows_bracket=False):
        lower_words = ["of", "a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by"]

        if is_first_word:
            if any(c.isupper() for c in word):
                return word
            else:
                return word.capitalize()
        elif follows_bracket:
            return word.capitalize()
        elif any(c.isupper() for c in word):
            return word
        elif re.match(r"^[IVXLCDM]+$", word.upper()):
            return word.upper()
        elif "." in word:
            parts = word.split('.')
            return '.'.join(part.capitalize() for part in parts)
        elif "'" in word or "’" in word:
            apos_pos = max(word.find("'"), word.find("’"))
            if 0 < apos_pos < len(word) - 1:
                return word[:apos_pos].capitalize() + word[apos_pos:]
            else:
                return word.capitalize()
        elif "-" in word:
            parts = word.split('-')
            return '-'.join(part.capitalize() for part in parts)
        elif word.lower() in lower_words:
            return word
        else:
            return word.capitalize()



    # Regex to capture words (including McNames, O'Names, possessives)
    word_pattern = r"\b(?:Mc\w+|O'\w+|\w+(?:['’]\w+)?)\b"
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
                elif processed_word.lower().endswith("’s"):
                    processed_word = processed_word[:-2] + "’s"
                # Special rule for "O'" (applied directly)
                elif word.lower().startswith("o'") and len(word) > 2 and word[2].lower() != 's' and word[2] != ' ':
                    processed_word = "O'" + fix_caps_word(word[2:], is_first_word=False, follows_bracket=False)
                elif word.lower().startswith("o’") and len(word) > 2 and word[2].lower() != 's' and word[2] != ' ':
                    processed_word = "O’" + fix_caps_word(word[2:], is_first_word=False, follows_bracket=False)
                result.append(processed_word)
                capitalize_next = False
            else:  # It's a non-word part
                result.append(word)
                capitalize_next = word in "({[<"
        else:
            result.append(" ")  # It's whitespace

    processed_text = "".join(result)
    # Final pass to ensure possessive 's is lowercase
    processed_text = re.sub(r"(\w)['’]S\b", r"\1's", processed_text)

    return processed_text

#------------------------------------

# def normalize_contributor_entry(x: Union[str, None], contributors_dict: Dict[str, str]) -> Union[str, None]:
#     if x is None:
#         return None

#     if DELIMITER in x:
#         items = x.split(DELIMITER)
#         normalized_items = []
#         for item in items:
#             stripped_item = item.strip()
#             lowered_item = stripped_item.lower()
#             if lowered_item in contributors_dict:
#                 normalized_items.append(contributors_dict[lowered_item])
#             else:
#                 parts = SPLIT_PATTERN.split(stripped_item)
#                 normalized_parts = []
#                 seen = set()
#                 for part in parts:
#                     stripped = part.strip()
#                     lowered = stripped.lower()
#                     normalized = contributors_dict.get(lowered, smart_title(stripped))
#                     if normalized not in seen:
#                         normalized_parts.append(normalized)
#                         seen.add(normalized)
#                 normalized_items.append(DELIMITER.join(normalized_parts) if normalized_parts else stripped_item) # changed from None to stripped_item
#         return DELIMITER.join(normalized_items)
#     else:
#         lowered_x = x.lower()
#         if lowered_x in contributors_dict:
#             return contributors_dict[lowered_x]  # Use standardized name directly

#         parts = SPLIT_PATTERN.split(x)
#         normalized_parts = []
#         seen = set()

#         for part in parts:
#             stripped = part.strip()
#             lowered = stripped.lower()
#             normalized = contributors_dict.get(lowered, smart_title(stripped))

#             if normalized not in seen:
#                 normalized_parts.append(normalized)
#                 seen.add(normalized)

#         return DELIMITER.join(normalized_parts) if normalized_parts else None


def normalize_contributor_entry(x: Union[str, None], contributors_dict: Dict[str, str]) -> Union[str, None]:
    if x is None:
        return None

    if DELIMITER in x:
        items = x.split(DELIMITER)
        normalized_items = []
        for item in items:
            stripped_item = item.strip()
            lowered_item = stripped_item.lower()
            if lowered_item in contributors_dict:
                normalized_items.append(contributors_dict[lowered_item])
            else:
                parts = SPLIT_PATTERN.split(stripped_item)
                for part in parts:  # Removed inner deduplication logic
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
        lowered_x = x.lower()
        if lowered_x in contributors_dict:
            return contributors_dict[lowered_x]

        parts = SPLIT_PATTERN.split(x)
        normalized_parts = []
        seen = set()  # This 'seen' is still necessary for single-name deduplication

        for part in parts:
            stripped = part.strip()
            lowered = stripped.lower()
            normalized = contributors_dict.get(lowered, smart_title(stripped))

            if normalized not in seen:
                normalized_parts.append(normalized)
                seen.add(normalized)

        return DELIMITER.join(normalized_parts) if normalized_parts else None

def batch_normalize_contributors(df: pl.DataFrame, columns: List[str], contributors_dict: Dict[str, str]) -> pl.DataFrame:
    expressions = []
    for column in columns:
        expr = pl.col(column).map_elements(
            lambda x: normalize_contributor_entry(x, contributors_dict),
            return_dtype=pl.Utf8
        ).alias(column)
        expressions.append(expr)
    return df.with_columns(expressions)


def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int],
    columns_to_update: List[str]
) -> int:
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    timestamp = datetime.now(timezone.utc).isoformat()
    updated_count = 0

    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)

        changed_cols = [
            col for col in columns_to_update
            if record[col] != original_row[col] and record[col] is not None
        ]

        if not changed_cols:
            continue

        new_sqlmodded = int(original_row["sqlmodded"] or 0) + len(changed_cols)
        set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
        values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

        cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

        for col in changed_cols:
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, col, original_row[col], record[col], timestamp, SCRIPT_NAME)
            )

        updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


# ---------- Main ----------
def main():
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Fetch contributors
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

        # Fetch tracks
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

        # Filter to rows with contributor data
        filter_expr = pl.any_horizontal([pl.col(col).is_not_null() for col in columns_to_replace])
        tracks_filtered = tracks.filter(filter_expr)
        logging.info(f"Processing {tracks_filtered.height} tracks with contributor fields...")

        # Normalize contributors in batch
        updated_tracks = batch_normalize_contributors(tracks_filtered, columns_to_replace, contributors_dict)

        # Detect changes
        change_expr = pl.any_horizontal([
            (tracks_filtered[col].is_not_null()) & (tracks_filtered[col] != updated_tracks[col])
            for col in columns_to_replace
        ])

        changed_rowids = updated_tracks.filter(change_expr)["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks,
                original_df=tracks_filtered,
                changed_rowids=changed_rowids,
                columns_to_update=columns_to_replace
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
