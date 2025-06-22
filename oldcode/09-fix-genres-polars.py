# --- imports ---
import polars as pl
import sqlite3
import logging
from typing import Set, List, Optional, Tuple, Dict
from string_grouper import match_strings
from datetime import datetime, timezone
import pandas as pd
from functools import lru_cache
import re

# --- constants and config ---
DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres'
DELIMITER = '\\\\'

# --- hard-coded replacement mapping ---
HARD_CODED_REPLACEMENTS = {
    'acoustic': 'Singer/Songwriter',
    'acoustic pop': 'Pop/Rock\\\\Singer/Songwriter',
    'alternative': 'Adult Alternative Pop/Rock',
    'alternative & indie': 'Alternative/Indie Rock',
    'alternative / indie rock / pop / rock': 'Alternative/Indie Rock\\\\Pop/Rock',
    'alternative rock': 'Alternative/Indie Rock',
    'blues/country/folk': 'Blues\\\\Country\\\\Folk',
    'folk/americana': 'Folk\\\\Americana',
    'indie': 'Indie Rock',
    'jazz, blues': 'Jazz Blues',
    'jazz, rock': 'Jazz-Rock',
    'jazz vocal': 'Vocal Jazz',
    'metal': 'Heavy Metal',
    'pop-folk': 'Pop/Rock\\\\Folk',
    'pop, rock': 'Pop/Rock',
    'pop, singer & songwriter': 'Pop/Rock\\\\Singer/Songwriter',
    'rock / blues': 'Blues-Rock',
    'rock blues': 'Blues-Rock',
    'rock': 'Pop/Rock',
    'singer / songwriter': 'Singer/Songwriter',
    'songwriter': 'Singer/Songwriter',
    'songwriting': 'Singer/Songwriter',
    'soundtrack': 'Soundtracks',
    'south african': 'South African Pop/Rock',
    'world': 'International',
    'world music': 'International'
}

# --- logging setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# --- optimized normalization with caching ---
@lru_cache(maxsize=2048)
def normalize_before_match(s: str) -> str:
    """Cached normalization for repeated strings - PERFORMANCE OPTIMIZATION."""
    return (
        s.lower()
        .replace('&', '/')
        .replace('_', ' ')
        .replace('  ', ' ')
        .strip()
    )

# --- vectorized hard-coded replacements ---
def apply_hard_coded_replacements_vectorized(series: pl.Series) -> pl.Series:
    """Apply hard-coded replacements using vectorized string operations."""
    result = series
    for original, replacement in HARD_CODED_REPLACEMENTS.items():
        # Case-insensitive replacement for whole string matches
        result = result.str.replace_all(
            f"(?i)^{re.escape(original)}$",
            replacement,
            literal=False
        )
    return result

# --- optimized split and trim with hard-coded replacements ---
def split_and_trim(series: pl.Series) -> pl.Series:
    """Split delimited strings, apply hard-coded replacements, and trim whitespace."""
    return (
        series
        # First apply hard-coded replacements on full strings
        .pipe(apply_hard_coded_replacements_vectorized)
        .str.replace_all(r"\s*[,;|]\s*", DELIMITER)
        .str.split(DELIMITER)
        .list.eval(
            pl.when(pl.element().str.strip_chars().str.len_chars() > 0)
            .then(pl.element().str.strip_chars())
        )
        .list.drop_nulls()
        # Then apply hard-coded replacements on individual list items
        .list.eval(
            pl.element().pipe(apply_hard_coded_replacements_vectorized)
        )
    )

# --- optimized corrected mapping - ORIGINAL LOGIC WITH PERFORMANCE TWEAKS ---
def build_corrected_mapping_optimized(
    raw_tags: List[str],
    valid_tags: Set[str],
    similarity_threshold: float = 0.95
) -> Dict[str, Optional[str]]:
    """
    Build mapping from raw tags to valid tags using exact matching first,
    then fuzzy matching for remaining tags.
    ORIGINAL LOGIC: Returns ALL mappings including identity mappings.
    """
    # PERFORMANCE: Create normalized versions and quick lookup
    normalized_valid = {normalize_before_match(v): v for v in valid_tags}
    unique_tags = list(set(raw_tags))

    # First pass - exact matches - ORIGINAL LOGIC
    exact_matches = {
        tag: normalized_valid.get(normalize_before_match(tag))
        for tag in unique_tags
    }

    # Second pass - fuzzy match only for non-exact matches - ORIGINAL LOGIC
    fuzzy_candidates = [
        tag for tag in unique_tags
        if exact_matches[tag] is None
    ]

    if not fuzzy_candidates:
        return exact_matches

    # PERFORMANCE: Only perform fuzzy matching on necessary tags
    # PERFORMANCE: Use string dtype for pandas (faster than object)
    fuzzy_matches = match_strings(
        pd.Series(fuzzy_candidates, dtype="string"),
        pd.Series(list(valid_tags), dtype="string"),
        min_similarity=similarity_threshold
    )

    # Update mapping with fuzzy matches - ORIGINAL LOGIC
    result = exact_matches.copy()
    norm_to_original = {normalize_before_match(tag): tag for tag in fuzzy_candidates}

    for _, row in fuzzy_matches.iterrows():
        norm_input = normalize_before_match(row["left_side"])
        replacement = row["right_side"]
        original = norm_to_original.get(norm_input)
        if original and replacement:
            result[original] = replacement

    return result

# --- database utilities - ORIGINAL LOGIC WITH PERFORMANCE TWEAKS ---
def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the SQLite database."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    """Convert SQLite query results to Polars DataFrame with proper type handling."""
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if id_column and col_name == id_column:
            data[col_name] = pl.Series(col_data, dtype=pl.Int64)
        elif col_name == "sqlmodded":
            data[col_name] = pl.Series([int(x) if x is not None else 0 for x in col_data], dtype=pl.Int32)
        else:
            data[col_name] = pl.Series(col_data, dtype=pl.Utf8)

    return pl.DataFrame(data)

def import_genre_style_sqlmodded(conn: sqlite3.Connection) -> pl.DataFrame:
    """Import genre and style data from the database - ORIGINAL QUERY PRESERVED."""
    query = f"""
        SELECT rowid, __path, genre, style, COALESCE(sqlmodded, 0) AS sqlmodded
        FROM {ALIB_TABLE} WHERE genre IS NOT NULL OR style IS NOT NULL
    """
    return sqlite_to_polars(conn, query, id_column="rowid")

def import_valid_tags(conn: sqlite3.Connection) -> Set[str]:
    """Import valid genre tags from the reference table."""
    query = f"SELECT genre_name FROM {REF_VALIDATION_TABLE}"
    try:
        df = sqlite_to_polars(conn, query)
        return set(df['genre_name'].to_list())
    except sqlite3.OperationalError as e:
        logging.error(f"Error importing valid tags from {REF_VALIDATION_TABLE}: {e}")
        return set()

# --- main process - ORIGINAL LOGIC WITH PERFORMANCE OPTIMIZATIONS ---
def main():
    """Main processing function - ORIGINAL LOGIC with performance optimizations."""
    try:
        # PERFORMANCE: Optimize database connection
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
        conn.execute("PRAGMA temp_store = MEMORY")

        cursor = conn.cursor()

        # Create changelog table - ORIGINAL LOGIC
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS changelog (
                rowid INTEGER,
                column TEXT,
                old_value TEXT,
                new_value TEXT,
                timestamp TEXT,
                script TEXT
            )
            """
        )

        logging.info("Importing data...")
        df = import_genre_style_sqlmodded(conn)
        logging.info(f"Imported {df.height} rows.")

        valid_tags = import_valid_tags(conn)
        logging.info(f"Imported {len(valid_tags)} valid tags.")

        logging.info("Building consolidated list of potential changes...")

        # ORIGINAL TAG EXTRACTION LOGIC - with lazy evaluation for performance
        genre_tags = (
            df
            .lazy()
            .select(split_and_trim(pl.col("genre")).alias("genre_split"))
            .select(pl.col("genre_split").list.explode())
            .filter(pl.col("genre_split").str.len_chars() > 0)
            .collect()
            .get_column("genre_split")
            .unique()
            .to_list()
        )

        style_tags = (
            df
            .lazy()
            .select(split_and_trim(pl.col("style")).alias("style_split"))
            .select(pl.col("style_split").list.explode())
            .filter(pl.col("style_split").str.len_chars() > 0)
            .collect()
            .get_column("style_split")
            .unique()
            .to_list()
        )

        # ORIGINAL LOGIC: Build correction mapping with ALL mappings (including identity)
        alib_changes = build_corrected_mapping_optimized(genre_tags + style_tags, valid_tags)
        logging.info(f"Built correction mapping for {len(alib_changes)} unique tags.")

        logging.info("Processing and applying changes...")
        timestamp = datetime.now(timezone.utc).isoformat()

        # ORIGINAL LOGIC: Process both columns - split and prepare for validation
        df = df.with_columns([
            split_and_trim(pl.col("genre")).alias("genre_split"),
            split_and_trim(pl.col("style")).alias("style_split")
        ])

        # PERFORMANCE: Create mapping function that preserves None values
        def apply_mapping(tag: str) -> Optional[str]:
            """Apply tag correction mapping - preserves None values for NULL database writes."""
            return alib_changes.get(tag, tag)

        # Apply mappings with proper None handling - filter None values from lists
        df = df.with_columns([
            pl.col("genre_split")
            .list.eval(
                pl.element().map_elements(apply_mapping, return_dtype=pl.Utf8)
            )
            .list.drop_nulls()  # Remove None/null values from the list
            .alias("validated_genre"),

            pl.col("style_split")
            .list.eval(
                pl.element().map_elements(apply_mapping, return_dtype=pl.Utf8)
            )
            .list.drop_nulls()  # Remove None/null values from the list
            .alias("validated_style")
        ])

        # MODIFIED LOGIC: Handle empty lists by setting to None for NULL database writes
        df = df.with_columns([
            pl.when(pl.col("validated_genre").list.len() == 0)
            .then(None)
            .otherwise(pl.col("validated_genre").list.join(DELIMITER))
            .alias("new_genre"),

            pl.when(pl.col("validated_style").list.len() == 0)
            .then(None)
            .otherwise(pl.col("validated_style").list.join(DELIMITER))
            .alias("new_style")
        ])

        # ORIGINAL LOGIC: Identify rows that need updating
        changed_mask = (
            (pl.col("new_genre") != pl.col("genre").fill_null("")) |
            (pl.col("new_style") != pl.col("style").fill_null(""))
        )

        changed_df = df.filter(changed_mask)
        updated_row_ids = changed_df.get_column("rowid").to_list()

        if not updated_row_ids:
            logging.info("No updates required.")
            return

        logging.info(f"Found {len(updated_row_ids)} rows requiring updates.")

        # ORIGINAL LOGIC: Generate changelog entries
        changelog_entries = []

        # Genre changes
        genre_changes = changed_df.filter(
            pl.col("new_genre") != pl.col("genre").fill_null("")
        ).select([
            pl.col("rowid"),
            pl.lit("genre").alias("column"),
            pl.col("genre").alias("old_value"),
            pl.col("new_genre").alias("new_value"),
            pl.lit(timestamp).alias("timestamp"),
            pl.lit("dropgenres-polars(string-grouper)").alias("script")
        ])

        # Style changes
        style_changes = changed_df.filter(
            pl.col("new_style") != pl.col("style").fill_null("")
        ).select([
            pl.col("rowid"),
            pl.lit("style").alias("column"),
            pl.col("style").alias("old_value"),
            pl.col("new_style").alias("new_value"),
            pl.lit(timestamp).alias("timestamp"),
            pl.lit("dropgenres-polars(string-grouper)").alias("script")
        ])

        # Combine changelog entries - ORIGINAL LOGIC
        if genre_changes.height > 0:
            changelog_entries.extend(genre_changes.to_dicts())
        if style_changes.height > 0:
            changelog_entries.extend(style_changes.to_dicts())

        # ORIGINAL LOGIC: Update the main dataframe with validated values
        df = df.with_columns([
            pl.when(changed_mask)
            .then(pl.col("new_genre"))
            .otherwise(pl.col("genre"))
            .alias("genre"),

            pl.when(changed_mask)
            .then(pl.col("new_style"))
            .otherwise(pl.col("style"))
            .alias("style"),

            pl.when(pl.col("rowid").is_in(updated_row_ids))
            .then(pl.col("sqlmodded") + 1)
            .otherwise(pl.col("sqlmodded"))
            .alias("sqlmodded")
        ])

        # ORIGINAL LOGIC: Prepare update data for database
        update_data = (
            df.filter(pl.col("rowid").is_in(updated_row_ids))
            .select(["rowid", "genre", "style", "sqlmodded"])
            .to_dicts()
        )

        # PERFORMANCE: Execute database updates in batch with optimized SQL
        logging.info(f"Updating {len(update_data)} rows in database...")
        cursor.executemany(
            f"UPDATE {ALIB_TABLE} SET genre = :genre, style = :style, sqlmodded = :sqlmodded WHERE rowid = :rowid",
            update_data
        )

        # ORIGINAL LOGIC: Insert changelog entries
        if changelog_entries:
            logging.info(f"Inserting {len(changelog_entries)} changelog entries...")
            cursor.executemany(
                "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (:rowid, :column, :old_value, :new_value, :timestamp, :script)",
                changelog_entries
            )

        # PERFORMANCE: Single commit for all changes
        conn.commit()
        logging.info(f"Successfully committed {len(changelog_entries)} changes to database.")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
