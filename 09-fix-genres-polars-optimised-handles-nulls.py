# --- imports ---
import argparse
import polars as pl
import sqlite3
import logging
from typing import Set, List, Optional, Dict, Iterator
from string_grouper import match_strings
from datetime import datetime, timezone
import pandas as pd
import os
import pickle
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import re
from functools import lru_cache

# --- constants and config ---
DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres'
DELIMITER = '\\\\'
CHUNK_SIZE = 100000  # Larger chunks for your system
BATCH_SIZE = 10000   # Larger batch operations
CACHE_DIR = '/tmp/amg_cache'
SIMILARITY_THRESHOLD = 0.95
NUM_CORES = 12

# Be explicit about schema to avoid Polars inferencing
ALIB_SCHEMA = {
    'rowid': pl.Int64,
    '__path': pl.Utf8,
    'genre': pl.Utf8,
    'style': pl.Utf8,
    'sqlmodded': pl.Int16  # Unlikely to ever even get to 999
}

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
    'singer & songwriter': 'Singer/Songwriter',
    'singer and songwriter': 'Singer/Songwriter',
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
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# --- cache management ---
def ensure_cache_dir():
    """Ensure cache directory exists."""
    os.makedirs(CACHE_DIR, exist_ok=True)

def get_cache_key(tags: List[str], valid_tags: Set[str]) -> str:
    """Generate cache key for fuzzy matching results."""
    content = f"{sorted(tags)}_{sorted(valid_tags)}"
    return hashlib.md5(content.encode()).hexdigest()

def load_cached_mapping(cache_key: str) -> Optional[Dict[str, Optional[str]]]:
    """Load cached mapping if it exists."""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.pkl")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logging.warning(f"Failed to load cache: {e}")
    return None

def save_cached_mapping(cache_key: str, mapping: Dict[str, Optional[str]]):
    """Save mapping to cache."""
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.pkl")
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(mapping, f)
    except Exception as e:
        logging.warning(f"Failed to save cache: {e}")

# --- optimized normalization with caching ---
@lru_cache(maxsize=8192)
def normalize_before_match(s: str) -> str:
    """Cached normalization for repeated strings."""
    return (
        s.lower()
        .replace('&', '/')
        .replace('_', ' ')
        .replace('  ', ' ')
        .strip()
    )

# --- pre-compiled regex patterns ---
DELIMITER_PATTERN = re.compile(r"\s*[,;|]\s*")

# --- OPTIMIZED: much smarter pre-filtering to minimize string_grouper workload ---
def intelligent_pre_filter(raw_tags: List[str], valid_tags: Set[str]) -> tuple[List[str], Dict[str, str]]:
    """
    Intelligent pre-filtering to dramatically reduce string_grouper workload.
    This is the key optimization - we want to send as few tags as possible to string_grouper.
    """
    exact_matches = {}
    fuzzy_candidates = []

    # Create multiple lookup strategies
    valid_tags_lower = {tag.lower(): tag for tag in valid_tags}
    valid_tags_normalized = {normalize_before_match(tag): tag for tag in valid_tags}

    # Apply hard-coded replacements first
    hard_coded_lower = {k.lower(): v for k, v in HARD_CODED_REPLACEMENTS.items()}

    processed_tags = set()

    for tag in raw_tags:
        if tag in processed_tags:
            continue
        processed_tags.add(tag)

        tag_lower = tag.lower()
        tag_normalized = normalize_before_match(tag)

        # 1. Check hard-coded replacements first
        if tag_lower in hard_coded_lower:
            exact_matches[tag] = hard_coded_lower[tag_lower]
        # 2. Exact case-insensitive match
        elif tag_lower in valid_tags_lower:
            exact_matches[tag] = valid_tags_lower[tag_lower]
        # 3. Normalized match
        elif tag_normalized in valid_tags_normalized:
            exact_matches[tag] = valid_tags_normalized[tag_normalized]
        # 4. Try some common variations before fuzzy matching
        elif f"{tag_lower} music" in valid_tags_lower:
            exact_matches[tag] = valid_tags_lower[f"{tag_lower} music"]
        elif tag_lower.replace("music", "").strip() in valid_tags_lower:
            exact_matches[tag] = valid_tags_lower[tag_lower.replace("music", "").strip()]
        # 5. Only send to fuzzy matching if no exact match found
        else:
            fuzzy_candidates.append(tag)

    logging.info(f"Intelligent filtering: {len(exact_matches)} exact matches, {len(fuzzy_candidates)} fuzzy candidates (reduced from {len(set(raw_tags))} total)")

    return fuzzy_candidates, exact_matches


def optimized_string_grouper_matching(
    fuzzy_candidates: List[str],
    valid_tags: Set[str],
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    batch_size: int = 5000  # Process in smaller batches to manage memory
) -> Dict[str, str]:
    """
    Process string_grouper in optimized batches to manage memory and improve performance.
    """
    if not fuzzy_candidates:
        return {}

    all_matches = {}
    valid_tags_series = pd.Series(list(valid_tags), dtype="string")

    # Process in batches to manage memory and potentially parallelize
    for i in range(0, len(fuzzy_candidates), batch_size):
        batch = fuzzy_candidates[i:i + batch_size]
        logging.info(f"Processing string_grouper batch {i//batch_size + 1}/{(len(fuzzy_candidates)-1)//batch_size + 1}")

        try:
            batch_series = pd.Series(batch, dtype="string")

            # Use n_blocks instead of n_jobs
            matches = match_strings(
                batch_series,
                valid_tags_series,
                min_similarity=similarity_threshold,
                max_n_matches=1,  # Only need best match
                ignore_index=True,  # Faster processing
                # n_blocks=NUM_CORES  # Changed from n_jobs to n_blocks
                #n_jobs=min(NUM_CORES, len(batch))
                n_jobs=min(NUM_CORES, len(batch))
            )

            # Process results
            if not matches.empty:
                for _, row in matches.iterrows():
                    all_matches[row["left_side"]] = row["right_side"]

        except Exception as e:
            logging.error(f"Error in string_grouper batch processing: {e}")
            # Continue with other batches
            continue

    return all_matches

# --- OPTIMIZED: build corrected mapping with better caching and batching ---
def build_corrected_mapping_optimized(
    raw_tags: List[str],
    valid_tags: Set[str],
    similarity_threshold: float = SIMILARITY_THRESHOLD
) -> Dict[str, Optional[str]]:
    """
    Build mapping with optimized string_grouper usage and comprehensive caching.
    """
    ensure_cache_dir()

    # Check cache first
    cache_key = get_cache_key(raw_tags, valid_tags)
    cached_result = load_cached_mapping(cache_key)
    if cached_result is not None:
        logging.info("Using cached fuzzy matching results")
        return cached_result

    # Intelligent pre-filtering (this is the key optimization)
    fuzzy_candidates, exact_matches = intelligent_pre_filter(raw_tags, valid_tags)

    # Only run string_grouper on the minimal set of candidates
    fuzzy_matches = {}
    if fuzzy_candidates:
        logging.info(f"Running optimized string_grouper on {len(fuzzy_candidates)} candidates...")
        fuzzy_matches = optimized_string_grouper_matching(fuzzy_candidates, valid_tags, similarity_threshold)
        logging.info(f"String_grouper found {len(fuzzy_matches)} matches")

    # Build comprehensive mapping
    result = exact_matches.copy()
    result.update(fuzzy_matches)

    # Add identity mappings for unmatched tags
    for tag in raw_tags:
        if tag not in result:
            result[tag] = tag  # Keep original if no match found

    # Cache the result
    save_cached_mapping(cache_key, result)
    return result

# --- OPTIMIZED: single-pass tag collection using SQL aggregation ---
def collect_all_tags_optimized(conn: sqlite3.Connection) -> List[str]:
    """
    Collect all unique tags using SQL to minimize data transfer and processing.
    This is much faster than processing in Python.
    """

    # Use SQL to do the heavy lifting - much faster than Python processing
    query = f"""
    WITH RECURSIVE split_tags AS (
        -- Split genres
        SELECT DISTINCT
            TRIM(
                CASE
                    WHEN genre LIKE '%{DELIMITER}%' THEN
                        SUBSTR(genre, 1, INSTR(genre, '{DELIMITER}') - 1)
                    ELSE genre
                END
            ) as tag
        FROM {ALIB_TABLE}
        WHERE genre IS NOT NULL AND genre != ''

        UNION ALL

        SELECT DISTINCT
            TRIM(
                CASE
                    WHEN SUBSTR(genre, INSTR(genre, '{DELIMITER}') + {len(DELIMITER)}) LIKE '%{DELIMITER}%' THEN
                        SUBSTR(SUBSTR(genre, INSTR(genre, '{DELIMITER}') + {len(DELIMITER)}), 1,
                               INSTR(SUBSTR(genre, INSTR(genre, '{DELIMITER}') + {len(DELIMITER)}), '{DELIMITER}') - 1)
                    ELSE SUBSTR(genre, INSTR(genre, '{DELIMITER}') + {len(DELIMITER)})
                END
            ) as tag
        FROM {ALIB_TABLE}
        WHERE genre IS NOT NULL
        AND genre LIKE '%{DELIMITER}%'
        AND INSTR(genre, '{DELIMITER}') > 0
        AND SUBSTR(genre, INSTR(genre, '{DELIMITER}') + {len(DELIMITER)}) != ''

        -- Split styles
        UNION ALL

        SELECT DISTINCT
            TRIM(
                CASE
                    WHEN style LIKE '%{DELIMITER}%' THEN
                        SUBSTR(style, 1, INSTR(style, '{DELIMITER}') - 1)
                    ELSE style
                END
            ) as tag
        FROM {ALIB_TABLE}
        WHERE style IS NOT NULL AND style != ''

        UNION ALL

        SELECT DISTINCT
            TRIM(
                CASE
                    WHEN SUBSTR(style, INSTR(style, '{DELIMITER}') + {len(DELIMITER)}) LIKE '%{DELIMITER}%' THEN
                        SUBSTR(SUBSTR(style, INSTR(style, '{DELIMITER}') + {len(DELIMITER)}), 1,
                               INSTR(SUBSTR(style, INSTR(style, '{DELIMITER}') + {len(DELIMITER)}), '{DELIMITER}') - 1)
                    ELSE SUBSTR(style, INSTR(style, '{DELIMITER}') + {len(DELIMITER)})
                END
            ) as tag
        FROM {ALIB_TABLE}
        WHERE style IS NOT NULL
        AND style LIKE '%{DELIMITER}%'
        AND INSTR(style, '{DELIMITER}') > 0
        AND SUBSTR(style, INSTR(style, '{DELIMITER}') + {len(DELIMITER)}) != ''
    )
    SELECT DISTINCT tag
    FROM split_tags
    WHERE tag IS NOT NULL
    AND TRIM(tag) != ''
    ORDER BY tag
    """

    try:
        cursor = conn.execute(query)
        tags = [row[0] for row in cursor.fetchall()]
        logging.info(f"SQL-based tag collection found {len(tags)} unique tags")
        return tags
    except Exception as e:
        logging.warning(f"SQL tag collection failed, falling back to Python method: {e}")
        # Fallback to Python-based collection
        return collect_tags_python_fallback(conn)

def collect_tags_python_fallback(conn: sqlite3.Connection) -> List[str]:
    """Fallback Python-based tag collection if SQL method fails."""
    query = f"SELECT genre, style FROM {ALIB_TABLE} WHERE genre IS NOT NULL OR style IS NOT NULL"

    # df = pl.read_database(query, conn)
    df = pl.read_database(query, conn, schema_overrides={'genre': pl.Utf8, 'style': pl.Utf8})

    all_tags = set()

    # Process genres
    if "genre" in df.columns:
        genre_tags = (
            df.lazy()
            .select(pl.col("genre").fill_null(""))
            .filter(pl.col("genre") != "")
            .select(pl.col("genre").str.split(DELIMITER))
            .select(pl.col("genre").list.explode())
            .select(pl.col("genre").str.strip_chars())
            .filter(pl.col("genre") != "")
            .collect()
            .get_column("genre")
            .unique()
            .to_list()
        )
        all_tags.update(genre_tags)

    # Process styles
    if "style" in df.columns:
        style_tags = (
            df.lazy()
            .select(pl.col("style").fill_null(""))
            .filter(pl.col("style") != "")
            .select(pl.col("style").str.split(DELIMITER))
            .select(pl.col("style").list.explode())
            .select(pl.col("style").str.strip_chars())
            .filter(pl.col("style") != "")
            .collect()
            .get_column("style")
            .unique()
            .to_list()
        )
        all_tags.update(style_tags)

    return list(all_tags)

def create_changelog_entries_vectorized(df: pl.DataFrame) -> tuple[pl.DataFrame, list]:
    """
    Vectorized approach to identify changes and prepare changelog entries.
    Properly handles null comparisons.
    """
    changelog_entries = []
    timestamp = datetime.now(timezone.utc).isoformat()

    # Create change detection columns with proper null handling
    changes_df = (
        df.lazy()
        .with_columns([
            # Normalize nulls to empty strings for comparison
            pl.col("genre").fill_null("").alias("genre_norm"),
            pl.col("style").fill_null("").alias("style_norm"),
            pl.col("new_genre").fill_null("").alias("new_genre_norm"),
            pl.col("new_style").fill_null("").alias("new_style_norm")
        ])
        .with_columns([
            # Detect actual changes
            (pl.col("new_genre_norm") != pl.col("genre_norm")).alias("genre_changed"),
            (pl.col("new_style_norm") != pl.col("style_norm")).alias("style_changed")
        ])
        .with_columns([
            # Overall change flag
            (pl.col("genre_changed") | pl.col("style_changed")).alias("has_changes")
        ])
        .filter(pl.col("has_changes"))  # Only keep rows with actual changes
        .collect()
    )

    # Generate changelog entries for changed rows
    for row in changes_df.iter_rows(named=True):
        if row['genre_changed']:
            changelog_entries.append({
                'alib_rowid': row['rowid'],
                'column': 'genre',
                'old_value': row['genre'],  # Original value (may be None)
                'new_value': row['new_genre'],  # New value (may be None)
                'timestamp': timestamp,
                'script': 'optimized-string-grouper-polars'
            })

        if row['style_changed']:
            changelog_entries.append({
                'alib_rowid': row['rowid'],
                'column': 'style',
                'old_value': row['style'],  # Original value (may be None)
                'new_value': row['new_style'],  # New value (may be None)
                'timestamp': timestamp,
                'script': 'optimized-string-grouper-polars'
            })

    return changes_df, changelog_entries

# --- OPTIMIZED: vectorized tag processing with pre-built mapping ---
# Modified section of process_tags_vectorized function
def process_tags_vectorized(
    df: pl.DataFrame,
    tag_mapping: Dict[str, str]
) -> pl.DataFrame:
    """
    Process genre/style tags using vectorized operations with pre-built mapping.
    Modified to preserve styles separately instead of merging everything into genre.
    """

    def split_and_clean_tags(series: pl.Series) -> pl.Series:
        """Split, clean, and normalize tags in vectorized fashion."""
        return (
            series
            .cast(pl.Utf8)
            .fill_null("")
            .str.replace_all(DELIMITER_PATTERN.pattern, DELIMITER, literal=False)
            .str.split(DELIMITER)
            .list.eval(
                pl.when(pl.element().str.strip_chars().str.len_chars() > 0)
                .then(pl.element().str.strip_chars())
                .otherwise(None)
            )
            .list.drop_nulls()
        )

    def map_tag_list(tag_list):
        """Map a list of tags using the mapping dictionary."""
        if tag_list is None:
            return []

        mapped = []
        for tag in tag_list:
            mapped_tag = tag_mapping.get(tag, tag)  # Use original if not in mapping
            if mapped_tag and mapped_tag.strip():  # Only add non-empty mappings
                mapped.append(mapped_tag)

        # Remove duplicates while preserving order
        seen = set()
        result = []
        for item in mapped:
            if item not in seen:
                seen.add(item)
                result.append(item)

        return result

    # Process both genre and style columns
    result = (
        df.lazy()
        .with_columns([
            split_and_clean_tags(pl.col("genre")).alias("genre_tags"),
            split_and_clean_tags(pl.col("style")).alias("style_tags")
        ])
        .with_columns([
            # Map genre tags
            pl.col("genre_tags").map_elements(map_tag_list, return_dtype=pl.List(pl.String)).alias("mapped_genre"),
            # Map style tags
            pl.col("style_tags").map_elements(map_tag_list, return_dtype=pl.List(pl.String)).alias("mapped_style")
        ])
        .with_columns([
            # Create separate cleaned genre and style fields
            # pl.when(pl.col("mapped_genre").list.len() == 0)
            # .then(None)
            # .otherwise(pl.col("mapped_genre").list.join(DELIMITER))
            # .alias("new_genre"),

            # pl.when(pl.col("mapped_style").list.len() == 0)
            # .then(None)
            # .otherwise(pl.col("mapped_style").list.join(DELIMITER))
            # .alias("new_style")

            pl.when(pl.col("mapped_genre").list.len() == 0)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("mapped_genre").list.join(DELIMITER))
            .alias("new_genre"),

            pl.when(pl.col("mapped_style").list.len() == 0)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("mapped_style").list.join(DELIMITER))
            .alias("new_style")
        ])
        .collect()
    )

    return result

def merge_genre_style_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """
    Efficiently merge and deduplicate genre and style tags into genre field.
    Style field remains unchanged from its cleaned state.
    """
    # First ensure our input columns are properly typed
    df = df.with_columns([
        pl.col("new_genre").cast(pl.Utf8),
        pl.col("new_style").cast(pl.Utf8)
    ])

    # Create intermediate columns with proper list types
    df = df.with_columns([
        # Convert genre to list (handle nulls)
        pl.when(pl.col("new_genre").is_null() | (pl.col("new_genre").str.len_chars() == 0))
        .then(pl.lit([], dtype=pl.List(pl.Utf8)))
        .otherwise(pl.col("new_genre").str.split(DELIMITER))
        .alias("genre_list"),

        # Convert style to list (handle nulls)
        pl.when(pl.col("new_style").is_null() | (pl.col("new_style").str.len_chars() == 0))
        .then(pl.lit([], dtype=pl.List(pl.Utf8)))
        .otherwise(pl.col("new_style").str.split(DELIMITER))
        .alias("style_list")
    ])

    # Clean and merge the lists
    df = df.with_columns([
        # Clean genre list
        pl.col("genre_list").list.eval(
            pl.when(pl.element().str.len_chars() > 0)
            .then(pl.element())
            .otherwise(None)
        ).list.drop_nulls().alias("genre_clean"),

        # Clean style list
        pl.col("style_list").list.eval(
            pl.when(pl.element().str.len_chars() > 0)
            .then(pl.element())
            .otherwise(None)
        ).list.drop_nulls().alias("style_clean")
    ])

    # Merge and deduplicate
    df = df.with_columns([
        # Combine lists, deduplicate while preserving order
        pl.concat_list(["genre_clean", "style_clean"])
        .list.eval(pl.element().drop_nulls())
        .list.unique(maintain_order=True)
        .alias("merged_tags")
    ])

    # Convert back to delimited string
    df = df.with_columns([
        # Handle empty lists
        pl.when(pl.col("merged_tags").list.len() == 0)
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("merged_tags").list.join(DELIMITER))
        .alias("new_genre")
    ])

    # Drop intermediate columns
    return df.drop(["genre_list", "style_list", "genre_clean", "style_clean", "merged_tags"])

# --- database utilities ---
def optimize_sqlite_connection(conn: sqlite3.Connection):
    """Apply SQLite optimizations for your 64GB system."""
    optimizations = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -2097152",  # 2GB cache for your system
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 8589934592",  # 8GB memory map
        "PRAGMA page_size = 4096",
        "PRAGMA wal_autocheckpoint = 10000",
        "PRAGMA optimize"
    ]

    for pragma in optimizations:
        try:
            conn.execute(pragma)
        except sqlite3.Error as e:
            logging.warning(f"Failed to apply {pragma}: {e}")

def import_valid_tags(conn: sqlite3.Connection) -> Set[str]:
    """Import valid genre tags from reference table."""
    query = f"SELECT genre_name FROM {REF_VALIDATION_TABLE}"
    try:
        cursor = conn.execute(query)
        return {row[0] for row in cursor.fetchall()}
    except sqlite3.OperationalError as e:
        logging.error(f"Error importing valid tags: {e}")
        return set()

def batch_database_updates(conn: sqlite3.Connection, updates: List[Dict], changelog_entries: List[Dict]) -> bool:
    """Perform batch database updates with optimized prepared statements."""
    cursor = conn.cursor()

    try:
        # Batch update main table with prepared statement
        if updates:
            cursor.executemany(
                f"UPDATE {ALIB_TABLE} SET genre = ?, style = ?, sqlmodded = ? WHERE rowid = ?",
                [(u['genre'], u['style'], u['sqlmodded'], u['rowid']) for u in updates]
            )

        # Batch insert changelog with prepared statement
        if changelog_entries:
            cursor.executemany(
                "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                [(c['alib_rowid'], c['column'], c['old_value'], c['new_value'], c['timestamp'], c['script']) for c in changelog_entries]
            )

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"Database batch update failed: {e}")
        return False


def main():
    """
    Optimized main function that keeps string_grouper but minimizes its workload
    through intelligent pre-filtering and caching.  Includes optional genre-style merging.
    """

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Clean and standardize genre/style tags')
    parser.add_argument('--merge-genres-styles', action='store_true',
                        help='Merge cleaned genre and style tags into genre field (style field preserved)')
    args = parser.parse_args()

    start_time = datetime.now()

    try:
        # Database setup
        conn = sqlite3.connect(DB_PATH)
        optimize_sqlite_connection(conn)

        # Create changelog table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS changelog (
                alib_rowid INTEGER,
                column TEXT,
                old_value TEXT,
                new_value TEXT,
                timestamp TEXT,
                script TEXT
            )
        """)

        logging.info("Loading valid tags...")
        valid_tags = import_valid_tags(conn)
        logging.info(f"Loaded {len(valid_tags)} valid tags")

        logging.info("Collecting all unique tags from database...")
        all_raw_tags = collect_all_tags_optimized(conn)
        logging.info(f"Found {len(all_raw_tags)} unique tags to process")

        logging.info("Building optimized correction mapping with string_grouper...")
        tag_mapping = build_corrected_mapping_optimized(all_raw_tags, valid_tags)
        logging.info(f"Built mapping for {len(tag_mapping)} tags")

        # Process database in chunks
        logging.info("Processing database records...")
        query = f"""
            SELECT rowid, __path, genre, style, COALESCE(sqlmodded, 0) AS sqlmodded
            FROM {ALIB_TABLE}
            WHERE genre IS NOT NULL OR style IS NOT NULL
            ORDER BY rowid
        """

        total_processed = 0
        total_updated = 0
        timestamp = datetime.now(timezone.utc).isoformat()

        update_batch = []
        changelog_batch = []

        def safe_compare(new_val, old_val):
            """Safely compare values, treating None and empty string as equivalent."""
            new_norm = new_val if new_val is not None else ""
            old_norm = old_val if old_val is not None else ""
            return new_norm != old_norm

        def create_changelog_entries_vectorized(df: pl.DataFrame) -> tuple[pl.DataFrame, list]:
            """
            Vectorized approach to identify changes and prepare changelog entries.
            Properly handles null comparisons.
            """
            changelog_entries = []

            # Create change detection columns with proper null handling
            changes_df = (
                df.lazy()
                .with_columns([
                    # Normalize nulls to empty strings for comparison
                    pl.col("genre").fill_null("").alias("genre_norm"),
                    pl.col("style").fill_null("").alias("style_norm"),
                    pl.col("new_genre").fill_null("").alias("new_genre_norm"),
                    pl.col("new_style").fill_null("").alias("new_style_norm")
                ])
                .with_columns([
                    # Detect actual changes
                    (pl.col("new_genre_norm") != pl.col("genre_norm")).alias("genre_changed"),
                    (pl.col("new_style_norm") != pl.col("style_norm")).alias("style_changed")
                ])
                .with_columns([
                    # Overall change flag
                    (pl.col("genre_changed") | pl.col("style_changed")).alias("has_changes")
                ])
                .filter(pl.col("has_changes"))  # Only keep rows with actual changes
                .collect()
            )

            # Generate changelog entries for changed rows
            for row in changes_df.iter_rows(named=True):
                if row['genre_changed']:
                    changelog_entries.append({
                        'alib_rowid': row['rowid'],
                        'column': 'genre',
                        'old_value': row['genre'],  # Original value (may be None)
                        'new_value': row['new_genre'],  # New value (may be None)
                        'timestamp': timestamp,
                        'script': 'optimized-string-grouper-polars'
                    })

                if row['style_changed']:
                    changelog_entries.append({
                        'alib_rowid': row['rowid'],
                        'column': 'style',
                        'old_value': row['style'],  # Original value (may be None)
                        'new_value': row['new_style'],  # New value (may be None)
                        'timestamp': timestamp,
                        'script': 'optimized-string-grouper-polars'
                    })

            return changes_df, changelog_entries

        cursor = conn.cursor()
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break

            # Convert to Polars DataFrame
            # df = pl.DataFrame({
            #     'rowid': [r[0] for r in rows],
            #     '__path': [r[1] for r in rows],
            #     'genre': [r[2] for r in rows],
            #     'style': [r[3] for r in rows],
            #     'sqlmodded': [r[4] for r in rows]
            # })

            df = pl.DataFrame({
                'rowid': pl.Series([r[0] for r in rows], dtype=pl.Int64),
                '__path': pl.Series([r[1] for r in rows], dtype=pl.Utf8),
                'genre': pl.Series([r[2] for r in rows], dtype=pl.Utf8),
                'style': pl.Series([r[3] for r in rows], dtype=pl.Utf8),
                'sqlmodded': pl.Series([int(r[4]) for r in rows], dtype=pl.Int64)
            })

            # Process tags using string_grouper results
            processed_df = process_tags_vectorized(df, tag_mapping)

            # Optionally merge styles into (deduped) genres:
            if args.merge_genres_styles:
                processed_df = merge_genre_style_vectorized(processed_df)

            # Find changes using improved null handling
            changed_df, new_changelog_entries = create_changelog_entries_vectorized(processed_df)

            if changed_df.height > 0:
                # Prepare batch updates
                for row in changed_df.iter_rows(named=True):
                    update_batch.append({
                        'rowid': row['rowid'],
                        'genre': row['new_genre'],
                        'style': row['new_style'],
                        'sqlmodded': row['sqlmodded'] + 1
                    })

                # Add the properly filtered changelog entries
                changelog_batch.extend(new_changelog_entries)

            total_processed += len(rows)

            # Batch commit
            if len(update_batch) >= BATCH_SIZE:
                if batch_database_updates(conn, update_batch, changelog_batch):
                    total_updated += len(update_batch)
                    logging.info(f"Processed {total_processed:,} rows, updated {total_updated:,}")

                update_batch.clear()
                changelog_batch.clear()

        # Final batch
        if update_batch:
            if batch_database_updates(conn, update_batch, changelog_batch):
                total_updated += len(update_batch)

        # Results
        end_time = datetime.now()
        duration = end_time - start_time
        duration_minutes = duration.total_seconds() / 60
        rows_per_second = total_processed / duration.total_seconds() if duration.total_seconds() > 0 else 0

        print(f"\nOptimized Genre Cleanup (with string_grouper) Completed!")
        merge_status = " (with genre-style merge)" if args.merge_genres_styles else ""
        print(f"\nOptimized Genre Cleanup{merge_status} Completed!")
        print(f"Processed: {total_processed:,} rows")
        print(f"Updated: {total_updated:,} rows")
        print(f"Time: {duration_minutes:.2f} minutes")
        print(f"Rate: {rows_per_second:,.1f} rows/sec")


    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
