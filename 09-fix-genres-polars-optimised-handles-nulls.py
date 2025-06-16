# --- imports ---
import polars as pl
import sqlite3
import logging
from typing import Set, List, Optional, Tuple, Dict, Iterator
from string_grouper import match_strings
from datetime import datetime, timezone
import pandas as pd
from functools import lru_cache
import re
import os
import pickle
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# --- constants and config ---
DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres'
DELIMITER = '\\\\'
CHUNK_SIZE = 50000  # Larger chunks for your 64GB RAM
BATCH_SIZE = 5000   # Larger batch operations
CACHE_DIR = '/tmp/amg_cache'
SIMILARITY_THRESHOLD = 0.95
NUM_CORES = 12      # Utilize your 12 cores

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
@lru_cache(maxsize=4096)  # Increased cache size
def normalize_before_match(s: str) -> str:
    """Cached normalization for repeated strings - PERFORMANCE OPTIMIZATION."""
    return (
        s.lower()
        .replace('&', '/')
        .replace('_', ' ')
        .replace('  ', ' ')
        .strip()
    )

# --- pre-compiled regex patterns for performance ---
HARD_CODED_PATTERNS = {
    original: (re.compile(f"(?i)^{re.escape(original)}$"), replacement)
    for original, replacement in HARD_CODED_REPLACEMENTS.items()
}
DELIMITER_PATTERN = re.compile(r"\s*[,;|]\s*")

# --- vectorized hard-coded replacements with pre-compiled patterns ---
def apply_hard_coded_replacements_vectorized(series: pl.Series) -> pl.Series:
    """Apply hard-coded replacements using pre-compiled regex patterns."""
    result = series
    for pattern, replacement in HARD_CODED_PATTERNS.values():
        result = result.str.replace_all(pattern.pattern, replacement, literal=False)
    return result

# --- optimized split and trim with vectorized operations and proper null handling ---
def split_and_trim_vectorized(series: pl.Series) -> pl.Series:
    """
    Optimized split and trim using pure vectorized operations.
    Fixed to handle null-only columns by ensuring proper dtype.
    """
    return (
        series
        .fill_null("")  # Convert nulls to empty strings first
        .pipe(apply_hard_coded_replacements_vectorized)
        .str.replace_all(DELIMITER_PATTERN.pattern, DELIMITER, literal=False)
        .str.split(DELIMITER)
        .list.eval(
            pl.when(pl.element().str.strip_chars().str.len_chars() > 0)
            .then(pl.element().str.strip_chars())
        )
        .list.drop_nulls()
        .list.eval(
            pl.element().pipe(apply_hard_coded_replacements_vectorized)
        )
        .cast(pl.List(pl.String))  # Explicitly cast to ensure consistent dtype
    )

# --- pre-filter tags to reduce string_grouper workload ---
def pre_filter_tags(raw_tags: List[str], valid_tags: Set[str]) -> Tuple[List[str], Dict[str, str]]:
    """Pre-filter tags to reduce fuzzy matching workload."""
    normalized_valid = {normalize_before_match(v): v for v in valid_tags}

    exact_matches = {}
    fuzzy_candidates = []

    for tag in set(raw_tags):  # Remove duplicates early
        normalized = normalize_before_match(tag)
        if normalized in normalized_valid:
            exact_matches[tag] = normalized_valid[normalized]
        else:
            fuzzy_candidates.append(tag)

    return fuzzy_candidates, exact_matches

# --- optimized corrected mapping with caching ---
def build_corrected_mapping_optimized(
    raw_tags: List[str],
    valid_tags: Set[str],
    similarity_threshold: float = SIMILARITY_THRESHOLD
) -> Dict[str, Optional[str]]:
    """
    Build mapping with caching and pre-filtering optimizations.
    """
    ensure_cache_dir()

    # Check cache first
    cache_key = get_cache_key(raw_tags, valid_tags)
    cached_result = load_cached_mapping(cache_key)
    if cached_result is not None:
        logging.info("Using cached fuzzy matching results")
        return cached_result

    # Pre-filter to reduce fuzzy matching workload
    fuzzy_candidates, exact_matches = pre_filter_tags(raw_tags, valid_tags)

    logging.info(f"Exact matches: {len(exact_matches)}, Fuzzy candidates: {len(fuzzy_candidates)}")

    if not fuzzy_candidates:
        # Add identity mappings for tags not in exact matches
        result = exact_matches.copy()
        for tag in raw_tags:
            if tag not in result:
                result[tag] = tag
        save_cached_mapping(cache_key, result)
        return result

    # Optimized fuzzy matching with string dtype
    logging.info("Performing fuzzy matching...")
    fuzzy_matches = match_strings(
        pd.Series(fuzzy_candidates, dtype="string"),
        pd.Series(list(valid_tags), dtype="string"),
        min_similarity=similarity_threshold,
        max_n_matches=1  # Only need best match
    )

    # Build final mapping
    result = exact_matches.copy()

    # Add fuzzy matches
    fuzzy_dict = {}
    if not fuzzy_matches.empty:
        for _, row in fuzzy_matches.iterrows():
            fuzzy_dict[row["left_side"]] = row["right_side"]

    # Complete mapping with identity for unmatched
    for tag in raw_tags:
        if tag not in result:
            result[tag] = fuzzy_dict.get(tag, tag)

    # Cache the result
    save_cached_mapping(cache_key, result)
    return result

# --- optimized database utilities ---
def optimize_sqlite_connection(conn: sqlite3.Connection):
    """Apply SQLite optimizations tuned for 64GB RAM system."""
    optimizations = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -1048576",  # 1GB cache for your 64GB system
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 2147483648",  # 2GB memory map
        "PRAGMA page_size = 4096",
        "PRAGMA wal_autocheckpoint = 10000",  # Less frequent checkpoints
        "PRAGMA optimize"
    ]

    for pragma in optimizations:
        try:
            conn.execute(pragma)
        except sqlite3.Error as e:
            logging.warning(f"Failed to apply {pragma}: {e}")

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the SQLite database."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

def sqlite_to_polars_chunked(conn: sqlite3.Connection, query: str,
                           chunk_size: int = CHUNK_SIZE) -> Iterator[pl.DataFrame]:
    """Stream SQLite query results in chunks."""
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]

    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break

        data = {}
        for i, col_name in enumerate(column_names):
            col_data = [row[i] for row in rows]
            if col_name == "rowid":
                data[col_name] = pl.Series(col_data, dtype=pl.Int64)
            elif col_name == "sqlmodded":
                data[col_name] = pl.Series([int(x) if x is not None else 0 for x in col_data], dtype=pl.Int32)
            else:
                data[col_name] = pl.Series(col_data, dtype=pl.Utf8)

        yield pl.DataFrame(data)

def import_genre_style_streaming(conn: sqlite3.Connection) -> Iterator[pl.DataFrame]:
    """Import data in streaming fashion."""
    query = f"""
        SELECT rowid, __path, genre, style, COALESCE(sqlmodded, 0) AS sqlmodded
        FROM {ALIB_TABLE} WHERE genre IS NOT NULL OR style IS NOT NULL
        ORDER BY rowid
    """
    return sqlite_to_polars_chunked(conn, query, CHUNK_SIZE)

def import_valid_tags(conn: sqlite3.Connection) -> Set[str]:
    """Import valid genre tags from the reference table."""
    query = f"SELECT genre_name FROM {REF_VALIDATION_TABLE}"
    try:
        cursor = conn.execute(query)
        return {row[0] for row in cursor.fetchall()}
    except sqlite3.OperationalError as e:
        logging.error(f"Error importing valid tags: {e}")
        return set()

# --- parallel processing for tag extraction ---
def extract_tags_parallel(chunks: List[pl.DataFrame]) -> Tuple[Set[str], Set[str]]:
    """Extract tags in parallel using multiple cores."""
    if len(chunks) <= 1:
        # Single chunk, process directly
        if chunks:
            return process_chunk_tags(chunks[0])
        return set(), set()

    all_genre_tags = set()
    all_style_tags = set()

    # Use ProcessPoolExecutor for CPU-bound tag extraction
    with ProcessPoolExecutor(max_workers=min(NUM_CORES, len(chunks))) as executor:
        future_to_chunk = {executor.submit(process_chunk_tags, chunk): chunk for chunk in chunks}

        for future in as_completed(future_to_chunk):
            try:
                genre_tags, style_tags = future.result()
                all_genre_tags.update(genre_tags)
                all_style_tags.update(style_tags)
            except Exception as e:
                logging.error(f"Error processing chunk: {e}")

    return all_genre_tags, all_style_tags

def process_chunk_tags(chunk: pl.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Extract tags from a chunk using lazy evaluation.
    Fixed to handle null-only columns properly.
    """
    # Process genre tags with null safety
    genre_tags = (
        chunk
        .lazy()
        .select(split_and_trim_vectorized(pl.col("genre")).alias("genre_split"))
        .select(pl.col("genre_split").list.explode())
        .filter(
            pl.col("genre_split").is_not_null() &
            (pl.col("genre_split").str.len_chars() > 0)
        )
        .collect()
        .get_column("genre_split")
        .unique()
        .to_list()
    )

    # Process style tags with null safety
    style_tags = (
        chunk
        .lazy()
        .select(split_and_trim_vectorized(pl.col("style")).alias("style_split"))
        .select(pl.col("style_split").list.explode())
        .filter(
            pl.col("style_split").is_not_null() &
            (pl.col("style_split").str.len_chars() > 0)
        )
        .collect()
        .get_column("style_split")
        .unique()
        .to_list()
    )

    return genre_tags, style_tags

def apply_corrections_to_chunk(chunk: pl.DataFrame,
                             corrections: Dict[str, Optional[str]]) -> pl.DataFrame:
    """Apply corrections to a data chunk using vectorized operations."""

    def apply_mapping_optimized(tag: str) -> Optional[str]:
        """Optimized mapping function."""
        return corrections.get(tag, tag)

    # Process chunk with vectorized operations
    result = (
        chunk
        .lazy()
        .with_columns([
            split_and_trim_vectorized(pl.col("genre")).alias("genre_split"),
            split_and_trim_vectorized(pl.col("style")).alias("style_split")
        ])
        .with_columns([
            pl.col("genre_split")
            .list.eval(pl.element().map_elements(apply_mapping_optimized, return_dtype=pl.Utf8))
            .list.drop_nulls()
            .alias("validated_genre"),

            pl.col("style_split")
            .list.eval(pl.element().map_elements(apply_mapping_optimized, return_dtype=pl.Utf8))
            .list.drop_nulls()
            .alias("validated_style")
        ])
        .with_columns([
            pl.when(pl.col("validated_genre").list.len() == 0)
            .then(None)
            .otherwise(pl.col("validated_genre").list.join(DELIMITER))
            .alias("new_genre"),

            pl.when(pl.col("validated_style").list.len() == 0)
            .then(None)
            .otherwise(pl.col("validated_style").list.join(DELIMITER))
            .alias("new_style")
        ])
        .collect()
    )

    return result

def batch_database_updates(conn: sqlite3.Connection, updates: List[Dict],
                         changelog_entries: List[Dict]):
    """Perform batch database updates with prepared statements."""
    cursor = conn.cursor()

    try:
        # Batch update main table
        if updates:
            cursor.executemany(
                f"UPDATE {ALIB_TABLE} SET genre = :genre, style = :style, sqlmodded = :sqlmodded WHERE rowid = :rowid",
                updates
            )

        # Batch insert changelog
        if changelog_entries:
            cursor.executemany(
                "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (:rowid, :column, :old_value, :new_value, :timestamp, :script)",
                changelog_entries
            )

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"Database batch update failed: {e}")
        return False

# --- main process with streaming and optimization ---
def main():
    """High-performance main function optimized for 64GB RAM + 12-core system."""
    start_time = datetime.now()
    total_rows_processed = 0
    total_rows_updated = 0

    try:
        # Optimized database connection
        conn = sqlite3.connect(DB_PATH)
        optimize_sqlite_connection(conn)
        cursor = conn.cursor()

        # Create changelog table
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

        logging.info("Loading valid tags...")
        valid_tags = import_valid_tags(conn)
        logging.info(f"Loaded {len(valid_tags)} valid tags.")

        # Phase 1: Load data in larger chunks for your 64GB system
        logging.info("Phase 1: Loading data in memory-optimized chunks...")
        chunks = []
        chunk_count = 0
        
        # Initialize accumulator sets - THIS FIXES THE UNBOUNDLOCALERROR
        all_genre_tags = set()
        all_style_tags = set()

        for chunk in import_genre_style_streaming(conn):
            chunks.append(chunk)
            total_rows_processed += chunk.height
            chunk_count += 1

            # Process in batches of chunks to balance memory vs parallelism
            if len(chunks) >= NUM_CORES or chunk_count % 20 == 0:
                logging.info(f"Loaded {len(chunks)} chunks ({total_rows_processed} total rows)")

                # Extract tags in parallel
                genre_tags, style_tags = extract_tags_parallel(chunks)
                
                # Update accumulator sets
                all_genre_tags.update(genre_tags)
                all_style_tags.update(style_tags)

                chunks.clear()  # Free memory

        # Process remaining chunks
        if chunks:
            genre_tags, style_tags = extract_tags_parallel(chunks)
            all_genre_tags.update(genre_tags)
            all_style_tags.update(style_tags)

        logging.info(f"Collected {len(all_genre_tags)} unique genre tags and {len(all_style_tags)} unique style tags")

        # Phase 2: Build correction mapping (with caching)
        logging.info("Phase 2: Building correction mapping...")
        all_tags = list(all_genre_tags | all_style_tags)
        corrections = build_corrected_mapping_optimized(all_tags, valid_tags)
        logging.info(f"Built correction mapping for {len(corrections)} unique tags.")

        # Phase 3: Stream process with larger batches for your system
        logging.info("Phase 3: Processing with high-performance batching...")
        timestamp = datetime.now(timezone.utc).isoformat()

        update_batch = []
        changelog_batch = []
        processed_count = 0

        for chunk in import_genre_style_streaming(conn):
            processed_chunk = apply_corrections_to_chunk(chunk, corrections)

            # Identify changes
            changed_mask = (
                (processed_chunk.get_column("new_genre") != processed_chunk.get_column("genre").fill_null("")) |
                (processed_chunk.get_column("new_style") != processed_chunk.get_column("style").fill_null(""))
            )

            changed_df = processed_chunk.filter(changed_mask)

            if changed_df.height > 0:
                # Prepare updates
                updates = (
                    changed_df
                    .with_columns(pl.col("sqlmodded") + 1)
                    .select(["rowid", "new_genre", "new_style", "sqlmodded"])
                    .rename({"new_genre": "genre", "new_style": "style"})
                    .to_dicts()
                )
                update_batch.extend(updates)

                # Prepare changelog entries
                for row in changed_df.iter_rows(named=True):
                    if row["new_genre"] != (row["genre"] or ""):
                        changelog_batch.append({
                            "rowid": row["rowid"],
                            "column": "genre",
                            "old_value": row["genre"],
                            "new_value": row["new_genre"],
                            "timestamp": timestamp,
                            "script": "optimized-dropgenres-polars(string-grouper)"
                        })

                    if row["new_style"] != (row["style"] or ""):
                        changelog_batch.append({
                            "rowid": row["rowid"],
                            "column": "style",
                            "old_value": row["style"],
                            "new_value": row["new_style"],
                            "timestamp": timestamp,
                            "script": "optimized-dropgenres-polars(string-grouper)"
                        })

            processed_count += chunk.height

            # Larger batch commits for your high-spec system
            if len(update_batch) >= BATCH_SIZE:
                if batch_database_updates(conn, update_batch, changelog_batch):
                    total_rows_updated += len(update_batch)
                    logging.info(f"High-perf batch commit: {len(update_batch)} updates, {len(changelog_batch)} changelog entries")

                update_batch.clear()
                changelog_batch.clear()

            if processed_count % (CHUNK_SIZE * 2) == 0:
                logging.info(f"High-performance processing: {processed_count} rows...")

        # Final batch commit
        if update_batch:
            if batch_database_updates(conn, update_batch, changelog_batch):
                total_rows_updated += len(update_batch)
                logging.info(f"Final high-perf commit: {len(update_batch)} updates, {len(changelog_batch)} changelog entries")

        # Final statistics
        end_time = datetime.now()
        total_duration = end_time - start_time
        duration_minutes = total_duration.total_seconds() / 60
        rows_per_second = total_rows_processed / total_duration.total_seconds() if total_duration.total_seconds() > 0 else 0

        print(f"\nHigh-Performance Genre Cleanup Completed!")
        print(f"Hardware: AMD Ryzen 9 7900 (12-core) + 64GB RAM")
        print(f"Processed: {total_rows_processed:,} rows")
        print(f"Updated: {total_rows_updated:,} rows")
        print(f"Time: {duration_minutes:.2f} minutes")
        print(f"Rate: {rows_per_second:,.1f} rows/sec")
        print(f"Cores utilized: {NUM_CORES}")

        logging.info(f"High-performance genre cleanup completed. "
                    f"Processed: {total_rows_processed} rows, "
                    f"Updated: {total_rows_updated} rows, "
                    f"Time: {duration_minutes:.2f} minutes, "
                    f"Rate: {rows_per_second:.1f} rows/sec")

    except Exception as e:
        end_time = datetime.now()
        total_duration = end_time - start_time
        duration_minutes = total_duration.total_seconds() / 60

        logging.error(f"An error occurred after {duration_minutes:.2f} minutes: {e}", exc_info=True)
        print(f"\nHigh-performance genre cleanup failed after {duration_minutes:.2f} minutes. Error: {e}")

        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()