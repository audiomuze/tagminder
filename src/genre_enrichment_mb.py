# --- imports ---
import argparse
import polars as pl
import sqlite3
import logging
from datetime import datetime, timezone
from typing import List, Dict

# --- constants and config ---
DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_MB_TABLE = '_REF_mb_disambiguated'
DELIMITER = '\\\\'
CHUNK_SIZE = 100000
BATCH_SIZE = 10000

# Generic genres that trigger enrichment
GENERIC_GENRES = {'Pop', 'Pop/Rock', 'Jazz', 'Classical'}

# --- logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

def deduplicate_tags_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """
    Deduplicate tags in genre and style columns after merging.
    Removes duplicate tags while preserving order.
    """
    def deduplicate_tag_string(tag_string: str) -> str:
        """Deduplicate tags in a delimited string."""
        if not tag_string or tag_string.strip() == "":
            return ""
        
        # Split, deduplicate while preserving order, and rejoin
        tags = tag_string.split(DELIMITER)
        seen = set()
        unique_tags = []
        for tag in tags:
            stripped_tag = tag.strip()
            if stripped_tag and stripped_tag not in seen:
                seen.add(stripped_tag)
                unique_tags.append(stripped_tag)
        
        return DELIMITER.join(unique_tags) if unique_tags else ""

    # Apply deduplication to both genre and style columns
    df = df.with_columns([
        pl.col("new_genre").map_elements(
            deduplicate_tag_string, return_dtype=pl.Utf8
        ).alias("new_genre_dedup"),
        pl.col("new_style").map_elements(
            deduplicate_tag_string, return_dtype=pl.Utf8
        ).alias("new_style_dedup")
    ])
    
    return df

def merge_genre_style_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """
    Efficiently merge and deduplicate genre and style tags into genre field.
    Style field remains unchanged from its cleaned state.
    """
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
        .alias("new_genre_merged")
    ])

    # Drop intermediate columns
    return df.drop(["genre_list", "style_list", "genre_clean", "style_clean", "merged_tags"])

def create_changelog_entries_vectorized(df: pl.DataFrame, timestamp: str) -> tuple[pl.DataFrame, list]:
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
            pl.col("original_genre").fill_null("").alias("genre_norm"),
            pl.col("original_style").fill_null("").alias("style_norm"),
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
        .filter(pl.col("has_changes"))
        .collect()
    )

    # Generate changelog entries for changed rows
    for row in changes_df.iter_rows(named=True):
        if row['genre_changed']:
            changelog_entries.append({
                'alib_rowid': row['rowid'],
                'column': 'genre',
                'old_value': row['original_genre'],
                'new_value': row['new_genre'],
                'timestamp': timestamp,
                'script': 'genre-enrichment-mb-reference'
            })

        if row['style_changed']:
            changelog_entries.append({
                'alib_rowid': row['rowid'],
                'column': 'style',
                'old_value': row['original_style'],
                'new_value': row['new_style'],
                'timestamp': timestamp,
                'script': 'genre-enrichment-mb-reference'
            })

    return changes_df, changelog_entries

# --- database utilities ---
def optimize_sqlite_connection(conn: sqlite3.Connection):
    """Apply SQLite optimizations."""
    optimizations = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -2097152",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 8589934592",
        "PRAGMA page_size = 4096",
        "PRAGMA wal_autocheckpoint = 10000",
        "PRAGMA optimize"
    ]

    for pragma in optimizations:
        try:
            conn.execute(pragma)
        except sqlite3.Error as e:
            logging.warning(f"Failed to apply {pragma}: {e}")

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
                "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                [(c['alib_rowid'], c['column'], c['old_value'], c['new_value'], c['timestamp'], c['script']) for c in changelog_entries]
            )

        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"Database batch update failed: {e}")
        return False

def load_mb_reference_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Load MusicBrainz reference data from database."""
    query = f"""
    SELECT mbid, genre, styles
    FROM {REF_MB_TABLE}
    WHERE mbid IS NOT NULL
    """
    
    df = pl.read_database(query, conn, schema_overrides={
        'mbid': pl.Utf8,
        'genre': pl.Utf8,
        'styles': pl.Utf8
    })
    
    logging.info(f"Loaded {len(df)} MusicBrainz reference records")
    return df

def merge_existing_and_reference_tags(df: pl.DataFrame) -> pl.DataFrame:
    """
    Merge existing genre/style with reference data.
    Returns new genre and style columns without modifying originals.
    """
    
    def merge_tag_strings(existing: str, reference: str) -> str:
        """Merge two tag strings, preserving order."""
        if not existing or existing.strip() == "":
            return reference if reference else ""
        if not reference or reference.strip() == "":
            return existing
        
        # Combine with delimiter
        return f"{existing}{DELIMITER}{reference}"
    
    # Create NEW columns for merged tags (don't modify originals)
    df = df.with_columns([
        pl.struct(["original_genre", "ref_genre"])
        .map_elements(
            lambda x: merge_tag_strings(x["original_genre"], x["ref_genre"]),
            return_dtype=pl.Utf8
        )
        .alias("merged_genre"),
        
        pl.struct(["original_style", "ref_styles"])
        .map_elements(
            lambda x: merge_tag_strings(x["original_style"], x["ref_styles"]),
            return_dtype=pl.Utf8
        )
        .alias("merged_style")
    ])
    
    return df

def main():
    """
    Main function to enrich genre/style metadata from MusicBrainz reference data.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Enrich genre/style tags from MusicBrainz reference data')
    parser.add_argument('--dont-merge-genres-styles', action='store_true',
                        help='Do NOT merge genre and style tags into genre field (style field preserved)')
    args = parser.parse_args()

    start_time = datetime.now()
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        # Database setup
        conn = sqlite3.connect(DB_PATH)
        optimize_sqlite_connection(conn)

        # Create changelog table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS changelog (
                alib_rowid INTEGER,
                alib_column TEXT,
                old_value TEXT,
                new_value TEXT,
                timestamp TEXT,
                script TEXT
            )
        """)

        logging.info("Loading MusicBrainz reference data...")
        mb_ref_df = load_mb_reference_data(conn)

        # Build the selection query for albums needing enrichment
        query = f"""
        SELECT 
            rowid, 
            __path,
            albumartist,
            musicbrainz_albumartistid,
            genre, 
            style, 
            COALESCE(sqlmodded, 0) AS sqlmodded
        FROM {ALIB_TABLE}
        WHERE (
            genre IS NULL 
            OR genre = '' 
            OR genre = 'Pop'
            OR genre = 'Pop/Rock' 
            OR genre = 'Jazz'
            OR genre = 'Classical'
        )
        ORDER BY rowid
        """

        logging.info("Processing database records...")
        
        total_processed = 0
        total_updated = 0
        total_matched = 0
        
        update_batch = []
        changelog_batch = []

        cursor = conn.cursor()
        cursor.execute(query)

        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break

            # Convert to Polars DataFrame
            df = pl.DataFrame({
                'rowid': pl.Series([r[0] for r in rows], dtype=pl.Int64),
                '__path': pl.Series([r[1] for r in rows], dtype=pl.Utf8),
                'albumartist': pl.Series([r[2] for r in rows], dtype=pl.Utf8),
                'musicbrainz_albumartistid': pl.Series([r[3] for r in rows], dtype=pl.Utf8),
                'genre': pl.Series([r[4] for r in rows], dtype=pl.Utf8),
                'style': pl.Series([r[5] for r in rows], dtype=pl.Utf8),
                'sqlmodded': pl.Series([int(r[6]) for r in rows], dtype=pl.Int64)
            })

            # Join with MusicBrainz reference data
            enriched_df = df.join(
                mb_ref_df,
                left_on='musicbrainz_albumartistid',
                right_on='mbid',
                how='left'
            ).rename({
                'genre_right': 'ref_genre',
                'styles': 'ref_styles'
            })

            # Track albums with matches (must have at least genre data)
            matched_mask = enriched_df['ref_genre'].is_not_null()
            matched_df = enriched_df.filter(matched_mask)
            total_matched += len(matched_df)

            if len(matched_df) > 0:
                # PRESERVE original values before any modification
                processed_df = matched_df.with_columns([
                    pl.col("genre").alias("original_genre"),
                    pl.col("style").alias("original_style")
                ])
                
                # Merge existing + reference tags into NEW columns
                merged_df = merge_existing_and_reference_tags(processed_df)
                
                # Start with merged tags as the new values
                processed_df = merged_df.with_columns([
                    pl.col("merged_genre").alias("new_genre"),
                    pl.col("merged_style").alias("new_style")
                ])

                # DEFAULT BEHAVIOR: Merge styles into genres (unless --dont-merge-genres-styles is specified)
                if not args.dont_merge_genres_styles:
                    processed_df = merge_genre_style_vectorized(processed_df)
                    processed_df = processed_df.with_columns([
                        pl.col("new_genre_merged").alias("new_genre"),
                        pl.col("new_style").alias("new_style")  # Keep original merged style
                    ])
                else:
                    # Only deduplicate separately if we're NOT merging
                    processed_df = deduplicate_tags_vectorized(processed_df)
                    processed_df = processed_df.with_columns([
                        pl.col("new_genre_dedup").alias("new_genre"),
                        pl.col("new_style_dedup").alias("new_style")
                    ])

                # Find changes - compare new values against ORIGINAL values
                changed_df, new_changelog_entries = create_changelog_entries_vectorized(
                    processed_df, timestamp
                )

                if changed_df.height > 0:
                    # Prepare batch updates
                    for row in changed_df.iter_rows(named=True):
                        update_batch.append({
                            'rowid': row['rowid'],
                            'genre': row['new_genre'],
                            'style': row['new_style'],
                            'sqlmodded': row['sqlmodded'] + 1
                        })

                    changelog_batch.extend(new_changelog_entries)

            total_processed += len(rows)

            # Batch commit
            if len(update_batch) >= BATCH_SIZE:
                if batch_database_updates(conn, update_batch, changelog_batch):
                    total_updated += len(update_batch)
                    logging.info(f"Processed {total_processed:,} rows, matched {total_matched:,}, updated {total_updated:,}")

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

        merge_status = "" if not args.dont_merge_genres_styles else " (without genre-style merge)"
        print(f"\nGenre Enrichment from MusicBrainz Reference{merge_status} Completed!")
        print(f"=" * 80)
        print(f"Processed: {total_processed:,} rows")
        print(f"Matched with reference: {total_matched:,} rows")
        print(f"Updated: {total_updated:,} rows")
        print(f"Time: {duration_minutes:.2f} minutes")
        print(f"Rate: {rows_per_second:,.1f} rows/sec")
        print(f"=" * 80)

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()