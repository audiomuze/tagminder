"""
Script Name: string-grouper-similarity-analysis-polars-optimized.py

Purpose:
    Builds a consolidated list of contributors from multiple database columns (artist, albumartist,
    composer, writer, lyricist, engineer, producer) and uses string-grouper to identify potential
    duplicates that may represent the same entity.

    Optimized version that eliminates unnecessary temporary table creation and improves
    vectorization performance.

Author: audiomuze
Created: 2025-07-19
Optimized: 2025-07-20
"""

import os
import polars as pl
import sqlite3
from typing import Set, Tuple
import logging
import argparse
from string_grouper import match_strings
import pandas as pd  # Still needed for string_grouper compatibility

# ---------- Configuration ----------
SCRIPT_NAME = "string-grouper-similarity-analysis-polars-optimized.py"
DB_PATH = '/tmp/amg/dbtemplate.db'
SIMILARITY_THRESHOLD = 0.85
OUTPUT_CSV = '/tmp/amg/_INF_string_grouper_possible_namesakes.csv'

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Global Constants ----------
CONTRIBUTOR_COLUMNS = [
    'artist', 'albumartist', 'composer', 'writer',
    'lyricist', 'engineer', 'producer'
]

# ---------- Database Helper Functions ----------

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the SQLite database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None

def sqlite_to_polars(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """
    Convert SQLite query results to a Polars DataFrame with proper type handling.
    Simplified version without unused id_column parameter.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        return pl.DataFrame({col: [] for col in column_names})

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if col_name in ("rowid", "sqlmodded", "alib_rowid"):
            data[col_name] = [int(x or 0) for x in col_data]
        else:
            data[col_name] = [str(x) if x is not None else None for x in col_data]

    return pl.DataFrame(data)

# ---------- Optimized Contributor Extraction ----------

def extract_and_process_contributors(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Extract and process all contributors in a single optimized function.
    Uses vectorized operations throughout for better performance.
    """
    logging.info("Extracting and processing contributors...")

    # Build dynamic query to get all contributor columns
    contributor_select = ', '.join(CONTRIBUTOR_COLUMNS)
    null_check = ' OR '.join([f'{col} IS NOT NULL' for col in CONTRIBUTOR_COLUMNS])

    contributors_df = sqlite_to_polars(
        conn,
        f"""
        SELECT {contributor_select}
        FROM alib
        WHERE {null_check}
        """
    )

    if contributors_df.height == 0:
        logging.warning("No contributor data found")
        return pl.DataFrame({"contributor": [], "lcontributor": []})

    # Vectorized extraction: unpivot all contributor columns into single column
    melted_df = contributors_df.unpivot(
        on=CONTRIBUTOR_COLUMNS,
        variable_name="contributor_type",
        value_name="contributor"
    ).filter(
        pl.col("contributor").is_not_null() &
        (pl.col("contributor").str.strip_chars() != "")
    )

    # Handle delimited entries using vectorized string operations
    # Split on double backslash and explode to separate rows
    processed_df = melted_df.with_columns([
        pl.col("contributor").str.split("\\\\").alias("contributor_parts")
    ]).explode("contributor_parts").with_columns([
        pl.col("contributor_parts").str.strip_chars().alias("contributor")
    ]).filter(
        pl.col("contributor").is_not_null() &
        (pl.col("contributor") != "")
    ).select("contributor").unique()

    # Add lowercase column for matching in single operation
    result_df = processed_df.with_columns([
        pl.col("contributor").str.to_lowercase().alias("lcontributor")
    ])

    logging.info(f"Extracted {result_df.height} distinct contributors")
    return result_df

# ---------- Optimized Processing Functions ----------

def get_processed_contributors_vectorized(conn: sqlite3.Connection) -> Set[str]:
    """
    Get all previously processed contributors using vectorized operations.
    Returns a flat set of contributor names rather than pairs for simpler filtering.
    """
    processed_contributors = set()

    # Get from vetted contributors
    if table_exists(conn, '_REF_vetted_contributors'):
        vetted_df = sqlite_to_polars(
            conn,
            "SELECT current_val FROM _REF_vetted_contributors WHERE current_val IS NOT NULL"
        )
        if vetted_df.height > 0:
            processed_contributors.update(vetted_df['current_val'].to_list())

    # Get from workspace (both current and replacement values)
    if table_exists(conn, '_REF_contributors_workspace'):
        workspace_df = sqlite_to_polars(
            conn,
            """
            SELECT current_val, replacement_val
            FROM _REF_contributors_workspace
            WHERE status IN (0, 1)
            AND current_val IS NOT NULL
            AND replacement_val IS NOT NULL
            """
        )
        if workspace_df.height > 0:
            processed_contributors.update(workspace_df['current_val'].to_list())
            processed_contributors.update(workspace_df['replacement_val'].to_list())

    logging.info(f"Found {len(processed_contributors)} previously processed contributors")
    return processed_contributors

# def perform_similarity_analysis_optimized(contributors_df: pl.DataFrame, processed_contributors: Set[str], similarity_threshold: float) -> pl.DataFrame:
#     """
#     Optimized similarity analysis with better filtering.

#     Args:
#         contributors_df: DataFrame with contributor data
#         processed_contributors: Set of already processed contributors
#         similarity_threshold: Minimum similarity threshold for matches
#     """
#     logging.info(f"Running similarity analysis (threshold: {similarity_threshold})")

#     # Filter out already processed contributors before analysis
#     unprocessed_df = contributors_df.filter(
#         ~pl.col("contributor").is_in(list(processed_contributors))
#     )

#     if unprocessed_df.height < 2:
#         logging.info("Not enough unprocessed contributors for similarity analysis")
#         return pl.DataFrame({
#             "left_contributor": [],
#             "right_contributor": [],
#             "similarity": []
#         })

#     # Convert to pandas for string_grouper (unavoidable dependency)
#     contributors_pandas = unprocessed_df.to_pandas()

#     try:
#         matches = match_strings(
#             contributors_pandas['contributor'],
#             min_similarity=similarity_threshold
#         )
#     except Exception as e:
#         logging.error(f"String grouper analysis failed: {e}")
#         return pl.DataFrame({
#             "left_contributor": [],
#             "right_contributor": [],
#             "similarity": []
#         })

#     # Filter exact matches and convert back to Polars
#     if matches.empty:
#         similarities_df = pl.DataFrame({
#             "left_contributor": [],
#             "right_contributor": [],
#             "similarity": []
#         })
#     else:
#         # Filter non-exact matches
#         filtered_matches = matches[
#             matches['left_contributor'] != matches['right_contributor']
#         ]

#         if filtered_matches.empty:
#             similarities_df = pl.DataFrame({
#                 "left_contributor": [],
#                 "right_contributor": [],
#                 "similarity": []
#             })
#         else:
#             similarities_df = pl.from_pandas(filtered_matches)

#     logging.info(f"Found {similarities_df.height} potential matches")
#     return similarities_df



# def create_workspace_entries_vectorized(similarities_df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Vectorized creation of workspace entries with optimized sorting.
#     Sorted by similarity descending, then cv_sort ascending for efficient review.
#     """
#     if similarities_df.height == 0:
#         return pl.DataFrame({
#             "current_val": [],
#             "replacement_val": [],
#             "similarity": [],
#             "status": [],
#             "cv_sort": [],
#             "rv_sort": []
#         })

#     # Vectorized "the" processing using when/then expressions
#     workspace_df = similarities_df.with_columns([
#         pl.col("left_contributor").alias("current_val"),
#         pl.col("right_contributor").alias("replacement_val"),
#         pl.col("similarity"),  # Include similarity score
#         pl.lit(None, dtype=pl.Int64).alias("status"),
#         # Vectorized "the" processing for sorting
#         pl.when(pl.col("left_contributor").str.to_lowercase().str.starts_with("the "))
#         .then(pl.col("left_contributor").str.slice(4).str.to_lowercase() + ", the")
#         .otherwise(pl.col("left_contributor").str.to_lowercase())
#         .alias("cv_sort"),
#         pl.when(pl.col("right_contributor").str.to_lowercase().str.starts_with("the "))
#         .then(pl.col("right_contributor").str.slice(4).str.to_lowercase() + ", the")
#         .otherwise(pl.col("right_contributor").str.to_lowercase())
#         .alias("rv_sort")
#     ]).select([
#         "current_val", "replacement_val", "similarity", "status", "cv_sort", "rv_sort"
#     ]).sort([
#         pl.col("similarity"),  # High similarity first
#         "cv_sort"  # Then alphabetically by current_val
#     ], descending=[True, False])

#     return workspace_df

# ---------- Optimized Output Functions ----------

# def save_results_optimized(similarities_df: pl.DataFrame, csv_path: str, save_csv: bool = True) -> None:
#     """
#     Save similarity results to CSV if requested.

#     Args:
#         similarities_df: DataFrame with similarity results
#         csv_path: Path for CSV output
#         save_csv: Whether to save CSV output (default True)
#     """
#     if similarities_df.height == 0:
#         logging.info("No results to save")
#         return

#     # Save to CSV only if requested
#     if save_csv:
#         similarities_df.write_csv(csv_path, separator='|')
#         logging.info(f"Saved {similarities_df.height} matches to {csv_path}")
#     else:
#         logging.info(f"Skipping CSV output (found {similarities_df.height} matches)")

def perform_similarity_analysis_optimized(contributors_df: pl.DataFrame, processed_contributors: Set[str], similarity_threshold: float) -> pl.DataFrame:
    """
    Optimized similarity analysis with better filtering and index preservation.

    Args:
        contributors_df: DataFrame with contributor data
        processed_contributors: Set of already processed contributors
        similarity_threshold: Minimum similarity threshold for matches
    """
    logging.info(f"Running similarity analysis (threshold: {similarity_threshold})")

    # Filter out already processed contributors before analysis
    unprocessed_df = contributors_df.filter(
        ~pl.col("contributor").is_in(list(processed_contributors))
    ).with_row_index("original_index")  # Add row index for ordering

    if unprocessed_df.height < 2:
        logging.info("Not enough unprocessed contributors for similarity analysis")
        return pl.DataFrame({
            "left_contributor": [],
            "right_contributor": [],
            "left_index": [],
            "right_index": [],
            "similarity": []
        })

    # Convert to pandas for string_grouper (unavoidable dependency)
    contributors_pandas = unprocessed_df.to_pandas()

    try:
        matches = match_strings(
            contributors_pandas['contributor'],
            min_similarity=similarity_threshold
        )
    except Exception as e:
        logging.error(f"String grouper analysis failed: {e}")
        return pl.DataFrame({
            "left_contributor": [],
            "right_contributor": [],
            "left_index": [],
            "right_index": [],
            "similarity": []
        })

    # Filter exact matches and convert back to Polars with index information
    if matches.empty:
        similarities_df = pl.DataFrame({
            "left_contributor": [],
            "right_contributor": [],
            "left_index": [],
            "right_index": [],
            "similarity": []
        })
    else:
        # Filter non-exact matches
        filtered_matches = matches[
            matches['left_contributor'] != matches['right_contributor']
        ]

        if filtered_matches.empty:
            similarities_df = pl.DataFrame({
                "left_contributor": [],
                "right_contributor": [],
                "left_index": [],
                "right_index": [],
                "similarity": []
            })
        else:
            # Convert back to Polars and ensure we have the index columns
            similarities_df = pl.from_pandas(filtered_matches)

            # If string_grouper didn't provide indices, we need to add them
            if "left_index" not in similarities_df.columns:
                # Create mapping from contributor to index
                contributor_to_index = dict(zip(
                    unprocessed_df['contributor'].to_list(),
                    unprocessed_df['original_index'].to_list()
                ))

                similarities_df = similarities_df.with_columns([
                    pl.col("left_contributor").map_elements(
                        lambda x: contributor_to_index.get(x, -1),
                        return_dtype=pl.Int64
                    ).alias("left_index"),
                    pl.col("right_contributor").map_elements(
                        lambda x: contributor_to_index.get(x, -1),
                        return_dtype=pl.Int64
                    ).alias("right_index")
                ])

    logging.info(f"Found {similarities_df.height} potential matches")
    return similarities_df


def save_results_optimized(similarities_df: pl.DataFrame, csv_path: str, save_csv: bool = True) -> None:
    """
    Save similarity results to CSV with sophisticated index-based ordering.
    Implements the same logic as the provided SQL query.

    Args:
        similarities_df: DataFrame with similarity results including indices
        csv_path: Path for CSV output
        save_csv: Whether to save CSV output (default True)
    """
    if similarities_df.height == 0:
        logging.info("No results to save")
        return

    if save_csv:
        # Apply the sophisticated ordering logic equivalent to the SQL query
        ordered_df = similarities_df.with_columns([
            # Calculate min_index (equivalent to CASE WHEN left_index < right_index THEN left_index ELSE right_index END)
            pl.min_horizontal(["left_index", "right_index"]).alias("min_index"),
            # Calculate max_index (equivalent to CASE WHEN left_index > right_index THEN left_index ELSE right_index END)
            pl.max_horizontal(["left_index", "right_index"]).alias("max_index")
        ]).sort([
            "min_index",      # First ordering criterion
            "max_index",      # Second ordering criterion
            "left_index"      # Third ordering criterion (tie-breaker)
        ])

        # Remove the temporary ordering columns before saving
        final_df = ordered_df.select([
            "left_contributor", "right_contributor",
            "left_index", "right_index", "similarity"
        ])

        final_df.write_csv(csv_path, separator='|')
        logging.info(f"Saved {final_df.height} matches to {csv_path} with index-based ordering")
    else:
        logging.info(f"Skipping CSV output (found {similarities_df.height} matches)")


# def create_workspace_entries_vectorized(similarities_df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Vectorized creation of workspace entries with optimized sorting.
#     Updated to handle the new index columns in the similarities DataFrame.
#     """
#     if similarities_df.height == 0:
#         return pl.DataFrame({
#             "current_val": [],
#             "replacement_val": [],
#             "similarity": [],
#             "status": [],
#             "cv_sort": [],
#             "rv_sort": []
#         })

#     # Vectorized "the" processing using when/then expressions
#     workspace_df = similarities_df.with_columns([
#         pl.col("left_contributor").alias("current_val"),
#         pl.col("right_contributor").alias("replacement_val"),
#         pl.col("similarity"),  # Include similarity score
#         pl.lit(None, dtype=pl.Int64).alias("status"),
#         # Vectorized "the" processing for sorting
#         pl.when(pl.col("left_contributor").str.to_lowercase().str.starts_with("the "))
#         .then(pl.col("left_contributor").str.slice(4).str.to_lowercase() + ", the")
#         .otherwise(pl.col("left_contributor").str.to_lowercase())
#         .alias("cv_sort"),
#         pl.when(pl.col("right_contributor").str.to_lowercase().str.starts_with("the "))
#         .then(pl.col("right_contributor").str.slice(4).str.to_lowercase() + ", the")
#         .otherwise(pl.col("right_contributor").str.to_lowercase())
#         .alias("rv_sort")
#     ]).select([
#         "current_val", "replacement_val", "similarity", "status", "cv_sort", "rv_sort"
#     ]).sort([
#         pl.col("similarity"),  # High similarity first
#         "cv_sort"  # Then alphabetically by current_val
#     ], descending=[True, False])

#     return workspace_df


def create_workspace_entries_vectorized(similarities_df: pl.DataFrame) -> pl.DataFrame:
    """
    Vectorized creation of workspace entries with group-based ordering.

    Orders by:
    1. Group related pairs using index-based ordering (min_index, max_index, left_index)
    2. Order groups by average similarity score descending, then by concatenated group names ascending
    3. Within each group, maintain index-based ordering from CSV logic
    """
    if similarities_df.height == 0:
        return pl.DataFrame({
            "current_val": [],
            "replacement_val": [],
            "similarity": [],
            "status": [],
            "cv_sort": [],
            "rv_sort": []
        })

    # Step 1: Add the sophisticated index-based grouping columns and create group identifier
    grouped_df = similarities_df.with_columns([
        # Calculate min_index and max_index for grouping (same as CSV ordering)
        pl.min_horizontal(["left_index", "right_index"]).alias("min_index"),
        pl.max_horizontal(["left_index", "right_index"]).alias("max_index")
    ]).with_columns([
        # Create group identifier by concatenating all unique contributor names in each pair
        pl.concat_list([
            pl.col("left_contributor"),
            pl.col("right_contributor")
        ]).flatten().unique().sort().str.join(" - ").alias("group_id")
    ])

    # Step 2: Calculate group-level statistics for ordering groups
    group_stats = grouped_df.group_by("group_id").agg([
        # Average similarity for the group (more representative than max)
        pl.col("similarity").mean().alias("group_avg_similarity"),
        # Group name is the same as group_id (natural sort order)
        pl.col("group_id").first().alias("group_concat_names"),
        pl.col("min_index").first().alias("group_min_index"),
        pl.col("max_index").first().alias("group_max_index")
    ])

    # Save group statistics to CSV for inspection
    try:
        group_stats_for_csv = group_stats.sort([
            pl.col("group_avg_similarity"),
            pl.col("group_concat_names")
        ], descending=[True, False])

        group_stats_for_csv.write_csv('/tmp/amg/_DEBUG_group_statistics.csv', separator='|')
        logging.info(f"Saved group statistics to /tmp/amg/_DEBUG_group_statistics.csv ({group_stats.height} groups)")
    except Exception as e:
        logging.warning(f"Failed to save group statistics CSV: {e}")

    # Step 3: Join group stats back to main dataframe and create workspace columns
    workspace_df = grouped_df.join(group_stats, on="group_id", how="left").with_columns([
        pl.col("left_contributor").alias("current_val"),
        pl.col("right_contributor").alias("replacement_val"),
        pl.col("similarity"),
        pl.lit(None, dtype=pl.Int64).alias("status"),
        # Vectorized "the" processing for individual sorting
        pl.when(pl.col("left_contributor").str.to_lowercase().str.starts_with("the "))
        .then(pl.col("left_contributor").str.slice(4).str.to_lowercase() + ", the")
        .otherwise(pl.col("left_contributor").str.to_lowercase())
        .alias("cv_sort"),
        pl.when(pl.col("right_contributor").str.to_lowercase().str.starts_with("the "))
        .then(pl.col("right_contributor").str.slice(4).str.to_lowercase() + ", the")
        .otherwise(pl.col("right_contributor").str.to_lowercase())
        .alias("rv_sort")
    ])

    # Step 4: Apply the multi-level sorting
    final_df = workspace_df.sort([
        # Primary: Group by average similarity descending
        pl.col("group_avg_similarity"),
        # Secondary: Natural sort order by concatenated group names
        pl.col("group_concat_names"),
        # Tertiary: Within group, use sophisticated index-based ordering (from CSV logic)
        pl.col("min_index"),
        pl.col("max_index"),
        pl.col("left_index")
    ], descending=[True, False, False, False, False])

    # Step 5: Return only the required columns
    return final_df.select([
        "current_val", "replacement_val", "similarity", "status", "cv_sort", "rv_sort"
    ])


def update_workspace_optimized(conn: sqlite3.Connection, workspace_df: pl.DataFrame) -> None:
    """
    Optimized workspace update with similarity-based sorting for efficient review.
    Table includes similarity score and is sorted by similarity desc, cv_sort asc.
    """
    if workspace_df.height == 0:
        logging.info("No workspace entries to add")
        return

    cursor = conn.cursor()

    # Ensure table exists with similarity column
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _REF_contributors_workspace (
            current_val TEXT,
            replacement_val TEXT,
            similarity REAL,
            status INTEGER,
            cv_sort TEXT,
            rv_sort TEXT,
            notes TEXT,
            PRIMARY KEY (current_val, replacement_val)
        )
    """)

    # Batch insert with proper error handling
    workspace_data = workspace_df.to_pandas()
    insert_count = 0

    for _, row in workspace_data.iterrows():
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO _REF_contributors_workspace
                (current_val, replacement_val, similarity, status, cv_sort, rv_sort, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['current_val'],
                row['replacement_val'],
                row['similarity'],
                row['status'],
                row['cv_sort'],
                row['rv_sort'],
                None
            ))
            if cursor.rowcount > 0:
                insert_count += 1
        except sqlite3.Error as e:
            logging.warning(f"Failed to insert workspace entry: {e}")

    conn.commit()
    logging.info(f"Added {insert_count} new workspace entries")

# ---------- Argument Parsing ----------

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='String-grouper similarity analysis for contributor deduplication',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python %(prog)s                           # Run with default similarity (0.85)
  python %(prog)s --csv                     # Include CSV output
  python %(prog)s --similarity=0.90         # Use higher similarity threshold
  python %(prog)s --csv --similarity=0.75   # Lower threshold with CSV output
        """
    )

    parser.add_argument(
        '--csv',
        action='store_true',
        help='Generate CSV output file (default: database only)'
    )

    parser.add_argument(
        '--similarity',
        type=float,
        default=SIMILARITY_THRESHOLD,
        metavar='THRESHOLD',
        help=f'Similarity threshold (0.0-1.0, default: {SIMILARITY_THRESHOLD})'
    )

    args = parser.parse_args()

    # Validate similarity threshold
    if not 0.0 <= args.similarity <= 1.0:
        parser.error("Similarity threshold must be between 0.0 and 1.0")

    return args

# ---------- Streamlined Main Function ----------

def main():
    """
    Streamlined main execution with eliminated temporary table creation.
    """
    args = parse_arguments()

    logging.info("Starting optimized string-grouper similarity analysis")
    logging.info(f"Similarity threshold: {args.similarity}")
    if args.csv:
        logging.info("CSV output enabled")
    else:
        logging.info("Database output only (use --csv for CSV output)")

    if not os.path.exists(DB_PATH):
        logging.error(f"Database file does not exist: {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)

        # Step 1: Extract and process contributors (no temp table needed)
        contributors_df = extract_and_process_contributors(conn)
        if contributors_df.height == 0:
            logging.warning("No contributors found, exiting")
            return

        # Step 2: Get processed contributors
        processed_contributors = get_processed_contributors_vectorized(conn)

        # Step 3: Perform similarity analysis with custom threshold
        similarities_df = perform_similarity_analysis_optimized(contributors_df, processed_contributors, args.similarity)

        # Step 4: Save results (CSV only if requested)
        save_results_optimized(similarities_df, OUTPUT_CSV, save_csv=args.csv)

        # Step 5: Update workspace
        workspace_df = create_workspace_entries_vectorized(similarities_df)
        update_workspace_optimized(conn, workspace_df)

        # Summary
        if similarities_df.height > 0:
            logging.info(f"Analysis complete: {similarities_df.height} potential matches found")
        else:
            logging.info("Analysis complete: No new matches found")

    except Exception as e:
        logging.error(f"Analysis failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
