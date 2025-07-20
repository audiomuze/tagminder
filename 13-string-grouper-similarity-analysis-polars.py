"""
Script Name: string-grouper-similarity-analysis-polars.py

Purpose:
    Builds a consolidated list of contributors from multiple database columns (artist, albumartist, 
    composer, writer, lyricist, engineer, producer) and uses string-grouper to identify potential
    duplicates that may represent the same entity.

    The script:
    - Extracts distinct contributors from all contributor fields in the alib table
    - Creates a temporary table (_TMP_distinct_contributors) for analysis
    - Uses string-grouper to find similar names based on configurable similarity threshold
    - Filters out previously processed entries from reference tables
    - Outputs results to CSV and database table for manual review
    - Prepares data for the _REF_contributors_workspace table for user evaluation

    This is a Polars-vectorized version of the original pandas-based approach, providing
    improved performance and memory efficiency while maintaining the same functionality.

    It is part of tagminder.

Usage:
    python string-grouper-similarity-analysis-polars.py

Author: audiomuze
Created: 2025-07-19

Dependencies:
    pip install polars string-grouper

"""

import os
import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Union, Set
import logging
from datetime import datetime, timezone
from string_grouper import match_strings
import pandas as pd  # Still needed for string_grouper compatibility

# ---------- Configuration ----------
SCRIPT_NAME = "string-grouper-similarity-analysis-polars.py"
DB_PATH = '/tmp/amg/dbtemplate.db'
SIMILARITY_THRESHOLD = 0.85  # Configurable similarity threshold for string matching
OUTPUT_CSV = '_INF_string_grouper_possible_namesakes.csv'
OUTPUT_TABLE = '_INF_string_grouper_possible_namesakes'

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Global Constants ----------
# Contributor columns to analyze
CONTRIBUTOR_COLUMNS = [
    'artist', 'albumartist', 'composer', 'writer', 
    'lyricist', 'engineer', 'producer'
]

# ---------- Database Helper Functions ----------

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if a table exists in the SQLite database.

    Args:
        conn: SQLite database connection
        table_name: Name of the table to check

    Returns:
        Boolean indicating if the table exists
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None

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

    if not rows:
        # Return empty DataFrame with proper schema
        data = {col: [] for col in column_names}
        return pl.DataFrame(data)

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if col_name in ("rowid", "sqlmodded", "alib_rowid"):
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

# ---------- Contributor Extraction Functions ----------

def extract_all_contributors(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Extract all contributors from the alib table across multiple contributor columns,
    creating a consolidated list of unique contributors.

    This function:
    - Pulls data from all contributor fields (artist, albumartist, composer, etc.)
    - Handles delimited entries (split on double backslash \\\\)
    - Creates lowercase versions for matching
    - Removes duplicates and null/empty values
    - Creates the _TMP_distinct_contributors table

    Args:
        conn: SQLite database connection

    Returns:
        Polars DataFrame with distinct contributors and their lowercase versions
    """
    logging.info("Extracting contributors from all contributor columns...")
    
    # Load all contributor data from alib
    contributors_df = sqlite_to_polars(
        conn,
        f"""
        SELECT rowid,
               {', '.join(CONTRIBUTOR_COLUMNS)}
        FROM alib
        WHERE {' OR '.join([f'{col} IS NOT NULL' for col in CONTRIBUTOR_COLUMNS])}
        ORDER BY rowid
        """,
        id_column="rowid"
    )
    
    if contributors_df.height == 0:
        logging.warning("No contributor data found in alib table")
        return pl.DataFrame({"contributor": [], "lcontributor": []})
    
    # Extract and consolidate all contributors
    all_contributors = []
    
    for col in CONTRIBUTOR_COLUMNS:
        # Get non-null values from this column
        col_contributors = (
            contributors_df
            .select(col)
            .filter(pl.col(col).is_not_null())
            .filter(pl.col(col).str.strip_chars() != "")
        )
        
        if col_contributors.height == 0:
            continue
            
        # Handle delimited entries (split on double backslash)
        for row in col_contributors.iter_rows():
            contributor_field = row[0]
            if contributor_field and '\\\\' in contributor_field:
                # Split on double backslash and process each part
                parts = [part.strip() for part in contributor_field.split('\\\\')]
                all_contributors.extend([part for part in parts if part])
            elif contributor_field:
                all_contributors.append(contributor_field.strip())
    
    if not all_contributors:
        logging.warning("No contributors found after processing")
        return pl.DataFrame({"contributor": [], "lcontributor": []})
    
    # Create DataFrame with unique contributors and lowercase versions
    contributors_df = pl.DataFrame({"contributor": all_contributors}).unique()
    
    # Add lowercase column for matching
    contributors_df = contributors_df.with_columns([
        pl.col("contributor").str.to_lowercase().alias("lcontributor")
    ])
    
    # Filter out empty or null values
    contributors_df = contributors_df.filter(
        (pl.col("contributor").is_not_null()) &
        (pl.col("contributor").str.strip_chars() != "")
    )
    
    logging.info(f"Extracted {contributors_df.height} distinct contributors")
    return contributors_df

def create_distinct_contributors_table(conn: sqlite3.Connection, contributors_df: pl.DataFrame) -> None:
    """
    Create or update the _TMP_distinct_contributors table with extracted contributor data.

    Args:
        conn: SQLite database connection
        contributors_df: DataFrame containing distinct contributors
    """
    cursor = conn.cursor()
    
    # Create table structure
    cursor.execute("DROP TABLE IF EXISTS _TMP_distinct_contributors")
    cursor.execute("""
        CREATE TABLE _TMP_distinct_contributors (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            contributor TEXT UNIQUE,
            lcontributor TEXT
        )
    """)
    
    # Convert to pandas for database insertion (more efficient for bulk inserts)
    contributors_pandas = contributors_df.to_pandas()
    contributors_pandas.to_sql('_TMP_distinct_contributors', conn, if_exists='append', index=False)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tmp_contributors_lower ON _TMP_distinct_contributors(lcontributor)")
    conn.commit()
    
    logging.info(f"Created _TMP_distinct_contributors table with {contributors_df.height} records")

# ---------- String Grouper Analysis Functions ----------

def get_previously_processed_contributors(conn: sqlite3.Connection) -> Set[Tuple[str, str]]:
    """
    Get all previously processed contributor pairs from reference tables to avoid
    re-processing entries that have already been evaluated.

    Args:
        conn: SQLite database connection

    Returns:
        Set of tuples (current_val, replacement_val) that have been previously processed
    """
    processed_pairs = set()
    
    # Get processed entries from _REF_vetted_contributors
    if table_exists(conn, '_REF_vetted_contributors'):
        vetted_df = sqlite_to_polars(
            conn,
            "SELECT current_val FROM _REF_vetted_contributors"
        )
        # Add as single-item tuples (current_val as both left and right for filtering)
        for row in vetted_df.iter_rows():
            current_val = row[0]
            if current_val:
                processed_pairs.add((current_val, current_val))
    
    # Get processed entries from _REF_contributors_workspace
    if table_exists(conn, '_REF_contributors_workspace'):
        workspace_df = sqlite_to_polars(
            conn,
            """
            SELECT current_val, replacement_val 
            FROM _REF_contributors_workspace 
            WHERE status IN (0, 1)
            """
        )
        for row in workspace_df.iter_rows():
            current_val, replacement_val = row
            if current_val and replacement_val:
                processed_pairs.add((current_val, replacement_val))
    
    logging.info(f"Found {len(processed_pairs)} previously processed contributor pairs")
    return processed_pairs

def perform_string_grouper_analysis(contributors_df: pl.DataFrame, processed_pairs: Set[Tuple[str, str]]) -> pl.DataFrame:
    """
    Perform string-grouper similarity analysis on contributors to identify potential duplicates.

    Args:
        contributors_df: DataFrame with distinct contributors
        processed_pairs: Set of previously processed contributor pairs to exclude

    Returns:
        DataFrame with similarity matches that haven't been previously processed
    """
    logging.info(f"Running string-grouper analysis with similarity threshold {SIMILARITY_THRESHOLD}")
    
    # Convert to pandas for string_grouper compatibility
    contributors_pandas = contributors_df.to_pandas()
    
    # Perform string matching
    matches = match_strings(contributors_pandas['contributor'], min_similarity=SIMILARITY_THRESHOLD)
    
    # Filter out exact matches (we only want potential duplicates)
    similarities = matches[matches['left_contributor'] != matches['right_contributor']].copy()
    
    if similarities.empty:
        logging.info("No similar contributors found")
        return pl.DataFrame({
            "left_contributor": [],
            "right_contributor": [],
            "similarity": []
        })
    
    logging.info(f"Found {len(similarities)} potential similarity matches before filtering")
    
    # Filter out previously processed pairs
    def is_processed_pair(left, right):
        return (left, right) in processed_pairs or (right, left) in processed_pairs or (left, left) in processed_pairs or (right, right) in processed_pairs
    
    similarities = similarities[
        ~similarities.apply(lambda row: is_processed_pair(row['left_contributor'], row['right_contributor']), axis=1)
    ].copy()
    
    logging.info(f"After filtering previously processed entries: {len(similarities)} matches remain")
    
    # Convert back to Polars
    if not similarities.empty:
        return pl.from_pandas(similarities)
    else:
        return pl.DataFrame({
            "left_contributor": [],
            "right_contributor": [],
            "similarity": []
        })

def create_contributor_workspace_entries(similarities_df: pl.DataFrame) -> pl.DataFrame:
    """
    Create entries for the _REF_contributors_workspace table with proper sorting columns.
    
    Moves "the" to the end of strings for better sorting (e.g., "The Beatles" becomes "beatles, the").

    Args:
        similarities_df: DataFrame with similarity matches

    Returns:
        DataFrame formatted for _REF_contributors_workspace insertion
    """
    if similarities_df.height == 0:
        return pl.DataFrame({
            "current_val": [],
            "replacement_val": [],
            "status": [],
            "cv_sort": [],
            "rv_sort": []
        })
    
    def move_the_to_end(name: str) -> str:
        """Move 'the' from beginning to end for sorting purposes."""
        if name and name.lower().startswith('the '):
            return name[4:].lower() + ', the'
        return name.lower()
    
    # Create workspace entries
    workspace_df = similarities_df.with_columns([
        pl.col("left_contributor").alias("current_val"),
        pl.col("right_contributor").alias("replacement_val"),
        pl.lit(None, dtype=pl.Int64).alias("status"),
        pl.col("left_contributor").map_elements(
            move_the_to_end, return_dtype=pl.Utf8
        ).alias("cv_sort"),
        pl.col("right_contributor").map_elements(
            move_the_to_end, return_dtype=pl.Utf8
        ).alias("rv_sort")
    ]).select([
        "current_val", "replacement_val", "status", "cv_sort", "rv_sort"
    ]).sort(["cv_sort", "rv_sort"])
    
    return workspace_df

# ---------- Output Functions ----------

def save_results_to_csv(similarities_df: pl.DataFrame, filename: str) -> None:
    """
    Save similarity results to CSV file for manual review.

    Args:
        similarities_df: DataFrame with similarity matches
        filename: Output CSV filename
    """
    if similarities_df.height == 0:
        logging.info("No results to save to CSV")
        return
    
    similarities_df.write_csv(filename, separator='|')
    logging.info(f"Saved {similarities_df.height} similarity matches to {filename}")

def save_results_to_database(conn: sqlite3.Connection, similarities_df: pl.DataFrame, table_name: str) -> None:
    """
    Save similarity results to database table for further analysis.

    Args:
        conn: SQLite database connection
        similarities_df: DataFrame with similarity matches
        table_name: Name of the output table
    """
    cursor = conn.cursor()
    
    # Drop existing table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    if similarities_df.height == 0:
        # Create empty table structure
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                left_contributor TEXT,
                right_contributor TEXT,
                similarity REAL
            )
        """)
        conn.commit()
        logging.info(f"Created empty {table_name} table")
        return
    
    # Convert to pandas and save
    similarities_pandas = similarities_df.to_pandas()
    similarities_pandas.to_sql(table_name, conn, if_exists='replace', index=True)
    
    conn.commit()
    logging.info(f"Saved {similarities_df.height} similarity matches to {table_name} table")

def update_contributors_workspace(conn: sqlite3.Connection, workspace_df: pl.DataFrame) -> None:
    """
    Update the _REF_contributors_workspace table with new entries for manual evaluation.

    Args:
        conn: SQLite database connection
        workspace_df: DataFrame with workspace entries to add
    """
    if workspace_df.height == 0:
        logging.info("No new entries to add to contributors workspace")
        return
    
    cursor = conn.cursor()
    
    # Create workspace table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _REF_contributors_workspace (
            current_val TEXT,
            replacement_val TEXT,
            status INTEGER,
            cv_sort TEXT,
            rv_sort TEXT,
            notes TEXT,
            PRIMARY KEY (current_val, replacement_val)
        )
    """)
    
    # Insert only new entries (avoid duplicates)
    workspace_pandas = workspace_df.to_pandas()
    
    # Use INSERT OR IGNORE to avoid constraint violations
    for _, row in workspace_pandas.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO _REF_contributors_workspace 
            (current_val, replacement_val, status, cv_sort, rv_sort, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row['current_val'], 
            row['replacement_val'], 
            row['status'], 
            row['cv_sort'], 
            row['rv_sort'],
            None
        ))
    
    conn.commit()
    added_count = cursor.rowcount
    logging.info(f"Added {added_count} new entries to _REF_contributors_workspace")

# ---------- Main Execution Function ----------

def main():
    """
    Main execution function that orchestrates the string-grouper similarity analysis process.

    Process flow:
    1. Check database connectivity
    2. Extract all contributors from alib table
    3. Create/update _TMP_distinct_contributors table
    4. Get previously processed contributor pairs
    5. Perform string-grouper similarity analysis
    6. Filter out previously processed entries
    7. Save results to CSV and database table
    8. Update _REF_contributors_workspace for manual evaluation
    """
    
    logging.info(f"Starting string-grouper similarity analysis")
    logging.info(f"Connecting to database: {DB_PATH}")

    if not os.path.exists(DB_PATH):
        logging.error(f"Database file does not exist: {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    try:
        # Step 1: Extract all contributors
        contributors_df = extract_all_contributors(conn)
        
        if contributors_df.height == 0:
            logging.warning("No contributors found, exiting")
            return
        
        # Step 2: Create distinct contributors table
        create_distinct_contributors_table(conn, contributors_df)
        
        # Step 3: Get previously processed contributor pairs
        processed_pairs = get_previously_processed_contributors(conn)
        
        # Step 4: Perform string-grouper analysis
        similarities_df = perform_string_grouper_analysis(contributors_df, processed_pairs)
        
        # Step 5: Save results
        save_results_to_csv(similarities_df, OUTPUT_CSV)
        save_results_to_database(conn, similarities_df, OUTPUT_TABLE)
        
        # Step 6: Create workspace entries for manual evaluation
        workspace_df = create_contributor_workspace_entries(similarities_df)
        update_contributors_workspace(conn, workspace_df)
        
        if similarities_df.height > 0:
            logging.info(f"Analysis complete. Found {similarities_df.height} potential contributor matches for review.")
            logging.info(f"Results saved to {OUTPUT_CSV} and {OUTPUT_TABLE} table.")
            logging.info(f"New entries added to _REF_contributors_workspace for manual evaluation.")
        else:
            logging.info("Analysis complete. No new potential matches found.")
            
    except Exception as e:
        logging.error(f"Error during analysis: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
