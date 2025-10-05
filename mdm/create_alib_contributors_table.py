"""
Script Name: generate_alib_contributor_names.py

Purpose:
    Creates a comprehensive table (alib_contributor_names) containing all distinct 
    contributor names found across multiple contributor fields in the alib table.
    
    Processing steps:
    - Extracts contributors from 12 different contributor columns
    - Splits delimited entries (using '\\\\' delimiter)
    - Deduplicates and sorts all contributor names
    - Cross-references with in_allmusic_reference_data to flag known artists
    
    Uses fully vectorized Polars operations for optimal performance.

Author: audiomuze
Created: 2025-10-04
"""

import polars as pl
import sqlite3
import logging
from datetime import datetime, timezone

# ---------- Configuration ----------
SCRIPT_NAME = "generate_alib_contributor_names.py"
DB_PATH = "/tmp/amg/dbtemplate.db"

# Delimiter used for splitting multiple contributors
DELIMITER = "\\\\"  # Double backslash

# Contributor columns to process
CONTRIBUTOR_COLUMNS = [
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

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_all_contributors(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Extract all contributor names from the alib table using vectorized operations.
    
    Returns:
        Polars DataFrame with a single 'contributor' column containing all 
        unique, sorted contributor names.
    """
    logging.info("Extracting contributors from alib table...")
    
    # Build the SQL query to union all contributor columns
    union_clauses = []
    for col in CONTRIBUTOR_COLUMNS:
        union_clauses.append(f"""
            SELECT DISTINCT {col} as contributor
            FROM alib
            WHERE {col} IS NOT NULL AND {col} != ''
        """)
    
    query = " UNION ALL ".join(union_clauses)
    
    # Execute query and load into Polars
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Create DataFrame with all contributors (including delimited ones)
    df = pl.DataFrame({
        "contributor": [row[0] for row in rows]
    })
    
    logging.info(f"Extracted {df.height} contributor entries (including delimited)")
    
    return df


def split_and_deduplicate_contributors(df: pl.DataFrame) -> pl.DataFrame:
    """
    Vectorized splitting of delimited contributor entries and deduplication.
    
    Args:
        df: DataFrame with 'contributor' column containing potentially delimited entries
        
    Returns:
        DataFrame with individual contributor names, deduplicated and sorted
    """
    logging.info("Splitting delimited entries and deduplicating...")
    
    # Vectorized operations:
    # 1. Split all entries on delimiter
    # 2. Explode lists into individual rows
    # 3. Strip whitespace
    # 4. Filter out empty strings
    # 5. Get unique values
    # 6. Sort
    contributors = (
        df
        .with_columns([
            pl.col("contributor").str.split(DELIMITER).alias("split_contributors")
        ])
        .explode("split_contributors")
        .select([
            pl.col("split_contributors")
            .str.strip_chars()
            .alias("contributor")
        ])
        .filter(
            (pl.col("contributor").is_not_null()) & 
            (pl.col("contributor") != "")
        )
        .unique()
        .sort("contributor")
    )
    
    logging.info(f"Found {contributors.height} distinct contributors")
    
    return contributors


def add_reference_check(
    contributors_df: pl.DataFrame, 
    conn: sqlite3.Connection
) -> pl.DataFrame:
    """
    Add a column indicating whether the contributor exists in AllMusic reference data.
    
    Uses case-insensitive matching against allmusic_reference_data.allmusic_artist.
    Filters out NULLs and empty strings from reference data.
    
    Args:
        contributors_df: DataFrame with contributor names
        conn: SQLite database connection
        
    Returns:
        DataFrame with added 'in_allmusic_reference_data' column (1 if match, 0 otherwise)
    """
    logging.info("Checking contributors against AllMusic reference data...")
    
    try:
        # Load reference data from allmusic_reference_data table
        # Filter out NULLs and empty strings in the query
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT LOWER(allmusic_artist) as lartist
            FROM allmusic_reference_data
            WHERE allmusic_artist IS NOT NULL AND allmusic_artist != ''
        """)
        
        reference_artists = [row[0] for row in cursor.fetchall()]
        
        if not reference_artists:
            logging.warning("No reference data found in allmusic_reference_data table")
            # Return DataFrame with all NULLs
            return contributors_df.with_columns([
                pl.lit(None, dtype=pl.Int64).alias("in_allmusic_reference_data")
            ])
        
        logging.info(f"Loaded {len(reference_artists)} reference artists from AllMusic")
        
        # Create reference DataFrame for efficient joining
        reference_df = pl.DataFrame({
            "lcontributor": reference_artists,
            "in_reference": [1] * len(reference_artists)
        })
        
        # Add lowercase column for matching, then left join with reference data
        result = (
            contributors_df
            .with_columns([
                pl.col("contributor").str.to_lowercase().alias("lcontributor")
            ])
            .join(
                reference_df,
                on="lcontributor",
                how="left"
            )
            .select([
                "contributor",
                pl.col("in_reference").alias("in_allmusic_reference_data")
            ])
        )
        
        matches = result.filter(pl.col("in_allmusic_reference_data") == 1).height
        logging.info(f"Found {matches} contributors matching AllMusic reference data")
        
        return result
        
    except sqlite3.Error as e:
        logging.warning(f"Could not access allmusic_reference_data table: {e}")
        logging.warning("Setting all in_allmusic_reference_data values to NULL")
        return contributors_df.with_columns([
            pl.lit(None, dtype=pl.Int64).alias("in_allmusic_reference_data")
        ])


def write_to_database(df: pl.DataFrame, conn: sqlite3.Connection) -> None:
    """
    Drop and recreate the alib_contributor_names table, then write the DataFrame to it.
    
    Args:
        df: DataFrame containing contributor names and reference flags
        conn: SQLite database connection
    """
    logging.info("Writing results to alib_contributor_names table...")
    
    cursor = conn.cursor()
    
    try:
        conn.execute("BEGIN TRANSACTION")
        
        # Drop existing table if it exists
        cursor.execute("DROP TABLE IF EXISTS alib_contributor_names")
        
        # Create new table
        cursor.execute("""
            CREATE TABLE alib_contributor_names (
                contributor TEXT PRIMARY KEY,
                in_allmusic_reference_data INTEGER DEFAULT 0
            )
        """)
        
        # Convert DataFrame to list of tuples for insertion
        records = df.select([
            "contributor",
            "in_allmusic_reference_data"
        ]).rows()
        
        # Batch insert all records
        cursor.executemany(
            "INSERT INTO alib_contributor_names VALUES (?, ?)",
            records
        )
        
        # Create index for efficient lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS ix_alib_contributor_names 
            ON alib_contributor_names(contributor)
        """)
        
        conn.commit()
        logging.info(f"Successfully wrote {len(records)} contributors to alib_contributor_names")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error writing to database: {e}")
        raise


def main():
    """
    Main execution function - orchestrates the full pipeline.
    """
    start_time = datetime.now()
    logging.info(f"Starting {SCRIPT_NAME}")
    logging.info(f"Connecting to database: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Step 1: Extract all contributors from alib table
        contributors_df = extract_all_contributors(conn)
        
        # Step 2: Split delimited entries and deduplicate
        unique_contributors_df = split_and_deduplicate_contributors(contributors_df)
        
        # Step 3: Add reference data check
        final_df = add_reference_check(unique_contributors_df, conn)
        
        # Step 4: Write to database
        write_to_database(final_df, conn)
        
        # Summary
        elapsed = datetime.now() - start_time
        logging.info(f"Processing complete in {elapsed.total_seconds():.2f} seconds")
        logging.info(f"Final contributor count: {final_df.height}")
        
    except Exception as e:
        logging.error(f"Error during execution: {e}", exc_info=True)
        raise
        
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
