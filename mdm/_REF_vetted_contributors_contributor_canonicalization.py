import polars as pl
import sqlite3
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DB_PATH = "/tmp/amg/dbtemplate.db"
DELIMITER = "\\\\"


def check_database_exists(db_path: str) -> bool:
    """
    Verify database and required tables exist.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        True if all checks pass, False otherwise
    """
    if not Path(db_path).exists():
        logger.error(f"Database not found: {db_path}")
        return False
    
    required_tables = [
        '_REF_vetted_contributors',
        '_REF_mb_disambiguated',
        'allmusic_reference_data'
    ]
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        missing_tables = set(required_tables) - existing_tables
        if missing_tables:
            logger.error(f"Missing required tables: {missing_tables}")
            return False
            
        logger.info(f"Database validation successful. All required tables found.")
        return True
        
    except Exception as e:
        logger.error(f"Error validating database: {e}")
        return False


def load_data(db_path: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Load data from SQLite tables with explicit string typing.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Tuple of (vetted_contributors, mb_disambiguated, allmusic_reference)
    """
    conn = sqlite3.connect(db_path)
    
    # Load vetted contributors with null status
    logger.info("Loading _REF_vetted_contributors...")
    vetted_query = """
        SELECT rowid, replacement_val, status, source 
        FROM _REF_vetted_contributors 
        WHERE status IS NULL
    """
    vetted = pl.read_database(
        vetted_query,
        connection=conn,
        schema_overrides={'replacement_val': pl.String, 'status': pl.String, 'source': pl.String}
    )
    logger.info(f"Loaded {len(vetted)} rows from _REF_vetted_contributors")
    
    # Load MusicBrainz disambiguation data
    logger.info("Loading _REF_mb_disambiguated...")
    mb_query = "SELECT entity, lentity FROM _REF_mb_disambiguated"
    mb_disambiguated = pl.read_database(
        mb_query,
        connection=conn,
        schema_overrides={'entity': pl.String, 'lentity': pl.String}
    )
    logger.info(f"Loaded {len(mb_disambiguated)} rows from _REF_mb_disambiguated")
    
    # Load AllMusic reference data
    logger.info("Loading allmusic_reference_data...")
    allmusic_query = "SELECT allmusic_artist FROM allmusic_reference_data"
    allmusic = pl.read_database(
        allmusic_query,
        connection=conn,
        schema_overrides={'allmusic_artist': pl.String}
    )
    logger.info(f"Loaded {len(allmusic)} rows from allmusic_reference_data")
    
    conn.close()
    return vetted, mb_disambiguated, allmusic


def process_combined_replacements(
    df: pl.DataFrame,
    mb_reference: pl.DataFrame,
    allmusic_reference: pl.DataFrame
) -> pl.DataFrame:
    """
    Vectorized processing with cascading lookup: MusicBrainz first, then AllMusic.
    
    This function:
    1. Splits delimited replacement_val strings into lists
    2. For each item, checks MusicBrainz first, then AllMusic if no match
    3. Replaces items with canonical forms where case differs
    4. Sets status='99' where all items matched (in either source)
    5. Sets source based on which references were used:
       - '_REF_mb_disambiguated' if only MB used
       - 'allmusic_reference_data' if only AllMusic used
       - '_REF_mb_disambiguated;allmusic_reference_data' if both used
    
    Args:
        df: DataFrame with replacement_val column
        mb_reference: MusicBrainz reference DataFrame
        allmusic_reference: AllMusic reference DataFrame
        
    Returns:
        DataFrame with updated replacement_val, status, and source columns
    """
    logger.info(f"Starting combined matching pass on {len(df)} rows...")
    
    # Create lookup tables for both references
    mb_lookup = (
        mb_reference
        .with_columns(pl.col('entity').str.to_lowercase().alias('lower_key'))
        .group_by('lower_key')
        .agg(pl.col('entity').first())
        .select(['lower_key', 'entity'])
    )
    logger.info(f"Created MusicBrainz lookup with {len(mb_lookup)} entries")
    
    allmusic_lookup = (
        allmusic_reference
        .with_columns(pl.col('allmusic_artist').str.to_lowercase().alias('lower_key'))
        .group_by('lower_key')
        .agg(pl.col('allmusic_artist').first())
        .select(['lower_key', 'allmusic_artist'])
    )
    logger.info(f"Created AllMusic lookup with {len(allmusic_lookup)} entries")
    
    # Split replacement_val into lists of contributors
    df = df.with_columns(
        pl.col('replacement_val')
        .str.split(DELIMITER)
        .alias('contributors')
    )
    
    # Explode for item-level processing
    exploded = (
        df
        .select(['rowid', 'contributors'])
        .explode('contributors')
        .with_columns(
            pl.col('contributors').str.to_lowercase().alias('lower_contrib')
        )
    )
    
    # First, try to match against MusicBrainz
    exploded = (
        exploded
        .join(mb_lookup, left_on='lower_contrib', right_on='lower_key', how='left')
        .rename({'entity': 'mb_canonical'})
    )
    
    # Then, try to match against AllMusic (for items not found in MB)
    exploded = (
        exploded
        .join(allmusic_lookup, left_on='lower_contrib', right_on='lower_key', how='left')
        .rename({'allmusic_artist': 'allmusic_canonical'})
    )
    
    # Determine which source matched and the canonical form to use
    exploded = exploded.with_columns([
        # Determine which source(s) matched
        pl.col('mb_canonical').is_not_null().alias('mb_matched'),
        pl.col('allmusic_canonical').is_not_null().alias('allmusic_matched'),
        
        # Choose canonical form: prefer MusicBrainz, fall back to AllMusic
        pl.coalesce([pl.col('mb_canonical'), pl.col('allmusic_canonical')]).alias('canonical'),
        
        # Track if either matched
        (pl.col('mb_canonical').is_not_null() | pl.col('allmusic_canonical').is_not_null()).alias('matched')
    ])
    
    # Replace contributors with canonical form if case differs
    exploded = exploded.with_columns(
        pl.when(pl.col('matched') & (pl.col('contributors') != pl.col('canonical')))
        .then(pl.col('canonical'))
        .otherwise(pl.col('contributors'))
        .alias('final_contributor')
    )
    
    # Aggregate back: check if all items matched and which sources were used
    aggregated = (
        exploded
        .group_by('rowid')
        .agg([
            pl.col('final_contributor').str.join(DELIMITER).alias('new_replacement_val'),
            pl.col('matched').all().alias('all_matched'),
            pl.col('mb_matched').any().alias('used_mb'),
            pl.col('allmusic_matched').any().alias('used_allmusic')
        ])
    )
    
    # Determine source value based on which references were used
    aggregated = aggregated.with_columns(
        pl.when(pl.col('used_mb') & pl.col('used_allmusic'))
        .then(pl.lit('_REF_mb_disambiguated;allmusic_reference_data'))
        .when(pl.col('used_mb'))
        .then(pl.lit('_REF_mb_disambiguated'))
        .when(pl.col('used_allmusic'))
        .then(pl.lit('allmusic_reference_data'))
        .otherwise(pl.lit(None))
        .alias('source_value')
    )
    
    # Join back with original dataframe
    result = (
        df
        .join(aggregated, on='rowid', how='left')
        .with_columns([
            pl.col('new_replacement_val').alias('replacement_val'),
            pl.when(pl.col('all_matched'))
            .then(pl.lit('99'))
            .otherwise(pl.col('status'))
            .alias('status'),
            pl.when(pl.col('all_matched'))
            .then(pl.col('source_value'))
            .otherwise(pl.col('source'))
            .alias('source')
        ])
        .drop(['contributors', 'new_replacement_val', 'all_matched', 'used_mb', 'used_allmusic', 'source_value'])
    )
    
    changes = (result['replacement_val'] != df['replacement_val']).sum()
    status_updates = (result['status'] == '99').sum()
    mb_only = (result['source'] == '_REF_mb_disambiguated').sum()
    allmusic_only = (result['source'] == 'allmusic_reference_data').sum()
    both = (result['source'] == '_REF_mb_disambiguated;allmusic_reference_data').sum()
    
    logger.info(f"Combined pass complete: {changes} values changed, {status_updates} statuses set to '99'")
    logger.info(f"  - MusicBrainz only: {mb_only} rows")
    logger.info(f"  - AllMusic only: {allmusic_only} rows")
    logger.info(f"  - Both sources: {both} rows")
    
    return result


def write_updates(db_path: str, df: pl.DataFrame, original_df: pl.DataFrame):
    """
    Write updated rows back to database.
    
    Only writes rows where replacement_val, status, or source has changed.
    
    Args:
        db_path: Path to SQLite database
        df: DataFrame with updated values
        original_df: Original DataFrame for comparison
    """
    # Identify changed rows
    changed_mask = (
        (df['replacement_val'] != original_df['replacement_val']) |
        (df['status'] != original_df['status']) |
        (df['source'] != original_df['source'])
    )
    changed_rows = df.filter(changed_mask)
    
    if len(changed_rows) == 0:
        logger.info("No changes to write to database")
        return
    
    logger.info(f"Writing {len(changed_rows)} updated rows to database...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Prepare update statements
    for row in changed_rows.iter_rows(named=True):
        cursor.execute(
            """
            UPDATE _REF_vetted_contributors 
            SET replacement_val = ?, status = ?, source = ?
            WHERE rowid = ?
            """,
            (row['replacement_val'], row['status'], row['source'], row['rowid'])
        )
    
    conn.commit()
    conn.close()
    logger.info("Database updates committed successfully")


def main():
    """Main execution flow for contributor canonicalization."""
    logger.info("=" * 70)
    logger.info("Starting Contributor Name Canonicalization Process")
    logger.info("=" * 70)
    
    # Validate database
    if not check_database_exists(DB_PATH):
        logger.error("Database validation failed. Exiting.")
        return
    
    # Load data
    try:
        vetted, mb_disambiguated, allmusic = load_data(DB_PATH)
        original_vetted = vetted.clone()
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return
    
    # Combined pass: Check each item against MB first, then AllMusic
    try:
        vetted = process_combined_replacements(
            vetted,
            mb_disambiguated,
            allmusic
        )
    except Exception as e:
        logger.error(f"Combined matching pass failed: {e}")
        return
    
    # Write all changes in single operation
    try:
        write_updates(DB_PATH, vetted, original_vetted)
    except Exception as e:
        logger.error(f"Database write failed: {e}")
        return
    
    logger.info("=" * 70)
    logger.info("Contributor Name Canonicalization Complete")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
