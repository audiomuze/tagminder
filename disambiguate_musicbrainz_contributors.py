import polars as pl
import sqlite3
import logging
import time
import os


def setup_logging():
    """Set up logging for the process"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("musicbrainz_sync.log"), logging.StreamHandler()],
    )


def ensure_tables_exist(db_path: str):
    """
    Ensure target tables exist, creating them if necessary
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if _REF_mb_disambiguated exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='_REF_mb_disambiguated'
        """)

        if cursor.fetchone() is None:
            logging.info("Creating _REF_mb_disambiguated table...")
            cursor.execute("""
                CREATE TABLE _REF_mb_disambiguated(
                    mbid TEXT,
                    entity TEXT,
                    updated_from_allmusic TEXT,
                    gender TEXT,
                    disambiguation TEXT,
                    lentity TEXT,
                    genre TEXT,
                    styles TEXT,
                    disambiguated TEXT
                )
            """)
            conn.commit()
            logging.info("Created _REF_mb_disambiguated table")

        # Check if musicbrainz_namesakes exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='musicbrainz_namesakes'
        """)

        if cursor.fetchone() is None:
            logging.info("Creating musicbrainz_namesakes table...")
            cursor.execute("""
                CREATE TABLE musicbrainz_namesakes(
                    mbid TEXT,
                    entity TEXT,
                    updated_from_allmusic TEXT,
                    gender TEXT,
                    disambiguation TEXT,
                    lentity TEXT,
                    genre TEXT,
                    styles TEXT,
                    disambiguated TEXT
                )
            """)
            conn.commit()
            logging.info("Created musicbrainz_namesakes table")

        conn.close()

    except Exception as e:
        logging.error(f"Error ensuring tables exist: {e}")
        raise


def load_source_data(db_path: str) -> pl.DataFrame:
    """
    Load data from musicbrainz_raw_data table including gender and disambiguation

    Returns:
        DataFrame with mbid, entity, gender, disambiguation, and lentity
    """
    try:
        logging.info("Loading data from musicbrainz_raw_data...")

        conn = sqlite3.connect(db_path)

        # Load all relevant columns from source
        df = pl.read_database(
            query="SELECT mbid, contributor, gender, disambiguation FROM musicbrainz_raw_data",
            connection=conn,
            schema_overrides={
                "mbid": pl.String,
                "contributor": pl.String,
                "gender": pl.String,
                "disambiguation": pl.String,
            },
        )

        conn.close()

        # Map contributor to entity and create lentity (lowercase entity)
        df = df.rename({"contributor": "entity"})
        df = df.with_columns([pl.col("entity").str.to_lowercase().alias("lentity")])

        logging.info(f"Loaded {len(df)} records from musicbrainz_raw_data")
        return df

    except Exception as e:
        logging.error(f"Error loading source data: {e}")
        raise


def load_existing_tables(db_path: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Load existing data from both target tables including all columns
    Returns empty DataFrames if tables are empty

    Returns:
        Tuple of (disambiguated_df, namesakes_df)
    """
    try:
        conn = sqlite3.connect(db_path)

        # Load from _REF_mb_disambiguated
        logging.info("Loading existing data from _REF_mb_disambiguated...")
        try:
            disambiguated_df = pl.read_database(
                query="SELECT mbid, entity, gender, disambiguation, lentity, genre, styles, disambiguated FROM _REF_mb_disambiguated",
                connection=conn,
                schema_overrides={
                    "mbid": pl.String,
                    "entity": pl.String,
                    "gender": pl.String,
                    "disambiguation": pl.String,
                    "lentity": pl.String,
                    "genre": pl.String,
                    "styles": pl.String,
                    "disambiguated": pl.String,
                },
            )
            logging.info(
                f"Loaded {len(disambiguated_df)} records from _REF_mb_disambiguated"
            )
        except Exception as e:
            logging.info("_REF_mb_disambiguated is empty, creating empty DataFrame")
            disambiguated_df = pl.DataFrame(
                schema={
                    "mbid": pl.String,
                    "entity": pl.String,
                    "gender": pl.String,
                    "disambiguation": pl.String,
                    "lentity": pl.String,
                    "genre": pl.String,
                    "styles": pl.String,
                    "disambiguated": pl.String,
                }
            )

        # Load from musicbrainz_namesakes
        try:
            namesakes_df = pl.read_database(
                query="SELECT mbid, entity, gender, disambiguation, lentity, genre, styles, disambiguated FROM musicbrainz_namesakes",
                connection=conn,
                schema_overrides={
                    "mbid": pl.String,
                    "entity": pl.String,
                    "gender": pl.String,
                    "disambiguation": pl.String,
                    "lentity": pl.String,
                    "genre": pl.String,
                    "styles": pl.String,
                    "disambiguated": pl.String,
                },
            )
            logging.info(
                f"Loaded {len(namesakes_df)} records from musicbrainz_namesakes"
            )
        except Exception as e:
            logging.info("musicbrainz_namesakes is empty, creating empty DataFrame")
            namesakes_df = pl.DataFrame(
                schema={
                    "mbid": pl.String,
                    "entity": pl.String,
                    "gender": pl.String,
                    "disambiguation": pl.String,
                    "lentity": pl.String,
                    "genre": pl.String,
                    "styles": pl.String,
                    "disambiguated": pl.String,
                }
            )

        conn.close()
        return disambiguated_df, namesakes_df

    except Exception as e:
        logging.error(f"Error loading existing data: {e}")
        raise


def identify_duplicate_lentities(source_df: pl.DataFrame) -> set:
    """
    Identify lentity values that appear multiple times in source

    Returns:
        Set of duplicate lentity values
    """
    logging.info("Identifying duplicate lentities in source data...")

    duplicate_lentities = (
        source_df.group_by("lentity")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
        .select("lentity")
        .to_series()
        .to_list()
    )

    logging.info(f"Found {len(duplicate_lentities)} duplicate lentities")
    return set(duplicate_lentities)


def merge_and_rebuild_tables(
    source_df: pl.DataFrame,
    existing_disambig_df: pl.DataFrame,
    existing_namesakes_df: pl.DataFrame,
    duplicate_lentities: set,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Merge source data with existing tables using vectorized operations

    Preserves manually disambiguated records (where disambiguated IS NOT NULL)
    and ensures they don't appear in musicbrainz_namesakes.

    Prioritizes source data for gender/disambiguation but preserves manually
    entered genre/styles/disambiguated values.

    Returns:
        Tuple of (new_disambiguated_df, new_namesakes_df)
    """
    logging.info("Building merged DataFrames...")

    # Get preservation list - only mbids that were manually disambiguated
    preserved_mbids = set()
    if len(existing_disambig_df) > 0:
        manually_disambiguated = existing_disambig_df.filter(
            pl.col("disambiguated").is_not_null()
        )
        preserved_mbids = set(manually_disambiguated.select("mbid").to_series())
        logging.info(
            f"Found {len(preserved_mbids)} manually disambiguated records to preserve"
        )

    # Combine all existing data to preserve metadata
    # Note: Only preserve genre/styles/disambiguated, not gender/disambiguation
    # as those should come from the source
    all_existing = pl.concat(
        [
            existing_disambig_df.select(["mbid", "genre", "styles", "disambiguated"]),
            existing_namesakes_df.select(["mbid", "genre", "styles", "disambiguated"]),
        ]
    )

    # Add columns to source data for new records
    source_with_nulls = source_df.with_columns(
        [
            pl.lit(None).cast(pl.String).alias("genre"),
            pl.lit(None).cast(pl.String).alias("styles"),
            pl.lit(None).cast(pl.String).alias("disambiguated"),
        ]
    )

    # Left join to preserve existing genre/styles/disambiguated values
    # gender and disambiguation come from source_df and will override existing values
    merged = (
        source_with_nulls.join(
            all_existing,
            on="mbid",
            how="left",
            suffix="_existing",
        )
        .with_columns(
            [
                # Use existing values for these manually maintained fields
                pl.coalesce(["genre_existing", "genre"]).alias("genre"),
                pl.coalesce(["styles_existing", "styles"]).alias("styles"),
                pl.coalesce(["disambiguated_existing", "disambiguated"]).alias(
                    "disambiguated"
                ),
            ]
        )
        .select(
            [
                "mbid",
                "entity",
                "gender",
                "disambiguation",
                "lentity",
                "genre",
                "styles",
                "disambiguated",
            ]
        )
    )

    # Add is_duplicate flag for vectorized lookup
    merged = merged.with_columns(
        [pl.col("lentity").is_in(duplicate_lentities).alias("is_duplicate")]
    )

    # Determine target table using vectorized conditional logic:
    # 1. If mbid is in preserved_mbids -> disambiguated (manually processed)
    # 2. Else if is_duplicate -> namesakes (needs manual review)
    # 3. Else -> disambiguated (unique name)
    merged = merged.with_columns(
        [
            pl.when(pl.col("mbid").is_in(preserved_mbids))
            .then(pl.lit("disambiguated"))
            .when(pl.col("is_duplicate"))
            .then(pl.lit("namesakes"))
            .otherwise(pl.lit("disambiguated"))
            .alias("target_table")
        ]
    )

    # Split into two tables based on target_table column
    new_disambiguated_df = (
        merged.filter(pl.col("target_table") == "disambiguated")
        .select(
            [
                "mbid",
                "entity",
                "gender",
                "disambiguation",
                "lentity",
                "genre",
                "styles",
                "disambiguated",
            ]
        )
        .sort("lentity")
    )

    new_namesakes_df = (
        merged.filter(pl.col("target_table") == "namesakes")
        .select(
            [
                "mbid",
                "entity",
                "gender",
                "disambiguation",
                "lentity",
                "genre",
                "styles",
                "disambiguated",
            ]
        )
        .sort("lentity")
    )

    logging.info(
        f"Built new tables: {len(new_disambiguated_df)} disambiguated, {len(new_namesakes_df)} namesakes"
    )

    return new_disambiguated_df, new_namesakes_df


def write_tables_to_database(
    db_path: str, disambiguated_df: pl.DataFrame, namesakes_df: pl.DataFrame
):
    """
    Write complete DataFrames to database tables in a single transaction
    Uses DROP/CREATE/INSERT for maximum efficiency
    """
    conn = None
    try:
        logging.info("Writing tables to database...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Drop and recreate _REF_mb_disambiguated with correct schema
        logging.info(
            f"Replacing _REF_mb_disambiguated with {len(disambiguated_df)} records..."
        )
        cursor.execute("DROP TABLE IF EXISTS _REF_mb_disambiguated")
        cursor.execute("""
        CREATE TABLE _REF_mb_disambiguated(
            mbid TEXT,
            entity TEXT,
            updated_from_allmusic TEXT,
            gender TEXT,
            disambiguation TEXT,
            lentity TEXT,
            genre TEXT,
            styles TEXT,
            disambiguated TEXT
        )
        """)

        # Create index on lentity for faster lookups
        cursor.execute("CREATE INDEX ix_lentities ON _REF_mb_disambiguated(lentity)")

        # Insert all disambiguated records in batches
        if len(disambiguated_df) > 0:
            insert_sql = """
            INSERT INTO _REF_mb_disambiguated (mbid, entity, gender, disambiguation, lentity, genre, styles, disambiguated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            batch_size = 50000
            for i in range(0, len(disambiguated_df), batch_size):
                batch = disambiguated_df.slice(i, batch_size)
                cursor.executemany(insert_sql, batch.rows())
                logging.info(
                    f"  Inserted batch {i // batch_size + 1}: {len(batch)} records"
                )

        # Drop and recreate musicbrainz_namesakes with correct schema
        logging.info(
            f"Replacing musicbrainz_namesakes with {len(namesakes_df)} records..."
        )
        cursor.execute("DROP TABLE IF EXISTS musicbrainz_namesakes")
        cursor.execute("""
        CREATE TABLE musicbrainz_namesakes(
            mbid TEXT,
            entity TEXT,
            updated_from_allmusic TEXT,
            gender TEXT,
            disambiguation TEXT,
            lentity TEXT,
            genre TEXT,
            styles TEXT,
            disambiguated TEXT
        )
        """)

        # Insert all namesakes records in batches
        if len(namesakes_df) > 0:
            insert_sql = """
            INSERT INTO musicbrainz_namesakes (mbid, entity, gender, disambiguation, lentity, genre, styles, disambiguated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            batch_size = 50000
            for i in range(0, len(namesakes_df), batch_size):
                batch = namesakes_df.slice(i, batch_size)
                cursor.executemany(insert_sql, batch.rows())
                logging.info(
                    f"  Inserted batch {i // batch_size + 1}: {len(batch)} records"
                )

        conn.commit()
        logging.info("All changes committed successfully")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error writing to database: {e}")
        raise
    finally:
        if conn:
            conn.close()


def verify_sync(db_path: str):
    """
    Verify the synchronization was successful
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM musicbrainz_raw_data")
        source_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM _REF_mb_disambiguated")
        disambig_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM musicbrainz_namesakes")
        namesakes_count = cursor.fetchone()[0]

        # Check for any records in source not in target tables
        cursor.execute("""
        SELECT COUNT(*) FROM musicbrainz_raw_data
        WHERE mbid NOT IN (
            SELECT mbid FROM _REF_mb_disambiguated
            UNION
            SELECT mbid FROM musicbrainz_namesakes
        )
        """)
        missing_count = cursor.fetchone()[0]

        # Check for any mbids that appear in both tables (should be zero)
        cursor.execute("""
        SELECT COUNT(*) FROM _REF_mb_disambiguated
        WHERE mbid IN (SELECT mbid FROM musicbrainz_namesakes)
        """)
        duplicate_count = cursor.fetchone()[0]

        logging.info(f"Verification results:")
        logging.info(f"  Records in musicbrainz_raw_data: {source_count}")
        logging.info(f"  Records in _REF_mb_disambiguated: {disambig_count}")
        logging.info(f"  Records in musicbrainz_namesakes: {namesakes_count}")
        logging.info(f"  Total in target tables: {disambig_count + namesakes_count}")
        logging.info(f"  Records from source missing in targets: {missing_count}")
        logging.info(f"  Records appearing in BOTH target tables: {duplicate_count}")

        if missing_count == 0 and duplicate_count == 0:
            logging.info(
                "Verification PASSED: All source records are in exactly one target table"
            )
        else:
            if missing_count > 0:
                logging.warning(
                    f"✗ Verification WARNING: {missing_count} source records not found in targets"
                )
            if duplicate_count > 0:
                logging.warning(
                    f"✗ Verification WARNING: {duplicate_count} mbids appear in BOTH target tables"
                )

        conn.close()

    except Exception as e:
        logging.error(f"Error during verification: {e}")


def sync_musicbrainz_raw_data(db_path: str = "dbtemplate.db"):
    """
    Main function to sync musicbrainz_raw_data into target tables
    Uses efficient DataFrame operations and bulk writes

    Preserves manually disambiguated records in _REF_mb_disambiguated
    and ensures no mbid appears in both target tables.

    Updates gender/disambiguation from source data while preserving
    manually entered genre/styles/disambiguated values.
    """
    start_time = time.time()

    try:
        setup_logging()
        logging.info(
            "Starting MusicBrainz source synchronization (optimized approach)..."
        )

        # Ensure target tables exist
        ensure_tables_exist(db_path)

        # Load all data into memory
        source_df = load_source_data(db_path)
        existing_disambig_df, existing_namesakes_df = load_existing_tables(db_path)

        # Identify duplicates
        duplicate_lentities = identify_duplicate_lentities(source_df)

        # Merge and rebuild tables using vectorized operations
        new_disambiguated_df, new_namesakes_df = merge_and_rebuild_tables(
            source_df, existing_disambig_df, existing_namesakes_df, duplicate_lentities
        )

        # Write complete tables back to database
        write_tables_to_database(db_path, new_disambiguated_df, new_namesakes_df)

        # Verify
        # verify_sync(db_path)

        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f"Synchronization completed in {total_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Synchronization failed: {e}")
        raise


if __name__ == "__main__":
    db_path = "/tmp/amg/dbtemplate.db"

    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"ERROR: Database file not found: {db_path}")
        print("Please check the path and ensure the database file exists.")
        exit(1)
    try:
        sync_musicbrainz_raw_data(db_path)
    except Exception as e:
        logging.error(f"Script failed: {e}")
        raise
