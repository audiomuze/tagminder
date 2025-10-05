import polars as pl
import sqlite3
import logging
import time
import os

def setup_logging():
    """Set up logging for the process"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('musicbrainz_processing.log'),
            logging.StreamHandler()
        ]
    )

def create_lazy_scan(db_path: str = "dbtemplate.db") -> pl.LazyFrame:
    """
    Create a lazy scan of the SQLite table
    """
    try:
        logging.info("Creating lazy scan of SQLite table...")

        # Use read_database_uri for URI-style connections
        df = pl.read_database_uri(
            query="SELECT mbid, entity, lentity, disambiguated, genre, styles FROM _REF_mb_disambiguated",
            uri=f"sqlite:///{db_path}",
            schema_overrides={
                "mbid": pl.String,
                "entity": pl.String,
                "lentity": pl.String,
                "disambiguated": pl.String,
                "genre": pl.String,
                "styles": pl.String
            }
        )

        logging.info(f"Data loaded successfully - {len(df)} rows")
        return df.lazy()

    except Exception as e:
        logging.error(f"Error reading from database: {e}")
        logging.info("Trying direct SQLite connection...")

        # Fallback to direct SQLite connection
        try:
            conn = sqlite3.connect(db_path)
            df = pl.read_database(
                query="SELECT mbid, entity, lentity, disambiguated, genre, styles FROM _REF_mb_disambiguated",
                connection=conn,
                schema_overrides={
                    "mbid": pl.String,
                    "entity": pl.String,
                    "lentity": pl.String,
                    "disambiguated": pl.String,
                    "genre": pl.String,
                    "styles": pl.String
                }
            )
            conn.close()
            logging.info(f"Fallback successful - {len(df)} rows loaded")
            return df.lazy()
        except Exception as e2:
            logging.error(f"Fallback also failed: {e2}")
            raise

def find_namesakes_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Find namesakes using lazy evaluation for optimal performance
    """
    try:
        logging.info("Building lazy query plan for namesakes...")

        # First, get duplicate lentities - collect this part to get actual values
        duplicate_lentities_df = (
            lf.group_by("lentity")
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)
            .select("lentity")
            .collect()
        )
        
        duplicate_lentities = duplicate_lentities_df["lentity"].to_list()

        # Then filter for namesakes (duplicates with null disambiguated field)
        namesakes_lf = lf.filter(
            pl.col("lentity").is_in(duplicate_lentities) &
            pl.col("disambiguated").is_null()
        )

        logging.info("Lazy query plan built successfully")
        return namesakes_lf

    except Exception as e:
        logging.error(f"Error building lazy query: {e}")
        raise

def process_namesakes_efficiently(namesakes_lf: pl.LazyFrame, db_path: str = "dbtemplate.db"):
    """
    Process namesakes using efficient execution
    """
    conn = None
    try:
        logging.info("Executing query to materialize namesakes...")
        start_time = time.time()

        # Collect the results
        namesakes_df = namesakes_lf.collect()
        materialize_time = time.time() - start_time

        logging.info(f"Materialized {len(namesakes_df)} namesakes in {materialize_time:.2f} seconds")

        if len(namesakes_df) == 0:
            logging.info("No namesakes found to process")
            return

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Drop existing table if it exists
        logging.info("Dropping existing musicbrainz_namesakes table...")
        cursor.execute("DROP TABLE IF EXISTS musicbrainz_namesakes")

        # Create new table
        logging.info("Creating new musicbrainz_namesakes table...")
        cursor.execute("""
        CREATE TABLE musicbrainz_namesakes (
            mbid TEXT PRIMARY KEY,
            entity TEXT,
            lentity TEXT,
            disambiguated TEXT,
            genre TEXT,
            styles TEXT
        )
        """)

        # Insert namesakes using direct SQL execution
        logging.info("Inserting namesakes into database...")
        insert_start = time.time()

        # Use direct SQL insertion instead of write_database
        insert_sql = """
        INSERT INTO musicbrainz_namesakes (mbid, entity, lentity, disambiguated, genre, styles)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        batch_size = 10000
        inserted_count = 0
        
        for i in range(0, len(namesakes_df), batch_size):
            batch = namesakes_df.slice(i, batch_size)
            records = batch.rows()
            cursor.executemany(insert_sql, records)
            inserted_count += len(batch)
        
        insert_time = time.time() - insert_start
        logging.info(f"Inserted {inserted_count} records in {insert_time:.2f} seconds")

        # Delete namesakes from original table
        logging.info("Deleting namesakes from original table...")
        delete_start = time.time()

        uuids_to_delete = namesakes_df["mbid"].to_list()
        if uuids_to_delete:
            # Delete in batches to avoid SQL parameter limits
            batch_size = 10000
            deleted_count = 0

            for i in range(0, len(uuids_to_delete), batch_size):
                batch = uuids_to_delete[i:i + batch_size]
                placeholders = ','.join(['?'] * len(batch))
                cursor.execute(f"DELETE FROM _REF_mb_disambiguated WHERE mbid IN ({placeholders})", batch)
                deleted_count += cursor.rowcount

            delete_time = time.time() - delete_start
            logging.info(f"Deleted {deleted_count} records in {delete_time:.2f} seconds")

        conn.commit()
        logging.info("Transaction committed successfully")

    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error processing namesakes: {e}")
        raise
    finally:
        if conn:
            conn.close()

def verify_processing(db_path: str = "dbtemplate.db"):
    """
    Verify the processing was successful
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM _REF_mb_disambiguated")
        original_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM musicbrainz_namesakes")
        namesakes_count = cursor.fetchone()[0]

        cursor.execute("""
        SELECT COUNT(*) FROM _REF_mb_disambiguated
        WHERE mbid IN (SELECT mbid FROM musicbrainz_namesakes)
        """)
        remaining_namesakes = cursor.fetchone()[0]

        logging.info(f"Verification results:")
        logging.info(f"  Records in original table: {original_count}")
        logging.info(f"  Records in namesakes table: {namesakes_count}")
        logging.info(f"  Namesakes remaining in original table: {remaining_namesakes}")

        if remaining_namesakes == 0:
            logging.info("✓ Verification PASSED: No namesakes remain in original table")
        else:
            logging.warning(f"✗ Verification FAILED: {remaining_namesakes} namesakes remain in original table")

        conn.close()

    except Exception as e:
        logging.error(f"Error during verification: {e}")

def process_musicbrainz_namesakes_lazy(db_path: str = "dbtemplate.db"):
    """
    Main function using lazy processing for optimal performance
    """
    start_time = time.time()

    try:
        setup_logging()
        logging.info("Starting MusicBrainz namesakes processing with lazy evaluation...")

        # Create lazy scan
        lf = create_lazy_scan(db_path)

        # Build lazy query for namesakes
        namesakes_lf = find_namesakes_lazy(lf)

        # Execute and process
        process_namesakes_efficiently(namesakes_lf, db_path)

        # Verify processing
        verify_processing(db_path)

        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f"Processing completed in {total_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise

def fallback_eager_approach(db_path: str = "dbtemplate.db"):
    """
    Fallback approach using eager evaluation
    """
    try:
        setup_logging()
        logging.info("Starting fallback eager approach...")

        # Connect directly to SQLite
        conn = sqlite3.connect(db_path)

        # Load data without type inference
        df = pl.read_database(
            query="SELECT mbid, entity, lentity, disambiguated, genre, styles FROM _REF_mb_disambiguated",
            connection=conn,
            schema_overrides={
                "mbid": pl.String,
                "entity": pl.String,
                "lentity": pl.String,
                "disambiguated": pl.String,
                "genre": pl.String,
                "styles": pl.String
            }
        )

        conn.close()
        logging.info(f"Loaded {len(df)} rows")

        # Find duplicate lentities
        duplicate_lentities = (
            df.group_by("lentity")
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 1)["lentity"]
        ).to_list()

        # Filter for namesakes
        namesakes_df = df.filter(
            (pl.col("lentity").is_in(duplicate_lentities)) &
            (pl.col("disambiguated").is_null())
        )

        logging.info(f"Found {len(namesakes_df)} namesakes")

        # Process namesakes
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS musicbrainz_namesakes")
        cursor.execute("""
        CREATE TABLE musicbrainz_namesakes (
            mbid TEXT PRIMARY KEY,
            entity TEXT,
            lentity TEXT,
            disambiguated TEXT,
            genre TEXT,
            styles TEXT
        )
        """)

        # Insert namesakes using direct SQL
        if len(namesakes_df) > 0:
            insert_sql = """
            INSERT INTO musicbrainz_namesakes (mbid, entity, lentity, disambiguated, genre, styles)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            batch_size = 10000
            for i in range(0, len(namesakes_df), batch_size):
                batch = namesakes_df.slice(i, batch_size)
                records = batch.rows()
                cursor.executemany(insert_sql, records)

            # Delete from original
            uuids_to_delete = namesakes_df["mbid"].to_list()
            if uuids_to_delete:
                batch_size = 10000
                for i in range(0, len(uuids_to_delete), batch_size):
                    batch = uuids_to_delete[i:i + batch_size]
                    placeholders = ','.join(['?'] * len(batch))
                    cursor.execute(f"DELETE FROM _REF_mb_disambiguated WHERE mbid IN ({placeholders})", batch)

        conn.commit()
        conn.close()

        verify_processing(db_path)
        logging.info("Fallback approach completed successfully")

    except Exception as e:
        logging.error(f"Fallback approach failed: {e}")
        raise

if __name__ == "__main__":
    db_path = "/tmp/amg/dbtemplate.db"

    # Check if database file exists first
    if not os.path.exists(db_path):
        print(f"ERROR: Database file not found: {db_path}")
        print("Please check the path and ensure the database file exists.")

        # Look for database files in common locations
        possible_paths = [
            "dbtemplate.db",
            "./dbtemplate.db",
            "../dbtemplate.db",
            "/tmp/dbtemplate.db"
        ]

        print("\nLooking for database files in common locations:")
        for path in possible_paths:
            if os.path.exists(path):
                print(f"  Found: {path}")
                response = input(f"Use this database file? (y/n): ")
                if response.lower() == 'y':
                    db_path = path
                    break
        else:
            print("No database files found. Please provide the correct path.")
            exit(1)

    try:
        # Try main approach
        process_musicbrainz_namesakes_lazy(db_path)
    except Exception as e:
        logging.warning(f"Main approach failed: {e}")
        logging.info("Trying fallback approach...")
        try:
            fallback_eager_approach(db_path)
        except Exception as e2:
            logging.error(f"All approaches failed. Last error: {e2}")
            raise