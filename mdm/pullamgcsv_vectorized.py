"""
Script Name: pullamgcsv_vectorized.py (Optimized for Large Datasets)

Purpose:
    Processes a pipe-separated CSV file containing AllMusic artist reference data and loads it
    into a SQLite database with comprehensive validation and data cleaning.

Performance optimizations:
- Batch processing for memory efficiency
- Optimal indexes for fast UPSERT operations
- SQLite performance pragmas
- Minimal memory footprint

Modified: 2025-10-25 (Large dataset optimization)
"""

import polars as pl
import sqlite3
import logging
import os
import time


def setup_logging():
    """
    Set up logging to ensure the log file exists.
    Configures error logging to allmusic_data_errors.log with timestamp format.
    """
    logging.basicConfig(
        filename="allmusic_data_errors.log",
        level=logging.ERROR,
        format="%(asctime)s - %(message)s",
    )


def create_sqlite_table(conn: sqlite3.Connection):
    """
    Create the SQLite table with the specific column schema.
    Uses composite PRIMARY KEY on (artist, allmusic_artist) for uniqueness.

    Args:
        conn: SQLite database connection object
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS allmusic_reference_data (
        csv_row_number INTEGER,
        artist TEXT NOT NULL,
        allmusic_artist TEXT,
        name_similarity TEXT,
        similarity_override TEXT,
        genre TEXT,
        styles TEXT,
        mnid TEXT,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (artist, allmusic_artist)
    )
    """

    conn.execute(create_table_sql)
    conn.commit()


def create_indexes(conn: sqlite3.Connection):
    """
    Create indexes to optimize UPSERT and query performance for large datasets.
    """
    indexes = [
        # Primary composite key index (should already exist for PRIMARY KEY)
        "CREATE INDEX IF NOT EXISTS idx_artist_allmusic ON allmusic_reference_data (artist, allmusic_artist)",
        # Individual column indexes for flexible querying
        "CREATE INDEX IF NOT EXISTS idx_artist ON allmusic_reference_data (artist)",
        "CREATE INDEX IF NOT EXISTS idx_allmusic_artist ON allmusic_reference_data (allmusic_artist)",
        # Index for the auto_set_similarity_override operation
        "CREATE INDEX IF NOT EXISTS idx_similarity_override ON allmusic_reference_data (name_similarity, similarity_override)",
        # Index for common query patterns
        "CREATE INDEX IF NOT EXISTS idx_genre ON allmusic_reference_data (genre)",
        "CREATE INDEX IF NOT EXISTS idx_mnid ON allmusic_reference_data (mnid)",
    ]

    cursor = conn.cursor()
    for index_sql in indexes:
        cursor.execute(index_sql)
    conn.commit()
    print("Performance indexes created/verified")


def configure_sqlite_performance(conn: sqlite3.Connection):
    """
    Configure SQLite for maximum performance during bulk operations.
    """
    cursor = conn.cursor()
    # Disable synchronous writes for bulk operations (we'll reset later)
    cursor.execute("PRAGMA synchronous = OFF")
    # Use memory journal for faster transactions
    cursor.execute("PRAGMA journal_mode = MEMORY")
    # Increase cache size
    cursor.execute("PRAGMA cache_size = 100000")
    # Increase page size
    cursor.execute("PRAGMA page_size = 4096")
    # Use exclusive locking mode
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE")
    # Increase temp store
    cursor.execute("PRAGMA temp_store = MEMORY")
    conn.commit()


def reset_sqlite_safety(conn: sqlite3.Connection):
    """
    Reset SQLite to safe defaults after bulk operations.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA locking_mode = NORMAL")
    conn.commit()


def upsert_rows_sql_batch(
    conn: sqlite3.Connection, new_df: pl.DataFrame, batch_size: int = 5000
):
    """
    UPSERT using SQL's INSERT OR REPLACE with COALESCE to preserve similarity_override.
    Optimized for large datasets with batch processing.

    Args:
        conn: SQLite database connection object
        new_df: Polars DataFrame containing new/updated data from CSV
        batch_size: Number of rows to process in each batch

    Returns:
        Tuple of (total_processed, skipped_count)
    """
    start_time = time.time()

    # Single-pass vectorized replacement using regex to replace all consecutive double quotes
    new_df = new_df.with_columns(
        pl.col("allmusic_artist")
        .str.replace_all(r'"{2,}', '"')
        .alias("allmusic_artist")
    )

    # Filter out rows where allmusic_artist is null or empty
    new_df_with_allmusic = new_df.filter(
        pl.col("allmusic_artist").is_not_null()
        & (pl.col("allmusic_artist").str.strip_chars().str.len_chars() > 0)
    )

    skipped_count = len(new_df) - len(new_df_with_allmusic)

    if len(new_df_with_allmusic) == 0:
        return 0, skipped_count

    # Prepare data for upsert - initialize similarity_override as empty string
    upsert_df = new_df_with_allmusic.select(
        [
            pl.col("csv_row_number").cast(pl.Int64),
            pl.col("artist").cast(pl.String),
            pl.col("allmusic_artist").cast(pl.String),
            pl.col("name_similarity").cast(pl.String),
            pl.lit("").alias("similarity_override").cast(pl.String),
            pl.col("genre").cast(pl.String),
            pl.col("styles").cast(pl.String),
            pl.col("mnid").cast(pl.String),
            pl.col("url").cast(pl.String),
        ]
    )

    # SQLite INSERT OR REPLACE (no ON CONFLICT clause needed)
    upsert_sql = """
    INSERT OR REPLACE INTO allmusic_reference_data
    (csv_row_number, artist, allmusic_artist, name_similarity, similarity_override,
     genre, styles, mnid, url)
    SELECT
        ? as csv_row_number,
        ? as artist,
        ? as allmusic_artist,
        ? as name_similarity,
        COALESCE(
            (SELECT similarity_override
             FROM allmusic_reference_data
             WHERE artist = ? AND allmusic_artist = ?),
            ?
        ) as similarity_override,
        ? as genre,
        ? as styles,
        ? as mnid,
        ? as url
    """

    cursor = conn.cursor()

    # Build parameter tuples
    rows_to_upsert = [
        (
            row[0],
            row[1],
            row[2],
            row[3],  # new values
            row[1],
            row[2],
            row[4],  # COALESCE subquery params + default sim_override
            row[5],
            row[6],
            row[7],
            row[8],
        )  # remaining new values
        for row in upsert_df.iter_rows()
    ]

    # Process in batches to avoid memory issues and show progress
    total_rows = len(rows_to_upsert)
    processed_count = 0

    print(f"Processing {total_rows:,} rows in batches of {batch_size:,}...")

    for i in range(0, total_rows, batch_size):
        batch = rows_to_upsert[i : i + batch_size]
        cursor.executemany(upsert_sql, batch)
        processed_count += len(batch)

        # Show progress
        progress = (i + len(batch)) / total_rows * 100
        elapsed = time.time() - start_time
        rows_per_sec = (i + len(batch)) / elapsed if elapsed > 0 else 0

        print(
            f"Progress: {i + len(batch):,}/{total_rows:,} rows ({progress:.1f}%) - {rows_per_sec:.0f} rows/sec"
        )

    conn.commit()

    total_time = time.time() - start_time
    print(
        f"UPSERT completed: {processed_count:,} rows in {total_time:.2f} seconds ({processed_count / total_time:.0f} rows/sec)"
    )

    return processed_count, skipped_count


def auto_set_similarity_override(conn: sqlite3.Connection):
    """
    Set similarity_override to 1 where name_similarity is 1 and similarity_override is NULL.

    Args:
        conn: SQLite database connection object

    Returns:
        Count of rows updated
    """
    cursor = conn.cursor()
    update_sql = """
    UPDATE allmusic_reference_data
    SET similarity_override = '1'
    WHERE name_similarity = '1' OR name_similarity = '1.0'
    AND (similarity_override IS NULL OR similarity_override = '')
    """
    cursor.execute(update_sql)
    conn.commit()
    updated_count = cursor.rowcount
    return updated_count


def clean_null_chars(value):
    """
    Remove null characters (hex 00) from a value.
    Handles bytes, strings, and None values.

    Args:
        value: Input value to clean

    Returns:
        Cleaned value with null characters removed
    """
    if value is None:
        return None
    elif isinstance(value, bytes):
        # Remove null bytes from binary data
        cleaned_bytes = value.replace(b"\x00", b"")
        try:
            # Try to decode as UTF-8
            return cleaned_bytes.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            # If it's not valid UTF-8, return as hex representation
            return (
                f"[BINARY: {len(cleaned_bytes)} bytes: {cleaned_bytes.hex()[:20]}...]"
            )
    elif isinstance(value, str):
        # Remove null characters from strings
        return value.replace("\x00", "")
    else:
        # Convert to string and remove nulls
        return str(value).replace("\x00", "")


def read_csv_as_text_only(input_file: str):
    """
    Read CSV file treating EVERY column as UTF-8 text with ZERO type inference.
    Enhanced to better handle binary content and clean null characters.

    Args:
        input_file: Path to the CSV file to read

    Returns:
        Polars DataFrame with all columns as strings and null characters cleaned
    """
    try:
        # Define explicit schema - ALL columns as String (UTF-8)
        schema = {
            "artist": pl.String,
            "allmusic_artist": pl.String,
            "name_similarity": pl.String,
            "genre": pl.String,
            "styles": pl.String,
            "mnid": pl.String,
            "url": pl.String,
        }

        # Read with ZERO inference and explicit UTF-8 handling
        df = pl.read_csv(
            input_file,
            separator="|",
            has_header=True,
            infer_schema_length=0,
            schema=schema,
            encoding="utf8",
            try_parse_dates=False,
            ignore_errors=True,
            null_values=[],
            low_memory=True,
            truncate_ragged_lines=False,
            skip_rows_after_header=0,
        )

        # Clean null characters from all columns
        for col in df.columns:
            df = df.with_columns(
                pl.col(col).map_elements(clean_null_chars, return_dtype=pl.String)
            )

        return df

    except Exception as e:
        logging.error(f"CSV reading error: {e}")
        print(f"Error reading CSV: {e}")
        raise


def validate_df_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """
    Vectorized validation using Polars expressions.

    Performs the following validations:
    1. Artist column is required and not empty
    2. Checks for binary content in any column
    3. Validates name_similarity is a number between 0-1
    4. Validates URL format (if present)

    Args:
        df: Polars DataFrame to validate

    Returns:
        DataFrame with validation results and error messages
    """
    # Add row numbers first
    df_with_validation = df.with_row_index("csv_row_number", offset=2)

    # Validation 1: artist column is required
    artist_valid = pl.col("artist").str.strip_chars().str.len_chars() > 0

    # Validation 2: Check if any column still contains binary content after cleaning
    def has_binary_content(col):
        return (
            pl.col(col).map_elements(
                lambda x: isinstance(x, bytes) if x is not None else False,
                return_dtype=pl.Boolean,
            )
        ).fill_null(False)

    # Check which columns have binary content
    binary_checks_per_column = {
        col: has_binary_content(col) for col in df.columns if col != "csv_row_number"
    }

    # Get binary details for each column
    def get_binary_details(col):
        """Get binary information for each column"""
        return (
            pl.col(col)
            .map_elements(
                lambda x: f"bytes_{len(x)}" if isinstance(x, bytes) else "clean",
                return_dtype=pl.String,
            )
            .fill_null("null")
        )

    binary_details_per_column = {
        col: get_binary_details(col) for col in df.columns if col != "csv_row_number"
    }

    # Combine to check if ANY column has binary content
    binary_checks = pl.any_horizontal(*binary_checks_per_column.values())

    # Create detailed binary information string
    binary_details = pl.concat_str(
        [
            pl.lit(f"{col}:") + binary_details_per_column[col] + pl.lit("; ")
            for col in binary_details_per_column.keys()
        ],
        separator="",
    ).alias("binary_details")

    # Validation 3: name_similarity should be valid number between 0-1
    def validate_similarity():
        clean_col = pl.col("name_similarity").str.replace(",", "").str.strip_chars()

        # First check if it's empty or valid number format
        is_valid_format = (
            (clean_col == "")  # Empty is OK
            | clean_col.str.contains("^[0-9.]+$")  # Valid number format
        ).fill_null(False)

        # Only try to cast to float if it's a valid format and not empty
        is_in_range = (
            (clean_col == "")  # Empty passes range check
            | (
                is_valid_format
                & clean_col.cast(pl.Float64, strict=False).is_between(0, 1)
            )
        ).fill_null(False)

        return is_in_range

    similarity_valid = validate_similarity()

    # Validation 4: URL format validation
    url_valid = (
        (pl.col("url").str.strip_chars() == "")  # Empty is OK
        | pl.col("url").str.starts_with("http://")
        | pl.col("url").str.starts_with("https://")
    ).fill_null(True)  # Null URLs are considered valid (optional field)

    # Combine all validations
    is_valid = artist_valid & ~binary_checks & similarity_valid & url_valid

    # Generate specific error messages with binary details
    error_messages = (
        pl.when(~artist_valid)
        .then(pl.lit("Artist column is required and cannot be empty"))
        .when(binary_checks)
        .then(
            pl.lit("Binary content detected after cleaning - details: ")
            + binary_details
        )
        .when(~similarity_valid)
        .then(pl.lit("Invalid name_similarity value (must be number between 0-1)"))
        .when(~url_valid)
        .then(pl.lit("Invalid URL format (must start with http:// or https://)"))
        .otherwise(pl.lit(""))
        .alias("error_message")
    ).fill_null("Unknown validation error")

    return df_with_validation.with_columns(
        [is_valid.alias("is_valid"), error_messages, binary_details]
    )


def process_csv_to_sqlite(
    input_file: str, db_name: str = "dbtemplate.db", batch_size: int = 5000
):
    """
    Process CSV file using vectorized validation and upsert valid rows into SQLite database.
    Invalid rows are logged to allmusic_data_errors.log and skipped.
    Only processes rows where allmusic_artist is not null.
    Uses SQL's native UPSERT instead of manual joins for performance.

    Args:
        input_file: Path to the CSV file to process
        db_name: SQLite database file name/path
        batch_size: Number of rows to process in each batch

    Returns:
        Tuple of (processed_count, skipped_count)
    """
    try:
        # Set up logging
        setup_logging()

        # Initialize SQLite database
        conn = sqlite3.connect(db_name)
        create_sqlite_table(conn)
        create_indexes(conn)
        configure_sqlite_performance(conn)

        # Read CSV with ZERO type inference - everything as UTF-8 text
        print(f"Reading CSV file: {input_file}")
        df = read_csv_as_text_only(input_file)

        if df is None or len(df) == 0:
            print("No data found or could not read CSV file")
            conn.close()
            return (0, 0)

        print(f"Loaded {len(df):,} rows from {input_file}")

        # Vectorized validation
        print("Validating data...")
        validated_df = validate_df_vectorized(df)

        # Separate valid and invalid rows
        valid_rows = validated_df.filter(pl.col("is_valid"))
        invalid_rows = validated_df.filter(~pl.col("is_valid"))

        print(f"Valid rows: {len(valid_rows):,}")
        print(f"Invalid rows: {len(invalid_rows):,}")

        # Log invalid rows with specific error messages
        if len(invalid_rows) > 0:
            print("Logging invalid rows...")
            for row in invalid_rows.iter_rows(named=True):
                # Create a clean row representation without validation columns
                row_data = {col: row[col] for col in df.columns}
                row_values = list(row_data.values())

                # Safely handle error_message
                error_msg = row.get("error_message", "No error message")
                if error_msg is None:
                    error_msg = "No error message"

                if "Binary content detected" in error_msg:
                    # Extract binary details from the error message
                    binary_info = (
                        error_msg.split("details: ")[-1]
                        if "details: " in error_msg
                        else "No binary details"
                    )

                    # Create a readable representation of the row
                    readable_row = []
                    for col_name, value in row_data.items():
                        if value is None:
                            readable_row.append("NULL")
                        elif isinstance(value, bytes):
                            # Show binary data as hex preview
                            hex_preview = (
                                value.hex()[:20] + "..."
                                if len(value) > 20
                                else value.hex()
                            )
                            readable_row.append(
                                f"[BINARY: {len(value)} bytes: {hex_preview}]"
                            )
                        else:
                            readable_row.append(
                                str(value)[:100] + "..."
                                if len(str(value)) > 100
                                else str(value)
                            )

                    row_string = " | ".join(readable_row)
                    logging.error(
                        f"rowid: {row['csv_row_number']}: {error_msg} - {row_string}"
                    )
                    print(
                        f"BINARY CONTENT FOUND - row {row['csv_row_number']}: {binary_info}"
                    )

                else:
                    # For non-binary errors
                    row_string = "|".join(
                        str(val) if val is not None else "" for val in row_values
                    )
                    logging.error(
                        f"rowid: {row['csv_row_number']}: {error_msg} - {row_string}"
                    )

        # Upsert valid rows into SQLite using SQL native UPSERT
        processed_count = 0
        skipped_count = 0

        if len(valid_rows) > 0:
            print("Upserting valid rows to database...")
            # Drop validation columns before upserting
            valid_rows_for_upsert = valid_rows.drop(
                ["is_valid", "error_message", "binary_details"]
            )
            processed_count, skipped_count = upsert_rows_sql_batch(
                conn, valid_rows_for_upsert, batch_size
            )
            print(f"Processed {processed_count:,} rows (inserted/updated)")
            print(f"Skipped {skipped_count:,} rows with null/empty allmusic_artist")

        # Auto-set similarity_override where name_similarity is 1
        print("Auto-setting similarity_override...")
        auto_updated = auto_set_similarity_override(conn)
        if auto_updated > 0:
            print(
                f"Auto-set similarity_override to 1 for {auto_updated:,} rows where name_similarity = 1"
            )

        # Reset to safe SQLite settings
        reset_sqlite_safety(conn)

        # Close database connection
        conn.close()

        return processed_count, skipped_count

    except Exception as e:
        logging.error(f"Processing error: {e}")
        print(f"Error processing file: {e}")
        import traceback

        traceback.print_exc()
        if "conn" in locals():
            conn.close()
        return (0, 0)


def verify_database(
    db_name: str = "dbtemplate.db",
    input_file: str = "allmusic_data.csv",
    skipped_allmusic_null_count: int = 0,
):
    """
    Verify the database was created correctly and reconcile record counts.

    Args:
        db_name: SQLite database file name/path
        input_file: Path to the original CSV file
        skipped_allmusic_null_count: Count of rows skipped due to null allmusic_artist
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Count total rows in database table
        cursor.execute("SELECT COUNT(*) FROM allmusic_reference_data")
        db_rows = cursor.fetchone()[0]

        # Count total rows in CSV (excluding header)
        csv_row_count = 0
        try:
            with open(input_file, "r", encoding="utf-8") as f:
                # Skip header and count remaining rows
                next(f)  # Skip header
                csv_row_count = sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting CSV rows: {e}")
            csv_row_count = "unknown"

        # Count error log entries (each represents one invalid row)
        error_log_count = 0
        try:
            with open("allmusic_data_errors.log", "r", encoding="utf-8") as f:
                # Count lines that contain validation errors
                error_log_count = sum(1 for line in f if "rowid:" in line)
        except FileNotFoundError:
            error_log_count = 0
        except Exception as e:
            print(f"Error reading error log: {e}")
            error_log_count = "unknown"

        print(f"\nDatabase Statistics:")
        print(f"- Total records in database              : {db_rows:,}")
        print(f"- Log entries (invalid rows)             : {error_log_count:,}")
        print(
            f"- Skipped (no allmusic_artist)           : {skipped_allmusic_null_count:,}"
        )
        print(f"- Rows in CSV (excluding header)         : {csv_row_count:,}")

        conn.close()

    except Exception as e:
        print(f"Error verifying database: {e}")


if __name__ == "__main__":
    input_file = "allmusic_data.csv"
    db_name = "dbtemplate.db"

    # Adjust batch size based on your system memory
    # Smaller = less memory usage, larger = faster processing
    batch_size = 10000  # Start with 10K, adjust based on performance

    print("Starting CSV to SQLite processing...")
    print(
        "Expected columns: artist|allmusic_artist|name_similarity|genre|styles|mnid|url"
    )
    print(
        "All columns will be stored as UTF-8 TEXT - ZERO type inference will be applied"
    )
    print("Null characters (hex 00) will be automatically stripped from all values")
    print("Using composite key (artist, allmusic_artist) for uniqueness")
    print("Using SQL native UPSERT for maximum performance")
    print("Only processing rows where allmusic_artist is not null/empty")
    print("Preserving existing similarity_override values")
    print(f"Using batch size: {batch_size:,} rows per batch")

    # Process with SQL UPSERT
    start_time = time.time()
    processed, skipped = process_csv_to_sqlite(input_file, db_name, batch_size)
    total_time = time.time() - start_time

    # Verify the database
    verify_database(db_name, input_file, skipped)

    print(f"\nProcessing completed in {total_time:.2f} seconds.")
    print(f"Database updated: {db_name}")
    print(f"Overall processing rate: {processed / total_time:.0f} rows/sec")
    print("Check allmusic_data_errors.log for any validation errors.")
