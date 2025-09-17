"""
Script Name: pullamgcsv.py

Purpose:
    Processes a pipe-separated CSV file containing AllMusic artist reference data and loads it
    into a SQLite database with comprehensive validation and data cleaning.

    The script performs the following operations:
    1. Cleans up previous run artifacts (database table and error log)
    2. Reads CSV file with zero type inference, treating all columns as UTF-8 text
    3. Validates data using vectorized Polars operations for performance
    4. Handles binary content and null character removal from all fields
    5. Inserts only valid rows where allmusic_artist is not null/empty
    6. Logs validation errors with detailed information
    7. Provides database reconciliation to verify data integrity
    8. Supports both single-pass and chunked processing for large files

    Key features:
    - Uses CSV row numbers as primary keys for traceability
    - Replaces double quotes in allmusic_artist column
    - Validates name_similarity values (0-1 range)
    - Validates URL formats
    - Handles binary content detection and logging
    - Provides comprehensive error logging

    This script is part of tagminder.

Usage:
    python pullamgcsv.py [input_file] [db_name]

Arguments:
    input_file: Path to the input CSV file (defaults to "allmusic_data.csv")
    db_name: Optional path to the SQLite database file (defaults to "dbtemplate.db")

Author: audiomuze
Created: 2025-09-17
Modified: 2025-09-17
"""


import polars as pl
import sqlite3
import logging
import os

def setup_logging():
    """
    Set up logging after cleanup to ensure the log file exists.
    Configures error logging to allmusic_data_errors.log with timestamp format.
    """
    logging.basicConfig(
        filename='allmusic_data_errors.log',
        level=logging.ERROR,
        format='%(asctime)s - %(message)s'
    )

def cleanup_previous_run(db_name: str = "dbtemplate.db"):
    """
    Clean up previous run by wiping the table and error log.

    Args:
        db_name: SQLite database file name/path
    """
    try:
        # Clear error log
        if os.path.exists('allmusic_data_errors.log'):
            os.remove('allmusic_data_errors.log')
            print("Cleared previous allmusic_data_errors.log")

        # Clear database table if it exists
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='allmusic_reference_data'")
        table_exists = cursor.fetchone()

        if table_exists:
            cursor.execute("DELETE FROM allmusic_reference_data")
            print("Cleared previous data from allmusic_reference_data table")

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error during cleanup: {e}")

def create_sqlite_table(conn: sqlite3.Connection):
    """
    Create the SQLite table with the specific column schema.
    Uses csv_row_number as the primary key for traceability to source CSV.

    Args:
        conn: SQLite database connection object
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS allmusic_reference_data (
        csv_row_number INTEGER PRIMARY KEY,
        artist TEXT NOT NULL,
        allmusic_artist TEXT,
        name_similarity TEXT,
        genre TEXT,
        styles TEXT,
        mnid TEXT,
        url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    conn.execute(create_table_sql)
    conn.commit()


def insert_valid_rows(conn: sqlite3.Connection, df: pl.DataFrame):
    """
    Insert valid rows into the SQLite table using CSV row number as ID.
    Replaces all instances of '""' with '"' in allmusic_artist column.
    Only inserts rows where allmusic_artist is not null and not empty.

    Args:
        conn: SQLite database connection object
        df: Polars DataFrame containing validated data

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    # Prepare INSERT statement with csv_row_number as primary key
    insert_sql = """
    INSERT INTO allmusic_reference_data (csv_row_number, artist, allmusic_artist, name_similarity, genre, styles, mnid, url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Single-pass vectorized replacement using regex to replace all consecutive double quotes
    # This replaces any even number of consecutive double quotes with half the number of single quotes
    df = df.with_columns(
        pl.col("allmusic_artist")
        .str.replace_all(r'"{2,}', '"')
        .alias("allmusic_artist")
    )

    # Filter out rows where allmusic_artist is null or empty
    df_with_allmusic = df.filter(
        pl.col("allmusic_artist").is_not_null() &
        (pl.col("allmusic_artist").str.strip_chars().str.len_chars() > 0)
    )

    skipped_count = len(df) - len(df_with_allmusic)

    # Convert all values to strings to ensure TEXT storage
    string_df = df_with_allmusic.select([
        pl.col("original_row_number").cast(pl.Int64),
        pl.col("artist").cast(pl.String),
        pl.col("allmusic_artist").cast(pl.String),
        pl.col("name_similarity").cast(pl.String),
        pl.col("genre").cast(pl.String),
        pl.col("styles").cast(pl.String),
        pl.col("mnid").cast(pl.String),
        pl.col("url").cast(pl.String)
    ])

    # Convert Polars DataFrame to list of tuples for batch insertion
    rows_to_insert = string_df.iter_rows()

    # Batch insert
    cursor = conn.cursor()
    cursor.executemany(insert_sql, rows_to_insert)
    conn.commit()

    return cursor.rowcount, skipped_count

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
        cleaned_bytes = value.replace(b'\x00', b'')
        try:
            # Try to decode as UTF-8
            return cleaned_bytes.decode('utf-8', errors='replace')
        except UnicodeDecodeError:
            # If it's not valid UTF-8, return as hex representation
            return f"[BINARY: {len(cleaned_bytes)} bytes: {cleaned_bytes.hex()[:20]}...]"
    elif isinstance(value, str):
        # Remove null characters from strings
        return value.replace('\x00', '')
    else:
        # Convert to string and remove nulls
        return str(value).replace('\x00', '')

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
        # This completely bypasses any type inference
        schema = {
            "artist": pl.String,
            "allmusic_artist": pl.String,
            "name_similarity": pl.String,
            "genre": pl.String,
            "styles": pl.String,
            "mnid": pl.String,
            "url": pl.String
        }

        # Read with ZERO inference and explicit UTF-8 handling
        df = pl.read_csv(
            input_file,
            separator="|",
            has_header=True,
            # CRITICAL: Set to 0 to disable ALL schema inference
            infer_schema_length=0,
            # Force explicit schema - NO inference allowed
            schema=schema,
            # Explicit UTF-8 encoding
            encoding="utf8",
            # Disable ALL automatic parsing
            try_parse_dates=False,
            # Handle errors by continuing (don't crash)
            ignore_errors=True,
            # Don't treat any values as null - preserve raw data
            null_values=[],
            # Low memory processing
            low_memory=True,
            # Don't truncate strings at any length
            truncate_ragged_lines=False,
            # Don't skip blank lines - preserve data structure
            skip_rows_after_header=0
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
    df_with_validation = df.with_row_index("original_row_number", offset=2)

    # Validation 1: artist column is required
    artist_valid = pl.col("artist").str.strip_chars().str.len_chars() > 0

    # Validation 2: Check if any column still contains binary content after cleaning
    def has_binary_content(col):
        return (
            pl.col(col).map_elements(
                lambda x: isinstance(x, bytes) if x is not None else False,
                return_dtype=pl.Boolean
            )
        ).fill_null(False)

    # Check which columns have binary content
    binary_checks_per_column = {
        col: has_binary_content(col) for col in df.columns if col != "original_row_number"
    }

    # Get binary details for each column
    def get_binary_details(col):
        """Get binary information for each column"""
        return pl.col(col).map_elements(
            lambda x:
                f"bytes_{len(x)}" if isinstance(x, bytes) else
                "clean",
            return_dtype=pl.String
        ).fill_null("null")

    binary_details_per_column = {
        col: get_binary_details(col) for col in df.columns if col != "original_row_number"
    }

    # Combine to check if ANY column has binary content
    binary_checks = pl.any_horizontal(*binary_checks_per_column.values())

    # Create detailed binary information string
    binary_details = pl.concat_str([
        pl.lit(f"{col}:") + binary_details_per_column[col] + pl.lit("; ")
        for col in binary_details_per_column.keys()
    ], separator="").alias("binary_details")

    # Validation 3: name_similarity should be valid number between 0-1
    def validate_similarity():
        clean_col = pl.col("name_similarity").str.replace(",", "").str.strip_chars()

        # First check if it's empty or valid number format
        is_valid_format = (
            (clean_col == "") |  # Empty is OK
            clean_col.str.contains("^[0-9.]+$")  # Valid number format
        ).fill_null(False)

        # Only try to cast to float if it's a valid format and not empty
        is_in_range = (
            (clean_col == "") |  # Empty passes range check
            (
                is_valid_format &
                clean_col.cast(pl.Float64, strict=False).is_between(0, 1)
            )
        ).fill_null(False)

        return is_in_range

    similarity_valid = validate_similarity()

    # Validation 4: URL format validation
    url_valid = (
        (pl.col("url").str.strip_chars() == "") |  # Empty is OK
        pl.col("url").str.starts_with("http://") |
        pl.col("url").str.starts_with("https://")
    ).fill_null(True)  # Null URLs are considered valid (optional field)

    # Combine all validations
    is_valid = artist_valid & ~binary_checks & similarity_valid & url_valid

    # Generate specific error messages with binary details
    error_messages = (
        pl.when(~artist_valid).then(pl.lit("Artist column is required and cannot be empty"))
        .when(binary_checks).then(pl.lit("Binary content detected after cleaning - details: ") + binary_details)
        .when(~similarity_valid).then(pl.lit("Invalid name_similarity value (must be number between 0-1)"))
        .when(~url_valid).then(pl.lit("Invalid URL format (must start with http:// or https://)"))
        .otherwise(pl.lit(""))
        .alias("error_message")
    ).fill_null("Unknown validation error")

    return df_with_validation.with_columns([
        is_valid.alias("is_valid"),
        error_messages,
        binary_details
    ])


def process_csv_to_sqlite(input_file: str, db_name: str = "dbtemplate.db"):
    """
    Process CSV file using vectorized validation and insert valid rows into SQLite database.
    Invalid rows are logged to allmusic_data_errors.log and skipped.
    Only insert rows where allmusic_artist is not null.

    Args:
        input_file: Path to the CSV file to process
        db_name: SQLite database file name/path

    Returns:
        Count of rows skipped due to null allmusic_artist values
    """
    try:
        # Clean up previous run
        cleanup_previous_run(db_name)

        # Set up logging AFTER cleanup
        setup_logging()

        # Initialize SQLite database
        conn = sqlite3.connect(db_name)
        create_sqlite_table(conn)

        # Read CSV with ZERO type inference - everything as UTF-8 text
        df = read_csv_as_text_only(input_file)

        if df is None or len(df) == 0:
            print("No data found or could not read CSV file")
            conn.close()
            return

        print(f"Loaded {len(df)} rows from {input_file}")

        # Vectorized validation
        validated_df = validate_df_vectorized(df)

        # Separate valid and invalid rows
        valid_rows = validated_df.filter(pl.col("is_valid"))
        invalid_rows = validated_df.filter(~pl.col("is_valid"))

        print(f"Valid rows: {len(valid_rows)}")
        print(f"Invalid rows: {len(invalid_rows)}")

        # Log invalid rows with specific error messages
        if len(invalid_rows) > 0:
            for row in invalid_rows.iter_rows(named=True):
                # Create a clean row representation without validation columns
                row_data = {col: row[col] for col in df.columns}
                row_values = list(row_data.values())

                # Safely handle error_message
                error_msg = row.get('error_message', 'No error message')
                if error_msg is None:
                    error_msg = 'No error message'

                if "Binary content detected" in error_msg:
                    # Extract binary details from the error message
                    binary_info = error_msg.split("details: ")[-1] if "details: " in error_msg else "No binary details"

                    # Create a readable representation of the row
                    readable_row = []
                    for col_name, value in row_data.items():
                        if value is None:
                            readable_row.append("NULL")
                        elif isinstance(value, bytes):
                            # Show binary data as hex preview
                            hex_preview = value.hex()[:20] + "..." if len(value) > 20 else value.hex()
                            readable_row.append(f"[BINARY: {len(value)} bytes: {hex_preview}]")
                        else:
                            readable_row.append(str(value)[:100] + "..." if len(str(value)) > 100 else str(value))

                    row_string = " | ".join(readable_row)
                    logging.error(f"rowid: {row['original_row_number']}: {error_msg} - {row_string}")
                    print(f"BINARY CONTENT FOUND - row {row['original_row_number']}: {binary_info}")

                else:
                    # For non-binary errors
                    row_string = "|".join(str(val) if val is not None else "" for val in row_values)
                    logging.error(f"rowid: {row['original_row_number']}: {error_msg} - {row_string}")

        # Insert valid rows into SQLite (only those with allmusic_artist not null)
        skipped_allmusic_null_count = 0
        if len(valid_rows) > 0:
            # Drop validation columns before insertion
            valid_rows_for_insert = valid_rows.drop(["is_valid", "error_message", "binary_details"])
            inserted_count, skipped_count = insert_valid_rows(conn, valid_rows_for_insert)
            skipped_allmusic_null_count = skipped_count
            print(f"Inserted {inserted_count} valid rows into SQLite table")
            print(f"Skipped {skipped_count} rows with null/empty allmusic_artist")

        # Close database connection
        conn.close()

        return skipped_allmusic_null_count

    except Exception as e:
        logging.error(f"Processing error: {e}")
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.close()
        return 0

def alternative_approach_chunked_sqlite(input_file: str, db_name: str = "dbtemplate.db"):
    """
    Alternative approach using chunked processing for very large files.
    Handles EOF errors gracefully with ZERO type inference.

    Args:
        input_file: Path to the CSV file to process
        db_name: SQLite database file name/path
    """
    try:
        # Clean up previous run
        cleanup_previous_run(db_name)

        # Set up logging AFTER cleanup
        setup_logging()

        # Initialize SQLite database
        conn = sqlite3.connect(db_name)
        create_sqlite_table(conn)

        # Define explicit schema - ALL columns as String (UTF-8)
        schema = {
            "artist": pl.String,
            "allmusic_artist": pl.String,
            "name_similarity": pl.String,
            "genre": pl.String,
            "styles": pl.String,
            "mnid": pl.String,
            "url": pl.String
        }

        # Process in chunks
        chunk_size = 10000
        total_rows = 0
        invalid_count = 0
        inserted_count = 0
        eof_errors = 0

        try:
            # Read CSV in chunks with ZERO type inference
            reader = pl.read_csv_batched(
                input_file,
                separator="|",
                has_header=True,
                # CRITICAL: Set to 0 to disable ALL schema inference
                infer_schema_length=0,
                # Force explicit schema - NO inference allowed
                schema=schema,
                # Explicit UTF-8 encoding
                encoding="utf8",
                # Disable ALL automatic parsing
                try_parse_dates=False,
                # Handle errors by continuing
                ignore_errors=True,
                # Preserve empty strings as empty strings
                null_values=None,
                # Low memory processing
                low_memory=True,
                # Batch size for chunked reading
                batch_size=chunk_size
            )

            chunk_index = 0
            while True:
                try:
                    chunk = reader.next_batches(1)
                    if not chunk:
                        break

                    chunk = chunk[0]
                    total_rows += len(chunk)

                    # Verify all columns are String type
                    for col_name, dtype in zip(chunk.columns, chunk.dtypes):
                        if dtype != pl.String:
                            raise ValueError(f"Column {col_name} has type {dtype}, expected String. Type inference occurred!")

                    # Clean null characters from all columns in the chunk
                    for col in chunk.columns:
                        chunk = chunk.with_columns(
                            pl.col(col).map_elements(clean_null_chars, return_dtype=pl.String)
                        )

                    # Vectorized validation for the chunk
                    validated_chunk = validate_df_vectorized(chunk)

                    # Separate valid and invalid rows
                    valid_chunk = validated_chunk.filter(pl.col("is_valid"))
                    invalid_chunk = validated_chunk.filter(~pl.col("is_valid"))

                    # Log invalid rows
                    if len(invalid_chunk) > 0:
                        for row in invalid_chunk.iter_rows(named=True):
                            row_data = {col: row[col] for col in chunk.columns}
                            logging.error(f"rowid: {row['original_row_number']}: {row['error_message']} - {list(row_data.values())}")

                    invalid_count += len(invalid_chunk)

                    # Insert valid rows into SQLite
                    chunk_inserted = 0
                    if len(valid_chunk) > 0:
                        valid_for_insert = valid_chunk.drop(["is_valid", "error_message"])
                        chunk_inserted = insert_valid_rows(conn, valid_for_insert)
                        inserted_count += chunk_inserted

                    print(f"Processed chunk {chunk_index+1}: {len(chunk)} rows, {chunk_inserted} inserted")
                    chunk_index += 1

                except StopIteration:
                    break

        except pl.exceptions.ComputeError as e:
            if "unexpected eof" in str(e).lower() or "end of file" in str(e).lower():
                eof_errors += 1
                logging.error(f"EOF error encountered but processing continued: {e}")
                print(f"Warning: EOF error encountered but processing continued: {e}")
            else:
                raise e

        # Close database connection
        conn.close()

        print(f"Total rows processed: {total_rows}")
        print(f"Invalid rows: {invalid_count}")
        print(f"EOF errors encountered: {eof_errors}")
        print(f"Valid rows inserted: {inserted_count}")

    except Exception as e:
        logging.error(f"Chunked processing error: {e}")
        print(f"Error in chunked processing: {e}")
        if 'conn' in locals():
            conn.close()



def verify_database(db_name: str = "dbtemplate.db", input_file: str = "allmusic_data.csv", skipped_allmusic_null_count: int = 0):
    """
    Verify the database was created correctly and reconcile record counts.
    Database records + error log rows + skipped allmusic_artist null rows should equal total CSV rows.

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
            with open(input_file, 'r', encoding='utf-8') as f:
                # Skip header and count remaining rows
                next(f)  # Skip header
                csv_row_count = sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting CSV rows: {e}")
            csv_row_count = "unknown"

        # Count error log entries (each represents one invalid row)
        error_log_count = 0
        try:
            with open('allmusic_data_errors.log', 'r', encoding='utf-8') as f:
                # Count lines that contain validation errors
                error_log_count = sum(1 for line in f if "rowid:" in line)
        except FileNotFoundError:
            error_log_count = 0
        except Exception as e:
            print(f"Error reading error log: {e}")
            error_log_count = "unknown"

        # Calculate reconciliation
        if isinstance(db_rows, int) and isinstance(error_log_count, int):
            reconciliation = db_rows + error_log_count + skipped_allmusic_null_count
            status = "reconciliation OK" if reconciliation == csv_row_count else "reconciliation failed"
        else:
            reconciliation = "cannot calculate"
            status = "UNKNOWN"

        print(f"\nDatabase Reconciliation:")
        print(f"- database records                       : {db_rows}")
        print(f"- log entries (invalid rows)             : {error_log_count}")
        print(f"- skipped (no allmusic_artist)           : {skipped_allmusic_null_count}")
        print(f"Records processed (db + errors + skipped): {reconciliation}")
        print(f"Rows in CSV (excluding header)           : {csv_row_count}")
        print(f"Status: {status}")

        # Show detailed mismatch if any
        if (isinstance(db_rows, int) and isinstance(error_log_count, int) and
            isinstance(csv_row_count, int) and reconciliation != csv_row_count):
            print(f"Mismatch details:")
            print(f"  Expected total: {csv_row_count}")
            print(f"  Actual total: {reconciliation}")
            print(f"  Difference: {abs(csv_row_count - reconciliation)}")

        conn.close()

    except Exception as e:
        print(f"Error verifying database: {e}")

if __name__ == "__main__":
    input_file = "allmusic_data.csv"
    db_name = "dbtemplate.db"

    print("Starting CSV to SQLite processing...")
    print("Expected columns: artist|allmusic_artist|name_similarity|genre|styles|mnid|url")
    print("All columns will be stored as UTF-8 TEXT - ZERO type inference will be applied")
    print("Null characters (hex 00) will be automatically stripped from all values")
    print("Using CSV row numbers as primary keys for traceability")
    print("Using VECTORIZED validation for improved performance")
    print("Only inserting rows where allmusic_artist is not null/empty")

    # For moderate-sized files:
    skipped_count = process_csv_to_sqlite(input_file, db_name)

    # For very large files (uncomment if needed):
    # alternative_approach_chunked_sqlite(input_file, db_name)

    # Verify the database with reconciliation
    verify_database(db_name, input_file, skipped_count)

    print(f"\nProcessing completed. Database created: {db_name}")
    print("Check allmusic_data_errors.log for any validation errors.")
