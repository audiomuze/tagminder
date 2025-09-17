"""
Script Name: pullmbids.py

Purpose:
    Processes a tab-delimited CSV file containing MusicBrainz artist data (without headers)
    and loads specific columns into a SQLite database with comprehensive validation and data cleaning.

    The script performs the following operations:
    1. Cleans up previous run artifacts (database table and error log)
    2. Reads CSV file with zero type inference, treating all columns as UTF-8 text
    3. Extracts only columns 1, 2, 12, 13 (0-based indexing)
    4. Validates data using vectorized Polars operations for performance
    5. Handles binary content and null character removal from all fields
    6. Replaces double quotes in contributor column (exactly like original script)
    7. Cleanses problematic apostrophes in contributor column (vectorized operation)
    8. Inserts only valid rows where mbid is not null/empty
    9. Logs validation errors with detailed information
    10. Provides database reconciliation to verify data integrity
    11. Supports both single-pass and chunked processing for large files

    Key features:
    - Uses CSV row numbers as primary keys for traceability
    - Validates MBID format (UUID structure)
    - Handles binary content detection and logging
    - Provides comprehensive error logging
    - Double quote elimination in contributor column (same as original)
    - Apostrophe normalization in contributor column

    This script is part of tagminder.

Usage:
    python pullmbids.py [input_file] [db_name]

Arguments:
    input_file: Path to the input CSV file (defaults to "mbartist.csv")
    db_name: Optional path to the SQLite database file (defaults to "dbtemplate.db")

Author: audiomuze
Created: 2025-09-17
Modified: 2025-09-17
"""

import polars as pl
import sqlite3
import logging
import os
import sys
import re

def setup_logging():
    """
    Set up logging after cleanup to ensure the log file exists.
    Configures error logging to mbartist_errors.log with timestamp format.
    """
    logging.basicConfig(
        filename='mbartist_errors.log',
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
        if os.path.exists('mbartist_errors.log'):
            os.remove('mbartist_errors.log')
            print("Cleared previous mbartist_errors.log")

        # Clear database table if it exists
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='musicbrainz_source'")
        table_exists = cursor.fetchone()

        if table_exists:
            cursor.execute("DELETE FROM musicbrainz_source")
            print("Cleared previous data from musicbrainz_source table")

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
    CREATE TABLE IF NOT EXISTS musicbrainz_source (
        csv_row_number INTEGER PRIMARY KEY,
        mbid TEXT NOT NULL,
        contributor TEXT,
        gender TEXT,
        disambiguation TEXT,
        import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    conn.execute(create_table_sql)
    conn.commit()

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

def read_mbartist_csv(input_file: str):
    """
    Read MBArtist CSV file (without headers) treating EVERY column as UTF-8 text with ZERO type inference.
    Uses the exact same approach as the AllMusic script but for tab-delimited files.
    Extract only columns 1, 2, 12, 13 (0-based indexing) and handle null characters.
    Enhanced with fallback method for severely malformed CSV data.

    Args:
        input_file: Path to the CSV file to read

    Returns:
        Polars DataFrame with selected columns as strings and null characters cleaned
    """
    try:
        return _read_mbartist_csv_primary(input_file)
    except Exception as e:
        print(f"Primary CSV reading failed: {e}")
        print("Attempting fallback method with manual line parsing...")
        return _read_mbartist_csv_fallback(input_file)

def _read_mbartist_csv_primary(input_file: str):
    """
    Primary method for reading MBArtist CSV using Polars - simplified approach.
    """
    # First, determine the number of columns by reading the first line
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().strip()
        num_columns = first_line.count('\t') + 1
        print(f"Detected {num_columns} columns in the CSV file")

    # Check if we have enough columns for the ones we need
    required_columns = [1, 2, 12, 13]
    max_required = max(required_columns)

    if num_columns <= max_required:
        raise ValueError(f"CSV file has only {num_columns} columns, but need at least {max_required + 1} columns for indices {required_columns}")

    # Create a schema with all columns as strings
    schema = {f"column_{i+1}": pl.String for i in range(num_columns)}

    # Simple approach: treat everything between tabs as literal data
    df = pl.read_csv(
        input_file,
        separator="\t",
        has_header=False,
        infer_schema_length=0,
        schema=schema,
        encoding="utf8",
        try_parse_dates=False,
        ignore_errors=True,
        # recognize \N as null
        null_values=["\\N"],
        # The key fix: disable ALL quote processing
        quote_char="",  # Empty string disables quote processing entirely
        low_memory=True
    )

    # Select only the columns we need: 1, 2, 12, 13 (0-based indexing)
    # Map to meaningful names immediately for easier processing
    selected_columns = [f"column_{i+1}" for i in [1, 2, 12, 13]]
    df_selected = df.select(selected_columns)

    # Rename columns to meaningful names
    df_selected = df_selected.rename({
        "column_2": "mbid",
        "column_3": "contributor",
        "column_13": "gender",
        "column_14": "disambiguation"
    })

    # Clean null characters from all selected columns
    for col in df_selected.columns:
        df_selected = df_selected.with_columns(
            pl.col(col).map_elements(clean_null_chars, return_dtype=pl.String)
        )

    return df_selected

def _read_mbartist_csv_fallback(input_file: str):
    """
    Fallback method for reading severely malformed CSV files using manual line parsing.
    This method bypasses Polars CSV parsing entirely and manually splits lines.
    """
    try:
        rows = []
        row_count = 0
        required_columns = [1, 2, 12, 13]  # 0-based indexing

        with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    # Strip newline and split on tabs
                    line = line.rstrip('\n\r')
                    fields = line.split('\t')

                    # Check if we have enough columns
                    if len(fields) <= max(required_columns):
                        # Pad with empty strings if needed
                        while len(fields) <= max(required_columns):
                            fields.append('')

                    # Extract only the columns we need
                    selected_fields = [
                        fields[1] if len(fields) > 1 and fields[1] != "\\N" else None,      # mbid
                        fields[2] if len(fields) > 2 and fields[2] != "\\N" else None,      # contributor
                        fields[12] if len(fields) > 12 and fields[12] != "\\N" else None,   # gender
                        fields[13] if len(fields) > 13 and fields[13] != "\\N" else None    # disambiguation
                    ]

                    # Clean null characters from each field
                    cleaned_fields = [clean_null_chars(field) for field in selected_fields]

                    rows.append(cleaned_fields)
                    row_count += 1

                    # Progress indicator for large files
                    if row_count % 10000 == 0:
                        print(f"Processed {row_count} rows...")

                except Exception as line_error:
                    # Log individual line errors but continue processing
                    print(f"Error processing line {line_num}: {line_error}")
                    print(f"Problematic line content: {line[:100]}...")
                    # Add empty row to maintain row number alignment
                    rows.append(['', '', '', ''])
                    row_count += 1

        print(f"Fallback method processed {row_count} rows")

        # Create Polars DataFrame from manually parsed data
        df = pl.DataFrame({
            'mbid': [row[0] for row in rows],
            'contributor': [row[1] for row in rows],
            'gender': [row[2] for row in rows],
            'disambiguation': [row[3] for row in rows]
        })

        return df

    except Exception as e:
        logging.error(f"Fallback CSV reading error: {e}")
        print(f"Error in fallback CSV reading: {e}")
        raise

def clean_contributor_apostrophes(contributor_col: pl.Series) -> pl.Series:
    """
    Vectorized operation to clean problematic apostrophes in contributor column.
    Replaces specific problematic apostrophe encodings with standard apostrophes.

    Args:
        contributor_col: Polars Series containing contributor data

    Returns:
        Series with apostrophes normalized
    """
    # Replace problematic apostrophe encodings
    cleaned = (
        contributor_col
        .str.replace_all(r'â€™', "'")  # Common malformed apostrophe
        .str.replace_all(r'Ì', "'")    # Another problematic encoding
        .str.replace_all(r' ́', "'")    # Combining acute accent (looks like apostrophe)
        .str.replace_all(r'’', "'")    # Right single quotation mark to standard apostrophe
    )

    return cleaned

def validate_df_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """
    Vectorized validation using Polars expressions adapted for MusicBrainz data.

    Performs the following validations:
    1. MBID column is required and not empty
    2. MBID should follow UUID format (basic validation)
    3. Checks for binary content in any column
    4. Validates contributor field presence

    Args:
        df: Polars DataFrame to validate

    Returns:
        DataFrame with validation results and error messages
    """
    # Add row numbers first (starting from 1 since no header in source)
    df_with_validation = df.with_row_index("original_row_number", offset=1)

    # Validation 1: mbid column is required and not empty
    mbid_valid = (
        pl.col("mbid").is_not_null() &
        (pl.col("mbid").str.strip_chars().str.len_chars() > 0)
    )

    # Validation 2: Basic MBID format validation (UUID-like: 8-4-4-4-12 hex chars)
    # MusicBrainz IDs are UUIDs, so they should match the pattern
    mbid_format_valid = (
        pl.col("mbid").str.strip_chars().str.contains(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            literal=False
        )
    ).fill_null(False)

    # Validation 3: Check if any column still contains binary content after cleaning
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

    # Validation 4: Contributor should be present (not strictly required but flagged)
    contributor_present = (
        pl.col("contributor").is_not_null() &
        (pl.col("contributor").str.strip_chars().str.len_chars() > 0)
    )

    # Combine all validations (contributor is optional, so not included in main validation)
    is_valid = mbid_valid & mbid_format_valid & ~binary_checks

    # Generate specific error messages with binary details
    error_messages = (
        pl.when(~mbid_valid).then(pl.lit("MBID column is required and cannot be empty"))
        .when(~mbid_format_valid).then(pl.lit("Invalid MBID format (must be valid UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"))
        .when(binary_checks).then(pl.lit("Binary content detected after cleaning - details: ") + binary_details)
        .otherwise(pl.lit(""))
        .alias("error_message")
    ).fill_null("Unknown validation error")

    # Add warning for missing contributor (but don't mark as invalid)
    warning_messages = (
        pl.when(~contributor_present).then(pl.lit("Missing contributor information"))
        .otherwise(pl.lit(""))
        .alias("warning_message")
    )

    return df_with_validation.with_columns([
        is_valid.alias("is_valid"),
        error_messages,
        warning_messages,
        binary_details
    ])

def insert_valid_rows(conn: sqlite3.Connection, df: pl.DataFrame):
    """
    Insert valid rows into the SQLite table using CSV row number as ID.
    Replaces all instances of '""' with '"' in contributor column (same as original script).
    Cleanses problematic apostrophes in contributor column (vectorized operation).
    Only inserts rows where mbid is not null and not empty.

    Args:
        conn: SQLite database connection object
        df: Polars DataFrame containing validated data

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    # Prepare INSERT statement with csv_row_number as primary key
    insert_sql = """
    INSERT INTO musicbrainz_source (csv_row_number, mbid, contributor, gender, disambiguation)
    VALUES (?, ?, ?, ?, ?)
    """

    # Single-pass vectorized replacement using regex to replace all consecutive double quotes
    # This replaces any even number of consecutive double quotes with half the number of single quotes
    # Applied specifically to the contributor column (same as original)
    df = df.with_columns(
        pl.col("contributor")
        .str.replace_all(r'"{2,}', '"')
        .alias("contributor")
    )

    # Vectorized apostrophe cleansing for contributor column
    df = df.with_columns(
        clean_contributor_apostrophes(pl.col("contributor")).alias("contributor")
    )

    # Filter out rows where mbid is null or empty (this should already be done by validation, but double-check)
    df_with_mbid = df.filter(
        pl.col("mbid").is_not_null() &
        (pl.col("mbid").str.strip_chars().str.len_chars() > 0)
    )

    skipped_count = len(df) - len(df_with_mbid)

    # Convert all values to strings but preserve None values
    string_df = df_with_mbid.select([
        pl.col("original_row_number").cast(pl.Int64),
        pl.col("mbid"),  # Don't cast to String if it's None
        pl.col("contributor"),  # Don't cast to String if it's None
        pl.col("gender"),  # Don't cast to String if it's None
        pl.col("disambiguation")  # Don't cast to String if it's None
    ])

    # Convert Polars DataFrame to list of tuples for batch insertion
    rows_to_insert = string_df.iter_rows()

    # Batch insert
    cursor = conn.cursor()
    cursor.executemany(insert_sql, rows_to_insert)
    conn.commit()

    return cursor.rowcount, skipped_count

def process_mbartist_to_sqlite(input_file: str, db_name: str = "dbtemplate.db"):
    """
    Process MBArtist CSV file using vectorized validation and insert valid rows into SQLite database.
    Invalid rows are logged to mbartist_errors.log and skipped.
    Only insert rows where mbid is not null.

    Args:
        input_file: Path to the CSV file to process
        db_name: SQLite database file name/path

    Returns:
        Count of rows skipped due to null mbid values
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
        df = read_mbartist_csv(input_file)

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
                    row_string = "\t".join(str(val) if val is not None else "" for val in row_values)
                    logging.error(f"rowid: {row['original_row_number']}: {error_msg} - {row_string}")

        # Log warnings for valid rows (like missing contributor)
        valid_rows_with_warnings = valid_rows.filter(
            pl.col("warning_message").str.strip_chars().str.len_chars() > 0
        )

        if len(valid_rows_with_warnings) > 0:
            print(f"Valid rows with warnings: {len(valid_rows_with_warnings)}")
            # Could optionally log warnings here if desired

        # Insert valid rows into SQLite (only those with mbid not null)
        skipped_mbid_null_count = 0
        if len(valid_rows) > 0:
            # Drop validation columns before insertion
            valid_rows_for_insert = valid_rows.drop(["is_valid", "error_message", "warning_message", "binary_details"])
            inserted_count, skipped_count = insert_valid_rows(conn, valid_rows_for_insert)
            skipped_mbid_null_count = skipped_count
            print(f"Inserted {inserted_count} valid rows into SQLite table")
            if skipped_count > 0:
                print(f"Skipped {skipped_count} rows with null/empty mbid")

        # Close database connection
        conn.close()

        return skipped_mbid_null_count

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
    Alternative approach using chunked processing for very large MusicBrainz files.
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

        # First, determine the number of columns by reading the first line
        with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline().strip()
            num_columns = first_line.count('\t') + 1

        # Check if we have enough columns
        required_columns = [1, 2, 12, 13]
        max_required = max(required_columns)

        if num_columns <= max_required:
            raise ValueError(f"CSV file has only {num_columns} columns, but need at least {max_required + 1} columns")

        # Define explicit schema - ALL columns as String (UTF-8)
        schema = {f"column_{i+1}": pl.String for i in range(num_columns)}

        # Process in chunks
        chunk_size = 10000
        total_rows = 0
        invalid_count = 0
        inserted_count = 0
        eof_errors = 0

        try:
            # Simple chunked approach: treat everything between tabs as literal data
            reader = pl.read_csv_batched(
                input_file,
                separator="\t",
                has_header=False,
                infer_schema_length=0,
                schema=schema,
                encoding="utf8",
                try_parse_dates=False,
                ignore_errors=True,
                null_values=None,
                low_memory=True,
                batch_size=chunk_size,
                # The key fix: disable ALL quote processing
                quote_char=""  # Empty string disables quote processing entirely
            )

            chunk_index = 0
            while True:
                try:
                    chunk = reader.next_batches(1)
                    if not chunk:
                        break

                    chunk = chunk[0]
                    total_rows += len(chunk)

                    # Select and rename columns
                    selected_columns = [f"column_{i+1}" for i in [1, 2, 12, 13]]
                    chunk_selected = chunk.select(selected_columns)
                    chunk_selected = chunk_selected.rename({
                        "column_2": "mbid",
                        "column_3": "contributor",
                        "column_13": "gender",
                        "column_14": "disambiguation"
                    })

                    # Clean null characters from all columns in the chunk
                    for col in chunk_selected.columns:
                        chunk_selected = chunk_selected.with_columns(
                            pl.col(col).map_elements(clean_null_chars, return_dtype=pl.String)
                        )

                    # Vectorized validation for the chunk
                    validated_chunk = validate_df_vectorized(chunk_selected)

                    # Separate valid and invalid rows
                    valid_chunk = validated_chunk.filter(pl.col("is_valid"))
                    invalid_chunk = validated_chunk.filter(~pl.col("is_valid"))

                    # Log invalid rows
                    if len(invalid_chunk) > 0:
                        for row in invalid_chunk.iter_rows(named=True):
                            row_data = {col: row[col] for col in chunk_selected.columns}
                            logging.error(f"rowid: {row['original_row_number']}: {row['error_message']} - {list(row_data.values())}")

                    invalid_count += len(invalid_chunk)

                    # Insert valid rows into SQLite
                    chunk_inserted = 0
                    if len(valid_chunk) > 0:
                        valid_for_insert = valid_chunk.drop(["is_valid", "error_message", "warning_message", "binary_details"])
                        chunk_inserted, _ = insert_valid_rows(conn, valid_for_insert)
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

def verify_database(db_name: str = "dbtemplate.db", input_file: str = "mbartist.csv", skipped_mbid_null_count: int = 0):
    """
    Verify the database was created correctly and reconcile record counts.
    Database records + error log rows + skipped mbid null rows should equal total CSV rows.

    Args:
        db_name: SQLite database file name/path
        input_file: Path to the original CSV file
        skipped_mbid_null_count: Count of rows skipped due to null mbid
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Count total rows in database table
        cursor.execute("SELECT COUNT(*) FROM musicbrainz_source")
        db_rows = cursor.fetchone()[0]

        # Count total rows in CSV
        csv_row_count = 0
        try:
            with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
                csv_row_count = sum(1 for _ in f)
        except Exception as e:
            print(f"Error counting CSV rows: {e}")
            csv_row_count = "unknown"

        # Count error log entries (each represents one invalid row)
        error_log_count = 0
        try:
            with open('mbartist_errors.log', 'r', encoding='utf-8') as f:
                # Count lines that contain validation errors
                error_log_count = sum(1 for line in f if "rowid:" in line)
        except FileNotFoundError:
            error_log_count = 0
        except Exception as e:
            print(f"Error reading error log: {e}")
            error_log_count = "unknown"

        # Calculate reconciliation
        if isinstance(db_rows, int) and isinstance(error_log_count, int):
            reconciliation = db_rows + error_log_count + skipped_mbid_null_count
            status = "reconciliation OK" if reconciliation == csv_row_count else "reconciliation failed"
        else:
            reconciliation = "cannot calculate"
            status = "UNKNOWN"

        print(f"\nDatabase Reconciliation:")
        print(f"- database records                       : {db_rows}")
        print(f"- log entries (invalid rows)             : {error_log_count}")
        print(f"- skipped (no mbid)                      : {skipped_mbid_null_count}")
        print(f"Records processed (db + errors + skipped): {reconciliation}")
        print(f"Rows in CSV                              : {csv_row_count}")
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
    input_file = "mbartist.csv"
    db_name = "dbtemplate.db"

    print("Starting MusicBrainz Artist CSV to SQLite processing...")
    print(f"Input file: {input_file}")
    print("Format: Tab-delimited CSV (no headers)")
    print("Extracting columns: 1, 2, 12, 13 (0-based indexing)")
    print("Mapping: column_2->mbid, column_3->contributor, column_13->gender, column_14->disambiguation")
    print("All columns will be stored as UTF-8 TEXT - ZERO type inference will be applied")
    print("Null characters (hex 00) will be automatically stripped from all values")
    print("Using CSV row numbers as primary keys for traceability")
    print("Using VECTORIZED validation for improved performance")
    print("Only inserting rows where mbid is not null/empty")
    print("MBID format validation: UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    print("Apostrophe normalization: Replacing problematic apostrophes with standard ones in contributor column")

    # For moderate-sized files:
    skipped_count = process_mbartist_to_sqlite(input_file, db_name)

    # For very large files (uncomment if needed):
    # alternative_approach_chunked_sqlite(input_file, db_name)

    # Verify the database with reconciliation
    verify_database(db_name, input_file, skipped_count)

    print(f"\nProcessing completed. Database created: {db_name}")
    print("Check mbartist_errors.log for any validation errors.")
