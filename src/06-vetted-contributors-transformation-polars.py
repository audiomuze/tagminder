"""
Script Name: 06-vetted-contributors-transformation-polars.py

Purpose:
    Connects to a SQLite music library database (alib table) containing track metadata with various contributor fields
    (artist, composer, arranger, lyricist, etc.) and applies transformations based on a reference disambiguation table.

    The script filters out contributors that already exist in their canonical form in the
    disambiguation table, eliminating them from further processing to improve efficiency and prevent
    unnecessary changes.

    Processes and transforms contributor names across multiple contributor fields by:
    - Loading transformation mappings from _REF_vetted_contributors table
    - Handling multiple contributors separated by delimiters (double backslash \\)
    - Applying dictionary-based transformations to convert current_val -> replacement_val
    - Deduplicating entries
    - ONLY processing entries that need transformation based on the reference table

    Detects changes by comparing original vs transformed data using Polars vectorized operations
    Updates the database only for rows that actually changed, while:
    - Incrementing a modification counter (sqlmodded)
    - Logging all changes to a changelog table with timestamps
    - Using database transactions for data integrity

    The script is optimized for performance using Polars DataFrames and vectorized operations, with
    comprehensive logging and error handling. It's designed to apply consistent transformations to
    contributor name fields in a music library database while maintaining a full audit trail of changes.

It is part of tagminder.

Usage:
    python 06-vetted-contributors-transformation-polars.py

Author: audiomuze
Created: 2025-07-19

"""

import os
import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Union
import logging
from datetime import datetime, timezone
import re

# ---------- Configuration ----------
SCRIPT_NAME = "06-vetted-contributors-transformation-polars.py"
DB_PATH = "/tmp/amg/dbtemplate.db"

# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- Global Constants ----------
# Main delimiter for joining multiple contributors
DELIMITER = "\\\\"  # Double backslash for splitting and joining

# Regex pattern for splitting on various delimiters, but not commas followed by suffixes
SPLIT_PATTERN = re.compile(
    r"(?:\\\\|;|/|,(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b))"
)

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
    cursor.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """,
        (table_name,),
    )
    return cursor.fetchone() is not None


def sqlite_to_polars(
    conn: sqlite3.Connection, query: str, id_column: Union[str, Tuple[str, ...]] = None
) -> pl.DataFrame:
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
                name=col_name, values=[int(x or 0) for x in col_data], dtype=pl.Int64
            )
        else:
            # String columns with null handling
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8,
            )

    return pl.DataFrame(data)


# ---------- Text Processing Functions ----------

# def transform_contributor_entry_all(x: Union[str, None], transform_dict: Dict[str, Tuple[str, str]]) -> Union[str, None]:
#     """
#     Transform a contributor entry by:
#     1. First attempting whole-field replacement
#     2. Then splitting ONLY on DELIMITER (double backslash) and transforming individual items
#     3. Applying dictionary-based transformations
#     4. Deduplicating entries

#     Args:
#         x: Contributor string to transform (can be None)
#         transform_dict: Dictionary mapping lowercase current values to
#                       (original_case_current_val, replacement_val) tuples

#     Returns:
#         Transformed contributor string with proper formatting
#     """
#     if x is None:
#         return None

#     stripped_x = x.strip()
#     if not stripped_x:  # Handle empty strings
#         return stripped_x

#     # First try whole-field replacement
#     lookup_key = stripped_x.lower()
#     if lookup_key in transform_dict:
#         original_case, replacement = transform_dict[lookup_key]
#         if stripped_x != replacement:
#             return replacement

#     # Only split on DELIMITER (double backslash), ignore other delimiters
#     if DELIMITER in stripped_x:
#         items = stripped_x.split(DELIMITER)
#         transformed_items = []
#         seen = set()

#         for item in items:
#             stripped_item = item.strip()
#             if not stripped_item:
#                 continue

#             # Apply transformation if available
#             item_lookup = stripped_item.lower()
#             if item_lookup in transform_dict:
#                 original_case, replacement = transform_dict[item_lookup]
#                 if stripped_item != replacement:
#                     transformed_item = replacement
#                 else:
#                     transformed_item = stripped_item
#             else:
#                 transformed_item = stripped_item

#             # Deduplicate
#             if transformed_item not in seen:
#                 transformed_items.append(transformed_item)
#                 seen.add(transformed_item)

#         if not transformed_items:
#             return None
#         elif len(transformed_items) == 1:
#             return transformed_items[0]
#         else:
#             return DELIMITER.join(transformed_items)
#     else:
#         # No DELIMITER found and whole-field replacement didn't match
#         return stripped_x


def transform_contributor_entry_all(
    x: Union[str, None], transform_dict: Dict[str, Tuple[str, str]]
) -> Union[str, None]:
    """
    Transform a contributor entry by:
    1. First attempting whole-field replacement
    2. Then if the cell contains delimiter (\\\\), applying item-level processing regardless of whole-field result
    3. Otherwise, return the cell unchanged

    This ensures that both whole-field AND item-level transformations can be applied to the same cell.

    Args:
        x: Contributor string to transform (can be None)
        transform_dict: Dictionary mapping lowercase current values to
                      (original_case_current_val, replacement_val) tuples

    Returns:
        Transformed contributor string with proper formatting
    """
    if x is None:
        return None

    stripped_x = x.strip()
    if not stripped_x:  # Handle empty strings
        return stripped_x

    # Step 1: Try whole-field replacement first
    current_value = stripped_x
    lookup_key = current_value.lower()
    if lookup_key in transform_dict:
        original_case, replacement = transform_dict[lookup_key]
        if current_value != replacement:
            current_value = replacement

    # Step 2: If the current value contains DELIMITER, also do item-level processing
    if DELIMITER in current_value:
        items = current_value.split(DELIMITER)
        transformed_items = []
        seen = set()

        for item in items:
            stripped_item = item.strip()
            if not stripped_item:
                continue

            # Apply transformation if available
            item_lookup = stripped_item.lower()
            if item_lookup in transform_dict:
                original_case, replacement = transform_dict[item_lookup]
                if stripped_item != replacement:
                    transformed_item = replacement
                else:
                    transformed_item = stripped_item
            else:
                transformed_item = stripped_item

            # Deduplicate
            if transformed_item not in seen:
                transformed_items.append(transformed_item)
                seen.add(transformed_item)

        if not transformed_items:
            return None
        elif len(transformed_items) == 1:
            return transformed_items[0]
        else:
            return DELIMITER.join(transformed_items)
    else:
        # Step 3: No DELIMITER found, return the current value (which may have been whole-field transformed)
        return current_value


# ---------- Surgical Filtering Functions ----------


def create_transformation_masks(
    df: pl.DataFrame, columns: List[str], transform_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Create boolean masks for each contributor column indicating which entries
    need transformation based on the transform dictionary.

    This function uses Polars vectorization to efficiently check all contributor
    fields against the transformation dictionary, creating mask columns that
    identify entries that need processing.

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names to check
        transform_dict: Dictionary mapping current values to replacement values

    Returns:
        DataFrame with original data plus boolean mask columns (named {column}_needs_transform)
    """
    mask_expressions = []

    # Create a set of keys that need transformation for efficient lookup
    transform_keys = set(transform_dict.keys())

    for column in columns:
        # Create mask: True if the contributor contains any value that needs transformation
        # For simplicity, we'll check if the whole field or any part (when split) needs transformation
        mask_expr = pl.col(column).is_not_null().alias(f"{column}_needs_transform")
        mask_expressions.append(mask_expr)

    return df.with_columns(mask_expressions)


def filter_transformable_tracks(
    df: pl.DataFrame, columns: List[str], transform_dict: Dict[str, str]
) -> pl.DataFrame:
    """
    Filter to only tracks that have at least one contributor field that needs transformation
    based on the transformation dictionary.

    This optimizes performance by eliminating tracks where no contributor fields
    require transformation.

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names
        transform_dict: Dictionary mapping current values to replacement values

    Returns:
        Filtered DataFrame containing only tracks that need transformation
    """
    transform_keys = set(transform_dict.keys())

    # Create expressions to check if any contributor field contains transformable values
    needs_transform_conditions = []

    for col in columns:
        # Check if column is not null and contains any transformable values
        # We'll use a simple approach: if any part of the string (when split by delimiter)
        # exists in the transform_dict keys
        condition = pl.col(col).is_not_null()
        needs_transform_conditions.append(condition)

    # Filter to tracks that have at least one non-null contributor field
    # We'll do a more precise filtering during the transformation step
    needs_processing_expr = pl.any_horizontal(needs_transform_conditions)

    return df.filter(needs_processing_expr)


def selective_transform_contributors(
    df: pl.DataFrame, columns: List[str], transform_dict: Dict[str, Tuple[str, str]]
) -> pl.DataFrame:
    """
    Selectively transform contributor columns, applying dictionary-based transformations
    to convert current values to replacement values.

    Args:
        df: DataFrame with contributor columns
        columns: List of contributor column names to transform
        transform_dict: Dictionary mapping lowercase current values to
                      (original_case_current_val, replacement_val) tuples

    Returns:
        DataFrame with transformed contributor columns
    """
    expressions = []

    for column in columns:
        # Apply transformation using the helper function
        expr = (
            pl.col(column)
            .map_elements(
                lambda x: transform_contributor_entry_all(x, transform_dict),
                return_dtype=pl.Utf8,
            )
            .alias(column)
        )
        expressions.append(expr)

    return df.with_columns(expressions)


def detect_transformation_changes(
    original_df: pl.DataFrame, updated_df: pl.DataFrame, columns: List[str]
) -> List[int]:
    """
    Detect changes between original and updated DataFrames after transformation.

    Args:
        original_df: Original DataFrame before transformation
        updated_df: Updated DataFrame after transformation
        columns: List of contributor column names

    Returns:
        List of rowids that have actual changes requiring database updates
    """
    # Create change detection expressions
    change_expressions = []

    for col in columns:
        # Consider it a change if:
        # 1. The original value was not null
        # 2. The values actually differ after transformation
        change_expr = (
            original_df[col].is_not_null()  # Original value exists
            & (original_df[col] != updated_df[col])  # Values differ
        )
        change_expressions.append(change_expr)

    # Any row with at least one change
    any_change_expr = pl.any_horizontal(change_expressions)

    return updated_df.filter(any_change_expr)["rowid"].to_list()


# ---------- Database Update Functions ----------


def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int],
    columns_to_update: List[str],
) -> int:
    """
    Write transformed contributor updates to the database with full changelog tracking.

    This function:
    - Updates only the rows that actually changed
    - Increments the sqlmodded counter for each field changed
    - Logs all changes to the changelog table with timestamps
    - Uses database transactions for data integrity

    Args:
        conn: SQLite database connection
        updated_df: DataFrame with transformed contributor data
        original_df: DataFrame with original contributor data
        changed_rowids: List of rowids that need updating
        columns_to_update: List of contributor column names to update

    Returns:
        Number of rows actually updated in the database
    """
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    # Ensure changelog table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            alib_column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    timestamp = datetime.now(timezone.utc).isoformat()
    updated_count = 0

    # Process only the rows that changed
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)

        # Identify which columns actually changed and have new values
        changed_cols = [
            col
            for col in columns_to_update
            if record[col] != original_row[col] and record[col] is not None
        ]

        if not changed_cols:
            continue

        # Increment sqlmodded counter by number of fields changed
        new_sqlmodded = int(original_row["sqlmodded"] or 0) + len(changed_cols)
        set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
        values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

        # Update the main table
        cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)

        # Log each field change to changelog
        for col in changed_cols:
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, col, original_row[col], record[col], timestamp, SCRIPT_NAME),
            )

        updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


def mark_transformations_as_processed(conn: sqlite3.Connection) -> None:
    """
    Mark transformation records in the reference table as processed by setting status = TRUE.

    Args:
        conn: SQLite database connection
    """
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE _REF_vetted_contributors SET status = TRUE WHERE status = FALSE"
    )
    conn.commit()
    logging.info("Marked transformation records as processed in reference table")


# ---------- Main Execution Function ----------


def main():
    """
    Main execution function that orchestrates the contributor transformation process
    with case-insensitive matching and case-preserving replacement

    Process flow:
    1. Check if transformation reference table exists
    2. Load transformation dictionary from _REF_vetted_contributors table
    3. Load track data with contributor fields
    4. Filter to tracks that may need transformation
    5. Apply dictionary-based transformations (both whole-field and per-item)
    6. Detect actual changes
    7. Update database with changes and log to changelog
    8. Mark transformation records as processed
    """

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
        # Check if transformation reference table exists
        if not table_exists(conn, "_REF_vetted_contributors"):
            logging.info(
                "No contributor transformation data present, nothing to transform"
            )
            return

        # Load transformation dictionary with case handling
        logging.info("Fetching transformation dictionary...")
        transformations = sqlite_to_polars(
            conn, "SELECT current_val, replacement_val FROM _REF_vetted_contributors"
        ).with_columns(
            [
                pl.col("current_val").str.strip_chars(),
                pl.col("replacement_val").str.strip_chars(),
            ]
        )

        if transformations.height == 0:
            logging.info("No transformation records found in reference table")
            return

        # Create mapping dictionary: lowercase current_val -> (original current_val, replacement_val)
        transform_dict = {
            row["current_val"].lower(): (row["current_val"], row["replacement_val"])
            for row in transformations.iter_rows(named=True)
        }
        logging.info(f"Loaded {len(transform_dict)} transformation mappings")

        # Load track data
        logging.info("Fetching tracks data...")
        tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid,
                   artist, albumartist, composer, writer, lyricist,
                   engineer, producer,
                   COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            ORDER BY rowid
            """,
            id_column=("rowid", "sqlmodded"),
        )

        # Define columns to transform
        columns_to_transform = [
            "artist",
            "albumartist",
            "composer",
            "writer",
            "lyricist",
            "engineer",
            "producer",
        ]

        # Filter to tracks needing transformation (case-insensitive)
        logging.info("Filtering tracks for transformation...")
        transform_keys = set(transform_dict.keys())
        tracks_filtered = tracks.filter(
            pl.any_horizontal(
                [
                    pl.col(col)
                    .str.strip_chars()
                    .str.to_lowercase()
                    .is_in(transform_keys)
                    for col in columns_to_transform
                ]
            )
        )
        logging.info(
            f"Processing {tracks_filtered.height} tracks for transformation..."
        )

        if tracks_filtered.height == 0:
            logging.info("No tracks need transformation")
            return

        # Store original data before transformation (for change detection)
        original_tracks = tracks_filtered.clone()

        # Apply transformations (both whole-field and per-item)
        logging.info("Applying contributor transformations...")
        updated_tracks = selective_transform_contributors(
            tracks_filtered, columns_to_transform, transform_dict
        )

        # Detect changes
        changed_rowids = detect_transformation_changes(
            original_tracks, updated_tracks, columns_to_transform
        )
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks,
                original_df=original_tracks,
                changed_rowids=changed_rowids,
                columns_to_update=columns_to_transform,
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")

            # Mark transformations as processed
            # mark_transformations_as_processed(conn)
            # removed because null = not yet reviewed, 0 = reviewed and rejected, 1 = reviewed and accepted
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
