"""
Purpose:
    Transform contributor fields in `alib` using a vetted transformation mapping
    (current value -> replacement value).

    Only rows that require transformation are updated; modifications increment
    `__sqlmodded` and are logged to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - _REF_vetted_contributors
    - changelog
    - sqlite_master (introspection)

Author: audiomuze
Last updated: 2026-04-13
"""
import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Union
import logging
import re

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# ---------- Configuration ----------


# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- Global Constants ----------
# Multi-value delimiter used by Tagminder (written to SQLite as two literal backslashes).
# Source of truth: tagminder.toml [strings].multivalue_delimiter
DELIMITER = tm_config.get_multivalue_delimiter()

# Regex pattern for splitting on various delimiters, but not commas followed by suffixes
_DELIM_REGEX = re.escape(DELIMITER)
SPLIT_PATTERN = re.compile(
    rf"(?:{_DELIM_REGEX}|;|/|,(?!\s*(?:[Jj][Rr]|[Ss][Rr]|[Ii][Ii][Ii]|[Ii][Vv]|[Vv])\b))"
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

    for column in columns:
        # Create mask: True if the contributor contains any value that needs transformation
        # For simplicity, we'll check if the whole field or any part (when split) needs transformation
        mask_expr = pl.col(column).is_not_null().alias(f"{column}_needs_transform")
        mask_expressions.append(mask_expr)

    return df.with_columns(mask_expressions)


def filter_transformable_tracks(
    df: pl.DataFrame,
    columns: List[str],
    transform_dict: Dict[str, Tuple[str, str]],
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
    needs_transform_conditions = []

    for col in columns:
        # Round 1 candidate: whole-cell case-insensitive key match.
        whole_match = (
            pl.col(col).str.strip_chars().str.to_lowercase().is_in(transform_keys)
        )

        # Round 2 candidate: any delimiter-split token case-insensitively matches a key.
        token_match = (
            pl.col(col)
            .str.split(DELIMITER)
            .list.eval(
                pl.element().str.strip_chars().str.to_lowercase().is_in(transform_keys)
            )
            .list.any()
            .fill_null(False)
        )

        needs_transform_conditions.append(
            pl.col(col).is_not_null() & (whole_match | token_match)
        )

    return df.filter(pl.any_horizontal(needs_transform_conditions))


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
    - Increments the __sqlmodded counter for each field changed
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
    tm_db.ensure_changelog_table(conn)

    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()
    updated_count = 0

    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    # Process only the rows that changed
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original_df.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for record in records:
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            eligible_fields = [
                col for col in columns_to_update if record.get(col) is not None
            ]

            def _norm(v: object) -> str | None:
                if v is None:
                    return None
                return v if isinstance(v, str) else str(v)

            changes: list[tuple[str, object, object]] = []
            changed_cols: list[str] = []
            for col in eligible_fields:
                old_v = original_row.get(col)
                new_v = record.get(col)
                if _norm(old_v) == _norm(new_v):
                    continue
                changes.append((col, old_v, new_v))
                changed_cols.append(col)

            if not changes:
                continue

            # Increment __sqlmodded counter by number of fields changed
            new_sqlmodded = int(original_row["__sqlmodded"] or 0) + len(changed_cols)
            sql = tm_db.build_update_sql(table="alib", set_cols=changed_cols)
            values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

            # Update the main table
            cursor.execute(sql, values)

            # Log each field change to changelog
            changelog.add(alib_path=alib_path, changes=changes)

            updated_count += 1

        changelog.flush(cursor)

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

    try:
        conn, db_path, _, _ = tm_run.open_db(
            default_db_path=None,
            ensure_changelog=True,
            require_exists=True,
        )
        master_db_path = tm_config.get_master_data_db_path(default=db_path)
        master_conn = conn if master_db_path == db_path else tm_db.connect(master_db_path, read_only=True)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    try:
        # Check if transformation reference table exists
        if not table_exists(master_conn, "_REF_vetted_contributors"):
            logging.info(
                "No contributor transformation data present, nothing to transform"
            )
            return

        # Load transformation dictionary with case handling
        logging.info("Fetching transformation dictionary...")
        transformations = tm_polars_db.sqlite_to_polars(
            master_conn, "SELECT current_val, replacement_val FROM _REF_vetted_contributors"
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
        tracks = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid,
                   artist, albumartist, composer, writer, lyricist,
                   engineer, producer,
                     COALESCE(__sqlmodded, 0) AS __sqlmodded
            FROM alib
            ORDER BY rowid
            """,
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

        # Filter to tracks needing transformation (whole-cell OR token-level match)
        logging.info("Filtering tracks for transformation...")
        tracks_filtered = filter_transformable_tracks(
            tracks, columns_to_transform, transform_dict
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
        if 'master_conn' in locals() and master_conn is not conn:
            master_conn.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
