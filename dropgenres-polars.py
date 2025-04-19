"""
Script Name: dropgenres-polars.py

Purpose:
    This script processes all records in alib and validates genre and style tags,
    removing any erntries that don't appear in the table _ref_genres.

    It is the de-facto way of getting rid of genre metadata you don't want in your music collection.
    It is part of tagminder.

Usage:
    python dropgenres-polars.py
    uv run dropgenres-polars.py

Author: audiomuze
Created: 2025-04-18
"""


import polars as pl
import sqlite3
import logging
from typing import Set, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_PATH = '/tmp/amg/dbtemplate.db' # Ensure this path is correct for your environment
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres' # This table is used for validating both genres and styles
DELIMITER = '\\\\'

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    """
    Execute a query and convert the results to a Polars DataFrame.

    Args:
        conn: SQLite database connection
        query: SQL query to execute
        id_column: Name of ID column to preserve type

    Returns:
        Polars DataFrame with results
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query)  # Execute the query
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e} with query: {query}")
        raise  # Re-raise the exception to be handled upstream

    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        # Use id_column parameter for type preservation
        if id_column and col_name == id_column:
             data[col_name] = pl.Series(col_data, dtype=pl.Int64)
        elif col_name == "sqlmodded":
            # Convert None to 0 and ensure Int32 type
            data[col_name] = pl.Series([int(x) if x is not None else 0 for x in col_data], dtype=pl.Int32)
        else:
            # Default to Utf8 for other columns
            data[col_name] = pl.Series(col_data, dtype=pl.Utf8)

    df = pl.DataFrame(data)
    return df

def import_genre_style_sqlmodded(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Imports rowid, __path, genre, style, and sqlmodded from alib.
    Ensures sqlmodded is treated as 0 if NULL.
    """
    query = f"""
        SELECT rowid, __path, genre, style, COALESCE(sqlmodded, 0) AS sqlmodded
        FROM {ALIB_TABLE}
    """
    return sqlite_to_polars(conn, query, id_column="rowid")

def import_valid_tags(conn: sqlite3.Connection) -> Set[str]:
    """
    Imports the 'genre_name' column from _REF_genres into a set for efficient lookup,
    as this table is used for validating both genres and styles.
    Handles case where the table or column might be missing.
    """
    query = f"SELECT genre_name FROM {REF_VALIDATION_TABLE}"
    try:
        df = sqlite_to_polars(conn, query)
        return set(df['genre_name'].to_list())
    except sqlite3.OperationalError as e:
         logging.error(f"Error importing valid tags from {REF_VALIDATION_TABLE}. Does the table exist with 'genre_name' column? Error: {e}")
         # Return empty set if table/column is missing, allowing processing to continue
         # but marking all tags as invalid.
         return set()


def split_and_trim(series: pl.Series) -> pl.Series:
    """
    Splits a string series by the defined delimiter and trims whitespace from elements.
    Handles multiple delimiters and filters out empty strings after splitting/trimming.
    """
    def trim_list(lst: Optional[List[str]]) -> List[str]:
        if lst is None:
            return [] # Handle None input gracefully
        # Trim whitespace and filter out any resulting empty strings
        return [item.strip() for item in lst if item is not None and item.strip()]

    return (
        series.str.replace_all(r"\s*,\s*", DELIMITER)  # Normalize comma delimiters (with optional surrounding space)
        .str.replace_all(r"\s*;\s*", DELIMITER)  # Normalize semicolon delimiters (with optional surrounding space)
        # The above handle cases like "a, b", "a;b", "a ,b", etc.
        # The following two lines handle cases like "a,,b" or "a;;b" or ",a" or "a,"
        .str.replace_all(r",", DELIMITER)
        .str.replace_all(r";", DELIMITER)
        .str.split(DELIMITER)
        .map_elements(trim_list, return_dtype=pl.List(pl.Utf8)) # Apply trimming and filtering
    )

def validate_list_against_set(item_list: Optional[List[str]], valid_items: Set[str]) -> Optional[List[Optional[str]]]:
    """
    Validates items in a list against a set of valid items.
    Returns a list where invalid items are replaced with None.
    Returns None if the input list is None.
    Assumes items in item_list are already stripped of whitespace.
    """
    if item_list is None:
        return None
    # Check if each item exists in the set of valid items.
    # item is guaranteed not to be None or empty string here due to split_and_trim's map_elements.
    return [item if item in valid_items else None for item in item_list]

def process_genre_and_style(df: pl.DataFrame, valid_tags: Set[str]) -> pl.DataFrame:
    """
    Processes both genre and style columns: splits, trims, and validates against valid_tags.
    Sets invalid tags to None, calculates sqlmodded increment, determines replacements,
    and calculates discarded counts for both.
    """
 
    # --- Process Style ---
    # Process style tags first, because at some point we are going to want Genre to be the deduplicated
    # combination of validated genres and styles
    # Apply splitting and validation to the style column using the same valid_tags
    df = df.with_columns(
        split_and_trim(pl.col("style")).alias("_style_split")
    )

    df = df.with_columns(
        pl.col("_style_split")
        .map_elements(lambda lst: validate_list_against_set(lst, valid_tags), return_dtype=pl.List(pl.Utf8))
        .alias("_style_validated")
    )

    # Calculate sqlmodded increment for style changes
    # An increment occurs if the validated list is different from the original split list
    df = df.with_columns(
         pl.when(pl.col("_style_validated") != pl.col("_style_split"))
         .then(pl.lit(1).cast(pl.Int32))
         .otherwise(pl.lit(0).cast(pl.Int32))
         .alias("_style_mod_increment")
     )

    # Calculate count of discarded style tags
    df = df.with_columns(
        pl.col("_style_split").list.len().alias("_style_split_count"),
        # FIX: Use list.sum() after list.eval(is_not_null) to get a scalar count per row
        pl.col("_style_validated").list.eval(pl.element().is_not_null()).list.sum().alias("_style_validated_count"),
    )

    df = df.with_columns(
        (pl.col("_style_split_count") - pl.col("_style_validated_count")).alias("_style_discarded_count")
    )

    # Determine the replacement style string
    # Join non-nulls from the validated list.
    # If the joined result differs from the original 'style' string, use the joined result.
    # If it's the same, set _replacement_style to None (signifying no change needed).
    # If all validated tags were null (resulting list is empty), the joined string is "".
    validated_style_joined = pl.col("_style_validated").list.eval(
        pl.element().filter(pl.element().is_not_null())
    ).list.join(DELIMITER)

    df = df.with_columns(
        pl.when(validated_style_joined != pl.col("style"))
        .then(validated_style_joined) # Use the joined string (which might be "") if it's different from original
        .otherwise(pl.lit(None).cast(pl.Utf8)) # Use None if no change from original style
        .alias("_replacement_style")
    )

     # Drop style intermediate counts
    df = df.drop(["_style_split_count", "_style_validated_count"])

    # --- Process Genre ---
    df = df.with_columns(
        split_and_trim(pl.col("genre")).alias("_genre_split")
    )

    df = df.with_columns(
        pl.col("_genre_split")
        .map_elements(lambda lst: validate_list_against_set(lst, valid_tags), return_dtype=pl.List(pl.Utf8))
        .alias("_genre_validated")
    )

    # Calculate sqlmodded increment for genre changes
    # An increment occurs if the validated list is different from the original split list
    df = df.with_columns(
        pl.when(pl.col("_genre_validated") != pl.col("_genre_split"))
        .then(pl.lit(1).cast(pl.Int32))
        .otherwise(pl.lit(0).cast(pl.Int32))
        .alias("_genre_mod_increment")
    )

    # Calculate count of discarded genre tags
    df = df.with_columns(
        pl.col("_genre_split").list.len().alias("_genre_split_count"),
        # FIX: Use list.sum() after list.eval(is_not_null) to get a scalar count per row
        pl.col("_genre_validated").list.eval(pl.element().is_not_null()).list.sum().alias("_genre_validated_count"),
    )

    df = df.with_columns(
        (pl.col("_genre_split_count") - pl.col("_genre_validated_count")).alias("_genre_discarded_count")
    )

    # Determine the replacement genre string
    # Join non-nulls from the validated list.
    # If the joined result differs from the original 'genre' string, use the joined result.
    # If it's the same, set _replacement_genre to None (signifying no change needed).
    # If all validated tags were null (resulting list is empty), the joined string is "".
    validated_genre_joined = pl.col("_genre_validated").list.eval(
        pl.element().filter(pl.element().is_not_null())
    ).list.join(DELIMITER)

    df = df.with_columns(
        pl.when(validated_genre_joined != pl.col("genre"))
        .then(validated_genre_joined) # Use the joined string (which might be "") if it's different from original
        .otherwise(pl.lit(None).cast(pl.Utf8)) # Use None if no change from original genre
        .alias("_replacement_genre")
    )

    # Drop genre intermediate counts
    df = df.drop(["_genre_split_count", "_genre_validated_count"])



    # --- Update sqlmodded based on both genre and style changes ---
    # Add the *total* increment from genre and style modifications to the original sqlmodded value
    df = df.with_columns(
        (pl.col("sqlmodded") + pl.col("_genre_mod_increment") + pl.col("_style_mod_increment")).alias("sqlmodded")
    )

    # Drop all intermediate working columns
    df = df.drop([
        "_genre_split", "_genre_validated", "_genre_mod_increment",
        "_style_split", "_style_validated", "_style_mod_increment",
    ])

    return df


def main():
    """Main function to import data, process genre/style, and write back changes to ALIB_TABLE."""
    conn = sqlite3.connect(DB_PATH)
    updated_row_count = 0

    try:
        logging.info(f"Importing data from {ALIB_TABLE}...")
        alib_df = import_genre_style_sqlmodded(conn)
        logging.info(f"Imported {alib_df.height} rows.")

        # Import valid tags (genres/styles) from the single reference table
        logging.info(f"Importing valid tags from {REF_VALIDATION_TABLE}...")
        valid_tags = import_valid_tags(conn)
        logging.info(f"Imported {len(valid_tags)} valid tags.")

        logging.info("Processing genre and style columns...")
        # Process both columns using the same set of valid tags
        processed_df = process_genre_and_style(alib_df.clone(), valid_tags)
        logging.info("Genre and style processing complete.")

        # Identify rows where *either* genre or style needs updating
        # This is true if the calculated _replacement_genre or _replacement_style is NOT NULL
        # (meaning a change was determined necessary by process_genre_and_style)
        rows_to_update = processed_df.filter(
            (pl.col("_replacement_genre").is_not_null()) | (pl.col("_replacement_style").is_not_null())
        )

        logging.info(f"Found {rows_to_update.height} rows to update.")

        if rows_to_update.height > 0:
            cursor = conn.cursor()
            # Use a single update statement per row to update genre, style, and sqlmodded
            # COALESCE(?, column) updates the column only if the parameter is NOT NULL.
            # If _replacement_genre/_replacement_style is None (no change needed), COALESCE uses the original value.
            # If _replacement_genre/_replacement_style is "" (all tags invalid), COALESCE sets the column to "".
            # If _replacement_genre/_replacement_style is a joined string, COALESCE sets the column to that string.
            sql = f"UPDATE {ALIB_TABLE} SET genre = COALESCE(?, genre), style = COALESCE(?, style), sqlmodded = ? WHERE rowid = ?"

            # Use iter_rows for potentially better memory usage on large DataFrames,
            # although rows(named=True) is fine for moderate sizes.
            for row in rows_to_update.iter_rows(named=True):
                rowid = row['rowid']
                replacement_genre = row['_replacement_genre'] # This will be string or None
                replacement_style = row['_replacement_style'] # This will be string or None
                sqlmodded_value = row['sqlmodded'] # This is the final calculated value

                try:
                    # Pass None directly if no replacement was needed. Pass "" if all tags were invalid.
                    # Pass the joined string if some valid tags remained and it differed from original.
                    cursor.execute(sql, (replacement_genre, replacement_style, sqlmodded_value, rowid))
                    updated_row_count += 1
                except sqlite3.Error as e:
                     logging.error(f"Error updating rowid {rowid}: {e}")
                     # Log the error and continue with the next row.
                     pass

            conn.commit()
            logging.info(f"Updated {updated_row_count} rows in {ALIB_TABLE}.")

        # Report discarded counts (these columns are added by process_genre_and_style)
        # Ensure these columns exist before summing, in case processed_df is empty
        # FIX: Sum the scalar u32/i32 columns directly
        total_genre_discarded = processed_df["_genre_discarded_count"].sum() if "_genre_discarded_count" in processed_df.columns else 0
        total_style_discarded = processed_df["_style_discarded_count"].sum() if "_style_discarded_count" in processed_df.columns else 0

        logging.info(f"Total genre tags discarded: {total_genre_discarded}")
        logging.info(f"Total style tags discarded: {total_style_discarded}")


    except Exception as e:
        logging.error(f"An error occurred during processing or updating: {e}", exc_info=True) # Log traceback for debugging
        conn.rollback() # Rollback changes on error
        raise # Re-raise the exception

    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    # This block will now just run the main function, assuming the DB and tables exist.
    main()
