"""
Script Name: dropgenres-polars.py

Purpose:
    This script processes all records in alib and validates genre and style tags,
    removing any entries that don't appear in the table _ref_genres.  It leverages cosine similarity
    via string grouper to replace close matches with a genre entry from the regerence table

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
from typing import Set, List, Optional, Tuple
from string_grouper import match_strings
from datetime import datetime, timezone
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s')


DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres'
DELIMITER = '\\\\'

# --------------------- Data Import ---------------------

def sqlite_to_polars(conn: sqlite3.Connection, query: str, id_column: str = None) -> pl.DataFrame:
    cursor = conn.cursor()
    cursor.execute(query)
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        if id_column and col_name == id_column:
            data[col_name] = pl.Series(col_data, dtype=pl.Int64)
        elif col_name == "sqlmodded":
            data[col_name] = pl.Series([int(x) if x is not None else 0 for x in col_data], dtype=pl.Int32)
        else:
            data[col_name] = pl.Series(col_data, dtype=pl.Utf8)

    return pl.DataFrame(data)

def import_genre_style_sqlmodded(conn: sqlite3.Connection) -> pl.DataFrame:
    query = f"""
        SELECT rowid, __path, genre, style, COALESCE(sqlmodded, 0) AS sqlmodded
        FROM {ALIB_TABLE}
    """
    return sqlite_to_polars(conn, query, id_column="rowid")

def import_valid_tags(conn: sqlite3.Connection) -> Set[str]:
    query = f"SELECT genre_name FROM {REF_VALIDATION_TABLE}"
    try:
        df = sqlite_to_polars(conn, query)
        return set(df['genre_name'].to_list())
    except sqlite3.OperationalError as e:
        logging.error(f"Error importing valid tags from {REF_VALIDATION_TABLE}: {e}")
        return set()

# --------------------- Cleaning & Validation ---------------------

def split_and_trim(series: pl.Series) -> pl.Series:
    def trim_list(lst: Optional[List[str]]) -> List[str]:
        if lst is None:
            return []
        return [item.strip() for item in lst if item and item.strip()]

    return (
        series.str.replace_all(r"\s*,\s*", DELIMITER)
              .str.replace_all(r"\s*;\s*", DELIMITER)
              .str.replace_all(r"\s*\|\s*", DELIMITER)
              .str.replace_all(r",", DELIMITER)
              .str.replace_all(r";", DELIMITER)
              .str.split(DELIMITER)
              .map_elements(trim_list, return_dtype=pl.List(pl.Utf8))
    )



def normalize_before_match(s: str) -> str:
    return (
        s.lower()
         .replace('&', '/')
         .replace('_', ' ')
         .replace('  ', ' ')
         .strip()
    )

def validate_list_against_set_fuzzy(
    item_list: Optional[List[str]],
    valid_items: Set[str],
    similarity_threshold: float = 0.95
) -> Tuple[List[Optional[str]], int]:
    if item_list is None or not item_list:
        return [], 0

    if not valid_items:
        return [None for _ in item_list], 0

    # Create normalized -> original map for case-accurate matches
    valid_normalized_map = {
        normalize_before_match(v): v for v in valid_items
    }

    exact_matches = {}
    to_fuzz = []

    for item in item_list:
        norm_item = normalize_before_match(item)
        if norm_item in valid_normalized_map:
            exact_matches[item] = valid_normalized_map[norm_item]
        else:
            to_fuzz.append(item)

    if not to_fuzz:
        return [exact_matches.get(item, None) for item in item_list], 0

    # Apply fuzzy matching on normalized inputs
    to_fuzz_series = pd.Series([normalize_before_match(i) for i in to_fuzz], dtype="object")
    valid_series = pd.Series(list(valid_items), dtype="object")

    matches_df = match_strings(to_fuzz_series, valid_series, min_similarity=similarity_threshold)
    fuzzy_matches = {
        row['left_side']: row['right_side']
        for _, row in matches_df.iterrows()
        if row['similarity'] >= similarity_threshold
    }

    transformed_count = 0
    final_result = []

    for item in item_list:
        if item in exact_matches:
            final_result.append(exact_matches[item])
        else:
            norm_item = normalize_before_match(item)
            match = fuzzy_matches.get(norm_item)
            final_result.append(match)
            if match:
                transformed_count += 1

    return final_result, transformed_count



# --------------------- Genre/Style Processing ---------------------

def process_genre_and_style(df: pl.DataFrame, valid_tags: Set[str]) -> pl.DataFrame:
    # --- Process Style ---
    df = df.with_columns(split_and_trim(pl.col("style")).alias("_style_split"))

    style_split = df["_style_split"].to_list()
    style_validated_result = [validate_list_against_set_fuzzy(lst, valid_tags) for lst in style_split]
    style_validated, style_fuzzy_counts = zip(*style_validated_result) if style_validated_result else ([], [])
    total_style_fuzzy = sum(style_fuzzy_counts)

    df = df.with_columns([
        pl.Series(name="_style_validated", values=style_validated).cast(pl.List(pl.Utf8))
    ])
    df = df.with_columns([
        pl.Series(name="_style_fuzzy_matched_count", values=style_fuzzy_counts).cast(pl.Int64)
    ])

    df = df.with_columns([
        pl.when(pl.col("_style_validated") != pl.col("_style_split"))
          .then(1).otherwise(0).alias("_style_mod_increment"),
        pl.col("_style_split").list.len().alias("_style_split_count"),
        pl.col("_style_validated").list.eval(pl.element().is_not_null()).list.sum().alias("_style_validated_count"),
    ])
    df = df.with_columns((pl.col("_style_split_count") - pl.col("_style_validated_count")).alias("_style_discarded_count"))

    validated_style_joined = pl.col("_style_validated").list.eval(
        pl.element().filter(pl.element().is_not_null())
    ).list.join(pl.lit(DELIMITER))

    df = df.with_columns(
        pl.when(pl.col("_style_validated") != pl.col("_style_split"))
          .then(pl.when(validated_style_joined == "").then(None).otherwise(validated_style_joined))
          .otherwise(None).alias("_replacement_style")
    ).drop(["_style_split_count", "_style_validated_count"])


    # --- Process Genre ---
    df = df.with_columns(split_and_trim(pl.col("genre")).alias("_genre_split"))

    genre_split = df["_genre_split"].to_list()
    genre_validated_result = [validate_list_against_set_fuzzy(lst, valid_tags) for lst in genre_split]
    genre_validated, genre_fuzzy_counts = zip(*genre_validated_result) if genre_validated_result else ([], [])
    total_genre_fuzzy = sum(genre_fuzzy_counts)

    df = df.with_columns([
        pl.Series(name="_genre_validated", values=genre_validated).cast(pl.List(pl.Utf8))
    ])
    df = df.with_columns([
        pl.Series(name="_genre_fuzzy_matched_count", values=genre_fuzzy_counts).cast(pl.Int64)
    ])

    df = df.with_columns([
        pl.when(pl.col("_genre_validated") != pl.col("_genre_split"))
          .then(1).otherwise(0).alias("_genre_mod_increment"),
        pl.col("_genre_split").list.len().alias("_genre_split_count"),
        pl.col("_genre_validated").list.eval(pl.element().is_not_null()).list.sum().alias("_genre_validated_count"),
    ])
    df = df.with_columns((pl.col("_genre_split_count") - pl.col("_genre_validated_count")).alias("_genre_discarded_count"))

    validated_genre_joined = pl.col("_genre_validated").list.eval(
        pl.element().filter(pl.element().is_not_null())
    ).list.join(pl.lit(DELIMITER))

    df = df.with_columns(
        pl.when(pl.col("_genre_validated") != pl.col("_genre_split"))
          .then(pl.when(validated_genre_joined == "").then(None).otherwise(validated_genre_joined))
          .otherwise(None).alias("_replacement_genre")
    ).drop(["_genre_split_count", "_genre_validated_count"])


    # --- Finalize ---
    df = df.with_columns(
        (pl.col("sqlmodded") + pl.col("_genre_mod_increment") + pl.col("_style_mod_increment")).alias("sqlmodded")
    )
    df = df.drop([
        "_genre_split", "_genre_validated", "_genre_mod_increment",
        "_style_split", "_style_validated", "_style_mod_increment",
    ])

    # Attach fuzzy match metadata
    df.meta = {
        "total_genre_fuzzy": total_genre_fuzzy,
        "total_style_fuzzy": total_style_fuzzy
    }

    return df



# --------------------- Main ---------------------

def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        logging.info(f"Importing data from {ALIB_TABLE}...")
        alib_df = import_genre_style_sqlmodded(conn)
        logging.info(f"Imported {alib_df.height} rows.")

        logging.info(f"Importing valid tags from {REF_VALIDATION_TABLE}...")
        valid_tags = import_valid_tags(conn)
        logging.info(f"Imported {len(valid_tags)} valid tags.")

        logging.info("Processing genre and style columns...")
        processed_df = process_genre_and_style(alib_df.clone(), valid_tags)

        rows_to_update = processed_df.filter(
            (pl.col("_replacement_genre").is_not_null()) | (pl.col("_replacement_style").is_not_null())
        )
        rows_to_update = rows_to_update.with_columns([
            processed_df["_genre_fuzzy_matched_count"],
            processed_df["_style_fuzzy_matched_count"]
        ])

        logging.info(f"Found {rows_to_update.height} rows to update.")
        logging.debug(f"Available columns: {rows_to_update.columns}")


        if rows_to_update.height > 0:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS changelog (
                    rowid INTEGER,
                    column TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    timestamp TEXT,
                    script TEXT
                )
            """)

            timestamp = datetime.now(timezone.utc).isoformat()
            script_name = "dropgenres-polars.py"
            sql = f"UPDATE {ALIB_TABLE} SET genre = COALESCE(?, genre), style = COALESCE(?, style), sqlmodded = ? WHERE rowid = ?"

            genre_lookup = {row["rowid"]: row["genre"] for row in rows_to_update.iter_rows(named=True)}
            style_lookup = {row["rowid"]: row["style"] for row in rows_to_update.iter_rows(named=True)}
            updated_row_count = 0

            for row in rows_to_update.iter_rows(named=True):
                rowid = row["rowid"]

                # Determine script source based on fuzzy matching
                style_fuzzy = row.get("_style_fuzzy_matched_count", 0)
                genre_fuzzy = row.get("_genre_fuzzy_matched_count", 0)
                script_name = (
                    "dropgenres-polars(string-grouper)"
                    if style_fuzzy > 0 or genre_fuzzy > 0
                    else "dropgenres-polars.py"
                )

                if row["_replacement_genre"] is not None:
                    old = genre_lookup.get(rowid)
                    cursor.execute(
                        "UPDATE alib SET genre = ?, sqlmodded = ? WHERE rowid = ?",
                        (row["_replacement_genre"], row["sqlmodded"], rowid)
                    )
                    cursor.execute(
                        "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                        (rowid, "genre", old, row["_replacement_genre"], timestamp, script_name)
                    )

                if row["_replacement_style"] is not None:
                    old = style_lookup.get(rowid)
                    cursor.execute(
                        "UPDATE alib SET style = ?, sqlmodded = ? WHERE rowid = ?",
                        (row["_replacement_style"], row["sqlmodded"], rowid)
                    )
                    cursor.execute(
                        "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                        (rowid, "style", old, row["_replacement_style"], timestamp, script_name)
                    )

                updated_row_count += 1


            conn.commit()
            logging.info(f"Updated {updated_row_count} rows in {ALIB_TABLE}.")

        # New enhanced fuzzy match logging
        total_genre_discarded = processed_df["_genre_discarded_count"].sum() if "_genre_discarded_count" in processed_df.columns else 0
        total_style_discarded = processed_df["_style_discarded_count"].sum() if "_style_discarded_count" in processed_df.columns else 0

        total_genre_fuzzy = processed_df.meta.get("total_genre_fuzzy", 0)
        total_style_fuzzy = processed_df.meta.get("total_style_fuzzy", 0)

        logging.info(f"Total genre tags discarded: {total_genre_discarded}")
        logging.info(f"Total genre tags transformed via fuzzy match: {total_genre_fuzzy}")
        logging.info(f"Total style tags discarded: {total_style_discarded}")
        logging.info(f"Total style tags transformed via fuzzy match: {total_style_fuzzy}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        conn.rollback()
        raise

    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()


# I'm going to upload the current version of the script, which now operates as intended.  Study it deeply and understand how it goes about what it does.  See if you can identify any opportunities for optimisation of performance without sacrificing any precision.  Show each optimisation and discuss them in turn
# delete from alib where rowid not in (select rowid from changelog);