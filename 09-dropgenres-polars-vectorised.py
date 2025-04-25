# --- imports unchanged ---
import polars as pl
import sqlite3
import logging
from typing import Set, List, Optional, Tuple, Dict
from string_grouper import match_strings
from datetime import datetime, timezone
import pandas as pd

# --- constants and config unchanged ---
DB_PATH = '/tmp/amg/dbtemplate.db'
ALIB_TABLE = 'alib'
REF_VALIDATION_TABLE = '_REF_genres'
DELIMITER = '\\\\'




# ------------------- Logging -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# ------------------- Normalization -------------------

def normalize_before_match(s: str) -> str:
    return (
        s.lower()
         .replace('&', '/')
         .replace('_', ' ')
         .replace('  ', ' ')
         .strip()
    )

# ------------------- Utility: Trim & Split -------------------

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

# ------------------- Vectorized Mapping Builder -------------------

def build_resolved_mapping(
    raw_tags: List[str], valid_tags: Set[str], similarity_threshold: float = 0.95
) -> Tuple[Dict[str, Optional[str]], Set[str]]:
    valid_normalized_map = {
        normalize_before_match(v): v for v in valid_tags
    }

    resolved = {}
    fuzzy_used = set()

    # Exact matches
    for tag in raw_tags:
        norm = normalize_before_match(tag)
        if norm in valid_normalized_map:
            resolved[tag] = valid_normalized_map[norm]

    # Fuzzy for the rest
    to_fuzz = [tag for tag in raw_tags if tag not in resolved]
    if not to_fuzz:
        return resolved, fuzzy_used

    fuzz_input_norm = [normalize_before_match(t) for t in to_fuzz]
    fuzz_matches = match_strings(
        pd.Series(fuzz_input_norm, dtype="object"),
        pd.Series(list(valid_tags), dtype="object"),
        min_similarity=similarity_threshold
    )

    norm_to_original = {normalize_before_match(t): t for t in to_fuzz}

    for _, row in fuzz_matches.iterrows():
        norm_input = row["left_side"]
        original = norm_to_original.get(norm_input)
        if original:
            resolved[original] = row["right_side"]
            fuzzy_used.add(original)

    # Unmatched get None
    for tag in to_fuzz:
        if tag not in resolved:
            resolved[tag] = None

    return resolved, fuzzy_used

# ------------------- Vectorized Application -------------------

def apply_resolved_map_to_column(
    df: pl.DataFrame,
    column: str,
    resolved_map: Dict[str, Optional[str]]
) -> pl.DataFrame:
    split_col = f"_{column}_split"
    validated_col = f"_{column}_validated"
    replacement_col = f"_replacement_{column}"
    mod_increment_col = f"_{column}_mod_increment"
    discarded_col = f"_{column}_discarded_count"

    df = df.with_columns(split_and_trim(pl.col(column)).alias(split_col))

    exploded = df.select(["rowid", split_col]).explode(split_col).rename({split_col: "tag"})

    mapped = exploded.with_columns(
        pl.col("tag").map_elements(lambda x: resolved_map.get(x, None), return_dtype=pl.Utf8).alias("mapped")
    )

    regrouped = mapped.group_by("rowid").agg([
        pl.col("tag").alias(split_col),
        pl.col("mapped").alias(validated_col)
    ])

    df = df.join(regrouped, on="rowid", how="left")

    joined = pl.col(validated_col).list.eval(
        pl.element().filter(pl.element().is_not_null())
    ).list.join(pl.lit(DELIMITER))

    df = df.with_columns([
        (pl.col(split_col) != pl.col(validated_col)).cast(pl.Int64).alias(mod_increment_col),
        (pl.col(split_col).list.len() - pl.col(validated_col).list.eval(pl.element().is_not_null()).list.sum())
            .alias(discarded_col),
        pl.when(pl.col(split_col) != pl.col(validated_col))
          .then(pl.when(joined == "").then(None).otherwise(joined))
          .otherwise(None)
          .alias(replacement_col)
    ])

    return df


# ------------------- Data Import -------------------

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    return cursor.fetchone() is not None

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

# ------------------- Main -------------------

def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        logging.info("Importing data...")
        df = import_genre_style_sqlmodded(conn)
        logging.info(f"Imported {df.height} rows.")

        valid_tags = import_valid_tags(conn)
        logging.info(f"Imported {len(valid_tags)} valid tags.")

        logging.info("Building unique tag pool...")
        all_tags = (
            df.select([
                split_and_trim(pl.col("genre")).alias("genre_split"),
                split_and_trim(pl.col("style")).alias("style_split")
            ])
            .unpivot(value_name="value", variable_name="column")
            .explode("value")
            .drop_nulls()
            .unique(subset=["value"])
            .get_column("value")
            .to_list()
        )

        resolved_map, fuzzy_used = build_resolved_mapping(all_tags, valid_tags)
        fuzzy_used_set = set(fuzzy_used)

        logging.info("Applying resolved genre/style tags...")

        df = apply_resolved_map_to_column(df, "genre", resolved_map)
        df = apply_resolved_map_to_column(df, "style", resolved_map)

        df = df.with_columns(
            (pl.col("sqlmodded") + pl.col("_genre_mod_increment") + pl.col("_style_mod_increment")).alias("sqlmodded")
        )

        to_update = df.filter(
            (pl.col("_replacement_genre").is_not_null()) | (pl.col("_replacement_style").is_not_null())
        )

        if to_update.is_empty():
            logging.info("No changes to apply.")
            return

        logging.info(f"Updating {to_update.height} rows...")

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
        genre_lookup = {r["rowid"]: r["genre"] for r in to_update.iter_rows(named=True)}
        style_lookup = {r["rowid"]: r["style"] for r in to_update.iter_rows(named=True)}

        updated = 0
        for row in to_update.iter_rows(named=True):
            rowid = row["rowid"]
            genre_old = genre_lookup.get(rowid)
            style_old = style_lookup.get(rowid)

            # Detect fuzzy match usage (handle None safely)
            genre_list = row.get("_genre_split") or []
            style_list = row.get("_style_split") or []

            genre_fuzz = any(item in fuzzy_used_set for item in genre_list)
            style_fuzz = any(item in fuzzy_used_set for item in style_list)

            script = (
                "dropgenres-polars(string-grouper)"
                if genre_fuzz or style_fuzz else
                "dropgenres-polars.py"
            )

            if row["_replacement_genre"] is not None:
                cursor.execute(
                    f"UPDATE {ALIB_TABLE} SET genre = ?, sqlmodded = ? WHERE rowid = ?",
                    (row["_replacement_genre"], row["sqlmodded"], rowid)
                )
                cursor.execute(
                    "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, "genre", genre_old, row["_replacement_genre"], timestamp, script)
                )

            if row["_replacement_style"] is not None:
                cursor.execute(
                    f"UPDATE {ALIB_TABLE} SET style = ?, sqlmodded = ? WHERE rowid = ?",
                    (row["_replacement_style"], row["sqlmodded"], rowid)
                )
                cursor.execute(
                    "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, "style", style_old, row["_replacement_style"], timestamp, script)
                )

            updated += 1

        conn.commit()
        logging.info(f"Updated {updated} rows.")

        # Summary
        logging.info(f"Total genre tags transformed via fuzzy match: {sum(1 for t in df['_genre_split'].explode().to_list() if t in fuzzy_used_set)}")
        logging.info(f"Total style tags transformed via fuzzy match: {sum(1 for t in df['_style_split'].explode().to_list() if t in fuzzy_used_set)}")
        logging.info(f"Total genre tags discarded: {df['_genre_discarded_count'].sum()}")
        logging.info(f"Total style tags discarded: {df['_style_discarded_count'].sum()}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        conn.rollback()
        raise

    finally:
        conn.close()
        logging.info("Database connection closed.")



if __name__ == "__main__":
    main()
