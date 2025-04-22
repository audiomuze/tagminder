"""
Script Name: addcomposers-polars.py

Purpose:
    Adds composer metadata to tracks based on artist and title match against the composr occurence with
    the most instances in your collection.

    It is the de-facto way of adding composer tags to tracks where the same track is performed elsewhere
    in your library and has composer metadata.

    It is part of tagminder.

Usage:
    python addcomposers-polars.py
    uv run addcomposers-polars.py

Author: audiomuze
Created: 2025-04-21

"""
import sqlite3
import polars as pl
import logging
import re
from datetime import datetime, timezone

DB_PATH = "/tmp/amg/dbtemplate.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def normalize_list(s: str) -> list[str]:
    if s is None:
        return []
    parts = re.split(r"[;,/&]|\\\\| and ", s.lower())
    return sorted(set(part.strip() for part in parts if part.strip()))

def normalize_title(title: str) -> str:
    if not title:
        return ""
    title = title.lower()
    title = re.sub(r"\(live.*|\[live.*", "", title)
    title = re.sub(r"[^\w\s]", "", title)
    return title.strip()

def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, COALESCE(sqlmodded, 0) as sqlmodded, title, composer, artist, albumartist
        FROM alib
    """
    df = pl.read_database(query, conn)
    df = df.with_columns([
    pl.col("sqlmodded").cast(pl.Int64)
    ])

    return df.with_columns([
        pl.col("composer").fill_null("").alias("composer"),
        pl.col("artist").fill_null("").alias("artist"),
        pl.col("albumartist").fill_null("").alias("albumartist"),
        pl.col("title").fill_null("").alias("title"),
        pl.col("title").map_elements(normalize_title, return_dtype=pl.String).alias("norm_title"),
    ])

def infer_composers_by_exploded_artist(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("artist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("artist_parts"),
        pl.col("albumartist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("albumartist_parts"),
        pl.col("composer").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("composer_parts"),
        pl.col("composer").alias("original_composer"),
    ])

    artist_df = df.explode("artist_parts").select([
        pl.col("norm_title"),
        pl.col("artist_parts").alias("single_artist"),
        pl.col("composer_parts"),
        pl.col("original_composer"),
    ])
    albumartist_df = df.explode("albumartist_parts").select([
        pl.col("norm_title"),
        pl.col("albumartist_parts").alias("single_artist"),
        pl.col("composer_parts"),
        pl.col("original_composer"),
    ])

    combined = pl.concat([artist_df, albumartist_df])

    valid = combined.filter(pl.col("composer_parts").list.len() > 0).with_columns([
        pl.col("composer_parts").map_elements(lambda parts: "|".join(parts), return_dtype=pl.String).alias("norm_key")
    ])

    counts = (
        valid.group_by(["norm_title", "single_artist", "norm_key", "original_composer"])
        .agg(pl.len().alias("count"))
    )

    top = (
        counts.sort("count", descending=True)
        .group_by(["norm_title", "single_artist"])
        .agg(pl.first("original_composer").alias("inferred_composer"))
    )

    logging.info(f"Inferred {top.height} composer groups via majority vote")
    return top

def apply_composer_propagation(df: pl.DataFrame, inferred: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("artist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("artist_parts"),
        pl.col("albumartist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("albumartist_parts"),
        pl.col("title").map_elements(normalize_title, return_dtype=pl.String).alias("norm_title")
    ])

    artist_df = df.explode("artist_parts").select([
        "rowid", "norm_title", pl.col("artist_parts").alias("single_artist")
    ])
    albumartist_df = df.explode("albumartist_parts").select([
        "rowid", "norm_title", pl.col("albumartist_parts").alias("single_artist")
    ])

    joined_keys = pl.concat([artist_df, albumartist_df]).unique()
    matched = joined_keys.join(inferred, on=["norm_title", "single_artist"], how="left")
    composer_matches = matched.group_by("rowid").agg([
        pl.col("inferred_composer").drop_nulls().first().alias("inferred_composer")
    ])

    enriched = df.join(composer_matches, on="rowid", how="left")

    enriched = enriched.with_columns([
        pl.when((pl.col("composer") == "") & pl.col("inferred_composer").is_not_null())
          .then(pl.col("inferred_composer"))
          .otherwise(pl.col("composer")).alias("new_composer")
    ])
    enriched = enriched.with_columns([
        (pl.col("sqlmodded") + ((pl.col("composer") != pl.col("new_composer")).cast(pl.Int64()))).alias("new_sqlmodded")
    ])

    return enriched.select([
        "rowid", "title", "artist", "albumartist", "composer",
        "new_composer", "sqlmodded", "new_sqlmodded"
    ])

def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter((pl.col("composer").fill_null("") != pl.col("new_composer").fill_null("")))
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
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

    updates = 0
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "addcomposers-polars.py"

    for row in changed.to_dicts():
        cursor.execute(
            "UPDATE alib SET composer = ?, sqlmodded = ? WHERE rowid = ?",
            (row["new_composer"], row["new_sqlmodded"], row["rowid"])
        )
        cursor.execute(
            "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            (row["rowid"], "composer", row["composer"], row["new_composer"], timestamp, script_name)
        )

        updates += 1

    conn.commit()
    logging.info(f"Updated and logged {updates} composer fields.")
    return updates

def main():
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} rows")

        inferred_df = infer_composers_by_exploded_artist(df)
        updated_df = apply_composer_propagation(df, inferred_df)
        write_updates(conn, df, updated_df)

    finally:
        conn.close()
        logging.info("Connection closed.")

if __name__ == "__main__":
    main()
