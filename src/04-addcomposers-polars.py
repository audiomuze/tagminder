"""
Script Name: addcomposers-polars.py

Purpose:
    Adds composer metadata to tracks based on artist and title match against the composer occurence with
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
    """
    Normalize a delimited string into a sorted list of unique, lowercased parts.
    
    Args:
        s: Input string with delimiters (;,/&,\\, and)
        
    Returns:
        Sorted list of normalized string parts
    """
    if s is None:
        return []
    parts = re.split(r"[;,/&]|\\\\| and ", s.lower())
    return sorted(set(part.strip() for part in parts if part.strip()))

def normalize_title(title: str) -> str:
    """
    Normalize a title by lowercasing, removing live annotations, and stripping punctuation.
    
    Args:
        title: Input title string
        
    Returns:
        Normalized title string
    """
    if not title:
        return ""
    title = title.lower()
    title = re.sub(r"\(live.*|\[live.*", "", title)
    title = re.sub(r"[^\w\s]", "", title)
    return title.strip()

def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """
    Fetch track data from the database and normalize it.
    
    Args:
        conn: SQLite database connection
        
    Returns:
        Polars DataFrame with normalized track data
    """
    query = """
        SELECT rowid, COALESCE(sqlmodded, 0) as sqlmodded, title, composer, artist, albumartist
        FROM alib
    """

    cursor = conn.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Create DataFrame with explicit row orientation to avoid warning
    df = pl.DataFrame(
        data=rows,
        schema=columns,
        orient="row",  # Explicitly specify row orientation
        infer_schema_length=len(rows)  # Ensure proper type inference for long strings
    )

    # Fill nulls and normalize with proper casting
    return df.with_columns([
        pl.col("rowid").cast(pl.Int64),
        pl.col("sqlmodded").cast(pl.Int64),
        pl.col("title").cast(pl.String).fill_null(""),
        pl.col("composer").cast(pl.String).fill_null(""),
        pl.col("artist").cast(pl.String).fill_null(""),
        pl.col("albumartist").cast(pl.String).fill_null(""),
        pl.col("title").map_elements(normalize_title, return_dtype=pl.String).alias("norm_title"),
    ])

def infer_composers_by_exploded_artist(df: pl.DataFrame) -> pl.DataFrame:
    """
    Infer composers based on majority vote across artist/albumartist and title combinations.
    
    For each normalized title + artist combination, find the most common composer value.
    
    Args:
        df: Input DataFrame with track data
        
    Returns:
        DataFrame with inferred composers per (norm_title, single_artist) group
    """
    # Normalize artist/albumartist/composer fields into list parts
    df = df.with_columns([
        pl.col("artist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("artist_parts"),
        pl.col("albumartist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("albumartist_parts"),
        pl.col("composer").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("composer_parts"),
        pl.col("composer").alias("original_composer"),
    ])

    # Explode artist and albumartist into individual rows
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

    # Combine both artist sources
    combined = pl.concat([artist_df, albumartist_df])

    # Filter to rows with composer data and create normalized key
    valid = combined.filter(pl.col("composer_parts").list.len() > 0).with_columns([
        pl.col("composer_parts").map_elements(lambda parts: "|".join(parts), return_dtype=pl.String).alias("norm_key")
    ])

    # Count occurrences of each composer for each (title, artist) combination
    counts = (
        valid.group_by(["norm_title", "single_artist", "norm_key", "original_composer"])
        .agg(pl.len().alias("count"))
    )

    # Select the most common composer for each (title, artist) combination
    top = (
        counts.sort("count", descending=True)
        .group_by(["norm_title", "single_artist"])
        .agg(pl.first("original_composer").alias("inferred_composer"))
    )

    logging.info(f"Inferred {top.height} composer groups via majority vote")
    return top

def apply_composer_propagation(df: pl.DataFrame, inferred: pl.DataFrame) -> pl.DataFrame:
    """
    Apply inferred composers to tracks missing composer metadata.
    
    Matches tracks to inferred composers based on normalized title and artist/albumartist.
    Only updates tracks where composer field is empty.
    
    Args:
        df: Original track DataFrame
        inferred: DataFrame with inferred composers
        
    Returns:
        DataFrame with new_composer and new_sqlmodded columns added
    """
    # Normalize fields for matching
    df = df.with_columns([
        pl.col("artist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("artist_parts"),
        pl.col("albumartist").map_elements(normalize_list, return_dtype=pl.List(pl.String)).alias("albumartist_parts"),
        pl.col("title").map_elements(normalize_title, return_dtype=pl.String).alias("norm_title")
    ])

    # Explode artist and albumartist for matching
    artist_df = df.explode("artist_parts").select([
        "rowid", "norm_title", pl.col("artist_parts").alias("single_artist")
    ])
    albumartist_df = df.explode("albumartist_parts").select([
        "rowid", "norm_title", pl.col("albumartist_parts").alias("single_artist")
    ])

    # Combine and deduplicate matching keys
    joined_keys = pl.concat([artist_df, albumartist_df]).unique()
    
    # Join with inferred composers
    matched = joined_keys.join(inferred, on=["norm_title", "single_artist"], how="left")
    
    # Aggregate back to one row per track (take first non-null inferred composer)
    composer_matches = matched.group_by("rowid").agg([
        pl.col("inferred_composer").drop_nulls().first().alias("inferred_composer")
    ])

    # Join inferred composers back to original DataFrame
    enriched = df.join(composer_matches, on="rowid", how="left")

    # Apply composer only if original is empty and we have an inferred value
    enriched = enriched.with_columns([
        pl.when((pl.col("composer") == "") & pl.col("inferred_composer").is_not_null())
          .then(pl.col("inferred_composer"))
          .otherwise(pl.col("composer")).alias("new_composer")
    ])
    
    # Increment sqlmodded counter if composer changed
    enriched = enriched.with_columns([
        (pl.col("sqlmodded") + ((pl.col("composer") != pl.col("new_composer")).cast(pl.Int64))).alias("new_sqlmodded")
    ])

    return enriched.select([
        "rowid", "title", "artist", "albumartist", "composer",
        "new_composer", "sqlmodded", "new_sqlmodded"
    ])

def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    """
    Write composer updates to database and log changes.
    
    Only updates rows where composer has actually changed.
    Creates changelog table if it doesn't exist.
    
    Args:
        conn: SQLite database connection
        original: Original DataFrame (unused but kept for signature compatibility)
        updated: DataFrame with new composer values
        
    Returns:
        Number of rows updated
    """
    # Filter to only rows where composer changed
    changed = updated.filter((pl.col("composer").fill_null("") != pl.col("new_composer").fill_null("")))
    if changed.is_empty():
        logging.info("No changes to write.")
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

    updates = 0
    timestamp = datetime.now(timezone.utc).isoformat()
    script_name = "addcomposers-polars.py"

    # Update each changed row and log the change
    for row in changed.to_dicts():
        cursor.execute(
            "UPDATE alib SET composer = ?, sqlmodded = ? WHERE rowid = ?",
            (row["new_composer"], row["new_sqlmodded"], row["rowid"])
        )
        cursor.execute(
            "INSERT INTO changelog (alib_rowid, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            (row["rowid"], "composer", row["composer"], row["new_composer"], timestamp, script_name)
        )
        updates += 1

    conn.commit()
    logging.info(f"Updated and logged {updates} composer fields.")
    return updates

def main():
    """Main execution function."""
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        logging.info(f"Using Polars version: {pl.__version__}")
        
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
