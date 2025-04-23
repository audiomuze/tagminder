"""
Script Name: islive-polars.py

Purpose:
    Normalize 'live' markers from title into subtitle and set the live flag:
        - Remove (live), [live], etc. from title
        - Add [Live] to subtitle if there's no other live form
        - Set live = '1' where applicable
        - Track and write only modified rows
        - Log all changes to a 'changes' table

Optimized for speed using Polars vectorized expressions.

It is part of tagminder.

Usage:
    python islive-polars.py
    uv run islive-polars.py

Author: audiomuze
Created: 2025-04-20

"""

import sqlite3
import polars as pl
import logging
import re
from datetime import datetime, timezone

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["title", "subtitle", "album", "live"]

# ---------- Regex patterns ----------
LIVE_CLEAN_PATTERN = r"(?i)(?:[\(\[\{<]\s*live\s*[\)\]\}>]|- live)\s*$"  # Standalone 'live' at end
LIVE_WORD_PATTERN = r"(?i)\blive\b"  # For subtitle check

# ---------- Fetch data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, title, subtitle, album, live, COALESCE(sqlmodded, 0) as sqlmodded
        FROM alib
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]

        if name == "rowid":
            data[name] = pl.Series(name=name, values=col_data, dtype=pl.Int64)
        elif name == "sqlmodded":
            data[name] = pl.Series(name=name, values=[int(x) if x is not None else 0 for x in col_data], dtype=pl.Int64)
        elif name == "live":
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "0" for x in col_data], dtype=pl.Utf8)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else None for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Apply normalization ----------
def apply_live_normalization(df: pl.DataFrame) -> pl.DataFrame:
    # Clean title and album (if they end with bracketed or - live)
    df = df.with_columns([
        pl.col("title").str.replace_all(LIVE_CLEAN_PATTERN, "").str.strip_chars().alias("new_title"),
        pl.col("album").str.replace_all(LIVE_CLEAN_PATTERN, "").str.strip_chars().alias("new_album"),
    ])

    # Detect changes
    title_changed = pl.col("title") != pl.col("new_title")
    album_changed = pl.col("album") != pl.col("new_album")

    # Subtitle: append [Live] only if 'Live' isn't already present
    subtitle_updated = pl.when(
        pl.col("subtitle").is_not_null() & pl.col("subtitle").str.contains(LIVE_WORD_PATTERN)
    ).then(
        pl.col("subtitle")
    ).otherwise(
        pl.when(pl.col("subtitle").is_not_null())
        .then(
            pl.concat_str([
                pl.col("subtitle").str.strip_chars(),
                pl.lit("[Live]")
            ], separator=" ").str.strip_chars()
        )
        .otherwise(pl.lit("[Live]"))
    )

    # Set live = '1' if not already
    df = df.with_columns([
        subtitle_updated.alias("new_subtitle"),
        pl.when(pl.col("live") != "1")
            .then(pl.lit("1"))
            .otherwise(pl.col("live"))
            .alias("new_live"),
    ])

    subtitle_changed = pl.col("subtitle") != pl.col("new_subtitle")
    live_changed = pl.col("live") != pl.col("new_live")

    sqlmodded_delta = (
        title_changed.cast(pl.Int32()) +
        album_changed.cast(pl.Int32()) +
        subtitle_changed.cast(pl.Int32()) +
        live_changed.cast(pl.Int32())
    )

    df = df.with_columns([
        pl.col("new_title").alias("title"),
        pl.col("new_album").alias("album"),
        pl.col("new_subtitle").alias("subtitle"),
        pl.col("new_live").alias("live"),
        (pl.col("sqlmodded") + sqlmodded_delta).alias("sqlmodded")
    ])

    return df.drop(["new_title", "new_album", "new_subtitle", "new_live"])

# ---------- Write updates ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("sqlmodded") > original["sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS changes (
    #         rowid INTEGER,
    #         column TEXT,
    #         old_value TEXT,
    #         new_value TEXT
    #     )
    # """)
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
    script_name = "islive-polars.py"

    for record in changed.to_dicts():
        rowid = record["rowid"]
        original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)

        changed_cols = []
        for col in COLUMNS:
            if record[col] != original_row[col]:
                changed_cols.append(col)
                # cursor.execute(
                #     "INSERT INTO changes (rowid, column, old_value, new_value) VALUES (?, ?, ?, ?)",
                #     (rowid, col, original_row[col], record[col])
                # )
                cursor.execute(
                    "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, col, original_row[col], record[col], timestamp, script_name)
                )

        if changed_cols:
            set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
            values = [record[col] for col in changed_cols] + [int(record["sqlmodded"]), rowid]  # Enforce type safety
            cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)
            updates += 1

    conn.commit()
    logging.info(f"Updated {updates} rows and logged all changes.")
    return updates

# ---------- Main ----------
def main():
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} rows")

        original_df = df.clone()
        updated_df = apply_live_normalization(df)

        changed_rows = updated_df.filter(pl.col("sqlmodded") > original_df["sqlmodded"]).height
        logging.info(f"Detected {changed_rows} rows with changes")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
