"""
Purpose:
    Detect and set compilation flags based on artist data and directory path
    patterns, and update `albumartist` when applicable.

    Only modified rows are written back; changes are logged to `changelog` and
    `__sqlmodded` is incremented.

Optimized for speed using Polars vectorized expressions.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import sqlite3
import polars as pl
import logging

from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_changes
from tagminder.core import tm_run
# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Config ----------
COLUMNS = ["__dirpath", "compilation", "artist", "albumartist"]

# ---------- Fetch data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch data from database including __dirpath, compilation, artist, and albumartist columns."""
    query = """
        SELECT rowid, __dirpath, compilation, artist, albumartist, COALESCE(__sqlmodded, 0) as __sqlmodded
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
            data[name] = tm_polars.series_rowid(col_data)
        elif name == "__sqlmodded":
            data[name] = tm_polars.series_sqlmodded(col_data)
        else:
            # Handle all text columns (compilation, artist, albumartist, __dirpath)
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "" for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Helper functions ----------
def count_unique_artists_per_dirpath(df: pl.DataFrame) -> pl.DataFrame:
    """Count unique artists per directory path."""
    return df.group_by("__dirpath").agg([
        pl.col("artist").n_unique().alias("unique_artist_count"),
        pl.col("artist").first().alias("first_artist")  # For single-artist case
    ])

def is_empty_or_null(col: pl.Expr) -> pl.Expr:
    """Check if column value is empty, null, or whitespace-only."""
    # Use str.strip_chars() instead of str.strip() and handle potential None values
    return col.is_null() | (col.str.strip_chars() == "")

def is_various_artists(col: pl.Expr) -> pl.Expr:
    """Check if albumartist indicates various artists (case insensitive)."""
    return col.str.to_lowercase().str.strip_chars(" \t\n\r").is_in(["various artists", "various"])

# ---------- Apply artist-based compilation detection ----------
def apply_artist_based_detection(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply artist-based compilation detection logic with priority over path patterns.
    
    Rules:
    1) Empty/null albumartist + multiple artists = compilation
    2) Empty/null albumartist + single artist = not compilation, set albumartist = artist  
    3) albumartist = 'various artists'/'various' + multiple artists = compilation
    """
    # Get unique artist counts per dirpath
    artist_counts = count_unique_artists_per_dirpath(df)
    
    # Join artist counts back to main dataframe
    df = df.join(artist_counts, on="__dirpath", how="left")
    
    # Create conditions for each rule
    empty_albumartist = is_empty_or_null(pl.col("albumartist"))
    various_albumartist = is_various_artists(pl.col("albumartist"))
    multiple_artists = pl.col("unique_artist_count") > 1
    single_artist = pl.col("unique_artist_count") == 1
    
    # Rule 1: Empty albumartist + multiple artists = compilation
    rule1_condition = empty_albumartist & multiple_artists
    
    # Rule 2: Empty albumartist + single artist = not compilation, set albumartist
    rule2_condition = empty_albumartist & single_artist
    
    # Rule 3: Various artists albumartist + multiple artists = compilation  
    rule3_condition = various_albumartist & multiple_artists
    
    # Apply the rules with priority
    df = df.with_columns([
        # New compilation value
        pl.when(rule1_condition | rule3_condition)
        .then(pl.lit("1"))
        .when(rule2_condition)
        .then(pl.lit("0"))
        .otherwise(pl.col("compilation"))  # Keep existing value
        .alias("new_compilation"),
        
        # New albumartist value
        pl.when(rule2_condition)
        .then(pl.col("first_artist"))  # Set to artist value for single-artist albums
        .otherwise(pl.col("albumartist"))  # Keep existing value
        .alias("new_albumartist"),
        
        # Track which rows were modified by artist-based rules
        (rule1_condition | rule2_condition | rule3_condition).alias("artist_rule_applied")
    ])
    
    return df

# ---------- Apply directory path compilation detection ----------
def apply_path_based_detection(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply directory path-based compilation detection only to rows not modified by artist rules.
    
    Logic: Extract the segment after the last "/" and check if it starts with compilation indicators
    """
    # Split the dirpath by "/" and get the last segment 
    df = df.with_columns([
        pl.col("__dirpath")
        .str.split("/")
        .list.get(-1, null_on_oob=True)
        .alias("last_segment")
    ])

    # Check if the last segment starts with compilation patterns
    compilation_pattern = (
        pl.col("last_segment").str.starts_with("VA - ") | 
        pl.col("last_segment").str.starts_with("/VA/") |
        pl.col("last_segment").str.starts_with("Various Artists - ") |
        pl.col("last_segment").str.starts_with("/OST/") |
        pl.col("last_segment").str.starts_with("OST - ")
    )

    # Apply path-based detection only to rows NOT modified by artist rules
    df = df.with_columns([
        pl.when(~pl.col("artist_rule_applied") & compilation_pattern)
        .then(pl.lit("1"))
        .when(~pl.col("artist_rule_applied") & ~compilation_pattern)
        .then(pl.lit("0"))
        .otherwise(pl.col("new_compilation"))  # Keep artist-rule results
        .alias("final_compilation")
    ])

    return df.drop(["last_segment"])

# ---------- Apply all compilation detection ----------
def apply_compilation_detection(df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply comprehensive compilation detection with artist-based rules taking priority.
    """
    logging.info("Applying artist-based compilation detection...")
    df = apply_artist_based_detection(df)
    
    # Log artist-based changes
    artist_modified = df.filter(pl.col("artist_rule_applied")).height
    logging.info(f"Artist-based rules modified {artist_modified} rows")
    
    logging.info("Applying path-based compilation detection to remaining rows...")
    df = apply_path_based_detection(df)
    
    # Detect all changes (compilation and albumartist)
    compilation_changed = pl.col("compilation") != pl.col("final_compilation")
    albumartist_changed = pl.col("albumartist") != pl.col("new_albumartist")
    any_change = compilation_changed | albumartist_changed
    
    # Calculate __sqlmodded delta
    sqlmodded_delta = any_change.cast(pl.Int32())
    
    # Update the dataframe with final values
    df = df.with_columns([
        pl.col("final_compilation").alias("compilation"),
        pl.col("new_albumartist").alias("albumartist"),
        (pl.col("__sqlmodded") + sqlmodded_delta).cast(pl.Int16).alias("__sqlmodded")
    ])
    
    # Clean up temporary columns
    return df.drop([
        "unique_artist_count", "first_artist", "new_compilation", 
        "new_albumartist", "final_compilation", "artist_rule_applied"
    ])

# ---------- Write updates ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    """Write updates to database and log changes."""
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)

    updates = 0
    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()

    changed_rowids = changed["rowid"].to_list()
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for record in changed.to_dicts():
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            def _norm(v: object) -> str | None:
                if v is None:
                    return None
                return v if isinstance(v, str) else str(v)

            changes: list[tuple[str, object, object]] = []
            changed_cols: list[str] = []
            for col in COLUMNS:
                old_v = original_row.get(col)
                new_v = record.get(col)
                if _norm(old_v) == _norm(new_v):
                    continue
                changes.append((col, old_v, new_v))
                changed_cols.append(col)

            if changes:
                changelog.add(alib_path=alib_path, changes=changes)

                sql = tm_db.build_update_sql(table="alib", set_cols=changed_cols)
                values = [record[col] for col in changed_cols] + [int(record["__sqlmodded"] or 0), rowid]
                cursor.execute(sql, values)
                updates += 1

            changelog.flush(cursor)

    logging.info(f"Updated {updates} rows and logged all changes.")
    return updates

# ---------- Analyze results ----------
def analyze_compilation_results(df: pl.DataFrame) -> None:
    """Log analysis of compilation detection results for debugging."""
    # Get distinct dirpaths and their compilation status
    distinct_paths = df.select([
        "__dirpath", "compilation", "artist", "albumartist"
    ]).unique().sort("__dirpath")

    logging.info(f"Found {distinct_paths.height} distinct directory paths")

    # Show sample compilation matches
    compilations = distinct_paths.filter(pl.col("compilation") == "1")
    if not compilations.is_empty():
        logging.info(f"Found {compilations.height} compilation paths")
        sample_compilations = compilations.select(["__dirpath", "albumartist"]).head(10)
        logging.info("Sample compilations:")
        for row in sample_compilations.to_dicts():
            logging.info(f"  {row['__dirpath']} (albumartist: '{row['albumartist']}')")

    # Show sample regular albums
    non_compilations = distinct_paths.filter(pl.col("compilation") == "0")
    if not non_compilations.is_empty():
        sample_regular = non_compilations.select(["__dirpath", "albumartist"]).head(5)
        logging.info("Sample regular albums:")
        for row in sample_regular.to_dicts():
            logging.info(f"  {row['__dirpath']} (albumartist: '{row['albumartist']}')")

# ---------- Main ----------
def main():
    """Main execution function."""
    logging.info("Connecting to database...")
    try:
        conn, _, _, _ = tm_run.open_db(ensure_changelog=True, log_connect=False, require_exists=True)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    if not tm_db.table_exists(conn, "alib"):
        logging.error("Required table 'alib' not found in database")
        conn.close()
        return

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} rows")

        original_df = df.clone()
        updated_df = apply_compilation_detection(df)

        # Analyze the results
        analyze_compilation_results(updated_df)

        changed_rows = updated_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info(f"Detected {changed_rows} rows with changes")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
        else:
            logging.info("No compilation flags needed updating.")

    except Exception as e:
        logging.error(f"Error during processing: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()