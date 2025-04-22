"""
Script Name: contributors-polars.py

Purpose:
    This script processes all records in alib containing one or more contributor entries.  
    For each track it checks if any contributor names match entries in the reference dictionary (case-insensitive)
    and replaces them with the standardized versions from _REF_vetted_contributors if they differ.

    It is the de-facto way of ensuring artist name consistency throughout your music collection.
    To update MBIDs in alib you should run mbids-polars.py afterward.

    It is part of tagminder.

Usage:
    python contributors-polars.py
    uv run contributors-polars.py

Author: audiomuze
Created: 2025-04-18
"""

import polars as pl
import sqlite3
from typing import Dict, List, Tuple, Any, Union
import logging
from datetime import datetime, timezone

# ---------- Config ----------
SCRIPT_NAME = "contributors-polars.py"

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Helpers ----------
def sqlite_to_polars(
    conn: sqlite3.Connection,
    query: str,
    id_column: Union[str, Tuple[str, ...]] = None
) -> pl.DataFrame:
    cursor = conn.cursor()
    cursor.execute(query)
    
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    
    data = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]
        
        if col_name in ("rowid", "sqlmodded"):
            data[col_name] = pl.Series(
                name=col_name,
                values=[int(x or 0) for x in col_data],
                dtype=pl.Int64
            )
        else:
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8
            )
    
    return pl.DataFrame(data)

def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame, 
    original_df: pl.DataFrame,
    changed_rowids: List[int],
    columns_to_update: List[str]
) -> int:
    if not changed_rowids:
        logging.info("No changes to write to database")
        return 0

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")

    # Ensure changelog table exists
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
    updated_count = 0

    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)

        changed_cols = []
        for col in columns_to_update:
            if record[col] != original_row[col] and record[col] is not None:
                changed_cols.append(col)
                cursor.execute(
                    "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        rowid,
                        col,
                        original_row[col],
                        record[col],
                        timestamp,
                        SCRIPT_NAME
                    )
                )

        if not changed_cols:
            continue

        new_sqlmodded = int(original_row["sqlmodded"]) + len(changed_cols)
        set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
        values = [record[col] for col in changed_cols] + [new_sqlmodded, rowid]

        cursor.execute(f"UPDATE alib SET {set_clause} WHERE rowid = ?", values)
        updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count

# ---------- Main ----------
def main():
    db_path = '/tmp/amg/dbtemplate.db'
    logging.info(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)

    try:
        # Fetch contributors
        logging.info("Fetching contributors data...")
        contributors = sqlite_to_polars(
            conn,
            "SELECT current_val, replacement_val FROM _REF_vetted_contributors"
        )
        contributors_dict = dict(zip(
            contributors["current_val"].str.to_lowercase(),
            contributors["replacement_val"]
        ))

        # Fetch track data
        logging.info("Fetching tracks data...")
        tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid,
                   artist, composer, arranger, lyricist, writer,
                   albumartist, ensemble, performer, personnel,
                   conductor, producer, engineer, mixer, remixer,
                   COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            ORDER BY rowid
            """,
            id_column=("rowid", "sqlmodded")
        )

        columns_to_replace = [
            "artist", "composer", "arranger", "lyricist", "writer",
            "albumartist", "ensemble", "performer", "personnel",
            "conductor", "producer", "engineer", "mixer", "remixer"
        ]

        # Filter to rows with contributor data
        filter_expr = None
        for col in columns_to_replace:
            expr = pl.col(col).is_not_null()
            filter_expr = expr if filter_expr is None else (filter_expr | expr)

        tracks_filtered = tracks.filter(filter_expr)
        logging.info(f"Processing {tracks_filtered.height} tracks with data...")

        updated_tracks = tracks_filtered.clone()

        for col in columns_to_replace:
            updated_tracks = updated_tracks.with_columns(
                pl.col(col).map_elements(
                    lambda x: contributors_dict.get(x.lower(), x) if x is not None else None,
                    return_dtype=pl.Utf8
                ).alias(col)
            )

        # Detect changes
        logging.info("Detecting changes...")
        change_masks = [
            (tracks_filtered[col].is_not_null()) & (tracks_filtered[col] != updated_tracks[col])
            for col in columns_to_replace
        ]
        final_change_mask = None
        for mask in change_masks:
            final_change_mask = mask if final_change_mask is None else (final_change_mask | mask)

        changed_rowids = updated_tracks.filter(final_change_mask)["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes")

        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks,
                original_df=tracks_filtered,
                changed_rowids=changed_rowids,
                columns_to_update=columns_to_replace
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    main()
