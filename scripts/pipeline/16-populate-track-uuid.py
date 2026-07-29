"""
Purpose:
    Populate missing `track_uuid` values in `alib`.

    For each row where `track_uuid` is NULL/empty, the script:
    - generates a new UUIDv7
    - updates `alib.track_uuid`
    - increments `__sqlmodded`
    - logs the change to `changelog`

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-26
"""

import sqlite3
import polars as pl
import logging
import uuid
from typing import cast

from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_changes
from tagminder.core import tm_run
# ---------- Config ----------


# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch UUID Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch rows that need UUID generation."""
    query = """
        SELECT rowid, track_uuid, COALESCE(__sqlmodded, 0) as __sqlmodded
        FROM alib
        WHERE track_uuid IS NULL
           OR TRIM(track_uuid) = ''
           OR substr(track_uuid, 15, 1) != '7'
           ORDER BY __path
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        return pl.DataFrame({
            "rowid": pl.Series("rowid", [], dtype=pl.Int64),
            "track_uuid": pl.Series("track_uuid", [], dtype=pl.Utf8),
            "__sqlmodded": pl.Series("__sqlmodded", [], dtype=pl.Int16),
        })

    data: dict[str, object] = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
    data["rowid"] = tm_polars.series_rowid(data["rowid"])
    data["__sqlmodded"] = tm_polars.series_sqlmodded(data["__sqlmodded"])
    track_uuid_values = cast(list[object], data["track_uuid"])
    data["track_uuid"] = pl.Series("track_uuid", 
                                     [x if x is not None else "" for x in track_uuid_values], 
                                     dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Generate UUIDs ----------
def generate_uuids(df: pl.DataFrame) -> pl.DataFrame:
    """Generate UUIDs for rows that need them using vectorized operations."""
    if df.is_empty():
        return df
    
    # Generate UUIDs for all rows (vectorized)
    new_uuids = [str(uuid.uuid7()) for _ in range(df.height)]
    
    # Create updated dataframe with new UUIDs and incremented __sqlmodded
    return df.with_columns([
        pl.Series("track_uuid", new_uuids, dtype=pl.Utf8),
        (pl.col("__sqlmodded") + 1).cast(pl.Int16).alias("__sqlmodded")
    ])

# ---------- Write Updates with Changelog ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    """Write UUID updates to database and log changes."""
    if updated.is_empty():
        logging.info("No UUID changes to write.")
        return 0

    logging.info(f"Writing {updated.height} new UUIDs to database")
    sample_ids = updated["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()
    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)

    update_sql = tm_db.build_update_sql(table="alib", set_cols=["track_uuid"])

    updated_rowids = updated["rowid"].to_list()
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, updated_rowids)

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original.filter(pl.col("rowid").is_in(updated_rowids)).to_dicts()
    }

    updates = 0
    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
        for record in updated.to_dicts():
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            changelog.add(
                alib_path=alib_path,
                changes=[
                    (
                        "track_uuid",
                        original_row.get("track_uuid"),
                        record.get("track_uuid"),
                    )
                ],
            )

            # Update the alib table
            cursor.execute(
                update_sql,
                (record["track_uuid"], int(record["__sqlmodded"] or 0), rowid),
            )
            updates += 1

        changelog.flush(cursor)

    logging.info(f"Updated {updates} UUID rows and logged changes.")
    return updates

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
        logging.info(f"Found {df.height} rows needing UUIDs")

        if df.is_empty():
            logging.info("No rows need UUID generation.")
            return

        original_df = df.clone()
        updated_df = generate_uuids(df)

        logging.info(f"Generated UUIDs for {updated_df.height} rows")
        write_updates(conn, original_df, updated_df)
        
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
