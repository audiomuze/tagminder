import sqlite3
import polars as pl
import logging
import uuid
from datetime import datetime, timezone

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
SCRIPT_NAME = "06-add_uuid.py"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch UUID Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch rows that need UUID generation."""
    query = """
        SELECT rowid, tagminder_uuid, COALESCE(sqlmodded, 0) as sqlmodded
        FROM alib
        WHERE tagminder_uuid IS NULL OR TRIM(tagminder_uuid) = ''
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        return pl.DataFrame({
            "rowid": pl.Series("rowid", [], dtype=pl.Int64),
            "tagminder_uuid": pl.Series("tagminder_uuid", [], dtype=pl.Utf8),
            "sqlmodded": pl.Series("sqlmodded", [], dtype=pl.Int64),
        })

    data = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
    data["rowid"] = pl.Series("rowid", data["rowid"], dtype=pl.Int64)
    data["sqlmodded"] = pl.Series("sqlmodded", [int(x) for x in data["sqlmodded"]], dtype=pl.Int64)
    data["tagminder_uuid"] = pl.Series("tagminder_uuid", 
                                     [x if x is not None else "" for x in data["tagminder_uuid"]], 
                                     dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Generate UUIDs ----------
def generate_uuids(df: pl.DataFrame) -> pl.DataFrame:
    """Generate UUIDs for rows that need them using vectorized operations."""
    if df.is_empty():
        return df
    
    # Generate UUIDs for all rows (vectorized)
    new_uuids = [str(uuid.uuid4()) for _ in range(df.height)]
    
    # Create updated dataframe with new UUIDs and incremented sqlmodded
    return df.with_columns([
        pl.Series("tagminder_uuid", new_uuids, dtype=pl.Utf8),
        (pl.col("sqlmodded") + 1).alias("sqlmodded")
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

    timestamp = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
    
    # Ensure changelog table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS changelog (
            alib_rowid INTEGER,
            column TEXT,
            old_value TEXT,
            new_value TEXT,
            timestamp TEXT,
            script TEXT
        )
    """)

    updates = 0
    for record in updated.to_dicts():
        rowid = record["rowid"]
        original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)
        
        # Log the UUID change
        cursor.execute(
            "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
            (rowid, "tagminder_uuid", original_row["tagminder_uuid"], record["tagminder_uuid"], timestamp, SCRIPT_NAME)
        )
        
        # Update the alib table
        cursor.execute(
            "UPDATE alib SET tagminder_uuid = ?, sqlmodded = ? WHERE rowid = ?",
            (record["tagminder_uuid"], int(record["sqlmodded"]), rowid)
        )
        updates += 1

    conn.commit()
    logging.info(f"Updated {updates} UUID rows and logged changes.")
    return updates

# ---------- Main ----------
def main():
    """Main execution function."""
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

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
