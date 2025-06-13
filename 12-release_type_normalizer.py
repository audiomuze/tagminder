import polars as pl
import sqlite3
from typing import Dict, List, Union
import logging
from datetime import datetime, timezone

# ---------- Config ----------
SCRIPT_NAME = "release-type-normalizer.py"
DB_PATH = '/tmp/amg/dbtemplate.db'

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------- Release Type Mapping ----------
# query to inspect outcomes:
# select distinct old_value,new_value from changelog order by old_value;
# inspect changelog:
# select changelog.rowid, alib.albumartist, alib.album, column, old_value, alib.releasetype from changelog inner join alib on alib.rowid == changelog.rowid;
RELEASE_TYPE_MAPPING = {
    "album": "Studio Album",
    "box set": "Box Set",
    "compilation": "Greatest Hits & Anthologies",
    "composite reissue": "Studio Album",
    "demo": "Demos, Soundboards & Bootlegs",
    "ep": "Extended Play",
    "live": "Live Album",
    "live album": "Live Album",
    "album\\\\compilation": "Greatest Hits & Anthologies\\\\Studio Album",
    "compilation\\\\album": "Greatest Hits & Anthologies\\\\Studio Album",
    "album\\\\soundtrack": "Soundtrack\\\\Studio Album",
    "soundtrack\\\\album": "Soundtrack\\\\Studio Album",
    "live\\\\album": "Live Album",
    "album\\\\live": "Live Album",
    "live\\\\single": "Single",
    "single\\\\live": "Single",
    "ep\\\\live": "Extended Play",
    "live\\\\ep": "Extended Play",
    "mixtape/street": "Mixtape/Street",
    "remix": "Remix",
    "single": "Single",
    "soundtrack": "Soundtrack",
    "studio album": "Studio Album"
}
DELIMITER = '\\\\'

# ---------- Helpers ----------
def sqlite_to_polars(conn: sqlite3.Connection, query: str) -> pl.DataFrame:
    """
    Convert SQLite query results to Polars DataFrame with proper type handling.
    """
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


def apply_multi_value_mappings(x: Union[str, None], mapping: Dict[str, str]) -> Union[str, None]:
    """
    Apply mappings for entries that contain the delimiter (multi-value mappings).
    These are applied as direct string replacements without splitting.
    
    Args:
        x: The release type string to normalize (can be None)
        mapping: Dictionary mapping old multi-value strings to new values
        
    Returns:
        String with multi-value mappings applied or None
    """
    if x is None:
        return None
    
    # Apply direct string replacement for multi-value mappings (exact match)
    if x in mapping:
        return mapping[x]
    
    return x


def normalize_single_value_entry(x: Union[str, None], mapping: Dict[str, str]) -> Union[str, None]:
    """
    Normalize a release type entry by splitting on delimiter, mapping single values,
    deduplicating, and rejoining.
    
    Args:
        x: The release type string to normalize (can be None)
        mapping: Dictionary mapping old single values to new values
        
    Returns:
        Normalized release type string or None
    """
    if x is None:
        return None

    if DELIMITER in x:
        # Split delimited values
        items = x.split(DELIMITER)
        normalized_items = []
        
        for item in items:
            stripped_item = item.strip()
            # Apply mapping (case sensitive)
            normalized = mapping.get(stripped_item, stripped_item)
            normalized_items.append(normalized)
        
        # Deduplicate while preserving order
        final_normalized_items = []
        seen = set()
        for item in normalized_items:
            if item not in seen:
                final_normalized_items.append(item)
                seen.add(item)
        
        return DELIMITER.join(final_normalized_items)
    else:
        # Single value
        stripped_x = x.strip() if x else x
        return mapping.get(stripped_x, stripped_x)


def batch_normalize_release_types(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """
    Apply release type normalization to the releasetype column using two-stage vectorized operations.
    Stage 1: Apply only mappings where left side contains delimiter (no splitting)
    Stage 2: Apply only mappings where left side doesn't contain delimiter (with splitting)
    Both stages process ALL rows.
    
    Args:
        df: Input DataFrame
        mapping: Dictionary mapping old values to new values
        
    Returns:
        DataFrame with normalized releasetype column
    """
    # Separate mappings into multi-value and single-value based on left side containing delimiter
    multi_value_mapping = {k: v for k, v in mapping.items() if DELIMITER in k}
    single_value_mapping = {k: v for k, v in mapping.items() if DELIMITER not in k}
    
    result_df = df
    
    # Stage 1: Apply multi-value mappings (direct string replacement) to ALL rows
    if multi_value_mapping:
        logging.info(f"Stage 1: Applying {len(multi_value_mapping)} multi-value mappings to all rows...")
        multi_expr = pl.col("releasetype").map_elements(
            lambda x: apply_multi_value_mappings(x, multi_value_mapping),
            return_dtype=pl.Utf8
        ).alias("releasetype")
        result_df = result_df.with_columns(multi_expr)
    
    # Stage 2: Apply single-value mappings (with splitting and deduplication) to ALL rows
    if single_value_mapping:
        logging.info(f"Stage 2: Applying {len(single_value_mapping)} single-value mappings to all rows...")
        single_expr = pl.col("releasetype").map_elements(
            lambda x: normalize_single_value_entry(x, single_value_mapping),
            return_dtype=pl.Utf8
        ).alias("releasetype")
        result_df = result_df.with_columns(single_expr)
    
    return result_df


def write_updates_to_db(
    conn: sqlite3.Connection,
    updated_df: pl.DataFrame,
    original_df: pl.DataFrame,
    changed_rowids: List[int]
) -> int:
    """
    Write updates to the database and log changes.
    
    Args:
        conn: SQLite database connection
        updated_df: DataFrame with updated values
        original_df: DataFrame with original values
        changed_rowids: List of rowids that have changes
        
    Returns:
        Number of updated rows
    """
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

    # Filter to only changed rows
    update_df = updated_df.filter(pl.col("rowid").is_in(changed_rowids))
    records = update_df.to_dicts()

    for record in records:
        rowid = record["rowid"]
        original_row = original_df.filter(pl.col("rowid") == rowid).row(0, named=True)
        
        # Check if releasetype actually changed and is not None
        new_value = record["releasetype"]
        old_value = original_row["releasetype"]
        
        if new_value != old_value and new_value is not None:
            # Increment sqlmodded counter
            new_sqlmodded = int(original_row["sqlmodded"] or 0) + 1
            
            # Update the database
            cursor.execute(
                "UPDATE alib SET releasetype = ?, sqlmodded = ? WHERE rowid = ?",
                (new_value, new_sqlmodded, rowid)
            )
            
            # Log the change
            cursor.execute(
                "INSERT INTO changelog VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, "releasetype", old_value, new_value, timestamp, SCRIPT_NAME)
            )
            
            updated_count += 1

    conn.commit()
    logging.info(f"Updated {updated_count} rows and logged all changes.")
    return updated_count


# ---------- Main ----------
def main():
    """
    Main function to normalize release types in the database.
    """
    logging.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Fetch data
        logging.info("Fetching release type data...")
        tracks = sqlite_to_polars(
            conn,
            """
            SELECT rowid, releasetype, COALESCE(sqlmodded, 0) AS sqlmodded
            FROM alib
            WHERE releasetype IS NOT NULL
            ORDER BY rowid
            """
        )
        
        logging.info(f"Processing {tracks.height} tracks with release type data...")
        
        # Normalize release types in batch
        updated_tracks = batch_normalize_release_types(tracks, RELEASE_TYPE_MAPPING)
        
        # Detect changes using vectorized comparison
        change_expr = (
            (pl.col("releasetype").is_not_null()) & 
            (tracks["releasetype"] != updated_tracks["releasetype"])
        )
        
        changed_rowids = updated_tracks.filter(change_expr)["rowid"].to_list()
        logging.info(f"Found {len(changed_rowids)} tracks with changes")
        
        if changed_rowids:
            num_updated = write_updates_to_db(
                conn,
                updated_df=updated_tracks,
                original_df=tracks,
                changed_rowids=changed_rowids
            )
            logging.info(f"Successfully updated {num_updated} tracks in the database")
        else:
            logging.info("No changes detected, database not updated")

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()