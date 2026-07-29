"""
Purpose:
    Normalize live markers by moving them from `title` into `subtitle` and
    setting the `live` flag.

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
import re

from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_run
# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Config ----------
COLUMNS = ["title", "subtitle", "album", "live"]
DELIMITER = tm_config.get_multivalue_delimiter()
SUBTITLE_SEPARATOR = "; "
LEGACY_SUBTITLE_DELIM_REGEX = re.escape(DELIMITER)

# ---------- Regex patterns ----------
LIVE_CLEAN_PATTERN = r"(?i)(?:[\(\[\{<]\s*live\s*[\)\]\}>]|- live)\s*$"  # Standalone 'live' at end
LIVE_WORD_PATTERN = r"(?i)\blive\b"  # For subtitle check

# ---------- Fetch data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, title, subtitle, album, live, COALESCE(__sqlmodded, 0) as __sqlmodded
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

    # Subtitle: normalize legacy delimiters and append [Live] only if missing.
    subtitle_normalized = (
        pl.col("subtitle")
        .str.replace_all(LEGACY_SUBTITLE_DELIM_REGEX, SUBTITLE_SEPARATOR)
        .str.replace_all(r"\s*;\s*", SUBTITLE_SEPARATOR)
        .str.strip_chars()
    )

    subtitle_updated = pl.when(
        pl.col("subtitle").is_not_null() & subtitle_normalized.str.contains(LIVE_WORD_PATTERN)
    ).then(
        subtitle_normalized
    ).otherwise(
        pl.when(pl.col("subtitle").is_not_null())
        .then(
            pl.concat_str([
                subtitle_normalized,
                pl.lit("[Live]")
            ], separator=SUBTITLE_SEPARATOR).str.strip_chars()
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
        (pl.col("__sqlmodded") + sqlmodded_delta).cast(pl.Int16).alias("__sqlmodded")
    ])

    return df.drop(["new_title", "new_album", "new_subtitle", "new_live"])

# ---------- Write updates ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)
    # cursor.execute("""
    #     CREATE TABLE IF NOT EXISTS changes (
    #         rowid INTEGER,
    #         alib_column TEXT,
    #         old_value TEXT,
    #         new_value TEXT
    #     )
    # """)
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

# ---------- Main ----------
def main():
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
        updated_df = apply_live_normalization(df)

        changed_rows = updated_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info(f"Detected {changed_rows} rows with changes")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
