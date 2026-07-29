"""
Purpose:
    Normalize the `subtitle` tag in `alib` by extracting bracketed parts,
    de-duplicating (case-insensitive), normalizing capitalization, and removing
    redundant live markers.

    Writes updates back to `alib`, increments `__sqlmodded`, and logs changes to
    `changelog`.

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
from tagminder.core import tm_run
# ---------- Config ----------
# Legacy in-database multi-value separator used by older subtitle data.
LEGACY_DELIM = r'\\'
# New subtitle separator for distinct subtitle items.
SUBTITLE_SEPARATOR = '; '


# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch Subtitle Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, subtitle, COALESCE(__sqlmodded, 0) as __sqlmodded
        FROM alib
        WHERE subtitle IS NOT NULL AND TRIM(subtitle) != ''
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
    data["rowid"] = tm_polars.series_rowid(data["rowid"])
    data["__sqlmodded"] = tm_polars.series_sqlmodded(data["__sqlmodded"])
    data["subtitle"] = pl.Series("subtitle", data["subtitle"], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Subtitle Normalization ----------
def _join_subtitle_items(items: list[str]) -> str:
    """Join subtitle items using the canonical subtitle separator."""
    normalized = [item.strip() for item in items if item and item.strip()]
    return SUBTITLE_SEPARATOR.join(normalized)


def normalize_subtitle(text: str) -> str:
    parts = re.findall(r'[\(\[{<]([^\)\]\}>]+)[\)\]\}>]', text)
    if not parts:
        # Preserve legacy values but migrate old multi-value separators.
        if LEGACY_DELIM in text:
            return _join_subtitle_items(text.split(LEGACY_DELIM))
        return text

    seen = set()
    cleaned_parts = []

    for part in parts:
        clean = part.strip()
        key = clean.lower()
        if key not in seen:
            seen.add(key)
            cleaned_parts.append(clean)

    # Determine if we should drop [Live]
    other_with_live = [p for p in cleaned_parts if "live" in p.lower() and p.lower() != "live"]

    final_parts = []
    for part in cleaned_parts:
        key = part.lower()
        if key == "live" and other_with_live:
            continue

        # Capitalize first word unless it's all uppercase
        words = part.split()
        if words:
            if not words[0].isupper():
                words[0] = words[0].capitalize()

        # Capitalize letters after full stops
        def capitalize_abbreviations(text):
            return re.sub(r'(?<=\.)[a-zA-Z]', lambda m: m.group(0).upper(), text)

        formatted = ' '.join(words)
        formatted = capitalize_abbreviations(formatted)

        final_parts.append(f"[{formatted}]")

    return SUBTITLE_SEPARATOR.join(final_parts) if final_parts else "[Live]"

# ---------- Process Subtitles ----------
def process_subtitles(df: pl.DataFrame) -> pl.DataFrame:
    updated_rows = []

    for row in df.to_dicts():
        original = row["subtitle"]
        normalized = normalize_subtitle(original)

        if normalized != original:
            row["subtitle"] = normalized
            row["__sqlmodded"] += 1

        updated_rows.append(row)

    return pl.DataFrame({
        "rowid": pl.Series("rowid", [r["rowid"] for r in updated_rows], dtype=pl.Int64),
        "subtitle": pl.Series("subtitle", [r["subtitle"] for r in updated_rows], dtype=pl.Utf8),
        "__sqlmodded": pl.Series("__sqlmodded", [r["__sqlmodded"] for r in updated_rows], dtype=pl.Int16),
    })

# ---------- Write Updates with Changelog ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No subtitle changes to write.")
        return 0

    logging.info(f"Writing {changed.height} updated subtitles to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()
    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)

    changed_rowids = changed["rowid"].to_list()
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, changed_rowids)

    original_by_rowid = {
        int(r["rowid"]): r
        for r in original.filter(pl.col("rowid").is_in(changed_rowids)).to_dicts()
    }

    update_sql = tm_db.build_update_sql(table="alib", set_cols=["subtitle"])

    updates = 0
    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)
        for record in changed.to_dicts():
            rowid = record["rowid"]
            alib_path = path_by_rowid.get(int(rowid), str(rowid))
            original_row = original_by_rowid[int(rowid)]

            old_v = original_row.get("subtitle")
            new_v = record.get("subtitle")
            if old_v != new_v:
                changelog.add(alib_path=alib_path, changes=[("subtitle", old_v, new_v)])
                cursor.execute(
                    update_sql,
                    (record["subtitle"], int(record["__sqlmodded"] or 0), rowid),
                )
                updates += 1

        changelog.flush(cursor)

    logging.info(f"Updated {updates} subtitle rows and logged changes.")
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
        logging.info(f"Loaded {df.height} subtitle rows")

        original_df = df.clone()
        updated_df = process_subtitles(df)

        changed_rows = updated_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info(f"Detected {changed_rows} changed subtitle rows")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
