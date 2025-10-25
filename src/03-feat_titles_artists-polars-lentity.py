"""
Script Name: 03-feat_titles_artists-polars.py

Purpose:
This script processes music library records from the 'alib' table to clean and standardize title, subtitle, artist,
and live performance information. It specifically handles:
1. Extracting and properly formatting featured artists from the artist field, separating them with a delimiter
2. Processing bracketed suffixes in titles to:
   - Move featured artist information to the artist field
   - Identify live performance indicators and update relevant fields
   - Extract subtitle information (remixes, versions, etc.) into the subtitle field
3. Maintaining a changelog of all modifications made to the database

The script intelligently processes bracketed content in titles, recognizing various patterns like:
- Featured artists (feat., with, w/, etc.)
- Live performance indicators
- Version information (remixes, remasters, alternate takes, etc.)
- Other subtitle-worthy information

It ensures consistent formatting across the library by:
- Using a standardized delimiter (\\) between multiple artists
- Properly moving content between title, subtitle, and artist fields
- Setting live performance flags when appropriate

This script is part of tagminder and serves as the standard way to normalize title and artist information
throughout your music collection.

Usage:
python 03-feat_titles_artists-polars.py
uv run 03-feat_titles_artists-polars.py

Author: audiomuze
Created: 2025-04-26
"""

import sqlite3
import polars as pl
import logging
import re
from datetime import datetime, timezone

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
COLUMNS = ["title", "subtitle", "artist", "live"]
DELIM = r'\\'
SCRIPT_NAME = "titles-polars.py"

# ---------- Regex ----------
BRACKET_PATTERN = r"(?i)\s*[\(\[\{<]([^)\]\}>]+)[\)\]\}>]\s*$"
FEATURE_PREFIXES = ("with", "w/", "feat", "feat.", "featuring")
LIVE_PREFIX = "live"
SUBTITLE_PREFIXES = (
    "remix", "rmx", "remaster", "remastered",
    "demo", "outtake", "alt", "alternate", "alt.",
    "mix", "early mix", "instrumental", "bonus", "radio",
    "reprise", "unplugged", "acoustic", "electric", "akoesties",
    "acoustic", "orchestral", "piano", "dj"
)
TRAILING_MATCHES = {"mix", "session", "demos", "remaster", "remastered", "remix"}

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, title, subtitle, artist, live, COALESCE(sqlmodded, 0) as sqlmodded
        FROM alib
        WHERE title IS NOT NULL AND TRIM(title) != ''
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {}
    for i, name in enumerate(col_names):
        col_data = [row[i] for row in rows]

        if name == "rowid" or name == "sqlmodded":
            data[name] = pl.Series(name=name, values=[int(x or 0) for x in col_data], dtype=pl.Int64)
        elif name == "live":
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "0" for x in col_data], dtype=pl.Utf8)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else None for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)


def fetch_disambiguated_artists(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch disambiguated artist names with both original and lowercase versions"""
    cursor = conn.cursor()
    cursor.execute("SELECT entity, lentity FROM _REF_mb_disambiguated")
    return pl.DataFrame(cursor.fetchall(), schema=["entity", "lentity"], orient="row")


# ---------- Clean Artist Field from Feature Prefixes ----------
def clean_artist_feature_prefixes(df: pl.DataFrame, disambiguated_df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes artist tags with these rules:
    1. First checks if lowercase artist matches any lentity in disambiguated_df
    2. If match found:
       - If artist matches entity exactly: skip processing
       - If case differs: replace artist with entity value
    3. If no match: process feature prefixes as before
    """
    # Create a lookup dictionary {lentity: entity}
    case_map = dict(zip(disambiguated_df["lentity"].to_list(),
                      disambiguated_df["entity"].to_list()))

    feature_pattern = re.compile(r"\s+(feat\.?|featuring|with|w/)\s+", flags=re.IGNORECASE)

    def process_artist(artist: str) -> dict:
        if not artist:
            return {"artist": None, "modded": 0}

        lower_artist = artist.lower()
        if lower_artist in case_map:
            # Case-sensitive comparison and correction
            canonical = case_map[lower_artist]
            if artist != canonical:
                return {"artist": canonical, "modded": 1}
            return {"artist": artist, "modded": 0}  # Exact match, no change needed

        # Only process feature prefixes if not in disambiguated list
        match = feature_pattern.search(artist)
        if match:
            split_result = feature_pattern.split(artist, maxsplit=1)
            if len(split_result) >= 2:
                main_part = split_result[0].strip()
                featured_part = split_result[2].strip()
                if main_part and featured_part:
                    cleaned_artist = f"{main_part}{DELIM}{featured_part}".strip()
                    if cleaned_artist != artist:
                        return {"artist": cleaned_artist, "modded": 1}
        return {"artist": artist, "modded": 0}

    # Process all artists in vectorized operation
    results = df["artist"].map_elements(
        lambda x: process_artist(x) if x is not None else {"artist": None, "modded": 0},
        return_dtype=pl.Struct([
            pl.Field("artist", pl.Utf8),
            pl.Field("modded", pl.Int8)
        ])
    ).struct.unnest()

    return df.with_columns([
        pl.coalesce(results["artist"], pl.lit(None)).alias("artist"),
        (pl.col("sqlmodded") + results["modded"]).alias("sqlmodded")
    ])


# ---------- Apply Bracketed Suffix Rules ----------
def apply_suffix_extraction(df: pl.DataFrame) -> pl.DataFrame:
    updated_rows = []

    for row in df.to_dicts():
        title = row["title"]
        subtitle = row["subtitle"] or ""
        artist = row["artist"] or ""
        live = row["live"] or "0"
        sqlmodded = row["sqlmodded"] or 0

        match = re.search(BRACKET_PATTERN, title, re.IGNORECASE)
        if match:
            bracket_content = match.group(1).strip()
            words = bracket_content.split()
            if words:
                first_word = words[0].lower()

                # Normalize variants
                if first_word in {"remastered", "remaster"}:
                    first_word = "remastered"
                elif first_word == "rmx":
                    first_word = "remix"
                elif first_word in {"alt.", "alternate", "alt"}:
                    first_word = "alt. take"
                elif first_word in {"early", "early mix"}:
                    first_word = "early mix"

                rest = " ".join(words[1:]).strip() if first_word in FEATURE_PREFIXES else bracket_content.strip()
                rest_clean = rest.strip("[](){}<>").strip()
                rest_wrapped = f"[{rest_clean}]" if rest_clean else ""
                changed_cols = []

                trailing_word_match = False
                bracket_words = bracket_content.lower().split()
                if bracket_words and bracket_words[-1] in TRAILING_MATCHES:
                    trailing_word_match = True

                if first_word in FEATURE_PREFIXES and rest_clean:
                    # Always clean up the title if the bracket suffix exists
                    new_title = re.sub(BRACKET_PATTERN, "", title).strip()
                    if new_title != title:
                        row["title"] = new_title
                        changed_cols.append("title")

                    # Only append to artist if not already present
                    if rest_clean not in artist:
                        row["artist"] = f"{artist}{DELIM}{rest_clean}" if artist else rest_clean
                        changed_cols.append("artist")


                elif first_word == LIVE_PREFIX and rest_clean:
                    new_title = re.sub(BRACKET_PATTERN, "", title).strip()
                    if new_title != title:
                        row["title"] = new_title
                        changed_cols.append("title")

                    if "live at" not in subtitle.lower() and rest_wrapped not in subtitle:
                        row["subtitle"] = f"{subtitle}{DELIM}{rest_wrapped}" if subtitle else rest_wrapped
                        changed_cols.append("subtitle")

                    if live != "1":
                        row["live"] = "1"
                        changed_cols.append("live")

                elif first_word in SUBTITLE_PREFIXES or trailing_word_match:
                    new_title = re.sub(BRACKET_PATTERN, "", title).strip()
                    if new_title != title:
                        row["title"] = new_title
                        changed_cols.append("title")

                    if rest_wrapped and rest_wrapped not in subtitle:
                        row["subtitle"] = f"{subtitle}{DELIM}{rest_wrapped}" if subtitle else rest_wrapped
                        changed_cols.append("subtitle")

                # No fallback: unmatched suffix is ignored

                if changed_cols:
                    sqlmodded = (sqlmodded or 0) + len(changed_cols)
                    row["sqlmodded"] = sqlmodded

        updated_rows.append(row)

    return pl.DataFrame({
        "rowid": pl.Series("rowid", [r["rowid"] for r in updated_rows], dtype=pl.Int64),
        "title": pl.Series("title", [r["title"] for r in updated_rows], dtype=pl.Utf8),
        "subtitle": pl.Series("subtitle", [r["subtitle"] for r in updated_rows], dtype=pl.Utf8),
        "artist": pl.Series("artist", [r["artist"] for r in updated_rows], dtype=pl.Utf8),
        "live": pl.Series("live", [r["live"] for r in updated_rows], dtype=pl.Utf8),
        "sqlmodded": pl.Series("sqlmodded", [r["sqlmodded"] for r in updated_rows], dtype=pl.Int64),
    })

# ---------- Write Updates with Changelog ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("sqlmodded") > original["sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    timestamp = datetime.now(timezone.utc).isoformat()

    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
    cursor.execute(f"""
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
    for record in changed.to_dicts():
        rowid = record["rowid"]
        original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)

        changed_cols = []
        for col in COLUMNS:
            if record[col] != original_row[col]:
                changed_cols.append(col)
                cursor.execute(
                    "INSERT INTO changelog (alib_rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                    (rowid, col, original_row[col], record[col], timestamp, SCRIPT_NAME)
                )

        if changed_cols:
            set_clause = ", ".join(f"{col} = ?" for col in changed_cols) + ", sqlmodded = ?"
            values = [record[col] for col in changed_cols] + [int(record["sqlmodded"]), rowid]
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
        # Load disambiguated artists with case information
        disambiguated_df = fetch_disambiguated_artists(conn)
        logging.info(f"Loaded {disambiguated_df.height} disambiguated artist references")

        # Load main data
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} tracks for processing")

        original_df = df.clone()
        df = clean_artist_feature_prefixes(df, disambiguated_df)
        updated_df = apply_suffix_extraction(df)

        changed_rows = updated_df.filter(pl.col("sqlmodded") > original_df["sqlmodded"]).height
        logging.info(f"Detected {changed_rows} modified rows")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
