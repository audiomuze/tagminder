"""
Purpose:
    Process music library records from the `alib` table to clean and standardize
    `title`, `subtitle`, `artist`, and live-performance markers.

    The script extracts featured-artist patterns, moves bracketed suffixes into
    the appropriate fields, sets live flags when appropriate, and logs all
    modifications to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - contributors_unified_disambiguated
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import sqlite3
import polars as pl
import logging
import re

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_run
# ---------- Config ----------
COLUMNS = ["title", "subtitle", "artist", "live"]
DELIM = r'\\'
SUBTITLE_SEPARATOR = "; "
 

# ---------- Regex ----------
BRACKET_PATTERN = r"(?i)\s*[\(\[\{<]([^)\]\}>]+)[\)\]\}>]\s*$"
FEATURE_PREFIXES = ("with", "w/", "feat", "feat.", "featuring")
LIVE_PREFIX = "live"
SUBTITLE_PREFIXES = (
    "remix", "rmx", "remaster", "remastered",
    "demo", "outtake", "alt", "alternate", "alt.",
    "mix", "early mix", "instrumental", "bonus", "radio",
    "reprise", "unplugged", "acoustic", "electric", "akoesties",
    "orchestral", "piano", "dj"
)
TRAILING_MATCHES = {"mix", "session", "demos", "remaster", "remastered", "remix", "version"}
# Normalize first-word variants to canonical form (one lookup, reuse across rows)
WORD_NORMALIZATION = {
    "remastered": "remastered",
    "remaster": "remastered",
    "rmx": "remix",
    "alt": "alt. take",
    "alternate": "alt. take",
    "alt.": "alt. take",
    "early": "early mix",
    "early mix": "early mix",
}

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, title, subtitle, artist, live, COALESCE(__sqlmodded, 0) as __sqlmodded
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

        if name == "rowid":
            data[name] = pl.Series(name=name, values=[int(x or 0) for x in col_data], dtype=pl.Int64)
        elif name == "__sqlmodded":
            data[name] = pl.Series(name=name, values=[int(x or 0) for x in col_data], dtype=pl.Int16)
        elif name == "live":
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else "0" for x in col_data], dtype=pl.Utf8)
        else:
            data[name] = pl.Series(name=name, values=[str(x) if x is not None else None for x in col_data], dtype=pl.Utf8)

    return pl.DataFrame(data)


def fetch_disambiguated_artists(conn: sqlite3.Connection) -> pl.DataFrame:
    """Fetch disambiguated artist names with both original and lowercase versions"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT preferred__artist_name AS contributor, lpreferred__artist_name "
        "FROM contributors_unified_disambiguated"
    )
    return pl.DataFrame(
        cursor.fetchall(), schema=["contributor", "lpreferred__artist_name"], orient="row"
    )


# ---------- Clean Artist Field from Feature Prefixes ----------
def clean_artist_feature_prefixes(df: pl.DataFrame, disambiguated_df: pl.DataFrame) -> pl.DataFrame:
    """
    Processes artist tags with these rules:
    1. First checks if lowercase artist matches any lpreferred__artist_name in disambiguated_df
    2. If match found:
         - If artist matches contributor exactly: skip processing
         - If case differs: replace artist with contributor value
    3. If no match: process feature prefixes as before
    """
    # Create a lookup dictionary {lpreferred__artist_name: contributor}
    case_map = dict(
        zip(
            disambiguated_df["lpreferred__artist_name"].to_list(),
            disambiguated_df["contributor"].to_list(),
        )
    )

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
        (pl.col("__sqlmodded") + results["modded"]).cast(pl.Int16).alias("__sqlmodded")
    ])


# ---------- Apply Bracketed Suffix Rules ----------
def _append_to_subtitle(subtitle: str, rest_wrapped: str, check_live_at: bool = False) -> tuple[str, bool]:
    """
    Helper to append rest_wrapped to subtitle with optional live-at guard.
    Returns (updated_subtitle, was_appended).
    """
    if not rest_wrapped:
        return subtitle, False
    if check_live_at and "live at" in subtitle.lower():
        return subtitle, False
    # Prevent duplicates using delimiter-aware, case-insensitive token matching.
    # Accept both legacy '\\' and current '; ' subtitle separators.
    existing_tokens = [
        seg.strip()
        for seg in re.split(rf"(?:{re.escape(DELIM)}|\s*;\s*)", subtitle)
        if seg and seg.strip()
    ]
    candidate_norm = rest_wrapped.strip().lower()
    if any(token.lower() == candidate_norm for token in existing_tokens):
        return subtitle, False

    existing_tokens.append(rest_wrapped.strip())
    return SUBTITLE_SEPARATOR.join(existing_tokens), True


def apply_suffix_extraction(df: pl.DataFrame) -> pl.DataFrame:
    updated_rows = []

    for row in df.to_dicts():
        title = row["title"]
        subtitle = row["subtitle"] or ""
        artist = row["artist"] or ""
        live = row["live"] or "0"
        modded_count = row["__sqlmodded"] or 0

        match = re.search(BRACKET_PATTERN, title, re.IGNORECASE)
        if match:
            bracket_content = match.group(1).strip()
            words = bracket_content.split()
            if words:
                first_word = words[0].lower()
                # Normalize variants using lookup dict (single operation, reusable)
                first_word = WORD_NORMALIZATION.get(first_word, first_word)

                rest = " ".join(words[1:]).strip() if first_word in FEATURE_PREFIXES else bracket_content.strip()
                rest_clean = rest.strip("[](){}<>").strip()
                rest_wrapped = f"[{rest_clean}]" if rest_clean else ""
                changed_cols = []

                # Extract title-strip once before branching (eliminates 3 redundant regex calls)
                stripped_title = re.sub(BRACKET_PATTERN, "", title).strip()
                title_changed = stripped_title != title

                # Pre-extract trailing word match
                trailing_word_match = False
                bracket_words = bracket_content.lower().split()
                if bracket_words:
                    last_word = re.sub(r"^[^a-z0-9]+|[^a-z0-9]+$", "", bracket_words[-1])
                    trailing_word_match = last_word in TRAILING_MATCHES

                if first_word in FEATURE_PREFIXES and rest_clean:
                    # Use pre-computed stripped title
                    if title_changed:
                        row["title"] = stripped_title
                        changed_cols.append("title")

                    # Only append to artist if not already present
                    if rest_clean not in artist:
                        row["artist"] = f"{artist}{DELIM}{rest_clean}" if artist else rest_clean
                        changed_cols.append("artist")


                elif first_word == LIVE_PREFIX and rest_clean:
                    # Use pre-computed stripped title
                    if title_changed:
                        row["title"] = stripped_title
                        changed_cols.append("title")

                    # Use helper function with live-at guard
                    new_subtitle, was_appended = _append_to_subtitle(subtitle, rest_wrapped, check_live_at=True)
                    if was_appended:
                        row["subtitle"] = new_subtitle
                        changed_cols.append("subtitle")

                    if live != "1":
                        row["live"] = "1"
                        changed_cols.append("live")

                elif first_word in SUBTITLE_PREFIXES or trailing_word_match:
                    # Use pre-computed stripped title
                    if title_changed:
                        row["title"] = stripped_title
                        changed_cols.append("title")

                    # Use helper function without live-at guard
                    new_subtitle, was_appended = _append_to_subtitle(subtitle, rest_wrapped, check_live_at=False)
                    if was_appended:
                        row["subtitle"] = new_subtitle
                        changed_cols.append("subtitle")

                # No fallback: unmatched suffix is ignored

                if changed_cols:
                    modded_count = (modded_count or 0) + len(changed_cols)
                    row["__sqlmodded"] = modded_count

        updated_rows.append(row)

    return pl.DataFrame({
        "rowid": pl.Series("rowid", [r["rowid"] for r in updated_rows], dtype=pl.Int64),
        "title": pl.Series("title", [r["title"] for r in updated_rows], dtype=pl.Utf8),
        "subtitle": pl.Series("subtitle", [r["subtitle"] for r in updated_rows], dtype=pl.Utf8),
        "artist": pl.Series("artist", [r["artist"] for r in updated_rows], dtype=pl.Utf8),
        "live": pl.Series("live", [r["live"] for r in updated_rows], dtype=pl.Utf8),
        "__sqlmodded": pl.Series("__sqlmodded", [r["__sqlmodded"] for r in updated_rows], dtype=pl.Int16),
    })

# ---------- Write Updates with Changelog ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("__sqlmodded") > original["__sqlmodded"])
    if changed.is_empty():
        logging.info("No changes to write.")
        return 0

    logging.info(f"Writing {changed.height} changed rows to database")
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

    updates = 0
    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)
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
        conn, db_path, _, _ = tm_run.open_db(ensure_changelog=True, log_connect=False, require_exists=True)
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

    master_db_path = tm_config.get_master_data_db_path(default=db_path)
    master_conn = conn if master_db_path == db_path else tm_db.connect(master_db_path, read_only=True)

    try:
        tm_db.require_table_columns(
            master_conn,
            "contributors_unified_disambiguated",
            ("preferred__artist_name", "lpreferred__artist_name"),
            hint="Run emit_contributors.py first so contributors_unified_disambiguated is available.",
        )

        # Load disambiguated artists with case information
        disambiguated_df = fetch_disambiguated_artists(master_conn)
        logging.info(f"Loaded {disambiguated_df.height} disambiguated artist references")

        # Load main data
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} tracks for processing")

        original_df = df.clone()
        df = clean_artist_feature_prefixes(df, disambiguated_df)
        updated_df = apply_suffix_extraction(df)

        changed_rows = updated_df.filter(pl.col("__sqlmodded") > original_df["__sqlmodded"]).height
        logging.info(f"Detected {changed_rows} modified rows")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        if master_conn is not conn:
            master_conn.close()
        conn.close()

if __name__ == "__main__":
    main()
