import sqlite3
import polars as pl
import logging
import re
from datetime import datetime, timezone

# ---------- Config ----------
DB_PATH = "/tmp/amg/dbtemplate.db"
DELIM = r'\\'
SCRIPT_NAME = "subtitles-polars.py"

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- Fetch Subtitle Data ----------
def fetch_data(conn: sqlite3.Connection) -> pl.DataFrame:
    query = """
        SELECT rowid, subtitle, COALESCE(sqlmodded, 0) as sqlmodded
        FROM alib
        WHERE subtitle IS NOT NULL AND TRIM(subtitle) != ''
    """
    cursor = conn.cursor()
    cursor.execute(query)

    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
    data["rowid"] = pl.Series("rowid", data["rowid"], dtype=pl.Int64)
    data["sqlmodded"] = pl.Series("sqlmodded", [int(x) for x in data["sqlmodded"]], dtype=pl.Int64)
    data["subtitle"] = pl.Series("subtitle", data["subtitle"], dtype=pl.Utf8)

    return pl.DataFrame(data)

# ---------- Subtitle Normalization ----------
def normalize_subtitle(text: str) -> str:
    parts = re.findall(r'[\(\[{<]([^\)\]\}>]+)[\)\]\}>]', text)
    if not parts:
        return text

    seen = set()
    normalized = []
    cleaned_parts = []

    for part in parts:
        clean = part.strip()
        key = clean.lower()
        if key not in seen:
            seen.add(key)
            cleaned_parts.append(clean)

    # Determine if we should drop [Live]
    live_entries = [p for p in cleaned_parts if p.lower() == "live"]
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

    return DELIM.join(final_parts) if final_parts else "[Live]"

# ---------- Process Subtitles ----------
def process_subtitles(df: pl.DataFrame) -> pl.DataFrame:
    updated_rows = []

    for row in df.to_dicts():
        original = row["subtitle"]
        normalized = normalize_subtitle(original)

        if normalized != original:
            row["subtitle"] = normalized
            row["sqlmodded"] += 1

        updated_rows.append(row)

    return pl.DataFrame({
        "rowid": pl.Series("rowid", [r["rowid"] for r in updated_rows], dtype=pl.Int64),
        "subtitle": pl.Series("subtitle", [r["subtitle"] for r in updated_rows], dtype=pl.Utf8),
        "sqlmodded": pl.Series("sqlmodded", [r["sqlmodded"] for r in updated_rows], dtype=pl.Int64),
    })

# ---------- Write Updates with Changelog ----------
def write_updates(conn: sqlite3.Connection, original: pl.DataFrame, updated: pl.DataFrame) -> int:
    changed = updated.filter(pl.col("sqlmodded") > original["sqlmodded"])
    if changed.is_empty():
        logging.info("No subtitle changes to write.")
        return 0

    logging.info(f"Writing {changed.height} updated subtitles to database")
    sample_ids = changed["rowid"].to_list()[:5]
    logging.info(f"Sample changed rowids: {sample_ids}")

    timestamp = datetime.now(timezone.utc).isoformat()
    cursor = conn.cursor()
    conn.execute("BEGIN TRANSACTION")
    cursor.execute(f"""
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
    for record in changed.to_dicts():
        rowid = record["rowid"]
        original_row = original.filter(pl.col("rowid") == rowid).row(0, named=True)

        if record["subtitle"] != original_row["subtitle"]:
            cursor.execute(
                "INSERT INTO changelog (rowid, column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, "subtitle", original_row["subtitle"], record["subtitle"], timestamp, SCRIPT_NAME)
            )
            cursor.execute(
                "UPDATE alib SET subtitle = ?, sqlmodded = ? WHERE rowid = ?",
                (record["subtitle"], int(record["sqlmodded"]), rowid)
            )
            updates += 1

    conn.commit()
    logging.info(f"Updated {updates} subtitle rows and logged changes.")
    return updates

# ---------- Main ----------
def main():
    logging.info("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    try:
        df = fetch_data(conn)
        logging.info(f"Loaded {df.height} subtitle rows")

        original_df = df.clone()
        updated_df = process_subtitles(df)

        changed_rows = updated_df.filter(pl.col("sqlmodded") > original_df["sqlmodded"]).height
        logging.info(f"Detected {changed_rows} changed subtitle rows")

        if changed_rows > 0:
            write_updates(conn, original_df, updated_df)
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
