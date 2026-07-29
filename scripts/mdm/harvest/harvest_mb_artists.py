"""Extract MusicBrainz artist data from mbdump.tar.bz2 into SQLite.

This script is intentionally scoped to MusicBrainz dump ingestion only.
It does not read from AllMusic or Wikimedia databases.

Input:
- MusicBrainz dump archive (mbdump.tar.bz2), configured via
    [musicbrainz].dump_archive in harvest_master_data.toml.

Output:
- SQLite table: musicbrainz_artists

The table is dropped/recreated on each run.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import sqlite3
import tarfile
import time
from pathlib import Path
from typing import Any, Callable

from tagminder.core import tm_config
log = logging.getLogger("harvest_mb_artists")

PROGRESS_LOG_INTERVAL = 500_000
MASTER_CONFIG_FILE = "harvest_master_data.toml"
WIKIDATA_ENTITY_PREFIX = "http://www.wikidata.org/entity/"

WIKIDATA_RE = re.compile(r"wikidata\.org/.+?(Q\d+)", flags=re.IGNORECASE)
ALLMUSIC_RE = re.compile(r"(mn\d{10})", flags=re.IGNORECASE)


def _stream_all_members(
    tar_archive: str,
    wanted: set[str],
) -> dict[str, io.BytesIO]:
    """Single-pass stream scan of tar archive. Returns wanted members as in-memory BytesIO buffers."""
    buffers: dict[str, io.BytesIO] = {}
    with tarfile.open(tar_archive, "r:bz2") as tf:
        for member in tf:
            if member.name not in wanted:
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            buffers[member.name] = io.BytesIO(f.read())
            if len(buffers) == len(wanted):
                break
    return buffers


def _text_stream(buf: io.BytesIO) -> io.TextIOWrapper:
    """Convert BytesIO buffer to UTF-8 TextIOWrapper for csv reader."""
    buf.seek(0)
    return io.TextIOWrapper(buf, encoding="utf-8")


def parse_external_link(url: str) -> tuple[str | None, str | None]:
    """Return (source, normalized_id) for supported URL types."""
    wd_match = WIKIDATA_RE.search(url)
    if wd_match:
        return ("wikidata", wd_match.group(1).upper())

    am_match = ALLMUSIC_RE.search(url)
    if am_match:
        return ("allmusic", am_match.group(1).lower())

    return (None, None)


def _wikidata_uri_from_qid(qid: str | None) -> str | None:
    if not qid:
        return None
    qid_clean = qid.strip().upper()
    if not qid_clean.startswith("Q"):
        return None
    return f"{WIKIDATA_ENTITY_PREFIX}{qid_clean}"


def _resolve_master_config_path() -> Path:
    cwd_candidate = (Path.cwd() / MASTER_CONFIG_FILE).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    script_path = Path(__file__).resolve()
    checked: list[Path] = [cwd_candidate]
    for parent in script_path.parents:
        candidate = (parent / MASTER_CONFIG_FILE).resolve()
        checked.append(candidate)
        if candidate.exists():
            return candidate

    looked_in = "\n".join(f"- {path}" for path in checked)
    raise FileNotFoundError(
        f"{MASTER_CONFIG_FILE} not found. Looked in:\n{looked_in}"
    )


def load_musicbrainz_ingestion_paths() -> tuple[str, str]:
    """Load MusicBrainz dump path and output DB path from harvest_master_data.toml."""
    config_path = _resolve_master_config_path()
    cfg = tm_config.load_config(config_path=config_path)

    config_dir = config_path.parent

    mb_value = cfg.get("musicbrainz") if isinstance(cfg, dict) else None
    mb_cfg = mb_value if isinstance(mb_value, dict) else {}

    tar_candidate = str(mb_cfg.get("dump_archive", "")).strip()
    if not tar_candidate:
        raise FileNotFoundError(
            "MusicBrainz dump_archive path not found.\n"
            "Set [musicbrainz].dump_archive in harvest_master_data.toml.\n"
            "Download mbdump.tar.bz2 from https://musicbrainz.org/doc/MusicBrainz_Database"
        )
    tar_path = Path(tar_candidate).expanduser()
    if not tar_path.is_absolute():
        tar_path = (config_dir / tar_path).resolve()

    db_candidate = str(mb_cfg.get("contributors_db", "")).strip()
    if not db_candidate:
        db_candidate = "master-data.db"
    if not db_candidate:
        raise ValueError("Could not resolve database path from TOML or defaults")
    db_path = Path(db_candidate).expanduser()
    if not db_path.is_absolute():
        db_path = (config_dir / db_path).resolve()

    return str(tar_path), str(db_path)


def harvest_pipeline() -> None:
    """Extract MusicBrainz artist core + MB-linked IDs into a single table."""
    tar_archive, db_file = load_musicbrainz_ingestion_paths()

    tar_path = Path(tar_archive)
    if not tar_path.exists():
        raise FileNotFoundError(
            f"MusicBrainz dump archive not found: {tar_archive}\n"
            "Download mbdump.tar.bz2 from https://musicbrainz.org/doc/MusicBrainz_Database "
            "and configure [musicbrainz].dump_archive in harvest_master_data.toml."
        )

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    log.info("[1/4] Single-pass archive scan: %s", tar_archive)
    t_total = time.perf_counter()
    t_step = time.perf_counter()
    
    wanted = {"mbdump/url", "mbdump/l_artist_url", "mbdump/artist"}
    buffers = _stream_all_members(tar_archive, wanted)
    
    for required in wanted:
        if required not in buffers:
            raise RuntimeError(f"{required} not found in archive")

    log.info("[2/4] Parsing URL registry (wikidata/allmusic IDs)...")
    url_map: dict[int, tuple[str, str]] = {}
    url_stream = _text_stream(buffers["mbdump/url"])
    for row in csv.reader(url_stream, delimiter="\t"):
        if not row:
            continue
        url_id_str = row[0].strip() if row[0] else ""
        url_str = row[2].strip() if len(row) > 2 and row[2] else ""

        if not url_id_str or not url_str or url_id_str == r"\N" or url_str == r"\N":
            continue

        source, normalized_id = parse_external_link(url_str)
        if source and normalized_id:
            url_map[int(url_id_str)] = (source, normalized_id)

    log.info("  parsed %d relevant URL rows in %.1fs", len(url_map), time.perf_counter() - t_step)
    t_step = time.perf_counter()

    log.info("[3/4] Resolving artist↔URL relationships...")
    artist_wd_id_map: dict[int, str] = {}
    artist_am_id_map: dict[int, str] = {}

    l_art_url_stream = _text_stream(buffers["mbdump/l_artist_url"])
    for row in csv.reader(l_art_url_stream, delimiter="\t"):
        if not row or len(row) < 4:
            continue

        artist_id_str = row[2].strip() if row[2] else ""
        url_id_str = row[3].strip() if row[3] else ""
        if (
            not artist_id_str
            or not url_id_str
            or artist_id_str == r"\N"
            or url_id_str == r"\N"
        ):
            continue

        artist_id = int(artist_id_str)
        url_id = int(url_id_str)

        payload = url_map.get(url_id)
        if payload is None:
            continue

        source, normalized_id = payload
        if source == "wikidata":
            artist_wd_id_map[artist_id] = normalized_id
        elif source == "allmusic":
            artist_am_id_map[artist_id] = normalized_id

    log.info(
        "  resolved artist links: wikidata=%d allmusic=%d in %.1fs",
        len(artist_wd_id_map),
        len(artist_am_id_map),
        time.perf_counter() - t_step,
    )
    t_step = time.perf_counter()

    log.info("[4/4] Streaming MB artist rows into musicbrainz_artists (%s)...", db_file)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS musicbrainz_artists;")
    cursor.execute(
        """
        CREATE TABLE musicbrainz_artists (
            artist_id INTEGER PRIMARY KEY,
            mbid TEXT,
            artist_name TEXT,
            begin_date_year INTEGER,
            begin_date_month INTEGER,
            begin_date_day INTEGER,
            end_date_year INTEGER,
            end_date_month INTEGER,
            end_date_day INTEGER,
            type INTEGER,
            area INTEGER,
            gender INTEGER,
            disambiguation TEXT,
            ended INTEGER,
            wikidata_uri TEXT,
            wikidata_id TEXT,
            allmusic_mnid TEXT,
            source_dump TEXT,
            extracted_utc TEXT
        );
        """
    )

    insert_sql = """
        INSERT INTO musicbrainz_artists (
            artist_id,
            mbid,
            artist_name,
            begin_date_year,
            begin_date_month,
            begin_date_day,
            end_date_year,
            end_date_month,
            end_date_day,
            type,
            area,
            gender,
            disambiguation,
            ended,
            wikidata_uri,
            wikidata_id,
            allmusic_mnid,
            source_dump,
            extracted_utc
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
    """

    source_dump = Path(tar_archive).name
    extracted_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    chunk: list[tuple[Any, ...]] = []
    chunk_size = 50_000
    total_artists = 0

    def clean_val(
        row: list[str], idx: int, target_type: Callable[[str], Any] = str
    ) -> Any | None:
        value = row[idx].strip() if row[idx] else ""
        if value == r"\N" or value == "":
            return None
        try:
            return target_type(value)
        except ValueError:
            return None

    artist_stream = _text_stream(buffers["mbdump/artist"])
    reader = csv.reader(artist_stream, delimiter="\t")

    for row in reader:
        if not row or len(row) < 17:
            continue

        artist_id = int(row[0].strip())
        ended_raw = row[16].strip() if row[16] else ""
        ended_int = 1 if ended_raw in ("t", "true", "1") else 0
        wikidata_id = artist_wd_id_map.get(artist_id)

        record = (
            artist_id,
            clean_val(row, 1, str),  # mbid
            clean_val(row, 2, str),  # artist_name
            clean_val(row, 4, int),
            clean_val(row, 5, int),
            clean_val(row, 6, int),
            clean_val(row, 7, int),
            clean_val(row, 8, int),
            clean_val(row, 9, int),
            clean_val(row, 10, int),
            clean_val(row, 11, int),
            clean_val(row, 12, int),
            clean_val(row, 13, str),
            ended_int,
            _wikidata_uri_from_qid(wikidata_id),
            wikidata_id,
            artist_am_id_map.get(artist_id),
            source_dump,
            extracted_utc,
        )
        chunk.append(record)

        if len(chunk) >= chunk_size:
            cursor.executemany(insert_sql, chunk)
            total_artists += len(chunk)
            if total_artists % PROGRESS_LOG_INTERVAL == 0:
                log.info("  progress: inserted %d MusicBrainz artists", total_artists)
            chunk = []

    if chunk:
        cursor.executemany(insert_sql, chunk)
        total_artists += len(chunk)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_artists_mbid ON musicbrainz_artists(mbid);")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_mb_artists_wikidata_id ON musicbrainz_artists(wikidata_id);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_mb_artists_wikidata_uri ON musicbrainz_artists(wikidata_uri);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_mb_artists_allmusic_mnid ON musicbrainz_artists(allmusic_mnid);"
    )

    conn.commit()
    conn.close()

    log.info("  wrote %d MusicBrainz rows in %.1fs", total_artists, time.perf_counter() - t_step)
    log.info("Pipeline complete — database '%s' ready (total %.1fs)", db_file, time.perf_counter() - t_total)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        harvest_pipeline()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        raise SystemExit(1)
    except KeyboardInterrupt:
        log.warning("Pipeline aborted by user.")
        raise SystemExit(130)
