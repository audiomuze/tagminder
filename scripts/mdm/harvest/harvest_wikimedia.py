"""Ingest Wikidata identity records from latest-all.json.gz into SQLite.

This script streams the Wikidata entity JSON dump (usually
``latest-all.json.gz``), applies truthy statement selection locally per
property, extracts a practical enrichment set for music-identity workflows,
and writes a single rebuilt SQLite table.

Design goals:
- Streaming parse (no full in-memory entity index)
- Single denormalized table for downstream enrichment
- Rebuild semantics (drop/create on each run)
- Truthy-equivalent statement selection from latest-all.json.gz
- Focus on entities that have MusicBrainz artist ID or AllMusic ID

Configuration (harvest_master_data.toml)
======================================

[wikimedia]
all_json_gz = "/path/to/latest-all.json.gz"  # required
wikimedia_db = "/path/to/wikimedia.db"       # required
target_table = "wikidata_music_identity"     # optional
label_language = "en"                        # optional
apple_music_artist_id_property = "P2850"     # optional; override if needed
wikipedia_base_url = "https://en.wikipedia.org/" # optional; source base URL for wikipedia_url extraction
"""

from __future__ import annotations

import bz2
import gzip
import io
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

log = logging.getLogger("harvest_wikimedia")

DEFAULT_ALL_JSON_GZ = "/tmp/amg/latest-all.json.gz"
DEFAULT_DB_FILE = "/tmp/amg/wikimedia.db"
DEFAULT_TARGET_TABLE = "wikidata_music_identity"
DEFAULT_LABEL_LANGUAGE = "en"
DEFAULT_APPLE_MUSIC_ARTIST_ID_PROPERTY = "P2850"
DEFAULT_WIKIPEDIA_BASE_URL = "https://en.wikipedia.org/"
MASTER_CONFIG_FILE = "harvest_master_data.toml"
INSERT_BATCH_SIZE = 10_000
GZIP_READ_BUFFER_BYTES = 8 * 1024 * 1024
JSON_PROGRESS_INTERVAL = 500_000

WIKIDATA_ENTITY_PREFIX = "http://www.wikidata.org/entity/"


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


def _fmt_int(value: int) -> str:
    return f"{value:,}"


def _json_array(items: set[str] | None) -> str | None:
    if not items:
        return None
    return json.dumps(sorted(items), ensure_ascii=False)


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _connect_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    _apply_sqlite_pragmas(conn)
    return conn


def _apply_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    pragmas = [
        "PRAGMA busy_timeout = 5000",
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -2097152",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 8589934592",
        "PRAGMA wal_autocheckpoint = 10000",
        "PRAGMA optimize",
    ]
    for pragma in pragmas:
        try:
            conn.execute(pragma)
        except sqlite3.Error:
            continue


@contextmanager
def _open_compressed_text(path: str):
    # Use an explicit larger buffer to reduce read syscall overhead on huge dumps.
    with open(path, "rb") as raw:
        if path.endswith(".gz"):
            compressed: Any = gzip.GzipFile(fileobj=raw, mode="rb")
        elif path.endswith(".bz2"):
            compressed = bz2.BZ2File(raw, mode="rb")
        else:
            compressed = raw

        with compressed:
            with io.BufferedReader(compressed, buffer_size=GZIP_READ_BUFFER_BYTES) as buffered:
                with io.TextIOWrapper(buffered, encoding="utf-8", errors="replace") as text:
                    yield text


def _qid_to_wikidata_uri(qid: str | None) -> str | None:
    if not qid:
        return None
    return f"{WIKIDATA_ENTITY_PREFIX}{qid}"


def _normalize_gender(value: str | None) -> str:
    """Map Wikidata P21 values to TagMinder-style gender values."""
    if not value:
        return "not applicable"

    qid = str(value).strip().upper()
    if qid == "Q6581097":
        return "male"
    if qid == "Q6581072":
        return "female"
    return "not applicable"


def load_wikimedia_ingestion_settings() -> dict[str, str]:
    """Load Wikimedia ingestion settings from harvest_master_data.toml."""
    all_json_gz = DEFAULT_ALL_JSON_GZ
    db_file = DEFAULT_DB_FILE
    target_table = DEFAULT_TARGET_TABLE
    label_language = DEFAULT_LABEL_LANGUAGE
    apple_music_artist_id_property = DEFAULT_APPLE_MUSIC_ARTIST_ID_PROPERTY
    wikipedia_base_url = DEFAULT_WIKIPEDIA_BASE_URL

    resolved_config_path = _resolve_master_config_path()

    import tomllib

    with resolved_config_path.open("rb") as f:
        cfg = tomllib.load(f)

    root_cfg = cfg if isinstance(cfg, dict) else {}
    wd_value = root_cfg.get("wikimedia")
    wd_cfg = wd_value if isinstance(wd_value, dict) else {}

    all_json_gz = str(wd_cfg.get("all_json_gz", all_json_gz)).strip()
    db_file = str(wd_cfg.get("wikimedia_db", db_file)).strip()
    target_table = str(wd_cfg.get("target_table", target_table)).strip() or DEFAULT_TARGET_TABLE
    label_language = str(wd_cfg.get("label_language", label_language)).strip().lower() or DEFAULT_LABEL_LANGUAGE
    wikipedia_base_url = str(wd_cfg.get("wikipedia_base_url", wikipedia_base_url)).strip() or DEFAULT_WIKIPEDIA_BASE_URL
    apple_music_artist_id_property = (
        str(wd_cfg.get("apple_music_artist_id_property", apple_music_artist_id_property)).strip().upper()
        or DEFAULT_APPLE_MUSIC_ARTIST_ID_PROPERTY
    )

    all_json_path = Path(all_json_gz).expanduser()
    db_path = Path(db_file).expanduser()

    if not all_json_path.is_absolute():
        all_json_path = (resolved_config_path.parent / all_json_path).resolve()
    if not db_path.is_absolute():
        db_path = (resolved_config_path.parent / db_path).resolve()

    return {
        "all_json_gz": str(all_json_path),
        "db_file": str(db_path),
        "target_table": target_table,
        "label_language": label_language,
        "apple_music_artist_id_property": apple_music_artist_id_property,
        "wikipedia_base_url": wikipedia_base_url,
    }


def _truthy_statements(claims: dict[str, Any], pid: str) -> list[dict[str, Any]]:
    raw_statements = claims.get(pid)
    if not isinstance(raw_statements, list):
        return []

    preferred: list[dict[str, Any]] = []
    normal: list[dict[str, Any]] = []

    for statement in raw_statements:
        if not isinstance(statement, dict):
            continue
        rank = statement.get("rank")
        if rank == "deprecated":
            continue
        if rank == "preferred":
            preferred.append(statement)
        elif rank == "normal":
            normal.append(statement)

    return preferred or normal


def _statement_datavalue(statement: dict[str, Any]) -> Any | None:
    mainsnak = statement.get("mainsnak")
    if not isinstance(mainsnak, dict):
        return None
    if mainsnak.get("snaktype") != "value":
        return None
    datavalue = mainsnak.get("datavalue")
    if not isinstance(datavalue, dict):
        return None
    return datavalue.get("value")


def _claim_text_value(statement: dict[str, Any]) -> str | None:
    value = _statement_datavalue(statement)
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        entity_id = value.get("id")
        if isinstance(entity_id, str) and entity_id.strip():
            return entity_id.strip()
        time_value = value.get("time")
        if isinstance(time_value, str) and time_value.strip():
            return time_value.strip()
        text_value = value.get("text")
        if isinstance(text_value, str) and text_value.strip():
            return text_value.strip()
    return None


def _claim_qid_value(statement: dict[str, Any]) -> str | None:
    value = _statement_datavalue(statement)
    if not isinstance(value, dict):
        return None

    entity_id = value.get("id")
    if isinstance(entity_id, str) and entity_id.startswith("Q"):
        return entity_id

    numeric_id = value.get("numeric-id")
    if isinstance(numeric_id, int) and numeric_id > 0:
        return f"Q{numeric_id}"

    return None


def _first_truthy_text(claims: dict[str, Any], pid: str) -> str | None:
    for statement in _truthy_statements(claims, pid):
        value = _claim_text_value(statement)
        if value is not None:
            return value
    return None


def _first_truthy_qid(claims: dict[str, Any], pid: str) -> str | None:
    for statement in _truthy_statements(claims, pid):
        qid = _claim_qid_value(statement)
        if qid is not None:
            return qid
    return None


def _truthy_qid_set(claims: dict[str, Any], pid: str) -> set[str] | None:
    values: set[str] = set()
    for statement in _truthy_statements(claims, pid):
        qid = _claim_qid_value(statement)
        if qid is not None:
            values.add(qid)
    return values or None


def _extract_label(entity: dict[str, Any], label_language: str) -> str | None:
    labels = entity.get("labels")
    if not isinstance(labels, dict):
        return None
    label = labels.get(label_language)
    if not isinstance(label, dict):
        return None
    value = label.get("value")
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _extract_aliases(entity: dict[str, Any], label_language: str) -> set[str] | None:
    aliases = entity.get("aliases")
    if not isinstance(aliases, dict):
        return None

    alias_entries = aliases.get(label_language)
    if not isinstance(alias_entries, list):
        return None

    values: set[str] = set()
    for entry in alias_entries:
        if not isinstance(entry, dict):
            continue
        value = entry.get("value")
        if isinstance(value, str):
            value = value.strip()
            if value:
                values.add(value)

    return values or None


def _wikipedia_site_key(wikipedia_base_url: str) -> str | None:
    host = (urlparse(wikipedia_base_url).netloc or "").lower()
    if not host.endswith(".wikipedia.org"):
        return None
    prefix = host[: -len(".wikipedia.org")]
    if not prefix:
        return None
    return prefix.replace("-", "_").replace(".", "_") + "wiki"


def _extract_wikipedia_url(
    entity: dict[str, Any],
    wikipedia_site_key: str | None,
    wikipedia_base_url: str,
) -> str | None:
    if not wikipedia_site_key:
        return None

    sitelinks = entity.get("sitelinks")
    if not isinstance(sitelinks, dict):
        return None

    sitelink = sitelinks.get(wikipedia_site_key)
    if not isinstance(sitelink, dict):
        return None

    direct_url = sitelink.get("url")
    if isinstance(direct_url, str) and direct_url.strip():
        return direct_url.strip()

    title = sitelink.get("title")
    if not isinstance(title, str) or not title.strip():
        return None

    encoded_title = quote(title.strip().replace(" ", "_"), safe="()")
    return f"{wikipedia_base_url.rstrip('/')}/wiki/{encoded_title}"


def _parse_entity_json_line(raw: str, line_number: int) -> dict[str, Any] | None:
    line = raw.strip()
    if not line or line == "[" or line == "]":
        return None
    if line.endswith(","):
        line = line[:-1]
    line = line.strip()
    if not line or line == "]":
        return None

    try:
        entity = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse latest-all.json.gz at line {line_number}: {exc}") from exc

    return entity if isinstance(entity, dict) else None


def _build_identity_record(
    entity: dict[str, Any],
    label_language: str,
    apple_music_artist_pid: str,
    wikipedia_site_key: str | None,
    wikipedia_base_url: str,
) -> dict[str, Any] | None:
    qid = entity.get("id")
    if not isinstance(qid, str) or not qid.startswith("Q"):
        return None

    claims = entity.get("claims")
    if not isinstance(claims, dict):
        claims = {}

    scalar_pid_map = {
        "P434": "mbid",
        "P1728": "allmusic_mnid",
        "P3478": "songkick_artist_id",
        apple_music_artist_pid: "apple_music_artist_id",
        "P1953": "discogs_artist_id",
        "P2205": "spotify_artist_id",
        "P3192": "lastfm_artist_id",
        "P2397": "youtube_channel_id",
        "P213": "isni",
        "P214": "viaf_id",
        "P856": "official_website",
        "P569": "date_of_birth",
        "P570": "date_of_death",
        "P571": "inception",
        "P576": "dissolved",
    }
    qid_scalar_pid_map = {
        "P21": "gender",
        "P19": "place_of_birth_wikidata_id",
        "P20": "place_of_death_wikidata_id",
    }
    list_pid_map = {
        "P31": "instance_of_wikidata_ids",
        "P106": "occupation_wikidata_ids",
        "P27": "citizenship_wikidata_ids",
        "P495": "origin_country_wikidata_ids",
        "P136": "genre_wikidata_ids",
        "P1303": "instrument_wikidata_ids",
        "P463": "member_of_wikidata_ids",
    }

    record: dict[str, Any] = {
        "wikidata_id": qid,
        "wikidata_uri": _qid_to_wikidata_uri(qid),
    }

    for pid, col in scalar_pid_map.items():
        value = _first_truthy_text(claims, pid)
        if value is not None:
            record[col] = value

    if not record.get("mbid") and not record.get("allmusic_mnid"):
        return None

    for pid, col in qid_scalar_pid_map.items():
        value = _first_truthy_qid(claims, pid)
        if value is not None:
            record[col] = value

    for pid, col in list_pid_map.items():
        values = _truthy_qid_set(claims, pid)
        if values:
            record[col] = values

    label = _extract_label(entity, label_language)
    if label is not None:
        record["wikidata_label"] = label

    aliases = _extract_aliases(entity, label_language)
    if aliases:
        record["wikidata_aliases"] = aliases

    wikipedia_url = _extract_wikipedia_url(entity, wikipedia_site_key, wikipedia_base_url)
    if wikipedia_url is not None:
        record["wikipedia_url"] = wikipedia_url

    return record


def _musicbrainz_url(mbid: str | None) -> str | None:
    if not mbid:
        return None
    return f"https://musicbrainz.org/artist/{mbid}"


def _allmusic_url(mnid: str | None) -> str | None:
    if not mnid:
        return None
    return f"https://www.allmusic.com/artist/{mnid}"


def _songkick_url(songkick_id: str | None) -> str | None:
    if not songkick_id:
        return None
    return f"https://www.songkick.com/artists/{songkick_id}"


def _apple_lookup_url(apple_artist_id: str | None) -> str | None:
    if not apple_artist_id:
        return None
    return f"https://itunes.apple.com/lookup?id={apple_artist_id}&entity=musicArtist"


def _discogs_url(discogs_artist_id: str | None) -> str | None:
    if not discogs_artist_id:
        return None
    return f"https://www.discogs.com/artist/{discogs_artist_id}"


def _spotify_url(spotify_artist_id: str | None) -> str | None:
    if not spotify_artist_id:
        return None
    return f"https://open.spotify.com/artist/{spotify_artist_id}"


def ingest_all_json_dump() -> None:
    settings = load_wikimedia_ingestion_settings()
    all_json_gz = settings["all_json_gz"]
    db_file = settings["db_file"]
    target_table = settings["target_table"]
    label_language = settings["label_language"]
    apple_music_artist_pid = settings["apple_music_artist_id_property"]
    wikipedia_base_url = settings["wikipedia_base_url"]

    if not Path(all_json_gz).exists():
        raise FileNotFoundError(f"Entity JSON dump not found: {all_json_gz}")

    dump_size_gb = Path(all_json_gz).stat().st_size / (1024**3)
    log.info(
        "Run config: Stream from %s (%.2f GiB), db=%s, table=%s, label_language=%s, batch_size=%s",
        all_json_gz,
        dump_size_gb,
        db_file,
        target_table,
        label_language,
        _fmt_int(INSERT_BATCH_SIZE),
    )

    t_total = time.perf_counter()
    t_scan = time.perf_counter()
    wikipedia_site_key = _wikipedia_site_key(wikipedia_base_url)

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)
    conn = _connect_sqlite(db_file)
    cursor = conn.cursor()

    quoted_table = _quote_ident(target_table)
    cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
    cursor.execute(
        f"""
        CREATE TABLE {quoted_table} (
            wikidata_uri TEXT PRIMARY KEY,
            wikidata_id TEXT,
            wikidata_label TEXT,
            wikidata_aliases TEXT,
            mbid TEXT,
            allmusic_mnid TEXT,
            songkick_artist_id TEXT,
            apple_music_artist_id TEXT,
            discogs_artist_id TEXT,
            spotify_artist_id TEXT,
            lastfm_artist_id TEXT,
            youtube_channel_id TEXT,
            isni TEXT,
            viaf_id TEXT,
            official_website TEXT,
            gender TEXT,
            instance_of_wikidata_ids TEXT,
            occupation_wikidata_ids TEXT,
            citizenship_wikidata_ids TEXT,
            origin_country_wikidata_ids TEXT,
            place_of_birth_wikidata_id TEXT,
            place_of_death_wikidata_id TEXT,
            date_of_birth TEXT,
            date_of_death TEXT,
            inception TEXT,
            dissolved TEXT,
            genre_wikidata_ids TEXT,
            instrument_wikidata_ids TEXT,
            member_of_wikidata_ids TEXT,
            wikidata_url TEXT,
            musicbrainz_url TEXT,
            allmusic_url TEXT,
            discogs_url TEXT,
            spotify_url TEXT,
            songkick_url TEXT,
            wikipedia_url TEXT,
            apple_lookup_url TEXT,
            source_dump TEXT,
            extracted_utc TEXT
        )
        """
    )

    insert_query = f"""
        INSERT INTO {quoted_table} (
            wikidata_uri, wikidata_id, wikidata_label, wikidata_aliases,
            mbid, allmusic_mnid, songkick_artist_id, apple_music_artist_id,
            discogs_artist_id, spotify_artist_id, lastfm_artist_id, youtube_channel_id,
            isni, viaf_id, official_website, gender,
            instance_of_wikidata_ids, occupation_wikidata_ids, citizenship_wikidata_ids, origin_country_wikidata_ids,
            place_of_birth_wikidata_id, place_of_death_wikidata_id,
            date_of_birth, date_of_death, inception, dissolved,
            genre_wikidata_ids, instrument_wikidata_ids, member_of_wikidata_ids,
            wikidata_url, musicbrainz_url, allmusic_url, discogs_url, spotify_url, songkick_url, wikipedia_url, apple_lookup_url,
            source_dump, extracted_utc
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?
        )
    """

    extracted_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    source_dump = str(Path(all_json_gz).name)

    batch: list[tuple[Any, ...]] = []
    rows_written = 0
    entities_scanned = 0
    retained_entities = 0

    with _open_compressed_text(all_json_gz) as f:
        for line_number, raw in enumerate(f, start=1):
            entity = _parse_entity_json_line(raw, line_number)
            if entity is None:
                continue

            entities_scanned += 1
            if entities_scanned % JSON_PROGRESS_INTERVAL == 0:
                log.info(
                    "  progress: %s entities scanned, %s candidates retained, %s rows written",
                    _fmt_int(entities_scanned),
                    _fmt_int(retained_entities),
                    _fmt_int(rows_written),
                )

            record = _build_identity_record(
                entity,
                label_language=label_language,
                apple_music_artist_pid=apple_music_artist_pid,
                wikipedia_site_key=wikipedia_site_key,
                wikipedia_base_url=wikipedia_base_url,
            )
            if record is None:
                continue

            retained_entities += 1

            mbid = record.get("mbid")
            mnid = record.get("allmusic_mnid")
            songkick_id = record.get("songkick_artist_id")
            apple_artist_id = record.get("apple_music_artist_id")
            discogs_artist_id = record.get("discogs_artist_id")
            spotify_artist_id = record.get("spotify_artist_id")

            batch.append(
                (
                    record.get("wikidata_uri"),
                    record.get("wikidata_id"),
                    record.get("wikidata_label"),
                    _json_array(record.get("wikidata_aliases")),
                    mbid,
                    mnid,
                    songkick_id,
                    apple_artist_id,
                    discogs_artist_id,
                    spotify_artist_id,
                    record.get("lastfm_artist_id"),
                    record.get("youtube_channel_id"),
                    record.get("isni"),
                    record.get("viaf_id"),
                    record.get("official_website"),
                    _normalize_gender(record.get("gender")),
                    _json_array(record.get("instance_of_wikidata_ids")),
                    _json_array(record.get("occupation_wikidata_ids")),
                    _json_array(record.get("citizenship_wikidata_ids")),
                    _json_array(record.get("origin_country_wikidata_ids")),
                    record.get("place_of_birth_wikidata_id"),
                    record.get("place_of_death_wikidata_id"),
                    record.get("date_of_birth"),
                    record.get("date_of_death"),
                    record.get("inception"),
                    record.get("dissolved"),
                    _json_array(record.get("genre_wikidata_ids")),
                    _json_array(record.get("instrument_wikidata_ids")),
                    _json_array(record.get("member_of_wikidata_ids")),
                    record.get("wikidata_uri"),
                    _musicbrainz_url(mbid),
                    _allmusic_url(mnid),
                    _discogs_url(discogs_artist_id),
                    _spotify_url(spotify_artist_id),
                    _songkick_url(songkick_id),
                    record.get("wikipedia_url"),
                    _apple_lookup_url(apple_artist_id),
                    source_dump,
                    extracted_utc,
                )
            )

            if len(batch) >= INSERT_BATCH_SIZE:
                cursor.executemany(insert_query, batch)
                rows_written += len(batch)
                batch.clear()

    if batch:
        cursor.executemany(insert_query, batch)
        rows_written += len(batch)

    if rows_written != retained_entities:
        raise RuntimeError(
            f"Insert count mismatch: wrote {rows_written} rows for {retained_entities} retained entities"
        )

    log.info(
        "  scan complete: %s entities scanned, %s candidates retained in %.1fs",
        _fmt_int(entities_scanned),
        _fmt_int(retained_entities),
        time.perf_counter() - t_scan,
    )

    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(f'idx_{target_table}_mbid')} ON {quoted_table}(mbid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(f'idx_{target_table}_mnid')} ON {quoted_table}(allmusic_mnid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(f'idx_{target_table}_wikidata_id')} ON {quoted_table}(wikidata_id)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(f'idx_{target_table}_songkick')} ON {quoted_table}(songkick_artist_id)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(f'idx_{target_table}_apple')} ON {quoted_table}(apple_music_artist_id)"
    )

    conn.commit()
    conn.close()

    log.info(
        "  wrote %s rows into '%s' in %.1fs (total %.1fs)",
        _fmt_int(rows_written),
        target_table,
        time.perf_counter() - t_scan,
        time.perf_counter() - t_total,
    )


def main() -> None:
    ingest_all_json_dump()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        main()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        raise SystemExit(1)
    except KeyboardInterrupt:
        log.warning("Ingestion aborted by user.")
        raise SystemExit(130)
