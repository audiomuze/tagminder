"""Infer MusicBrainz work links for tracks in `alib`.

Purpose:
    Draft a conservative work-resolution step that compares each library track
    against the flattened MusicBrainz work lookup and records the best candidate
    with explainable confidence tiers.

    The first pass uses only stable, indexed signals:
    - exact `musicbrainz_workid` when the library already has one
    - exact normalized title against `canonical_works_lookup.work_title_norm`
    - exact normalized title against alias/title token set in `all_title_norm_tokens_mv`
    - contributor-name corroboration against role-name columns in the work lookup
    - artist-MBID corroboration (resolved via `musicbrainz_artists`) against role-id columns

    The script always refreshes its inference review table, updates the user's
    canonical `user_vetted_works` reference, and auto-applies only the strict
    definitive matches back to `alib`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - canonical_works_lookup
    - user_vetted_works (master-data canonical layer)
    - work_inference_candidates
    - changelog

Author: audiomuze
Last updated: 2026-07-20
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_polars_db

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
TRACK_TABLE = "alib"
WORK_LOOKUP_TABLE = "canonical_works_metadata"
CANDIDATE_TABLE = "work_inference_candidates"
USER_VETTED_WORKS_TABLE = "user_vetted_works"
MASTER_CONFIG_FILE = "tagminder.toml"

# Role types and scoring rules
ROLE_TYPES = ["composer", "arranger", "lyricist", "writer", "orchestrator", "translator", "other"]
ROLE_NAME_COLUMN = "musicbrainz_work_role_artist_names"
ROLE_ID_COLUMN = "musicbrainz_work_role_artist_mbids"
ROLE_DELIMITER = "\\\\"  # Double backslash as separator in MB data
ROLE_PAIR_DELIMITER = ":"  # Separates role from name

# Contributor columns in alib that we match against work roles
TRACK_CONTRIBUTOR_COLUMNS = [
    "artist",
    "composer",
    "arranger",
    "lyricist",
    "writer",
    "albumartist",
    "ensemble",
    "conductor",
    "producer",
    "engineer",
    "mixer",
    "remixer",
]

# Artist MBID columns in alib for matching against work role MBIDs
TRACK_ARTIST_MBID_COLUMNS = [
    "musicbrainz_artistid",
    "musicbrainz_albumartistid",
]

# Tracks with only artist/albumartist are too sparse for reliable work inference.
# Keep these rows in summary output, but skip candidate matching work for them.
MATCHING_ENRICHMENT_COLUMNS = [
    "composer",
    "arranger",
    "lyricist",
    "writer",
    "ensemble",
    "conductor",
    "producer",
    "engineer",
    "mixer",
    "remixer",
    *TRACK_ARTIST_MBID_COLUMNS,
    "musicbrainz_workid",
]

LOOKUP_SOURCE_COLUMN = "is_user_vetted"
LOOKUP_COLUMNS = [
    "work_id",
    "musicbrainz_workid",
    "work_title",
    "work_title_norm",
    "all_title_norm_tokens",
    "musicbrainz_work_role_artist_names",
    "musicbrainz_work_role_artist_mbids",
]
ROLE_SCORE_RULES: dict[str, tuple[int, int]] = {
    "composer": (20, 10),
    "arranger": (10, 5),
    "lyricist": (10, 5),
    "writer": (10, 5),
    "orchestrator": (10, 5),
    "translator": (10, 5),
    "other": (10, 5),
}
LOOKUP_FETCH_CHUNK_SIZE = 2_000
ALIAS_LIKE_CHUNK_SIZE = 250
AUTO_APPLY_SCORE = 80
AUTO_APPLY_MARGIN = 20
DEFAULT_REQUIRE_CORROBORATION_FOR_TITLE_ONLY = True
DEFAULT_REQUIRE_EXACT_WORKID_OR_UNIQUE_EXACT_TITLE_FOR_AUTO_APPLY = True


@dataclass(frozen=True)
class CandidateResult:
    rowid: int
    alib_path: str | None
    existing_work: str | None
    existing_musicbrainz_workid: str | None
    title: str | None
    title_norm: str | None
    best_work_id: int | None
    best_work_mbid: str | None
    best_work_title: str | None
    best_work_title_norm: str | None
    best_score: int
    runner_up_score: int
    candidate_count: int
    confidence_tier: str
    reason_codes: str
    should_apply: int


logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("infer_works_from_library")


def _resolve_schema_toml_path() -> Path:
    cwd_candidate = (Path.cwd() / MASTER_CONFIG_FILE).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        candidate = (parent / MASTER_CONFIG_FILE).resolve()
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        f"Missing {MASTER_CONFIG_FILE}. Looked in current directory and parents of {script_path}."
    )


def _load_db_path(default: str | None = None) -> str:
    config_path = _resolve_schema_toml_path()
    cfg = tm_config.load_config(config_path=config_path)
    db_cfg = cfg.get("db") if isinstance(cfg, dict) else {}
    db_candidate = str(db_cfg.get("path", "")).strip() if isinstance(db_cfg, dict) else ""
    db_candidate = default or db_candidate or "/tmp/amg/tagminder-staging.db"
    db_path = Path(db_candidate).expanduser()
    if not db_path.is_absolute():
        db_path = (config_path.parent / db_path).resolve()
    return str(db_path)


def _load_master_data_db_path(default: str | None = None) -> str:
    config_path = _resolve_schema_toml_path()
    cfg = tm_config.load_config(config_path=config_path)
    md_cfg = cfg.get("master_data") if isinstance(cfg, dict) else {}
    md_candidate = str(md_cfg.get("path", "")).strip() if isinstance(md_cfg, dict) else ""
    md_candidate = default or md_candidate or "/tmp/amg/master-data.db"
    md_path = Path(md_candidate).expanduser()
    if not md_path.is_absolute():
        md_path = (config_path.parent / md_path).resolve()
    return str(md_path)


def _to_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _load_infer_works_guardrail_config() -> bool:
    config_path = _resolve_schema_toml_path()
    cfg = tm_config.load_config(config_path=config_path)
    scripts_cfg = cfg.get("scripts") if isinstance(cfg, dict) else None
    script_cfg = scripts_cfg.get("22-infer-works-in-library.py") if isinstance(scripts_cfg, dict) else None
    raw_value = (
        script_cfg.get("auto_apply_requires_corroboration_for_title_only")
        if isinstance(script_cfg, dict)
        else None
    )
    return _to_bool(raw_value, DEFAULT_REQUIRE_CORROBORATION_FOR_TITLE_ONLY)


def _load_infer_works_unique_title_auto_apply_config() -> bool:
    config_path = _resolve_schema_toml_path()
    cfg = tm_config.load_config(config_path=config_path)
    scripts_cfg = cfg.get("scripts") if isinstance(cfg, dict) else None
    script_cfg = scripts_cfg.get("22-infer-works-in-library.py") if isinstance(scripts_cfg, dict) else None
    raw_value = (
        script_cfg.get("auto_apply_requires_exact_workid_or_unique_exact_title")
        if isinstance(script_cfg, dict)
        else None
    )
    return _to_bool(raw_value, DEFAULT_REQUIRE_EXACT_WORKID_OR_UNIQUE_EXACT_TITLE_FOR_AUTO_APPLY)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.lower().replace('"', "").replace("\"", "")
    return " ".join(text.split())


def _normalize_people(value: Any) -> set[str]:
    if value is None:
        return set()
    raw = str(value).strip()
    if not raw:
        return set()
    parts = re.split(r"(?:\\\\|;|/|&|\band\b|,)", raw, flags=re.IGNORECASE)
    return {
        _normalize_text(part)
        for part in parts
        if _normalize_text(part)
    }


def _normalize_mbid(value: Any) -> str:
    if value is None:
        return ""
    match = re.search(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        str(value),
    )
    if match is None:
        return ""
    return match.group(0).lower()


def _normalize_mbid_set(value: Any) -> set[str]:
    if value is None:
        return set()
    raw = str(value).strip()
    if not raw:
        return set()
    parts = re.split(r"(?:\\\\|;|/|&|,|\s+)", raw)
    mbids = {_normalize_mbid(part) for part in parts}
    return {mbid for mbid in mbids if mbid}


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({tm_db.quote_ident(table_name)})").fetchall()
    return {str(row[1]) for row in rows if row and row[1] is not None}


def _merge_multivalue_tokens(*values: Any) -> str | None:
    delimiter = tm_config.get_multivalue_delimiter()
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value is None:
            continue
        raw = str(value).strip()
        if not raw:
            continue
        for token in raw.split(delimiter):
            norm = _normalize_text(token)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            ordered.append(norm)
    if not ordered:
        return None
    return delimiter.join(ordered)


def _parse_role_string(role_string: str | None, role_type: str) -> set[str]:
    """
    Parse a role string like 'composer:John\\composer:Jane\\writer:Bob' 
    and extract all names for the given role_type.
    Returns a set of normalized names.
    """
    if not role_string:
        return set()
    
    result = set()
    parts = role_string.split(ROLE_DELIMITER)
    for part in parts:
        if not part or ROLE_PAIR_DELIMITER not in part:
            continue
        role, name = part.split(ROLE_PAIR_DELIMITER, 1)
        if role.strip() == role_type:
            normalized = _normalize_text(name.strip())
            if normalized:
                result.add(normalized)
    return result


def _count_role_matches(track_names: set[str], work_names: set[str]) -> int:
    """Count how many track names appear in work names."""
    if not track_names or not work_names:
        return 0
    return len(track_names & work_names)


def _ensure_user_vetted_works_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {USER_VETTED_WORKS_TABLE} (
            alib_path TEXT PRIMARY KEY,
            observed_title TEXT,
            observed_title_norm TEXT,
            work_id INTEGER,
            musicbrainz_workid TEXT,
            work_title TEXT,
            work_title_norm TEXT,
            all_title_norm_tokens_mv TEXT,
            composer_artist_ids_mv TEXT,
            arranger_artist_ids_mv TEXT,
            lyricist_artist_ids_mv TEXT,
            writer_artist_ids_mv TEXT,
            orchestrator_artist_ids_mv TEXT,
            translator_artist_ids_mv TEXT,
            other_artist_ids_mv TEXT,
            composer_artist_names_mv TEXT,
            arranger_artist_names_mv TEXT,
            lyricist_artist_names_mv TEXT,
            writer_artist_names_mv TEXT,
            orchestrator_artist_names_mv TEXT,
            translator_artist_names_mv TEXT,
            other_artist_names_mv TEXT,
            confidence_tier TEXT,
            best_score INTEGER,
            reason_codes TEXT,
            vetted INTEGER NOT NULL DEFAULT 0,
            first_seen_utc TEXT NOT NULL,
            last_seen_utc TEXT NOT NULL,
            source_script TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{USER_VETTED_WORKS_TABLE}_vetted ON {USER_VETTED_WORKS_TABLE}(vetted)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{USER_VETTED_WORKS_TABLE}_title_norm ON {USER_VETTED_WORKS_TABLE}(observed_title_norm)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{USER_VETTED_WORKS_TABLE}_musicbrainz_workid ON {USER_VETTED_WORKS_TABLE}(musicbrainz_workid)"
    )


def _confidence_tier(best_score: int, margin: int, exact_workid: bool, person_match: bool) -> str:
    if exact_workid and best_score >= 90 and margin >= 20:
        return "A"
    if best_score >= 80 and margin >= 20 and person_match:
        return "B"
    if best_score >= 60:
        return "C"
    return "D"


def _chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _load_artist_id_map(lookup_conn: sqlite3.Connection, mbids: list[str]) -> dict[str, int]:
    if not mbids or not _table_exists(lookup_conn, "musicbrainz_artists"):
        return {}

    mapping: dict[str, int] = {}
    unique_mbids = sorted({value for value in mbids if isinstance(value, str) and value.strip()})
    for chunk in _chunked(unique_mbids, LOOKUP_FETCH_CHUNK_SIZE):
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT mbid, artist_id
            FROM musicbrainz_artists
            WHERE mbid IN ({placeholders})
              AND artist_id IS NOT NULL
        """
        for mbid, artist_id in lookup_conn.execute(query, chunk):
            norm_mbid = _normalize_mbid(mbid)
            if not norm_mbid or artist_id is None:
                continue
            mapping[norm_mbid] = int(artist_id)
    return mapping


def _collect_track_artist_mbids(tracks: pl.DataFrame) -> list[str]:
    mbids: set[str] = set()
    usable_columns = [col for col in TRACK_ARTIST_MBID_COLUMNS if col in tracks.columns]
    if not usable_columns:
        return []

    for row in tracks.select(usable_columns).iter_rows(named=True):
        for column in usable_columns:
            for mbid in _normalize_mbid_set(row.get(column)):
                mbids.add(mbid)
    return sorted(mbids)


def _load_track_frame(conn: sqlite3.Connection) -> pl.DataFrame:
    existing = _table_columns(conn, TRACK_TABLE)
    required = ["__path", "title"]
    missing = [col for col in required if col not in existing]
    if missing:
        raise RuntimeError(f"Missing required columns in {TRACK_TABLE}: {', '.join(missing)}")

    select_columns = [
        "rowid",
        "COALESCE(__sqlmodded, 0) AS __sqlmodded",
        "__path",
        "title",
    ]
    for col in ["work", "musicbrainz_workid", *TRACK_CONTRIBUTOR_COLUMNS, *TRACK_ARTIST_MBID_COLUMNS]:
        if col in existing:
            select_columns.append(tm_db.quote_ident(col))
        else:
            select_columns.append(f"NULL AS {tm_db.quote_ident(col)}")

    query = f"""
        SELECT {', '.join(select_columns)}
        FROM {TRACK_TABLE}
        WHERE title IS NOT NULL AND TRIM(title) != ''
    """

    df = tm_polars_db.sqlite_to_polars(
        conn,
        query,
        dtype_overrides={"rowid": pl.Int64(), "__sqlmodded": pl.Int16()},
    )
    matching_enrichment_present_exprs = [
        pl.col(col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() != ""
        for col in MATCHING_ENRICHMENT_COLUMNS
        if col in df.columns
    ]
    eligible_for_matching_expr = (
        pl.any_horizontal(matching_enrichment_present_exprs)
        if matching_enrichment_present_exprs
        else pl.lit(False)
    )
    return df.with_columns(
        [
            pl.col("title").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("title_norm"),
            pl.when(
                pl.col("musicbrainz_workid")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                == ""
            )
            .then(None)
            .otherwise(
                pl.col("musicbrainz_workid").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            )
            .alias("musicbrainz_workid"),
            eligible_for_matching_expr.alias("_eligible_for_matching"),
        ]
    )


def _load_lookup_subset(
    lookup_conn: sqlite3.Connection,
    title_norms: list[str],
    work_mbids: list[str],
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    select_sql = f"SELECT {', '.join(LOOKUP_COLUMNS)} FROM {WORK_LOOKUP_TABLE} WHERE {{predicate}}"
    # user_vetted_works has separate role columns (_mv suffix), not combined ones
    # Build combined role strings from separate columns
    vetted_select_sql = f"""
        SELECT
            work_id,
            musicbrainz_workid,
            work_title,
            COALESCE(NULLIF(observed_title_norm, ''), work_title_norm) AS work_title_norm,
            all_title_norm_tokens_mv as all_title_norm_tokens,
            (
                COALESCE('composer:' || composer_artist_names_mv || '\\\\', '') ||
                COALESCE('arranger:' || arranger_artist_names_mv || '\\\\', '') ||
                COALESCE('lyricist:' || lyricist_artist_names_mv || '\\\\', '') ||
                COALESCE('writer:' || writer_artist_names_mv || '\\\\', '') ||
                COALESCE('orchestrator:' || orchestrator_artist_names_mv || '\\\\', '') ||
                COALESCE('translator:' || translator_artist_names_mv || '\\\\', '') ||
                COALESCE('other:' || other_artist_names_mv || '\\\\', '')
            ) AS musicbrainz_work_role_artist_names,
            (
                COALESCE('composer:' || composer_artist_ids_mv || '\\\\', '') ||
                COALESCE('arranger:' || arranger_artist_ids_mv || '\\\\', '') ||
                COALESCE('lyricist:' || lyricist_artist_ids_mv || '\\\\', '') ||
                COALESCE('writer:' || writer_artist_ids_mv || '\\\\', '') ||
                COALESCE('orchestrator:' || orchestrator_artist_ids_mv || '\\\\', '') ||
                COALESCE('translator:' || translator_artist_ids_mv || '\\\\', '') ||
                COALESCE('other:' || other_artist_ids_mv || '\\\\', '')
            ) AS musicbrainz_work_role_artist_mbids
        FROM {USER_VETTED_WORKS_TABLE}
        WHERE vetted = 1 AND {{predicate}}
    """

    def _fetch_chunks(column: str, values: list[str]) -> None:
        for chunk in _chunked(values, LOOKUP_FETCH_CHUNK_SIZE):
            placeholders = ",".join(["?"] * len(chunk))
            predicate = f"{tm_db.quote_ident(column)} IN ({placeholders})"
            query = select_sql.format(predicate=predicate)
            frame = tm_polars_db.sqlite_to_polars(
                lookup_conn,
                query,
                params=chunk,
                dtype_overrides={"work_id": pl.Int64()},
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(False).alias(LOOKUP_SOURCE_COLUMN)))

    def _fetch_vetted_chunks(values: list[str]) -> None:
        if not _table_exists(lookup_conn, USER_VETTED_WORKS_TABLE):
            return
        for chunk in _chunked(values, LOOKUP_FETCH_CHUNK_SIZE):
            placeholders = ",".join(["?"] * len(chunk))
            predicate = f"(observed_title_norm IN ({placeholders}) OR work_title_norm IN ({placeholders}))"
            params = [*chunk, *chunk]
            query = vetted_select_sql.format(predicate=predicate)
            frame = tm_polars_db.sqlite_to_polars(
                lookup_conn,
                query,
                params=params,
                dtype_overrides={"work_id": pl.Int64()},
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(True).alias(LOOKUP_SOURCE_COLUMN)))

    def _fetch_vetted_workid_chunks(values: list[str]) -> None:
        if not _table_exists(lookup_conn, USER_VETTED_WORKS_TABLE):
            return
        for chunk in _chunked(values, LOOKUP_FETCH_CHUNK_SIZE):
            placeholders = ",".join(["?"] * len(chunk))
            predicate = f"musicbrainz_workid IN ({placeholders})"
            query = vetted_select_sql.format(predicate=predicate)
            frame = tm_polars_db.sqlite_to_polars(
                lookup_conn,
                query,
                params=chunk,
                dtype_overrides={"work_id": pl.Int64()},
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(True).alias(LOOKUP_SOURCE_COLUMN)))

    def _fetch_alias_token_chunks(values: list[str]) -> None:
        for chunk in _chunked(values, ALIAS_LIKE_CHUNK_SIZE):
            predicates: list[str] = []
            params: list[str] = []
            for value in chunk:
                predicates.append("all_title_norm_tokens LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(value)}%")
            if not predicates:
                continue
            query = select_sql.format(predicate=" OR ".join(predicates))
            frame = tm_polars_db.sqlite_to_polars(
                lookup_conn,
                query,
                params=params,
                dtype_overrides={"work_id": pl.Int64()},
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(False).alias(LOOKUP_SOURCE_COLUMN)))

    def _fetch_vetted_alias_token_chunks(values: list[str]) -> None:
        if not _table_exists(lookup_conn, USER_VETTED_WORKS_TABLE):
            return
        for chunk in _chunked(values, ALIAS_LIKE_CHUNK_SIZE):
            predicates: list[str] = []
            params: list[str] = []
            for value in chunk:
                predicates.append("all_title_norm_tokens_mv LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(value)}%")
            if not predicates:
                continue
            query = vetted_select_sql.format(predicate=" OR ".join(predicates))
            frame = tm_polars_db.sqlite_to_polars(
                lookup_conn,
                query,
                params=params,
                dtype_overrides={"work_id": pl.Int64()},
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(True).alias(LOOKUP_SOURCE_COLUMN)))

    title_values = sorted({value for value in title_norms if isinstance(value, str) and value.strip()})
    workid_values = sorted({value for value in work_mbids if isinstance(value, str) and value.strip()})

    if title_values:
        _fetch_vetted_chunks(title_values)
        _fetch_vetted_alias_token_chunks(title_values)
        _fetch_chunks("work_title_norm", title_values)
        _fetch_alias_token_chunks(title_values)
    if workid_values:
        _fetch_vetted_workid_chunks(workid_values)
        _fetch_chunks("musicbrainz_workid", workid_values)

    if not frames:
        return pl.DataFrame(
            {
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "musicbrainz_workid": pl.Series(name="musicbrainz_workid", values=[], dtype=pl.Utf8),
                "work_title": pl.Series(name="work_title", values=[], dtype=pl.Utf8),
                "work_title_norm": pl.Series(name="work_title_norm", values=[], dtype=pl.Utf8),
                "all_title_norm_tokens": pl.Series(name="all_title_norm_tokens", values=[], dtype=pl.Utf8),
                ROLE_NAME_COLUMN: pl.Series(name=ROLE_NAME_COLUMN, values=[], dtype=pl.Utf8),
                ROLE_ID_COLUMN: pl.Series(name=ROLE_ID_COLUMN, values=[], dtype=pl.Utf8),
                LOOKUP_SOURCE_COLUMN: pl.Series(name=LOOKUP_SOURCE_COLUMN, values=[], dtype=pl.Boolean),
            }
        )

    return pl.concat(frames, how="vertical_relaxed").unique(subset=["work_id", LOOKUP_SOURCE_COLUMN])


def _explode_people_frame(
    df: pl.DataFrame,
    *,
    source_columns: list[str],
    key_columns: list[str],
) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for column in source_columns:
        if column not in df.columns:
            continue
        frame = (
            df.select(
                [
                    *key_columns,
                    tm_polars.expr_tokens(pl.col(column), delimiter=tm_config.get_multivalue_delimiter()).alias("tokens"),
                ]
            )
            .explode("tokens")
            .with_columns(
                pl.col("tokens").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("person_norm")
            )
            .select([*key_columns, "person_norm"])
            .filter(pl.col("person_norm") != "")
        )
        if not frame.is_empty():
            frames.append(frame)

    if not frames:
        return pl.DataFrame({key: pl.Series(name=key, values=[], dtype=pl.Int64 if key == "rowid" else pl.Utf8) for key in key_columns + ["person_norm"]})

    return pl.concat(frames, how="vertical_relaxed").unique(subset=[*key_columns, "person_norm"])


def _explode_lookup_people(lookup: pl.DataFrame) -> pl.DataFrame:
    """Explode combined role:name strings from canonical_works_metadata.
    
    Input format: 'composer:John\\writer:Jane' (role:name pairs separated by double backslash)
    Output: DataFrame with (work_id, role, person_norm) tuples.
    """
    if ROLE_NAME_COLUMN not in lookup.columns:
        return pl.DataFrame(
            {
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "person_norm": pl.Series(name="person_norm", values=[], dtype=pl.Utf8),
            }
        )
    
    def _split_role_string(role_string: str | None) -> list[tuple[str, str]]:
        """Split 'composer:John\\writer:Jane' into [(role, name), ...]"""
        if not role_string:
            return []
        result = []
        parts = role_string.split(ROLE_DELIMITER)
        for part in parts:
            if not part or ROLE_PAIR_DELIMITER not in part:
                continue
            role, name = part.split(ROLE_PAIR_DELIMITER, 1)
            result.append((role.strip(), name.strip()))
        return result
    
    # Collect all (work_id, role, name) tuples
    tuples: list[tuple[int, str, str]] = []
    for row in lookup.select(["work_id", ROLE_NAME_COLUMN]).iter_rows(named=True):
        work_id = int(row["work_id"])
        role_string = row.get(ROLE_NAME_COLUMN)
        for role, name in _split_role_string(role_string):
            normalized = _normalize_text(name)
            if normalized:
                tuples.append((work_id, role, normalized))
    
    if not tuples:
        return pl.DataFrame(
            {
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "person_norm": pl.Series(name="person_norm", values=[], dtype=pl.Utf8),
            }
        )
    
    return pl.DataFrame(
        {
            "work_id": [t[0] for t in tuples],
            "role": [t[1] for t in tuples],
            "person_norm": [t[2] for t in tuples],
        }
    ).unique(subset=["work_id", "role", "person_norm"])


def _explode_lookup_artist_ids(lookup: pl.DataFrame) -> pl.DataFrame:
    """Explode combined role:mbid strings from canonical_works_metadata.
    
    Input format: 'composer:uuid1\\writer:uuid2' (role:mbid pairs separated by double backslash)
    Output: DataFrame with (work_id, role, artist_id_text) tuples.
    """
    if ROLE_ID_COLUMN not in lookup.columns:
        return pl.DataFrame(
            {
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "artist_id_text": pl.Series(name="artist_id_text", values=[], dtype=pl.Utf8),
            }
        )
    
    def _split_role_string(role_string: str | None) -> list[tuple[str, str]]:
        """Split 'composer:uuid1\\writer:uuid2' into [(role, mbid), ...]"""
        if not role_string:
            return []
        result = []
        parts = role_string.split(ROLE_DELIMITER)
        for part in parts:
            if not part or ROLE_PAIR_DELIMITER not in part:
                continue
            role, mbid = part.split(ROLE_PAIR_DELIMITER, 1)
            mbid_text = mbid.strip()
            if mbid_text:
                result.append((role.strip(), mbid_text))
        return result
    
    # Collect all (work_id, role, mbid) tuples
    tuples: list[tuple[int, str, str]] = []
    for row in lookup.select(["work_id", ROLE_ID_COLUMN]).iter_rows(named=True):
        work_id = int(row["work_id"])
        role_string = row.get(ROLE_ID_COLUMN)
        for role, mbid in _split_role_string(role_string):
            if mbid:
                tuples.append((work_id, role, mbid))
    
    if not tuples:
        return pl.DataFrame(
            {
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "artist_id_text": pl.Series(name="artist_id_text", values=[], dtype=pl.Utf8),
            }
        )
    
    return pl.DataFrame(
        {
            "work_id": [t[0] for t in tuples],
            "role": [t[1] for t in tuples],
            "artist_id_text": [t[2] for t in tuples],
        }
    ).unique(subset=["work_id", "role", "artist_id_text"])


def _explode_track_artist_ids(
    tracks: pl.DataFrame,
    *,
    source_columns: list[str],
    artist_id_by_mbid: dict[str, int],
) -> pl.DataFrame:
    if not source_columns or not artist_id_by_mbid:
        return pl.DataFrame(
            {
                "rowid": pl.Series(name="rowid", values=[], dtype=pl.Int64),
                "artist_id_text": pl.Series(name="artist_id_text", values=[], dtype=pl.Utf8),
            }
        )

    exploded: list[tuple[int, str]] = []
    for row in tracks.select(["rowid", *source_columns]).iter_rows(named=True):
        rowid = int(row["rowid"])
        resolved_ids: set[str] = set()
        for column in source_columns:
            for mbid in _normalize_mbid_set(row.get(column)):
                artist_id = artist_id_by_mbid.get(mbid)
                if artist_id is not None:
                    resolved_ids.add(str(artist_id))
        for artist_id_text in resolved_ids:
            exploded.append((rowid, artist_id_text))

    if not exploded:
        return pl.DataFrame(
            {
                "rowid": pl.Series(name="rowid", values=[], dtype=pl.Int64),
                "artist_id_text": pl.Series(name="artist_id_text", values=[], dtype=pl.Utf8),
            }
        )

    return pl.DataFrame(
        {
            "rowid": [rowid for rowid, _ in exploded],
            "artist_id_text": [artist_id_text for _, artist_id_text in exploded],
        }
    ).unique(subset=["rowid", "artist_id_text"])


def _build_candidate_rows(
    tracks: pl.DataFrame,
    lookup: pl.DataFrame,
    artist_id_by_mbid: dict[str, int],
) -> pl.DataFrame:
    candidate_columns = [
        "rowid",
        "__path",
        "__sqlmodded",
        "work",
        "musicbrainz_workid",
        "title",
        "title_norm",
        "work_id",
        "work_mbid",
        "work_title",
        "work_title_norm",
        LOOKUP_SOURCE_COLUMN,
        "title_score",
        "alias_score",
        "workid_score",
        "vetted_score",
        "exact_title_norm",
        "exact_alias_norm",
        "exact_workid",
    ]

    track_base = tracks.select(
        [
            "rowid",
            "__path",
            "__sqlmodded",
            "work",
            "musicbrainz_workid",
            "title",
            "title_norm",
        ]
    )

    lookup_title = lookup.select(["work_id", "musicbrainz_workid", "work_title", "work_title_norm", LOOKUP_SOURCE_COLUMN])
    title_pairs = (
        track_base.join(lookup_title, left_on="title_norm", right_on="work_title_norm", how="inner")
        .with_columns([
            pl.col("musicbrainz_workid_right").alias("work_mbid"),
        ])
        .select(
            [
                "rowid",
                "__path",
                "__sqlmodded",
                "work",
                "musicbrainz_workid",
                "title",
                "title_norm",
                "work_id",
                "work_mbid",
                "work_title",
                LOOKUP_SOURCE_COLUMN,
            ]
        )
        .with_columns(
            [
                pl.col("title_norm").alias("work_title_norm"),
                pl.col(LOOKUP_SOURCE_COLUMN).fill_null(False),
                pl.lit(35).alias("title_score"),
                pl.lit(0).alias("alias_score"),
                pl.lit(0).alias("workid_score"),
                pl.when(pl.col(LOOKUP_SOURCE_COLUMN)).then(pl.lit(40)).otherwise(pl.lit(0)).alias("vetted_score"),
                pl.lit(True).alias("exact_title_norm"),
                pl.lit(False).alias("exact_alias_norm"),
                pl.lit(False).alias("exact_workid"),
            ]
        )
        .select(candidate_columns)
    )

    workid_pairs = (
        track_base.join(lookup_title, left_on="musicbrainz_workid", right_on="musicbrainz_workid", how="inner")
        .with_columns([
            pl.col("musicbrainz_workid").alias("work_mbid"),
        ])
        .select(
            [
                "rowid",
                "__path",
                "__sqlmodded",
                "work",
                "musicbrainz_workid",
                "title",
                "title_norm",
                "work_id",
                "work_mbid",
                "work_title",
                "work_title_norm",
                LOOKUP_SOURCE_COLUMN,
            ]
        )
        .with_columns(
            [
                pl.col(LOOKUP_SOURCE_COLUMN).fill_null(False),
                pl.lit(0).alias("title_score"),
                pl.lit(0).alias("alias_score"),
                pl.lit(70).alias("workid_score"),
                pl.when(pl.col(LOOKUP_SOURCE_COLUMN)).then(pl.lit(40)).otherwise(pl.lit(0)).alias("vetted_score"),
                pl.lit(False).alias("exact_title_norm"),
                pl.lit(False).alias("exact_alias_norm"),
                pl.lit(True).alias("exact_workid"),
            ]
        )
        .select(candidate_columns)
    )

    lookup_alias_tokens = (
        lookup.select(["work_id", "musicbrainz_workid", "work_title", "work_title_norm", "all_title_norm_tokens", LOOKUP_SOURCE_COLUMN])
        .rename({"musicbrainz_workid": "work_mbid"})
        .with_columns(
            tm_polars.expr_tokens(
                pl.col("all_title_norm_tokens"),
                delimiter=tm_config.get_multivalue_delimiter(),
            ).alias("tokens")
        )
        .explode("tokens")
        .with_columns(
            pl.col("tokens").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("alias_title_norm")
        )
        .filter((pl.col("alias_title_norm") != "") & (pl.col("alias_title_norm") != pl.col("work_title_norm")))
        .select(["work_id", "work_mbid", "work_title", "work_title_norm", "alias_title_norm", LOOKUP_SOURCE_COLUMN])
        .unique(subset=["work_id", "alias_title_norm", LOOKUP_SOURCE_COLUMN])
    )

    alias_pairs = track_base.join(
        lookup_alias_tokens,
        left_on="title_norm",
        right_on="alias_title_norm",
        how="inner",
    ).select(
        [
            "rowid",
            "__path",
            "__sqlmodded",
            "work",
            "musicbrainz_workid",
            "title",
            "title_norm",
            "work_id",
            "work_mbid",
            "work_title",
            "work_title_norm",
            LOOKUP_SOURCE_COLUMN,
        ]
    ).with_columns(
        [
            pl.col(LOOKUP_SOURCE_COLUMN).fill_null(False),
            pl.lit(0).alias("title_score"),
            pl.lit(20).alias("alias_score"),
            pl.lit(0).alias("workid_score"),
            pl.when(pl.col(LOOKUP_SOURCE_COLUMN)).then(pl.lit(40)).otherwise(pl.lit(0)).alias("vetted_score"),
            pl.lit(False).alias("exact_title_norm"),
            pl.lit(True).alias("exact_alias_norm"),
            pl.lit(False).alias("exact_workid"),
        ]
    ).select(candidate_columns)

    candidate_pairs = pl.concat([title_pairs, workid_pairs, alias_pairs], how="vertical_relaxed")
    if candidate_pairs.is_empty():
        return candidate_pairs

    exact_title_counts = (
        candidate_pairs.filter(pl.col("exact_title_norm"))
        .group_by("rowid")
        .agg(pl.len().alias("exact_title_candidate_count"))
    )

    candidate_pairs = candidate_pairs.group_by(["rowid", "work_id"]).agg(
        [
            pl.first("__path").alias("__path"),
            pl.first("__sqlmodded").alias("__sqlmodded"),
            pl.first("work").alias("work"),
            pl.first("musicbrainz_workid").alias("musicbrainz_workid"),
            pl.first("title").alias("title"),
            pl.first("title_norm").alias("title_norm"),
            pl.first("work_mbid").alias("work_mbid"),
            pl.first("work_title").alias("work_title"),
            pl.first("work_title_norm").alias("work_title_norm"),
            pl.max(LOOKUP_SOURCE_COLUMN).alias(LOOKUP_SOURCE_COLUMN),
            pl.sum("title_score").alias("title_score"),
            pl.sum("alias_score").alias("alias_score"),
            pl.sum("workid_score").alias("workid_score"),
            pl.sum("vetted_score").alias("vetted_score"),
            pl.max("exact_title_norm").alias("exact_title_norm"),
            pl.max("exact_alias_norm").alias("exact_alias_norm"),
            pl.max("exact_workid").alias("exact_workid"),
        ]
    )

    candidate_rowids = candidate_pairs.get_column("rowid").unique().to_list()
    if not candidate_rowids:
        return candidate_pairs

    candidate_tracks = track_base.filter(pl.col("rowid").is_in(candidate_rowids))
    track_people = _explode_people_frame(
        candidate_tracks,
        source_columns=[col for col in TRACK_CONTRIBUTOR_COLUMNS if col in candidate_tracks.columns],
        key_columns=["rowid"],
    )
    track_artist_ids = _explode_track_artist_ids(
        candidate_tracks,
        source_columns=[col for col in TRACK_ARTIST_MBID_COLUMNS if col in candidate_tracks.columns],
        artist_id_by_mbid=artist_id_by_mbid,
    )
    lookup_people = _explode_lookup_people(lookup)
    lookup_artist_ids = _explode_lookup_artist_ids(lookup)

    people_join_possible = not track_people.is_empty() and not lookup_people.is_empty()
    artist_id_join_possible = not track_artist_ids.is_empty() and not lookup_artist_ids.is_empty()

    if not people_join_possible and not artist_id_join_possible:
        candidate_rows = candidate_pairs.with_columns(
            [
                pl.lit(0).alias("people_score"),
                pl.lit(0).alias("artist_id_score"),
                pl.lit(False).alias("person_match"),
                pl.lit(0).alias("person_match_count"),
                pl.lit(False).alias("artist_id_match"),
                pl.lit(0).alias("artist_id_match_count"),
            ]
        )
    else:
        matched_people = pl.DataFrame(
            {
                "rowid": pl.Series(name="rowid", values=[], dtype=pl.Int64),
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "person_norm": pl.Series(name="person_norm", values=[], dtype=pl.Utf8),
            }
        )
        if people_join_possible:
            matched_people = (
                track_people.join(lookup_people, on="person_norm", how="inner")
                .select(["rowid", "work_id", "role", "person_norm"])
                .unique(subset=["rowid", "work_id", "role", "person_norm"])
            )

        matched_artist_ids = pl.DataFrame(
            {
                "rowid": pl.Series(name="rowid", values=[], dtype=pl.Int64),
                "work_id": pl.Series(name="work_id", values=[], dtype=pl.Int64),
                "role": pl.Series(name="role", values=[], dtype=pl.Utf8),
                "artist_id_text": pl.Series(name="artist_id_text", values=[], dtype=pl.Utf8),
            }
        )
        if artist_id_join_possible:
            matched_artist_ids = (
                track_artist_ids.join(lookup_artist_ids, on="artist_id_text", how="inner")
                .select(["rowid", "work_id", "role", "artist_id_text"])
                .unique(subset=["rowid", "work_id", "role", "artist_id_text"])
            )

        role_counts = matched_people.group_by(["rowid", "work_id", "role"]).agg(pl.len().alias("match_count"))
        role_wide = role_counts.pivot(
            index=["rowid", "work_id"],
            values="match_count",
            on="role",
            aggregate_function="first",
        )

        if role_wide.is_empty():
            role_wide = pl.DataFrame({"rowid": [], "work_id": []})

        for role in ROLE_SCORE_RULES:
            if role not in role_wide.columns:
                role_wide = role_wide.with_columns(pl.lit(0).alias(role))

        id_role_counts = matched_artist_ids.group_by(["rowid", "work_id", "role"]).agg(pl.len().alias("id_match_count"))
        id_role_wide = id_role_counts.pivot(
            index=["rowid", "work_id"],
            values="id_match_count",
            on="role",
            aggregate_function="first",
        )
        if id_role_wide.is_empty():
            id_role_wide = pl.DataFrame({"rowid": [], "work_id": []})
        for role in ROLE_SCORE_RULES:
            if role not in id_role_wide.columns:
                id_role_wide = id_role_wide.with_columns(pl.lit(0).alias(role))

        score_exprs = []
        id_score_exprs = []
        reason_exprs = []
        id_reason_exprs = []
        for role, (cap, weight) in ROLE_SCORE_RULES.items():
            score_exprs.append(
                pl.when(pl.col(role) > 0)
                .then(pl.min_horizontal(pl.lit(cap), pl.col(role) * weight))
                .otherwise(pl.lit(0))
            )
            reason_exprs.append(pl.when(pl.col(role) > 0).then(pl.lit(f"{role}_match")).otherwise(pl.lit("")))
            id_score_exprs.append(
                pl.when(pl.col(f"id_{role}") > 0)
                .then(pl.min_horizontal(pl.lit(cap), pl.col(f"id_{role}") * weight))
                .otherwise(pl.lit(0))
            )
            id_reason_exprs.append(
                pl.when(pl.col(f"id_{role}") > 0).then(pl.lit(f"{role}_artistid_match")).otherwise(pl.lit(""))
            )

        id_role_wide = id_role_wide.rename({role: f"id_{role}" for role in ROLE_SCORE_RULES})
        role_wide = role_wide.join(id_role_wide, on=["rowid", "work_id"], how="left")
        for role in ROLE_SCORE_RULES:
            if f"id_{role}" not in role_wide.columns:
                role_wide = role_wide.with_columns(pl.lit(0).alias(f"id_{role}"))

        role_wide = role_wide.with_columns(
            [
                pl.sum_horizontal(score_exprs).alias("people_score"),
                pl.sum_horizontal(id_score_exprs).alias("artist_id_score"),
                (
                    (pl.sum_horizontal([pl.col(role) for role in ROLE_SCORE_RULES]) > 0)
                    | (pl.sum_horizontal([pl.col(f"id_{role}") for role in ROLE_SCORE_RULES]) > 0)
                ).alias("person_match"),
                pl.sum_horizontal([pl.col(role) for role in ROLE_SCORE_RULES]).alias("person_match_count"),
                (pl.sum_horizontal([pl.col(f"id_{role}") for role in ROLE_SCORE_RULES]) > 0).alias("artist_id_match"),
                pl.sum_horizontal([pl.col(f"id_{role}") for role in ROLE_SCORE_RULES]).alias("artist_id_match_count"),
                pl.concat_str(reason_exprs, separator=";")
                .str.replace_all(r";{2,}", ";")
                .str.strip_chars(";")
                .alias("person_reason_codes"),
                pl.concat_str(id_reason_exprs, separator=";")
                .str.replace_all(r";{2,}", ";")
                .str.strip_chars(";")
                .alias("artist_id_reason_codes"),
            ]
        )

        candidate_rows = candidate_pairs.join(role_wide, on=["rowid", "work_id"], how="left").with_columns(
            [
                pl.col(role).fill_null(0) for role in ROLE_SCORE_RULES
            ] + [
                pl.col(f"id_{role}").fill_null(0) for role in ROLE_SCORE_RULES
            ] + [
                pl.col("people_score").fill_null(0),
                pl.col("artist_id_score").fill_null(0),
                pl.col("person_match").fill_null(False),
                pl.col("person_match_count").fill_null(0),
                pl.col("artist_id_match").fill_null(False),
                pl.col("artist_id_match_count").fill_null(0),
                pl.col("person_reason_codes").fill_null(""),
                pl.col("artist_id_reason_codes").fill_null(""),
            ]
        )

    missing_role_exprs = [
        pl.lit(0).alias(role)
        for role in ROLE_SCORE_RULES
        if role not in candidate_rows.columns
    ]
    if "person_reason_codes" not in candidate_rows.columns:
        missing_role_exprs.append(pl.lit("").alias("person_reason_codes"))
    if "artist_id_reason_codes" not in candidate_rows.columns:
        missing_role_exprs.append(pl.lit("").alias("artist_id_reason_codes"))
    for role in ROLE_SCORE_RULES:
        if f"id_{role}" not in candidate_rows.columns:
            missing_role_exprs.append(pl.lit(0).alias(f"id_{role}"))
    if missing_role_exprs:
        candidate_rows = candidate_rows.with_columns(missing_role_exprs)

    if "artist_id_score" not in candidate_rows.columns:
        candidate_rows = candidate_rows.with_columns(pl.lit(0).alias("artist_id_score"))
    if "artist_id_match" not in candidate_rows.columns:
        candidate_rows = candidate_rows.with_columns(pl.lit(False).alias("artist_id_match"))
    if "artist_id_match_count" not in candidate_rows.columns:
        candidate_rows = candidate_rows.with_columns(pl.lit(0).alias("artist_id_match_count"))

    candidate_rows = candidate_rows.with_columns(
        [
            (
                pl.col("title_score")
                + pl.col("alias_score")
                + pl.col("workid_score")
                + pl.col("vetted_score")
                + pl.col("people_score")
                + pl.col("artist_id_score")
            ).alias("total_score"),
            pl.concat_str(
                [
                    pl.when(pl.col(LOOKUP_SOURCE_COLUMN)).then(pl.lit("user_vetted_source")).otherwise(pl.lit("")),
                    pl.when(pl.col("exact_workid")).then(pl.lit("exact_workid")).otherwise(pl.lit("")),
                    pl.when(pl.col("exact_title_norm")).then(pl.lit("exact_title_norm")).otherwise(pl.lit("")),
                    pl.when(pl.col("exact_alias_norm")).then(pl.lit("exact_alias_norm")).otherwise(pl.lit("")),
                    *[
                        pl.when(pl.col(role) > 0).then(pl.lit(f"{role}_match")).otherwise(pl.lit(""))
                        for role in ROLE_SCORE_RULES
                    ],
                    *[
                        pl.when(pl.col(f"id_{role}") > 0).then(pl.lit(f"{role}_artistid_match")).otherwise(pl.lit(""))
                        for role in ROLE_SCORE_RULES
                    ],
                    pl.when(pl.col("exact_title_norm") & ~pl.col("person_match")).then(pl.lit("title_only")).otherwise(pl.lit("")),
                    pl.when(pl.col("exact_alias_norm") & ~pl.col("person_match")).then(pl.lit("alias_only")).otherwise(pl.lit("")),
                ],
                separator=";",
            )
            .str.replace_all(r";{2,}", ";")
            .str.strip_chars(";")
            .alias("reason_codes"),
        ]
    )

    score_lists = candidate_rows.group_by("rowid").agg(
        [
            pl.col("total_score").sort(descending=True).alias("scores"),
            pl.len().alias("candidate_count"),
        ]
    ).with_columns(
        [
            pl.col("scores").list.get(0, null_on_oob=True).fill_null(0).alias("best_score"),
            pl.col("scores").list.get(1, null_on_oob=True).fill_null(0).alias("runner_up_score"),
        ]
    ).drop("scores")

    best_rows = candidate_rows.sort(["rowid", "total_score", "work_id"], descending=[False, True, True]).group_by(
        "rowid"
    ).agg(pl.all().first())

    return (
        best_rows.join(score_lists, on="rowid", how="left")
        .join(exact_title_counts, on="rowid", how="left")
        .with_columns(pl.col("exact_title_candidate_count").fill_null(0))
    )


def _materialize_summary_frame(
    tracks: pl.DataFrame,
    candidate_rows: pl.DataFrame,
    *,
    require_corroboration_for_title_only: bool,
    require_exact_workid_or_unique_exact_title: bool,
) -> pl.DataFrame:
    base = tracks.select(
        ["rowid", "__path", "__sqlmodded", "work", "musicbrainz_workid", "title", "title_norm", "_eligible_for_matching"]
    )

    if candidate_rows.is_empty():
        return base.with_columns(
            [
                pl.lit(None, dtype=pl.Int64).alias("best_work_id"),
                pl.lit(None, dtype=pl.Utf8).alias("best_musicbrainz_workid"),
                pl.lit(None, dtype=pl.Utf8).alias("best_work_title"),
                pl.lit(None, dtype=pl.Utf8).alias("best_work_title_norm"),
                pl.lit(0).alias("best_score"),
                pl.lit(0).alias("runner_up_score"),
                pl.lit(0).alias("candidate_count"),
                pl.lit("D").alias("confidence_tier"),
                pl.lit("no_candidate").alias("reason_codes"),
                pl.lit(0).alias("should_apply"),
                pl.lit(0).alias("applied"),
                pl.lit(None, dtype=pl.Utf8).alias("applied_utc"),
                pl.lit(None, dtype=pl.Utf8).alias("notes"),
                pl.lit(0).alias("_pre_guardrail_should_apply"),
                pl.lit(False).alias("_guardrail_block_apply"),
                pl.lit(False).alias("_route_block_apply"),
            ]
        ).select(
            [
                pl.col("rowid"),
                pl.col("__path").alias("alib_path"),
                pl.col("work").alias("existing_work"),
                pl.col("musicbrainz_workid").alias("existing_musicbrainz_workid"),
                pl.col("title"),
                pl.col("title_norm"),
                pl.col("best_work_id"),
                pl.col("best_work_mbid"),
                pl.col("best_work_title"),
                pl.col("best_work_title_norm"),
                pl.col("best_score"),
                pl.col("runner_up_score"),
                pl.col("candidate_count"),
                pl.col("confidence_tier"),
                pl.col("reason_codes"),
                pl.col("should_apply"),
                pl.col("applied"),
                pl.col("applied_utc"),
                pl.col("notes"),
                pl.col("_pre_guardrail_should_apply"),
                pl.col("_guardrail_block_apply"),
                pl.col("_route_block_apply"),
            ]
        )

    candidate_rows = candidate_rows.sort(["rowid", "total_score", "work_id"], descending=[False, True, True]).group_by(
        "rowid"
    ).agg(pl.all().first())

    summary = base.join(candidate_rows, on="rowid", how="left")
    summary = summary.with_columns(
        [
            pl.col("work_id").alias("best_work_id"),
            pl.col("work_mbid").alias("best_work_mbid"),
            pl.col("work_title").alias("best_work_title"),
            pl.col("work_title_norm").alias("best_work_title_norm"),
            pl.col("best_score").fill_null(0),
            pl.col("runner_up_score").fill_null(0),
            pl.col("candidate_count").fill_null(0),
            pl.col("reason_codes").fill_null("no_candidate"),
            pl.col("person_match").fill_null(False),
            pl.col("artist_id_match").fill_null(False),
            pl.col("exact_workid").fill_null(False),
            pl.col("exact_title_norm").fill_null(False),
            pl.col("exact_title_candidate_count").fill_null(0),
        ]
    )
    summary = summary.with_columns(
        pl.when(
            (pl.col("reason_codes") == "no_candidate")
            & (~pl.col("_eligible_for_matching").fill_null(False))
        )
        .then(pl.lit("insufficient_matching_metadata"))
        .otherwise(pl.col("reason_codes"))
        .alias("reason_codes")
    )
    if require_corroboration_for_title_only:
        summary = summary.with_columns(
            (
                pl.col("reason_codes").str.contains(r"(^|;)(title_only|alias_only)(;|$)")
                & ~(pl.col("person_match") | pl.col("artist_id_match"))
            ).alias("guardrail_block_apply")
        )
    else:
        summary = summary.with_columns(pl.lit(False).alias("guardrail_block_apply"))

    if require_exact_workid_or_unique_exact_title:
        summary = summary.with_columns(
            (
                ~pl.col("exact_workid")
                & ~(pl.col("exact_title_norm") & (pl.col("exact_title_candidate_count") == 1))
            ).alias("route_block_apply")
        )
    else:
        summary = summary.with_columns(pl.lit(False).alias("route_block_apply"))
    summary = summary.with_columns(
        [
            pl.when(pl.col("best_work_id").is_null()).then(pl.lit("D")).otherwise(
                pl.when(pl.col("exact_workid") & (pl.col("best_score") >= 90) & ((pl.col("best_score") - pl.col("runner_up_score")) >= 20)).then(pl.lit("A"))
                .when((pl.col("best_score") >= 80) & ((pl.col("best_score") - pl.col("runner_up_score") >= 20)) & pl.col("person_match")).then(pl.lit("B"))
                .when(pl.col("best_score") >= 60).then(pl.lit("C"))
                .otherwise(pl.lit("D"))
            ).alias("confidence_tier"),
        ]
    )
    summary = summary.with_columns(
        [
            pl.when(
                pl.col("best_work_id").is_not_null()
                & (pl.col("confidence_tier").is_in(["A", "B"]))
                & (pl.col("best_score") >= AUTO_APPLY_SCORE)
                & ((pl.col("best_score") - pl.col("runner_up_score")) >= AUTO_APPLY_MARGIN)
                & ~pl.col("route_block_apply")
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("_pre_guardrail_should_apply"),
        ]
    )
    summary = summary.with_columns(
        [
            pl.when(pl.col("_pre_guardrail_should_apply") == 1)
            .then(pl.when(~pl.col("guardrail_block_apply")).then(pl.lit(1)).otherwise(pl.lit(0)))
            .otherwise(pl.lit(0))
            .alias("should_apply"),
            pl.lit(0).alias("applied"),
            pl.lit(None, dtype=pl.Utf8).alias("applied_utc"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.col("guardrail_block_apply").alias("_guardrail_block_apply"),
            pl.col("route_block_apply").alias("_route_block_apply"),
        ]
    )

    return summary.select(
        [
            pl.col("rowid"),
            pl.col("__path").alias("alib_path"),
            pl.col("work").alias("existing_work"),
            pl.col("musicbrainz_workid").alias("existing_musicbrainz_workid"),
            pl.col("title"),
            pl.col("title_norm"),
            pl.col("best_work_id"),
            pl.col("best_work_mbid"),
            pl.col("best_work_title"),
            pl.col("best_work_title_norm"),
            pl.col("best_score"),
            pl.col("runner_up_score"),
            pl.col("candidate_count"),
            pl.col("confidence_tier"),
            pl.col("reason_codes"),
            pl.col("should_apply"),
            pl.col("applied"),
            pl.col("applied_utc"),
            pl.col("notes"),
            pl.col("_pre_guardrail_should_apply"),
            pl.col("_guardrail_block_apply"),
            pl.col("_route_block_apply"),
        ]
    )


def _create_candidate_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {CANDIDATE_TABLE}")
    cursor.execute(
        f"""
        CREATE TABLE {CANDIDATE_TABLE} (
            rowid INTEGER PRIMARY KEY,
            alib_path TEXT,
            existing_work TEXT,
            existing_musicbrainz_workid TEXT,
            title TEXT,
            title_norm TEXT,
            best_work_id INTEGER,
            best_musicbrainz_workid TEXT,
            best_work_title TEXT,
            best_work_title_norm TEXT,
            best_score INTEGER,
            runner_up_score INTEGER,
            candidate_count INTEGER,
            confidence_tier TEXT,
            reason_codes TEXT,
            should_apply INTEGER NOT NULL DEFAULT 0,
            applied INTEGER NOT NULL DEFAULT 0,
            applied_utc TEXT,
            notes TEXT
        )
        """
    )
    cursor.execute(f"CREATE INDEX idx_{CANDIDATE_TABLE}_tier ON {CANDIDATE_TABLE}(confidence_tier)")
    cursor.execute(f"CREATE INDEX idx_{CANDIDATE_TABLE}_workid ON {CANDIDATE_TABLE}(best_musicbrainz_workid)")


def _write_candidate_rows(conn: sqlite3.Connection, rows: pl.DataFrame) -> None:
    cursor = conn.cursor()
    _create_candidate_table(cursor)
    insert_sql = f"""
        INSERT INTO {CANDIDATE_TABLE} (
            rowid,
            alib_path,
            existing_work,
            existing_musicbrainz_workid,
            title,
            title_norm,
            best_work_id,
            best_musicbrainz_workid,
            best_work_title,
            best_work_title_norm,
            best_score,
            runner_up_score,
            candidate_count,
            confidence_tier,
            reason_codes,
            should_apply,
            applied,
            applied_utc,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    batch: list[tuple[Any, ...]] = []
    columns = [
        "rowid",
        "alib_path",
        "existing_work",
        "existing_musicbrainz_workid",
        "title",
        "title_norm",
        "best_work_id",
        "best_work_mbid",
        "best_work_title",
        "best_work_title_norm",
        "best_score",
        "runner_up_score",
        "candidate_count",
        "confidence_tier",
        "reason_codes",
        "should_apply",
        "applied",
        "applied_utc",
        "notes",
    ]
    for record in rows.select(columns).iter_rows(named=True):
        batch.append(tuple(record[column] for column in columns))
        if len(batch) >= 5_000:
            cursor.executemany(insert_sql, batch)
            batch.clear()
    if batch:
        cursor.executemany(insert_sql, batch)
    conn.commit()


def _upsert_user_vetted_works(
    master_conn: sqlite3.Connection,
    summary_rows: pl.DataFrame,
    lookup_subset: pl.DataFrame,
) -> tuple[int, int]:
    _ensure_user_vetted_works_table(master_conn)
    candidates = summary_rows.filter(pl.col("best_work_id").is_not_null())
    if candidates.is_empty():
        return (0, 0)

    lookup_dedup = (
        lookup_subset.sort(LOOKUP_SOURCE_COLUMN, descending=True)
        .group_by("work_id")
        .agg(pl.all().first())
    )
    source_rows = candidates.join(
        lookup_dedup,
        left_on="best_work_id",
        right_on="work_id",
        how="left",
    )
    if source_rows.is_empty():
        return (0, 0)

    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()
    upsert_sql = f"""
        INSERT INTO {USER_VETTED_WORKS_TABLE} (
            alib_path,
            observed_title,
            observed_title_norm,
            work_id,
            musicbrainz_workid,
            work_title,
            work_title_norm,
            all_title_norm_tokens_mv,
            composer_artist_ids_mv,
            arranger_artist_ids_mv,
            lyricist_artist_ids_mv,
            writer_artist_ids_mv,
            orchestrator_artist_ids_mv,
            translator_artist_ids_mv,
            other_artist_ids_mv,
            composer_artist_names_mv,
            arranger_artist_names_mv,
            lyricist_artist_names_mv,
            writer_artist_names_mv,
            orchestrator_artist_names_mv,
            translator_artist_names_mv,
            other_artist_names_mv,
            confidence_tier,
            best_score,
            reason_codes,
            vetted,
            first_seen_utc,
            last_seen_utc,
            source_script
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(alib_path) DO UPDATE SET
            observed_title = excluded.observed_title,
            observed_title_norm = excluded.observed_title_norm,
            work_id = excluded.work_id,
            musicbrainz_workid = excluded.musicbrainz_workid,
            work_title = excluded.work_title,
            work_title_norm = excluded.work_title_norm,
            all_title_norm_tokens_mv = excluded.all_title_norm_tokens_mv,
            composer_artist_ids_mv = excluded.composer_artist_ids_mv,
            arranger_artist_ids_mv = excluded.arranger_artist_ids_mv,
            lyricist_artist_ids_mv = excluded.lyricist_artist_ids_mv,
            writer_artist_ids_mv = excluded.writer_artist_ids_mv,
            orchestrator_artist_ids_mv = excluded.orchestrator_artist_ids_mv,
            translator_artist_ids_mv = excluded.translator_artist_ids_mv,
            other_artist_ids_mv = excluded.other_artist_ids_mv,
            composer_artist_names_mv = excluded.composer_artist_names_mv,
            arranger_artist_names_mv = excluded.arranger_artist_names_mv,
            lyricist_artist_names_mv = excluded.lyricist_artist_names_mv,
            writer_artist_names_mv = excluded.writer_artist_names_mv,
            orchestrator_artist_names_mv = excluded.orchestrator_artist_names_mv,
            translator_artist_names_mv = excluded.translator_artist_names_mv,
            other_artist_names_mv = excluded.other_artist_names_mv,
            confidence_tier = excluded.confidence_tier,
            best_score = excluded.best_score,
            reason_codes = excluded.reason_codes,
            vetted = MAX({USER_VETTED_WORKS_TABLE}.vetted, excluded.vetted),
            last_seen_utc = excluded.last_seen_utc,
            source_script = excluded.source_script
    """

    batch: list[tuple[Any, ...]] = []
    vetted_count = 0
    for row in source_rows.iter_rows(named=True):
        alib_path = row.get("alib_path")
        if alib_path is None:
            continue
        vetted_value = 1 if int(row.get("should_apply") or 0) == 1 else 0
        vetted_count += vetted_value
        merged_tokens = _merge_multivalue_tokens(
            row.get("all_title_norm_tokens_mv"),
            row.get("title_norm"),
            row.get("best_work_title_norm"),
        )
        batch.append(
            (
                str(alib_path),
                row.get("title"),
                row.get("title_norm"),
                row.get("best_work_id"),
                row.get("best_work_mbid"),
                row.get("best_work_title"),
                row.get("best_work_title_norm"),
                merged_tokens,
                row.get("composer_artist_ids_mv"),
                row.get("arranger_artist_ids_mv"),
                row.get("lyricist_artist_ids_mv"),
                row.get("writer_artist_ids_mv"),
                row.get("orchestrator_artist_ids_mv"),
                row.get("translator_artist_ids_mv"),
                row.get("other_artist_ids_mv"),
                row.get("composer_artist_names_mv"),
                row.get("arranger_artist_names_mv"),
                row.get("lyricist_artist_names_mv"),
                row.get("writer_artist_names_mv"),
                row.get("orchestrator_artist_names_mv"),
                row.get("translator_artist_names_mv"),
                row.get("other_artist_names_mv"),
                row.get("confidence_tier"),
                int(row.get("best_score") or 0),
                row.get("reason_codes"),
                vetted_value,
                timestamp,
                timestamp,
                script,
            )
        )
        if len(batch) >= 2_000:
            master_conn.executemany(upsert_sql, batch)
            batch.clear()

    if batch:
        master_conn.executemany(upsert_sql, batch)
    master_conn.commit()
    return (source_rows.height, vetted_count)


def _apply_high_confidence_matches(
    conn: sqlite3.Connection,
    rows: pl.DataFrame,
) -> int:
    existing_columns = _table_columns(conn, TRACK_TABLE)
    can_update_work = "work" in existing_columns
    can_update_workid = "musicbrainz_workid" in existing_columns
    if not (can_update_work or can_update_workid):
        log.warning("No target columns found in %s; skipping apply phase.", TRACK_TABLE)
        return 0

    cursor = conn.cursor()
    tm_db.ensure_changelog_table(conn)
    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()
    updates = 0

    selected = rows.filter(pl.col("should_apply") == 1)
    if selected.is_empty():
        log.info("No rows met auto-apply criteria.")
        return 0

    rowids = [int(rowid) for rowid in selected.get_column("rowid").to_list()]
    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, rowids)
    update_cols: list[str] = []
    if can_update_work:
        update_cols.append("work")
    if can_update_workid:
        update_cols.append("musicbrainz_workid")

    update_sql = tm_db.build_update_sql(table=TRACK_TABLE, set_cols=update_cols)

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)
        for row in selected.iter_rows(named=True):
            existing = conn.execute(
                f"SELECT {', '.join(update_cols)}, COALESCE(__sqlmodded, 0) FROM {TRACK_TABLE} WHERE rowid = ?",
                (int(row["rowid"]),),
            ).fetchone()
            if existing is None:
                continue

            values: list[Any] = []
            changes: list[tuple[str, Any, Any]] = []
            idx = 0
            if can_update_work:
                current_work = existing[idx]
                idx += 1
                new_work = current_work
                if (current_work is None or str(current_work).strip() == "") and row.get("best_work_title"):
                    new_work = row.get("best_work_title")
                values.append(new_work)
                if new_work != current_work:
                    changes.append(("work", current_work, new_work))
            if can_update_workid:
                current_workid = existing[idx]
                new_workid = row.get("best_musicbrainz_workid") if row.get("best_musicbrainz_workid") else current_workid
                values.append(new_workid)
                if new_workid != current_workid:
                    changes.append(("musicbrainz_workid", current_workid, new_workid))

            current_sqlmodded = int(existing[-1] or 0)
            values.append(current_sqlmodded + max(1, len(changes)))

            if not changes:
                continue

            values.append(int(row["rowid"]))
            cursor.execute(update_sql, values)
            updates += 1
            alib_path = path_by_rowid.get(int(row["rowid"]), row.get("alib_path") or str(row["rowid"]))
            changelog.add(alib_path=alib_path, changes=changes)
            changelog.flush(cursor)

        cursor.execute(
            f"""
            UPDATE {CANDIDATE_TABLE}
            SET applied = 1,
                applied_utc = ?
            WHERE rowid IN ({','.join(['?'] * len(rowids))})
              AND should_apply = 1
            """,
            [timestamp, *rowids],
        )

    conn.commit()
    return updates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Infer work links for library tracks using MusicBrainz work lookup data.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite database path (default: tagminder.toml [db].path).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = _load_db_path(default=args.db)
    master_db_path = _load_master_data_db_path()
    require_corroboration_for_title_only = _load_infer_works_guardrail_config()
    require_exact_workid_or_unique_exact_title = _load_infer_works_unique_title_auto_apply_config()
    log.info("Connecting to staging database: %s", db_path)
    log.info("Connecting to master-data database: %s", master_db_path)
    log.info(
        "Auto-apply guardrail (title/alias-only requires corroboration): %s",
        "enabled" if require_corroboration_for_title_only else "disabled",
    )
    log.info(
        "Auto-apply route guardrail (exact workid or unique exact title required): %s",
        "enabled" if require_exact_workid_or_unique_exact_title else "disabled",
    )
    conn = tm_db.connect(db_path)
    lookup_conn = tm_db.connect(master_db_path, read_only=True)
    master_write_conn = tm_db.connect(master_db_path)

    try:
        if not _table_exists(lookup_conn, WORK_LOOKUP_TABLE):
            raise RuntimeError(
                f"Missing required lookup table {WORK_LOOKUP_TABLE} in master-data DB ({master_db_path}). Run build_mb_work_lookup.py first."
            )

        track_rows = _load_track_frame(conn)
        log.info("Loaded %d track rows", track_rows.height)

        eligible_track_rows = track_rows.filter(pl.col("_eligible_for_matching").fill_null(False))
        skipped_sparse_rows = int(track_rows.height - eligible_track_rows.height)
        log.info(
            "Eligible for matching: %d rows (skipped sparse rows: %d)",
            eligible_track_rows.height,
            skipped_sparse_rows,
        )

        artist_id_by_mbid = _load_artist_id_map(
            lookup_conn,
            mbids=_collect_track_artist_mbids(eligible_track_rows),
        )
        log.info("Loaded %d MBID→artist_id mappings", len(artist_id_by_mbid))

        lookup_subset = _load_lookup_subset(
            lookup_conn,
            title_norms=eligible_track_rows.get_column("title_norm").drop_nulls().unique().to_list(),
            work_mbids=eligible_track_rows.get_column("musicbrainz_workid").drop_nulls().unique().to_list(),
        )
        log.info("Loaded %d lookup rows", lookup_subset.height)

        candidate_rows = _build_candidate_rows(
            eligible_track_rows,
            lookup_subset,
            artist_id_by_mbid=artist_id_by_mbid,
        )
        summary_rows = _materialize_summary_frame(
            track_rows,
            candidate_rows,
            require_corroboration_for_title_only=require_corroboration_for_title_only,
            require_exact_workid_or_unique_exact_title=require_exact_workid_or_unique_exact_title,
        )
        guardrail_blocked_count = 0
        route_guardrail_blocked_count = 0
        pre_guardrail_apply_count = 0
        if "_pre_guardrail_should_apply" in summary_rows.columns:
            pre_guardrail_apply_count = int(
                summary_rows.filter(pl.col("_pre_guardrail_should_apply") == 1).height
            )
        if "_guardrail_block_apply" in summary_rows.columns:
            guardrail_blocked_count = int(
                summary_rows.filter(
                    (pl.col("_pre_guardrail_should_apply") == 1)
                    & (pl.col("_guardrail_block_apply"))
                ).height
            )
        if "_route_block_apply" in summary_rows.columns:
            route_guardrail_blocked_count = int(
                summary_rows.filter(
                    (pl.col("_pre_guardrail_should_apply") == 1)
                    & (pl.col("_route_block_apply"))
                ).height
            )
        _write_candidate_rows(conn, summary_rows)
        log.info("Wrote %d candidate rows to %s", summary_rows.height, CANDIDATE_TABLE)
        log.info(
            "Auto-apply eligibility before guardrails=%d, blocked by corroboration guardrail=%d, blocked by route guardrail=%d",
            pre_guardrail_apply_count,
            guardrail_blocked_count,
            route_guardrail_blocked_count,
        )

        persisted_rows, vetted_rows = _upsert_user_vetted_works(
            master_write_conn,
            summary_rows,
            lookup_subset,
        )
        log.info(
            "Upserted %d rows into %s (%d with vetted=1)",
            persisted_rows,
            USER_VETTED_WORKS_TABLE,
            vetted_rows,
        )
        updates = _apply_high_confidence_matches(conn, summary_rows)
        log.info("Applied %d high-confidence work links", updates)
    finally:
        conn.close()
        lookup_conn.close()
        master_write_conn.close()


if __name__ == "__main__":
    main()
