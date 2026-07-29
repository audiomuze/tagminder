"""
Purpose:
    Populate and/or normalize contributor-related MusicBrainz ID tags in `alib`
    based on a disambiguated reference map.

    The script builds a work table of candidate updates, applies changes,
    supports interactive namesake disambiguation (including existing-MBID
    confirmation and explicit skip), writes deterministic synthetic UUIDv5
    IDs when needed, and persists user decisions for reuse.

    Synthetic reference rows are written with contributor display names
    formatted via shared contributor-case helpers while retaining normalized
    lookup keys.

    increments `__sqlmodded`, and logs per-field modifications to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - contributors_unified_disambiguated
    - contributors_unified_namesakes
    - _USR_disambiguation_decisions
    - DEBUG_mbid_updates (only when --debug is passed)
    - changelog
    - sqlite_master (introspection)

Author: audiomuze
Last updated: 2026-07-05
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import unicodedata
import uuid
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

try:
    import readline
except ImportError:  # pragma: no cover
    readline = None

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_contributor_case
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_run

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _configure_logging() -> None:
    cfg = tm_config.load_config()
    logging_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
    level_name = (
        logging_cfg.get("level", "INFO") if isinstance(logging_cfg, dict) else "INFO"
    )
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(level=level, format=_LOG_FORMAT, force=True)


_configure_logging()

# Multi-value delimiter used by Tagminder (written to SQLite as two literal backslashes).
# Source of truth: tagminder.toml [strings].multivalue_delimiter
DELIMITER = tm_config.get_multivalue_delimiter()
SYNTHETIC_MBID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "tagminder.synthetic.mbid.v1")
MBID_FIELD_MAP = {
    "artist": "musicbrainz_artistid",
    "albumartist": "musicbrainz_albumartistid",
    "composer": "musicbrainz_composerid",
    "engineer": "musicbrainz_engineerid",
    "producer": "musicbrainz_producerid",
}
MBID_ID_COLUMNS = list(MBID_FIELD_MAP.values())
FIELD_DISAMBIGUATION_HINTS = {
    "engineer": ("engineer",),
    "producer": ("producer",),
    "composer": ("composer", "writer", "songwriter"),
}
USER_DISAMBIGUATION_TABLE = "_USR_disambiguation_decisions"
DEBUG_UPDATES_TABLE = "DEBUG_mbid_updates"
SKIP_DISAMBIGUATION_SENTINEL = "__SKIP_DISAMBIGUATION__"
DISAMBIGUATED_TABLE = "contributors_unified_disambiguated"
NAMESAKES_TABLE = "contributors_unified_namesakes"
DECISION_SOURCE_USER = "user"
DECISION_SOURCE_AUTOMATED_NO_NAME_MATCH = "automated_no_name_match"


class UserAbortedDisambiguation(Exception):
    """Raised when a user aborts interactive MBID population."""

#         .str.replace_all(r"^\s+|\s+$", "")
#         .str.replace('"', '')
#         .map_elements(
#             lambda x: unicodedata.normalize('NFKD', x) if x else None,
#             return_dtype=pl.Utf8
#         )
#     )


# def normalize_text_series(series: pl.Series) -> pl.Series:
#     """Vectorized normalization with type guards"""
#     if series.dtype != pl.Utf8:
#         raise TypeError(f"Expected pl.Utf8, got {series.dtype}")

#     return (
#         series.str.to_lowercase()
#         .str.strip()
#         .str.replace_all(r"\s+", " ")  # Normalize internal whitespace
#         .map_elements(
#             lambda x: unicodedata.normalize('NFKD', x) if x else None,
#             return_dtype=pl.Utf8  # Preserve type
#         )
#     )


def normalize_string(text: str) -> str:
    """
    A single, consistent normalization function for all entity names.
    Handles case, whitespace, quotes, and diacritics.
    """
    if not isinstance(text, str):
        return ""
    # 1. Normalize unicode characters (diacritics)
    text = unicodedata.normalize("NFKD", text)
    # 2. Convert to lowercase
    text = text.lower()
    # 3. Remove double quotes
    text = text.replace('"', "")
    # 4. Normalize all whitespace (leading, trailing, and internal)
    text = " ".join(text.split())
    return text


def resolve_mbid_or_synthetic(
    contributor_name: str,
    contributors_dict: Dict[str, str],
    stats: Dict[str, Any] | None = None,
    field: str | None = None,
) -> str:
    """
    Resolve contributor to a real MBID when known, otherwise emit deterministic
    synthetic UUIDv5 for non-empty normalized names.
    """
    norm_name = normalize_string(contributor_name)
    if not norm_name:
        return ""

    mbid = contributors_dict.get(norm_name)
    if mbid:
        return mbid

    synthetic_mbid = str(uuid.uuid5(SYNTHETIC_MBID_NAMESPACE, norm_name))

    if stats is not None:
        stats["synthetic_fallback_resolutions_total"] = (
            int(stats.get("synthetic_fallback_resolutions_total", 0)) + 1
        )
        if field:
            by_field = stats.get("synthetic_fallback_resolutions")
            if by_field is None:
                by_field = defaultdict(int)
                stats["synthetic_fallback_resolutions"] = by_field
            by_field[field] = int(by_field.get(field, 0)) + 1

    return synthetic_mbid


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    """Return True when a table contains the specified column."""
    rows = conn.execute(f"PRAGMA table_info({tm_db.quote_ident(table_name)})").fetchall()
    return any(str(row[1]) == column_name for row in rows)


def _synthetic_disambiguation_text(
    albumartist_value: str | None,
    album_value: str | None,
) -> str:
    albumartist = (albumartist_value or "").strip() or "(unknown albumartist)"
    album = (album_value or "").strip() or "(unknown album)"
    return f"synthetic: albumartist={albumartist}; album={album}"


def persist_synthetic_reference_rows(
    conn: sqlite3.Connection,
    synthetic_rows: Dict[str, Dict[str, str]],
) -> int:
    if not synthetic_rows:
        return 0

    existing_mbids = {
        str(row[0])
        for row in conn.execute(
            f"SELECT merge_key_mbid FROM {DISAMBIGUATED_TABLE} WHERE merge_key_mbid IS NOT NULL"
        ).fetchall()
        if row and row[0]
    }

    rows_to_insert = []
    for mbid, payload in synthetic_rows.items():
        if not mbid or mbid in existing_mbids:
            continue
        contributor = payload.get("contributor") or ""
        lpreferred_artist_name = payload.get("lpreferred__artist_name") or normalize_string(contributor)
        disambiguation = payload.get("disambiguation") or "synthetic"
        rows_to_insert.append((mbid, contributor, lpreferred_artist_name, disambiguation))

    if not rows_to_insert:
        return 0

    conn.executemany(
        """
        INSERT INTO contributors_unified_disambiguated
        (merge_key_mbid, preferred__artist_name, lpreferred__artist_name, musicbrainz_disambiguation)
        VALUES (?, ?, ?, ?)
        """,
        rows_to_insert,
    )
    return len(rows_to_insert)


def ensure_disambiguation_decisions_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {USER_DISAMBIGUATION_TABLE} (
            contributor_name TEXT NOT NULL,
            albumartist_context TEXT NOT NULL,
            assigned_mbid TEXT NOT NULL,
            decision_source TEXT NOT NULL DEFAULT '{DECISION_SOURCE_USER}',
            created_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (contributor_name, albumartist_context)
        )
        """
    )

    # Backward-compatible migration for existing tables created before decision_source.
    if not _table_has_column(conn, USER_DISAMBIGUATION_TABLE, "decision_source"):
        conn.execute(
            f"""
            ALTER TABLE {USER_DISAMBIGUATION_TABLE}
            ADD COLUMN decision_source TEXT NOT NULL DEFAULT '{DECISION_SOURCE_USER}'
            """
        )


def load_user_disambiguation_decisions(conn: sqlite3.Connection) -> Dict[tuple[str, str], str]:
    if not _table_exists(conn, USER_DISAMBIGUATION_TABLE):
        return {}

    rows = conn.execute(
        f"SELECT contributor_name, albumartist_context, assigned_mbid FROM {USER_DISAMBIGUATION_TABLE}"
    ).fetchall()
    return {
        (normalize_string(name), normalize_string(context)): mbid
        for name, context, mbid in rows
        if isinstance(name, str)
        and isinstance(context, str)
        and isinstance(mbid, str)
        and name.strip()
        and context.strip()
        and mbid.strip()
    }


def persist_user_disambiguation_decisions(
    conn: sqlite3.Connection,
    decisions: Dict[tuple[str, str], str],
    source: str = DECISION_SOURCE_USER,
) -> None:
    if not decisions:
        return

    ensure_disambiguation_decisions_table(conn)
    now = tm_db.utc_now_iso()
    rows = [
        (name, context, mbid, source, now, now)
        for (name, context), mbid in decisions.items()
        if name and context and mbid and mbid != SKIP_DISAMBIGUATION_SENTINEL
    ]
    if not rows:
        return

    conn.executemany(
        f"""
        INSERT INTO {USER_DISAMBIGUATION_TABLE}
        (contributor_name, albumartist_context, assigned_mbid, decision_source, created_utc, updated_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(contributor_name, albumartist_context)
        DO UPDATE SET
            assigned_mbid=excluded.assigned_mbid,
            decision_source=excluded.decision_source,
            updated_utc=excluded.updated_utc
        """,
        rows,
    )


def load_namesakes_lookup(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, str]]]:
    if not _table_exists(conn, NAMESAKES_TABLE):
        return {}

    df_namesakes = pl.read_database(
        """
        SELECT
            merge_key_mbid AS mbid,
            preferred__artist_name AS contributor,
            musicbrainz_disambiguation AS disambiguation
        FROM contributors_unified_namesakes
        """,
        conn,
        schema_overrides={"mbid": pl.Utf8, "contributor": pl.Utf8, "disambiguation": pl.Utf8},
    ).with_columns(
        pl.col("contributor").map_elements(normalize_string, return_dtype=pl.Utf8).alias("norm_entity")
    )

    lookup: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in df_namesakes.iter_rows(named=True):
        norm_entity = row.get("norm_entity") or ""
        mbid = row.get("mbid") or ""
        if not norm_entity or not mbid:
            continue
        lookup[norm_entity].append(
            {
                "mbid": mbid,
                "contributor": row.get("contributor") or "",
                "disambiguation": row.get("disambiguation") or "",
            }
        )

    for key in list(lookup.keys()):
        # Keep output deterministic in the interactive TUI.
        lookup[key] = sorted(lookup[key], key=lambda r: (r["contributor"], r["mbid"]))

    return dict(lookup)


def _normalized_tokens(value: str | None) -> List[str]:
    if value is None:
        return []
    tokens = [normalize_string(token) for token in str(value).split(DELIMITER)]
    return [token for token in tokens if token]


def _display_tokens(value: str | None) -> List[str]:
    if value is None:
        return []
    tokens = [str(token).strip() for token in str(value).split(DELIMITER)]
    return [token for token in tokens if token]


def _disambiguation_context(albumartist_value: str | None, album_value: str | None) -> str:
    norm_albumartist = normalize_string(albumartist_value or "")
    if norm_albumartist:
        return norm_albumartist

    norm_album = normalize_string(album_value or "")
    if norm_album:
        return f"__album__:{norm_album}"

    return "__album__:__unknown__"


def _synthetic_mbid_from_context(norm_name: str, context: str) -> str:
    payload = f"v2|{norm_name}|{context}"
    return str(uuid.uuid5(SYNTHETIC_MBID_NAMESPACE, payload))


def _normalize_menu_choice(raw: str) -> str:
    """Normalize interactive menu input like '[1]', '1)', '(a)', ' c. ' to canonical form."""
    choice = (raw or "").strip().lower()
    if not choice:
        return ""

    # Accept wrapped forms such as [1], (2), [c], (a).
    if len(choice) >= 2 and ((choice[0], choice[-1]) in {("[", "]"), ("(", ")")}):
        choice = choice[1:-1].strip()

    # Accept trailing punctuation forms like "1)", "a)", "c.".
    choice = choice.lstrip("[(").rstrip(")].:;")
    return choice


def _option_token(value: str) -> str:
    """Render a selectable option token as a bracketed label."""
    return f"[{value}]"


MB_ARTIST_URL = "https://musicbrainz.org/artist/{mbid}"
MB_RELEASE_URL = "https://musicbrainz.org/release/{mbid}"
MB_RELEASEGROUP_URL = "https://musicbrainz.org/release-group/{mbid}"


def _mbid_link(mbid: str) -> str:
    """Return an explicit MusicBrainz artist URL (always visible/clickable)."""
    if not mbid:
        return mbid
    url = MB_ARTIST_URL.format(mbid=mbid)
    return f"{mbid} ({url})"


def _release_link(releaseid: str) -> str:
    """Return an explicit MusicBrainz release URL (always visible/clickable)."""
    if not releaseid:
        return releaseid
    url = MB_RELEASE_URL.format(mbid=releaseid)
    return f"{releaseid} ({url})"


def _releasegroup_link(releasegroupid: str) -> str:
    """Return an explicit MusicBrainz release-group URL (always visible/clickable)."""
    if not releasegroupid:
        return releasegroupid
    url = MB_RELEASEGROUP_URL.format(mbid=releasegroupid)
    return f"{releasegroupid} ({url})"


def _input_with_prefill(prompt: str, prefill: str) -> str:
    """Read input with an optional prefilled buffer when readline is available."""
    if not prefill:
        return input(prompt)
    if readline is None:
        return input(prompt)

    rl = readline
    rl.set_startup_hook(lambda: rl.insert_text(prefill))
    try:
        return input(prompt)
    finally:
        rl.set_startup_hook(None)


def _prompt_namesake_choice(case: Dict[str, Any]) -> str:
    print("\n" + "=" * 88)
    fields_label = ""
    if case.get("fields"):
        fields_label = " [" + ", ".join(sorted(case["fields"])) + "]"
    print(f"Contributor: {case['name_display']}{fields_label}")
    print(f"Album Artist: {case['albumartist']}")
    track_artists = sorted(case.get("track_artists", set()))
    if track_artists:
        print(f"Track Artist: {track_artists[0]}")
        for extra_track_artist in track_artists[1:]:
            print(f"              {extra_track_artist}")
    else:
        print("Track Artist: (unknown)")
    print(f"Album: {case['album']}")
    album_mbids = sorted(case.get("album_mbids", set()))
    if album_mbids:
        print(f"Album MBID: {_release_link(album_mbids[0])}")
        for extra_album_mbid in album_mbids[1:]:
            print(f"            {_release_link(extra_album_mbid)}")
    releasegroup_mbids = sorted(case.get("releasegroup_mbids", set()))
    if releasegroup_mbids:
        print(f"Release Group MBID: {_releasegroup_link(releasegroup_mbids[0])}")
        for extra_releasegroup_mbid in releasegroup_mbids[1:]:
            print(f"                    {_releasegroup_link(extra_releasegroup_mbid)}")
    print(f"Genre: {case['genre']}")
    dirpaths = sorted(case.get("dirpaths", set()))
    if dirpaths:
        print(f"Dirpath: {dirpaths[0]}")
        for extra in dirpaths[1:]:
            print(f"         {extra}")
    else:
        print("Dirpath: (unknown)")
    paths = sorted(case.get("paths", set()))
    if paths:
        print(f"__path: {paths[0]}")
        for extra in paths[1:]:
            print(f"        {extra}")
    else:
        print("__path: (unknown)")
    print("Disambiguation candidates:")

    candidates = case["candidates"]
    case_fields = set(case.get("fields") or ())
    albumartist_norm = normalize_string(case.get("albumartist") or "")
    existing_mbid_default = str(case.get("existing_mbid_default") or "").strip()
    existing_default_idx: int | None = None
    albumartist_default_idx: int | None = None
    field_hint_default_idx: int | None = None

    for idx, candidate in enumerate(candidates, start=1):
        candidate_name_norm = normalize_string(candidate.get("contributor") or "")
        disambig = candidate.get("disambiguation") or "(no disambiguation)"
        disambig_norm = normalize_string(disambig)
        is_existing_mbid_match = bool(
            existing_mbid_default
            and candidate.get("mbid") == existing_mbid_default
            and candidate_name_norm == case["norm_name"]
        )
        is_albumartist_match = bool(
            albumartist_norm and disambig_norm and albumartist_norm in disambig_norm
        )

        role_hint_terms: set[str] = set()
        for field_name in case_fields:
            role_hint_terms.update(FIELD_DISAMBIGUATION_HINTS.get(field_name, ()))
        is_field_hint_match = bool(
            role_hint_terms and any(term in disambig_norm for term in role_hint_terms)
        )

        if is_existing_mbid_match and existing_default_idx is None:
            existing_default_idx = idx
        if is_albumartist_match and albumartist_default_idx is None:
            albumartist_default_idx = idx
        if is_field_hint_match and field_hint_default_idx is None:
            field_hint_default_idx = idx

        default_idx = (
            existing_default_idx
            if existing_default_idx is not None
            else (
                albumartist_default_idx
                if albumartist_default_idx is not None
                else field_hint_default_idx
            )
        )

        marker = "* " if default_idx == idx else "  "
        hint_parts = []
        if is_existing_mbid_match:
            hint_parts.append("existing mbid match")
        if is_albumartist_match:
            hint_parts.append("albumartist match")
        if is_field_hint_match:
            hint_parts.append("role hint match")
        hint = f" [{' / '.join(hint_parts)}]" if hint_parts else ""
        print(
            f"{marker}{_option_token(str(idx))} {candidate.get('contributor') or case['name_display']}"
            f" | {disambig}{hint} | {_mbid_link(candidate['mbid'])}"
        )

    print()
    print(f"  {_option_token('c')} Create synthetic ID {_mbid_link(case['synthetic_preview'])}")
    print(f"  {_option_token('s')} Skip disambiguation for this case (leave unresolved)")
    print(f"  {_option_token('a')} Abort populating MBIDs")

    default_idx = (
        existing_default_idx
        if existing_default_idx is not None
        else (
            albumartist_default_idx
            if albumartist_default_idx is not None
            else field_hint_default_idx
        )
    )

    if default_idx is not None:
        if existing_default_idx is not None:
            print(
                f"\nDefault selection prefilled from existing track MBID: {_option_token(str(default_idx))}"
            )
            existing_default_mbid = candidates[existing_default_idx - 1]["mbid"]
            while True:
                print(
                    "Confirm this existing MBID for this disambiguation case? [y]es/[n]o/[s]kip/[a]bort:",
                    flush=True,
                )
                confirm_existing = _normalize_menu_choice(input("> "))
                if confirm_existing in {"", "y"}:
                    return existing_default_mbid
                if confirm_existing == "n":
                    break
                if confirm_existing == "s":
                    return SKIP_DISAMBIGUATION_SENTINEL
                if confirm_existing == "a":
                    raise UserAbortedDisambiguation("User aborted MBID population")
                print("Invalid response. Enter 'y', 'n', 's', or 'a'.")
        else:
            if albumartist_default_idx is not None:
                print(
                    f"\nDefault selection prefilled from albumartist match: {_option_token(str(default_idx))}"
                )
            else:
                print(
                    f"\nDefault selection prefilled from role hint match: {_option_token(str(default_idx))}"
                )

    while True:
        print("Select option (1/2/.../c/s/a):", flush=True)
        if default_idx is not None:
            raw_choice = _input_with_prefill(
                "> ",
                str(default_idx),
            )
            choice = _normalize_menu_choice(raw_choice) or str(default_idx)
        else:
            choice = _normalize_menu_choice(input("> "))
        if choice == "a":
            raise UserAbortedDisambiguation("User aborted MBID population")
        if choice == "c":
            return case["synthetic_preview"]
        if choice == "s":
            return SKIP_DISAMBIGUATION_SENTINEL
        if choice.isdigit():
            index = int(choice)
            if 1 <= index <= len(candidates):
                return candidates[index - 1]["mbid"]
        print(
            "Invalid selection. Choose a listed number, "
            f"{_option_token('c')} for synthetic, {_option_token('s')} to skip, or {_option_token('a')} to abort."
        )


def interactive_resolve_namesakes(cases: List[Dict[str, Any]]) -> Dict[tuple[str, str], str]:
    if not cases:
        return {}

    print("\nNamesake disambiguation is required before MBID updates can be written.")
    print("You can choose a candidate MBID per case, or create a synthetic ID.")

    decisions: Dict[tuple[str, str], str] = {}
    for case in cases:
        key = (case["norm_name"], case["context"]) 
        decisions[key] = _prompt_namesake_choice(case)

    while True:
        print("\nSummary of selections:")
        for idx, case in enumerate(cases, start=1):
            key = (case["norm_name"], case["context"])
            selected = decisions.get(key, "")
            selected_display = "(skipped)" if selected == SKIP_DISAMBIGUATION_SENTINEL else _mbid_link(selected)
            fields_label = ""
            if case.get("fields"):
                fields_label = " [" + ", ".join(sorted(case["fields"])) + "]"
            print(
                f"  {idx}) {case['name_display']}{fields_label} | {case['albumartist']} / {case['album']} -> {selected_display}"
            )

        print("Confirm all selections? [y]es/[n]o/[a]bort:", flush=True)
        confirm = _normalize_menu_choice(input("> "))
        if confirm == "y":
            return decisions
        if confirm == "a":
            raise UserAbortedDisambiguation("User aborted MBID population before confirmation")
        if confirm == "n":
            print("Enter item number to revise:", flush=True)
            item = _normalize_menu_choice(input("> "))
            if not item.isdigit():
                print("Please enter a numeric item index.")
                continue
            index = int(item)
            if not (1 <= index <= len(cases)):
                print("Item index out of range.")
                continue
            case = cases[index - 1]
            key = (case["norm_name"], case["context"])
            decisions[key] = _prompt_namesake_choice(case)
            continue
        print("Invalid response. Enter 'y', 'n', or 'a'.")


def _collect_pending_namesake_cases(
    df: pl.DataFrame,
    fields: Dict[str, str],
    contributors_dict: Dict[str, str],
    namesakes_lookup: Dict[str, List[Dict[str, str]]],
    existing_decisions: Dict[tuple[str, str], str],
) -> List[Dict[str, Any]]:
    pending: Dict[tuple[str, str], Dict[str, Any]] = {}

    for row in df.iter_rows(named=True):
        context = _disambiguation_context(row.get("albumartist"), row.get("album"))
        context_display = row.get("albumartist") or row.get("album") or "(unknown)"

        for field, mbid_field in fields.items():
            value = row.get(field)
            if value is None:
                continue

            norm_names = _normalized_tokens(value)
            display_names = _display_tokens(value)
            existing_mbid_tokens = [
                token.strip()
                for token in str(row.get(mbid_field) or "").split(DELIMITER)
                if token is not None
            ]

            for idx, norm_name in enumerate(norm_names):
                display_name = display_names[idx] if idx < len(display_names) else norm_name
                if norm_name in contributors_dict:
                    continue
                key = (norm_name, context)
                if key in existing_decisions:
                    continue

                candidates = namesakes_lookup.get(norm_name)
                if not candidates:
                    continue

                if key not in pending:
                    pending[key] = {
                        "norm_name": norm_name,
                        "context": context,
                        "context_display": context_display,
                        "name_display": display_name,
                        "albumartist": row.get("albumartist") or "(unknown)",
                        "track_artists": set(),
                        "album": row.get("album") or "(unknown)",
                        "genre": row.get("genre") or "(unknown genre)",
                        "dirpaths": set(),
                        "paths": set(),
                        "fields": set(),
                        "albums": set(),
                        "album_mbids": set(),
                        "releasegroup_mbids": set(),
                        "candidates": candidates,
                        "synthetic_preview": _synthetic_mbid_from_context(norm_name, context),
                        "existing_mbid_default": "",
                        "existing_mbid_conflict": False,
                    }

                current_name_display = str(pending[key].get("name_display") or "").strip()
                if (
                    display_name
                    and (not current_name_display or normalize_string(current_name_display) == norm_name)
                ):
                    pending[key]["name_display"] = display_name

                pending[key]["fields"].add(field)
                track_artist = row.get("artist")
                if track_artist:
                    pending[key]["track_artists"].add(str(track_artist))
                dirpath = row.get("__dirpath")
                if dirpath:
                    pending[key]["dirpaths"].add(str(dirpath))
                path = row.get("__path")
                if path:
                    pending[key]["paths"].add(str(path))
                album = row.get("album")
                if album:
                    pending[key]["albums"].add(str(album))
                album_mbid = str(row.get("musicbrainz_albumid") or "").strip()
                if album_mbid:
                    pending[key]["album_mbids"].add(album_mbid)
                releasegroup_mbid = str(row.get("musicbrainz_releasegroupid") or "").strip()
                if releasegroup_mbid:
                    pending[key]["releasegroup_mbids"].add(releasegroup_mbid)

                existing_mbid = existing_mbid_tokens[idx] if idx < len(existing_mbid_tokens) else ""
                if not existing_mbid:
                    continue

                is_valid_existing_mbid = any(
                    candidate.get("mbid") == existing_mbid
                    and normalize_string(candidate.get("contributor") or "") == norm_name
                    for candidate in candidates
                )
                if not is_valid_existing_mbid:
                    continue

                if pending[key].get("existing_mbid_conflict"):
                    continue

                current_default = str(pending[key].get("existing_mbid_default") or "").strip()
                if not current_default:
                    pending[key]["existing_mbid_default"] = existing_mbid
                    continue

                if current_default != existing_mbid:
                    pending[key]["existing_mbid_default"] = ""
                    pending[key]["existing_mbid_conflict"] = True

    cases = list(pending.values())
    cases.sort(key=lambda case: (case["name_display"], case["context_display"]))
    return cases


def _resolve_with_context(
    norm_name: str,
    context: str,
    contributors_dict: Dict[str, str],
    decision_lookup: Dict[tuple[str, str], str],
    stats: Dict[str, Any] | None = None,
    field: str | None = None,
) -> str:
    if not norm_name:
        return ""

    mbid = contributors_dict.get(norm_name)
    if mbid:
        return mbid

    decision = decision_lookup.get((norm_name, context))
    if decision:
        return decision

    synthetic_mbid = _synthetic_mbid_from_context(norm_name, context)

    if stats is not None:
        stats["synthetic_fallback_resolutions_total"] = (
            int(stats.get("synthetic_fallback_resolutions_total", 0)) + 1
        )
        if field:
            by_field = stats.get("synthetic_fallback_resolutions")
            if by_field is None:
                by_field = defaultdict(int)
                stats["synthetic_fallback_resolutions"] = by_field
            by_field[field] = int(by_field.get(field, 0)) + 1

    return synthetic_mbid


def load_dataframes(
    conn: sqlite3.Connection,
    *,
    alib_conn: sqlite3.Connection | None = None,
) -> Tuple[Dict[str, str], set[str], int]:
    """
    Load and normalize MusicBrainz reference data with strict typing
    Modified to use the centralized normalize_entity_series function
    """
    logging.info("Loading contributors dictionary with vectorized normalization...")

    # 1. Load reference data with strict UTF-8 typing
    df_contributors = pl.read_database(
        """
        SELECT
            preferred__artist_name AS contributor,
            merge_key_mbid AS mbid
        FROM contributors_unified_disambiguated
        """,
        conn,
        schema_overrides={"contributor": pl.Utf8, "mbid": pl.Utf8},
    )

    # 2. Apply consistent vectorized normalization
    df_contributors = df_contributors.with_columns(
        pl.col("contributor")
        .map_elements(normalize_string, return_dtype=pl.Utf8)
        .alias("norm_entity")
    )
    # 3. Create optimized lookup dictionary
    contributors_dict = dict(
        zip(df_contributors["norm_entity"].to_list(), df_contributors["mbid"].to_list())
    )
    logging.info(f"Loaded {len(contributors_dict)} normalized contributors")

    # 4. Get total rows count
    source_conn = alib_conn or conn
    total_rows = source_conn.execute("SELECT COUNT(*) FROM alib").fetchone()[0]
    logging.info(f"Total alib rows: {total_rows}")

    ref_mbid_set = {
        mbid
        for mbid in contributors_dict.values()
        if isinstance(mbid, str) and mbid.strip()
    }

    return contributors_dict, ref_mbid_set, total_rows


def _is_likely_synthetic_mbid(mbid: str, ref_mbid_set: set[str]) -> bool:
    """Heuristic synthetic marker used by this script's reporting."""
    return (
        isinstance(mbid, str)
        and len(mbid) == 36
        and mbid[14] == "5"
        and mbid not in ref_mbid_set
    )


def _count_synthetic_tokens(value: str | None, ref_mbid_set: set[str]) -> int:
    """Count synthetic MBID tokens in a delimiter-separated MBID string."""
    if not value:
        return 0
    return sum(
        1
        for token in str(value).split(DELIMITER)
        if _is_likely_synthetic_mbid(token.strip(), ref_mbid_set)
    )


def _synthetic_tokens_in_value(value: str | None, ref_mbid_set: set[str]) -> set[str]:
    """Return unique synthetic MBID tokens in a delimiter-separated MBID string."""
    if not value:
        return set()

    return {
        token.strip()
        for token in str(value).split(DELIMITER)
        if _is_likely_synthetic_mbid(token.strip(), ref_mbid_set)
    }


def _synthetic_token_counter(value: str | None, ref_mbid_set: set[str]) -> Counter[str]:
    """Return per-token counts for synthetic MBIDs in a delimiter-separated value."""
    if not value:
        return Counter()
    return Counter(
        token.strip()
        for token in str(value).split(DELIMITER)
        if _is_likely_synthetic_mbid(token.strip(), ref_mbid_set)
    )


def _counter_positive_subtract(left: Counter[str], right: Counter[str]) -> Counter[str]:
    """Return positive per-token delta for counters (left - right, clipped at zero)."""
    result: Counter[str] = Counter()
    for token, count in left.items():
        delta = int(count) - int(right.get(token, 0))
        if delta > 0:
            result[token] = delta
    return result


def _counter_intersection(left: Counter[str], right: Counter[str]) -> Counter[str]:
    """Return per-token overlap counts for two counters (multiset intersection)."""
    result: Counter[str] = Counter()
    for token, count in left.items():
        overlap = min(int(count), int(right.get(token, 0)))
        if overlap > 0:
            result[token] = overlap
    return result


def collect_orphan_mbid_clear_updates(
    conn: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Build updates that clear MBIDs when the paired contributor field is empty."""
    updates_by_rowid: dict[int, dict[str, Any]] = {}
    cleared_by_field: defaultdict[str, int] = defaultdict(int)

    for field, mbid_field in MBID_FIELD_MAP.items():
        rows = conn.execute(
            f"""
            SELECT rowid, {mbid_field}, COALESCE(__sqlmodded, 0)
            FROM alib
            WHERE ({field} IS NULL OR TRIM({field}) = '' OR TRIM({field}) = '""')
              AND ({mbid_field} IS NOT NULL AND TRIM({mbid_field}) != '' AND TRIM({mbid_field}) != '""')
            """
        ).fetchall()

        for rowid_raw, current_mbid, current_sqlmodded in rows:
            rowid = int(rowid_raw)
            update = updates_by_rowid.setdefault(
                rowid,
                {
                    "rowid": rowid,
                    "old_values": {},
                    "__sqlmodded": int(current_sqlmodded or 0),
                },
            )

            if mbid_field in update:
                continue

            update[mbid_field] = None
            update["old_values"][mbid_field] = current_mbid
            update["__sqlmodded"] = int(update.get("__sqlmodded") or 0) + 1
            cleared_by_field[field] += 1

    return list(updates_by_rowid.values()), dict(cleared_by_field)


def setup_changelog_table(conn: sqlite3.Connection):
    """
    Ensure changelog table exists for tracking changes

    Args:
        conn: SQLite database connection
    """
    tm_db.ensure_changelog_table(conn)
    logging.info("Changelog table ready")


def process_chunk(
    conn: sqlite3.Connection,
    contributors_dict: Dict[str, str],
    ref_mbid_set: set[str],
    decision_lookup: Dict[tuple[str, str], str],
    synthetic_ref_rows: Dict[str, Dict[str, str]],
    new_synthetic_decisions: Dict[tuple[str, str], str],
    offset: int,
    chunk_size: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Process a chunk of records with vectorized MBID matching

    Args:
        conn: Database connection
        contributors_dict: Normalized {entity: mbid} mapping
        offset: Chunk starting position
        chunk_size: Number of rows to process

    Returns:
        Tuple of (updates, statistics)
    """
    logging.info(
        f"Processing chunk {offset}-{offset + chunk_size} with vectorized operations"
    )

    # 1. Define field mappings and schema
    fields = MBID_FIELD_MAP

    schema = {
        "rowid": pl.Int64,
        "artist": pl.Utf8,
        "albumartist": pl.Utf8,
        "album": pl.Utf8,
        "composer": pl.Utf8,
        "engineer": pl.Utf8,
        "producer": pl.Utf8,
        "musicbrainz_artistid": pl.Utf8,
        "musicbrainz_albumartistid": pl.Utf8,
        "musicbrainz_composerid": pl.Utf8,
        "musicbrainz_engineerid": pl.Utf8,
        "musicbrainz_producerid": pl.Utf8,
        "__sqlmodded": pl.Int16,
    }

    # 2. Load chunk with consistent vectorized normalization
    query = f"""
                SELECT rowid, artist, albumartist, album, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid,
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid,
             COALESCE(__sqlmodded, 0) AS __sqlmodded
        FROM alib
                WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR
                            composer IS NOT NULL OR engineer IS NOT NULL OR
                            producer IS NOT NULL)
        ORDER BY rowid
        LIMIT {chunk_size} OFFSET {offset}
    """

    df = pl.read_database(query, conn, schema_overrides=schema).with_columns(
        *[
            pl.col(field)
            .map_elements(normalize_string, return_dtype=pl.Utf8)
            .alias(f"norm_{field}")
            for field in fields.keys()
        ]
    )

    # 3. Context-aware MBID matching (mode-invariant with full processing)
    updates_by_rowid: dict[int, dict[str, Any]] = {}
    stats = {
        "additions": defaultdict(int),
        "corrections": defaultdict(int),
        "synthetic_fallback_resolutions": defaultdict(int),
        "synthetic_fallback_resolutions_total": 0,
        "synthetic_written": defaultdict(int),
        "synthetic_written_total": 0,
        "synthetic_generated_distinct_set": set(),
        "synthetic_rows_written_total": 0,
        "synthetic_decision_driven_total": 0,
        "synthetic_auto_fallback_total": 0,
        "synthetic_carried_forward_total": 0,
        "synthetic_other_introduced_total": 0,
    }

    for row in df.iter_rows(named=True):
        changes_in_row = 0
        row_has_synthetic = False

        for field, mbid_field in fields.items():
            value = row.get(field)
            if value is None:
                continue

            entities = _normalized_tokens(value)
            display_entities = _display_tokens(value)
            context = _disambiguation_context(row.get("albumartist"), row.get("album"))
            current_mbid_tokens = [
                token.strip()
                for token in str(row.get(mbid_field) or "").split(DELIMITER)
            ]

            matched_mbids: list[str] = []
            for entity_idx, entity in enumerate(entities):
                display_entity = (
                    display_entities[entity_idx]
                    if entity_idx < len(display_entities)
                    else entity
                )
                decision_for_entity = decision_lookup.get((entity, context))
                if decision_for_entity == SKIP_DISAMBIGUATION_SENTINEL:
                    existing_token = (
                        current_mbid_tokens[entity_idx]
                        if entity_idx < len(current_mbid_tokens)
                        else ""
                    )
                    matched_mbids.append(existing_token)
                    continue

                resolved_mbid = _resolve_with_context(
                    entity,
                    context,
                    contributors_dict,
                    decision_lookup,
                    stats=stats,
                    field=field,
                )
                matched_mbids.append(resolved_mbid)

                if (
                    resolved_mbid
                    and _is_likely_synthetic_mbid(resolved_mbid, ref_mbid_set)
                    and (entity, context) not in decision_lookup
                ):
                    decision_lookup[(entity, context)] = resolved_mbid
                    new_synthetic_decisions[(entity, context)] = resolved_mbid

                if _is_likely_synthetic_mbid(resolved_mbid, ref_mbid_set):
                    synthetic_ref_rows.setdefault(
                        resolved_mbid,
                        {
                            "contributor": tm_contributor_case.smart_title(display_entity) or display_entity,
                            "lpreferred__artist_name": entity,
                            "disambiguation": _synthetic_disambiguation_text(
                                row.get("albumartist"), row.get("album")
                            ),
                        },
                    )

            if not matched_mbids:
                new_value = None
            else:
                joined = DELIMITER.join(matched_mbids)
                new_value = None if joined.replace("\\", "") == "" else joined

            current_mbid = row.get(mbid_field)
            is_current_empty = current_mbid is None or (
                isinstance(current_mbid, str)
                and (current_mbid.strip() == "" or current_mbid.strip() == '""')
            )

            update_needed = False
            if is_current_empty and new_value:
                stats["additions"][field] += 1
                update_needed = True
                changes_in_row += 1
            elif not is_current_empty and new_value != str(current_mbid).strip():
                stats["corrections"][field] += 1
                update_needed = True
                changes_in_row += 1

            if update_needed:
                rowid = int(row["rowid"])
                new_counter = _synthetic_token_counter(new_value, ref_mbid_set)
                if new_counter:
                    row_has_synthetic = True
                old_counter = _synthetic_token_counter(current_mbid, ref_mbid_set)
                introduced_counter = _counter_positive_subtract(new_counter, old_counter)
                carried_counter = _counter_intersection(new_counter, old_counter)

                introduced_total = int(sum(int(c) for c in introduced_counter.values()))
                carried_total = int(sum(int(c) for c in carried_counter.values()))

                if introduced_total:
                    stats["synthetic_generated_distinct_set"].update(introduced_counter.keys())
                    stats["synthetic_written"][field] += introduced_total
                    stats["synthetic_written_total"] += introduced_total
                    stats["synthetic_auto_fallback_total"] += introduced_total

                if carried_total:
                    stats["synthetic_carried_forward_total"] += carried_total

                if rowid not in updates_by_rowid:
                    updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}
                updates_by_rowid[rowid][mbid_field] = new_value
                updates_by_rowid[rowid]["old_values"][mbid_field] = current_mbid

        if row_has_synthetic:
            stats["synthetic_rows_written_total"] += 1

        if changes_in_row > 0:
            rowid = int(row["rowid"])
            current_sqlmodded = int(row.get("__sqlmodded") or 0)
            new_sqlmodded = current_sqlmodded + changes_in_row

            if rowid not in updates_by_rowid:
                updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}

            updates_by_rowid[rowid]["__sqlmodded"] = new_sqlmodded if new_sqlmodded > 0 else None

    updates = list(updates_by_rowid.values())

    if updates:
        stats["synthetic_generated_distinct_total"] = len(
            stats["synthetic_generated_distinct_set"]
        )

    logging.info(f"Chunk complete: {len(updates)} updates, {stats}")
    return updates, stats


def write_updates_to_db(
    updates: List[Dict[str, Any]],
    conn: sqlite3.Connection,
    stats: Dict[str, Any],
    batch_size: int = 1000,
    debug: bool = False,
):
    """
    Write updates to both temporary table and main table using batching, with changelog logging

    Args:
        updates: List of update dictionaries
        conn: SQLite database connection
        stats: Statistics dictionary
        batch_size: Size of batches for processing
    """
    if not updates:
        logging.info("No updates to write to database")
        return

    logging.info(
        f"Writing {len(updates)} updates to database in batches of {batch_size}"
    )

    cursor = conn.cursor()
    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()
    tm_db.ensure_changelog_table(conn)

    path_by_rowid = tm_db.fetch_paths_by_rowid(
        conn, [int(u["rowid"]) for u in updates if u.get("rowid") is not None]
    )

    # Get column names - now includes __sqlmodded
    columns = [
        "rowid",
        "musicbrainz_artistid",
        "musicbrainz_albumartistid",
        "musicbrainz_composerid",
        "musicbrainz_engineerid",
        "musicbrainz_producerid",
        "__sqlmodded",
    ]

    # Check if a transaction is already active
    cursor.execute("SELECT * FROM sqlite_master LIMIT 0")
    transaction_active = conn.in_transaction

    # Only begin a transaction if one isn't already active
    if not transaction_active:
        conn.execute("BEGIN TRANSACTION")

    try:
        # Prepare statements
        insert_placeholders = ", ".join(["?" for _ in columns])
        insert_query = (
            f"INSERT INTO {tm_db.quote_ident(DEBUG_UPDATES_TABLE)} ({', '.join(columns)}) "
            f"VALUES ({insert_placeholders})"
        )

        if debug:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {tm_db.quote_ident(DEBUG_UPDATES_TABLE)} (
                    rowid INTEGER,
                    musicbrainz_artistid TEXT,
                    musicbrainz_albumartistid TEXT,
                    musicbrainz_composerid TEXT,
                    musicbrainz_engineerid TEXT,
                    musicbrainz_producerid TEXT,
                    __sqlmodded INTEGER
                )
                """
            )
            cursor.execute(f"DELETE FROM {tm_db.quote_ident(DEBUG_UPDATES_TABLE)}")

        updates_written = 0

        # Process in batches
        for i in range(0, len(updates), batch_size):
            batch = updates[i : i + batch_size]
            logging.info(
                f"Processing batch {i // batch_size + 1}: {len(batch)} updates"
            )

            if debug:
                for update in batch:
                    insert_data = [update.get("rowid")]
                    for col in columns[1:]:  # Skip rowid
                        insert_data.append(update.get(col))
                    cursor.execute(insert_query, insert_data)

            # Update main table and log changes - only update non-null values
            changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script_name)
            for update in batch:
                update_cols = []
                update_vals = []
                rowid = update["rowid"]
                alib_path = path_by_rowid.get(int(rowid), str(rowid))
                old_values = update.get("old_values", {})

                # Only include fields that exist in the update and are not None.
                # Special handling for __sqlmodded: when update value is None, set it to NULL explicitly.
                for col in columns[1:]:  # Skip rowid
                    if col not in update:
                        continue

                    if update[col] is not None:
                        update_cols.append(f"{col} = ?")
                        update_vals.append(update[col])
                        continue

                    update_cols.append(f"{col} = NULL")

                # Emit changelog entries for MBID fields that are actually being updated.
                mbid_fields = [
                    col
                    for col in columns[1:]
                    if col.startswith("musicbrainz_") and col in update
                ]

                def _norm(v: object) -> str | None:
                    if v is None:
                        return None
                    return v if isinstance(v, str) else str(v)

                changes: list[tuple[str, object, object]] = []
                for col in mbid_fields:
                    old_v = old_values.get(col)
                    new_v = update.get(col)
                    if _norm(old_v) == _norm(new_v):
                        continue
                    changes.append((col, old_v, new_v))

                if changes:
                    changelog.add(alib_path=alib_path, changes=changes)

                if update_cols:  # Only proceed if there are columns to update
                    update_query = (
                        f"UPDATE alib SET {', '.join(update_cols)} WHERE rowid = ?"
                    )
                    cursor.execute(update_query, update_vals + [rowid])
                    updates_written += 1

                    changelog.flush(cursor)

        logging.info(f"Successfully wrote {updates_written} updates to main table")

        # Only commit if we started the transaction
        if not transaction_active:
            conn.commit()
            logging.info("Transaction committed successfully")

    except Exception as e:
        logging.error(f"Error writing updates: {e}")
        # Rollback on error only if we started the transaction
        if not transaction_active:
            conn.rollback()
            logging.info("Transaction rolled back due to error")
        raise e


def process_database(
    conn: sqlite3.Connection,
    master_conn: sqlite3.Connection,
    chunk_size: int = 50000,
    debug: bool = False,
):
    """
    Process the entire database in chunks

    Args:
        conn: SQLite database connection
        chunk_size: Size of each chunk (increased for Polars)
    """
    logging.info(f"Starting chunked database processing with chunk size: {chunk_size}")

    # Get contributors dictionary and total rows once
    contributors_dict, ref_mbid_set, total_rows = load_dataframes(
        master_conn,
        alib_conn=conn,
    )
    namesakes_lookup = load_namesakes_lookup(master_conn)
    decision_lookup = load_user_disambiguation_decisions(master_conn)

    has_musicbrainz_albumid = _table_has_column(conn, "alib", "musicbrainz_albumid")
    has_musicbrainz_releasegroupid = _table_has_column(conn, "alib", "musicbrainz_releasegroupid")

    pending_schema = {
        "artist": pl.Utf8,
        "albumartist": pl.Utf8,
        "album": pl.Utf8,
        "genre": pl.Utf8,
        "__dirpath": pl.Utf8,
        "__path": pl.Utf8,
        "composer": pl.Utf8,
        "engineer": pl.Utf8,
        "producer": pl.Utf8,
        "musicbrainz_artistid": pl.Utf8,
        "musicbrainz_albumartistid": pl.Utf8,
        "musicbrainz_composerid": pl.Utf8,
        "musicbrainz_engineerid": pl.Utf8,
        "musicbrainz_producerid": pl.Utf8,
    }
    if has_musicbrainz_albumid:
        pending_schema["musicbrainz_albumid"] = pl.Utf8
    if has_musicbrainz_releasegroupid:
        pending_schema["musicbrainz_releasegroupid"] = pl.Utf8

    pending_fields = MBID_FIELD_MAP

    def _merge_pending_case(
        target: Dict[tuple[str, str], Dict[str, Any]],
        case: Dict[str, Any],
    ) -> None:
        key = (case["norm_name"], case["context"])
        if key not in target:
            target[key] = {
                **case,
                "track_artists": set(case.get("track_artists") or set()),
                "dirpaths": set(case.get("dirpaths") or set()),
                "paths": set(case.get("paths") or set()),
                "fields": set(case.get("fields") or set()),
                "albums": set(case.get("albums") or set()),
                "album_mbids": set(case.get("album_mbids") or set()),
                "releasegroup_mbids": set(case.get("releasegroup_mbids") or set()),
            }
            return

        existing = target[key]
        existing["track_artists"].update(case.get("track_artists") or set())
        existing["dirpaths"].update(case.get("dirpaths") or set())
        existing["paths"].update(case.get("paths") or set())
        existing["fields"].update(case.get("fields") or set())
        existing["albums"].update(case.get("albums") or set())
        existing["album_mbids"].update(case.get("album_mbids") or set())
        existing["releasegroup_mbids"].update(case.get("releasegroup_mbids") or set())

        current_name_display = str(existing.get("name_display") or "").strip()
        incoming_name_display = str(case.get("name_display") or "").strip()
        if (
            incoming_name_display
            and (not current_name_display or normalize_string(current_name_display) == case["norm_name"])
        ):
            existing["name_display"] = incoming_name_display

        if str(existing.get("context_display") or "").strip() in {"", "(unknown)"}:
            incoming_context_display = str(case.get("context_display") or "").strip()
            if incoming_context_display:
                existing["context_display"] = incoming_context_display

        if existing.get("existing_mbid_conflict") or case.get("existing_mbid_conflict"):
            existing["existing_mbid_conflict"] = True
            existing["existing_mbid_default"] = ""
        else:
            existing_default = str(existing.get("existing_mbid_default") or "").strip()
            incoming_default = str(case.get("existing_mbid_default") or "").strip()
            if not existing_default:
                existing["existing_mbid_default"] = incoming_default
            elif incoming_default and existing_default != incoming_default:
                existing["existing_mbid_conflict"] = True
                existing["existing_mbid_default"] = ""

    pending_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    if namesakes_lookup:
        albumid_select = ", musicbrainz_albumid" if has_musicbrainz_albumid else ""
        releasegroupid_select = ", musicbrainz_releasegroupid" if has_musicbrainz_releasegroupid else ""

        for offset in range(0, total_rows, chunk_size):
            pending_query = f"""
                SELECT artist, albumartist, album{albumid_select}{releasegroupid_select}, genre, __dirpath, __path,
                       composer, engineer, producer,
                       musicbrainz_artistid, musicbrainz_albumartistid,
                       musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid
                FROM alib
                WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR
                      composer IS NOT NULL OR engineer IS NOT NULL OR
                      producer IS NOT NULL)
                ORDER BY rowid
                LIMIT {chunk_size} OFFSET {offset}
            """
            pending_df = pl.read_database(
                pending_query,
                conn,
                schema_overrides=pending_schema,
            )
            if pending_df.is_empty():
                continue

            cases = _collect_pending_namesake_cases(
                pending_df,
                pending_fields,
                contributors_dict,
                namesakes_lookup,
                decision_lookup,
            )
            for case in cases:
                _merge_pending_case(pending_by_key, case)

    pending_namesake_cases = list(pending_by_key.values())
    pending_namesake_cases.sort(key=lambda case: (case["name_display"], case["context_display"]))

    if pending_namesake_cases:
        logging.info(
            f"Detected {len(pending_namesake_cases)} namesake disambiguation case(s) requiring user input"
        )
        interactive_decisions = interactive_resolve_namesakes(pending_namesake_cases)
        decision_lookup.update(interactive_decisions)
        with tm_db.transaction(master_conn):
            persist_user_disambiguation_decisions(
                master_conn,
                interactive_decisions,
                source=DECISION_SOURCE_USER,
            )
    else:
        logging.info("No namesakes were encountered in this run")

    # Setup changelog table
    setup_changelog_table(conn)

    # Initialize statistics
    all_stats = {
        "additions": {},
        "corrections": {},
        "synthetic_fallback_resolutions": {},
        "synthetic_fallback_resolutions_total": 0,
        "synthetic_written": {},
        "synthetic_written_total": 0,
        "synthetic_generated_distinct_set": set(),
        "synthetic_rows_written_total": 0,
        "synthetic_decision_driven_total": 0,
        "synthetic_auto_fallback_total": 0,
        "synthetic_carried_forward_total": 0,
        "synthetic_other_introduced_total": 0,
    }
    synthetic_ref_rows: Dict[str, Dict[str, str]] = {}
    new_synthetic_decisions: Dict[tuple[str, str], str] = {}

    try:
        with tm_db.transaction(conn):
            logging.info("Started database transaction")

            # Process in chunks and write updates for each chunk immediately
            chunks_processed = 0
            for offset in range(0, total_rows, chunk_size):
                chunks_processed += 1
                logging.info(
                    f"Processing chunk {chunks_processed}: rows {offset} to {offset + chunk_size}..."
                )

                # Process the chunk
                chunk_updates, chunk_stats = process_chunk(
                    conn,
                    contributors_dict,
                    ref_mbid_set,
                    decision_lookup,
                    synthetic_ref_rows,
                    new_synthetic_decisions,
                    offset,
                    chunk_size,
                )

                # Combine statistics
                for category in [
                    "additions",
                    "corrections",
                    "synthetic_fallback_resolutions",
                    "synthetic_written",
                ]:
                    for field, count in chunk_stats[category].items():
                        all_stats[category][field] = (
                            all_stats[category].get(field, 0) + count
                        )

                all_stats["synthetic_fallback_resolutions_total"] += int(
                    chunk_stats.get("synthetic_fallback_resolutions_total", 0)
                )
                all_stats["synthetic_written_total"] += int(
                    chunk_stats.get("synthetic_written_total", 0)
                )
                all_stats["synthetic_rows_written_total"] += int(
                    chunk_stats.get("synthetic_rows_written_total", 0)
                )
                all_stats["synthetic_decision_driven_total"] += int(
                    chunk_stats.get("synthetic_decision_driven_total", 0)
                )
                all_stats["synthetic_auto_fallback_total"] += int(
                    chunk_stats.get("synthetic_auto_fallback_total", 0)
                )
                all_stats["synthetic_carried_forward_total"] += int(
                    chunk_stats.get("synthetic_carried_forward_total", 0)
                )
                all_stats["synthetic_other_introduced_total"] += int(
                    chunk_stats.get("synthetic_other_introduced_total", 0)
                )
                all_stats["synthetic_generated_distinct_set"].update(
                    chunk_stats.get("synthetic_generated_distinct_set", set())
                )

                # Write updates for this chunk immediately
                if chunk_updates:
                    # Pass conn in transaction mode
                    write_updates_to_db(chunk_updates, conn, all_stats, debug=debug)
                else:
                    logging.info("No updates needed for this chunk")

            orphan_updates, orphan_clears_by_field = collect_orphan_mbid_clear_updates(conn)
            if orphan_updates:
                total_orphan_clears = sum(orphan_clears_by_field.values())
                logging.info(
                    "Clearing %d orphan MBID value(s) across %d row(s)",
                    total_orphan_clears,
                    len(orphan_updates),
                )
                for field, count in orphan_clears_by_field.items():
                    all_stats["corrections"][field] = (
                        all_stats["corrections"].get(field, 0) + int(count)
                    )
                write_updates_to_db(orphan_updates, conn, all_stats, debug=debug)
            else:
                logging.info("No orphan MBIDs found to clear")

            if synthetic_ref_rows:
                logging.info(
                    "Synthetic lookup staging disabled by policy; %d synthetic row candidate(s) not inserted into %s",
                    len(synthetic_ref_rows),
                    DISAMBIGUATED_TABLE,
                )

            if new_synthetic_decisions:
                with tm_db.transaction(master_conn):
                    persist_user_disambiguation_decisions(
                        master_conn,
                        new_synthetic_decisions,
                        source=DECISION_SOURCE_AUTOMATED_NO_NAME_MATCH,
                    )
                logging.info(
                    "Persisted %d automatic synthetic disambiguation decision(s)",
                    len(new_synthetic_decisions),
                )

        logging.info(
            f"All {chunks_processed} chunks processed successfully. Transaction committed."
        )

        all_stats["synthetic_generated_distinct_total"] = len(
            all_stats["synthetic_generated_distinct_set"]
        )
        all_stats["distinct_synthetic_in_alib"] = count_distinct_synthetic_mbids_in_alib(conn)

        # Display final statistics
        display_statistics(all_stats)

    except Exception as e:
        logging.error(f"Error during chunked processing: {e}")
        raise e


def process_full_database(
    conn: sqlite3.Connection,
    master_conn: sqlite3.Connection,
    debug: bool = False,
):
    """
    Process the entire database in one go with Polars

    Args:
        conn: SQLite database connection
    """
    logging.info("Starting full database processing (non-chunked)")

    # Get contributors dictionary
    contributors_dict, ref_mbid_set, _ = load_dataframes(
        master_conn,
        alib_conn=conn,
    )
    namesakes_lookup = load_namesakes_lookup(master_conn)
    decision_lookup = load_user_disambiguation_decisions(master_conn)

    # Define field mappings
    fields = {
        "artist": "musicbrainz_artistid",
        "albumartist": "musicbrainz_albumartistid",
        "composer": "musicbrainz_composerid",
        "engineer": "musicbrainz_engineerid",
        "producer": "musicbrainz_producerid",
    }

    # Initialize statistics
    all_stats = {
        "additions": {},
        "corrections": {},
        "synthetic_fallback_resolutions": {},
        "synthetic_fallback_resolutions_total": 0,
        "synthetic_written": {},
        "synthetic_written_total": 0,
        "synthetic_generated_distinct_set": set(),
        "synthetic_rows_written_total": 0,
        "synthetic_decision_driven_total": 0,
        "synthetic_auto_fallback_total": 0,
        "synthetic_carried_forward_total": 0,
        "synthetic_other_introduced_total": 0,
    }

    has_musicbrainz_albumid = _table_has_column(conn, "alib", "musicbrainz_albumid")
    has_musicbrainz_releasegroupid = _table_has_column(conn, "alib", "musicbrainz_releasegroupid")
    new_synthetic_decisions: Dict[tuple[str, str], str] = {}

    # Define schema with explicit types
    schema = {
        "rowid": pl.Int64,
        "artist": pl.Utf8,
        "albumartist": pl.Utf8,
        "album": pl.Utf8,
        "genre": pl.Utf8,
        "__dirpath": pl.Utf8,
        "__path": pl.Utf8,
        "composer": pl.Utf8,
        "engineer": pl.Utf8,
        "producer": pl.Utf8,
        "musicbrainz_artistid": pl.Utf8,
        "musicbrainz_albumartistid": pl.Utf8,
        "musicbrainz_composerid": pl.Utf8,
        "musicbrainz_engineerid": pl.Utf8,
        "musicbrainz_producerid": pl.Utf8,
        "__sqlmodded": pl.Int16,
    }
    if has_musicbrainz_albumid:
        schema["musicbrainz_albumid"] = pl.Utf8
    if has_musicbrainz_releasegroupid:
        schema["musicbrainz_releasegroupid"] = pl.Utf8

    # Get all relevant data at once - include __sqlmodded field
    albumid_select = ", musicbrainz_albumid" if has_musicbrainz_albumid else ""
    releasegroupid_select = ", musicbrainz_releasegroupid" if has_musicbrainz_releasegroupid else ""
    query = f"""
        SELECT rowid, artist, albumartist, album{albumid_select}{releasegroupid_select}, genre, __dirpath, __path, composer, engineer, producer,
               musicbrainz_artistid, musicbrainz_albumartistid,
               musicbrainz_composerid, musicbrainz_engineerid, musicbrainz_producerid,
               COALESCE(__sqlmodded, 0) AS __sqlmodded
        FROM alib
        WHERE (artist IS NOT NULL OR albumartist IS NOT NULL OR
              composer IS NOT NULL OR engineer IS NOT NULL OR
              producer IS NOT NULL)
    """

    try:
        logging.info("Loading entire database with Polars...")
        df = pl.read_database(query, conn, schema_overrides=schema)
        logging.info(f"Loaded {df.height} rows for processing")

        pending_namesake_cases = _collect_pending_namesake_cases(
            df,
            fields,
            contributors_dict,
            namesakes_lookup,
            decision_lookup,
        )
        synthetic_ref_rows: Dict[str, Dict[str, str]] = {}
        decision_driven_synthetic_mbids: set[str] = set()
        auto_fallback_synthetic_mbids: set[str] = set()
        if pending_namesake_cases:
            logging.info(
                f"Detected {len(pending_namesake_cases)} namesake disambiguation case(s) requiring user input"
            )
            interactive_decisions = interactive_resolve_namesakes(pending_namesake_cases)
            decision_lookup.update(interactive_decisions)

            for case in pending_namesake_cases:
                key = (case["norm_name"], case["context"])
                selected_mbid = interactive_decisions.get(key)
                if selected_mbid != case["synthetic_preview"]:
                    continue
                if not selected_mbid:
                    continue

                decision_driven_synthetic_mbids.add(selected_mbid)

                synthetic_ref_rows[selected_mbid] = {
                    "contributor": tm_contributor_case.smart_title(case["name_display"]) or case["name_display"],
                    "lpreferred__artist_name": case["norm_name"],
                    "disambiguation": _synthetic_disambiguation_text(
                        case.get("context_display"),
                        ", ".join(sorted(case["albums"])) if case.get("albums") else None,
                    ),
                }
        else:
            logging.info("No namesakes were encountered in this run")

        # Process full dataset
        updates_by_rowid = {}

        with tm_db.transaction(conn):
            logging.info("Started database transaction")

            # Defer all writes until interactive disambiguation has completed.
            setup_changelog_table(conn)

            if pending_namesake_cases:
                with tm_db.transaction(master_conn):
                    persist_user_disambiguation_decisions(
                        master_conn,
                        interactive_decisions,
                        source=DECISION_SOURCE_USER,
                    )

            # Process each row
            processed_rows = 0
            for row in df.iter_rows(named=True):
                processed_rows += 1
                if processed_rows % 50000 == 0:
                    logging.info(f"Processed {processed_rows} rows...")

                changes_in_row = 0  # Track changes for this row to increment __sqlmodded
                row_has_synthetic = False

                for field, mbid_field in fields.items():
                    value = row[field]
                    if value is None:
                        continue

                    # Split the value and match entities
                    entities = _normalized_tokens(value)
                    display_entities = _display_tokens(value)
                    context = _disambiguation_context(row.get("albumartist"), row.get("album"))
                    current_mbid_tokens = [
                        token.strip()
                        for token in str(row.get(mbid_field) or "").split(DELIMITER)
                    ]

                    matched_mbids = []
                    for entity_idx, entity in enumerate(entities):
                        display_entity = (
                            display_entities[entity_idx]
                            if entity_idx < len(display_entities)
                            else entity
                        )
                        decision_for_entity = decision_lookup.get((entity, context))
                        if decision_for_entity and _is_likely_synthetic_mbid(
                            decision_for_entity, ref_mbid_set
                        ):
                            decision_driven_synthetic_mbids.add(decision_for_entity)
                        if decision_for_entity == SKIP_DISAMBIGUATION_SENTINEL:
                            existing_token = (
                                current_mbid_tokens[entity_idx]
                                if entity_idx < len(current_mbid_tokens)
                                else ""
                            )
                            matched_mbids.append(existing_token)
                            continue

                        resolved_mbid = _resolve_with_context(
                            entity,
                            context,
                            contributors_dict,
                            decision_lookup,
                            stats=all_stats,
                            field=field,
                        )
                        matched_mbids.append(resolved_mbid)

                        if (
                            resolved_mbid
                            and _is_likely_synthetic_mbid(resolved_mbid, ref_mbid_set)
                            and (entity, context) not in decision_lookup
                        ):
                            decision_lookup[(entity, context)] = resolved_mbid
                            new_synthetic_decisions[(entity, context)] = resolved_mbid

                        if entity in contributors_dict or decision_for_entity:
                            continue

                        auto_fallback_synthetic_mbids.add(resolved_mbid)

                        synthetic_ref_rows.setdefault(
                            resolved_mbid,
                            {
                                "contributor": tm_contributor_case.smart_title(display_entity) or display_entity,
                                "lpreferred__artist_name": entity,
                                "disambiguation": _synthetic_disambiguation_text(
                                    row.get("albumartist"), row.get("album")
                                ),
                            },
                        )

                    # Set to None if no contributors OR all MBIDs are empty strings
                    if not matched_mbids:
                        new_value = None
                    else:
                        joined = DELIMITER.join(matched_mbids)
                        new_value = None if joined.replace("\\", "") == "" else joined

                    # Check current value
                    current_mbid = row[mbid_field]
                    is_current_empty = current_mbid is None or (
                        isinstance(current_mbid, str)
                        and (current_mbid.strip() == "" or current_mbid.strip() == '""')
                    )
                    # Determine if this is an addition or correction
                    update_needed = False

                    if is_current_empty and new_value:
                        # Empty to non-empty = addition
                        field_type = field
                        all_stats["additions"][field_type] = (
                            all_stats["additions"].get(field_type, 0) + 1
                        )
                        update_needed = True
                        changes_in_row += 1
                    elif not is_current_empty and new_value != str(current_mbid).strip():
                        # Non-empty to different = correction
                        field_type = field
                        all_stats["corrections"][field_type] = (
                            all_stats["corrections"].get(field_type, 0) + 1
                        )
                        update_needed = True
                        changes_in_row += 1

                    if update_needed:
                        rowid = row["rowid"]
                        new_counter = _synthetic_token_counter(new_value, ref_mbid_set)
                        if new_counter:
                            row_has_synthetic = True
                        old_counter = _synthetic_token_counter(current_mbid, ref_mbid_set)
                        introduced_counter = _counter_positive_subtract(new_counter, old_counter)
                        carried_counter = _counter_intersection(new_counter, old_counter)

                        introduced_total = int(
                            sum(int(c) for c in introduced_counter.values())
                        )
                        carried_total = int(sum(int(c) for c in carried_counter.values()))

                        if introduced_total:
                            all_stats["synthetic_generated_distinct_set"].update(
                                introduced_counter.keys()
                            )
                            all_stats["synthetic_written"][field] = (
                                all_stats["synthetic_written"].get(field, 0)
                                + introduced_total
                            )
                            all_stats["synthetic_written_total"] += introduced_total

                            for token, token_count in introduced_counter.items():
                                count = int(token_count)
                                if token in decision_driven_synthetic_mbids:
                                    all_stats["synthetic_decision_driven_total"] += count
                                elif token in auto_fallback_synthetic_mbids:
                                    all_stats["synthetic_auto_fallback_total"] += count
                                else:
                                    all_stats["synthetic_other_introduced_total"] += count

                        if carried_total:
                            all_stats["synthetic_carried_forward_total"] += carried_total

                        if rowid not in updates_by_rowid:
                            updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}
                        updates_by_rowid[rowid][mbid_field] = new_value
                        # Store old value for changelog
                        updates_by_rowid[rowid]["old_values"][mbid_field] = current_mbid

                if row_has_synthetic:
                    all_stats["synthetic_rows_written_total"] += 1

                # If there were changes in this row, increment __sqlmodded
                if changes_in_row > 0:
                    rowid = row["rowid"]
                    current_sqlmodded = row["__sqlmodded"]
                    new_sqlmodded = current_sqlmodded + changes_in_row

                    if rowid not in updates_by_rowid:
                        updates_by_rowid[rowid] = {"rowid": rowid, "old_values": {}}

                    # Only include __sqlmodded if it's > 0
                    if new_sqlmodded > 0:
                        updates_by_rowid[rowid]["__sqlmodded"] = new_sqlmodded
                    else:
                        # Set to NULL explicitly if 0
                        updates_by_rowid[rowid]["__sqlmodded"] = None

            logging.info(f"Completed processing {processed_rows} rows")

            if synthetic_ref_rows:
                logging.info(
                    "Synthetic lookup staging disabled by policy; %d synthetic row candidate(s) not inserted into %s",
                    len(synthetic_ref_rows),
                    DISAMBIGUATED_TABLE,
                )

            if new_synthetic_decisions:
                with tm_db.transaction(master_conn):
                    persist_user_disambiguation_decisions(
                        master_conn,
                        new_synthetic_decisions,
                        source=DECISION_SOURCE_AUTOMATED_NO_NAME_MATCH,
                    )
                logging.info(
                    "Persisted %d automatic synthetic disambiguation decision(s)",
                    len(new_synthetic_decisions),
                )

            # Write all updates at once
            if updates_by_rowid:
                logging.info(f"Writing {len(updates_by_rowid)} updates to database...")
                write_updates_to_db(
                    list(updates_by_rowid.values()),
                    conn,
                    all_stats,
                    batch_size=5000,
                    debug=debug,
                )
            else:
                logging.info("No updates needed")

            orphan_updates, orphan_clears_by_field = collect_orphan_mbid_clear_updates(conn)
            if orphan_updates:
                total_orphan_clears = sum(orphan_clears_by_field.values())
                logging.info(
                    "Clearing %d orphan MBID value(s) across %d row(s)",
                    total_orphan_clears,
                    len(orphan_updates),
                )
                for field, count in orphan_clears_by_field.items():
                    all_stats["corrections"][field] = (
                        all_stats["corrections"].get(field, 0) + int(count)
                    )
                write_updates_to_db(
                    orphan_updates,
                    conn,
                    all_stats,
                    batch_size=5000,
                    debug=debug,
                )
            else:
                logging.info("No orphan MBIDs found to clear")

        logging.info("Transaction committed successfully")

        all_stats["synthetic_generated_distinct_total"] = len(
            all_stats["synthetic_generated_distinct_set"]
        )
        all_stats["distinct_synthetic_in_alib"] = count_distinct_synthetic_mbids_in_alib(conn)

        # Display statistics
        display_statistics(all_stats)

    except Exception as e:
        logging.error(f"Error processing database: {e}")
        raise e


def count_distinct_synthetic_mbids_in_alib(conn: sqlite3.Connection) -> int:
    """
    Count distinct synthetic MBIDs currently present in `alib` across MBID columns.
    Synthetic MBIDs are treated as deterministic UUIDv5 values generated by this script.

    Uses Python-side tokenization with the configured multi-value delimiter to avoid
    SQL escaping edge-cases when delimiter characters are backslash-heavy.
    """
    cols_sql = ", ".join(MBID_ID_COLUMNS)
    nonempty_predicate = " OR ".join(
        f"({col} IS NOT NULL AND TRIM({col}) != '')" for col in MBID_ID_COLUMNS
    )
    rows = conn.execute(
        f"SELECT {cols_sql} FROM alib WHERE {nonempty_predicate}"
    ).fetchall()

    seen: set[str] = set()
    for row in rows:
        for value in row:
            if not value:
                continue
            for token in str(value).split(DELIMITER):
                mbid = token.strip()
                if mbid and len(mbid) == 36 and mbid[14] == "5":
                    seen.add(mbid)

    return len(seen)


def display_statistics(stats: Dict[str, Any]):
    """Display statistics about the updates"""
    total_additions = sum(stats["additions"].values())
    total_corrections = sum(stats["corrections"].values())
    total_fallback_resolutions = int(
        stats.get("synthetic_fallback_resolutions_total", 0)
    )
    synthetic_generated_distinct_total = int(
        stats.get(
            "synthetic_generated_distinct_total",
            len(stats.get("synthetic_generated_distinct_set", set())),
        )
    )
    total_synthetic_written = int(stats.get("synthetic_written_total", 0))
    synthetic_rows_written_total = int(stats.get("synthetic_rows_written_total", 0))
    distinct_synthetic_in_alib = int(stats.get("distinct_synthetic_in_alib", 0))
    synthetic_decision_driven_total = int(
        stats.get("synthetic_decision_driven_total", 0)
    )
    synthetic_auto_fallback_total = int(
        stats.get("synthetic_auto_fallback_total", 0)
    )
    synthetic_carried_forward_total = int(
        stats.get("synthetic_carried_forward_total", 0)
    )
    synthetic_other_introduced_total = int(
        stats.get("synthetic_other_introduced_total", 0)
    )
    total_changes = total_additions + total_corrections

    logging.info("MusicBrainz ID Update Summary:")
    logging.info("==============================")
    logging.info(f"Total changes: {total_changes}")
    logging.info(f"  - New IDs added: {total_additions}")
    logging.info(f"  - Existing IDs corrected: {total_corrections}")
    logging.info(
        f"  - Synthetic fallback resolutions this run (token-level): {total_fallback_resolutions}"
    )
    logging.info(
        f"  - Distinct synthetic MBIDs generated this run: {synthetic_generated_distinct_total}"
    )
    logging.info(
        f"  - Synthetic MBID tokens newly introduced this run: {total_synthetic_written}"
    )
    logging.info(
        f"    - Decision-driven synthetic tokens introduced: {synthetic_decision_driven_total}"
    )
    logging.info(
        f"    - Auto-fallback synthetic tokens introduced: {synthetic_auto_fallback_total}"
    )
    logging.info(
        f"    - Other synthetic tokens introduced: {synthetic_other_introduced_total}"
    )
    logging.info(
        f"  - Synthetic MBID tokens carried forward in changed records: {synthetic_carried_forward_total}"
    )
    logging.info(
        f"  - Records updated with >=1 synthetic MBID this run: {synthetic_rows_written_total}"
    )
    logging.info(f"Distinct synthetic MBIDs currently in alib: {distinct_synthetic_in_alib}")

    # Print detailed statistics by field type
    if stats["additions"]:
        logging.info("Additions by field type:")
        for field, count in sorted(stats["additions"].items()):
            logging.info(f"  {field}: {count}")

    if stats["corrections"]:
        logging.info("Corrections by field type:")
        for field, count in sorted(stats["corrections"].items()):
            logging.info(f"  {field}: {count}")

    fallback_by_field = stats.get("synthetic_fallback_resolutions", {})
    if fallback_by_field:
        logging.info("Synthetic fallback resolutions by field type:")
        for field, count in sorted(fallback_by_field.items()):
            logging.info(f"  {field}: {count}")

    written_by_field = stats.get("synthetic_written", {})
    if written_by_field:
        logging.info("Synthetic MBID tokens written by field type:")
        for field, count in sorted(written_by_field.items()):
            logging.info(f"  {field}: {count}")


def update_with_polars(file_path: str, use_chunking: bool = False, debug: bool = False):
    """
    Optimized implementation using Polars for data processing

    Args:
        file_path: Path to the SQLite database
        use_chunking: Whether to use chunking or process entire database at once
    """
    logging.info(f"Starting MBID processing with database: {file_path}")
    logging.info(f"Using chunking: {use_chunking}")

    # Open main (alib) DB connection and resolve master-data DB connection.
    conn = tm_db.connect(file_path)
    master_db_path = tm_config.get_master_data_db_path(default=file_path)
    master_conn = conn if master_db_path == file_path else tm_db.connect(master_db_path)

    tm_db.require_table_columns(
        master_conn,
        DISAMBIGUATED_TABLE,
        ("preferred__artist_name", "lpreferred__artist_name", "merge_key_mbid"),
        hint="Load your MusicBrainz-derived contributor→MBID reference into this staging DB before running this step.",
    )

    try:
        if use_chunking:
            # Process in chunks (useful for very large databases or limited memory)
            process_database(
                conn,
                master_conn,
                chunk_size=50000,
                debug=debug,
            )  # Increased chunk size for Polars
        else:
            # Process entire database at once (preferred with Polars if memory allows)
            process_full_database(conn, master_conn, debug=debug)

        logging.info("MBID processing completed successfully")

    except UserAbortedDisambiguation:
        logging.warning("MBID processing cancelled by user before any database writes")
        return

    except Exception as e:
        logging.error(f"Fatal error during MBID processing: {e}")
        raise
    finally:
        if master_conn is not conn:
            master_conn.close()
        # Close the connection when done
        conn.close()
        logging.info("Database connection closed")


def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(
        prog="18-populate-musicbrainz-ids.py",
        description="Populate MusicBrainz IDs in alib.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=f"Write debug update mirror rows to {DEBUG_UPDATES_TABLE}",
    )
    args = parser.parse_args()

    db_path = tm_run.resolve_db_path()
    logging.info("=== MBID Processing Script Started ===")

    try:
        # Set use_chunking=True if memory constraints are an issue
        update_with_polars(db_path, use_chunking=False, debug=args.debug)
        logging.info("=== MBID Processing Script Completed Successfully ===")
    except Exception as e:
        logging.error(f"=== MBID Processing Script Failed: {e} ===")
        raise


if __name__ == "__main__":
    main()
