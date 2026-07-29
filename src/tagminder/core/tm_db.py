"""

Purpose:
    Small, dependency-light SQLite helpers shared across Tagminder scripts.

Goals:
- Standardize connection setup (WAL, busy timeout, common PRAGMAs)
- Provide a consistent changelog schema and helpers
- Provide safe SQL helpers (identifier quoting, SET clause building)
- Provide a transaction context manager

This module intentionally avoids importing Polars.

This module is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog
    - sqlite_master

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

import logging
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence


DEFAULT_BUSY_TIMEOUT_MS = 5000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def script_name() -> str:
    """Best-effort script name for changelog entries."""
    try:
        return Path(sys.argv[0]).name or "<unknown>"
    except Exception:
        return "<unknown>"


def quote_ident(name: str) -> str:
    """SQLite identifier quoting using double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def connect(
    db_path: str,
    *,
    detect_types: int = 0,
    uri: bool = False,
    read_only: bool = False,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    wal: bool = True,
    pragmas: bool = True,
) -> sqlite3.Connection:
    """Connect to SQLite with standardized settings.

    If read_only=True, `uri` will be forced on and the path will be opened with mode=ro.
    """

    path = db_path
    if read_only:
        uri = True
        if "?" in path:
            # Caller is already supplying a URI; assume they know what they're doing.
            pass
        else:
            path = f"file:{path}?mode=ro"

    conn = sqlite3.connect(path, detect_types=detect_types, uri=uri)

    if pragmas:
        apply_pragmas(conn, wal=wal, busy_timeout_ms=busy_timeout_ms)

    return conn


def apply_pragmas(
    conn: sqlite3.Connection,
    *,
    wal: bool = True,
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
) -> None:
    """Apply pragmatic defaults. Keep this conservative to avoid behavioral surprises."""

    try:
        conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    except sqlite3.Error:
        pass

    # WAL is generally beneficial for these ETL-style scripts; if the DB is on a
    # filesystem that doesn't support it, SQLite will raise.
    if wal:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.Error:
            pass

    try:
        conn.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.Error:
        pass


def optimize_for_etl(conn: sqlite3.Connection) -> None:
    """Apply optional, more aggressive PRAGMAs for ETL-style workloads.

    This is intentionally opt-in (callers must invoke it explicitly) because
    these settings trade durability/latency characteristics for throughput.
    """

    pragmas = [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -2097152",
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 8589934592",
        "PRAGMA page_size = 4096",
        "PRAGMA wal_autocheckpoint = 10000",
        "PRAGMA optimize",
    ]

    for pragma in pragmas:
        try:
            conn.execute(pragma)
        except sqlite3.Error:
            # Some pragmas may fail on certain filesystems/SQLite builds.
            continue


CHANGELOG_DDL = """
CREATE TABLE IF NOT EXISTS changelog (
    alib_path TEXT,
    alib_column TEXT,
    old_value TEXT,
    new_value TEXT,
    timestamp TEXT,
    script TEXT
)
""".strip()


def ensure_changelog_table(conn: sqlite3.Connection) -> None:
    """Ensure the canonical changelog table exists.

    If a legacy schema exists, this will migrate it in-place by renaming the
    existing table, creating the canonical one, copying rows over, and dropping
    the legacy table.
    """

    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='changelog'"
    )
    if cur.fetchone() is None:
        conn.execute(CHANGELOG_DDL)
        return

    # Inspect existing schema.
    cur = conn.execute("PRAGMA table_info(changelog)")
    cols = cur.fetchall()
    existing = {row[1]: (row[2] or "").upper() for row in cols}  # name -> type

    canonical_cols = {
        "alib_path": "TEXT",
        "alib_column": "TEXT",
        "old_value": "TEXT",
        "new_value": "TEXT",
        "timestamp": "TEXT",
        "script": "TEXT",
    }

    def _is_compatible(existing_type: str, want: str) -> bool:
        # We intentionally enforce exact declared types here so the table schema
        # is truly unified across scripts.
        if not existing_type:
            return False
        return existing_type == want

    compatible = (
        set(existing.keys()) == set(canonical_cols.keys())
        and all(_is_compatible(existing.get(k, ""), v) for k, v in canonical_cols.items())
    )
    if compatible:
        return

    # Migrate: rename old table, create new, copy rows, drop old.
    backup = f"changelog__backup_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    conn.execute(f"ALTER TABLE changelog RENAME TO {quote_ident(backup)}")
    conn.execute(CHANGELOG_DDL)

    # Copy legacy rows forward.
    # - If the legacy table already had alib_path, use it.
    # - Else if it had alib_rowid, attempt to map to alib.__path via JOIN; fallback to stringified rowid.
    id_col = None
    if "alib_path" in existing:
        id_col = "alib_path"
    elif "alib_rowid" in existing:
        id_col = "alib_rowid"

    has_alib = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='alib'"
        ).fetchone()
        is not None
    )
    has_path_col = False
    if has_alib:
        try:
            alib_cols = conn.execute("PRAGMA table_info(alib)").fetchall()
            has_path_col = any(row[1] == "__path" for row in alib_cols)
        except sqlite3.Error:
            has_path_col = False

    if id_col == "alib_path":
        conn.execute(
            "INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script) "
            f"SELECT CAST(alib_path AS TEXT), alib_column, old_value, new_value, timestamp, script FROM {quote_ident(backup)}"
        )
    elif id_col == "alib_rowid" and has_alib and has_path_col:
        # Best-effort mapping for older changelog tables that stored rowid.
        conn.execute(
            "INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script) "
            f"SELECT COALESCE(a.__path, CAST(b.alib_rowid AS TEXT)), b.alib_column, b.old_value, b.new_value, b.timestamp, b.script "
            f"FROM {quote_ident(backup)} AS b LEFT JOIN alib AS a ON a.rowid = b.alib_rowid"
        )
    else:
        # Fallback: preserve whatever identifier existed as text.
        if id_col is None:
            conn.execute(
                "INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script) "
                f"SELECT NULL, alib_column, old_value, new_value, timestamp, script FROM {quote_ident(backup)}"
            )
        else:
            conn.execute(
                "INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script) "
                f"SELECT CAST({quote_ident(id_col)} AS TEXT), alib_column, old_value, new_value, timestamp, script FROM {quote_ident(backup)}"
            )

    conn.execute(f"DROP TABLE {quote_ident(backup)}")


MASTER_DATA_CHANGELOG_DDL = """
CREATE TABLE IF NOT EXISTS master_data_changelog (
    table_name TEXT,
    rowid INTEGER,
    column_name TEXT,
    old_value TEXT,
    new_value TEXT,
    timestamp TEXT,
    script TEXT
)
""".strip()


REF_VETTED_CONTRIBUTORS_DDL = """
CREATE TABLE IF NOT EXISTS _REF_vetted_contributors (
    current_val TEXT,
    replacement_val TEXT,
    status INTEGER,
    source TEXT,
    lcurrent_val TEXT GENERATED ALWAYS AS (lower(current_val)) VIRTUAL,
    lreplacement_val TEXT GENERATED ALWAYS AS (lower(replacement_val)) VIRTUAL
)
""".strip()


REF_MB_DISAMBIGUATED_DDL = """
CREATE TABLE IF NOT EXISTS contributors_unified_disambiguated (
    merge_key_mbid TEXT,
    preferred__artist_name TEXT,
    lpreferred__artist_name TEXT,
    musicbrainz_disambiguation TEXT,
    allmusic_genres_json TEXT,
    allmusic_styles_json TEXT,
    synthetic_uuid INTEGER NOT NULL DEFAULT 0 CHECK (synthetic_uuid IN (0, 1))
)
""".strip()


REF_MB_NAMESAKES_DDL = """
CREATE TABLE IF NOT EXISTS contributors_unified_namesakes (
    merge_key_mbid TEXT,
    preferred__artist_name TEXT,
    lpreferred__artist_name TEXT,
    musicbrainz_disambiguation TEXT,
    allmusic_genres_json TEXT,
    allmusic_styles_json TEXT
)
""".strip()


def ensure_reference_lookup_tables(conn: sqlite3.Connection) -> None:
    """Validate core reference lookup tables and log implications.

    This helper is intentionally non-mutating: it does not create reference
    tables. Reference/master-data tables are expected to be provisioned by
    explicit harvest/curation flows in the master-data database.
    """
    table_implications = {
        "_REF_vetted_contributors": (
            "No vetted contributor mappings exist. 07-apply-vetted-contributor-mappings.py cannot apply replacements, "
            "89-validate-vetted-contributor-multi-values.py has nothing meaningful to validate, and contributor curation "
            "workflows lose vetted mapping context."
        ),
        "contributors_unified_disambiguated": (
            "No canonical contributor->MBID map exists. MBID enrichment/normalization scripts cannot resolve real MBIDs "
            "from this reference, and contributor normalization/features relying on this lookup will be incomplete."
        ),
        "contributors_unified_namesakes": (
            "No namesake candidate map exists. Namesake-aware disambiguation in 18-populate-musicbrainz-ids.py cannot "
            "present MBID candidates, and 06-normalize-contributors.py cannot supplement lookups with namesake entries."
        ),
    }

    for table, implication in table_implications.items():
        if not table_exists(conn, table):
            logging.warning("Reference table %s is missing. %s", table, implication)
            continue

        count = int(conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0] or 0)
        if count == 0:
            logging.warning("Reference table %s exists but is empty. %s", table, implication)


def ensure_reference_lookup_indexes(conn: sqlite3.Connection) -> None:
    """Create lookup indexes for reference tables that already exist."""

    index_ddls = {
        "_REF_vetted_contributors": [
            "CREATE INDEX IF NOT EXISTS idx_ref_vetted_lcurrent_val ON _REF_vetted_contributors(lcurrent_val)",
            "CREATE INDEX IF NOT EXISTS idx_ref_vetted_lreplacement_val ON _REF_vetted_contributors(lreplacement_val)",
            "CREATE INDEX IF NOT EXISTS idx_ref_vetted_status ON _REF_vetted_contributors(status)",
        ],
        "contributors_unified_disambiguated": [
            "CREATE INDEX IF NOT EXISTS idx_contrib_unified_disambig_lpreferred ON contributors_unified_disambiguated(lpreferred__artist_name)",
            "CREATE INDEX IF NOT EXISTS idx_contrib_unified_disambig_mbid ON contributors_unified_disambiguated(merge_key_mbid)",
            "CREATE INDEX IF NOT EXISTS idx_contrib_unified_disambig_synthetic_uuid ON contributors_unified_disambiguated(synthetic_uuid)",
        ],
        "contributors_unified_namesakes": [
            "CREATE INDEX IF NOT EXISTS idx_contrib_unified_namesakes_lpreferred ON contributors_unified_namesakes(lpreferred__artist_name)",
            "CREATE INDEX IF NOT EXISTS idx_contrib_unified_namesakes_mbid ON contributors_unified_namesakes(merge_key_mbid)",
        ],
    }

    for table, ddls in index_ddls.items():
        if not table_exists(conn, table):
            continue
        for ddl in ddls:
            conn.execute(ddl)


def ensure_master_data_changelog_table(conn: sqlite3.Connection) -> None:
    """Ensure the canonical master_data_changelog table exists.

    Like ensure_changelog_table, this enforces a unified schema across all
    scripts that touch reference/master data tables.
    """

    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='master_data_changelog'"
    )
    if cur.fetchone() is None:
        conn.execute(MASTER_DATA_CHANGELOG_DDL)
        return

    # Inspect existing schema.
    cur = conn.execute("PRAGMA table_info(master_data_changelog)")
    cols = cur.fetchall()
    existing = {row[1]: (row[2] or "").upper() for row in cols}  # name -> type

    canonical_cols = {
        "table_name": "TEXT",
        "rowid": "INTEGER",
        "column_name": "TEXT",
        "old_value": "TEXT",
        "new_value": "TEXT",
        "timestamp": "TEXT",
        "script": "TEXT",
    }

    def _is_compatible(existing_type: str, want: str) -> bool:
        if not existing_type:
            return False
        return existing_type == want

    compatible = (
        set(existing.keys()) == set(canonical_cols.keys())
        and all(_is_compatible(existing.get(k, ""), v) for k, v in canonical_cols.items())
    )
    if compatible:
        return

    # Migrate: rename old table, create new, copy rows, drop old.
    backup = f"master_data_changelog__backup_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    conn.execute(f"ALTER TABLE master_data_changelog RENAME TO {quote_ident(backup)}")
    conn.execute(MASTER_DATA_CHANGELOG_DDL)

    # Copy legacy rows forward (simple case: columns map directly).
    if set(existing.keys()) == set(canonical_cols.keys()):
        conn.execute(
            "INSERT INTO master_data_changelog (table_name, rowid, column_name, old_value, new_value, timestamp, script) "
            f"SELECT table_name, rowid, column_name, old_value, new_value, timestamp, script FROM {quote_ident(backup)}"
        )
    else:
        # Schema mismatch: log a warning but skip migration.
        logging.warning(
            f"Incompatible master_data_changelog schema detected. "
            f"Expected columns: {set(canonical_cols.keys())}, "
            f"found: {set(existing.keys())}. Backup created as {backup}."
        )
        return

    conn.execute(f"DROP TABLE {quote_ident(backup)}")


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Return True if a table exists in the current SQLite database."""

    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names for a table.

    If the table does not exist (or introspection fails), returns an empty set.
    """

    try:
        rows = conn.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
        return {str(r[1]) for r in rows if r and r[1] is not None}
    except sqlite3.Error:
        return set()


def require_table_columns(
    conn: sqlite3.Connection,
    table: str,
    required: Sequence[str],
    *,
    hint: str | None = None,
) -> None:
    """Fail fast if a required table/columns are missing.

    This is primarily used for reference tables that are populated outside of
    Tagminder (e.g., MusicBrainz-derived lookup tables).
    """

    required_set = {str(c) for c in required}
    if not table_exists(conn, table):
        msg = (
            f"Missing required table {table!r}. "
            f"Expected columns: {sorted(required_set)}."
        )
        if hint:
            msg += f" {hint}"
        raise RuntimeError(msg)

    existing = table_columns(conn, table)
    missing = sorted(required_set - existing)
    if missing:
        msg = (
            f"Table {table!r} is missing required columns: {missing}. "
            f"Existing columns: {sorted(existing)}."
        )
        if hint:
            msg += f" {hint}"
        raise RuntimeError(msg)


@dataclass(frozen=True)
class ChangelogEntry:
    alib_path: str
    alib_column: str
    old_value: str | None
    new_value: str | None
    timestamp: str
    script: str


@dataclass(frozen=True)
class MasterDataChangelogEntry:
    table_name: str
    rowid: int
    column_name: str
    old_value: str | None
    new_value: str | None
    timestamp: str
    script: str


def insert_changelog_entries(
    cursor: sqlite3.Cursor,
    entries: Sequence[ChangelogEntry],
) -> None:
    if not entries:
        return
    cursor.executemany(
        "INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                e.alib_path,
                e.alib_column,
                e.old_value,
                e.new_value,
                e.timestamp,
                e.script,
            )
            for e in entries
        ],
    )


def insert_master_data_changelog_entries(
    cursor: sqlite3.Cursor,
    entries: Sequence[MasterDataChangelogEntry],
) -> None:
    if not entries:
        return
    cursor.executemany(
        "INSERT INTO master_data_changelog (table_name, rowid, column_name, old_value, new_value, timestamp, script) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                e.table_name,
                e.rowid,
                e.column_name,
                e.old_value,
                e.new_value,
                e.timestamp,
                e.script,
            )
            for e in entries
        ],
    )


def fetch_paths_by_rowid(conn: sqlite3.Connection, rowids: Sequence[int]) -> dict[int, str]:
    """Fetch __path values for a set of SQLite rowids.

    Many scripts keep rowid in-memory for updates; this helper allows changelog
    inserts to record the stable __path identifier.
    """

    unique = sorted({int(r) for r in rowids if r is not None})
    if not unique:
        return {}

    out: dict[int, str] = {}
    chunk_size = 900  # SQLite variable limit safety
    for i in range(0, len(unique), chunk_size):
        chunk = unique[i : i + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        cur = conn.execute(
            f"SELECT rowid, __path FROM alib WHERE rowid IN ({placeholders})",
            chunk,
        )
        for rid, path in cur.fetchall():
            if rid is not None and path is not None:
                out[int(rid)] = str(path)
    return out


def build_update_sql(
    *,
    table: str,
    set_cols: Sequence[str],
    where_col: str = "rowid",
    sqlmodded_col: str = "__sqlmodded",
) -> str:
    """Build an UPDATE statement with safe quoting and NULLIF for __sqlmodded."""

    quoted_set = [f"{quote_ident(c)} = ?" for c in set_cols]
    quoted_set.append(f"{quote_ident(sqlmodded_col)} = NULLIF(?, 0)")

    return (
        f"UPDATE {quote_ident(table)} SET "
        + ", ".join(quoted_set)
        + f" WHERE {quote_ident(where_col)} = ?"
    )


@contextmanager
def transaction(conn: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """Context manager for a consistent BEGIN/COMMIT/ROLLBACK pattern."""

    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
