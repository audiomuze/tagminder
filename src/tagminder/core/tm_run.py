"""Standard run helpers for numbered Tagminder scripts.

Purpose:
    Keep the per-script `main()` functions small and consistent.

    Responsibilities:
        - Resolve staging DB path (supports `--db` override via tm_config)
        - Connect via tm_db.connect
        - Optionally ensure changelog schema exists
        - Provide common `script` and `timestamp` values

This module is part of Tagminder.

SQLite tables referenced:
    - changelog (optional; schema ensure)
    - sqlite_master (introspection; optional)

Author: audiomuze
Last updated: 2026-04-15
"""

from __future__ import annotations

import logging
from pathlib import Path
import sqlite3

from tagminder.core import tm_config
from tagminder.core import tm_db

def resolve_db_path(*, default_db_path: str | None = None) -> str:
    """Resolve database path using `--db` override or tagminder.toml."""

    return tm_config.get_db_path(default=default_db_path)


def open_db(
    *,
    default_db_path: str | None = None,
    db_path: str | None = None,
    read_only: bool = False,
    require_exists: bool = False,
    ensure_changelog: bool = False,
    ensure_reference_tables: bool = False,
    log_connect: bool = True,
) -> tuple[sqlite3.Connection, str, str, str]:
    """Resolve db path, connect, and return common run metadata.

    Returns:
        (conn, db_path, script_name, timestamp)
    """

    path = db_path or resolve_db_path(default_db_path=default_db_path)

    if require_exists and not Path(path).exists():
        raise FileNotFoundError(path)

    if log_connect:
        logging.info(f"Connecting to database: {path}")

    conn = tm_db.connect(path, read_only=read_only)

    if ensure_reference_tables and not read_only:
        tm_db.ensure_reference_lookup_indexes(conn)
        tm_db.ensure_reference_lookup_tables(conn)

    if ensure_changelog:
        tm_db.ensure_changelog_table(conn)

    return conn, path, tm_db.script_name(), tm_db.utc_now_iso()
