"""tm_snapshots.py

Purpose:
    Lightweight snapshot/metrics utilities for Tagminder dashboards.

Design goals:
- Offline-only: compute coherence/completeness metrics from the staging DB.
- Fast enough for large libraries: store aggregates, not full row snapshots.
- Deterministic and version-tolerant: handle missing columns gracefully.

This module is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog
    - _SNAP_runs
    - _SNAP_core_tags
    - _SNAP_critical_tags

Author: audiomuze
Last updated: 2026-04-18
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db

# Single source of truth for album-root logic.
album_root = tm_album.album_root


def register_sql_functions(conn: sqlite3.Connection) -> None:
    tm_album.register_sql_functions(conn, func_name="album_root")


@dataclass(frozen=True)
class ChangelogFingerprint:
    max_timestamp: str | None
    row_count: int


def get_changelog_fingerprint(conn: sqlite3.Connection) -> ChangelogFingerprint:
    tm_db.ensure_changelog_table(conn)
    row = conn.execute("SELECT MAX(timestamp), COUNT(*) FROM changelog").fetchone()
    max_ts = str(row[0]) if row and row[0] is not None else None
    n = int(row[1] or 0) if row else 0
    return ChangelogFingerprint(max_timestamp=max_ts, row_count=n)


SNAP_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS _SNAP_runs (
    run_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    created_utc TEXT NOT NULL,
    notes TEXT,
    changelog_max_timestamp TEXT,
    changelog_row_count INTEGER
)
""".strip()


SNAP_TAGS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id TEXT NOT NULL,
    column TEXT NOT NULL,
    library_pct_populated REAL NOT NULL,
    album_pct_populated REAL NOT NULL,
    column_present INTEGER NOT NULL,
    PRIMARY KEY (run_id, column)
)
""".strip()


def ensure_snapshot_tables(conn: sqlite3.Connection) -> None:
    conn.execute(SNAP_RUNS_DDL)
    conn.execute(SNAP_TAGS_DDL.format(table="_SNAP_core_tags"))
    conn.execute(SNAP_TAGS_DDL.format(table="_SNAP_critical_tags"))


def create_run(
    conn: sqlite3.Connection,
    *,
    label: str,
    notes: str | None,
    fingerprint: ChangelogFingerprint,
) -> str:
    run_id = tm_db.utc_now_iso()
    conn.execute(
        "INSERT INTO _SNAP_runs (run_id, label, created_utc, notes, changelog_max_timestamp, changelog_row_count) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            run_id,
            str(label),
            tm_db.utc_now_iso(),
            notes,
            fingerprint.max_timestamp,
            int(fingerprint.row_count),
        ),
    )
    return run_id


def get_latest_run_id(conn: sqlite3.Connection, *, label: str) -> str | None:
    row = conn.execute(
        "SELECT run_id FROM _SNAP_runs WHERE label=? ORDER BY created_utc DESC LIMIT 1",
        (label,),
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def get_run_created_utc(conn: sqlite3.Connection, *, run_id: str) -> str | None:
    row = conn.execute(
        "SELECT created_utc FROM _SNAP_runs WHERE run_id=?",
        (str(run_id),),
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def get_latest_run_id_at_or_before(conn: sqlite3.Connection, *, label: str, created_utc: str) -> str | None:
    """Return latest run_id with label and created_utc <= given timestamp."""

    row = conn.execute(
        "SELECT run_id FROM _SNAP_runs WHERE label=? AND created_utc <= ? ORDER BY created_utc DESC LIMIT 1",
        (str(label), str(created_utc)),
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def get_latest_run_fingerprint(conn: sqlite3.Connection, *, label: str) -> ChangelogFingerprint | None:
    row = conn.execute(
        "SELECT changelog_max_timestamp, changelog_row_count FROM _SNAP_runs "
        "WHERE label=? ORDER BY created_utc DESC LIMIT 1",
        (label,),
    ).fetchone()
    if not row:
        return None
    max_ts = str(row[0]) if row[0] is not None else None
    return ChangelogFingerprint(max_timestamp=max_ts, row_count=int(row[1] or 0))


def _alib_columns(conn: sqlite3.Connection) -> set[str]:
    return {str(r[1]) for r in conn.execute("PRAGMA table_info(alib)").fetchall()}


def _non_null_expr(col: str) -> str:
    q = tm_db.quote_ident(col)
    # Treat empty/whitespace strings as missing; keep numeric 0 as populated.
    return f"({q} IS NOT NULL AND TRIM(CAST({q} AS TEXT)) != '')"


def compute_coverage(
    conn: sqlite3.Connection,
    *,
    columns: list[str],
) -> dict[str, tuple[float, float, int]]:
    """Compute coverage for columns.

    Returns mapping:
        col -> (library_pct_populated, album_pct_populated, column_present)

    Album metric:
        per album-root: (# tracks populated) / (# tracks)
        then averaged across album-roots (equal weight per album).
    """

    cols_all = [c for c in columns if isinstance(c, str) and c and not c.startswith("__")]
    if not cols_all:
        return {}

    present_cols = _alib_columns(conn)
    cols_present = [c for c in cols_all if c in present_cols]

    register_sql_functions(conn)

    track_count = int(conn.execute("SELECT COUNT(*) FROM alib").fetchone()[0] or 0)

    lib_pct_by_col: dict[str, float] = {}
    alb_pct_by_col: dict[str, float] = {}

    if cols_present:
        # Track-level: single pass.
        sum_exprs = ", ".join(
            f"SUM(CASE WHEN {_non_null_expr(c)} THEN 1 ELSE 0 END) AS {tm_db.quote_ident(c)}" for c in cols_present
        )
        sums = conn.execute(f"SELECT {sum_exprs} FROM alib").fetchone()
        for i, c in enumerate(cols_present):
            n_present = int(((sums[i] if sums is not None else 0) or 0))
            lib_pct_by_col[c] = (n_present / track_count * 100.0) if track_count else 0.0

        # Album-root: compute sums and then avg ratios in SQL.
        sum_exprs_album = ", ".join(
            f"SUM(CASE WHEN {_non_null_expr(c)} THEN 1 ELSE 0 END) AS {tm_db.quote_ident(c)}" for c in cols_present
        )
        inner = (
            "SELECT album_root(__dirpath) AS root, COUNT(*) AS n_tracks, "
            f"{sum_exprs_album} "
            "FROM alib WHERE __dirpath IS NOT NULL GROUP BY root"
        )
        outer_avg_exprs = ", ".join(
            f"AVG(COALESCE({tm_db.quote_ident(c)}, 0) * 1.0 / NULLIF(n_tracks, 0)) AS {tm_db.quote_ident(c)}"
            for c in cols_present
        )
        avgs = conn.execute(f"SELECT {outer_avg_exprs} FROM ({inner})").fetchone()
        for i, c in enumerate(cols_present):
            alb_pct_by_col[c] = float(((avgs[i] if avgs is not None else 0.0) or 0.0)) * 100.0

    out: dict[str, tuple[float, float, int]] = {}
    for c in cols_all:
        present = 1 if c in present_cols else 0
        out[c] = (float(lib_pct_by_col.get(c, 0.0)), float(alb_pct_by_col.get(c, 0.0)), present)

    return out


def write_coverage_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    run_id: str,
    columns_in_order: list[str],
    coverage: dict[str, tuple[float, float, int]],
) -> None:
    # Remove any existing rows for the run_id in case of re-run.
    conn.execute(f"DELETE FROM {tm_db.quote_ident(table)} WHERE run_id = ?", (run_id,))

    rows: list[tuple[object, ...]] = []
    for c in columns_in_order:
        if c not in coverage:
            continue
        lib_pct, alb_pct, present = coverage[c]
        rows.append((run_id, c, float(lib_pct), float(alb_pct), int(present)))

    conn.executemany(
        f"INSERT INTO {tm_db.quote_ident(table)} (run_id, column, library_pct_populated, album_pct_populated, column_present) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )


def load_config_columns() -> tuple[list[str], list[str]]:
    cfg = tm_config.load_config()

    cleanup = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
    if not isinstance(cleanup, dict):
        raise RuntimeError("Missing config table [cleanup] in tagminder.toml")
    keep = cleanup.get("keep_columns")
    if not isinstance(keep, list) or not keep:
        raise RuntimeError("Invalid or missing [cleanup].keep_columns")
    keep_cols = [str(x) for x in keep if isinstance(x, str) and x]

    reports = cfg.get("reports", {}) if isinstance(cfg, dict) else {}
    report_cfg = reports.get("missing_critical_tags_by_album", {}) if isinstance(reports, dict) else {}
    if not isinstance(report_cfg, dict):
        raise RuntimeError("Missing config table [reports.missing_critical_tags_by_album] in tagminder.toml")
    critical = report_cfg.get("critical_columns")
    if not isinstance(critical, list) or not critical:
        raise RuntimeError("Invalid or missing [reports.missing_critical_tags_by_album].critical_columns")
    critical_cols = [str(x) for x in critical if isinstance(x, str) and x]

    # de-dupe preserve order
    def _dedupe(seq: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for s in seq:
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    return _dedupe(keep_cols), _dedupe(critical_cols)


def fetch_tag_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    run_id: str,
    columns_in_order: list[str],
) -> tuple[list[str], list[float], list[float]]:
    rows = conn.execute(
        f"SELECT column, library_pct_populated, album_pct_populated, column_present FROM {tm_db.quote_ident(table)} WHERE run_id = ?",
        (run_id,),
    ).fetchall()

    by_col: dict[str, tuple[float, float, int]] = {}
    for col, libp, albp, present in rows:
        by_col[str(col)] = (float(libp or 0.0), float(albp or 0.0), int(present or 0))

    cols: list[str] = []
    lib: list[float] = []
    alb: list[float] = []
    for c in columns_in_order:
        v = by_col.get(c)
        if v is None:
            # Column missing from snapshot table; treat as 0.
            cols.append(c)
            lib.append(0.0)
            alb.append(0.0)
        else:
            cols.append(c)
            lib.append(float(v[0]))
            alb.append(float(v[1]))

    return cols, lib, alb
