"""Changelog analysis helpers.

Purpose:
    Summarize Tagminder's `changelog` table into a compact, visualizable set of
    metrics (what changed, by whom, and where).

Design goals:
    - Fast for large changelogs: do aggregation in SQLite and return small
      result sets.
    - Deterministic: treat values as TEXT with TRIM semantics.
    - Reusable across dashboards/reports.

This module is part of Tagminder.

SQLite tables referenced:
    - changelog

Author: audiomuze
Last updated: 2026-04-18
"""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3

from tagminder.core import tm_album
from tagminder.core import tm_db

@dataclass(frozen=True)
class ChangelogSummary:
    start_ts: str
    end_ts: str

    raw_rows: int
    total_entries: int
    noop_entries: int
    tracks_touched: int
    albums_touched: int

    enrich_entries: int
    clear_entries: int
    modify_entries: int

    by_script: list[tuple[str, int]]
    by_column: list[tuple[str, int]]

    by_column_breakdown: list["ColumnChangeBreakdown"]

    noop_by_script: list[tuple[str, int]]


@dataclass(frozen=True)
class ColumnChangeBreakdown:
    column: str
    total: int
    adds: int
    deletes: int
    changes: int
    tracks_touched: int
    albums_touched: int
    retained: bool


def _is_empty_sql(col: str) -> str:
    q = tm_db.quote_ident(col)
    # Treat NULL/blank/"" as empty.
    return f"({q} IS NULL OR TRIM(CAST({q} AS TEXT)) = '' OR TRIM(CAST({q} AS TEXT)) = '\"\"')"


def _norm_value_sql(col: str) -> str:
    """Normalize a changelog value for comparisons.

    Rules:
        - NULL -> ''
        - TRIM(value)
        - '""' (literal quote-quote) -> ''
    """

    q = tm_db.quote_ident(col)
    trimmed = f"TRIM(CAST({q} AS TEXT))"
    return f"(CASE WHEN {q} IS NULL THEN '' WHEN {trimmed} = '\"\"' THEN '' ELSE {trimmed} END)"


def _album_root_from_path(path: str) -> str:
    p = str(path or "")
    if not p:
        return ""
    # Derive dirpath from full file path.
    if "/" in p:
        dirpath = p.rsplit("/", 1)[0]
    else:
        dirpath = ""
    return tm_album.album_root(dirpath)


def register_sql_functions(conn: sqlite3.Connection) -> None:
    conn.create_function("album_root_from_path", 1, _album_root_from_path)


def summarize(
    conn: sqlite3.Connection,
    *,
    start_ts: str,
    end_ts: str,
    top_n: int = 20,
    retained_columns: set[str] | None = None,
    system_prefix: str = "__",
) -> ChangelogSummary:
    """Summarize changelog entries in [start_ts, end_ts] (inclusive).

    Timestamps are compared lexicographically as ISO strings (Tagminder writes
    ISO-8601 UTC timestamps consistently).
    """

    tm_db.ensure_changelog_table(conn)
    register_sql_functions(conn)

    start_ts = str(start_ts)
    end_ts = str(end_ts)

    where_base = "timestamp >= ? AND timestamp <= ?"
    params = (start_ts, end_ts)

    norm_old = _norm_value_sql("old_value")
    norm_new = _norm_value_sql("new_value")
    changed = f"({norm_old} <> {norm_new})"
    where = f"{where_base} AND {changed}"
    where_noop = f"{where_base} AND NOT {changed}"

    # Totals.
    raw_rows = int(
        conn.execute(f"SELECT COUNT(*) FROM changelog WHERE {where_base}", params).fetchone()[0]
        or 0
    )
    total_entries = int(
        conn.execute(f"SELECT COUNT(*) FROM changelog WHERE {where}", params).fetchone()[0]
        or 0
    )
    noop_entries = max(0, raw_rows - total_entries)
    tracks_touched = int(
        conn.execute(f"SELECT COUNT(DISTINCT alib_path) FROM changelog WHERE {where}", params).fetchone()[0]
        or 0
    )
    albums_touched = int(
        conn.execute(
            f"SELECT COUNT(DISTINCT album_root_from_path(alib_path)) FROM changelog WHERE {where}",
            params,
        ).fetchone()[0]
        or 0
    )

    old_empty = _is_empty_sql("old_value")
    new_empty = _is_empty_sql("new_value")

    enrich_entries = int(
        conn.execute(
            f"SELECT SUM(CASE WHEN {old_empty} AND NOT {new_empty} THEN 1 ELSE 0 END) "
            f"FROM changelog WHERE {where}",
            params,
        ).fetchone()[0]
        or 0
    )
    clear_entries = int(
        conn.execute(
            f"SELECT SUM(CASE WHEN NOT {old_empty} AND {new_empty} THEN 1 ELSE 0 END) "
            f"FROM changelog WHERE {where}",
            params,
        ).fetchone()[0]
        or 0
    )
    modify_entries = max(0, total_entries - enrich_entries - clear_entries)

    # By script.
    by_script: list[tuple[str, int]] = []
    for script, n in conn.execute(
        f"SELECT COALESCE(NULLIF(TRIM(script), ''), '(unknown)') AS s, COUNT(*) AS n "
        f"FROM changelog WHERE {where} GROUP BY s ORDER BY n DESC LIMIT ?",
        (*params, int(top_n)),
    ).fetchall():
        by_script.append((str(script), int(n or 0)))

    # No-op rows by script (old==new after normalization).
    noop_by_script: list[tuple[str, int]] = []
    if noop_entries:
        for script, n in conn.execute(
            f"SELECT COALESCE(NULLIF(TRIM(script), ''), '(unknown)') AS s, COUNT(*) AS n "
            f"FROM changelog WHERE {where_noop} GROUP BY s ORDER BY n DESC LIMIT ?",
            (*params, int(top_n)),
        ).fetchall():
            noop_by_script.append((str(script), int(n or 0)))

    # By column.
    by_column_breakdown: list[ColumnChangeBreakdown] = []
    by_column: list[tuple[str, int]] = []
    for col, total, adds, deletes, tracks, albums in conn.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(alib_column), ''), '(unknown)') AS c,
            COUNT(*) AS total,
            SUM(CASE WHEN {old_empty} AND NOT {new_empty} THEN 1 ELSE 0 END) AS adds,
            SUM(CASE WHEN NOT {old_empty} AND {new_empty} THEN 1 ELSE 0 END) AS deletes,
            COUNT(DISTINCT alib_path) AS tracks,
            COUNT(DISTINCT album_root_from_path(alib_path)) AS albums
        FROM changelog
        WHERE {where}
        GROUP BY c
        ORDER BY total DESC
        LIMIT ?
        """.format(old_empty=old_empty, new_empty=new_empty, where=where),
        (*params, int(top_n)),
    ).fetchall():
        col_s = str(col)
        total_i = int(total or 0)
        adds_i = int(adds or 0)
        deletes_i = int(deletes or 0)
        changes_i = max(0, total_i - adds_i - deletes_i)
        tracks_i = int(tracks or 0)
        albums_i = int(albums or 0)

        retained = False
        if col_s.startswith(str(system_prefix or "__")):
            retained = True
        elif retained_columns is not None and col_s in retained_columns:
            retained = True

        by_column.append((col_s, total_i))
        by_column_breakdown.append(
            ColumnChangeBreakdown(
                column=col_s,
                total=total_i,
                adds=adds_i,
                deletes=deletes_i,
                changes=changes_i,
                tracks_touched=tracks_i,
                albums_touched=albums_i,
                retained=retained,
            )
        )

    return ChangelogSummary(
        start_ts=start_ts,
        end_ts=end_ts,
        raw_rows=raw_rows,
        total_entries=total_entries,
        noop_entries=noop_entries,
        tracks_touched=tracks_touched,
        albums_touched=albums_touched,
        enrich_entries=enrich_entries,
        clear_entries=clear_entries,
        modify_entries=modify_entries,
        by_script=by_script,
        by_column=by_column,

        by_column_breakdown=by_column_breakdown,

        noop_by_script=noop_by_script,
    )
