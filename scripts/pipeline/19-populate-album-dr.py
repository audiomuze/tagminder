"""
Purpose:
    Populate `album_dr` values in `alib` using a per-album DR scores file.

    The DR scores file is pipe-delimited and has one row per album folder:
        <__dirpath>|DR<n>

    For each `alib` row whose `__dirpath` appears in the file, this script:
    - strips the leading 'DR' and writes the remaining numeric score into `album_dr`
    - increments `__sqlmodded`
    - logs the change to `changelog`

    Rows whose `album_dr` already matches the numeric score are left unchanged.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-26
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path
import logging
import sqlite3

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_run

def _configure_logging() -> None:
    logging.basicConfig(
        level=tm_config.get_log_level(),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def _chunks(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _dr_scores_path_from_toml(*, default: str | None = None) -> str | None:
    cfg = tm_config.load_config()
    paths_cfg = cfg.get("paths", {}) if isinstance(cfg, dict) else {}
    path = paths_cfg.get("dr_scores") if isinstance(paths_cfg, dict) else None
    if path:
        return str(path)
    return default


def load_dr_scores(path: str | Path) -> dict[str, str]:
    """Load DR scores mapping: __dirpath -> numeric string score."""

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    mapping: dict[str, str] = {}
    with p.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if "|" not in line:
                logging.warning("Skipping invalid DR row (missing '|'): %r", line)
                continue

            dirpath_raw, score_raw = line.split("|", 1)
            dirpath = dirpath_raw.strip()
            score_token = score_raw.strip()

            if not dirpath:
                logging.warning("Skipping invalid DR row (empty dirpath): %r", line)
                continue

            token_upper = score_token.upper()
            if not token_upper.startswith("DR"):
                logging.warning("Skipping invalid DR row (missing 'DR' prefix): %r", line)
                continue

            score = score_token[2:].strip()
            if not score.isdigit():
                logging.warning("Skipping invalid DR row (non-numeric score): %r", line)
                continue

            mapping[dirpath] = score

    return mapping


def fetch_rows_for_dirpaths(
    conn: sqlite3.Connection,
    *,
    alib_table: str,
    dirpaths: list[str],
) -> list[tuple[int, str, str, str | None, int]]:
    """Return (rowid, __path, __dirpath, album_dr, __sqlmodded) for matching dirpaths."""

    if not dirpaths:
        return []

    out: list[tuple[int, str, str, str | None, int]] = []
    chunk_size = 900  # SQLite variable limit safety

    quoted_table = tm_db.quote_ident(alib_table)

    for chunk in _chunks(dirpaths, chunk_size):
        placeholders = ",".join(["?"] * len(chunk))
        cur = conn.execute(
            f"""
            SELECT
                rowid,
                __path,
                __dirpath,
                album_dr,
                COALESCE(__sqlmodded, 0) AS __sqlmodded
            FROM {quoted_table}
            WHERE __dirpath IN ({placeholders})
            ORDER BY __path
            """.strip(),
            chunk,
        )
        out.extend(cur.fetchall())

    return out


def compute_updates(
    rows: list[tuple[int, str, str, str | None, int]],
    dr_by_dirpath: dict[str, str],
) -> pl.DataFrame:
    """Compute the subset of rows that need album_dr updates (vectorized)."""

    if not rows or not dr_by_dirpath:
        return pl.DataFrame(
            {
                "rowid": pl.Series([], dtype=pl.Int64),
                "__path": pl.Series([], dtype=pl.Utf8),
                "album_dr": pl.Series([], dtype=pl.Utf8),
                "album_dr_new": pl.Series([], dtype=pl.Utf8),
                "__sqlmodded": pl.Series([], dtype=pl.Int64),
            }
        )

    # NOTE: We must avoid Polars type inference on rows, because album_dr in the
    # DB may contain non-numeric strings (e.g. "14\\12") which can crash the
    # builder if earlier rows look numeric.
    rowids: list[int] = []
    paths: list[str] = []
    dirpaths: list[str] = []
    album_drs: list[str | None] = []
    sqlmoddeds: list[int] = []

    for rowid, path, dirpath, album_dr, sqlmodded in rows:
        rowids.append(int(rowid))
        paths.append(str(path))
        dirpaths.append(str(dirpath))
        album_drs.append(None if album_dr is None else str(album_dr))
        sqlmoddeds.append(int(sqlmodded))

    df_rows = pl.DataFrame(
        {
            "rowid": pl.Series(rowids, dtype=pl.Int64),
            "__path": pl.Series(paths, dtype=pl.Utf8),
            "__dirpath": pl.Series(dirpaths, dtype=pl.Utf8),
            "album_dr": pl.Series(album_drs, dtype=pl.Utf8),
            "__sqlmodded": pl.Series(sqlmoddeds, dtype=pl.Int64),
        }
    )

    df_dr = pl.DataFrame(
        {
            "__dirpath": list(dr_by_dirpath.keys()),
            "album_dr_new": list(dr_by_dirpath.values()),
        }
    ).with_columns(
        [
            pl.col("__dirpath").cast(pl.Utf8),
            pl.col("album_dr_new").cast(pl.Utf8),
        ]
    )

    joined = df_rows.join(df_dr, on="__dirpath", how="inner")

    old_norm = pl.col("album_dr").fill_null("").cast(pl.Utf8).str.strip_chars()
    new_norm = pl.col("album_dr_new").fill_null("").cast(pl.Utf8).str.strip_chars()

    return joined.filter(old_norm != new_norm).select(
        ["rowid", "__path", "album_dr", "album_dr_new", "__sqlmodded"]
    )


def write_updates(
    conn: sqlite3.Connection,
    *,
    alib_table: str,
    updates_df: pl.DataFrame,
    script: str,
    timestamp: str,
) -> int:
    """Update album_dr for rows that differ from desired value and log to changelog."""

    if updates_df.is_empty():
        return 0

    update_sql = tm_db.build_update_sql(table=alib_table, set_cols=["album_dr"])

    updates = 0
    cursor = conn.cursor()

    with tm_db.transaction(conn):
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)

        for record in updates_df.to_dicts():
            rowid = int(record["rowid"])
            alib_path = str(record["__path"])
            old_value = record.get("album_dr")
            new_value = str(record.get("album_dr_new") or "").strip()
            new_sqlmodded = int(record.get("__sqlmodded") or 0) + 1

            changelog.add(
                alib_path=alib_path,
                changes=[("album_dr", old_value, new_value)],
            )

            cursor.execute(update_sql, (new_value, new_sqlmodded, rowid))
            updates += 1

        changelog.flush(cursor)

    return updates


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        prog="19-populate-album-dr.py",
        description="Populate album_dr for each __dirpath using a DR scores file.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database (default: tagminder.toml [db].path)",
    )

    args = parser.parse_args(argv)

    dr_scores_path = _dr_scores_path_from_toml(default=None)
    if not dr_scores_path:
        logging.error("DR scores path is not configured. Set tagminder.toml [paths].dr_scores")
        return 2

    if not Path(dr_scores_path).exists():
        logging.error("DR scores file not found: %s", dr_scores_path)
        return 2

    try:
        dr_by_dirpath = load_dr_scores(dr_scores_path)
    except FileNotFoundError:
        logging.error("DR scores file not found: %s", dr_scores_path)
        return 2

    if not dr_by_dirpath:
        logging.info("No DR rows loaded from %s; nothing to do.", dr_scores_path)
        return 0

    try:
        conn, db_path, script, timestamp = tm_run.open_db(
            db_path=args.db,
            require_exists=True,
            ensure_changelog=True,
        )
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return 1
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return 1

    try:
        cfg = tm_config.load_config()
        db_cfg = cfg.get("db", {}) if isinstance(cfg, dict) else {}
        alib_table = str(db_cfg.get("alib_table") or "alib") if isinstance(db_cfg, dict) else "alib"

        if not tm_db.table_exists(conn, alib_table):
            logging.error(f"Required table '{alib_table}' not found in database")
            conn.close()
            return 1

        if not tm_db.table_exists(conn, alib_table):
            raise RuntimeError(
                f"Missing required table {alib_table!r}. "
                "Run tags2db import to create/populate the staging database before running this step."
            )

        dirpaths = sorted(dr_by_dirpath.keys())
        logging.info("DR scores: %s (loaded %d dirpaths)", dr_scores_path, len(dirpaths))
        logging.info("DB: %s", db_path)

        rows = fetch_rows_for_dirpaths(conn, alib_table=alib_table, dirpaths=dirpaths)
        if not rows:
            logging.info("No rows matched any DR dirpaths; nothing to do.")
            return 0

        updates_df = compute_updates(rows, dr_by_dirpath)
        updates = write_updates(
            conn,
            alib_table=alib_table,
            updates_df=updates_df,
            script=script,
            timestamp=timestamp,
        )

        if updates:
            logging.info("Updated %d album_dr rows and logged changes.", updates)
        else:
            logging.info("No album_dr changes needed.")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
