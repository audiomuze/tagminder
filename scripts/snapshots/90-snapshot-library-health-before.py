#!/usr/bin/env python3
"""90-snapshot-library-health-before.py

Purpose:
    Capture a BEFORE snapshot of offline metadata quality aggregates.

    This snapshot is intended to be taken after import/ingestion into the staging
    DB, but before any Data Quality scripts are run.

Writes snapshot tables into the staging DB:
    - _SNAP_runs
    - _SNAP_core_tags
    - _SNAP_critical_tags

This script is part of Tagminder.

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

import argparse
import logging
from pathlib import Path

from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_snapshots

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    p = argparse.ArgumentParser(
        prog=Path(__file__).name,
        description="Capture BEFORE metadata-quality snapshot aggregates.",
    )
    p.add_argument("--db", default=None, help="Path to staging SQLite database")
    p.add_argument("--notes", default=None, help="Optional note stored with the snapshot")
    args = p.parse_args(argv)

    db_path = args.db or tm_config.get_db_path(default=None)
    db_path = str(Path(db_path).resolve())

    keep_cols, critical_cols = tm_snapshots.load_config_columns()

    conn = tm_db.connect(db_path, wal=False)
    try:
        tm_snapshots.ensure_snapshot_tables(conn)

        fp = tm_snapshots.get_changelog_fingerprint(conn)
        run_id = tm_snapshots.create_run(conn, label="before", notes=args.notes, fingerprint=fp)

        core_cov = tm_snapshots.compute_coverage(conn, columns=keep_cols)
        crit_cov = tm_snapshots.compute_coverage(conn, columns=critical_cols)

        tm_snapshots.write_coverage_rows(
            conn,
            table="_SNAP_core_tags",
            run_id=run_id,
            columns_in_order=keep_cols,
            coverage=core_cov,
        )
        tm_snapshots.write_coverage_rows(
            conn,
            table="_SNAP_critical_tags",
            run_id=run_id,
            columns_in_order=critical_cols,
            coverage=crit_cov,
        )

        conn.commit()

        logging.info("Captured BEFORE snapshot: %s", run_id)
        logging.info("Changelog fingerprint: max_ts=%s rows=%d", fp.max_timestamp, fp.row_count)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
