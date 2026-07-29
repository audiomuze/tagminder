"""
Purpose:
    Reset `alib.__sqlmodded` after a successful export.

    This script performs one operation:
        UPDATE alib SET __sqlmodded = NULL WHERE __sqlmodded IS NOT NULL

    The staging database is modified in-place.

This script is part of Tagminder.

SQLite tables referenced:
    - alib

Author: audiomuze
Last updated: 2026-04-15
"""

from __future__ import annotations

import logging

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


def main() -> int:
    _configure_logging()

    conn, db_path, script_name, timestamp = tm_run.open_db(ensure_changelog=False)
    logging.info("Resetting alib.__sqlmodded to NULL where set")
    logging.info("DB: %s", db_path)

    try:
        with tm_db.transaction(conn):
            cur = conn.execute(
                "UPDATE alib SET __sqlmodded = NULL WHERE __sqlmodded IS NOT NULL"
            )
            updated_rows = int(cur.rowcount if cur.rowcount is not None else 0)

        logging.info("Done: cleared __sqlmodded on %d row(s)", updated_rows)
        logging.info("Script: %s at %s", script_name, timestamp)
        logging.info("Exit code: 0")
        return 0

    except Exception as e:
        logging.error("Reset failed: %s", e)
        logging.info("Exit code: 1")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
