"""Export contributor AllMusic lookup candidates missing from amg_artists.

This script is intentionally read-only. It scans the contributor master tables
for three MNID sources:
- ``allmusic_mnid``
- ``musicbrainz_allmusic_mnid``
- ``wikimedia_allmusic_mnid``

It then emits rows whose MNIDs are missing from ``amg_artists`` and writes the
result to ``amg_lookups.tsv`` by default.

The filtering is done in SQLite so the anti-join can use the database engine's
set operations efficiently. Polars is only used for the final TSV materialization.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
import polars as pl

from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
MASTER_CONFIG_FILE = "harvest_master_data.toml"
DISAMBIGUATED_TABLE = "contributors_unified_disambiguated"
NAMESAKES_TABLE = "contributors_unified_namesakes"
AMG_TABLE = "amg_artists"
OUTPUT_COLUMNS = [
    "mnid_source",
    "source_table",
    "allmusic_mnid",
    "allmusic_artist",
    "allmusic_url",
    "allmusic_genres_json",
    "allmusic_styles_json",
]
MNID_SOURCE_COLUMNS = [
    "allmusic_mnid",
    "musicbrainz_allmusic_mnid",
    "wikimedia_allmusic_mnid",
]

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("amg_todo_list")


def _resolve_master_config_path() -> Path:
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


def _resolve_path(value: str, base_dir: Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)


def _load_paths(master_db_override: str | None, allmusic_db_override: str | None) -> tuple[str, str]:
    config_path = _resolve_master_config_path()
    cfg = tm_config.load_config(config_path=config_path)

    musicbrainz_cfg = cfg.get("musicbrainz") if isinstance(cfg, dict) else {}
    allmusic_cfg = cfg.get("allmusic") if isinstance(cfg, dict) else {}

    master_candidate = master_db_override or str(musicbrainz_cfg.get("contributors_db", "")).strip()
    if not master_candidate:
        master_candidate = tm_config.get_master_data_db_path(default=None)

    allmusic_candidate = allmusic_db_override or str(allmusic_cfg.get("metadata_db", "")).strip()
    if not allmusic_candidate:
        raise FileNotFoundError(
            "Could not resolve the AllMusic metadata database path from harvest_master_data.toml."
        )

    base_dir = config_path.parent
    return _resolve_path(master_candidate, base_dir), _resolve_path(allmusic_candidate, base_dir)


def _table_exists(conn: sqlite3.Connection, schema: str, table: str) -> bool:
    if schema == "main":
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    else:
        row = conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, schema: str, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA {schema}.table_info({tm_db.quote_ident(table)})").fetchall()
    return {str(row[1]) for row in rows if row and row[1] is not None}


def _validate_schema(master_conn: sqlite3.Connection) -> None:
    required_source_columns = {
        "allmusic_artist",
        "allmusic_url",
        "allmusic_genres_json",
        "allmusic_styles_json",
        *MNID_SOURCE_COLUMNS,
    }
    for table in (DISAMBIGUATED_TABLE, NAMESAKES_TABLE):
        if not _table_exists(master_conn, "main", table):
            raise RuntimeError(f"Missing required source table {table} in master-data DB")
        missing = required_source_columns - _table_columns(master_conn, "main", table)
        if missing:
            raise RuntimeError(
                f"Missing required columns in {table}: {', '.join(sorted(missing))}"
            )


def _build_query() -> str:
    select_template = """
        SELECT
            '{mnid_source}' AS mnid_source,
            '{source_table}' AS source_table,
            lower(trim({mnid_column})) AS allmusic_mnid,
            allmusic_artist,
            allmusic_url,
            allmusic_genres_json,
            allmusic_styles_json
        FROM {table}
        WHERE {mnid_column} IS NOT NULL
          AND trim({mnid_column}) <> ''
    """

    all_selects: list[str] = []
    for table in (DISAMBIGUATED_TABLE, NAMESAKES_TABLE):
        for mnid_column in MNID_SOURCE_COLUMNS:
            all_selects.append(
                select_template.format(
                    table=table,
                    source_table=table,
                    mnid_column=mnid_column,
                    mnid_source=mnid_column,
                )
            )

    return f"""
        WITH source_rows AS (
            {' UNION ALL '.join(all_selects)}
        ),
        missing_rows AS (
            SELECT
                source_rows.mnid_source,
                source_rows.source_table,
                source_rows.allmusic_mnid,
                source_rows.allmusic_artist,
                source_rows.allmusic_url,
                source_rows.allmusic_genres_json,
                source_rows.allmusic_styles_json
            FROM source_rows
            WHERE NOT EXISTS (
                SELECT 1
                FROM amg.{AMG_TABLE} amg
                WHERE lower(trim(amg.mnid)) = source_rows.allmusic_mnid
            )
        )
        SELECT
            mnid_source,
            source_table,
            allmusic_mnid,
            MAX(allmusic_artist) AS allmusic_artist,
            MAX(allmusic_url) AS allmusic_url,
            MAX(allmusic_genres_json) AS allmusic_genres_json,
            MAX(allmusic_styles_json) AS allmusic_styles_json
        FROM missing_rows
        GROUP BY mnid_source, source_table, allmusic_mnid
        ORDER BY mnid_source, source_table, allmusic_mnid
    """


def _load_rows(master_conn: sqlite3.Connection) -> pl.DataFrame:
    query = _build_query()
    return tm_polars_db.sqlite_to_polars(
        master_conn,
        query,
        dtype_overrides={col: pl.Utf8() for col in OUTPUT_COLUMNS},
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export AllMusic lookups for contributor MNIDs missing from amg_artists.",
    )
    parser.add_argument(
        "--master-db",
        default=None,
        help="Path to the master-data SQLite database (default: harvest_master_data.toml [musicbrainz].contributors_db).",
    )
    parser.add_argument(
        "--allmusic-db",
        default=None,
        help="Path to the AllMusic SQLite database (default: harvest_master_data.toml [allmusic].metadata_db).",
    )
    parser.add_argument(
        "--output",
        default="amg_lookups.tsv",
        help="TSV output path (default: amg_lookups.tsv in the current working directory).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    master_db_path, allmusic_db_path = _load_paths(args.master_db, args.allmusic_db)
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Connecting to master-data DB: %s", master_db_path)
    log.info("Connecting to AllMusic DB: %s", allmusic_db_path)
    master_conn = tm_db.connect(master_db_path, read_only=True)
    try:
        if not Path(allmusic_db_path).exists():
            raise FileNotFoundError(f"AllMusic metadata DB not found: {allmusic_db_path}")

        _validate_schema(master_conn)
        master_conn.execute("ATTACH DATABASE ? AS amg", (allmusic_db_path,))

        if not _table_exists(master_conn, "amg", AMG_TABLE):
            raise RuntimeError(f"Missing required table amg.{AMG_TABLE} in AllMusic DB")

        amg_columns = _table_columns(master_conn, "amg", AMG_TABLE)
        if "mnid" not in amg_columns:
            raise RuntimeError(f"Missing required column mnid in amg.{AMG_TABLE}")

        rows = _load_rows(master_conn)
        log.info("Found %d lookup rows missing from amg_artists across all MNID sources", rows.height)

        rows.write_csv(output_path, separator="\t")
        log.info("Wrote %d rows to %s", rows.height, output_path)
    finally:
        try:
            master_conn.execute("DETACH DATABASE amg")
        except Exception:
            pass
        master_conn.close()


if __name__ == "__main__":
    main()