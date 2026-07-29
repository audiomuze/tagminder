"""
Purpose:
    Apply metadata updates from a source Tagminder DB to a target Tagminder DB
    by joining on `track_uuid`.

    The script compares selected columns between source/target `alib` rows with
    matching `track_uuid`, updates only changed target values, increments
    `__sqlmodded` by the number of changed fields per row, and writes field-level
    changelog entries in the target DB.

    This is intended for path-independent synchronization when files were moved
    or renamed between databases.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-07-29
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, cast

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
EXCLUDED_DEFAULT_COLUMNS = {"track_uuid", "__sqlmodded"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize selected metadata columns from source DB to target DB "
            "using track_uuid as stable identity."
        )
    )
    parser.add_argument("--source-db", required=True, help="Path to source Tagminder DB")
    parser.add_argument("--target-db", required=True, help="Path to target Tagminder DB")
    parser.add_argument(
        "--columns",
        default="",
        help=(
            "Comma-separated column list to sync. If omitted, uses "
            "[cleanup].keep_columns filtered to columns shared by source/target."
        ),
    )
    parser.add_argument(
        "--include-system-columns",
        action="store_true",
        help="Allow syncing columns starting with '__' (still excludes track_uuid and __sqlmodded).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and report proposed changes without writing updates.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Batch size for applying changed rows after vectorized diffing.",
    )
    return parser.parse_args()


def _validate_paths(source_db: str, target_db: str) -> tuple[str, str]:
    source = str(Path(source_db).expanduser().resolve())
    target = str(Path(target_db).expanduser().resolve())

    if source == target:
        raise ValueError("Source and target DB paths must be different")
    if not Path(source).exists():
        raise FileNotFoundError(f"Source DB not found: {source}")
    if not Path(target).exists():
        raise FileNotFoundError(f"Target DB not found: {target}")

    return source, target


def _parse_columns_arg(columns_arg: str) -> list[str]:
    if not columns_arg.strip():
        return []

    seen: set[str] = set()
    out: list[str] = []
    for token in columns_arg.split(","):
        col = token.strip()
        if not col or col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _resolve_columns(
    source_cols: set[str],
    target_cols: set[str],
    requested_cols: list[str],
    include_system_columns: bool,
) -> list[str]:
    common = source_cols & target_cols
    if "track_uuid" not in common:
        raise ValueError("Both source and target alib tables must include track_uuid")
    if "__sqlmodded" not in target_cols:
        raise ValueError("Target alib table must include __sqlmodded")

    if requested_cols:
        missing = [c for c in requested_cols if c not in common]
        if missing:
            raise ValueError(
                "Requested columns are not shared by source/target alib: "
                + ", ".join(sorted(missing))
            )
        columns = list(requested_cols)
    else:
        cfg = tm_config.load_config()
        cleanup = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
        keep_columns = cleanup.get("keep_columns", []) if isinstance(cleanup, dict) else []

        if isinstance(keep_columns, list) and keep_columns:
            columns = [str(c) for c in keep_columns if isinstance(c, str) and c in common]
        else:
            columns = sorted(common)

    filtered: list[str] = []
    for col in columns:
        if col in EXCLUDED_DEFAULT_COLUMNS:
            continue
        if (not include_system_columns) and col.startswith("__"):
            continue
        filtered.append(col)

    if not filtered:
        raise ValueError("No columns selected to sync after filtering")

    return filtered


def _qid(name: str) -> str:
    return tm_db.quote_ident(name)


def _count_duplicate_track_uuids(conn: sqlite3.Connection, table_expr: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT track_uuid
            FROM {table_expr}
            WHERE track_uuid IS NOT NULL AND TRIM(track_uuid) != ''
            GROUP BY track_uuid
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0] if row else 0)


def _count_source_only_uuids(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        WITH src_unique AS (
            SELECT track_uuid, MIN(rowid) AS source_rowid
            FROM src.alib
            WHERE track_uuid IS NOT NULL AND TRIM(track_uuid) != ''
            GROUP BY track_uuid
            HAVING COUNT(*) = 1
        )
        SELECT COUNT(*)
        FROM src_unique su
        LEFT JOIN alib t ON t.track_uuid = su.track_uuid
        WHERE t.rowid IS NULL
        """
    ).fetchone()
    return int(row[0] if row else 0)


def _build_changed_rows_query(columns: list[str]) -> str:
    t_select = ", ".join(f"t.{_qid(c)} AS {_qid('t__' + c)}" for c in columns)
    s_select = ", ".join(f"s.{_qid(c)} AS {_qid('s__' + c)}" for c in columns)

    return f"""
        WITH src_unique AS (
            SELECT track_uuid, MIN(rowid) AS source_rowid
            FROM src.alib
            WHERE track_uuid IS NOT NULL AND TRIM(track_uuid) != ''
            GROUP BY track_uuid
            HAVING COUNT(*) = 1
        )
        SELECT
            t.rowid AS rowid,
            t.__path AS alib_path,
            t.track_uuid AS track_uuid,
            COALESCE(t.__sqlmodded, 0) AS target_sqlmodded,
            {t_select},
            {s_select}
        FROM alib t
        JOIN src_unique su ON su.track_uuid = t.track_uuid
        JOIN src.alib s ON s.rowid = su.source_rowid
        ORDER BY t.rowid
    """


def _categorical_candidate_columns(columns: list[str]) -> list[str]:
    """Return columns likely to benefit from Categorical encoding in compare phase."""
    categorical_markers = (
        "performer",
        "artist",
        "composer",
        "conductor",
        "orchestra",
        "ensemble",
        "musicbrainz",
        "mbid",
    )
    out: list[str] = []
    for col in columns:
        name = col.lower()
        if any(marker in name for marker in categorical_markers):
            out.append(col)
    return out


def _diff_flag_expr(left_col: str, right_col: str, out_col: str) -> pl.Expr:
    left = pl.col(left_col).cast(pl.Utf8, strict=False)
    right = pl.col(right_col).cast(pl.Utf8, strict=False)
    return (
        (left.is_null() & right.is_not_null())
        | (left.is_not_null() & right.is_null())
        | (left.fill_null("") != right.fill_null(""))
    ).alias(out_col)


def _sync_by_track_uuid(
    conn: sqlite3.Connection,
    *,
    columns: list[str],
    dry_run: bool,
    batch_size: int,
) -> dict[str, Any]:
    query = _build_changed_rows_query(columns)
    update_sql = tm_db.build_update_sql(table="alib", set_cols=columns)

    dtype_overrides = cast(
        dict[str, pl.DataType],
        {"rowid": pl.Int64, "target_sqlmodded": pl.Int16},
    )

    df = tm_polars_db.sqlite_to_polars(
        conn,
        query,
        dtype_overrides=dtype_overrides,
    )

    if df.is_empty():
        return {
            "rows_examined": 0,
            "rows_updated": 0,
            "field_change_counts": {},
            "sample": [],
        }

    with pl.StringCache():
        categorical_cols = _categorical_candidate_columns(columns)
        if categorical_cols:
            cast_exprs: list[pl.Expr] = []
            for col in categorical_cols:
                cast_exprs.append(pl.col(f"t__{col}").cast(pl.Categorical))
                cast_exprs.append(pl.col(f"s__{col}").cast(pl.Categorical))
            df = df.with_columns(cast_exprs)

        change_flag_exprs = [
            _diff_flag_expr(f"t__{col}", f"s__{col}", f"__chg__{col}")
            for col in columns
        ]
        df = df.with_columns(change_flag_exprs)

        df = df.with_columns(
            [
                pl.sum_horizontal([pl.col(f"__chg__{col}").cast(pl.Int16) for col in columns])
                .cast(pl.Int16)
                .alias("__changes_count"),
                pl.concat_list(
                    [
                        pl.when(pl.col(f"__chg__{col}"))
                        .then(pl.lit(col))
                        .otherwise(None)
                        for col in columns
                    ]
                )
                .list.drop_nulls()
                .alias("__changed_cols"),
            ]
        )

        impacted = df.filter(pl.col("__changes_count") > 0)

    script = tm_db.script_name()
    timestamp = tm_db.utc_now_iso()

    rows_examined = int(df.height)
    rows_updated = 0
    field_change_counts: Counter[str] = Counter()
    sample: list[dict[str, Any]] = []

    if not impacted.is_empty():
        summed = impacted.select(
            [pl.col(f"__chg__{col}").sum().alias(col) for col in columns]
        ).to_dicts()[0]
        for col, count in summed.items():
            value = int(count or 0)
            if value > 0:
                field_change_counts[col] = value

        for row in impacted.select(["rowid", "track_uuid", "__changed_cols"]).head(10).iter_rows(
            named=True
        ):
            sample.append(
                {
                    "rowid": int(row["rowid"]),
                    "track_uuid": row.get("track_uuid"),
                    "changes": [str(c) for c in (row.get("__changed_cols") or [])],
                }
            )

    if (not dry_run) and (not impacted.is_empty()):
        with tm_db.transaction(conn):
            cursor = conn.cursor()
            changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)

            for start in range(0, impacted.height, batch_size):
                chunk = impacted.slice(start, batch_size)
                for record in chunk.iter_rows(named=True):
                    rowid = int(record["rowid"])
                    alib_path = str(record.get("alib_path") or rowid)
                    old_sqlmodded = int(record.get("target_sqlmodded") or 0)

                    changes = [
                        (col, record.get(f"t__{col}"), record.get(f"s__{col}"))
                        for col in columns
                        if bool(record.get(f"__chg__{col}"))
                    ]
                    if not changes:
                        continue

                    update_values = [record.get(f"s__{col}") for col in columns]
                    new_sqlmodded = old_sqlmodded + int(record.get("__changes_count") or 0)

                    cursor.execute(update_sql, (*update_values, new_sqlmodded, rowid))
                    changelog.add(alib_path=alib_path, changes=changes)
                    rows_updated += 1

            changelog.flush(cursor)

    return {
        "rows_examined": rows_examined,
        "rows_updated": rows_updated,
        "field_change_counts": dict(sorted(field_change_counts.items())),
        "sample": sample,
    }


def _run_target_cleanup_policy(target_db: str) -> None:
    """Run the canonical null-unauthorised-tags cleanup on target DB."""
    cleanup_script = (
        Path(__file__).resolve().parents[1] / "pipeline" / "01-null-unauthorised-tags.py"
    )
    if not cleanup_script.exists():
        raise FileNotFoundError(f"Cleanup script not found: {cleanup_script}")

    cmd = [sys.executable, str(cleanup_script), "--db", target_db]
    logging.info("Running cleanup policy on target DB via: %s", cleanup_script)
    subprocess.run(cmd, check=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = _parse_args()

    try:
        source_db, target_db = _validate_paths(args.source_db, args.target_db)
    except Exception as exc:
        logging.error(str(exc))
        return

    if args.batch_size <= 0:
        logging.error("--batch-size must be > 0")
        return

    source_conn = tm_db.connect(source_db, read_only=True)
    target_conn = tm_db.connect(target_db)

    try:
        tm_db.require_table_columns(source_conn, "alib", ("track_uuid",))
        tm_db.require_table_columns(target_conn, "alib", ("track_uuid", "__sqlmodded", "__path"))
        tm_db.ensure_changelog_table(target_conn)

        source_cols = set(tm_db.table_columns(source_conn, "alib"))
        target_cols = set(tm_db.table_columns(target_conn, "alib"))
        requested_cols = _parse_columns_arg(args.columns)
        columns = _resolve_columns(
            source_cols,
            target_cols,
            requested_cols,
            args.include_system_columns,
        )

        logging.info("Source DB: %s", source_db)
        logging.info("Target DB: %s", target_db)
        logging.info("Columns selected (%d): %s", len(columns), ", ".join(columns))

        target_conn.execute("ATTACH DATABASE ? AS src", (source_db,))
        try:
            src_dup = _count_duplicate_track_uuids(target_conn, "src.alib")
            tgt_dup = _count_duplicate_track_uuids(target_conn, "alib")
            source_only = _count_source_only_uuids(target_conn)

            if src_dup:
                logging.warning(
                    "Source has %d duplicate non-empty track_uuid value(s); only unique source UUIDs are eligible for sync.",
                    src_dup,
                )
            if tgt_dup:
                logging.warning(
                    "Target has %d duplicate non-empty track_uuid value(s); all matching target rows will be updated.",
                    tgt_dup,
                )

            logging.info("Source-only unique UUIDs (no target match): %d", source_only)

            result = _sync_by_track_uuid(
                target_conn,
                columns=columns,
                dry_run=bool(args.dry_run),
                batch_size=int(args.batch_size),
            )
        finally:
            target_conn.execute("DETACH DATABASE src")

        rows_examined = int(result["rows_examined"])
        rows_updated = int(result["rows_updated"])
        field_counts = result["field_change_counts"]
        sample = result["sample"]

        if args.dry_run:
            logging.info("Dry-run complete. Candidate changed rows: %d", rows_examined)
        else:
            logging.info(
                "Sync complete. Updated %d row(s) with changelog + __sqlmodded increments.",
                rows_updated,
            )
            _run_target_cleanup_policy(target_db)

        if field_counts:
            logging.info("Field change counts:")
            for col, count in field_counts.items():
                logging.info("  %s: %d", col, int(count))
        else:
            logging.info("No field-level differences found for selected columns.")

        if sample:
            logging.info("Sample affected rows:")
            for item in sample:
                logging.info(
                    "  rowid=%s track_uuid=%s changes=%s",
                    item["rowid"],
                    item["track_uuid"],
                    ",".join(item["changes"]),
                )

    except Exception as exc:
        logging.error("Cross-db sync failed: %s", exc)
        raise
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
