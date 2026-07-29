"""
Purpose:
    Retire synthetic MBIDs when a unique real MusicBrainz candidate becomes
    available for the same contributor + context.

    Workflow:
    - Read synthetic assignments from `_USR_disambiguation_decisions`
    - Re-resolve candidates using normalized name + context against:
        - contributors_unified_disambiguated
        - contributors_unified_namesakes
    - In dry-run mode (default), report proposed retirements and impacted rows.
    - In apply mode, replace synthetic MBID tokens across all `musicbrainz_*id`
      columns in `alib`, increment `__sqlmodded`, write `changelog`, and update
      `_USR_disambiguation_decisions`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog
    - _USR_disambiguation_decisions
    - contributors_unified_disambiguated
    - contributors_unified_namesakes

Author: audiomuze
Last updated: 2026-07-29
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import unicodedata
from collections import Counter
from typing import Any

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
USER_DISAMBIGUATION_TABLE = "_USR_disambiguation_decisions"
DISAMBIGUATED_TABLE = "contributors_unified_disambiguated"
NAMESAKES_TABLE = "contributors_unified_namesakes"
DECISION_SOURCE_RETIRED = "retired_to_real"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Retire synthetic MBIDs by matching contributor name+context to unique "
            "real MBID candidates. Dry-run by default."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to alib and decisions table. Default is dry-run.",
    )
    parser.add_argument(
        "--db",
        default="",
        help="Path to staging DB (defaults to [db].path from tagminder.toml).",
    )
    parser.add_argument(
        "--master-db",
        default="",
        help=(
            "Path to master-data DB for contributor lookup + decisions table "
            "(defaults to [master_data].path)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of proposals to apply/report (0 = no limit).",
    )
    return parser.parse_args()


def _normalize_string(text: str) -> str:
    if not isinstance(text, str):
        return ""
    value = unicodedata.normalize("NFKD", text)
    value = value.lower()
    value = value.replace('"', "")
    value = " ".join(value.split())
    return value


def _empty_proposals_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "contributor_name": pl.Series([], dtype=pl.Utf8),
            "albumartist_context": pl.Series([], dtype=pl.Utf8),
            "synthetic_mbid": pl.Series([], dtype=pl.Utf8),
            "replacement_mbid": pl.Series([], dtype=pl.Utf8),
        }
    )


def _empty_decisions_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "norm_name": pl.Series([], dtype=pl.Utf8),
            "norm_context": pl.Series([], dtype=pl.Utf8),
            "assigned_mbid": pl.Series([], dtype=pl.Categorical),
        }
    )


def _load_real_candidate_lookups(
    master_conn: sqlite3.Connection,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    disambig_df = tm_polars_db.sqlite_to_polars(
        master_conn,
        f"""
        SELECT merge_key_mbid AS mbid, lpreferred__artist_name AS contributor_name
        FROM {DISAMBIGUATED_TABLE}
        WHERE merge_key_mbid IS NOT NULL
          AND TRIM(merge_key_mbid) != ''
          AND lpreferred__artist_name IS NOT NULL
          AND TRIM(lpreferred__artist_name) != ''
        """,
    ).with_columns(
        [
            pl.col("contributor_name")
            .map_elements(_normalize_string, return_dtype=pl.Utf8)
            .alias("norm_name"),
            pl.col("mbid").cast(pl.Utf8).str.strip_chars().cast(pl.Categorical).alias("mbid"),
        ]
    ).filter(
        pl.col("norm_name") != ""
    ).select(
        ["norm_name", "mbid"]
    ).unique()

    namesakes_df = tm_polars_db.sqlite_to_polars(
        master_conn,
        f"""
        SELECT
            merge_key_mbid AS mbid,
            preferred__artist_name AS contributor_name,
            musicbrainz_disambiguation
        FROM {NAMESAKES_TABLE}
        WHERE merge_key_mbid IS NOT NULL
          AND TRIM(merge_key_mbid) != ''
          AND preferred__artist_name IS NOT NULL
          AND TRIM(preferred__artist_name) != ''
        """,
    ).with_columns(
        [
            pl.col("contributor_name")
            .map_elements(_normalize_string, return_dtype=pl.Utf8)
            .alias("norm_name"),
            pl.col("musicbrainz_disambiguation")
            .map_elements(_normalize_string, return_dtype=pl.Utf8)
            .alias("disambig_norm"),
            pl.col("mbid").cast(pl.Utf8).str.strip_chars().cast(pl.Categorical).alias("mbid"),
        ]
    ).filter(
        pl.col("norm_name") != ""
    ).select(
        ["norm_name", "disambig_norm", "mbid"]
    ).unique()

    real_mbid_df = pl.concat(
        [
            disambig_df.select(["mbid"]),
            namesakes_df.select(["mbid"]),
        ],
        how="vertical_relaxed",
    ).unique()

    return disambig_df, namesakes_df, real_mbid_df


def _load_synthetic_decisions(
    master_conn: sqlite3.Connection,
    real_mbid_df: pl.DataFrame,
) -> pl.DataFrame:
    if not tm_db.table_exists(master_conn, USER_DISAMBIGUATION_TABLE):
        return _empty_decisions_df()

    decisions_df = tm_polars_db.sqlite_to_polars(
        master_conn,
        f"""
        SELECT contributor_name, albumartist_context, assigned_mbid
        FROM {USER_DISAMBIGUATION_TABLE}
        WHERE assigned_mbid IS NOT NULL
          AND TRIM(assigned_mbid) != ''
        """,
    ).with_columns(
        [
            pl.col("contributor_name")
            .map_elements(_normalize_string, return_dtype=pl.Utf8)
            .alias("norm_name"),
            pl.col("albumartist_context")
            .map_elements(_normalize_string, return_dtype=pl.Utf8)
            .alias("norm_context"),
            pl.col("assigned_mbid")
            .cast(pl.Utf8)
            .str.strip_chars()
            .cast(pl.Categorical)
            .alias("assigned_mbid"),
        ]
    ).filter(
        (pl.col("norm_name") != "")
        & (pl.col("norm_context") != "")
        & (pl.col("assigned_mbid").cast(pl.Utf8).str.len_bytes() == 36)
        & (pl.col("assigned_mbid").cast(pl.Utf8).str.slice(14, 1) == "5")
    ).select(
        ["norm_name", "norm_context", "assigned_mbid"]
    ).unique()

    # Keep only synthetic-looking MBIDs that are not known real MBIDs.
    if decisions_df.is_empty():
        return decisions_df

    return decisions_df.join(
        real_mbid_df,
        left_on="assigned_mbid",
        right_on="mbid",
        how="anti",
    )


def _build_proposals(
    synthetic_decisions: pl.DataFrame,
    disambig_df: pl.DataFrame,
    namesakes_df: pl.DataFrame,
) -> tuple[pl.DataFrame, Counter[str]]:
    if synthetic_decisions.is_empty():
        return _empty_proposals_df(), Counter()

    disambig_candidates = synthetic_decisions.join(
        disambig_df,
        on="norm_name",
        how="inner",
    ).select(
        [
            "norm_name",
            "norm_context",
            "assigned_mbid",
            pl.col("mbid").alias("replacement_mbid"),
        ]
    )

    namesake_candidates = synthetic_decisions.filter(
        ~pl.col("norm_context").str.starts_with("__album__:")
    ).join(
        namesakes_df,
        on="norm_name",
        how="inner",
    ).filter(
        (pl.col("norm_context") != "")
        & pl.col("disambig_norm").str.contains(pl.col("norm_context"), literal=True)
    ).select(
        [
            "norm_name",
            "norm_context",
            "assigned_mbid",
            pl.col("mbid").alias("replacement_mbid"),
        ]
    )

    all_candidates = pl.concat(
        [disambig_candidates, namesake_candidates],
        how="vertical_relaxed",
    ).unique()

    if all_candidates.is_empty():
        stats = Counter()
        stats["no_match"] = int(synthetic_decisions.height)
        return _empty_proposals_df(), stats

    candidate_summary = all_candidates.group_by(
        ["norm_name", "norm_context", "assigned_mbid"]
    ).agg(
        [
            pl.col("replacement_mbid").n_unique().alias("candidate_count"),
            pl.col("replacement_mbid").cast(pl.Utf8).sort().first().alias("replacement_mbid"),
        ]
    )

    proposals = candidate_summary.filter(
        (pl.col("candidate_count") == 1)
        & (pl.col("replacement_mbid") != pl.col("assigned_mbid").cast(pl.Utf8))
    ).select(
        [
            pl.col("norm_name").alias("contributor_name"),
            pl.col("norm_context").alias("albumartist_context"),
            pl.col("assigned_mbid").cast(pl.Utf8).alias("synthetic_mbid"),
            pl.col("replacement_mbid").cast(pl.Utf8).alias("replacement_mbid"),
        ]
    )

    matched_any = synthetic_decisions.join(
        candidate_summary.select(["norm_name", "norm_context", "assigned_mbid"]),
        on=["norm_name", "norm_context", "assigned_mbid"],
        how="inner",
    )

    ambiguous_count = candidate_summary.filter(pl.col("candidate_count") > 1).height
    no_match_count = synthetic_decisions.height - matched_any.height

    stats = Counter()
    stats["retirable_unique_match"] = proposals.height
    stats["ambiguous"] = ambiguous_count
    stats["no_match"] = no_match_count

    return proposals, stats


def _get_musicbrainz_id_columns(staging_conn: sqlite3.Connection) -> list[str]:
    cols = tm_db.table_columns(staging_conn, "alib")
    return [c for c in cols if c.startswith("musicbrainz_") and c.endswith("id")]


def _scan_alib_impacts_vectorized(
    staging_conn: sqlite3.Connection,
    replacement_map: dict[str, str],
    mbid_columns: list[str],
    delimiter: str,
) -> tuple[pl.DataFrame, Counter[str]]:
    if not replacement_map or not mbid_columns:
        return pl.DataFrame(), Counter()

    column_sql = ", ".join([tm_db.quote_ident(c) for c in mbid_columns])
    nonempty_predicate = " OR ".join(
        f"({tm_db.quote_ident(c)} IS NOT NULL AND TRIM({tm_db.quote_ident(c)}) != '')"
        for c in mbid_columns
    )

    alib_df = tm_polars_db.sqlite_to_polars(
        staging_conn,
        (
            f"SELECT rowid, __path, COALESCE(__sqlmodded, 0) AS __sqlmodded, {column_sql} "
            f"FROM alib WHERE {nonempty_predicate}"
        ),
        dtype_overrides={"rowid": pl.Int64, "__sqlmodded": pl.Int16},
    )

    if alib_df.is_empty():
        return alib_df, Counter()

    new_value_exprs: list[pl.Expr] = []
    changed_flag_exprs: list[pl.Expr] = []

    for col in mbid_columns:
        replacement_expr = pl.element().str.strip_chars()
        for old_mbid, new_mbid in replacement_map.items():
            replacement_expr = pl.when(replacement_expr == old_mbid).then(new_mbid).otherwise(replacement_expr)

        new_col = f"__new__{col}"
        changed_col = f"__chg__{col}"

        transformed = (
            pl.col(col)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.split(delimiter)
            .list.eval(replacement_expr)
            .list.join(delimiter)
        )

        new_value_exprs.append(
            pl.when(pl.col(col).is_null() | (pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars() == ""))
            .then(pl.col(col))
            .otherwise(transformed)
            .alias(new_col)
        )

        changed_flag_exprs.append(
            (pl.col(col).fill_null("") != pl.col(new_col).fill_null(""))
            .alias(changed_col)
        )

    scanned = alib_df.with_columns(new_value_exprs).with_columns(changed_flag_exprs)

    changed_count_expr = pl.sum_horizontal(
        [pl.col(f"__chg__{c}").cast(pl.Int16) for c in mbid_columns]
    ).alias("__changes_count")

    scanned = scanned.with_columns(changed_count_expr)
    impacted = scanned.filter(pl.col("__changes_count") > 0)

    field_counts = Counter()
    if not impacted.is_empty():
        sums = impacted.select(
            [pl.col(f"__chg__{c}").sum().alias(c) for c in mbid_columns]
        ).to_dicts()[0]
        for col, count in sums.items():
            value = int(count or 0)
            if value > 0:
                field_counts[col] = value

    return impacted, field_counts


def _apply_alib_replacements(
    staging_conn: sqlite3.Connection,
    impacted_rows: pl.DataFrame,
    mbid_columns: list[str],
) -> tuple[int, int]:
    if impacted_rows.is_empty():
        return 0, 0

    tm_db.ensure_changelog_table(staging_conn)

    timestamp = tm_db.utc_now_iso()
    script = tm_db.script_name()
    rows_updated = 0
    field_updates_total = 0

    with tm_db.transaction(staging_conn):
        cursor = staging_conn.cursor()
        changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)

        for row in impacted_rows.iter_rows(named=True):
            rowid = int(row["rowid"])
            alib_path = str(row.get("__path") or rowid)
            old_sqlmodded = int(row.get("__sqlmodded") or 0)

            changed_cols = [c for c in mbid_columns if bool(row.get(f"__chg__{c}"))]
            if not changed_cols:
                continue

            changes: list[tuple[str, Any, Any]] = []
            values: list[Any] = []
            for col in changed_cols:
                old_val = row.get(col)
                new_val = row.get(f"__new__{col}")
                values.append(new_val)
                changes.append((col, old_val, new_val))

            update_sql = tm_db.build_update_sql(
                table="alib",
                set_cols=changed_cols,
                where_col="rowid",
                sqlmodded_col="__sqlmodded",
            )
            new_sqlmodded = old_sqlmodded + len(changed_cols)
            cursor.execute(update_sql, [*values, new_sqlmodded, rowid])

            changelog.add(alib_path=alib_path, changes=changes)
            rows_updated += 1
            field_updates_total += len(changes)

        changelog.flush(cursor)

    return rows_updated, field_updates_total


def _apply_decision_updates(
    master_conn: sqlite3.Connection,
    proposals: pl.DataFrame,
) -> int:
    if proposals.is_empty():
        return 0

    now = tm_db.utc_now_iso()
    proposal_rows = proposals.select(
        [
            "replacement_mbid",
            pl.lit(DECISION_SOURCE_RETIRED).alias("decision_source"),
            pl.lit(now).alias("updated_utc"),
            "contributor_name",
            "albumartist_context",
            "synthetic_mbid",
        ]
    ).iter_rows(named=False)

    with tm_db.transaction(master_conn):
        cursor = master_conn.cursor()
        cursor.executemany(
            f"""
            UPDATE {USER_DISAMBIGUATION_TABLE}
            SET assigned_mbid = ?,
                decision_source = ?,
                updated_utc = ?
            WHERE contributor_name = ?
              AND albumartist_context = ?
              AND assigned_mbid = ?
            """,
            list(proposal_rows),
        )

    return proposals.height


def main() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = _parse_args()

    staging_db = args.db.strip() or tm_config.get_db_path()
    master_db = args.master_db.strip() or tm_config.get_master_data_db_path(default=staging_db)
    dry_run = not args.apply

    logging.info("Mode: %s", "dry-run" if dry_run else "apply")
    logging.info("Staging DB: %s", staging_db)
    logging.info("Master DB: %s", master_db)

    staging_conn = tm_db.connect(staging_db)
    master_conn = staging_conn if master_db == staging_db else tm_db.connect(master_db)

    try:
        tm_db.require_table_columns(staging_conn, "alib", ("rowid", "__path", "__sqlmodded"))
        tm_db.require_table_columns(
            master_conn,
            DISAMBIGUATED_TABLE,
            ("merge_key_mbid", "lpreferred__artist_name"),
        )
        tm_db.require_table_columns(
            master_conn,
            NAMESAKES_TABLE,
            ("merge_key_mbid", "preferred__artist_name", "musicbrainz_disambiguation"),
        )

        delimiter = tm_config.get_multivalue_delimiter(default="\\")
        mbid_columns = _get_musicbrainz_id_columns(staging_conn)
        if not mbid_columns:
            logging.warning("No musicbrainz_*id columns found in alib; nothing to process.")
            return

        with pl.StringCache():
            disambig_df, namesakes_df, real_mbid_df = _load_real_candidate_lookups(master_conn)
            synthetic_decisions = _load_synthetic_decisions(master_conn, real_mbid_df)

            if synthetic_decisions.is_empty():
                logging.info("No synthetic decisions found. Nothing to retire.")
                return

            proposals, resolution_stats = _build_proposals(
                synthetic_decisions,
                disambig_df,
                namesakes_df,
            )

            if args.limit and args.limit > 0 and not proposals.is_empty():
                proposals = proposals.head(args.limit)

            replacement_map = {
                row["synthetic_mbid"]: row["replacement_mbid"]
                for row in proposals.iter_rows(named=True)
            }

            impacted_rows, field_counts = _scan_alib_impacts_vectorized(
                staging_conn,
                replacement_map,
                mbid_columns,
                delimiter,
            )

        logging.info("Synthetic decisions scanned: %d", synthetic_decisions.height)
        logging.info("Retirable (unique candidate): %d", proposals.height)
        logging.info("Resolution stats: %s", dict(resolution_stats))
        logging.info("Impacted alib rows: %d", impacted_rows.height)
        if field_counts:
            logging.info("Impacted fields: %s", dict(sorted(field_counts.items())))

        if dry_run:
            if not proposals.is_empty():
                logging.info("Sample proposals (max 10):")
                for row in proposals.head(10).iter_rows(named=True):
                    logging.info(
                        "  %s | %s | %s -> %s",
                        row["contributor_name"],
                        row["albumartist_context"],
                        row["synthetic_mbid"],
                        row["replacement_mbid"],
                    )
            logging.info("Dry-run complete. Re-run with --apply to write changes.")
            return

        if proposals.is_empty():
            logging.info("No unique retirement proposals to apply.")
            return

        rows_updated, field_updates = _apply_alib_replacements(
            staging_conn,
            impacted_rows,
            mbid_columns,
        )
        decisions_updated = _apply_decision_updates(master_conn, proposals)

        logging.info("Applied alib updates: %d rows, %d field updates", rows_updated, field_updates)
        logging.info("Applied decision updates: %d", decisions_updated)

    finally:
        if master_conn is not staging_conn:
            master_conn.close()
        staging_conn.close()


if __name__ == "__main__":
    main()
