"""
Purpose:
	Deduplicate configured `alib` columns by token when they contain Tagminder's
	multi-value delimiter.

	For each configured column (tagminder.toml [cleanup].dedupe_columns):
	- if the value contains multivalue_delimiter, split into tokens
	- strip whitespace on each token
	- remove empty tokens
	- de-duplicate tokens while preserving first-seen order
	- re-join using the delimiter

	For rows that change, this script:
	- updates the column values in `alib`
	- increments `__sqlmodded` by the number of changed columns for that row
	- writes field-level change entries into `changelog`

	Vectorization:
	- Change detection and new values are computed in Polars.
	- SQLite writes + changelog are applied only for rows needing changes.

This script is part of Tagminder.

SQLite tables referenced:
	- alib
	- changelog

Author: audiomuze
Last updated: 2026-04-26
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sqlite3

import polars as pl

from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db
from tagminder.core import tm_run

def _configure_logging() -> None:
	logging.basicConfig(
		level=tm_config.get_log_level(),
		format="%(asctime)s - %(levelname)s - %(message)s",
	)


def _dedupe_columns_from_config() -> list[str]:
	cfg = tm_config.load_config()
	cleanup_cfg = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
	cols = cleanup_cfg.get("dedupe_columns") if isinstance(cleanup_cfg, dict) else None
	if not isinstance(cols, list) or not cols:
		raise RuntimeError("Missing or invalid tagminder.toml [cleanup].dedupe_columns")

	out: list[str] = []
	seen: set[str] = set()
	for c in cols:
		if not isinstance(c, str):
			continue
		name = c.strip()
		if not name or name in seen:
			continue
		seen.add(name)
		out.append(name)
	return out


def _clean_text_expr(expr: pl.Expr) -> pl.Expr:
	return expr.cast(pl.Utf8, strict=False).str.strip_chars().replace("", None)


def _dedupe_multivalue_expr(expr: pl.Expr, *, delimiter: str) -> pl.Expr:
	# Split on the configured delimiter only; this matches Tagminder's canonical
	# post-normalization contract.
	tokens = (
		expr.cast(pl.Utf8, strict=False)
		.fill_null("")
		.str.split(delimiter)
		.list.eval(pl.element().str.strip_chars())
		.list.filter(pl.element().is_not_null() & (pl.element() != ""))
		.list.unique(maintain_order=True)
	)
	return (
		pl.when(tokens.list.len() == 0)
		.then(pl.lit(None, dtype=pl.Utf8))
		.otherwise(tokens.list.join(delimiter))
	)


def _norm_compare_str(v: object) -> str | None:
	if v is None:
		return None
	if isinstance(v, str):
		s = v.strip()
		return s if s else None
	s = str(v).strip()
	return s if s else None


def _load_candidate_rows(
	conn: sqlite3.Connection,
	*,
	alib_table: str,
	cols: list[str],
	delimiter: str,
) -> pl.DataFrame:
	quoted_table = tm_db.quote_ident(alib_table)
	quoted_cols = ", ".join(tm_db.quote_ident(c) for c in cols)

	# Only rows that contain the delimiter in any dedupe column can change.
	where_parts = [f"instr(COALESCE({tm_db.quote_ident(c)}, ''), ?) > 0" for c in cols]
	where_sql = " OR ".join(where_parts) if where_parts else "0"

	query = (
		"SELECT __path, COALESCE(__sqlmodded, 0) AS __sqlmodded, "
		+ quoted_cols
		+ f" FROM {quoted_table} WHERE ({where_sql}) ORDER BY __path"
	)

	params = [delimiter for _ in cols]
	return tm_polars_db.sqlite_to_polars(
		conn,
		query,
		params=params,
		dtype_overrides={"__sqlmodded": pl.Int16},
	)


def write_updates(
	conn: sqlite3.Connection,
	*,
	alib_table: str,
	dedupe_cols: list[str],
	updates_df: pl.DataFrame,
	script: str,
	timestamp: str,
) -> int:
	if updates_df.is_empty():
		return 0

	update_sql = tm_db.build_update_sql(table=alib_table, set_cols=dedupe_cols, where_col="__path")
	cursor = conn.cursor()
	updates = 0

	flush_every_rows = 1000
	row_counter = 0

	with tm_db.transaction(conn):
		changelog = tm_changes.ChangelogBatch(timestamp=timestamp, script=script)

		for record in updates_df.iter_rows(named=True):
			alib_path = str(record["__path"])
			new_sqlmodded = int(record.get("__sqlmodded_new") or 0)

			changes: list[tuple[str, object, object]] = []
			new_values: list[object] = []
			for col in dedupe_cols:
				old_v = record.get(col)
				new_v = record.get(f"{col}__new")
				new_values.append(new_v)
				if _norm_compare_str(old_v) == _norm_compare_str(new_v):
					continue
				changes.append((col, old_v, new_v))

			if not changes:
				# Should not happen (updates_df is filtered), but be defensive.
				continue

			changelog.add(alib_path=alib_path, changes=changes)
			cursor.execute(update_sql, (*new_values, new_sqlmodded, alib_path))
			updates += 1
			row_counter += 1

			if row_counter % flush_every_rows == 0:
				changelog.flush(cursor)

		changelog.flush(cursor)

	return updates


def main(argv: list[str] | None = None) -> int:
	_configure_logging()

	parser = argparse.ArgumentParser(
		prog=Path(__file__).name,
		description="Deduplicate configured columns by token using Tagminder multi-value delimiter.",
	)
	parser.add_argument(
		"--db",
		metavar="PATH",
		default=None,
		help="Path to staging SQLite database (default: tagminder.toml [db].path)",
	)
	args = parser.parse_args(argv)

	delimiter = tm_config.get_multivalue_delimiter()
	if not isinstance(delimiter, str) or not delimiter:
		logging.error("Invalid multivalue delimiter from config")
		return 2

	try:
		configured_cols = _dedupe_columns_from_config()
	except Exception as e:
		logging.error("%s", e)
		return 2

	conn, db_path, script, timestamp = tm_run.open_db(
		db_path=args.db,
		require_exists=True,
		ensure_changelog=True,
	)

	try:
		cfg = tm_config.load_config()
		db_cfg = cfg.get("db", {}) if isinstance(cfg, dict) else {}
		alib_table = str(db_cfg.get("alib_table") or "alib") if isinstance(db_cfg, dict) else "alib"

		if not tm_db.table_exists(conn, alib_table):
			logging.error("Missing required table %r in DB %s", alib_table, db_path)
			return 2

		existing = tm_db.table_columns(conn, alib_table)
		if "__path" not in existing:
			logging.error("Missing required column alib.__path")
			return 2

		dedupe_cols = [c for c in configured_cols if c in existing]
		missing = [c for c in configured_cols if c not in existing]
		if missing:
			logging.warning("Configured dedupe columns missing in %s: %s", alib_table, ", ".join(missing))

		if not dedupe_cols:
			logging.error("No configured dedupe columns exist in %s", alib_table)
			return 2

		logging.info("DB: %s", db_path)
		logging.info("Table: %s", alib_table)
		logging.info("Delimiter: %r", delimiter)
		logging.info("Dedupe columns: %d", len(dedupe_cols))

		df = _load_candidate_rows(conn, alib_table=alib_table, cols=dedupe_cols, delimiter=delimiter)
		if df.is_empty():
			logging.info("No rows contain the multi-value delimiter; nothing to dedupe.")
			return 0

		# Clean inputs (strip + empty -> NULL) to stabilize comparisons.
		for c in dedupe_cols:
			if c in df.columns:
				df = df.with_columns(_clean_text_expr(pl.col(c)).alias(c))

		# Compute new values.
		new_cols = [
			_dedupe_multivalue_expr(pl.col(c), delimiter=delimiter).alias(f"{c}__new")
			for c in dedupe_cols
		]
		df2 = df.with_columns(new_cols)

		# Compute total change count and new __sqlmodded.
		# NOTE: Polars does not allow referencing columns created earlier in the
		# same `with_columns` call, so compute _chg_n directly from expressions.
		chg_exprs: list[pl.Expr] = []
		for c in dedupe_cols:
			old_norm = pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
			new_norm = pl.col(f"{c}__new").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
			chg_exprs.append((old_norm != new_norm).cast(pl.Int16))

		df3 = df2.with_columns(pl.sum_horizontal(chg_exprs).alias("_chg_n")).with_columns(
			(pl.col("__sqlmodded").cast(pl.Int64) + pl.col("_chg_n").cast(pl.Int64)).alias("__sqlmodded_new")
		)

		updates_df = df3.filter(pl.col("_chg_n") > 0).select(
			[
				"__path",
				"__sqlmodded",
				"__sqlmodded_new",
				*dedupe_cols,
				*[f"{c}__new" for c in dedupe_cols],
			]
		)

		if updates_df.is_empty():
			logging.info("No dedupe changes needed.")
			return 0

		updates = write_updates(
			conn,
			alib_table=alib_table,
			dedupe_cols=dedupe_cols,
			updates_df=updates_df,
			script=script,
			timestamp=timestamp,
		)

		logging.info("Updated %d rows and logged changes.", updates)
		return 0

	finally:
		conn.close()


if __name__ == "__main__":
	raise SystemExit(main())
