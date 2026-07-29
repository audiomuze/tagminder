#!/usr/bin/env python3
"""92-library-insights.py

Purpose:
	Generate a music-lover-oriented "Library Insights" HTML dashboard from the
	staging SQLite DB (alib) using Plotly.

Scope / Data contract:
	- Only uses:
		* system columns (prefix defined by tagminder.toml [columns].system_prefix)
		* columns listed in tagminder.toml [cleanup].keep_columns
	- Columns not in keep_columns (and not system columns) are assumed to be
	  discarded by the cleanup step and are intentionally ignored.

	- Contributor/credits fields that are currently too noisy are skipped:
		performer, involvedpeople, involvedpeople2, personnel,
		discogs_track_credits, discogs_release_credits, amg_credits, amg_artists

Implementation notes:
	- Aggregations are done in Polars (not SQL).
	- Multi-value text fields are tokenized by Tagminder's configured delimiter
	  (tagminder.toml [strings].multivalue_delimiter).
	- Album grouping uses album-root logic consistent with other reports:
	  disc subfolders like "CD1" / "Disc 02" roll up to the parent folder.

Outputs:
	- Writes HTML into tagminder.toml [paths].cache_dir (default /tmp).
	- Opens the report in the default browser.

This script is part of Tagminder.

SQLite tables referenced:
	- alib

Author: audiomuze
Last updated: 2026-04-26
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import webbrowser
from html import escape
from pathlib import Path
from typing import Any, cast

import polars as pl
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_polars_db

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


_SKIP_FIELDS: set[str] = {
	"performer",
	"involvedpeople",
	"involvedpeople2",
	"personnel",
	"discogs_track_credits",
	"discogs_release_credits",
	"amg_credits",
	"amg_artists",
}


_TRUTHY = {"1", "true", "t", "yes", "y"}


# Horizontal bar charts: allocate enough vertical pixels per item so that long
# label lists remain legible (avoid overlapping y-axis tick labels).
_HBAR_ROW_PX = 24
_HBAR_EXTRA_PX = 180


def _configure_logging() -> None:
	logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _load_keep_columns() -> list[str]:
	cfg = tm_config.load_config()
	cleanup = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
	if not isinstance(cleanup, dict):
		raise RuntimeError("Missing config table [cleanup] in tagminder.toml")

	cols = cleanup.get("keep_columns")
	if not isinstance(cols, list) or not cols or not all(isinstance(x, str) and x for x in cols):
		raise RuntimeError("Invalid or missing [cleanup].keep_columns")

	seen: set[str] = set()
	out: list[str] = []
	for c in cols:
		if c not in seen:
			seen.add(c)
			out.append(c)
	return out


def _load_system_prefix() -> str:
	cfg = tm_config.load_config()
	cols = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
	if not isinstance(cols, dict):
		return "__"
	system_prefix = cols.get("system_prefix")
	if isinstance(system_prefix, str) and system_prefix:
		return system_prefix
	return "__"


def _alib_columns(conn: sqlite3.Connection) -> set[str]:
	return {str(r[1]) for r in conn.execute("PRAGMA table_info(alib)").fetchall()}


def _clean_text(expr: pl.Expr) -> pl.Expr:
	return expr.cast(pl.Utf8, strict=False).str.strip_chars().replace("", None)


def _truthy_expr(col: str) -> pl.Expr:
	return (
		pl.col(col)
		.cast(pl.Utf8, strict=False)
		.fill_null("")
		.str.strip_chars()
		.str.to_lowercase()
		.is_in(list(_TRUTHY))
		.cast(pl.Int8)
	)


def _parse_year(expr: pl.Expr) -> pl.Expr:
	# Extract first YYYY in the string.
	return expr.cast(pl.Utf8, strict=False).str.extract(r"(\d{4})", 1).cast(pl.Int32, strict=False)


def _parse_float(expr: pl.Expr) -> pl.Expr:
	return expr.cast(pl.Utf8, strict=False).str.extract(r"(\d+(?:\.\d+)?)", 1).cast(pl.Float64, strict=False)


def _parse_int(expr: pl.Expr) -> pl.Expr:
	return expr.cast(pl.Utf8, strict=False).str.extract(r"(\d+)", 1).cast(pl.Int64, strict=False)


def _parse_signed_float(expr: pl.Expr) -> pl.Expr:
	return (
		expr.cast(pl.Utf8, strict=False)
		.str.extract(r"([+-]?\d+(?:\.\d+)?)", 1)
		.cast(pl.Float64, strict=False)
	)


def _dirname_expr(path_expr: pl.Expr) -> pl.Expr:
	return path_expr.cast(pl.Utf8, strict=False).str.extract(r"([^/]+)$", 1)


def _tokens_expr(col: str, *, delimiter: str) -> pl.Expr:
	# Tokenize by Tagminder delimiter only (canonical post-normalization).
	return (
		pl.col(col)
		.cast(pl.Utf8, strict=False)
		.fill_null("")
		.str.split(delimiter)
		.list.eval(pl.element().str.strip_chars())
		.list.filter(pl.element().is_not_null() & (pl.element() != ""))
		.list.unique(maintain_order=True)
	)


def _mode_by_group(
	df: pl.DataFrame,
	*,
	group_col: str,
	value_col: str,
	out_col: str,
) -> pl.DataFrame:
	if group_col not in df.columns or value_col not in df.columns:
		return pl.DataFrame({group_col: [], out_col: []})

	counts = (
		df.select([pl.col(group_col), pl.col(value_col)])
		.drop_nulls([group_col, value_col])
		.group_by([group_col, value_col])
		.len()
		.sort([group_col, "len"], descending=[False, True])
	)

	if counts.is_empty():
		return pl.DataFrame({group_col: [], out_col: []})

	return counts.unique(subset=[group_col], keep="first").select(
		[pl.col(group_col), pl.col(value_col).alias(out_col)]
	)


def _kpi_tile(label: str, value: str) -> str:
	return (
		"<div class=\"kpi\">"
		f"<div class=\"kpi-label\">{escape(label)}</div>"
		f"<div class=\"kpi-value\">{escape(value)}</div>"
		"</div>"
	)


def _render_html_page(
	*,
	title: str,
	subtitle: str,
	kpis_html: str,
	persona_html: str,
	sections_html: str,
	theme_bg: str,
	theme_fg: str,
	divider_rgba: str,
) -> str:
	return f"""<!doctype html>
<html lang=\"en\">
  <head>
	<meta charset=\"utf-8\" />
	<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
	<title>{escape(title)}</title>
	<style>
	  :root {{ color-scheme: dark light; }}
	  html, body {{ height: 100%; background: {theme_bg}; color: {theme_fg}; }}
	  body {{ margin: 0; }}

	  .page {{ padding: 14px 18px 22px 18px; }}
	  .page-title {{
		margin: 0 17px 6px 17px;
		font: 700 18px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
	  }}
	  .page-subtitle {{
		margin: 0 17px 10px 17px;
		font: 500 13px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
		opacity: 0.85;
	  }}

	  .toolbar {{
		margin: 0 17px 8px 17px;
		display: flex;
		gap: 10px;
		align-items: center;
		justify-content: flex-start;
	  }}
	  .btn {{
		appearance: none;
		border: 1px solid {divider_rgba};
		background: transparent;
		color: {theme_fg};
		padding: 7px 10px;
		border-radius: 8px;
		font: 600 12px/1.1 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
		cursor: pointer;
		opacity: 0.95;
	  }}
	  .btn:hover {{ opacity: 1.0; }}
	  .btn:disabled {{ opacity: 0.5; cursor: not-allowed; }}

	  .kpis {{
		margin: 10px 17px 14px 17px;
		display: grid;
		grid-template-columns: repeat(6, minmax(0, 1fr));
		gap: 10px;
	  }}
	  @media (max-width: 1200px) {{
		.kpis {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
	  }}
	  @media (max-width: 700px) {{
		.kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
	  }}
	  .kpi {{
		border-top: 1px solid {divider_rgba};
		padding-top: 8px;
	  }}
	  .kpi-label {{ font: 600 12px/1.1 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif; opacity: 0.8; }}
	  .kpi-value {{ font: 800 16px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif; margin-top: 4px; }}

	  .note {{
		margin: 0 17px 10px 17px;
		padding: 8px 10px;
		border-top: 1px solid {divider_rgba};
		font: 500 12px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
		opacity: 0.9;
	  }}

	  .sections {{ margin: 0 17px 0 17px; }}

	  .grid {{
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 18px;
		align-items: start;
	  }}
	  @media (max-width: 1100px) {{
		.grid {{ grid-template-columns: 1fr; }}
	  }}

	  .card {{
		border-top: 1px solid {divider_rgba};
		padding-top: 10px;
	  }}
	  .card-title {{
		margin: 0 0 8px 0;
		font: 600 14px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
		opacity: 0.95;
	  }}
	  .plot-wrap {{ margin: 0 0 4px 0; }}

	  .tm-table {{
		width: 100%;
		border-collapse: collapse;
		font: 500 12px/1.25 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
	  }}
	  .tm-table th, .tm-table td {{
		padding: 6px 8px;
		border-top: 1px solid {divider_rgba};
		vertical-align: top;
	  }}
	  .tm-table th {{
		text-align: left;
		font-weight: 700;
		opacity: 0.9;
	  }}
	  .tm-table th.num {{
		text-align: right;
	  }}
	  .tm-table td.num {{
		text-align: right;
		font-variant-numeric: tabular-nums;
	  }}
	  .tm-subnote {{
		margin: 6px 0 0 0;
		font: 500 12px/1.25 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
		opacity: 0.8;
	  }}
	</style>
  </head>
  <body>
	<div class=\"page\">
	  <div class=\"page-title\">{escape(title)}</div>
	  <div class=\"page-subtitle\">{escape(subtitle)}</div>
	  <div class=\"toolbar\">
		<button class=\"btn\" id=\"tm-download-all\" type=\"button\">Download all charts (PNG)</button>
	  </div>
	  <div class=\"kpis\">{kpis_html}</div>
	  {persona_html}
	  <div class=\"sections\">{sections_html}</div>
	</div>
	<script>
	  (function () {{
		const btn = document.getElementById('tm-download-all');
		if (!btn) return;

		async function downloadAllCharts() {{
		  if (!window.Plotly) {{
			alert('Plotly has not loaded yet. Try again in a moment.');
			return;
		  }}
		  const graphs = Array.from(document.querySelectorAll('.plotly-graph-div'));
		  if (!graphs.length) {{
			alert('No charts found on this page.');
			return;
		  }}

		  // High-resolution export: scale > 1.
		  const scale = 4;
		  btn.disabled = true;
		  const oldText = btn.textContent;
		  btn.textContent = 'Preparing downloads…';
		  try {{
			for (let i = 0; i < graphs.length; i++) {{
			  const gd = graphs[i];
			  const name = (gd && gd.id) ? gd.id : `chart-${{i+1}}`;
			  btn.textContent = `Downloading ${{i+1}}/${{graphs.length}}…`;
			  try {{
				const dataUrl = await Plotly.toImage(gd, {{ format: 'png', scale }});
				const a = document.createElement('a');
				a.href = dataUrl;
				a.download = name + '.png';
				document.body.appendChild(a);
				a.click();
				a.remove();
			  }} catch (e) {{
				console.warn('Failed to export', name, e);
			  }}
			  // Small delay helps some browsers queue multiple downloads.
			  await new Promise(r => setTimeout(r, 150));
			}}
		  }} finally {{
			btn.disabled = false;
			btn.textContent = oldText;
		  }}
		}}

		btn.addEventListener('click', downloadAllCharts);
	  }})();
	</script>
  </body>
</html>"""


def _fmt_pct(pct: float) -> str:
	try:
		p = float(pct or 0.0)
	except Exception:
		p = 0.0
	# Preserve detail for tiny shares (e.g. MP3 in a giant FLAC library).
	if p < 0.1:
		return f"{p:.4f}%"
	return f"{p:.2f}%"


def _fmt_float_compact(x: float) -> str:
	try:
		v = float(x)
	except Exception:
		return "Unknown"
	if v <= 0:
		return "Unknown"
	if abs(v - round(v)) < 1e-9:
		return str(int(round(v)))
	return (f"{v:.6f}").rstrip("0").rstrip(".")


def _render_table(
	*,
	title: str,
	headers: list[str],
	rows: list[list[str]],
	numeric_cols: set[int] | None = None,
	note: str | None = None,
) -> str:
	numeric_cols = set(numeric_cols or set())
	th_cells: list[str] = []
	for i, h in enumerate(headers):
		class_attr = " class=\"num\"" if i in numeric_cols else ""
		th_cells.append(f"<th{class_attr}>{escape(h)}</th>")
	th = "".join(th_cells)
	tbody_rows: list[str] = []
	for r in rows:
		tds: list[str] = []
		for i, cell in enumerate(r):
			class_attr = " class=\"num\"" if i in numeric_cols else ""
			tds.append(f"<td{class_attr}>{escape(cell)}</td>")
		tbody_rows.append("<tr>" + "".join(tds) + "</tr>")

	note_html = f"<div class=\"tm-subnote\">{escape(note)}</div>" if note else ""
	return (
		f"<div class=\"card\"><div class=\"card-title\">{escape(title)}</div>"
		f"<div class=\"plot-wrap\"><table class=\"tm-table\"><thead><tr>{th}</tr></thead>"
		f"<tbody>{''.join(tbody_rows)}</tbody></table>{note_html}</div></div>"
	)


def _safe_div(html: str | None) -> str:
	return html or "<div class=\"note\">No data available for this section.</div>"


def _to_html_fig(fig: go.Figure, *, include_plotlyjs: bool, div_id: str) -> str:
	return pio.to_html(
		fig,
		include_plotlyjs=cast(Any, ("cdn" if include_plotlyjs else False)),
		full_html=False,
		config={"responsive": True, "displaylogo": False},
		div_id=div_id,
	)


def _fmt_int(n: int) -> str:
	return f"{n:,d}"


def _fmt_hours(seconds: float) -> str:
	hours = float(seconds or 0.0) / 3600.0
	if hours < 10:
		return f"{hours:,.2f} h"
	if hours < 100:
		return f"{hours:,.1f} h"
	return f"{hours:,.0f} h"


def _fmt_gb(bytes_v: float) -> str:
	gb = float(bytes_v or 0.0) / 1_000_000_000.0
	if gb < 10:
		return f"{gb:,.2f} GB"
	if gb < 100:
		return f"{gb:,.1f} GB"
	return f"{gb:,.0f} GB"


def _pick_persona(
	*,
	persona_arg: str,
	scores: dict[str, float],
	) -> tuple[str, bool]:
	"""Return (persona, is_auto_selected)."""

	persona_arg = (persona_arg or "").strip().lower()
	if persona_arg and persona_arg != "auto":
		return persona_arg, False

	best = max(scores.items(), key=lambda kv: kv[1])
	# Lower threshold than earlier versions: genre/style signals are often sparse
	# even for strongly-skewed libraries.
	if best[1] >= 0.22:
		return best[0], True
	return "mixed", True


def _persona_scores(*, df: pl.DataFrame, available_cols: set[str], delimiter: str) -> dict[str, float]:
	def share_nonempty(col: str) -> float:
		if col not in available_cols or col not in df.columns:
			return 0.0
		s = df.select((pl.col(col).is_not_null() & (pl.col(col) != "")).mean()).item()
		try:
			return float(s or 0.0)
		except Exception:
			return 0.0

	def share_any_token(col: str, tokens: set[str]) -> float:
		if col not in available_cols or col not in df.columns:
			return 0.0
		# Per-track tokenization; compute fraction of rows containing any token.
		b = (
			df.select(
				_tokens_expr(col, delimiter=delimiter)
				.list.eval(pl.element().str.to_lowercase())
				.list.eval(pl.element().is_in(list(tokens)))
				.list.any()
				.alias("has")
			)
			.select(pl.col("has").mean())
			.item()
		)
		try:
			return float(b or 0.0)
		except Exception:
			return 0.0

	classical_tokens = {"classical", "baroque", "romantic", "opera", "symphony", "chamber"}
	jazz_tokens = {"jazz", "bebop", "hard bop", "swing", "fusion"}
	electronic_tokens = {
		"electronic",
		"electronica",
		"ambient",
		"techno",
		"house",
		"trance",
		"idm",
		"dnb",
		"drum & bass",
		"drum and bass",
	}
	rock_pop_tokens = {
		"rock",
		"pop",
		"indie",
		"alternative",
		"metal",
		"punk",
		"grunge",
		"folk",
	}

	genre_classical = max(share_any_token("genre", classical_tokens), share_any_token("style", classical_tokens))
	genre_jazz = max(share_any_token("genre", jazz_tokens), share_any_token("style", jazz_tokens))
	genre_electronic = max(
		share_any_token("genre", electronic_tokens), share_any_token("style", electronic_tokens)
	)
	genre_rock_pop = max(share_any_token("genre", rock_pop_tokens), share_any_token("style", rock_pop_tokens))

	classical_roles = ["composer", "conductor", "orchestra", "work", "movement"]
	classical_role_signal = (
		sum(share_nonempty(c) for c in classical_roles) / float(len(classical_roles))
		if classical_roles
		else 0.0
	)
	electronic_roles = ["remixer", "mixer", "producer"]
	electronic_role_signal = (
		sum(share_nonempty(c) for c in electronic_roles) / float(len(electronic_roles))
		if electronic_roles
		else 0.0
	)

	scores = {
		"classical": max(genre_classical, classical_role_signal),
		"jazz": genre_jazz,
		"electronic": max(genre_electronic, electronic_role_signal),
		"rock_pop": genre_rock_pop,
		"mixed": 0.0,
	}
	# Clamp to [0, 1] for sanity.
	return {k: max(0.0, min(1.0, float(v or 0.0))) for k, v in scores.items()}


def main(argv: list[str] | None = None) -> int:
	_configure_logging()

	p = argparse.ArgumentParser(
		prog=Path(sys.argv[0]).name,
		description="Generate Library Insights HTML dashboard (music-lover oriented).",
	)
	p.add_argument(
		"--db",
		metavar="PATH",
		default=None,
		help="Path to staging SQLite database (default: tagminder.toml [db].path)",
	)
	p.add_argument(
		"--theme",
		choices=("dark", "light"),
		default="dark",
		help="Color theme for the HTML report (default: dark)",
	)
	p.add_argument(
		"--persona",
		choices=("auto", "mixed", "classical", "jazz", "rock_pop", "electronic"),
		default="auto",
		help="Spotlight preset (default: auto)",
	)
	p.add_argument(
		"--top",
		type=int,
		default=30,
		help="Top-N cutoff for some charts (default: 30)",
	)
	args = p.parse_args(argv)

	db_path = args.db or tm_config.get_db_path(default=None)
	db_path = str(Path(db_path).resolve())

	delimiter = tm_config.get_multivalue_delimiter()
	keep_columns = _load_keep_columns()
	system_prefix = _load_system_prefix()

	desired_keep = [
		"title",
		"albumartist",
		"artist",
		"album",
		"releasetype",
		"year",
		"date",
		"releasedate",
		"originalyear",
		"originalreleasedate",
		"album_dr",
		"rating",
		"genre",
		"style",
		"studio",
		"recordedat",
		"recordinglocation",
		"label",
		"producer",
		"composer",
		"conductor",
		"orchestra",
		"ensemble",
		"engineer",
		"mixer",
		"remixer",
		"compilation",
		"live",
		"bootleg",
		"explicit",
		"work",
		"movement",
		"part",
		"replaygain_album_gain",
		"replaygain_album_peak",
		"replaygain_track_gain",
		"replaygain_track_peak",
	]

	desired_system = [
		f"{system_prefix}dirpath",
		f"{system_prefix}filetype",
		f"{system_prefix}length_seconds",
		f"{system_prefix}file_size_bytes",
		f"{system_prefix}file_mod_datetime_raw",
		f"{system_prefix}bitrate_num",
		f"{system_prefix}frequency_num",
		f"{system_prefix}bitspersample",
		f"{system_prefix}channels",
	]

	conn = tm_db.connect(db_path, read_only=True, wal=False)
	try:
		available_sql_cols = _alib_columns(conn)

		keep_set = set(keep_columns)
		keep_eff = [
			c
			for c in desired_keep
			if c in keep_set and c not in _SKIP_FIELDS and c in available_sql_cols
		]
		sys_eff = [c for c in desired_system if c in available_sql_cols]

		dir_col = f"{system_prefix}dirpath"
		if dir_col not in sys_eff:
			raise RuntimeError(f"alib.{dir_col} is required for album-level insights")

		select_cols = [*sys_eff, *keep_eff]

		dtype_overrides = {
			f"{system_prefix}length_seconds": pl.Float64,
			f"{system_prefix}file_size_bytes": pl.Int64,
			# DB contract: __frequency_num is TEXT (kHz); parse as Float64.
			f"{system_prefix}frequency_num": pl.Float64,
		}

		df = tm_polars_db.sqlite_to_polars(
			conn,
			"SELECT "
			+ ", ".join(tm_db.quote_ident(c) for c in select_cols)
			+ " FROM alib",
			dtype_overrides=dtype_overrides,
		)
	finally:
		conn.close()

	if df.is_empty():
		logging.info("No rows in alib; nothing to report")
		return 0

	# Normalize kept text columns.
	for c in keep_eff:
		if c in df.columns:
			df = df.with_columns(_clean_text(pl.col(c)).alias(c))

	dir_col = f"{system_prefix}dirpath"
	filetype_col = f"{system_prefix}filetype"
	len_col = f"{system_prefix}length_seconds"
	size_col = f"{system_prefix}file_size_bytes"

	df = df.with_columns(
		[
			tm_album.album_root_polars_expr(dir_col, out_col="album_root"),
			pl.col(len_col).cast(pl.Float64, strict=False).fill_null(0.0).alias("duration_s"),
			pl.col(size_col).cast(pl.Int64, strict=False).fill_null(0).alias("size_bytes"),
		]
	)
	df = df.with_columns(_dirname_expr(pl.col("album_root")).alias("album_dirname"))

	# Boolean-ish flags.
	for flag in ("compilation", "live", "bootleg", "explicit"):
		if flag in df.columns:
			df = df.with_columns(_truthy_expr(flag).alias(f"is_{flag}"))
		else:
			df = df.with_columns(pl.lit(0, dtype=pl.Int8).alias(f"is_{flag}"))

	# Years.
	df = df.with_columns(
		[
			_parse_year(pl.col("year"))
			.fill_null(_parse_year(pl.col("date")))
			.fill_null(_parse_year(pl.col("releasedate")))
			.alias("year_release"),
			_parse_year(pl.col("originalyear"))
			.fill_null(_parse_year(pl.col("originalreleasedate")))
			.alias("year_original"),
		]
	)

	# Ratings.
	if "rating" in df.columns:
		df = df.with_columns(_parse_float(pl.col("rating")).alias("rating_num"))
	else:
		df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("rating_num"))

	available_cols = set(df.columns)
	scores = _persona_scores(df=df, available_cols=available_cols, delimiter=delimiter)
	persona, persona_is_auto = _pick_persona(persona_arg=str(args.persona), scores=scores)

	# Album-level summary.
	album_roots = df.select(pl.col("album_root")).drop_nulls().unique()
	year_mode = _mode_by_group(
		df.select(["album_root", "year_release"]),
		group_col="album_root",
		value_col="year_release",
		out_col="album_year",
	)
	rel_type_mode = _mode_by_group(
		df.select(["album_root", "releasetype"]),
		group_col="album_root",
		value_col="releasetype",
		out_col="album_releasetype",
	)
	album_stats = (
		df.group_by("album_root")
		.agg(
			[
				pl.len().alias("album_tracks"),
				pl.col("duration_s").sum().alias("album_duration_s"),
				pl.col("rating_num").mean().alias("album_rating_avg"),
				pl.col("is_compilation").max().alias("album_is_compilation"),
			]
		)
	)
	album_names = (
		df.select([pl.col("album_root"), pl.col("album_dirname")])
		.drop_nulls(["album_root"])
		.unique(subset=["album_root"], keep="first")
	)
	df_album = (
		album_roots.join(year_mode, on="album_root", how="left")
		.join(rel_type_mode, on="album_root", how="left")
		.join(album_stats, on="album_root", how="left")
		.join(album_names, on="album_root", how="left")
		.with_columns(
			[
				pl.col("album_releasetype").fill_null("Unknown").alias("album_releasetype"),
				pl.col("album_is_compilation").fill_null(0).cast(pl.Int8).alias("album_is_compilation"),
			]
		)
	)

	# Acquisition timeline (ingestion lineage): derive album acquisition year/decade
	# from the file modification timestamp captured at ingest.
	mod_col = f"{system_prefix}file_mod_datetime_raw"
	if mod_col in df.columns:
		ts = _parse_int(pl.col(mod_col))
		# Heuristic: if values look like milliseconds since epoch, treat as ms.
		dt = (
			pl.when(ts.is_not_null() & (ts > 50_000_000_000))
			.then(pl.from_epoch(ts, time_unit="ms"))
			.when(ts.is_not_null() & (ts > 0))
			.then(pl.from_epoch(ts, time_unit="s"))
			.otherwise(pl.lit(None, dtype=pl.Datetime))
		)
		album_acq = (
			df.select([pl.col("album_root"), dt.alias("_mod_dt")])
			.drop_nulls(["album_root"])
			.group_by("album_root")
			.agg(pl.col("_mod_dt").min().alias("album_acquired_dt"))
			.with_columns(
				[
					pl.col("album_acquired_dt").dt.year().alias("album_acquired_year"),
					((pl.col("album_acquired_dt").dt.year() // 10) * 10).alias("album_acquired_decade"),
				]
			)
		)
		df_album = df_album.join(album_acq, on="album_root", how="left")

	# Albumartist pairs.
	if "albumartist" in df.columns:
		aa_pairs = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("albumartist", delimiter=delimiter).alias("albumartist_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("albumartist_tok")
			.drop_nulls(["albumartist_tok"])
			.unique(subset=["album_root", "albumartist_tok"])
		)
	else:
		aa_pairs = pl.DataFrame({"album_root": [], "albumartist_tok": []})

	# VA / compilation classification.
	va_set = {"various artists", "various", "va"}
	if not aa_pairs.is_empty():
		aa_va = (
			aa_pairs.with_columns(
				pl.col("albumartist_tok")
				.str.to_lowercase()
				.str.strip_chars()
				.is_in(list(va_set))
				.cast(pl.Int8)
				.alias("is_va")
			)
			.group_by("album_root")
			.agg(pl.max("is_va").alias("album_is_va"))
		)
	else:
		aa_va = pl.DataFrame({"album_root": [], "album_is_va": []})

	df_album = (
		df_album.join(aa_va, on="album_root", how="left")
		.with_columns(pl.col("album_is_va").fill_null(0).cast(pl.Int8))
		.with_columns(
			((pl.col("album_is_compilation") == 1) | (pl.col("album_is_va") == 1))
			.cast(pl.Int8)
			.alias("album_is_va_or_comp")
		)
	)

	# KPIs.
	track_count = int(df.height)
	album_count = int(df_album.height)
	total_duration_s = float(df.select(pl.col("duration_s").sum()).item())
	total_size_bytes = float(df.select(pl.col("size_bytes").sum()).item())

	filetype_count = (
		int(df.select(pl.col(filetype_col).n_unique()).item()) if filetype_col in df.columns else 0
	)
	unique_albumartists = (
		int(aa_pairs.select(pl.col("albumartist_tok").n_unique()).item())
		if not aa_pairs.is_empty()
		else 0
	)

	kpis_html = "".join(
		[
			_kpi_tile("Tracks", _fmt_int(track_count)),
			_kpi_tile("Albums (folder roots)", _fmt_int(album_count)),
			_kpi_tile("Album Artists", _fmt_int(unique_albumartists)),
			_kpi_tile("Listening Time", _fmt_hours(total_duration_s)),
			_kpi_tile("Storage", _fmt_gb(total_size_bytes)),
			_kpi_tile(f"Formats ({filetype_col})", _fmt_int(filetype_count)),
		]
	)

	# Theme.
	theme = str(args.theme).lower().strip()
	is_dark = theme == "dark"
	template = "plotly_dark" if is_dark else "plotly_white"
	page_bg = "#0b0e14" if is_dark else "#ffffff"
	page_fg = "#e6e6e6" if is_dark else "#111111"
	grid_color = "#2f3742" if is_dark else "#ddd"
	axis_line_color = "#6c7785" if is_dark else "#888"
	divider_rgba = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.10)"

	include_js = True

	# Persona visual: show the signal strengths used to pick (or explain) the persona.
	persona_items = [
		("classical", scores.get("classical", 0.0)),
		("jazz", scores.get("jazz", 0.0)),
		("electronic", scores.get("electronic", 0.0)),
		("rock_pop", scores.get("rock_pop", 0.0)),
		("mixed", scores.get("mixed", 0.0)),
	]
	persona_items = sorted(persona_items, key=lambda kv: kv[1], reverse=True)
	persona_labels = [k.replace("_", " ") for k, _ in persona_items]
	persona_vals = [v * 100.0 for _, v in persona_items]
	persona_colors = [
		("rgba(255, 127, 14, 0.85)" if k == persona else "rgba(31, 119, 180, 0.55)")
		for k, _ in persona_items
	]

	fig_persona = go.Figure(
		data=[
			go.Bar(
				y=persona_labels,
				x=persona_vals,
				orientation="h",
				marker_color=persona_colors,
				hovertemplate="%{y}<br>%{x:.1f}% signal<extra></extra>",
			)
		]
	)
	fig_persona.update_layout(
		template=template,
		height=260,
		margin={"l": 120, "r": 25, "t": 25, "b": 40},
		paper_bgcolor=page_bg,
		plot_bgcolor=page_bg,
		font={"size": 12, "color": page_fg},
		xaxis={"gridcolor": grid_color, "zeroline": False, "ticksuffix": "%", "range": [0, 100]},
		yaxis={"autorange": "reversed"},
		showlegend=False,
		title={"text": "Persona Signals", "x": 0.0, "xanchor": "left"},
	)
	persona_fig_html = _to_html_fig(fig_persona, include_plotlyjs=include_js, div_id="tm-insights-persona")
	include_js = False

	# 1) Format mix by filetype (count + hours)
	if filetype_col in df.columns:
		fmt = (
			df.group_by(filetype_col)
			.agg(
				[
					pl.len().alias("tracks"),
					pl.col("duration_s").sum().alias("duration_s"),
				]
			)
			.with_columns((pl.col("duration_s") / 3600.0).alias("hours"))
			.sort("tracks", descending=True)
		)
		fmt_top = fmt.head(max(15, int(args.top)))
		filetypes = fmt_top[filetype_col].to_list()
		tracks = fmt_top["tracks"].to_list()
		hours = fmt_top["hours"].to_list()

		fig_format = make_subplots(
			rows=1,
			cols=2,
			subplot_titles=("Tracks", "Listening time (hours)"),
			horizontal_spacing=0.10,
		)
		fig_format.add_trace(
			go.Bar(
				x=filetypes,
				y=tracks,
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{x}<br>%{y:,d} tracks<extra></extra>",
			),
			row=1,
			col=1,
		)
		fig_format.add_trace(
			go.Bar(
				x=filetypes,
				y=hours,
				marker_color="rgba(255, 127, 14, 0.75)",
				hovertemplate="%{x}<br>%{y:.1f} h<extra></extra>",
			),
			row=1,
			col=2,
		)
		fig_format.update_layout(
			template=template,
			height=420,
			margin={"l": 35, "r": 25, "t": 35, "b": 90},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_format.update_xaxes(tickangle=35, showline=True, linecolor=axis_line_color)
		fig_format.update_yaxes(gridcolor=grid_color, zeroline=False)

		html_format = _to_html_fig(fig_format, include_plotlyjs=include_js, div_id="tm-insights-format")
		include_js = False
	else:
		html_format = "<div class=\"note\">No filetype data available.</div>"

	# 1b) Format quality mix (lossless vs lossy)
	if filetype_col in df.columns:
		ft_disp = (
			pl.col(filetype_col)
			.cast(pl.Utf8, strict=False)
			.fill_null("")
			.str.strip_chars()
		)
		ft_lc = ft_disp.str.to_lowercase()
		# NOTE: these system columns are TEXT in SQLite. Preserve NULL vs literal '0'
		# by casting from text here rather than using tm_polars_db dtype_overrides.
		bps_raw = _clean_text(pl.col(f"{system_prefix}bitspersample")).cast(pl.Int64, strict=False)
		bps = pl.when(bps_raw.is_not_null() & (bps_raw > 0)).then(bps_raw).otherwise(pl.lit(None))
		freq_khz = pl.col(f"{system_prefix}frequency_num").cast(pl.Float64, strict=False)
		ch = _clean_text(pl.col(f"{system_prefix}channels")).cast(pl.Int64, strict=False)
		br = _clean_text(pl.col(f"{system_prefix}bitrate_num")).cast(pl.Int64, strict=False)

		lossless_types = {
			"flac",
			"wav",
			"wave",
			"aiff",
			"aif",
			"ape",
			"wavpack",
			"wv",
			"w64",
			"tta",
			"dsf",
			"dff",
			"alac",
		}
		lossy_types = {
			"mp3",
			"ogg vorbis",
			"vorbis",
			"opus",
			"aac",
		}
		# Rules:
		# - known lossy types => lossy
		# - known lossless container/codec => lossless
		# - bitspersample==1 => DSD => treat as lossless
		# - otherwise, bit depth presence (>0) is a strong lossless signal
		# - MP4/M4A/WMA are ambiguous; treat missing/0 bit depth as lossy variants
		is_dsd_expr = bps.is_not_null() & (bps == 1)
		is_lossless_expr = (
			pl.when(is_dsd_expr)
			.then(pl.lit(True))
			.when(ft_lc.is_in(list(lossy_types)))
			.then(pl.lit(False))
			.when(ft_lc.is_in(list(lossless_types)))
			.then(pl.lit(True))
			.when(bps.is_not_null() & (bps > 0))
			.then(pl.lit(True))
			.when(ft_lc.is_in(["m4a", "mp4", "wma"]) & bps.is_null())
			.then(pl.lit(False))
			.when(ft_lc == "")
			.then(pl.lit(None))
			.otherwise(pl.lit(None))
		)

		df_q = df.with_columns(
			[
				ft_disp.alias("_ft"),
				ft_lc.alias("_ft_lc"),
				is_lossless_expr.alias("_is_lossless"),
				is_dsd_expr.alias("_is_dsd"),
				br.alias("_br"),
				freq_khz.alias("_freq_khz"),
				bps.alias("_bps"),
				ch.alias("_ch"),
			]
		).with_columns(
			pl.when(pl.col("_is_lossless") == True)
			.then(pl.lit("Lossless"))
			.when(pl.col("_is_lossless") == False)
			.then(pl.lit("Lossy"))
			.otherwise(pl.lit("Unknown"))
			.alias("codec_class")
		)

		lossless_df = df_q.filter(pl.col("codec_class") == "Lossless")
		lossy_df = df_q.filter(pl.col("codec_class") == "Lossy")

		n_lossless = int(lossless_df.height)
		n_lossy = int(lossy_df.height)

		lossless_tbl = (
			lossless_df.group_by(["_ft", "_ch", "_bps", "_freq_khz"])
			.len()
			.rename({"len": "tracks"})
			.with_columns(
				(
					pl.col("tracks")
					/ pl.lit(float(n_lossless) if n_lossless else 1.0)
					* 100.0
				).alias("pct")
			)
			.with_columns(
				[
					pl.col("_ch").fill_null(9999).alias("_ch_sort"),
					pl.col("_bps").fill_null(9999).alias("_bps_sort"),
					pl.col("_freq_khz").fill_null(9999.0).alias("_freq_sort"),
				]
			)
			.sort(["_ft", "_ch_sort", "_bps_sort", "_freq_sort"], descending=[False, False, False, False])
			.drop(["_ch_sort", "_bps_sort", "_freq_sort"])
		)

		lossy_tbl = (
			lossy_df.group_by(["_ft", "_ch", "_br", "_freq_khz"])
			.len()
			.rename({"len": "tracks"})
			.with_columns(
				(
					pl.col("tracks")
					/ pl.lit(float(n_lossy) if n_lossy else 1.0)
					* 100.0
				).alias("pct")
			)
			.with_columns(
				[
					pl.col("_ch").fill_null(9999).alias("_ch_sort"),
					pl.col("_br").fill_null(9999999).alias("_br_sort"),
					pl.col("_freq_khz").fill_null(9999.0).alias("_freq_sort"),
				]
			)
			.sort(["_ft", "_ch_sort", "_br_sort", "_freq_sort"], descending=[False, False, False, False])
			.drop(["_ch_sort", "_br_sort", "_freq_sort"])
		)

		lossless_rows: list[list[str]] = []
		if not lossless_tbl.is_empty():
			for ft, bps_v, freq_v, ch_v, tracks_v, pct_v in lossless_tbl.select(
				["_ft", "_bps", "_freq_khz", "_ch", "tracks", "pct"]
			).iter_rows():
				ft_s = str(ft or "(Unknown)").strip() or "(Unknown)"
				bps_s = str(int(bps_v)) if bps_v is not None and int(bps_v) > 0 else "Unknown"
				freq_s = _fmt_float_compact(freq_v)
				ch_s = str(int(ch_v)) if ch_v is not None and int(ch_v) > 0 else "Unknown"
				lossless_rows.append([
					ft_s,
					ch_s,
					bps_s,
					freq_s,
					_fmt_int(int(tracks_v)),
					_fmt_pct(float(pct_v)),
				])

		lossy_rows: list[list[str]] = []
		if not lossy_tbl.is_empty():
			for ft, ch_v, br_v, freq_v, tracks_v, pct_v in lossy_tbl.select(
				["_ft", "_ch", "_br", "_freq_khz", "tracks", "pct"]
			).iter_rows():
				ft_s = str(ft or "(Unknown)").strip() or "(Unknown)"
				ch_s = str(int(ch_v)) if ch_v is not None and int(ch_v) > 0 else "Unknown"
				br_s = str(int(br_v)) if br_v is not None and int(br_v) > 0 else "Unknown"
				freq_s = _fmt_float_compact(freq_v)
				lossy_rows.append([
					ft_s,
					ch_s,
					br_s,
					freq_s,
					_fmt_int(int(tracks_v)),
					_fmt_pct(float(pct_v)),
				])

		parts: list[str] = []
		parts.append(
			_render_table(
				title="Lossless breakdown",
				headers=[
					f"{system_prefix}filetype",
					f"{system_prefix}channels",
					f"{system_prefix}bitspersample",
					f"{system_prefix}frequency_num",
					"tracks",
					"% of lossless",
				],
				rows=lossless_rows,
				numeric_cols={1, 2, 3, 4, 5},
				note=f"Denominator: total lossless tracks = {n_lossless:,d}",
			)
		)
		parts.append(
			_render_table(
				title="Lossy breakdown",
				headers=[
					f"{system_prefix}filetype",
					f"{system_prefix}channels",
					f"{system_prefix}bitrate_num",
					f"{system_prefix}frequency_num",
					"tracks",
					"% of lossy",
				],
				rows=lossy_rows,
				numeric_cols={1, 2, 3, 4, 5},
				note=f"Denominator: total lossy tracks = {n_lossy:,d}",
			)
		)

		html_quality = "".join(parts)
	else:
		html_quality = "<div class=\"note\">No format-quality system data available.</div>"

	# 1c) ReplayGain loudness / peak / PLR proxy (album-scoped)
	df_rg_album: pl.DataFrame | None = None
	html_rg_by_genre = ""
	html_rg_dyn_missing_dr = ""
	html_rg_brick_missing_dr = ""
	missing_dr_cards_html = ""
	use_dr_meter = False
	dynamics_title = "ReplayGain — PLR proxy"
	dynamics_axis = "PLR proxy (LU)"
	dynamics_subnote = "PLR proxy uses: peak dBFS − (target LUFS − album_gain). Assumes ReplayGain reference target = -18 LUFS."
	dynamics_bucket_subtitle = "Median PLR proxy (higher = more headroom)"
	dynamics_trend_subtitle = "Median PLR proxy by year (peak dBFS − loudness LUFS)"
	dyn_most_title = "ReplayGain — Most dynamic (PLR proxy)"
	dyn_least_title = "ReplayGain — Least dynamic (PLR proxy)"
	dyn_most_missing_dr_title = "ReplayGain — Most dynamic (PLR proxy; missing DR)"
	dyn_least_missing_dr_title = "ReplayGain — Least dynamic (PLR proxy; missing DR)"
	rg_cols = {
		"replaygain_album_gain",
		"replaygain_album_peak",
		"replaygain_track_gain",
		"replaygain_track_peak",
	}
	if ("replaygain_album_gain" in df.columns) and ("replaygain_album_peak" in df.columns):
		# ReplayGain gain values are typically strings like "-7.84 dB"; peaks are linear (1.0 == 0 dBFS).
		rg = (
			df.select(
				[
					pl.col("album_root"),
					pl.col("replaygain_album_gain"),
					pl.col("replaygain_album_peak"),
					pl.col("replaygain_track_gain") if "replaygain_track_gain" in df.columns else pl.lit(None).alias("replaygain_track_gain"),
					pl.col("replaygain_track_peak") if "replaygain_track_peak" in df.columns else pl.lit(None).alias("replaygain_track_peak"),
				]
			)
			.with_columns(
				[
					_parse_signed_float(pl.col("replaygain_album_gain")).alias("rg_album_gain_db"),
					_parse_float(pl.col("replaygain_album_peak")).alias("rg_album_peak"),
					_parse_signed_float(pl.col("replaygain_track_gain")).alias("rg_track_gain_db"),
					_parse_float(pl.col("replaygain_track_peak")).alias("rg_track_peak"),
				]
			)
		)

		album_gain_peak = (
			rg.drop_nulls(["album_root"])
			.group_by("album_root")
			.agg(
				[
					pl.col("rg_album_gain_db").median().alias("rg_album_gain_db"),
					pl.col("rg_album_peak").max().alias("rg_album_peak"),
					pl.col("rg_album_gain_db").is_not_null().sum().alias("rg_album_gain_n"),
					pl.col("rg_album_peak").is_not_null().sum().alias("rg_album_peak_n"),
					(
						pl.col("rg_track_gain_db").quantile(0.90)
						- pl.col("rg_track_gain_db").quantile(0.10)
					).alias("rg_track_gain_p90_p10_db"),
				]
			)
		)

		# Optional labels: prefer Album Artist — Album when available.
		album_title_mode = (
			_mode_by_group(
				df.select(["album_root", "album"]),
				group_col="album_root",
				value_col="album",
				out_col="album_name",
			)
			if "album" in df.columns
			else pl.DataFrame({"album_root": [], "album_name": []})
		)

		if "albumartist" in df.columns:
			aa_primary = (
				df.select(
					[
						pl.col("album_root"),
						_tokens_expr("albumartist", delimiter=delimiter).alias("aa_tok"),
					]
				)
				.drop_nulls(["album_root"])
				.explode("aa_tok")
				.drop_nulls(["aa_tok"])
				.with_columns(_clean_text(pl.col("aa_tok")).alias("aa_tok"))
				.filter(pl.col("aa_tok").is_not_null() & (pl.col("aa_tok") != ""))
				.group_by(["album_root", "aa_tok"])
				.len()
				.sort(["album_root", "len"], descending=[False, True])
				.unique(subset=["album_root"], keep="first")
				.select(["album_root", pl.col("aa_tok").alias("albumartist_primary")])
			)
		else:
			aa_primary = pl.DataFrame({"album_root": [], "albumartist_primary": []})

		album_label = (
			df_album.select(["album_root", "album_dirname", "album_year"]).join(
				album_title_mode, on="album_root", how="left"
			).join(
				aa_primary, on="album_root", how="left"
			)
			.with_columns(
				[
					_clean_text(pl.col("album_name")).alias("album_name"),
					_clean_text(pl.col("albumartist_primary")).alias("albumartist_primary"),
				]
			)
			.with_columns(
				pl.when(
					pl.col("albumartist_primary").is_not_null()
					& (pl.col("albumartist_primary") != "")
					& pl.col("album_name").is_not_null()
					& (pl.col("album_name") != "")
				)
				.then(pl.col("albumartist_primary") + pl.lit(" — ") + pl.col("album_name"))
				.when(pl.col("album_name").is_not_null() & (pl.col("album_name") != ""))
				.then(pl.col("album_name"))
				.otherwise(pl.col("album_dirname"))
				.alias("album_label")
			)
		)

		# Compute derived metrics.
		target_lufs = -18.0
		df_rg_album = (
			album_gain_peak.join(
				album_label.select(["album_root", "album_year", "album_label"]),
				on="album_root",
				how="left",
			)
			.with_columns(
				[
					pl.when(pl.col("rg_album_peak") > 0)
					.then(pl.lit(20.0) * pl.col("rg_album_peak").log10())
					.otherwise(pl.lit(None))
					.alias("rg_album_peak_dbfs"),
					(pl.lit(target_lufs) - pl.col("rg_album_gain_db")).alias("rg_album_lufs"),
				]
			)
			.with_columns((pl.col("rg_album_peak_dbfs") - pl.col("rg_album_lufs")).alias("rg_album_plr"))
		)

		# DR Meter score (Pleasurize Music Foundation) is preferred when present.
		# Tagminder stores `album_dr` on every track (already album-scoped). For
		# album-level charts, pick a representative value per album_root.
		if "album_dr" in df.columns:
			dr_raw = (
				df.select(
					[
						pl.col("album_root"),
						_parse_int(pl.col("album_dr")).alias("album_dr_num"),
					]
				)
				.drop_nulls(["album_root", "album_dr_num"])
			)
			dr_album = _mode_by_group(
				dr_raw,
				group_col="album_root",
				value_col="album_dr_num",
				out_col="album_dr_num",
			).with_columns(pl.col("album_dr_num").cast(pl.Int32, strict=False))
			df_rg_album = df_rg_album.join(dr_album, on="album_root", how="left")

		use_dr_meter = bool(
			df_rg_album is not None
			and ("album_dr_num" in df_rg_album.columns)
			and df_rg_album.select(pl.col("album_dr_num").is_not_null().any()).item()
		)
		if use_dr_meter:
			dynamics_title = "DR Meter — album_dr"
			dynamics_axis = "DR (album_dr)"
			dynamics_subnote = "Uses `album_dr` when present (DR Meter score; Pleasurize Music Foundation procedure)."
			dynamics_bucket_subtitle = "Median DR (album_dr)"
			dynamics_trend_subtitle = "Median DR (album_dr) by year"
			dyn_most_title = "DR Meter — Most dynamic (album_dr)"
			dyn_least_title = "DR Meter — Least dynamic (album_dr)"

		# Loudest / quietest by album gain.
		loudest = (
			df_rg_album.drop_nulls(["rg_album_gain_db", "album_label"])
			.sort("rg_album_gain_db")
			.head(int(args.top))
		)
		quietest = (
			df_rg_album.drop_nulls(["rg_album_gain_db", "album_label"])
			.sort("rg_album_gain_db", descending=True)
			.head(int(args.top))
		)

		fig_loud = go.Figure(
			data=[
				go.Bar(
					y=loudest["album_label"].to_list(),
					x=loudest["rg_album_gain_db"].to_list(),
					orientation="h",
					marker_color="rgba(214, 39, 40, 0.80)",
					hovertemplate="%{y}<br>%{x:.2f} dB (album gain)<extra></extra>",
				),
			]
		)
		fig_loud.update_layout(
			template=template,
			height=max(420, int(len(loudest) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 320, "r": 25, "t": 25, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
			title={"text": "ReplayGain — Loudest albums (most negative album gain)", "x": 0.0, "xanchor": "left"},
		)
		fig_loud.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="ReplayGain album gain (dB)")
		fig_loud.update_yaxes(autorange="reversed", automargin=True)
		html_rg_loud = _to_html_fig(fig_loud, include_plotlyjs=include_js, div_id="tm-insights-rg-loudest")
		include_js = False

		fig_quiet = go.Figure(
			data=[
				go.Bar(
					y=quietest["album_label"].to_list(),
					x=quietest["rg_album_gain_db"].to_list(),
					orientation="h",
					marker_color="rgba(31, 119, 180, 0.75)",
					hovertemplate="%{y}<br>%{x:.2f} dB (album gain)<extra></extra>",
				),
			]
		)
		fig_quiet.update_layout(
			template=template,
			height=max(420, int(len(quietest) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 320, "r": 25, "t": 25, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
			title={"text": "ReplayGain — Quietest albums (most positive/least negative album gain)", "x": 0.0, "xanchor": "left"},
		)
		fig_quiet.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="ReplayGain album gain (dB)")
		fig_quiet.update_yaxes(autorange="reversed", automargin=True)
		html_rg_quiet = _to_html_fig(fig_quiet, include_plotlyjs=include_js, div_id="tm-insights-rg-quietest")
		include_js = False

		# Dynamics: prefer DR Meter when available; otherwise fall back to PLR proxy.
		if use_dr_meter:
			dyn = df_rg_album.drop_nulls(["album_dr_num", "album_label"])
			most_dyn = dyn.sort("album_dr_num", descending=True).head(int(args.top))
			least_dyn = dyn.sort("album_dr_num").head(int(args.top))

			fig_dyn = go.Figure(
				data=[
					go.Bar(
						y=most_dyn["album_label"].to_list(),
						x=most_dyn["album_dr_num"].to_list(),
						orientation="h",
						marker_color="rgba(44, 160, 44, 0.75)",
						hovertemplate="%{y}<br>DR%{x:.0f}<extra></extra>",
					),
				]
			)
			fig_dyn.update_layout(
				template=template,
				height=max(420, int(len(most_dyn) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
				margin={"l": 320, "r": 25, "t": 25, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
				title={"text": dyn_most_title, "x": 0.0, "xanchor": "left"},
			)
			fig_dyn.update_xaxes(gridcolor=grid_color, zeroline=False, title_text=dynamics_axis)
			fig_dyn.update_yaxes(autorange="reversed", automargin=True)
			html_rg_dyn = _to_html_fig(fig_dyn, include_plotlyjs=include_js, div_id="tm-insights-rg-most-dynamic")
			include_js = False

			fig_brick = go.Figure(
				data=[
					go.Bar(
						y=least_dyn["album_label"].to_list(),
						x=least_dyn["album_dr_num"].to_list(),
						orientation="h",
						marker_color="rgba(255, 127, 14, 0.75)",
						hovertemplate="%{y}<br>DR%{x:.0f}<extra></extra>",
					),
				]
			)
			fig_brick.update_layout(
				template=template,
				height=max(420, int(len(least_dyn) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
				margin={"l": 320, "r": 25, "t": 25, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
				title={"text": dyn_least_title, "x": 0.0, "xanchor": "left"},
			)
			fig_brick.update_xaxes(gridcolor=grid_color, zeroline=False, title_text=dynamics_axis)
			fig_brick.update_yaxes(autorange="reversed", automargin=True)
			html_rg_brick = _to_html_fig(fig_brick, include_plotlyjs=include_js, div_id="tm-insights-rg-least-dynamic")
			include_js = False
		else:
			# PLR proxy: (peak dBFS) - (integrated loudness LUFS). Higher => more headroom vs loudness.
			dyn = df_rg_album.drop_nulls(["rg_album_plr", "album_label"])
			most_dyn = dyn.sort("rg_album_plr", descending=True).head(int(args.top))
			least_dyn = dyn.sort("rg_album_plr").head(int(args.top))

			fig_dyn = go.Figure(
				data=[
					go.Bar(
						y=most_dyn["album_label"].to_list(),
						x=most_dyn["rg_album_plr"].to_list(),
						orientation="h",
						marker_color="rgba(44, 160, 44, 0.75)",
						customdata=most_dyn[["rg_album_gain_db", "rg_album_peak_dbfs"]].to_numpy(),
						hovertemplate="%{y}<br>%{x:.2f} LU (PLR proxy)<br>gain: %{customdata[0]:.2f} dB<br>peak: %{customdata[1]:.2f} dBFS<extra></extra>",
					),
				]
			)
			fig_dyn.update_layout(
				template=template,
				height=max(420, int(len(most_dyn) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
				margin={"l": 320, "r": 25, "t": 25, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
				title={"text": dyn_most_title, "x": 0.0, "xanchor": "left"},
			)
			fig_dyn.update_xaxes(gridcolor=grid_color, zeroline=False, title_text=dynamics_axis)
			fig_dyn.update_yaxes(autorange="reversed", automargin=True)
			html_rg_dyn = _to_html_fig(fig_dyn, include_plotlyjs=include_js, div_id="tm-insights-rg-most-dynamic")
			include_js = False

			fig_brick = go.Figure(
				data=[
					go.Bar(
						y=least_dyn["album_label"].to_list(),
						x=least_dyn["rg_album_plr"].to_list(),
						orientation="h",
						marker_color="rgba(255, 127, 14, 0.75)",
						customdata=least_dyn[["rg_album_gain_db", "rg_album_peak_dbfs"]].to_numpy(),
						hovertemplate="%{y}<br>%{x:.2f} LU (PLR proxy)<br>gain: %{customdata[0]:.2f} dB<br>peak: %{customdata[1]:.2f} dBFS<extra></extra>",
					),
				]
			)
			fig_brick.update_layout(
				template=template,
				height=max(420, int(len(least_dyn) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
				margin={"l": 320, "r": 25, "t": 25, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
				title={"text": dyn_least_title, "x": 0.0, "xanchor": "left"},
			)
			fig_brick.update_xaxes(gridcolor=grid_color, zeroline=False, title_text=dynamics_axis)
			fig_brick.update_yaxes(autorange="reversed", automargin=True)
			html_rg_brick = _to_html_fig(fig_brick, include_plotlyjs=include_js, div_id="tm-insights-rg-least-dynamic")
			include_js = False

		# If DR is present for *some* albums but missing for others, keep DR charts
		# as DR-only and add separate PLR-proxy charts for the missing-DR subset.
		if use_dr_meter and df_rg_album is not None and ("album_dr_num" in df_rg_album.columns):
			missing_dr = df_rg_album.filter(pl.col("album_dr_num").is_null())
			missing_dyn = missing_dr.drop_nulls(["rg_album_plr", "album_label"])
			if not missing_dyn.is_empty():
				most_miss = missing_dyn.sort("rg_album_plr", descending=True).head(int(args.top))
				least_miss = missing_dyn.sort("rg_album_plr").head(int(args.top))

				fig_miss_dyn = go.Figure(
					data=[
						go.Bar(
							y=most_miss["album_label"].to_list(),
							x=most_miss["rg_album_plr"].to_list(),
							orientation="h",
							marker_color="rgba(44, 160, 44, 0.75)",
							customdata=most_miss[["rg_album_gain_db", "rg_album_peak_dbfs"]].to_numpy(),
							hovertemplate="%{y}<br>%{x:.2f} LU (PLR proxy)<br>gain: %{customdata[0]:.2f} dB<br>peak: %{customdata[1]:.2f} dBFS<extra></extra>",
						),
					],
				)
				fig_miss_dyn.update_layout(
					template=template,
					height=max(420, int(len(most_miss) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
					margin={"l": 320, "r": 25, "t": 25, "b": 55},
					paper_bgcolor=page_bg,
					plot_bgcolor=page_bg,
					font={"size": 12, "color": page_fg},
					showlegend=False,
					title={"text": dyn_most_missing_dr_title, "x": 0.0, "xanchor": "left"},
				)
				fig_miss_dyn.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="PLR proxy (LU)")
				fig_miss_dyn.update_yaxes(autorange="reversed", automargin=True)
				html_rg_dyn_missing_dr = _to_html_fig(
					fig_miss_dyn,
					include_plotlyjs=include_js,
					div_id="tm-insights-rg-most-dynamic-missing-dr",
				)
				include_js = False

				fig_miss_brick = go.Figure(
					data=[
						go.Bar(
							y=least_miss["album_label"].to_list(),
							x=least_miss["rg_album_plr"].to_list(),
							orientation="h",
							marker_color="rgba(255, 127, 14, 0.75)",
							customdata=least_miss[["rg_album_gain_db", "rg_album_peak_dbfs"]].to_numpy(),
							hovertemplate="%{y}<br>%{x:.2f} LU (PLR proxy)<br>gain: %{customdata[0]:.2f} dB<br>peak: %{customdata[1]:.2f} dBFS<extra></extra>",
						),
					],
				)
				fig_miss_brick.update_layout(
					template=template,
					height=max(420, int(len(least_miss) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
					margin={"l": 320, "r": 25, "t": 25, "b": 55},
					paper_bgcolor=page_bg,
					plot_bgcolor=page_bg,
					font={"size": 12, "color": page_fg},
					showlegend=False,
					title={"text": dyn_least_missing_dr_title, "x": 0.0, "xanchor": "left"},
				)
				fig_miss_brick.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="PLR proxy (LU)")
				fig_miss_brick.update_yaxes(autorange="reversed", automargin=True)
				html_rg_brick_missing_dr = _to_html_fig(
					fig_miss_brick,
					include_plotlyjs=include_js,
					div_id="tm-insights-rg-least-dynamic-missing-dr",
				)
				include_js = False

				missing_dr_cards_html = (
					f"<div class=\"card\"><div class=\"card-title\">{escape(dyn_most_missing_dr_title)}</div><div class=\"plot-wrap\">"
					+ _safe_div(html_rg_dyn_missing_dr)
					+ "</div></div>"
					+ f"<div class=\"card\"><div class=\"card-title\">{escape(dyn_least_missing_dr_title)}</div><div class=\"plot-wrap\">"
					+ _safe_div(html_rg_brick_missing_dr)
					+ "</div></div>"
				)

		# Trend over time: year -> median album gain + median dynamics.
		trend = (
			df_rg_album.drop_nulls(["album_year"])
			.group_by("album_year")
			.agg(
				[
					pl.col("rg_album_gain_db").median().alias("gain_med"),
					(
						pl.col("album_dr_num").median()
						if use_dr_meter and ("album_dr_num" in df_rg_album.columns)
						else pl.col("rg_album_plr").median()
					).alias("dyn_med"),
					pl.len().alias("albums"),
				]
			)
			.drop_nulls(["gain_med", "dyn_med"])
			.sort("album_year")
		)
		if trend.is_empty():
			html_rg_trend = "<div class=\"note\">ReplayGain year trend unavailable (missing year/gain/peak data).</div>"
		else:
			x_year = trend["album_year"].to_list()
			gain_med = trend["gain_med"].to_list()
			dyn_med = trend["dyn_med"].to_list()
			n_alb = trend["albums"].to_list()

			fig_trend = make_subplots(
				rows=2,
				cols=1,
				shared_xaxes=True,
				vertical_spacing=0.18,
				subplot_titles=(
					"Median ReplayGain album gain by year (more negative = louder)",
					dynamics_trend_subtitle,
				),
			)
			fig_trend.add_trace(
				go.Scatter(
					x=x_year,
					y=gain_med,
					mode="lines+markers",
					line={"color": "rgba(214, 39, 40, 0.9)", "width": 2},
					marker={"size": 5},
					customdata=n_alb,
					hovertemplate="%{x}<br>%{y:.2f} dB median gain<br>%{customdata:,d} albums<extra></extra>",
				),
				row=1,
				col=1,
			)
			fig_trend.add_trace(
				go.Scatter(
					x=x_year,
					y=dyn_med,
					mode="lines+markers",
					line={"color": "rgba(44, 160, 44, 0.9)", "width": 2},
					marker={"size": 5},
					customdata=n_alb,
					hovertemplate="%{x}<br>%{y:.2f} median dynamics<br>%{customdata:,d} albums<extra></extra>",
				),
				row=2,
				col=1,
			)
			fig_trend.update_layout(
				template=template,
				height=520,
				margin={"l": 55, "r": 30, "t": 35, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
			)
			fig_trend.update_xaxes(showline=True, linecolor=axis_line_color, title_text="Year", row=2, col=1)
			fig_trend.update_yaxes(gridcolor=grid_color, zeroline=False)
			html_rg_trend = _to_html_fig(fig_trend, include_plotlyjs=include_js, div_id="tm-insights-rg-trend")
			include_js = False
	else:
		df_rg_album = None
		html_rg_by_genre = ""
		html_rg_loud = "<div class=\"note\">ReplayGain data unavailable (missing replaygain_album_gain / replaygain_album_peak).</div>"
		html_rg_quiet = ""
		html_rg_dyn = ""
		html_rg_brick = ""
		html_rg_trend = ""

	# 2) Albums over time (years + decades)
	if "album_year" in df_album.columns:
		by_year = (
			df_album.drop_nulls(["album_year"])
			.group_by("album_year")
			.len()
			.sort("album_year")
			.rename({"len": "albums"})
		)
		if by_year.is_empty():
			html_time = "<div class=\"note\">No album year data available.</div>"
		else:
			years = by_year["album_year"].to_list()
			albums = by_year["albums"].to_list()

			by_decade = (
				df_album.drop_nulls(["album_year"])
				.with_columns(((pl.col("album_year") // 10) * 10).alias("decade"))
				.group_by("decade")
				.len()
				.sort("decade")
				.rename({"len": "albums"})
			)
			decades = by_decade["decade"].to_list()
			decade_albums = by_decade["albums"].to_list()

			fig_time = make_subplots(
				rows=2,
				cols=1,
				shared_xaxes=False,
				vertical_spacing=0.20,
				subplot_titles=("Albums by decade", "Albums by year"),
			)
			fig_time.add_trace(
				go.Bar(
					x=decades,
					y=decade_albums,
					marker_color="rgba(31, 119, 180, 0.75)",
					hovertemplate="%{x}s<br>%{y:,d} albums<extra></extra>",
				),
				row=1,
				col=1,
			)
			fig_time.add_trace(
				go.Scatter(
					x=years,
					y=albums,
					mode="lines+markers",
					line={"color": "rgba(255, 127, 14, 0.9)", "width": 2},
					marker={"size": 5},
					hovertemplate="%{x}<br>%{y:,d} albums<extra></extra>",
				),
				row=2,
				col=1,
			)
			fig_time.update_layout(
				template=template,
				height=520,
				margin={"l": 35, "r": 25, "t": 35, "b": 45},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
			)
			fig_time.update_xaxes(showline=True, linecolor=axis_line_color)
			fig_time.update_yaxes(gridcolor=grid_color, zeroline=False)

			html_time = _to_html_fig(fig_time, include_plotlyjs=include_js, div_id="tm-insights-time")
			include_js = False
	else:
		html_time = "<div class=\"note\">No year data available.</div>"

	# 2a) Album acquisition over time (by ingest file-mod timestamp)
	if "album_acquired_year" in df_album.columns:
		acq_by_year = (
			df_album.drop_nulls(["album_acquired_year"])
			.group_by("album_acquired_year")
			.len()
			.sort("album_acquired_year")
			.rename({"len": "albums"})
		)
		if acq_by_year.is_empty():
			html_acq = "<div class=\"note\">No acquisition timestamp data available.</div>"
		else:
			years = acq_by_year["album_acquired_year"].to_list()
			albums = acq_by_year["albums"].to_list()

			fig_acq = go.Figure(
				data=[
					go.Bar(
						x=years,
						y=albums,
						marker_color="rgba(148, 103, 189, 0.75)",
						hovertemplate="%{x}<br>%{y:,d} albums<extra></extra>",
					),
				]
			)
			fig_acq.update_layout(
				template=template,
				height=420,
				margin={"l": 35, "r": 25, "t": 35, "b": 45},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
				title={"text": "Albums acquired by year", "x": 0.0, "xanchor": "left"},
			)
			fig_acq.update_xaxes(showline=True, linecolor=axis_line_color, title_text="Year")
			fig_acq.update_yaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")

			html_acq = _to_html_fig(fig_acq, include_plotlyjs=include_js, div_id="tm-insights-acquisition")
			include_js = False
	else:
		html_acq = "<div class=\"note\">No acquisition timestamp data available.</div>"

	# 2b) Artist depth vs breadth (albums vs distinct years)
	if not aa_pairs.is_empty() and "album_year" in df_album.columns:
		aa_albums = aa_pairs.group_by("albumartist_tok").len().rename({"len": "albums"})
		aa_years = (
			aa_pairs.join(df_album.select(["album_root", "album_year"]), on="album_root", how="left")
			.drop_nulls(["album_year"])
			.group_by("albumartist_tok")
			.agg(pl.col("album_year").n_unique().alias("distinct_years"))
		)
		aa_depth = (
			aa_albums.join(aa_years, on="albumartist_tok", how="left")
			.with_columns(pl.col("distinct_years").fill_null(1).cast(pl.Int32))
			.sort("albums", descending=True)
		)
		aa_sc = aa_depth.head(600)
		x = aa_sc["albums"].to_list()
		y = aa_sc["distinct_years"].to_list()
		text = aa_sc["albumartist_tok"].to_list()
		sizes = [min(18, max(6, (float(v) ** 0.5) * 3.2)) for v in x]

		fig_ab = go.Figure(
			data=[
				go.Scatter(
					x=x,
					y=y,
					mode="markers",
					text=text,
					marker={
						"size": sizes,
						"color": y,
						"colorscale": "Viridis",
						"showscale": True,
						"colorbar": {"title": "Distinct years"},
						"opacity": 0.82,
					},
					hovertemplate="%{text}<br>%{x:,d} albums<br>%{y:,d} distinct years<extra></extra>",
				),
			]
		)
		fig_ab.update_layout(
			template=template,
			height=520,
			margin={"l": 55, "r": 30, "t": 25, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_ab.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
		fig_ab.update_yaxes(gridcolor=grid_color, zeroline=False, title_text="Distinct release years")

		html_artist_depth = _to_html_fig(fig_ab, include_plotlyjs=include_js, div_id="tm-insights-artist-depth")
		include_js = False
	else:
		html_artist_depth = "<div class=\"note\">No albumartist/year data available for depth vs breadth.</div>"

	# 3) Album count by albumartist
	if not aa_pairs.is_empty():
		aa_counts = aa_pairs.group_by("albumartist_tok").len().sort("len", descending=True)
		aa_top = aa_counts.head(max(30, int(args.top)))

		fig_aa = go.Figure()
		fig_aa.add_trace(
			go.Bar(
				y=aa_top["albumartist_tok"].to_list(),
				x=aa_top["len"].to_list(),
				orientation="h",
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
			)
		)
		fig_aa.update_layout(
			template=template,
			height=max(420, int(len(aa_top) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 220, "r": 25, "t": 25, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_aa.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
		fig_aa.update_yaxes(autorange="reversed")

		html_aa = _to_html_fig(fig_aa, include_plotlyjs=include_js, div_id="tm-insights-albums-per-artist")
		include_js = False
	else:
		html_aa = "<div class=\"note\">No albumartist data available.</div>"

	# 4) Releasetype by artist (stacked)
	if not aa_pairs.is_empty() and "album_releasetype" in df_album.columns:
		album_types = df_album.select(["album_root", "album_releasetype"]).with_columns(
			pl.col("album_releasetype").fill_null("Unknown")
		)
		aa_types = aa_pairs.join(album_types, on="album_root", how="left").with_columns(
			pl.col("album_releasetype").fill_null("Unknown")
		)

		totals = aa_types.group_by("albumartist_tok").len().sort("len", descending=True)
		top_artists = totals.head(15)["albumartist_tok"].to_list()

		type_totals = (
			aa_types.filter(pl.col("albumartist_tok").is_in(top_artists))
			.group_by("album_releasetype")
			.len()
			.sort("len", descending=True)
		)
		top_types = type_totals.head(6)["album_releasetype"].to_list()

		aa_types2 = aa_types.filter(pl.col("albumartist_tok").is_in(top_artists)).with_columns(
			pl.when(pl.col("album_releasetype").is_in(top_types))
			.then(pl.col("album_releasetype"))
			.otherwise(pl.lit("Other"))
			.alias("rtype")
		)

		pivot = (
			aa_types2.group_by(["albumartist_tok", "rtype"]).len()
			.pivot(values="len", index="albumartist_tok", on="rtype")
			.fill_null(0)
		)

		order_map = {a: i for i, a in enumerate(top_artists)}
		pivot = (
			pivot.with_columns(
				pl.col("albumartist_tok")
				.map_elements(lambda x: order_map.get(x, 9999), return_dtype=pl.Int32)
				.alias("_ord")
			)
			.sort("_ord")
			.drop(["_ord"])
		)

		rtypes = [c for c in pivot.columns if c != "albumartist_tok"]
		fig_rt = go.Figure()
		palette = [
			"rgba(31, 119, 180, 0.80)",
			"rgba(255, 127, 14, 0.80)",
			"rgba(44, 160, 44, 0.80)",
			"rgba(214, 39, 40, 0.80)",
			"rgba(148, 103, 189, 0.80)",
			"rgba(140, 86, 75, 0.80)",
			"rgba(127, 127, 127, 0.80)",
		]

		for i, rt in enumerate(rtypes):
			fig_rt.add_trace(
				go.Bar(
					name=str(rt),
					y=pivot["albumartist_tok"].to_list(),
					x=pivot[rt].to_list(),
					orientation="h",
					marker_color=palette[i % len(palette)],
					hovertemplate="%{y}<br>" + escape(str(rt)) + ": %{x:,d} albums<extra></extra>",
				)
			)

		fig_rt.update_layout(
			template=template,
			barmode="stack",
			height=max(520, int(len(top_artists) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 220, "r": 25, "t": 25, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			legend={"orientation": "h", "y": -0.15, "x": 0},
		)
		fig_rt.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
		fig_rt.update_yaxes(autorange="reversed", automargin=True)

		html_rt = _to_html_fig(fig_rt, include_plotlyjs=include_js, div_id="tm-insights-releasetype")
		include_js = False
	else:
		html_rt = "<div class=\"note\">No releasetype-by-artist view available.</div>"

	# 5) Ratings
	rating_present = bool(df.select(pl.col("rating_num").is_not_null().any()).item())
	if rating_present:
		rated = df.drop_nulls(["rating_num"]).select(["rating_num"])

		fig_rate = make_subplots(
			rows=1,
			cols=2,
			subplot_titles=("Rating distribution (tracks)", "Top album artists by avg album rating"),
			column_widths=[0.52, 0.48],
			horizontal_spacing=0.10,
		)
		fig_rate.add_trace(
			go.Histogram(
				x=rated["rating_num"].to_list(),
				nbinsx=20,
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{x}<br>%{y:,d} tracks<extra></extra>",
			),
			row=1,
			col=1,
		)

		if not aa_pairs.is_empty() and "album_rating_avg" in df_album.columns:
			albums_with_rating = df_album.drop_nulls(["album_rating_avg"]).select(
				["album_root", "album_rating_avg"]
			)
			aa_album_rating = aa_pairs.join(albums_with_rating, on="album_root", how="inner")

			min_albums = 5
			rated_by_artist = (
				aa_album_rating.group_by("albumartist_tok")
				.agg(
					[
						pl.len().alias("albums"),
						pl.col("album_rating_avg").mean().alias("avg_rating"),
					]
				)
				.filter(pl.col("albums") >= min_albums)
				.sort("avg_rating", descending=True)
			)

			if not rated_by_artist.is_empty():
				top_r = rated_by_artist.head(20)
				fig_rate.add_trace(
					go.Bar(
						y=top_r["albumartist_tok"].to_list(),
						x=top_r["avg_rating"].to_list(),
						orientation="h",
						marker_color="rgba(255, 127, 14, 0.75)",
						hovertemplate="%{y}<br>%{x:.2f} avg rating<extra></extra>",
					),
					row=1,
					col=2,
				)

		fig_rate.update_layout(
			template=template,
			height=420,
			margin={"l": 35, "r": 25, "t": 35, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_rate.update_xaxes(showline=True, linecolor=axis_line_color)
		fig_rate.update_yaxes(gridcolor=grid_color, zeroline=False)
		fig_rate.update_yaxes(autorange="reversed", row=1, col=2)

		html_rate = _to_html_fig(fig_rate, include_plotlyjs=include_js, div_id="tm-insights-rating")
		include_js = False
	else:
		html_rate = "<div class=\"note\">No rating data available.</div>"

	# 6) VA / compilation share
	if "album_is_va_or_comp" in df_album.columns and "album_year" in df_album.columns:
		n_albums = float(df_album.height)
		n_va = float(df_album.select(pl.col("album_is_va_or_comp").sum()).item())
		by_dec = (
			df_album.drop_nulls(["album_year"])
			.with_columns(((pl.col("album_year") // 10) * 10).alias("decade"))
			.group_by("decade")
			.agg(
				[
					pl.len().alias("albums"),
					pl.col("album_is_va_or_comp").sum().alias("va_albums"),
				]
			)
			.with_columns((pl.col("va_albums") * 100.0 / pl.col("albums")).alias("va_pct"))
			.sort("decade")
		)

		fig_va = make_subplots(
			rows=1,
			cols=2,
			specs=[[{"type": "domain"}, {"type": "xy"}]],
			# Keep these short; the card title already provides context and longer
			# subplot headings can overlap on narrower screens.
			subplot_titles=("Share (albums)", "Share by decade"),
			horizontal_spacing=0.12,
			column_widths=[0.42, 0.58],
		)
		fig_va.add_trace(
			go.Pie(
				labels=["VA/Compilation", "Other"],
				values=[n_va, max(0.0, n_albums - n_va)],
				hole=0.58,
				marker_colors=["rgba(255, 127, 14, 0.85)", "rgba(31, 119, 180, 0.55)"],
				textinfo="label+percent",
				hovertemplate="%{label}<br>%{value:.0f} albums<extra></extra>",
			),
			row=1,
			col=1,
		)
		fig_va.add_trace(
			go.Scatter(
				x=by_dec["decade"].to_list(),
				y=by_dec["va_pct"].to_list(),
				mode="lines+markers",
				line={"color": "rgba(255, 127, 14, 0.9)", "width": 2},
				marker={"size": 6},
				hovertemplate="%{x}s<br>%{y:.2f}%<extra></extra>",
			),
			row=1,
			col=2,
		)

		fig_va.update_layout(
			template=template,
			height=420,
			margin={"l": 35, "r": 25, "t": 48, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_va.update_annotations(font={"size": 12})
		fig_va.update_xaxes(showline=True, linecolor=axis_line_color)
		fig_va.update_yaxes(gridcolor=grid_color, zeroline=False, ticksuffix="%")

		html_va = _to_html_fig(fig_va, include_plotlyjs=include_js, div_id="tm-insights-va")
		include_js = False
	else:
		html_va = "<div class=\"note\">No compilation/VA data available.</div>"

	# 7) Producers
	if "producer" in df.columns:
		prod_pairs = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("producer", delimiter=delimiter).alias("producer_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("producer_tok")
			.drop_nulls(["producer_tok"])
		)

		prod_album = (
			prod_pairs.unique(subset=["album_root", "producer_tok"])
			.group_by("producer_tok")
			.len()
			.sort("len", descending=True)
		)
		top_prod = prod_album.head(max(30, int(args.top)))
		n_prod_row1 = int(top_prod.height)
		n_prod_row2 = 0

		prod_spread = None
		prod_top_collab = None
		if not aa_pairs.is_empty():
			prod_spread = (
				prod_pairs.unique(subset=["album_root", "producer_tok"])
				.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.group_by("producer_tok")
				.agg(pl.col("albumartist_tok").n_unique().alias("artist_spread"))
				.sort("artist_spread", descending=True)
			)
			prod_top_collab = (
				prod_pairs.unique(subset=["album_root", "producer_tok"])
				.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.unique(subset=["album_root", "producer_tok", "albumartist_tok"])
				.group_by(["producer_tok", "albumartist_tok"])
				.len()
				.sort(["producer_tok", "len"], descending=[False, True])
				.unique(subset=["producer_tok"], keep="first")
				.rename({"len": "albums_with_artist"})
				.sort("albums_with_artist", descending=True)
			)

		fig_prod = make_subplots(
			rows=2,
			cols=2,
			specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy", "colspan": 2}, None]],
			subplot_titles=(
				"Top producers (albums)",
				"Producers by artist spread",
				"Top collaborator per producer (albums with most-worked-with album artist)",
			),
			horizontal_spacing=0.12,
			vertical_spacing=0.18,
			column_widths=[0.55, 0.45],
		)
		fig_prod.add_trace(
			go.Bar(
				y=top_prod["producer_tok"].to_list(),
				x=top_prod["len"].to_list(),
				orientation="h",
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
			),
			row=1,
			col=1,
		)
		if prod_spread is not None and not prod_spread.is_empty():
			top_sp = prod_spread.head(20)
			n_prod_row1 = max(n_prod_row1, int(top_sp.height))
			fig_prod.add_trace(
				go.Bar(
					y=top_sp["producer_tok"].to_list(),
					x=top_sp["artist_spread"].to_list(),
					orientation="h",
					marker_color="rgba(255, 127, 14, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} distinct album artists<extra></extra>",
				),
				row=1,
				col=2,
			)
		if prod_top_collab is not None and not prod_top_collab.is_empty():
			top_pc = (
				prod_top_collab.drop_nulls(["producer_tok", "albumartist_tok"])
				.with_columns(
					[
						pl.col("producer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
						pl.col("albumartist_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
					]
				)
				.filter((pl.col("producer_tok") != "") & (pl.col("albumartist_tok") != ""))
				.head(20)
				.with_columns(
					(pl.col("producer_tok") + pl.lit(" | ") + pl.col("albumartist_tok")).alias("label")
				)
				.filter(pl.col("label").is_not_null() & (pl.col("label") != ""))
			)
			n_prod_row2 = int(top_pc.height)
			fig_prod.add_trace(
				go.Bar(
					y=top_pc["label"].to_list(),
					x=top_pc["albums_with_artist"].to_list(),
					orientation="h",
					marker_color="rgba(44, 160, 44, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
				),
				row=2,
				col=1,
			)

		fig_prod.update_layout(
			template=template,
			height=max(
				720,
				int((n_prod_row1 + n_prod_row2) * _HBAR_ROW_PX + (_HBAR_EXTRA_PX + 260)),
			),
			margin={"l": 35, "r": 25, "t": 35, "b": 70},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_prod.update_xaxes(gridcolor=grid_color, zeroline=False)
		fig_prod.update_yaxes(autorange="reversed", automargin=True)

		html_prod = _to_html_fig(fig_prod, include_plotlyjs=include_js, div_id="tm-insights-producers")
		include_js = False
	else:
		html_prod = "<div class=\"note\">No producer data available.</div>"

	# 7b) Engineers (mirrors Producers)
	if "engineer" in df.columns:
		eng_pairs = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("engineer", delimiter=delimiter).alias("engineer_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("engineer_tok")
			.drop_nulls(["engineer_tok"])
		)

		eng_album = (
			eng_pairs.unique(subset=["album_root", "engineer_tok"])
			.group_by("engineer_tok")
			.len()
			.sort("len", descending=True)
		)
		top_eng = eng_album.head(max(30, int(args.top)))
		n_eng_row1 = int(top_eng.height)
		n_eng_row2 = 0

		eng_spread = None
		eng_top_collab = None
		if not aa_pairs.is_empty():
			eng_spread = (
				eng_pairs.unique(subset=["album_root", "engineer_tok"])
				.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.group_by("engineer_tok")
				.agg(pl.col("albumartist_tok").n_unique().alias("artist_spread"))
				.sort("artist_spread", descending=True)
			)
			eng_top_collab = (
				eng_pairs.unique(subset=["album_root", "engineer_tok"])
				.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.unique(subset=["album_root", "engineer_tok", "albumartist_tok"])
				.group_by(["engineer_tok", "albumartist_tok"])
				.len()
				.sort(["engineer_tok", "len"], descending=[False, True])
				.unique(subset=["engineer_tok"], keep="first")
				.rename({"len": "albums_with_artist"})
				.sort("albums_with_artist", descending=True)
			)

		fig_eng = make_subplots(
			rows=2,
			cols=2,
			specs=[[{"type": "xy"}, {"type": "xy"}], [{"type": "xy", "colspan": 2}, None]],
			subplot_titles=(
				"Top engineers (albums)",
				"Engineers by artist spread",
				"Top collaborator per engineer (albums with most-worked-with album artist)",
			),
			horizontal_spacing=0.12,
			vertical_spacing=0.18,
			column_widths=[0.55, 0.45],
		)
		fig_eng.add_trace(
			go.Bar(
				y=top_eng["engineer_tok"].to_list(),
				x=top_eng["len"].to_list(),
				orientation="h",
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
			),
			row=1,
			col=1,
		)
		if eng_spread is not None and not eng_spread.is_empty():
			top_es = eng_spread.head(20)
			n_eng_row1 = max(n_eng_row1, int(top_es.height))
			fig_eng.add_trace(
				go.Bar(
					y=top_es["engineer_tok"].to_list(),
					x=top_es["artist_spread"].to_list(),
					orientation="h",
					marker_color="rgba(255, 127, 14, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} distinct album artists<extra></extra>",
				),
				row=1,
				col=2,
			)
		if eng_top_collab is not None and not eng_top_collab.is_empty():
			top_ec = (
				eng_top_collab.drop_nulls(["engineer_tok", "albumartist_tok"])
				.with_columns(
					[
						pl.col("engineer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
						pl.col("albumartist_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
					]
				)
				.filter((pl.col("engineer_tok") != "") & (pl.col("albumartist_tok") != ""))
				.head(20)
				.with_columns(
					(pl.col("engineer_tok") + pl.lit(" | ") + pl.col("albumartist_tok")).alias("label")
				)
				.filter(pl.col("label").is_not_null() & (pl.col("label") != ""))
			)
			n_eng_row2 = int(top_ec.height)
			fig_eng.add_trace(
				go.Bar(
					y=top_ec["label"].to_list(),
					x=top_ec["albums_with_artist"].to_list(),
					orientation="h",
					marker_color="rgba(44, 160, 44, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
				),
				row=2,
				col=1,
			)

		fig_eng.update_layout(
			template=template,
			height=max(
				720,
				int((n_eng_row1 + n_eng_row2) * _HBAR_ROW_PX + (_HBAR_EXTRA_PX + 260)),
			),
			margin={"l": 35, "r": 25, "t": 35, "b": 70},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_eng.update_xaxes(gridcolor=grid_color, zeroline=False)
		fig_eng.update_yaxes(autorange="reversed", automargin=True)

		html_eng = _to_html_fig(fig_eng, include_plotlyjs=include_js, div_id="tm-insights-engineers")
		include_js = False
	else:
		html_eng = "<div class=\"note\">No engineer data available.</div>"

	# 7c) Producer ↔ Engineer collaboration patterns
	# Album-level co-occurrence: (producer_tok, engineer_tok) pairs per album_root.
	if "producer" in df.columns and "engineer" in df.columns:
		pe_prod = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("producer", delimiter=delimiter).alias("producer_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("producer_tok")
			.drop_nulls(["producer_tok"])
			.unique(subset=["album_root", "producer_tok"])
		)
		pe_eng = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("engineer", delimiter=delimiter).alias("engineer_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("engineer_tok")
			.drop_nulls(["engineer_tok"])
			.unique(subset=["album_root", "engineer_tok"])
		)

		pe_pairs = (
			pe_prod.join(pe_eng, on="album_root", how="inner")
			.unique(subset=["album_root", "producer_tok", "engineer_tok"])
		)

		if pe_pairs.is_empty():
			html_pe = "<div class=\"note\">No producer/engineer co-occurrence data available.</div>"
		else:
			pe_counts = (
				pe_pairs.group_by(["producer_tok", "engineer_tok"]).len().rename({"len": "albums"})
			)

			# For each producer, engineer with max shared albums.
			prod_top_eng = (
				pe_counts.sort(["producer_tok", "albums"], descending=[False, True])
				.unique(subset=["producer_tok"], keep="first")
				.sort("albums", descending=True)
			)
			# For each engineer, producer with max shared albums.
			eng_top_prod = (
				pe_counts.sort(["engineer_tok", "albums"], descending=[False, True])
				.unique(subset=["engineer_tok"], keep="first")
				.sort("albums", descending=True)
			)

			prod_top = (
				prod_top_eng.drop_nulls(["producer_tok", "engineer_tok"]).with_columns(
					[
						pl.col("producer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
						pl.col("engineer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
					]
				)
				.filter((pl.col("producer_tok") != "") & (pl.col("engineer_tok") != ""))
				.head(20)
				.with_columns(
					(pl.col("producer_tok") + pl.lit(" | ") + pl.col("engineer_tok")).alias("label")
				)
				.filter(pl.col("label").is_not_null() & (pl.col("label") != ""))
			)
			eng_top = (
				eng_top_prod.drop_nulls(["engineer_tok", "producer_tok"]).with_columns(
					[
						pl.col("engineer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
						pl.col("producer_tok").cast(pl.Utf8, strict=False).str.strip_chars(),
					]
				)
				.filter((pl.col("engineer_tok") != "") & (pl.col("producer_tok") != ""))
				.head(20)
				.with_columns(
					(pl.col("engineer_tok") + pl.lit(" | ") + pl.col("producer_tok")).alias("label")
				)
				.filter(pl.col("label").is_not_null() & (pl.col("label") != ""))
			)

			fig_pe = make_subplots(
				rows=1,
				cols=2,
				subplot_titles=(
					"Producers → most frequent engineer (albums together)",
					"Engineers → most frequent producer (albums together)",
				),
				horizontal_spacing=0.12,
				column_widths=[0.5, 0.5],
			)

			if not prod_top.is_empty():
				fig_pe.add_trace(
					go.Bar(
						y=prod_top["label"].to_list(),
						x=prod_top["albums"].to_list(),
						orientation="h",
						marker_color="rgba(31, 119, 180, 0.75)",
						hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
					),
					row=1,
					col=1,
				)

			if not eng_top.is_empty():
				fig_pe.add_trace(
					go.Bar(
						y=eng_top["label"].to_list(),
						x=eng_top["albums"].to_list(),
						orientation="h",
						marker_color="rgba(255, 127, 14, 0.75)",
						hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
					),
					row=1,
					col=2,
				)

			fig_pe.update_layout(
				template=template,
				height=560,
				margin={"l": 35, "r": 25, "t": 35, "b": 70},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
			)
			fig_pe.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
			fig_pe.update_yaxes(autorange="reversed", automargin=True, dtick=1)

			html_pe = _to_html_fig(fig_pe, include_plotlyjs=include_js, div_id="tm-insights-producer-engineer")
			include_js = False
	else:
		html_pe = "<div class=\"note\">No producer/engineer co-occurrence data available.</div>"

	# 8) Labels
	if "label" in df.columns:
		label_pairs = (
			df.select(
				[
					pl.col("album_root"),
					_tokens_expr("label", delimiter=delimiter).alias("label_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("label_tok")
			.drop_nulls(["label_tok"])
			.unique(subset=["album_root", "label_tok"])
		)

		label_album = label_pairs.group_by("label_tok").len().sort("len", descending=True)
		top_label = label_album.head(max(30, int(args.top)))

		label_spread = None
		if not aa_pairs.is_empty():
			label_spread = (
				label_pairs.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.group_by("label_tok")
				.agg(
					[
						pl.len().alias("albums"),
						pl.col("albumartist_tok").n_unique().alias("artist_spread"),
					]
				)
				.filter(pl.col("albums") >= 5)
				.sort("artist_spread", descending=True)
			)

		fig_label = make_subplots(
			rows=1,
			cols=2,
			subplot_titles=("Top labels (albums)", "Labels by artist spread"),
			horizontal_spacing=0.12,
			column_widths=[0.55, 0.45],
		)
		fig_label.add_trace(
			go.Bar(
				y=top_label["label_tok"].to_list(),
				x=top_label["len"].to_list(),
				orientation="h",
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
			),
			row=1,
			col=1,
		)
		if label_spread is not None and not label_spread.is_empty():
			top_ls = label_spread.head(20)
			fig_label.add_trace(
				go.Bar(
					y=top_ls["label_tok"].to_list(),
					x=top_ls["artist_spread"].to_list(),
					orientation="h",
					marker_color="rgba(255, 127, 14, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} distinct album artists<extra></extra>",
				),
				row=1,
				col=2,
			)

		label_n = int(top_label.height)
		if label_spread is not None and not label_spread.is_empty():
			label_n = max(label_n, 20)
		fig_label.update_layout(
			template=template,
			height=max(520, int(label_n * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 35, "r": 25, "t": 35, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_label.update_xaxes(gridcolor=grid_color, zeroline=False)
		fig_label.update_yaxes(autorange="reversed", automargin=True)

		html_label = _to_html_fig(fig_label, include_plotlyjs=include_js, div_id="tm-insights-labels")
		include_js = False
	else:
		html_label = "<div class=\"note\">No label data available.</div>"

	# 8b) Studios / Locations (optional retained tags)
	studio_field = None
	for cand in ("studio", "recordedat", "recordinglocation"):
		if cand in df.columns:
			studio_field = cand
			break

	if studio_field is not None:
		st_pairs = (
			df.select([pl.col("album_root"), _tokens_expr(studio_field, delimiter=delimiter).alias("tok")])
			.drop_nulls(["album_root"])
			.explode("tok")
			.drop_nulls(["tok"])
			.unique(subset=["album_root", "tok"])
		)
		st_album = st_pairs.group_by("tok").len().sort("len", descending=True)
		top_st = st_album.head(max(30, int(args.top)))

		st_spread = None
		if not aa_pairs.is_empty():
			st_spread = (
				st_pairs.join(aa_pairs, on="album_root", how="left")
				.drop_nulls(["albumartist_tok"])
				.group_by("tok")
				.agg(
					[
						pl.len().alias("albums"),
						pl.col("albumartist_tok").n_unique().alias("artist_spread"),
					]
				)
				.filter(pl.col("albums") >= 5)
				.sort("artist_spread", descending=True)
			)

		fig_st = make_subplots(
			rows=1,
			cols=2,
			subplot_titles=(f"Top {studio_field} (albums)", f"{studio_field} by artist spread"),
			horizontal_spacing=0.12,
			column_widths=[0.55, 0.45],
		)
		fig_st.add_trace(
			go.Bar(
				y=top_st["tok"].to_list(),
				x=top_st["len"].to_list(),
				orientation="h",
				marker_color="rgba(31, 119, 180, 0.75)",
				hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
			),
			row=1,
			col=1,
		)
		if st_spread is not None and not st_spread.is_empty():
			top_ss = st_spread.head(20)
			fig_st.add_trace(
				go.Bar(
					y=top_ss["tok"].to_list(),
					x=top_ss["artist_spread"].to_list(),
					orientation="h",
					marker_color="rgba(255, 127, 14, 0.75)",
					hovertemplate="%{y}<br>%{x:,d} distinct album artists<extra></extra>",
				),
				row=1,
				col=2,
			)

		st_n = int(top_st.height)
		if st_spread is not None and not st_spread.is_empty():
			st_n = max(st_n, 20)
		fig_st.update_layout(
			template=template,
			height=max(520, int(st_n * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 35, "r": 25, "t": 35, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig_st.update_xaxes(gridcolor=grid_color, zeroline=False)
		fig_st.update_yaxes(autorange="reversed", automargin=True)

		html_studio = _to_html_fig(fig_st, include_plotlyjs=include_js, div_id="tm-insights-studios")
		include_js = False
	else:
		html_studio = "<div class=\"note\">No studio/location tags available (studio/recordedat/recordinglocation).</div>"

	# Persona spotlight
	spotlight_html = None
	highlights_html = None

	def _top_role(role_col: str, title: str, color: str, div_id: str) -> str | None:
		nonlocal include_js
		if role_col not in df.columns:
			return None
		pairs = (
			df.select([pl.col("album_root"), _tokens_expr(role_col, delimiter=delimiter).alias("tok")])
			.drop_nulls(["album_root"])
			.explode("tok")
			.drop_nulls(["tok"])
			.unique(subset=["album_root", "tok"])
		)
		if pairs.is_empty():
			return None
		top = pairs.group_by("tok").len().sort("len", descending=True).head(20)
		n_items = int(top.height)
		fig = go.Figure(
			data=[
				go.Bar(
					y=top["tok"].to_list(),
					x=top["len"].to_list(),
					orientation="h",
					marker_color=color,
					hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
				)
			]
		)
		fig.update_layout(
			template=template,
			height=max(420, int(n_items * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 220, "r": 25, "t": 25, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
		fig.update_yaxes(autorange="reversed", automargin=True)

		html = _to_html_fig(fig, include_plotlyjs=include_js, div_id=div_id)
		include_js = False
		return (
			f"<div class=\"card\"><div class=\"card-title\">{escape(title)}</div>"
			f"<div class=\"plot-wrap\">{html}</div></div>"
		)

	def _top_pairs_card(
		pairs: pl.DataFrame,
		*,
		token_col: str,
		title: str,
		color: str,
		div_id: str,
		height: int = 420,
		top_n: int = 20,
	) -> str | None:
		nonlocal include_js
		if pairs.is_empty() or token_col not in pairs.columns:
			return None
		top = pairs.group_by(token_col).len().sort("len", descending=True).head(top_n)
		if top.is_empty():
			return None
		n_items = int(top.height)
		fig = go.Figure(
			data=[
				go.Bar(
					y=top[token_col].to_list(),
					x=top["len"].to_list(),
					orientation="h",
					marker_color=color,
					hovertemplate="%{y}<br>%{x:,d} albums<extra></extra>",
				)
			]
		)
		fig.update_layout(
			template=template,
			height=max(int(height), int(n_items * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 220, "r": 25, "t": 25, "b": 45},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")
		fig.update_yaxes(autorange="reversed", automargin=True)

		html = _to_html_fig(fig, include_plotlyjs=include_js, div_id=div_id)
		include_js = False
		return (
			f"<div class=\"card\"><div class=\"card-title\">{escape(title)}</div>"
			f"<div class=\"plot-wrap\">{html}</div></div>"
		)

	def _most_instances_card(
		mask_expr: pl.Expr,
		*,
		title: str,
		color: str,
		div_id: str,
		top_n: int,
	) -> str | None:
		"""Top compositions by performer diversity.

		Definition: group by (composer, title); rank by distinct performing artists.
		"""
		nonlocal include_js
		needed = {"title", "artist", "composer"}
		if not needed.issubset(set(df_genre_tok.columns)):
			return None

		scoped = df_genre_tok.filter(mask_expr)
		if scoped.is_empty():
			return None

		base = (
			scoped.select(
				[
					_clean_text(pl.col("title")).alias("_title"),
					_tokens_expr("artist", delimiter=delimiter).alias("_artist_tok"),
					_tokens_expr("composer", delimiter=delimiter).alias("_composer_tok"),
				]
			)
			.drop_nulls(["_title"])
			.with_columns(
				[
					pl.col("_title")
					.str.to_lowercase()
					.str.replace_all(r"\\s+", " ")
					.str.strip_chars()
					.alias("title_key"),
					pl.col("_composer_tok")
					.list.eval(pl.element().str.to_lowercase())
					.list.sort()
					.list.join(" / ")
					.alias("composer_key"),
					pl.col("_composer_tok").list.sort().list.join(" / ").alias("composer_disp"),
					pl.col("_title").alias("title_disp"),
				]
			)
			.filter(
				pl.col("composer_key").is_not_null()
				& (pl.col("composer_key") != "")
				& pl.col("_artist_tok").list.len().gt(0)
			)
		)

		# Group strictly by normalized keys; choose representative display values via mode.
		tracks = base.group_by(["composer_key", "title_key"]).len().rename({"len": "tracks"})
		title_mode = (
			base.select(["composer_key", "title_key", "title_disp"])
			.drop_nulls(["composer_key", "title_key", "title_disp"])
			.group_by(["composer_key", "title_key", "title_disp"])
			.len()
			.sort(["composer_key", "title_key", "len"], descending=[False, False, True])
			.unique(subset=["composer_key", "title_key"], keep="first")
			.select(["composer_key", "title_key", pl.col("title_disp")])
		)
		composer_mode = (
			base.select(["composer_key", "title_key", "composer_disp"])
			.drop_nulls(["composer_key", "title_key", "composer_disp"])
			.group_by(["composer_key", "title_key", "composer_disp"])
			.len()
			.sort(["composer_key", "title_key", "len"], descending=[False, False, True])
			.unique(subset=["composer_key", "title_key"], keep="first")
			.select(["composer_key", "title_key", pl.col("composer_disp")])
		)
		artists = (
			base.select(["composer_key", "title_key", pl.col("_artist_tok")])
			.explode("_artist_tok")
			.drop_nulls(["_artist_tok"])
			.with_columns(
				pl.col("_artist_tok").cast(pl.Utf8, strict=False).str.strip_chars().alias("artist")
			)
			.filter(pl.col("artist").is_not_null() & (pl.col("artist") != ""))
			.group_by(["composer_key", "title_key"])
			.agg(pl.col("artist").n_unique().alias("distinct_artists"))
		)

		comp = (
			tracks.join(artists, on=["composer_key", "title_key"], how="left")
			.join(title_mode, on=["composer_key", "title_key"], how="left")
			.join(composer_mode, on=["composer_key", "title_key"], how="left")
			.with_columns(pl.col("distinct_artists").fill_null(0).cast(pl.Int32))
			.with_columns(
				[
					pl.col("title_disp").fill_null(pl.lit("(Unknown title)")),
					pl.col("composer_disp").fill_null(pl.lit("(Unknown composer)")),
				]
			)
			.with_columns(
				(pl.col("title_disp") + pl.lit(" — ") + pl.col("composer_disp")).alias("label")
			)
			.sort(["distinct_artists", "tracks"], descending=[True, True])
		)
		if comp.is_empty():
			return None

		comp_top = comp.head(int(top_n))
		fig = go.Figure(
			data=[
				go.Bar(
					y=comp_top["label"].to_list(),
					x=comp_top["distinct_artists"].to_list(),
					orientation="h",
					marker_color=color,
					customdata=comp_top["tracks"].to_list(),
					hovertemplate="%{y}<br>%{x:,d} distinct artists<br>%{customdata:,d} tracks<extra></extra>",
				)
			]
		)
		fig.update_layout(
			template=template,
			height=max(420, int(len(comp_top) * _HBAR_ROW_PX + _HBAR_EXTRA_PX)),
			margin={"l": 340, "r": 25, "t": 25, "b": 55},
			paper_bgcolor=page_bg,
			plot_bgcolor=page_bg,
			font={"size": 12, "color": page_fg},
			showlegend=False,
		)
		fig.update_xaxes(gridcolor=grid_color, zeroline=False, title_text="Distinct performing artists")
		fig.update_yaxes(autorange="reversed", automargin=True)

		html = _to_html_fig(fig, include_plotlyjs=include_js, div_id=div_id)
		include_js = False
		return (
			f"<div class=\"card\"><div class=\"card-title\">{escape(title)}</div>"
			f"<div class=\"plot-wrap\">{html}</div></div>"
		)

	spotlight_cards: list[str] = []

	# Genre scoping helpers (used for both spotlights + highlights).
	# NOTE: We scope by `genre` tokens because `composer` contains songwriters
	# for non-classical in this library.
	def _genre_tokens_lower() -> pl.Expr:
		if "genre" not in df.columns:
			return pl.lit([], dtype=pl.List(pl.Utf8))
		return _tokens_expr("genre", delimiter=delimiter).list.eval(
			pl.element().str.to_lowercase()
		)

	df_genre_tok = df.with_columns(_genre_tokens_lower().alias("_genre_tok"))

	CLASSICAL_GENRES = {
		"classical",
		"classical crossover",
		"opera",
		"indian classical",
		"symphony",
	}

	def _tok_any_in(allowed: set[str]) -> pl.Expr:
		return (
			pl.col("_genre_tok")
			.list.eval(pl.element().is_in(list(allowed)))
			.list.any()
		)

	def _tok_any_contains(regex: str) -> pl.Expr:
		return (
			pl.col("_genre_tok")
			.list.eval(pl.element().str.contains(regex))
			.list.any()
		)

	# Always show at least one representative spotlight for each major persona.
	# This keeps personas visible even when auto-select picks only one.
	persona_spotlights: list[tuple[str, list[tuple[str, str, str]]]] = [
		(
			"classical",
			[
				("composer", "Composers (albums)", "rgba(31, 119, 180, 0.75)"),
				("conductor", "Conductors (albums)", "rgba(255, 127, 14, 0.75)"),
				("orchestra", "Orchestras (albums)", "rgba(44, 160, 44, 0.75)"),
			],
		),
		(
			"jazz",
			[
				("label", "Labels (albums)", "rgba(31, 119, 180, 0.75)"),
				("engineer", "Engineers (albums)", "rgba(44, 160, 44, 0.75)"),
				("producer", "Producers (albums)", "rgba(255, 127, 14, 0.75)"),
			],
		),
		(
			"electronic",
			[
				("remixer", "Remixers (albums)", "rgba(31, 119, 180, 0.75)"),
				("mixer", "Mixers (albums)", "rgba(255, 127, 14, 0.75)"),
				("label", "Labels (albums)", "rgba(44, 160, 44, 0.75)"),
			],
		),
		(
			"rock_pop",
			[
				("style", "Styles (albums)", "rgba(255, 127, 14, 0.75)"),
				("genre", "Genres (albums)", "rgba(31, 119, 180, 0.75)"),
			],
		),
	]

	for persona_key, options in persona_spotlights:
		selected = " ✓" if persona_key == persona else ""
		persona_title = persona_key.replace("_", " ").title() + selected
		picked = None
		for role_col, short, color in options:
			if persona_key == "classical" and role_col in {"composer", "conductor", "orchestra"}:
				scoped = df_genre_tok.filter(_tok_any_in(CLASSICAL_GENRES))
				pairs = (
					scoped.select(
						[
							pl.col("album_root"),
							_tokens_expr(role_col, delimiter=delimiter).alias("tok"),
						]
					)
					.drop_nulls(["album_root"])
					.explode("tok")
					.drop_nulls(["tok"])
					.unique(subset=["album_root", "tok"])
				)
				card = _top_pairs_card(
					pairs,
					token_col="tok",
					title=f"{persona_title} — {short} (classical-genre tracks)",
					color=color,
					div_id=f"tm-insights-spotlight-{persona_key}-{role_col}",
					height=420,
					top_n=20,
				)
			else:
				card = _top_role(
					role_col,
					title=f"{persona_title} — {short}",
					color=color,
					div_id=f"tm-insights-spotlight-{persona_key}-{role_col}",
				)
			if card:
				picked = card
				break
			
		if picked:
			spotlight_cards.append(picked)

	if spotlight_cards:
		spotlight_html = "<div class=\"grid\">" + "".join(spotlight_cards) + "</div>"

	# Classical / Jazz highlight views (album-scoped)
	# IMPORTANT: These are scoped by *genre* (not by presence of composer), because
	# Tagminder commonly stores songwriters in the composer tag for non-classical.

	album_scope = (
		df_genre_tok.select(
			[
				pl.col("album_root"),
				_tok_any_in(CLASSICAL_GENRES).alias("_row_classical"),
				_tok_any_contains(r"\bjazz\b").alias("_row_jazz"),
				_tok_any_contains(r"(rock|pop|metal|punk|indie|alternative|grunge|folk|singer\/songwriter|singer\s*songwriter)").alias(
					"_row_rock_pop"
				),
				_tok_any_contains(r"(electronic|electronica|ambient|techno|house|trance|idm|dnb|drum\s*&\s*bass|drum\s+and\s+bass)").alias(
					"_row_electronic"
				),
			]
		)
		.drop_nulls(["album_root"])
		.group_by("album_root")
		.agg(
			[
				pl.max("_row_classical").alias("is_classical"),
				pl.max("_row_jazz").alias("is_jazz"),
				pl.max("_row_rock_pop").alias("is_rock_pop"),
				pl.max("_row_electronic").alias("is_electronic"),
			]
		)
	)

	# ReplayGain by genre bucket (album-scoped): median loudness + median dynamics.
	# Uses the same genre-bucketing logic as the drift chart (single bucket per album).
	if df_rg_album is not None and (not df_rg_album.is_empty()):
		rg_bucket_map = (
			album_scope.with_columns(
				pl.when(pl.col("is_classical"))
				.then(pl.lit("Classical"))
				.when(pl.col("is_jazz"))
				.then(pl.lit("Jazz"))
				.when(pl.col("is_rock_pop"))
				.then(pl.lit("Rock/Pop"))
				.when(pl.col("is_electronic"))
				.then(pl.lit("Electronic"))
				.otherwise(pl.lit("Other"))
				.alias("bucket")
			)
			.select(["album_root", "bucket"])
		)

		rg_by_bucket = (
			df_rg_album.join(rg_bucket_map, on="album_root", how="left")
			.with_columns(pl.col("bucket").fill_null("Other"))
			.group_by("bucket")
			.agg(
				[
					pl.len().alias("albums"),
					pl.col("rg_album_gain_db").median().alias("gain_med"),
					(
						pl.col("album_dr_num").median()
						if use_dr_meter and ("album_dr_num" in df_rg_album.columns)
						else pl.col("rg_album_plr").median()
					).alias("dyn_med"),
				]
			)
			.drop_nulls(["gain_med", "dyn_med"])
			.with_columns(
				pl.when(pl.col("bucket") == "Classical")
				.then(pl.lit(0))
				.when(pl.col("bucket") == "Jazz")
				.then(pl.lit(1))
				.when(pl.col("bucket") == "Rock/Pop")
				.then(pl.lit(2))
				.when(pl.col("bucket") == "Electronic")
				.then(pl.lit(3))
				.when(pl.col("bucket") == "Other")
				.then(pl.lit(4))
				.otherwise(pl.lit(99))
				.alias("_ord")
			)
			.sort("_ord")
			.drop(["_ord"])
		)

		if rg_by_bucket.is_empty():
			html_rg_by_genre = "<div class=\"note\">ReplayGain genre-bucket summary unavailable.</div>"
		else:
			buckets = rg_by_bucket["bucket"].to_list()
			gain_med = rg_by_bucket["gain_med"].to_list()
			dyn_med = rg_by_bucket["dyn_med"].to_list()
			albums = rg_by_bucket["albums"].to_list()

			dyn_hover = (
				"%{x}<br>DR%{y:.0f} median<br>%{customdata:,d} albums<extra></extra>"
				if use_dr_meter
				else "%{x}<br>%{y:.2f} LU median PLR proxy<br>%{customdata:,d} albums<extra></extra>"
			)

			fig_rg_bg = make_subplots(
				rows=1,
				cols=2,
				subplot_titles=(
					"Median ReplayGain album gain (more negative = louder)",
					dynamics_bucket_subtitle,
				),
				horizontal_spacing=0.14,
			)
			fig_rg_bg.add_trace(
				go.Bar(
					x=buckets,
					y=gain_med,
					marker_color="rgba(214, 39, 40, 0.80)",
					customdata=albums,
					hovertemplate="%{x}<br>%{y:.2f} dB median gain<br>%{customdata:,d} albums<extra></extra>",
				),
				row=1,
				col=1,
			)
			fig_rg_bg.add_trace(
				go.Bar(
					x=buckets,
					y=dyn_med,
					marker_color="rgba(44, 160, 44, 0.75)",
					customdata=albums,
					hovertemplate=dyn_hover,
				),
				row=1,
				col=2,
			)
			fig_rg_bg.update_layout(
				template=template,
				height=420,
				margin={"l": 45, "r": 25, "t": 55, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				showlegend=False,
			)
			fig_rg_bg.update_xaxes(showline=True, linecolor=axis_line_color)
			fig_rg_bg.update_yaxes(gridcolor=grid_color, zeroline=False)
			html_rg_by_genre = _to_html_fig(
				fig_rg_bg,
				include_plotlyjs=include_js,
				div_id="tm-insights-rg-by-genre",
			)
			include_js = False

	classical_roots = album_scope.filter(pl.col("is_classical")).select(["album_root"])
	jazz_roots = album_scope.filter(pl.col("is_jazz")).select(["album_root"])
	rock_pop_roots = album_scope.filter(pl.col("is_rock_pop")).select(["album_root"])
	electronic_roots = album_scope.filter(pl.col("is_electronic")).select(["album_root"])

	# Genre drift over time (by decade; album-scoped)
	if "album_year" in df_album.columns and not df_album.is_empty():
		drift = (
			df_album.drop_nulls(["album_year"])
			.with_columns(((pl.col("album_year") // 10) * 10).alias("decade"))
			.select(["album_root", "decade"])
			.join(album_scope, on="album_root", how="left")
			.with_columns(
				[
					pl.col("is_classical").fill_null(False),
					pl.col("is_jazz").fill_null(False),
					pl.col("is_rock_pop").fill_null(False),
					pl.col("is_electronic").fill_null(False),
				]
			)
			.with_columns(
				pl.when(pl.col("is_classical"))
				.then(pl.lit("Classical"))
				.when(pl.col("is_jazz"))
				.then(pl.lit("Jazz"))
				.when(pl.col("is_rock_pop"))
				.then(pl.lit("Rock/Pop"))
				.when(pl.col("is_electronic"))
				.then(pl.lit("Electronic"))
				.otherwise(pl.lit("Other"))
				.alias("bucket")
			)
			.group_by(["decade", "bucket"])
			.len()
			.rename({"len": "albums"})
			.sort(["decade", "bucket"])
		)
		if drift.is_empty():
			html_genre_drift = "<div class=\"note\">No genre drift data available.</div>"
		else:
			decades = sorted(drift["decade"].unique().to_list())
			buckets = ["Classical", "Jazz", "Rock/Pop", "Electronic", "Other"]
			palette = {
				"Classical": "rgba(31, 119, 180, 0.75)",
				"Jazz": "rgba(44, 160, 44, 0.75)",
				"Rock/Pop": "rgba(255, 127, 14, 0.75)",
				"Electronic": "rgba(148, 103, 189, 0.75)",
				"Other": "rgba(127, 127, 127, 0.55)",
			}
			fig_drift = go.Figure()
			for b in buckets:
				counts = (
					drift.filter(pl.col("bucket") == b)
					.select(["decade", "albums"])
				)
				m = {int(r[0]): int(r[1]) for r in counts.iter_rows()}
				y = [m.get(int(d), 0) for d in decades]
				fig_drift.add_trace(
					go.Bar(
						name=b,
						x=decades,
						y=y,
						marker_color=palette.get(b),
						hovertemplate="%{x}s<br>" + escape(b) + ": %{y:,d} albums<extra></extra>",
					)
				)
			fig_drift.update_layout(
				template=template,
				barmode="stack",
				height=420,
				margin={"l": 55, "r": 30, "t": 25, "b": 55},
				paper_bgcolor=page_bg,
				plot_bgcolor=page_bg,
				font={"size": 12, "color": page_fg},
				legend={"orientation": "h", "y": -0.18, "x": 0},
			)
			fig_drift.update_xaxes(showline=True, linecolor=axis_line_color, title_text="Decade")
			fig_drift.update_yaxes(gridcolor=grid_color, zeroline=False, title_text="Albums")

			html_genre_drift = _to_html_fig(fig_drift, include_plotlyjs=include_js, div_id="tm-insights-genre-drift")
			include_js = False
	else:
		html_genre_drift = "<div class=\"note\">No album year data available for genre drift.</div>"

	highlight_cards: list[str] = []
	if "composer" in df.columns:
		# Track-level scoping prevents leakage from mixed-genre albums.
		def _composer_pairs_for(mask_expr: pl.Expr) -> pl.DataFrame:
			return (
				df_genre_tok.filter(mask_expr)
				.select(
					[
						pl.col("album_root"),
						_tokens_expr("composer", delimiter=delimiter).alias("composer_tok"),
					]
				)
				.drop_nulls(["album_root"])
				.explode("composer_tok")
				.drop_nulls(["composer_tok"])
				.unique(subset=["album_root", "composer_tok"])
			)

		pairs_classical = _composer_pairs_for(_tok_any_in(CLASSICAL_GENRES))
		card = _top_pairs_card(
			pairs_classical,
			token_col="composer_tok",
			title="Classical — Top composers (classical-genre tracks; albums)",
			color="rgba(31, 119, 180, 0.75)",
			div_id="tm-insights-classical-composers",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

		# Most instances by performer diversity (composer + title), scoped by genre buckets.
		# This surfaces "most covered" / "most performed" compositions in the library.
		for key, mask, color, div_id in [
			(
				"Classical — Most instances (composer+title by distinct artists)",
				_tok_any_in(CLASSICAL_GENRES),
				"rgba(31, 119, 180, 0.75)",
				"tm-insights-classical-most-instances",
			),
			(
				"Jazz — Most instances (composer+title by distinct artists)",
				_tok_any_contains(r"\bjazz\b"),
				"rgba(44, 160, 44, 0.75)",
				"tm-insights-jazz-most-instances",
			),
			(
				"Rock/Pop — Most instances (composer+title by distinct artists)",
				_tok_any_contains(
					r"(rock|pop|metal|punk|indie|alternative|grunge|folk|singer\/songwriter|singer\s*songwriter)"
				),
				"rgba(255, 127, 14, 0.75)",
				"tm-insights-rockpop-most-instances",
			),
		]:
			card = _most_instances_card(
				mask,
				title=key,
				color=color,
				div_id=div_id,
				top_n=max(20, int(args.top)),
			)
			if card:
				highlight_cards.append(card)

		pairs_jazz = _composer_pairs_for(_tok_any_contains(r"\bjazz\b"))
		card = _top_pairs_card(
			pairs_jazz,
			token_col="composer_tok",
			title="Jazz — Top composers (jazz-genre tracks; albums)",
			color="rgba(44, 160, 44, 0.75)",
			div_id="tm-insights-jazz-composers",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

		pairs_rp = _composer_pairs_for(
			_tok_any_contains(
				r"(rock|pop|metal|punk|indie|alternative|grunge|folk|singer\/songwriter|singer\s*songwriter)"
			)
		)
		card = _top_pairs_card(
			pairs_rp,
			token_col="composer_tok",
			title="Rock/Pop — Top composer-tag names (songwriters; rock/pop-genre tracks; albums)",
			color="rgba(255, 127, 14, 0.75)",
			div_id="tm-insights-rockpop-composers",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

	# Conductor (explicitly exposed; classical-genre track-scoped)
	if "conductor" in df.columns:
		conductor_pairs = (
			df_genre_tok.filter(_tok_any_in(CLASSICAL_GENRES))
			.select(
				[
					pl.col("album_root"),
					_tokens_expr("conductor", delimiter=delimiter).alias("conductor_tok"),
				]
			)
			.drop_nulls(["album_root"])
			.explode("conductor_tok")
			.drop_nulls(["conductor_tok"])
			.unique(subset=["album_root", "conductor_tok"])
		)
		card = _top_pairs_card(
			conductor_pairs,
			token_col="conductor_tok",
			title="Classical — Top conductors (classical-genre tracks; albums)",
			color="rgba(148, 103, 189, 0.75)",
			div_id="tm-insights-classical-conductors",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

	if not aa_pairs.is_empty() and classical_roots.height > 0:
		classical_aa = aa_pairs.join(classical_roots, on="album_root", how="inner")
		card = _top_pairs_card(
			classical_aa,
			token_col="albumartist_tok",
			title="Classical — Top album artists (performers; albums)",
			color="rgba(255, 127, 14, 0.75)",
			div_id="tm-insights-classical-albumartists",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

	if not aa_pairs.is_empty() and jazz_roots.height > 0:
		jazz_aa = aa_pairs.join(jazz_roots, on="album_root", how="inner")
		card = _top_pairs_card(
			jazz_aa,
			token_col="albumartist_tok",
			title="Jazz — Top artists (albums)",
			color="rgba(44, 160, 44, 0.75)",
			div_id="tm-insights-jazz-albumartists",
			height=420,
			top_n=25,
		)
		if card:
			highlight_cards.append(card)

	if highlight_cards:
		highlights_html = "<div class=\"grid\">" + "".join(highlight_cards) + "</div>"

	persona_mode = "auto" if persona_is_auto else "requested"
	persona_note = f"Persona: {persona} ({persona_mode})"
	persona_html = (
		"<div class=\"sections\">"
		"<div class=\"card\">"
		f"<div class=\"card-title\">{escape(persona_note)}</div>"
		f"<div class=\"plot-wrap\">{persona_fig_html}</div>"
		"</div>"
	)
	if spotlight_html:
		persona_html += (
			"<div class=\"card\">"
			"<div class=\"card-title\">Persona Spotlights (all)</div>"
			+ spotlight_html
			+ "</div>"
		)
	if highlights_html:
		persona_html += (
			"<div class=\"card\">"
			"<div class=\"card-title\">Genre-Scoped Highlights (Classical / Jazz / Rock-Pop)</div>"
			+ highlights_html
			+ "</div>"
		)
	persona_html += "</div>"

	sections_html = (
		"<div class=\"grid\">"
		"<div class=\"card\"><div class=\"card-title\">Format Mix</div><div class=\"plot-wrap\">"
		+ _safe_div(html_format)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Format Quality (Lossless vs Lossy)</div><div class=\"plot-wrap\">"
		+ _safe_div(html_quality)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">ReplayGain (Loudness / Peaks / Dynamics)</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_trend)
		+ f"<div class=\"tm-subnote\">{escape(dynamics_subnote)}</div>"
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">ReplayGain — Genre buckets</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_by_genre)
		+ "<div class=\"tm-subnote\">Buckets are derived from genre tokens and collapsed album-scoped (one bucket per album; classical → jazz → rock/pop → electronic → other).</div>"
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">ReplayGain — Loudest albums</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_loud)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">ReplayGain — Quietest albums</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_quiet)
		+ "</div></div>"
		+ f"<div class=\"card\"><div class=\"card-title\">{escape(dyn_most_title)}</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_dyn)
		+ "</div></div>"
		+ f"<div class=\"card\"><div class=\"card-title\">{escape(dyn_least_title)}</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rg_brick)
		+ "</div></div>"
		+ missing_dr_cards_html
		+ "<div class=\"card\"><div class=\"card-title\">Collection Over Time</div><div class=\"plot-wrap\">"
		+ _safe_div(html_time)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Album Acquisition Timeline</div><div class=\"plot-wrap\">"
		+ _safe_div(html_acq)
		+ "<div class=\"tm-subnote\">Source: system column "
		+ escape(mod_col)
		+ " (epoch timestamp captured during ingest). Album value aggregates track timestamps (expected identical within an album).</div>"
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Genre Drift (by Decade)</div><div class=\"plot-wrap\">"
		+ _safe_div(html_genre_drift)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Album Depth (Albums by Album Artist)</div><div class=\"plot-wrap\">"
		+ _safe_div(html_aa)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Artist Breadth vs Depth</div><div class=\"plot-wrap\">"
		+ _safe_div(html_artist_depth)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Release Types by Artist</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rt)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Ratings</div><div class=\"plot-wrap\">"
		+ _safe_div(html_rate)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">VA / Compilations</div><div class=\"plot-wrap\">"
		+ _safe_div(html_va)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Labels</div><div class=\"plot-wrap\">"
		+ _safe_div(html_label)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Studios / Locations</div><div class=\"plot-wrap\">"
		+ _safe_div(html_studio)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Engineers</div><div class=\"plot-wrap\">"
		+ _safe_div(html_eng)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Producer ↔ Engineer</div><div class=\"plot-wrap\">"
		+ _safe_div(html_pe)
		+ "</div></div>"
		"<div class=\"card\"><div class=\"card-title\">Producers</div><div class=\"plot-wrap\">"
		+ _safe_div(html_prod)
		+ "</div></div>"
	)
	sections_html += "</div>"

	subtitle = "Insights from system columns + retained tags (keep_columns). Aggregated in Polars."

	try:
		cache_dir = tm_config.get_cache_dir(default="/tmp")
	except Exception:
		cache_dir = "/tmp"
	out_path = Path(cache_dir) / "tagminder-library-insights.html"
	out_path.parent.mkdir(parents=True, exist_ok=True)

	html = _render_html_page(
		title="Tagminder — Library Insights",
		subtitle=subtitle,
		kpis_html=kpis_html,
		persona_html=persona_html,
		sections_html=sections_html,
		theme_bg=page_bg,
		theme_fg=page_fg,
		divider_rgba=divider_rgba,
	)
	out_path.write_text(html, encoding="utf-8")
	logging.info("Wrote: %s", out_path)

	try:
		webbrowser.open(out_path.resolve().as_uri())
	except Exception:
		pass

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
