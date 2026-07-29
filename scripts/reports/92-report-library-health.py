#!/usr/bin/env python3
"""
Purpose:
    Generate a visually appealing, on-demand "Library Health" HTML report with two
    radar (spider) diagrams:

    1) Track-level non-compliance: % of tracks missing each critical field.
    2) Album-level non-compliance: % of albums impacted by missingness per field.

    The axes (fields) are read from tagminder.toml:
      [reports.missing_critical_tags_by_album].critical_columns

    Numerators are sourced from step 94's report table (exception-only wide table).

Policy / Definitions:
    - Missingness is whatever step 94 computed for each field.
    - Track-level denominator: COUNT(*) over alib.
      - Special case: albumartist denominator excludes compilation rows.
    - Album-level denominator: count of distinct album-root folders.
      - Album-root rolls up disc subfolders named like CD1 / Disc 02 / disc003
        (case-insensitive; optional space; 1-3 digits) to the parent folder.

Behavior:
    - If the step 94 report table is missing or doesn't match configured columns,
      this script runs 94 automatically first.
    - Writes an HTML file to cache_dir (tagminder.toml [paths].cache_dir).
    - Auto-opens the HTML in the default browser.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - _INF_missing_critical_tags_by_album (configurable; produced by step 94)

Author: audiomuze
Last updated: 2026-04-18
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import webbrowser
from pathlib import Path
import sqlite3
from html import escape

import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _get_94_config() -> tuple[list[str], str]:
    cfg = tm_config.load_config()
    reports = cfg.get("reports", {}) if isinstance(cfg, dict) else {}
    report_cfg = reports.get("missing_critical_tags_by_album", {}) if isinstance(reports, dict) else {}

    if not isinstance(report_cfg, dict):
        raise RuntimeError("Missing config table [reports.missing_critical_tags_by_album] in tagminder.toml")

    cols = report_cfg.get("critical_columns")
    if not isinstance(cols, list) or not cols or not all(isinstance(x, str) and x for x in cols):
        raise RuntimeError("Invalid or missing [reports.missing_critical_tags_by_album].critical_columns")

    table = report_cfg.get("table")
    if not isinstance(table, str) or not table:
        raise RuntimeError("Invalid or missing [reports.missing_critical_tags_by_album].table")

    # De-dupe preserving order.
    seen: set[str] = set()
    out_cols: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out_cols.append(c)

    return out_cols, table


def _get_keep_columns() -> list[str]:
    cfg = tm_config.load_config()
    cleanup = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
    if not isinstance(cleanup, dict):
        raise RuntimeError("Missing config table [cleanup] in tagminder.toml")

    cols = cleanup.get("keep_columns")
    if not isinstance(cols, list) or not cols or not all(isinstance(x, str) and x for x in cols):
        raise RuntimeError("Invalid or missing [cleanup].keep_columns")

    # De-dupe preserving order.
    seen: set[str] = set()
    out_cols: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out_cols.append(c)

    return out_cols


def _alib_columns(conn: sqlite3.Connection) -> set[str]:
    return {str(r[1]) for r in conn.execute("PRAGMA table_info(alib)").fetchall()}


def _non_null_expr(col: str) -> str:
    q = tm_db.quote_ident(col)
    # Treat empty/whitespace strings as missing; keep 0/false values.
    return f"({q} IS NOT NULL AND TRIM(CAST({q} AS TEXT)) != '')"


def _get_keep_columns_coverage(
    conn: sqlite3.Connection,
    *,
    keep_columns: list[str],
) -> tuple[list[str], list[float], list[float]]:
    """Return (columns, track_pct, album_pct) for keep_columns coverage.

        Coverage definition: value is present if non-NULL and non-empty after TRIM.
        Album coverage uses album-root grouping (disc subfolders rolled up) and is
        computed as:
            - per album-root: (#tracks with value) / (#tracks)
            - then averaged across album-roots (equal weight per album).
    """

    alib_cols = _alib_columns(conn)

    cols_all = [c for c in keep_columns if not c.startswith("__")]
    if not cols_all:
        return [], [], []

    cols_present = [c for c in cols_all if c in alib_cols]
    cols_missing = [c for c in cols_all if c not in alib_cols]

    track_pct_by_col: dict[str, float] = {c: 0.0 for c in cols_all}

    track_count = int(conn.execute("SELECT COUNT(*) FROM alib").fetchone()[0] or 0)
    if cols_present:
        sum_exprs = ", ".join(
            f"SUM(CASE WHEN {_non_null_expr(c)} THEN 1 ELSE 0 END) AS {tm_db.quote_ident(c)}" for c in cols_present
        )
        sums = conn.execute(f"SELECT {sum_exprs} FROM alib").fetchone()
        for i, c in enumerate(cols_present):
            n_present = int(((sums[i] if sums is not None else 0) or 0))
            track_pct_by_col[c] = (n_present / track_count * 100.0) if track_count else 0.0

    # Album-root coverage.
    tm_album.register_sql_functions(conn, func_name="album_root")

    # Compute SUM(present) and COUNT(*) per album root, then average ratios.
    album_pct_by_col: dict[str, float] = {c: 0.0 for c in cols_all}
    if cols_present:
        sum_exprs_album = ", ".join(
            f"SUM(CASE WHEN {_non_null_expr(c)} THEN 1 ELSE 0 END) AS {tm_db.quote_ident(c)}" for c in cols_present
        )
        inner = (
            "SELECT album_root(__dirpath) AS root, "
            "COUNT(*) AS n_tracks, "
            f"{sum_exprs_album} "
            "FROM alib WHERE __dirpath IS NOT NULL GROUP BY root"
        )

        # AVG over album roots (equal-weight per album).
        outer_avg_exprs = ", ".join(
            f"AVG(COALESCE({tm_db.quote_ident(c)}, 0) * 1.0 / NULLIF(n_tracks, 0)) AS {tm_db.quote_ident(c)}"
            for c in cols_present
        )
        avgs = conn.execute(f"SELECT {outer_avg_exprs} FROM ({inner})").fetchone()
        for i, c in enumerate(cols_present):
            album_pct_by_col[c] = float(((avgs[i] if avgs is not None else 0.0) or 0.0)) * 100.0

    # Preserve original keep_columns ordering, including missing columns as 0%.
    cols_out = cols_all
    track_pct = [track_pct_by_col.get(c, 0.0) for c in cols_out]
    album_pct = [album_pct_by_col.get(c, 0.0) for c in cols_out]
    return cols_out, track_pct, album_pct


def _build_keep_columns_bar_single(
    *,
    columns: list[str],
    values_pct: list[float],
    series_name: str,
    series_color: str,
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
) -> go.Figure:
    # Make sure sizes align.
    n = min(len(columns), len(values_pct))
    columns = columns[:n]
    values_pct = values_pct[:n]

    # Height scales with number of bars; container will provide scrolling.
    height = max(320, int(n * 18 + 140))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name=series_name,
            y=columns,
            x=values_pct,
            orientation="h",
            marker_color=series_color,
            hovertemplate="%{y}<br>%{x:.2f}%<extra></extra>",
        )
    )

    fig.update_layout(
        template=template,
        barmode="overlay",
        height=height,
        margin={"l": 220, "r": 30, "t": 20, "b": 50},
        paper_bgcolor=page_bg,
        plot_bgcolor=page_bg,
        font={"size": 12, "color": page_fg},
        showlegend=False,
    )
    fig.update_xaxes(
        range=[0, 100],
        ticksuffix="%",
        gridcolor=grid_color,
        zeroline=False,
        title_text="Coverage",
    )
    fig.update_yaxes(autorange="reversed")
    return fig


def _render_html_page(
    *,
    title: str,
    track_radar_html: str,
    album_radar_html: str,
    track_bar_html: str,
    album_bar_html: str,
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
                margin: 0 17px 10px 17px;
                font: 700 18px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
            }}
            .grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 18px;
                align-items: start;
            }}
            @media (max-width: 1100px) {{
                .grid {{ grid-template-columns: 1fr; }}
            }}
            .col {{
                border-top: 1px solid {divider_rgba};
                padding-top: 10px;
            }}
      .section-title {{
                margin: 8px 17px 8px 17px;
        font: 600 14px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
        opacity: 0.95;
      }}
            .plot-wrap {{ margin: 0 0 4px 0; }}
      .scrollbox {{
                margin: 0 17px 0 17px;
                height: 420px;
        overflow-y: auto;
                border-top: 1px solid {divider_rgba};
        padding-top: 10px;
      }}
      @media (max-height: 800px) {{
                .scrollbox {{ height: 340px; }}
      }}
      @media (max-height: 650px) {{
                .scrollbox {{ height: 280px; }}
      }}
    </style>
  </head>
  <body>
        <div class=\"page\">
            <div class=\"page-title\">{escape(title)}</div>
            <div class=\"grid\">
                <div class=\"col\">
                    <div class=\"section-title\">Track Level Tag Completeness</div>
                    <div class=\"plot-wrap\">{track_radar_html}</div>
                    <div class=\"section-title\">Core Tags Coverage — Library (% populated)</div>
                    <div class=\"scrollbox\">{track_bar_html}</div>
                </div>
                <div class=\"col\">
                    <div class=\"section-title\">Album Level Tag Completeness</div>
                    <div class=\"plot-wrap\">{album_radar_html}</div>
                    <div class=\"section-title\">Core Tags Coverage — Albums (avg % of tracks populated)</div>
                    <div class=\"scrollbox\">{album_bar_html}</div>
                </div>
            </div>
    </div>
  </body>
</html>"""


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({tm_db.quote_ident(table)})")
    return {str(row[1]) for row in cur.fetchall()}


def _ensure_94_report(db_path: str, *, critical_cols: list[str], report_table: str) -> None:
    """Run step 94 if its report table is missing or out-of-date."""

    # Open a read-only connection for checks.
    conn = tm_db.connect(db_path, read_only=True, wal=False)
    try:
        if not _table_exists(conn, report_table):
            needs = True
        else:
            have = _table_columns(conn, report_table)
            # report_table also has album_dirpath/album_dirname/total_tracks/timestamp/script
            needs = any(c not in have for c in critical_cols)
    finally:
        conn.close()

    if not needs:
        return

    logging.info("Refreshing step 94 report table (%s)…", report_table)

    cmd = [sys.executable, str(Path(__file__).resolve().with_name("94-report-missing-critical-tags-by-album.py")), "--db", db_path]
    completed = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parent))
    if completed.returncode != 0:
        raise RuntimeError(f"Step 94 failed (exit code {completed.returncode})")


def _get_denominators(conn: sqlite3.Connection) -> tuple[int, int, int, int]:
    """Return (track_count, eligible_tracks_albumartist, album_roots, eligible_albums_albumartist)."""

    track_count = int(conn.execute("SELECT COUNT(*) FROM alib").fetchone()[0] or 0)

    alib_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(alib)").fetchall()}
    has_compilation = "compilation" in alib_cols

    if has_compilation:
        # albumartist eligible tracks: exclude compilation==1.
        # Treat NULL/invalid as 0.
        eligible_albumartist_tracks = int(
            conn.execute(
                "SELECT COUNT(*) FROM alib WHERE COALESCE(CAST(compilation AS INTEGER), 0) != 1"
            ).fetchone()[0]
            or 0
        )
    else:
        eligible_albumartist_tracks = track_count

    # Album-root distinct count: compute from distinct __dirpath values.
    roots: set[str] = set()
    for (d,) in conn.execute("SELECT DISTINCT __dirpath FROM alib WHERE __dirpath IS NOT NULL").fetchall():
        roots.add(tm_album.album_root(str(d)))

    album_root_count = len(roots)

    # Albumartist eligible albums: exclude compilation albums (max compilation=1).
    if not has_compilation:
        eligible_albumartist_albums = album_root_count
    else:
        root_is_comp: dict[str, int] = {}
        for d, is_comp in conn.execute(
            "SELECT __dirpath, MAX(COALESCE(CAST(compilation AS INTEGER), 0)) "
            "FROM alib WHERE __dirpath IS NOT NULL GROUP BY __dirpath"
        ).fetchall():
            root = tm_album.album_root(str(d))
            v = int(is_comp or 0)
            prev = root_is_comp.get(root, 0)
            root_is_comp[root] = 1 if (prev == 1 or v == 1) else 0

        compilation_album_roots = sum(1 for v in root_is_comp.values() if v == 1)
        eligible_albumartist_albums = max(0, album_root_count - compilation_album_roots)

    return track_count, eligible_albumartist_tracks, album_root_count, eligible_albumartist_albums


def _get_metrics(
    conn: sqlite3.Connection,
    *,
    report_table: str,
    critical_cols: list[str],
    track_count: int,
    eligible_track_count_albumartist: int,
    album_root_count: int,
    eligible_album_count_albumartist: int,
) -> tuple[list[float], list[float]]:
    """Return (track_rates_pct, album_rates_pct) aligned to critical_cols."""

    # Track numerators: SUM(COALESCE(col,0)) for each critical col.
    sum_exprs = ", ".join(
        f"SUM(COALESCE({tm_db.quote_ident(c)}, 0)) AS {tm_db.quote_ident(c)}" for c in critical_cols
    )
    sums = conn.execute(
        f"SELECT {sum_exprs} FROM {tm_db.quote_ident(report_table)}"
    ).fetchone()

    sum_by_col: dict[str, int] = {}
    for i, c in enumerate(critical_cols):
        v = sums[i] if sums is not None else 0
        sum_by_col[c] = int(v or 0)

    # Album numerators: COUNT(*) of report rows where col IS NOT NULL.
    album_counts: dict[str, int] = {}
    for c in critical_cols:
        (n,) = conn.execute(
            f"SELECT COUNT(*) FROM {tm_db.quote_ident(report_table)} WHERE {tm_db.quote_ident(c)} IS NOT NULL"
        ).fetchone()
        album_counts[c] = int(n or 0)

    track_rates: list[float] = []
    album_rates: list[float] = []

    for c in critical_cols:
        denom_tracks = eligible_track_count_albumartist if c == "albumartist" else track_count
        denom_albums = eligible_album_count_albumartist if c == "albumartist" else album_root_count

        track_rates.append((sum_by_col[c] / denom_tracks * 100.0) if denom_tracks else 0.0)
        album_rates.append((album_counts[c] / denom_albums * 100.0) if denom_albums else 0.0)

    return track_rates, album_rates


def _wrap_axis_label(name: str) -> str:
    """Wrap long snake_case names so angular axis labels remain legible."""

    raw = str(name)
    parts = [p for p in raw.split("_") if p]
    if len(parts) <= 1:
        return raw

    # Build up to 3 short lines to keep the angular labels readable.
    # Prefer wrapping at underscores, but also respect a rough character budget.
    max_lines = 3
    max_line_len = 14

    lines: list[str] = []
    current: list[str] = []

    for part in parts:
        candidate = "_".join([*current, part]) if current else part

        # If adding this token would exceed budget and we still have room for
        # more lines, start a new line.
        if current and len(candidate) > max_line_len and len(lines) < (max_lines - 1):
            lines.append("_".join(current))
            current = [part]
        else:
            current.append(part)

    if current:
        lines.append("_".join(current))

    # If we exceeded max_lines (rare), merge overflow into the last line.
    if len(lines) > max_lines:
        lines = [*lines[: max_lines - 1], "_".join(lines[max_lines - 1 :])]

    lines = [l for l in lines if l]
    return "<br>".join(lines)


def _pick_rmax(max_val_pct: float) -> float:
    """Choose a radial max that stays readable for dynamic data.

    Goals:
      - Avoid misleading 'everything looks huge' when max_val is tiny.
      - Avoid an empty-looking chart when values are low.
      - Keep comparability reasonable when values are high.
    """

    max_val_pct = float(max_val_pct or 0.0)

    # If missingness gets meaningfully high, keep an absolute 0–100 scale.
    if max_val_pct >= 60.0:
        return 100.0

    # Otherwise, zoom a bit but never zoom so far that 1–3% looks like 80%.
    # (With point labels always shown, we can prioritize honest visuals.)
    return min(100.0, max(20.0, max_val_pct * 1.4))


def _pick_dtick_for_span(span: float) -> float:
    span = float(span)
    if span <= 25:
        return 5.0
    if span <= 60:
        return 10.0
    return 20.0


def _pick_radial_range(values_pct: list[float], *, metric: str) -> tuple[float, float]:
    """Return (rmin, rmax) for the polar radial axis.

    - For "incomplete" we show 0..rmax (hybrid scaled) for problem visibility.
    - For "complete" we always cap at 100, and may zoom the lower bound upward
      so dents/"holes" remain visually obvious when completeness is high.
    """

    metric = (metric or "").strip().lower()
    vals = [float(v) for v in (values_pct or [])]

    if metric == "complete":
        rmax = 100.0
        if not vals:
            return 0.0, rmax

        min_val = min(vals)

        # Zoom only when the library is mostly complete; otherwise keep 0..100.
        if min_val >= 90.0:
            rmin = max(0.0, min_val - 15.0)
        elif min_val >= 80.0:
            rmin = max(0.0, min_val - 20.0)
        elif min_val >= 70.0:
            rmin = max(0.0, min_val - 25.0)
        else:
            rmin = 0.0

        # Avoid an ultra-tight range.
        if (rmax - rmin) < 15.0:
            rmin = max(0.0, rmax - 15.0)

        return rmin, rmax

    # Default: incomplete
    max_val = max([*vals, 0.0])
    rmax = _pick_rmax(max_val)
    return 0.0, rmax


def _pick_angular_tickfont_size(n_axes: int) -> int:
    if n_axes <= 10:
        return 12
    if n_axes <= 14:
        return 11
    if n_axes <= 20:
        return 10
    return 9


def _build_radar(
    axis_names: list[str],
    r: list[float],
    *,
    name: str,
    line_color: str,
    fill_color: str,
) -> go.Scatterpolar:
    # Close the loop for a filled polygon.
    axis_wrapped = [_wrap_axis_label(a) for a in axis_names]
    theta2 = [*axis_wrapped, axis_wrapped[0]]
    axis_original2 = [*axis_names, axis_names[0]]
    r2 = [*r, r[0]]

    # With many axes, labels get dense; shrink markers slightly.
    n_axes = max(1, len(axis_names))
    marker_size = 7 if n_axes <= 14 else (6 if n_axes <= 20 else 5)

    return go.Scatterpolar(
        r=r2,
        theta=theta2,
        customdata=axis_original2,
        name=name,
        mode="lines+markers",
        marker={"size": marker_size, "color": line_color},
        line={"color": line_color, "width": 2},
        fill="toself",
        fillcolor=fill_color,
        cliponaxis=False,
        hovertemplate="%{customdata}<br>%{r:.2f}%<extra></extra>",
    )


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    p = argparse.ArgumentParser(
        prog=Path(sys.argv[0]).name,
        description="Generate Library Health spider diagrams (HTML) and open in browser.",
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
        "--metric",
        choices=("incomplete", "complete"),
        default="complete",
        help="Plot missingness (incomplete) or completeness (default: complete)",
    )
    args = p.parse_args(argv)

    db_path = args.db or tm_config.get_db_path(default=None)
    db_path = str(Path(db_path).resolve())

    critical_cols, report_table = _get_94_config()
    keep_columns = _get_keep_columns()

    _ensure_94_report(db_path, critical_cols=critical_cols, report_table=report_table)

    conn = tm_db.connect(db_path, read_only=True, wal=False)
    try:
        track_count, eligible_albumartist_tracks, album_root_count, eligible_albumartist_albums = _get_denominators(conn)

        logging.info("Tracks: %d", track_count)
        logging.info("Tracks (eligible albumartist): %d", eligible_albumartist_tracks)
        logging.info("Albums (root folders): %d", album_root_count)
        logging.info("Albums (eligible albumartist): %d", eligible_albumartist_albums)

        track_rates, album_rates = _get_metrics(
            conn,
            report_table=report_table,
            critical_cols=critical_cols,
            track_count=track_count,
            eligible_track_count_albumartist=eligible_albumartist_tracks,
            album_root_count=album_root_count,
            eligible_album_count_albumartist=eligible_albumartist_albums,
        )

        keep_cols_eff, keep_track_pct, keep_album_pct = _get_keep_columns_coverage(
            conn,
            keep_columns=keep_columns,
        )
    finally:
        conn.close()

    metric = str(args.metric).lower().strip()
    if metric == "complete":
        track_values = [max(0.0, min(100.0, 100.0 - v)) for v in track_rates]
        album_values = [max(0.0, min(100.0, 100.0 - v)) for v in album_rates]
    else:
        metric = "incomplete"
        track_values = track_rates
        album_values = album_rates

    # Shared scale for readability.
    rmin, rmax = _pick_radial_range([*track_values, *album_values], metric=metric)
    dtick = _pick_dtick_for_span(rmax - rmin)
    angular_tickfont_size = _pick_angular_tickfont_size(len(critical_cols))

    theme = str(args.theme).lower().strip()
    is_dark = theme == "dark"

    template = "plotly_dark" if is_dark else "plotly_white"
    page_bg = "#0b0e14" if is_dark else "#ffffff"
    page_fg = "#e6e6e6" if is_dark else "#111111"
    grid_color = "#2f3742" if is_dark else "#ddd"
    axis_line_color = "#6c7785" if is_dark else "#888"
    divider_rgba = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.10)"

    # Build two independent radar figures so we can place independent bar charts
    # under each without overlap.
    track_radar = go.Figure(
        data=[
            _build_radar(
                critical_cols,
                track_values,
                name="Tracks",
                line_color="#1f77b4",
                fill_color="rgba(31, 119, 180, 0.18)",
            )
        ]
    )
    album_radar = go.Figure(
        data=[
            _build_radar(
                critical_cols,
                album_values,
                name="Albums",
                line_color="#ff7f0e",
                fill_color="rgba(255, 127, 14, 0.18)",
            )
        ]
    )

    for radar in (track_radar, album_radar):
        radar.update_layout(
            template=template,
            showlegend=False,
            font={"size": 13, "color": page_fg},
            height=520,
            margin={"l": 35, "r": 35, "t": 25, "b": 25},
            paper_bgcolor=page_bg,
        )
        radar.update_polars(
            radialaxis={
                "range": [rmin, rmax],
                "dtick": dtick,
                "ticksuffix": "%",
                "showline": True,
                "showticklabels": True,
                "gridcolor": grid_color,
                "linecolor": axis_line_color,
            },
            angularaxis={
                "direction": "clockwise",
                "showline": True,
                "linecolor": axis_line_color,
                "tickfont": {"size": angular_tickfont_size},
            },
        )

    track_bar = _build_keep_columns_bar_single(
        columns=keep_cols_eff,
        values_pct=keep_track_pct,
        series_name="Library (% populated)",
        series_color="rgba(31, 119, 180, 0.75)",
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
    )
    album_bar = _build_keep_columns_bar_single(
        columns=keep_cols_eff,
        values_pct=keep_album_pct,
        series_name="Albums (avg % of tracks populated)",
        series_color="rgba(255, 127, 14, 0.75)",
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
    )

    try:
        cache_dir = tm_config.get_cache_dir(default="/tmp")
    except Exception:
        cache_dir = "/tmp"

    out_path = Path(cache_dir) / "tagminder-library-health.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    track_radar_html = pio.to_html(
        track_radar,
        include_plotlyjs="cdn",
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-library-health-radar-tracks",
    )
    album_radar_html = pio.to_html(
        album_radar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-library-health-radar-albums",
    )
    track_bar_html = pio.to_html(
        track_bar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-library-health-keepcols-tracks",
    )
    album_bar_html = pio.to_html(
        album_bar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-library-health-keepcols-albums",
    )

    html = _render_html_page(
        title="Tagminder — Library Health",
        track_radar_html=track_radar_html,
        album_radar_html=album_radar_html,
        track_bar_html=track_bar_html,
        album_bar_html=album_bar_html,
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
