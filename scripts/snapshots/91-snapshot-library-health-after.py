#!/usr/bin/env python3
"""91-snapshot-library-health-after.py

Purpose:
    Capture an AFTER snapshot of offline metadata quality aggregates and generate
    a before-vs-after dashboard.

Caching policy:
    - The AFTER snapshot is evaluated at runtime.
    - If the changelog fingerprint is unchanged since the latest AFTER snapshot
      and --force is not set, expensive per-column coverage aggregates are not
      recomputed.

Outputs:
    - Writes snapshot tables into the staging DB (_SNAP_*).
    - Writes compare HTML to cache_dir:
        tagminder-library-health-compare.html
    - Auto-opens the compare HTML.

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
import webbrowser
from dataclasses import dataclass
from html import escape
from pathlib import Path
import sqlite3

import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

from tagminder.core import tm_album
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.core import tm_changelog
from tagminder.core import tm_snapshots

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def _configure_logging() -> None:
    logging.basicConfig(level=tm_config.get_log_level(), format=_LOG_FORMAT, force=True)


def _wrap_axis_label(name: str) -> str:
    raw = str(name)
    parts = [p for p in raw.split("_") if p]
    if len(parts) <= 1:
        return raw

    max_lines = 3
    max_line_len = 14
    lines: list[str] = []
    current: list[str] = []

    for part in parts:
        candidate = "_".join([*current, part]) if current else part
        if current and len(candidate) > max_line_len and len(lines) < (max_lines - 1):
            lines.append("_".join(current))
            current = [part]
        else:
            current.append(part)

    if current:
        lines.append("_".join(current))

    if len(lines) > max_lines:
        lines = [*lines[: max_lines - 1], "_".join(lines[max_lines - 1 :])]

    return "<br>".join([l for l in lines if l])


def _format_noop_scripts(noop_by_script: list[tuple[str, int]] | None) -> str:
    items = [(str(s), int(n or 0)) for s, n in (noop_by_script or []) if str(s) and int(n or 0) > 0]
    if not items:
        return "none"

    return ", ".join([f"{s} ({n:,d})" for s, n in items])


def _build_radar_overlay(
    *,
    axis_names: list[str],
    before: list[float],
    after: list[float],
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
    axis_line_color: str,
    title: str,
    color_after: str,
    color_before: str,
) -> go.Figure:
    def _rgba(color: str, alpha: float) -> str:
        """Return an rgba() string with the requested alpha.

        Supports:
            - hex #RRGGBB
            - rgb(r,g,b)
            - rgba(r,g,b,a)
        """

        c = (color or "").strip()
        if c.startswith("rgba("):
            # Replace existing alpha.
            inner = c[len("rgba(") : -1]
            parts = [p.strip() for p in inner.split(",")]
            if len(parts) >= 3:
                r, g, b = parts[0], parts[1], parts[2]
                return f"rgba({r},{g},{b},{alpha})"
            return c
        if c.startswith("rgb("):
            inner = c[len("rgb(") : -1]
            parts = [p.strip() for p in inner.split(",")]
            if len(parts) >= 3:
                r, g, b = parts[0], parts[1], parts[2]
                return f"rgba({r},{g},{b},{alpha})"
            return c
        if c.startswith("#") and len(c) == 7:
            r = int(c[1:3], 16)
            g = int(c[3:5], 16)
            b = int(c[5:7], 16)
            return f"rgba({r},{g},{b},{alpha})"
        # Fallback: let Plotly try to interpret named CSS colors.
        return c
    axis_wrapped = [_wrap_axis_label(a) for a in axis_names]

    def _close(theta: list[str], r: list[float]) -> tuple[list[str], list[float]]:
        if not theta:
            return theta, r
        return [*theta, theta[0]], [*r, r[0]]

    theta_b, r_b = _close(axis_wrapped, before)
    theta_a, r_a = _close(axis_wrapped, after)

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=r_b,
            theta=theta_b,
            name="Before",
            mode="lines+markers",
            marker={"size": 6, "color": color_before},
            line={"color": color_before, "width": 2},
            fill="toself",
            fillcolor=_rgba(color_before, 0.22),
            hovertemplate="%{theta}<br>%{r:.2f}%<extra>Before</extra>",
        )
    )
    fig.add_trace(
        go.Scatterpolar(
            r=r_a,
            theta=theta_a,
            name="After",
            mode="lines+markers",
            marker={"size": 6, "color": color_after},
            line={"color": color_after, "width": 2},
            fill="toself",
            fillcolor=_rgba(color_after, 0.18),
            hovertemplate="%{theta}<br>%{r:.2f}%<extra>After</extra>",
        )
    )

    fig.update_layout(
        template=template,
        title={"text": title, "x": 0.0, "xanchor": "left"},
        showlegend=True,
        legend={"orientation": "h", "y": 1.08, "x": 0},
        font={"size": 13, "color": page_fg},
        height=520,
        margin={"l": 35, "r": 35, "t": 55, "b": 25},
        paper_bgcolor=page_bg,
    )

    fig.update_polars(
        radialaxis={
            "range": [0, 100],
            "dtick": 20,
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
            "tickfont": {"size": 10},
        },
    )

    return fig


def _build_bar_compare(
    *,
    columns: list[str],
    before: list[float],
    after: list[float],
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
    title: str,
    color_before: str,
    color_after: str,
) -> go.Figure:
    n = min(len(columns), len(before), len(after))
    columns = columns[:n]
    before = before[:n]
    after = after[:n]

    height = max(320, int(n * 18 + 140))

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Before",
            y=columns,
            x=before,
            orientation="h",
            marker_color=color_before,
            hovertemplate="%{y}<br>%{x:.2f}%<extra>Before</extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            name="After",
            y=columns,
            x=after,
            orientation="h",
            marker_color=color_after,
            hovertemplate="%{y}<br>%{x:.2f}%<extra>After</extra>",
        )
    )

    fig.update_layout(
        template=template,
        barmode="group",
        height=height,
        margin={"l": 220, "r": 30, "t": 40, "b": 50},
        paper_bgcolor=page_bg,
        plot_bgcolor=page_bg,
        font={"size": 12, "color": page_fg},
        title={"text": title, "x": 0.0, "xanchor": "left"},
        legend={"orientation": "h", "y": 1.08, "x": 0},
    )
    fig.update_xaxes(range=[0, 100], ticksuffix="%", gridcolor=grid_color, zeroline=False)
    fig.update_yaxes(autorange="reversed")
    return fig


def _render_html_page(
    *,
    title: str,
    theme_bg: str,
    theme_fg: str,
    divider_rgba: str,
    left_top_html: str,
    right_top_html: str,
    left_bottom_html: str,
    right_bottom_html: str,
    changelog_note_html: str,
    changelog_left_html: str,
    changelog_right_html: str,
    changelog_bottom_html: str,
    changelog_table_html: str,
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
      .plot-wrap {{ margin: 0 0 4px 0; }}
            .note {{
                margin: 14px 17px 8px 17px;
                padding: 8px 10px;
                border-top: 1px solid {divider_rgba};
                font: 500 12px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
                opacity: 0.92;
            }}
            .note-title {{
                font-weight: 800;
                font-size: 12px;
                margin: 0 0 6px 0;
                opacity: 0.98;
            }}
            .note-list {{
                margin: 0;
                padding-left: 16px;
            }}
            .note-list li {{
                margin: 2px 0;
            }}
            .note-k {{
                font-weight: 700;
            }}
      .scrollbox {{
        margin: 0 17px 0 17px;
        height: 420px;
        overflow-y: auto;
        border-top: 1px solid {divider_rgba};
        padding-top: 10px;
      }}
            .scrollbox-changelog {{
                height: 560px;
            }}
            .scroll-title {{
                margin: 0 0 8px 0;
                font: 800 12px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
                opacity: 0.98;
            }}
            table.tm-table {{
                width: 100%;
                border-collapse: collapse;
                font: 500 12px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif;
                opacity: 0.95;
            }}
            .tm-table th, .tm-table td {{
                padding: 4px 6px;
                border-bottom: 1px solid {divider_rgba};
                vertical-align: top;
            }}
            .tm-table th {{
                font-weight: 800;
                text-align: right;
                opacity: 0.98;
            }}
            .tm-table td {{
                text-align: right;
                opacity: 0.96;
            }}
            .tm-table th:first-child, .tm-table td:first-child {{
                text-align: left;
                width: 42%;
                word-break: break-word;
            }}
      @media (max-height: 800px) {{ .scrollbox {{ height: 340px; }} }}
      @media (max-height: 650px) {{ .scrollbox {{ height: 280px; }} }}
            @media (max-height: 800px) {{ .scrollbox-changelog {{ height: 420px; }} }}
            @media (max-height: 650px) {{ .scrollbox-changelog {{ height: 340px; }} }}
    </style>
  </head>
  <body>
    <div class=\"page\">
      <div class=\"page-title\">{escape(title)}</div>
      <div class=\"grid\">
        <div class=\"col\">
          <div class=\"plot-wrap\">{left_top_html}</div>
          <div class=\"scrollbox\">{left_bottom_html}</div>
        </div>
        <div class=\"col\">
          <div class=\"plot-wrap\">{right_top_html}</div>
          <div class=\"scrollbox\">{right_bottom_html}</div>
        </div>

                <div class=\"note\">{changelog_note_html}</div>

                <div class=\"grid\">
                    <div class=\"col\">
                        <div class=\"plot-wrap\">{changelog_left_html}</div>
                    </div>
                    <div class=\"col\">
                        <div class=\"plot-wrap\">{changelog_right_html}</div>
                    </div>
                </div>

                <div class=\"col\">
                    <div class=\"plot-wrap\">{changelog_bottom_html}</div>
                    <div class=\"scrollbox scrollbox-changelog\">{changelog_table_html}</div>
                </div>
    </div>
  </body>
</html>"""


def _build_changelog_column_table(
        *,
        title: str,
        rows: list[tm_changelog.ColumnChangeBreakdown],
) -> str:
        rows = rows or []
        rows = sorted(rows, key=lambda r: int(r.total or 0), reverse=True)

        parts: list[str] = []
        parts.append(f"<div class=\"scroll-title\">{escape(title)}</div>")
        parts.append('<table class="tm-table">')
        parts.append(
                "<thead><tr>"
                "<th>Field</th>"
                "<th>Total</th>"
                "<th>Add</th>"
                "<th>Change</th>"
                "<th>Delete</th>"
                "<th>Tracks</th>"
                "<th>Albums</th>"
                "</tr></thead>"
        )
        parts.append("<tbody>")
        for r in rows:
                col = escape(str(r.column))
                if r.retained:
                        col = f"<b><i>{col}</i></b>"
                parts.append(
                        "<tr>"
                        f"<td>{col}</td>"
                        f"<td>{int(r.total or 0):,d}</td>"
                        f"<td>{int(r.adds or 0):,d}</td>"
                        f"<td>{int(r.changes or 0):,d}</td>"
                        f"<td>{int(r.deletes or 0):,d}</td>"
                        f"<td>{int(r.tracks_touched or 0):,d}</td>"
                        f"<td>{int(r.albums_touched or 0):,d}</td>"
                        "</tr>"
                )
        parts.append("</tbody></table>")
        return "".join(parts)


def _build_changelog_bar(
    *,
    title: str,
    pairs: list[tuple[str, int]],
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
    color: str,
) -> go.Figure:
    labels = [p[0] for p in (pairs or [])]
    values = [int(p[1] or 0) for p in (pairs or [])]

    height = max(360, int(len(labels) * 18 + 160))
    fig = go.Figure(
        data=[
            go.Bar(
                y=labels,
                x=values,
                orientation="h",
                marker_color=color,
                hovertemplate="%{y}<br>%{x:,d} changes<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        template=template,
        height=height,
        margin={"l": 260, "r": 25, "t": 40, "b": 45},
        paper_bgcolor=page_bg,
        plot_bgcolor=page_bg,
        font={"size": 12, "color": page_fg},
        title={"text": title, "x": 0.0, "xanchor": "left"},
        showlegend=False,
    )
    fig.update_xaxes(gridcolor=grid_color, zeroline=False)
    fig.update_yaxes(autorange="reversed")
    return fig


def _build_changelog_column_breakdown(
    *,
    title: str,
    rows: list[tm_changelog.ColumnChangeBreakdown],
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
) -> go.Figure:
    rows = rows or []
    labels: list[str] = []
    for r in rows:
        col = escape(str(r.column))
        labels.append(f"<b><i>{col}</i></b>" if r.retained else col)
    adds = [int(r.adds or 0) for r in rows]
    deletes = [int(r.deletes or 0) for r in rows]
    changes = [int(r.changes or 0) for r in rows]

    custom = [
        [int(r.tracks_touched or 0), int(r.albums_touched or 0), int(r.total or 0)]
        for r in rows
    ]

    height = max(360, int(len(labels) * 18 + 170))
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Add",
            y=labels,
            x=adds,
            orientation="h",
            marker_color="rgba(44, 160, 44, 0.75)",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>%{x:,d} additions"
                "<br>Tracks: %{customdata[0]:,d}"
                "<br>Albums: %{customdata[1]:,d}"
                "<br>Total: %{customdata[2]:,d}"
                "<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            name="Delete",
            y=labels,
            x=deletes,
            orientation="h",
            marker_color="rgba(214, 39, 40, 0.75)",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>%{x:,d} deletions"
                "<br>Tracks: %{customdata[0]:,d}"
                "<br>Albums: %{customdata[1]:,d}"
                "<br>Total: %{customdata[2]:,d}"
                "<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            name="Change",
            y=labels,
            x=changes,
            orientation="h",
            marker_color="rgba(255, 127, 14, 0.75)",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>%{x:,d} changes"
                "<br>Tracks: %{customdata[0]:,d}"
                "<br>Albums: %{customdata[1]:,d}"
                "<br>Total: %{customdata[2]:,d}"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        template=template,
        barmode="stack",
        height=height,
        margin={"l": 280, "r": 25, "t": 70, "b": 45},
        paper_bgcolor=page_bg,
        plot_bgcolor=page_bg,
        font={"size": 12, "color": page_fg},
        title={"text": title, "x": 0.0, "xanchor": "left"},
        legend={"orientation": "h", "y": 1.06, "x": 0, "xanchor": "left", "yanchor": "bottom"},
    )
    fig.update_xaxes(gridcolor=grid_color, zeroline=False)
    fig.update_yaxes(autorange="reversed")
    return fig


def _build_changelog_column_impact(
    *,
    title: str,
    rows: list[tm_changelog.ColumnChangeBreakdown],
    template: str,
    page_bg: str,
    page_fg: str,
    grid_color: str,
) -> go.Figure:
    rows = rows or []
    rows = sorted(
        rows,
        key=lambda r: (
            int(r.tracks_touched or 0),
            int(r.albums_touched or 0),
            int(r.total or 0),
        ),
        reverse=True,
    )

    labels: list[str] = []
    for r in rows:
        col = escape(str(r.column))
        labels.append(f"<b><i>{col}</i></b>" if r.retained else col)
    tracks = [int(r.tracks_touched or 0) for r in rows]
    albums = [int(r.albums_touched or 0) for r in rows]
    custom = [
        [
            int(r.tracks_touched or 0),
            int(r.albums_touched or 0),
            int(r.total or 0),
            int(r.adds or 0),
            int(r.deletes or 0),
            int(r.changes or 0),
        ]
        for r in rows
    ]

    height = max(360, int(len(labels) * 18 + 170))
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Tracks",
            y=labels,
            x=tracks,
            orientation="h",
            marker_color="rgba(31, 119, 180, 0.75)",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>%{x:,d} tracks touched"
                "<br>Albums: %{customdata[1]:,d}"
                "<br>Total changelog rows: %{customdata[2]:,d}"
                "<br>Add: %{customdata[3]:,d} · Del: %{customdata[4]:,d} · Chg: %{customdata[5]:,d}"
                "<extra></extra>"
            ),
        )
    )
    fig.add_trace(
        go.Bar(
            name="Albums",
            y=labels,
            x=albums,
            orientation="h",
            marker_color="rgba(148, 103, 189, 0.75)",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>%{x:,d} albums touched"
                "<br>Tracks: %{customdata[0]:,d}"
                "<br>Total changelog rows: %{customdata[2]:,d}"
                "<br>Add: %{customdata[3]:,d} · Del: %{customdata[4]:,d} · Chg: %{customdata[5]:,d}"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        template=template,
        barmode="group",
        height=height,
        margin={"l": 280, "r": 25, "t": 40, "b": 55},
        paper_bgcolor=page_bg,
        plot_bgcolor=page_bg,
        font={"size": 12, "color": page_fg},
        title={"text": title, "x": 0.0, "xanchor": "left"},
        legend={"orientation": "h", "y": -0.18, "x": 0},
    )
    fig.update_xaxes(gridcolor=grid_color, zeroline=False)
    fig.update_yaxes(autorange="reversed")
    return fig


def _get_sqlmodded_stats(conn: sqlite3.Connection) -> dict[str, int] | None:
    """Return a few quick impact stats for alib.__sqlmodded.

    __sqlmodded is Tagminder's "dirty counter" since the last export/reset.
    """

    try:
        tm_album.register_sql_functions(conn, func_name="album_root")
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_tracks,
                SUM(COALESCE(__sqlmodded, 0)) AS sqlmodded_sum,
                MAX(COALESCE(__sqlmodded, 0)) AS sqlmodded_max,
                SUM(CASE WHEN COALESCE(__sqlmodded, 0) > 0 THEN 1 ELSE 0 END) AS sqlmodded_tracks,
                COUNT(
                    DISTINCT CASE
                        WHEN COALESCE(__sqlmodded, 0) > 0
                             AND __dirpath IS NOT NULL
                             AND TRIM(CAST(__dirpath AS TEXT)) <> ''
                        THEN album_root(__dirpath)
                    END
                ) AS sqlmodded_albums
            FROM alib
            """
        ).fetchone()
        if not row:
            return None

        keys = [
            "total_tracks",
            "sqlmodded_sum",
            "sqlmodded_max",
            "sqlmodded_tracks",
            "sqlmodded_albums",
        ]
        return {k: int(v or 0) for k, v in zip(keys, row)}
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    _configure_logging()

    p = argparse.ArgumentParser(
        prog=Path(__file__).name,
        description="Capture AFTER snapshot and generate before-vs-after dashboard.",
    )
    p.add_argument("--db", default=None, help="Path to staging SQLite database")
    p.add_argument("--before-run-id", default=None, help="Explicit BEFORE run_id to compare")
    p.add_argument("--notes", default=None, help="Optional note stored with the AFTER snapshot")
    p.add_argument("--theme", choices=("dark", "light"), default="dark")
    p.add_argument("--force", action="store_true", help="Force recompute coverage even if changelog unchanged")
    args = p.parse_args(argv)

    db_path = args.db or tm_config.get_db_path(default=None)
    db_path = str(Path(db_path).resolve())

    keep_cols, critical_cols = tm_snapshots.load_config_columns()

    cfg = tm_config.load_config()
    cols_cfg = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
    system_prefix = "__"
    if isinstance(cols_cfg, dict):
        sp = cols_cfg.get("system_prefix")
        if isinstance(sp, str) and sp:
            system_prefix = sp

    sqlmodded_stats: dict[str, int] | None = None

    conn = tm_db.connect(db_path, wal=False)
    try:
        tm_snapshots.ensure_snapshot_tables(conn)

        # Require a BEFORE snapshot before proceeding. (This script is primarily
        # a compare-dashboard generator; capturing AFTER without BEFORE is not
        # very useful and can be surprising.)
        before_any = args.before_run_id or tm_snapshots.get_latest_run_id(conn, label="before")
        if not before_any:
            raise SystemExit("No BEFORE snapshot found. Run 90-snapshot-library-health-before.py first.")

        # Determine AFTER run (may be reused), but only if it is not older than
        # the selected BEFORE snapshot.
        before_created_utc = tm_snapshots.get_run_created_utc(conn, run_id=str(before_any)) or str(before_any)

        fp_now = tm_snapshots.get_changelog_fingerprint(conn)
        fp_last_after = tm_snapshots.get_latest_run_fingerprint(conn, label="after")
        unchanged = (
            fp_last_after is not None
            and fp_last_after.max_timestamp == fp_now.max_timestamp
            and fp_last_after.row_count == fp_now.row_count
        )

        after_run = None
        if unchanged and not args.force:
            candidate_after = tm_snapshots.get_latest_run_id(conn, label="after")
            if candidate_after:
                candidate_after_created = (
                    tm_snapshots.get_run_created_utc(conn, run_id=candidate_after) or str(candidate_after)
                )
                if candidate_after_created >= before_created_utc:
                    after_run = candidate_after
                    logging.info(
                        "Changelog unchanged; reusing latest AFTER snapshot: %s", after_run
                    )
                else:
                    logging.info(
                        "Latest AFTER snapshot predates BEFORE; capturing fresh AFTER (before=%s after=%s)",
                        before_created_utc,
                        candidate_after_created,
                    )

        if not after_run:
            after_run = tm_snapshots.create_run(conn, label="after", notes=args.notes, fingerprint=fp_now)
            core_cov = tm_snapshots.compute_coverage(conn, columns=keep_cols)
            crit_cov = tm_snapshots.compute_coverage(conn, columns=critical_cols)
            tm_snapshots.write_coverage_rows(
                conn,
                table="_SNAP_core_tags",
                run_id=after_run,
                columns_in_order=keep_cols,
                coverage=core_cov,
            )
            tm_snapshots.write_coverage_rows(
                conn,
                table="_SNAP_critical_tags",
                run_id=after_run,
                columns_in_order=critical_cols,
                coverage=crit_cov,
            )
            conn.commit()
            logging.info("Captured AFTER snapshot: %s", after_run)

        if not after_run:
            raise SystemExit("No AFTER snapshot found and failed to create one.")

        after_created_utc = tm_snapshots.get_run_created_utc(conn, run_id=after_run) or str(after_run)

        if args.before_run_id:
            before_run = str(args.before_run_id)
        else:
            before_run = (
                tm_snapshots.get_latest_run_id_at_or_before(
                    conn, label="before", created_utc=after_created_utc
                )
                or str(before_any)
            )

        before_created_utc = tm_snapshots.get_run_created_utc(conn, run_id=before_run) or str(before_run)

        # Changelog summary window: BEFORE created_utc .. AFTER created_utc
        # (Defensive swap in case of clock skew or reversed selection.)
        if before_created_utc > after_created_utc:
            before_created_utc, after_created_utc = after_created_utc, before_created_utc

        changelog_summary = tm_changelog.summarize(
            conn,
            start_ts=before_created_utc,
            end_ts=after_created_utc,
            top_n=5000,
            retained_columns=set(keep_cols),
            system_prefix=system_prefix,
        )

        sqlmodded_stats = _get_sqlmodded_stats(conn)

        theme = str(args.theme).lower().strip()
        is_dark = theme == "dark"
        template = "plotly_dark" if is_dark else "plotly_white"
        page_bg = "#0b0e14" if is_dark else "#ffffff"
        page_fg = "#e6e6e6" if is_dark else "#111111"
        grid_color = "#2f3742" if is_dark else "#ddd"
        axis_line_color = "#6c7785" if is_dark else "#888"
        divider_rgba = "rgba(255,255,255,0.08)" if is_dark else "rgba(0,0,0,0.10)"

        # Fetch snapshot data.
        crit_cols, crit_lib_before, crit_alb_before = tm_snapshots.fetch_tag_rows(
            conn,
            table="_SNAP_critical_tags",
            run_id=before_run,
            columns_in_order=critical_cols,
        )
        _, crit_lib_after, crit_alb_after = tm_snapshots.fetch_tag_rows(
            conn,
            table="_SNAP_critical_tags",
            run_id=after_run,
            columns_in_order=critical_cols,
        )

        core_cols, core_lib_before, core_alb_before = tm_snapshots.fetch_tag_rows(
            conn,
            table="_SNAP_core_tags",
            run_id=before_run,
            columns_in_order=keep_cols,
        )
        _, core_lib_after, core_alb_after = tm_snapshots.fetch_tag_rows(
            conn,
            table="_SNAP_core_tags",
            run_id=after_run,
            columns_in_order=keep_cols,
        )

    finally:
        conn.close()

    # Build changelog figures and note.
    ch = changelog_summary
    if isinstance(sqlmodded_stats, dict):
        sqlmodded_tracks = int(sqlmodded_stats.get("sqlmodded_tracks", 0) or 0)
        sqlmodded_albums = int(sqlmodded_stats.get("sqlmodded_albums", 0) or 0)
        sqlmodded_sum = int(sqlmodded_stats.get("sqlmodded_sum", 0) or 0)
        sqlmodded_max = int(sqlmodded_stats.get("sqlmodded_max", 0) or 0)
        sql_note = (
            f"{sqlmodded_tracks:,d} track(s) · {sqlmodded_albums:,d} album(s) "
            f"(sum {sqlmodded_sum:,d}; max {sqlmodded_max:,d})"
        )
    else:
        sql_note = "n/a"

    changelog_note_html = (
        f"<div class=\"note-title\">What Changed — Summary [{escape(ch.start_ts)} → {escape(ch.end_ts)}]</div>"
        "<ul class=\"note-list\">"
        f"<li><span class=\"note-k\">Effective changes</span>: {ch.total_entries:,d} "
        f"(Add {ch.enrich_entries:,d} · Change {ch.modify_entries:,d} · Delete {ch.clear_entries:,d})</li>"
        f"<li><span class=\"note-k\">Impact</span>: {ch.tracks_touched:,d} track(s) · {ch.albums_touched:,d} album(s)</li>"
        f"<li><span class=\"note-k\">__sqlmodded</span>: {escape(sql_note)}</li>"
        f"<li><span class=\"note-k\">Changelog noise</span>: raw {ch.raw_rows:,d} · no-op old==new {ch.noop_entries:,d}</li>"
        f"<li><span class=\"note-k\">Generated by</span>: {escape(_format_noop_scripts(ch.noop_by_script))}</li>"
        "</ul>"
    )

    script_fig = _build_changelog_bar(
        title="What Changed — by Script (top)",
        pairs=(ch.by_script or [])[:20],
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
        color="rgba(31, 119, 180, 0.75)",
    )

    # Keep the impact chart readable: show the most impactful fields by reach.
    impact_rows = sorted(
        (ch.by_column_breakdown or []),
        key=lambda r: (
            int(r.tracks_touched or 0),
            int(r.albums_touched or 0),
            int(r.total or 0),
        ),
        reverse=True,
    )[:30]
    column_impact_fig = _build_changelog_column_impact(
        title="What Changed — by Field (impact: tracks / albums)",
        rows=impact_rows,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
    )

    breakdown_rows = sorted(
        (ch.by_column_breakdown or []),
        key=lambda r: int(r.total or 0),
        reverse=True,
    )
    breakdown_rows_top = breakdown_rows[:60]
    column_breakdown_fig = _build_changelog_column_breakdown(
        title="What Changed — by Field (add / delete / change) (top)",
        rows=breakdown_rows_top,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
    )

    changelog_table_html = _build_changelog_column_table(
        title=f"What Changed — All Fields ({len(breakdown_rows):,d})",
        rows=breakdown_rows,
    )

    # Build figures.
    track_radar = _build_radar_overlay(
        axis_names=crit_cols,
        before=crit_lib_before,
        after=crit_lib_after,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
        axis_line_color=axis_line_color,
        title="Track Level Tag Completeness (Critical Tags)",
        color_after="#1f77b4",
        color_before="rgba(148, 103, 189, 0.85)",
    )
    album_radar = _build_radar_overlay(
        axis_names=crit_cols,
        before=crit_alb_before,
        after=crit_alb_after,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
        axis_line_color=axis_line_color,
        title="Album Level Tag Completeness (Critical Tags)",
        color_after="#ff7f0e",
        color_before="rgba(148, 103, 189, 0.85)",
    )

    track_bar = _build_bar_compare(
        columns=core_cols,
        before=core_lib_before,
        after=core_lib_after,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
        title="Core Tags Coverage — Library (% populated)",
        color_before="rgba(160,160,160,0.75)",
        color_after="rgba(31, 119, 180, 0.75)",
    )
    album_bar = _build_bar_compare(
        columns=core_cols,
        before=core_alb_before,
        after=core_alb_after,
        template=template,
        page_bg=page_bg,
        page_fg=page_fg,
        grid_color=grid_color,
        title="Core Tags Coverage — Albums (avg % of tracks populated)",
        color_before="rgba(160,160,160,0.75)",
        color_after="rgba(255, 127, 14, 0.75)",
    )

    # Render HTML.
    try:
        cache_dir = tm_config.get_cache_dir(default="/tmp")
    except Exception:
        cache_dir = "/tmp"

    out_path = Path(cache_dir) / "tagminder-library-health-compare.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    left_top_html = pio.to_html(
        track_radar,
        include_plotlyjs="cdn",  # type: ignore[arg-type]
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-radar-tracks",
    )
    right_top_html = pio.to_html(
        album_radar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-radar-albums",
    )
    left_bottom_html = pio.to_html(
        track_bar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-bars-library",
    )
    right_bottom_html = pio.to_html(
        album_bar,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-bars-albums",
    )

    changelog_left_html = pio.to_html(
        script_fig,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-changelog-scripts",
    )
    changelog_right_html = pio.to_html(
        column_impact_fig,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-changelog-columns",
    )
    changelog_bottom_html = pio.to_html(
        column_breakdown_fig,
        include_plotlyjs=False,
        full_html=False,
        config={"responsive": True, "displaylogo": False},
        div_id="tm-compare-changelog-columns-breakdown",
    )

    title = f"Tagminder — Before vs After ({before_run} → {after_run})"
    html = _render_html_page(
        title=title,
        theme_bg=page_bg,
        theme_fg=page_fg,
        divider_rgba=divider_rgba,
        left_top_html=left_top_html,
        right_top_html=right_top_html,
        left_bottom_html=left_bottom_html,
        right_bottom_html=right_bottom_html,
        changelog_note_html=changelog_note_html,
        changelog_left_html=changelog_left_html,
        changelog_right_html=changelog_right_html,
        changelog_bottom_html=changelog_bottom_html,
        changelog_table_html=changelog_table_html,
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
