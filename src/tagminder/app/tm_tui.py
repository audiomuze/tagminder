"""

Purpose:
    Minimal Textual TUI front-end for Tagminder scripts.

MVP features:
- Script list
- Show selected script's docstring (description) before running
- Run selected script as a subprocess and show live output

This TUI does NOT import or execute any ETL script in-process.
It only spawns scripts as subprocesses using the current interpreter.

Exception:
    - Some small, read-only exploration tools may run in-process (e.g. graph
        exploration) to provide interactive browsing that is not practical via
        a subprocess-only runner.

Keys:
  Up/Down       - navigate scripts
  Enter         - run highlighted or double-clicked script
  Double-Click  - run script
  r             - run selected script
  n             - run selected, then select next numbered step
  y             - copy output to clipboard
  c             - clear output
    i             - focus script input box
    Esc           - return focus to script list
  q             - quit

Notes:
  - Use the filter box to quickly narrow the script list by step number, filename, or keywords.
  - When a script is running and awaits input, type your response in the "Script Input" field and press Enter.

This module is part of Tagminder.

SQLite tables referenced:
        - None

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

import asyncio
import re
import shlex
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import tomllib
from typing import Any

import sqlite3

from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Input, Label, ListItem, RichLog, Static, Tree

from rich.text import Text

from tagminder.app import tm_cli
from tagminder.core import tm_config
from tagminder.core import tm_db
from tagminder.ui import tm_artist_map
from tagminder.core import tm_graph

_MBID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_MUSICBRAINZ_ARTIST_URL_RE = re.compile(
    r"https://musicbrainz\.org/artist/(?P<mbid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)
_NAMESAKE_CANDIDATE_LINE_RE = re.compile(r"^\s*\[\d+\]\s")


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "tagminder.toml").exists() and (parent / "scripts").exists():
            return parent
    return here.parents[3]


@dataclass(frozen=True)
class ScriptItem:
    info: tm_cli.ScriptInfo
    docstring: str | None


class ScriptSelected(Message):
    def __init__(self, script: ScriptItem) -> None:
        super().__init__()
        self.script = script


class ArtistGraphExplorerScreen(Screen):
    """Browsable artist graph explorer (neverending navigation).

    MVP:
        - Search for an artist/person by substring
        - Select to focus the node
        - Browse neighbors endlessly by selecting neighbors
        - Back navigation
    """

    CSS = """
    Screen {
        layout: vertical;
    }

    #ag-main {
        height: 1fr;
    }

    #ag-search {
        height: 3;
        border: solid $secondary;
        padding: 0 1;
    }

    #ag-status {
        height: auto;
        padding: 0 1;
        opacity: 0.85;
    }

    #ag-cols {
        height: 1fr;
    }

    #ag-results {
        width: 45%;
        min-width: 28;
        border: solid $primary;
    }

    #ag-neighbors {
        width: 55%;
        min-width: 28;
        border: solid $primary;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Back"),
        Binding("b", "back", "Back"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._graph: tm_graph.WeightedGraph | None = None
        self._current: str | None = None
        self._history: list[str] = []
        self._loaded: bool = False

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="ag-main"):
            yield Static("Artist Graph Explorer", classes="titlebar")
            yield Input(placeholder="Search artist/person (substring)", id="ag-search")
            yield Static("Loading…", id="ag-status")
            with Horizontal(id="ag-cols"):
                yield Tree("Matches", id="ag-results")
                yield Tree("Neighbors", id="ag-neighbors")
        yield Footer()

    def action_close(self) -> None:
        self.app.pop_screen()

    def action_back(self) -> None:
        if not self._history:
            self.bell()
            return
        prev = self._history.pop()
        self._set_current(prev, push_history=False)

    async def on_mount(self) -> None:
        # Hide roots; these act like list views.
        for tree_id in ("#ag-results", "#ag-neighbors"):
            try:
                t = self.query_one(tree_id, Tree)
                t.show_root = False
            except Exception:
                pass

        self.query_one("#ag-search", Input).focus()
        await self._ensure_loaded()

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        status = self.query_one("#ag-status", Static)
        status.update("Building graph from alib…")

        # Cache on the App so reopening the screen is instant.
        cached = getattr(self.app, "_artist_graph_cache", None)
        if isinstance(cached, tm_graph.WeightedGraph):
            self._graph = cached
        else:

            def build() -> tm_graph.WeightedGraph:
                db_path = tm_config.get_db_path(default=None)
                delimiter = tm_config.get_multivalue_delimiter()
                system_prefix = tm_config.get_system_prefix()
                return tm_graph.build_artist_similarity_graph(
                    db_path=db_path,
                    system_prefix=system_prefix,
                    delimiter=delimiter,
                )

            try:
                self._graph = await asyncio.to_thread(build)
                setattr(self.app, "_artist_graph_cache", self._graph)
            except Exception as e:
                status.update(f"Failed to build graph: {e}")
                self._loaded = True
                return

        self._loaded = True

        n_nodes = len(self._graph.nodes) if self._graph is not None else 0
        n_edges = sum(len(v) for v in (self._graph.adjacency.values() if self._graph else []))
        status.update(
            f"Ready. Nodes: {n_nodes:,d} | Adjacency edges: {n_edges:,d}. Type to search, then Enter."
        )

        self._refresh_results()

    def on_input_changed(self, event: Input.Changed) -> None:
        if getattr(event.input, "id", None) != "ag-search":
            return
        self._refresh_results()

    def on_key(self, event: events.Key) -> None:
        # Enter on either tree should navigate.
        if event.key != "enter":
            return

        focused = self.focused
        fid = getattr(focused, "id", None)
        if fid not in {"ag-results", "ag-neighbors"}:
            return

        try:
            node = focused.cursor_node  # type: ignore[attr-defined]
        except Exception:
            node = None
        data = getattr(node, "data", None)
        if isinstance(data, str) and data:
            self._set_current(data, push_history=True)
            event.stop()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        data = getattr(event.node, "data", None)
        if isinstance(data, str) and data:
            self._set_current(data, push_history=True)
            try:
                event.tree.focus()
            except Exception:
                pass

    def _refresh_results(self) -> None:
        tree = self.query_one("#ag-results", Tree)
        try:
            tree.clear()
        except Exception:
            for child in list(tree.root.children):
                try:
                    child.remove()
                except Exception:
                    pass

        if not self._graph or not self._graph.nodes:
            tree.root.add("(No artist graph data)", data=None)
            return

        q = (self.query_one("#ag-search", Input).value or "").strip().lower()

        # Show either search matches, or a degree-sorted starter list.
        if not q:
            degree = [(n, len(self._graph.adjacency.get(n, []))) for n in self._graph.nodes]
            degree.sort(key=lambda t: (-t[1], t[0].lower()))
            items = [n for n, _ in degree[:200]]
        else:
            items: list[str] = []
            for n in self._graph.nodes:
                if q in n.lower():
                    items.append(n)
                    if len(items) >= 200:
                        break

        for n in items:
            d = len(self._graph.adjacency.get(n, []))
            label = f"{n}  ({d} links)"
            tree.root.add(label, data=n)

        try:
            tree.root.expand()
        except Exception:
            pass

    def _set_current(self, name: str, *, push_history: bool) -> None:
        if not self._graph:
            return
        name = (name or "").strip()
        if not name:
            return

        if push_history and self._current and self._current != name:
            self._history.append(self._current)

        self._current = name
        self._refresh_neighbors()

    def _refresh_neighbors(self) -> None:
        tree = self.query_one("#ag-neighbors", Tree)
        status = self.query_one("#ag-status", Static)

        try:
            tree.clear()
        except Exception:
            for child in list(tree.root.children):
                try:
                    child.remove()
                except Exception:
                    pass

        if not self._graph or not self._current:
            tree.root.add("(Select an artist from Matches)", data=None)
            status.update("Ready. Type to search, then Enter.")
            return

        neigh = self._graph.adjacency.get(self._current, [])
        status.update(
            f"Focus: {self._current} | neighbors: {len(neigh):,d} | history: {len(self._history):,d}"
        )

        if not neigh:
            tree.root.add("(No neighbors)", data=None)
            return

        for other, w in neigh[:300]:
            tree.root.add(f"{w} · {other}", data=other)

        try:
            tree.root.expand()
        except Exception:
            pass


class ArtistSimilarityMapLauncherScreen(Screen):
    """Generate + open the browser-based similarity map."""

    CSS = """
    Screen { layout: vertical; }
    #map-status { padding: 1 2; }
    """

    BINDINGS = [
        Binding("escape", "close", "Back"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Artist Similarity Map", classes="titlebar")
        yield Static("Preparing…", id="map-status")
        yield Footer()

    def action_close(self) -> None:
        self.app.pop_screen()

    async def on_mount(self) -> None:
        status = self.query_one("#map-status", Static)
        status.update("Building map…")

        try:
            cache_dir = tm_config.get_cache_dir(default="/tmp")
        except Exception:
            cache_dir = "/tmp"

        out_dir = Path(cache_dir)
        out_latest = out_dir / "tagminder-artist-similarity-map.html"

        import time

        run_id = int(time.time() * 1000)
        out_path = out_dir / f"tagminder-artist-similarity-map.{run_id}.html"

        cached = getattr(self.app, "_artist_graph_cache", None)
        graph: tm_graph.WeightedGraph
        if isinstance(cached, tm_graph.WeightedGraph):
            graph = cached
        else:
            def build() -> tm_graph.WeightedGraph:
                db_path = tm_config.get_db_path(default=None)
                delimiter = tm_config.get_multivalue_delimiter()
                system_prefix = tm_config.get_system_prefix()
                return tm_graph.build_artist_similarity_graph(
                    db_path=db_path,
                    system_prefix=system_prefix,
                    delimiter=delimiter,
                )

            graph = await asyncio.to_thread(build)
            setattr(self.app, "_artist_graph_cache", graph)

        def write() -> Path:
            return tm_artist_map.write_artist_similarity_map_html(
                out_path=out_path,
                graph=graph,
                top_k_per_node=30,
                min_weight=1,
            )

        try:
            path = await asyncio.to_thread(write)
        except Exception as e:
            status.update(f"Failed to write map: {e}")
            return

        # Also update the stable path for convenience.
        try:
            import shutil

            shutil.copy2(path, out_latest)
        except Exception:
            pass

        url = path.resolve().as_uri()

        # Open browser.
        opened = False
        try:
            import webbrowser

            opened = bool(webbrowser.open(url))
        except Exception:
            opened = False

        if not opened:
            try:
                subprocess.Popen(["xdg-open", url])
                opened = True
            except Exception:
                opened = False

        if opened:
            status.update(f"Wrote: {path}\nOpened in your browser.")
        else:
            status.update(f"Wrote: {path}\nCould not auto-open browser; open this file manually.")


def _colsel_default_config_path() -> Path:
    return _repo_root() / "tagminder.toml"


def _colsel_backup_name(original: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return original.with_name(original.name + f".bak-{ts}")


def _colsel_is_section_header(line: str) -> bool:
    s = line.strip()
    return s.startswith("[") and s.endswith("]") and not s.startswith("[[")


def _colsel_find_table_range(lines: list[str], table: str) -> tuple[int, int] | None:
    header = f"[{table}]"
    start = None
    for i, ln in enumerate(lines):
        if ln.strip() == header:
            start = i
            break
    if start is None:
        return None

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if _colsel_is_section_header(lines[j]):
            end = j
            break
    return start, end


def _colsel_format_list_block(key: str, items: list[str], *, indent: str, item_indent: str) -> list[str]:
    lines: list[str] = []
    lines.append(f"{indent}{key} = [")
    for it in items:
        lines.append(f"{item_indent}\"{it}\",")
    lines.append(f"{indent}]")
    return lines


def _colsel_replace_or_insert_list(*, text: str, table: str, key: str, items: list[str]) -> str:
    """Replace an existing key list within a TOML table, preserving formatting when possible."""

    lines = text.splitlines()
    rng = _colsel_find_table_range(lines, table)
    if rng is None:
        # Append new table at EOF.
        lines.append("")
        lines.append(f"[{table}]")
        indent = ""
        item_indent = "\t"
        lines.extend(_colsel_format_list_block(key, items, indent=indent, item_indent=item_indent))
        return "\n".join(lines) + ("\n" if text.endswith("\n") else "")

    start, end = rng

    block_start = None
    block_end = None
    indent = ""
    item_indent = "\t"

    for i in range(start + 1, end):
        ln = lines[i]
        stripped = ln.lstrip(" \t")
        if not stripped.startswith(key):
            continue

        left = stripped.split("=", 1)
        if len(left) != 2:
            continue
        k = left[0].strip()
        if k != key:
            continue

        after = left[1].strip()
        if after.startswith("["):
            block_start = i
            indent = ln[: len(ln) - len(stripped)]

            # Try to infer item indent from the first item line.
            for j in range(i + 1, end):
                if "]" in lines[j]:
                    break
                s2 = lines[j].strip()
                if not s2:
                    continue
                item_indent = lines[j][: len(lines[j]) - len(lines[j].lstrip(" \t"))] or (indent + "\t")
                break

            for j in range(i + 1, end):
                if lines[j].strip().startswith("]"):
                    block_end = j
                    break
            break

    new_block = _colsel_format_list_block(key, items, indent=indent, item_indent=item_indent)

    if block_start is not None and block_end is not None:
        lines = [*lines[:block_start], *new_block, *lines[block_end + 1 :]]
        return "\n".join(lines) + ("\n" if text.endswith("\n") else "")

    # Insert at end of table (before next section header).
    insert_at = end
    prefix: list[str] = []
    if insert_at > 0 and lines[insert_at - 1].strip():
        prefix = [""]
    lines = [*lines[:insert_at], *prefix, *new_block, *lines[insert_at:]]
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _colsel_get_list(cfg: dict[str, Any], table: str, key: str) -> list[str]:
    cur: Any = cfg
    for part in table.split("."):
        if not isinstance(cur, dict):
            return []
        cur = cur.get(part)

    if not isinstance(cur, dict):
        return []

    val = cur.get(key)
    if not isinstance(val, list):
        return []

    out: list[str] = []
    seen: set[str] = set()
    for x in val:
        if not isinstance(x, str):
            continue
        name = x.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


class ColumnSelectionMatrixScreen(Screen):
    CSS = """
    Screen {
        layout: vertical;
    }

    #topbar {
        height: auto;
        padding: 0 1;
    }

    #colsel-filter {
        height: 3;
        border: solid $secondary;
        padding: 0 1;
    }

    #colsel-status {
        height: auto;
        padding: 0 1;
        opacity: 0.85;
    }

    #colsel-table {
        height: 1fr;
        border: solid $primary;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Back"),
        Binding("q", "close", "Back"),
        Binding("s", "save", "Save"),
        Binding("enter", "toggle", "Toggle"),
        Binding("space", "toggle", "Toggle"),
    ]

    _LIST_SPECS: list[tuple[str, str, str]] = [
        ("cleanup", "keep_columns", "keep"),
        ("cleanup", "dedupe_columns", "dedupe"),
        ("reports.missing_critical_tags_by_album", "critical_columns", "critical"),
        ("reports.multi_value_tags_by_album", "tags", "multi"),
    ]

    def __init__(self, *, config_path: Path | None = None) -> None:
        super().__init__()
        self._config_path = config_path or _colsel_default_config_path()
        self._filter_text: str = ""
        self._system_prefix: str = "__"
        self._schema_columns: list[str] = []
        self._all_columns: list[str] = []

        # key: "table.key" -> set(columns)
        self._members: dict[str, set[str]] = {}
        self._order: dict[str, list[str]] = {}
        self._backup_path: Path | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="topbar"):
            yield Static("Configure column selections (tagminder.toml)")
            yield Input(placeholder="Filter columns…", id="colsel-filter")
            yield Static("Loading…", id="colsel-status")
        yield DataTable(id="colsel-table")
        yield Footer()

    def action_close(self) -> None:
        self.app.pop_screen()

    def on_mount(self) -> None:
        self._load_from_toml()
        self._build_table()
        self.query_one("#colsel-filter", Input).focus()

    def _load_from_toml(self) -> None:
        if not self._config_path.exists():
            self.query_one("#colsel-status", Static).update(f"Config not found: {self._config_path}")
            return

        with self._config_path.open("rb") as f:
            cfg = tomllib.load(f)

        cols_cfg = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
        if isinstance(cols_cfg, dict):
            sp = cols_cfg.get("system_prefix")
            if isinstance(sp, str) and sp:
                self._system_prefix = sp
            schema = cols_cfg.get("schema_columns")
            if isinstance(schema, list):
                self._schema_columns = [
                    s.strip() for s in schema if isinstance(s, str) and s.strip() and not s.strip().startswith(self._system_prefix)
                ]

        schema_cols = list(self._schema_columns)
        all_set = set(schema_cols)

        self._members.clear()
        self._order.clear()

        for table, key, label in self._LIST_SPECS:
            path = f"{table}.{key}"
            items = [
                c
                for c in _colsel_get_list(cfg, table, key)
                if c and not c.startswith(self._system_prefix)
            ]
            self._members[path] = set(items)
            self._order[path] = items
            all_set.update(items)

        extras = sorted([c for c in all_set if c not in schema_cols])
        self._all_columns = [*schema_cols, *extras]

        status = self.query_one("#colsel-status", Static)
        status.update(
            f"Config: {self._config_path} | Columns: {len(self._all_columns):,d} | "
            + " | ".join(
                f"{label}={len(self._members.get(f'{table}.{key}', set())):,d}"
                for table, key, label in self._LIST_SPECS
            )
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        if getattr(event.input, "id", None) != "colsel-filter":
            return
        self._filter_text = (event.value or "").strip().lower()
        self._build_table()

    def _filtered_columns(self) -> list[str]:
        if not self._filter_text:
            return self._all_columns
        needle = self._filter_text
        return [c for c in self._all_columns if needle in c.lower()]

    def _build_table(self) -> None:
        table = self.query_one("#colsel-table", DataTable)
        table.clear(columns=True)

        table.add_column("column", key="col")
        for table_name, key, label in self._LIST_SPECS:
            table.add_column(label, key=f"{table_name}.{key}")

        for col in self._filtered_columns():
            row = [col]
            for table_name, key, _label in self._LIST_SPECS:
                path = f"{table_name}.{key}"
                row.append("✓" if col in self._members.get(path, set()) else "")
            table.add_row(*row, key=col)

        try:
            table.cursor_type = "cell"
            table.focus()
        except Exception:
            pass

    def action_toggle(self) -> None:
        table = self.query_one("#colsel-table", DataTable)
        try:
            cell = table.coordinate_to_cell_key(table.cursor_coordinate)
            row_key = cell.row_key
            col_key = cell.column_key
        except Exception:
            return

        if not isinstance(row_key, str) or not row_key:
            return
        if not isinstance(col_key, str) or not col_key or col_key == "col":
            return

        members = self._members.setdefault(col_key, set())
        if row_key in members:
            members.remove(row_key)
        else:
            members.add(row_key)
            order = self._order.setdefault(col_key, [])
            if row_key not in order:
                order.append(row_key)

        table.update_cell(row_key, col_key, "✓" if row_key in members else "")

    def action_save(self) -> None:
        if not self._config_path.exists():
            self.bell()
            return

        # Preserve original ordering; append newly-added items in schema order.
        list_values: dict[str, list[str]] = {}
        for table_name, key, _label in self._LIST_SPECS:
            path = f"{table_name}.{key}"
            members = self._members.get(path, set())
            order = self._order.get(path, [])
            out = [c.strip() for c in order if c in members and c.strip()]

            missing = [c for c in self._all_columns if (c in members and c not in set(order))]
            out.extend([c.strip() for c in missing if c.strip()])

            seen: set[str] = set()
            out2: list[str] = []
            for c in out:
                if c in seen:
                    continue
                seen.add(c)
                out2.append(c)
            list_values[path] = out2

        orig = self._config_path.read_text(encoding="utf-8")

        if self._backup_path is None:
            self._backup_path = _colsel_backup_name(self._config_path)
            self._backup_path.write_text(orig, encoding="utf-8")

        new_text = orig
        for table_name, key, _label in self._LIST_SPECS:
            path = f"{table_name}.{key}"
            new_text = _colsel_replace_or_insert_list(
                text=new_text,
                table=table_name,
                key=key,
                items=list_values.get(path, []),
            )

        self._config_path.write_text(new_text, encoding="utf-8")

        self.query_one("#colsel-status", Static).update(
            f"Saved. Backup: {self._backup_path} | Columns: {len(self._all_columns):,d}"
        )


class ScriptRunnerApp(App[None]):
    CSS = """
    Screen {
        layout: vertical;
    }

    #main {
        height: 1fr;
    }

    #left {
        width: 42%;
        min-width: 32;
        border: solid $primary;
    }

    #right {
        width: 58%;
    }

    #filter {
        height: 3;
        border: solid $secondary;
        padding: 0 1;
    }

    #doc {
        height: 10;
        border: solid $secondary;
        padding: 0 1;
    }

    #args {
        height: 3;
        border: solid $secondary;
        padding: 0 1;
    }

    #output {
        height: 1fr;
        border: solid $secondary;
        padding: 0 1;
    }

    #script_input {
        height: 3;
        border: solid $secondary;
        padding: 0 1;
    }

    .titlebar {
        height: auto;
        padding: 0 1;
    }

    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "run_selected", "Run"),
        Binding("n", "run_next", "Run + Next"),
        Binding("y", "copy_output", "Copy output"),
        Binding("c", "clear_output", "Clear output"),
        Binding("i", "focus_script_input", "Input"),
        Binding("escape", "focus_script_tree", "Back to scripts"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._scripts: list[ScriptItem] = []
        self._visible: list[int] = []  # visible-index -> index into _scripts (leaf order)
        self._node_by_script_index: dict[int, object] = {}
        self._current_script_index: int | None = None
        self._current: ScriptItem | None = None
        self._current_action_id: str | None = None
        self._running_task: asyncio.Task[None] | None = None

        self._filter_text: str = ""

        # filename -> (exit_code, timestamp_str)
        self._last_run: dict[str, tuple[int, str]] = {}

        # When True, advance selection to the next numbered script after run completes.
        self._advance_after_run: bool = False

        # Keep a plain-text buffer of output lines so users can copy it.
        # (Mouse selection in terminal UIs is often awkward/unavailable.)
        self._output_lines: list[str] = []
        self._output_line_cap: int = 20000

        self._wal_cleanup_attempted: bool = False

        # Cached graph for in-app exploration tools.
        self._artist_graph_cache: tm_graph.WeightedGraph | None = None

        # Track for double-click detection on tree nodes (300ms window).
        self._last_selected_node: object | None = None
        self._last_selected_time: float = 0

        # Store stdin for running subprocess so we can send interactive input.
        self._proc_stdin: asyncio.StreamWriter | None = None

    def _cleanup_staging_db_wal_sidecars(self) -> None:
        """Best-effort cleanup so *.db-wal/*.db-shm don't persist after TUI exit.

        This does NOT aim to be perfect under contention (other processes holding
        connections). It's a user-facing hygiene step for the common case where
        the TUI is the only thing touching the DB.
        """

        if self._wal_cleanup_attempted:
            return
        self._wal_cleanup_attempted = True

        try:
            db_path = tm_config.db_path_from_toml(default=None)
        except Exception:
            db_path = None

        if not db_path:
            return

        try:
            conn = sqlite3.connect(str(db_path))
        except Exception:
            return

        try:
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass

            # Switching away from WAL is the most reliable way to ensure the
            # sidecar files disappear when the last connection closes.
            try:
                conn.execute("PRAGMA journal_mode = DELETE")
            except Exception:
                pass

            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def action_quit(self) -> None:
        # Run best-effort WAL cleanup before exiting.
        try:
            self._cleanup_staging_db_wal_sidecars()
        finally:
            self.exit()

    @staticmethod
    def _category_overview(category: str) -> str:
        cat = (category or "").strip()
        if cat == "Data Quality":
            return (
                "Data Quality\n"
                "\n"
                "Scripts that clean, normalize, infer, and enrich metadata in the staging DB.\n"
                "Goal: improve consistency and correctness before export/reporting."
            )
        if cat == "Reporting":
            return (
                "Reporting\n"
                "\n"
                "Exception-only reports written into the staging DB for review.\n"
                "Goal: surface albums/tracks that need attention."
            )
        if cat == "Tag Import/Export":
            return (
                "Tag Import/Export\n"
                "\n"
                "Operational scripts for importing tags into the staging DB and exporting them back to files.\n"
                "Includes export DB creation, rename operations, and housekeeping utilities."
            )
        if cat == "Master Data Management":
            return (
                "Master Data Management\n"
                "\n"
                "Scripts that ingest/harvest, validate, and maintain reference/master data tables used by transformations.\n"
                "Goal: keep vetted mapping sources trustworthy, auditable, and ready for downstream normalization/enrichment."
            )
        if cat == "Library Health":
            return (
                "Library Health\n"
                "\n"
                "Regeneratable, visually-oriented health summaries of the library.\n"
                "Typically produces HTML visualizations (e.g., radar/spider diagrams)."
            )
        return cat or "(No description.)"

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="main"):
            with Container(id="left"):
                yield Static("Scripts", classes="titlebar")
                yield Input(
                    placeholder="Filter scripts (e.g. 97 dupes, mbid, export)",
                    id="filter",
                )
                yield Tree("Scripts", id="script_tree")
            with Vertical(id="right"):
                yield Static("Description (docstring)", classes="titlebar")
                yield Static("Select a script on the left.", id="doc")
                yield Static("Args (optional, shell-style)", classes="titlebar")
                yield Input(
                    placeholder="e.g. import /tmp/db.sqlite /music --log DEBUG",
                    id="args",
                )
                yield Static("Output", classes="titlebar")
                yield RichLog(id="output", highlight=True, markup=False, wrap=True)
                yield Static("Script Input", classes="titlebar")
                yield Input(
                    placeholder="Enter response for script (if needed)",
                    id="script_input",
                )
        yield Footer()

    def on_mount(self) -> None:
        self.title = "tm — script runner"
        self.sub_title = "Select a script; press Enter or double-click to run"

        self._bootstrap_reference_tables_in_workspace()

        # Hide the root label; we provide our own "Scripts" titlebar.
        try:
            tree = self.query_one("#script_tree", Tree)
            tree.show_root = False
        except Exception:
            pass

        self._scripts = []
        for s in tm_cli.discover_scripts():
            doc = tm_cli._extract_docstring(s.path)
            self._scripts.append(ScriptItem(info=s, docstring=doc))

        self._refresh_script_tree(select_first_if_needed=True)

        # Default focus to the script tree (not the filter box), so Up/Down works immediately.
        self.query_one("#script_tree", Tree).focus()

    def _bootstrap_reference_tables_in_workspace(self) -> None:
        """Ensure reference lookup tables exist and report empty-table implications in the output pane."""

        out = self.query_one("#output", RichLog)
        try:
            db_path = tm_config.get_master_data_db_path()
        except Exception as e:
            out.write("\n=== Startup Check: Reference Tables ===")
            out.write(f"[ERROR] Could not resolve master_data DB path from config: {e}")
            return

        conn: sqlite3.Connection | None = None
        try:
            conn = tm_db.connect(db_path, read_only=True)

            out.write("\n=== Startup Check: Reference Tables ===")
            out.write(f"DB: {db_path}")

            implication_lines = {
                "_REF_vetted_contributors": (
                    "Vetted contributor mappings cannot be applied/validated "
                    "(07-apply-vetted-contributor-mappings.py, 89-validate-vetted-contributor-multi-values.py)."
                ),
                "contributors_unified_disambiguated": (
                    "Canonical contributor->MBID resolution is unavailable "
                    "for MBID enrichment/normalization workflows."
                ),
                "contributors_unified_namesakes": (
                    "Namesake candidate-assisted disambiguation is unavailable "
                    "(18-populate-musicbrainz-ids.py, 06-normalize-contributors.py)."
                ),
            }

            empty_tables = 0

            for table, implication in implication_lines.items():
                exists = tm_db.table_exists(conn, table)
                count = (
                    int(conn.execute(f"SELECT COUNT(*) FROM {tm_db.quote_ident(table)}").fetchone()[0] or 0)
                    if exists
                    else None
                )

                if not exists:
                    out.write(f"[MISSING] {table}")
                    out.write("  Impact: This reference table is unavailable until created/populated.")
                    continue

                if count == 0:
                    empty_tables += 1
                    out.write(f"[EMPTY]   {table} (rows=0)")
                    out.write(f"  Impact: {implication}")
                else:
                    out.write(f"[OK]      {table} (rows={count})")

            if empty_tables == 0:
                out.write("Summary: All required reference tables are populated.")
            else:
                out.write(
                    f"Summary: {empty_tables} required reference table(s) are empty. "
                    "Some enrichment/normalization features will be limited until populated."
                )
        except Exception as e:
            out.write(f"[ERROR] Reference-table startup check failed: {e}")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    @staticmethod
    def _category_for_filename(filename: str, path: str | None = None) -> str:
        fn = (filename or "").lower()
        p = (path or "").lower().replace("\\", "/")

        if "/scripts/mdm/" in p:
            return "Master Data Management"

        if "library-health" in fn:
            return "Library Health"

        if "library-insights" in fn:
            return "Library Health"

        # Reporting scripts are consistently named.
        if "report" in fn:
            return "Reporting"

        # Import/export and related operational utilities.
        if fn == "tags2db.py" or fn.startswith("98-") or fn.startswith("99-"):
            return "Tag Import/Export"

        # Master data maintenance scripts.
        if fn.startswith("89-"):
            return "Master Data Management"

        return "Data Quality"

    @staticmethod
    def _parse_step_number(filename: str) -> int | None:
        # Expect common pipeline naming like: 01-foo.py, 97-bar.py
        head = filename.split("-", 1)[0]
        if head.isdigit():
            try:
                return int(head)
            except ValueError:
                return None
        return None

    def _format_script_label(self, item: ScriptItem) -> str:
        status = self._last_run.get(item.info.filename)
        if not status:
            return item.info.filename

        rc, ts = status
        if rc == 0:
            prefix = f"[OK {ts}]"
        else:
            prefix = f"[ERR {rc} {ts}]"
        label = f"{prefix} {item.info.filename}"
        # Colorize anything that has been run.
        # - Green: last run succeeded
        # - Red: last run failed
        return Text(label, style=("green" if rc == 0 else "red"))

    def _matches_filter(self, item: ScriptItem, tokens: list[str]) -> bool:
        if not tokens:
            return True

        parts: list[str] = [item.info.filename]
        if item.info.declared_name:
            parts.append(item.info.declared_name)
        if item.info.purpose:
            parts.append(item.info.purpose)

        haystack = " ".join(parts).lower()
        return all(t in haystack for t in tokens)

    def _render_output_line(self, script_filename: str, rendered: str) -> str | Text:
        if script_filename != "18-populate-musicbrainz-ids.py":
            return rendered
        if not _NAMESAKE_CANDIDATE_LINE_RE.match(rendered):
            return rendered
        if not _MBID_RE.search(rendered):
            return rendered

        rich_text = Text(rendered)
        url_mbid_spans: list[tuple[int, int]] = []

        # Make explicit MusicBrainz URLs visibly clickable in the output pane.
        for match in _MUSICBRAINZ_ARTIST_URL_RE.finditer(rendered):
            url = match.group(0)
            rich_text.stylize(f"underline cyan link {url}", match.start(), match.end())
            url_mbid_spans.append(match.span("mbid"))

        def _inside_url_mbid(start: int, end: int) -> bool:
            return any(start >= s and end <= e for s, e in url_mbid_spans)

        # Keep standalone MBIDs clickable as well.
        for match in _MBID_RE.finditer(rendered):
            start, end = match.span()
            if _inside_url_mbid(start, end):
                continue
            mbid = match.group(0)
            rich_text.stylize(
                f"underline cyan link https://musicbrainz.org/artist/{mbid}",
                start,
                end,
            )
        return rich_text

    def _refresh_script_tree(self, *, select_first_if_needed: bool) -> None:
        tree = self.query_one("#script_tree", Tree)
        try:
            tree.clear()
        except Exception:
            # Fall back to manual child removal if needed.
            for child in list(tree.root.children):
                try:
                    child.remove()
                except Exception:
                    pass

        tokens = [t for t in (self._filter_text or "").lower().split() if t]

        def expand_to_node(node: object) -> None:
            """Ensure a node is visible by expanding its parent chain."""

            cur = getattr(node, "parent", None)
            # Expand from the node's parent up to root.
            while cur is not None and cur is not getattr(tree, "root", None):
                try:
                    cur.expand()
                except Exception:
                    pass
                cur = getattr(cur, "parent", None)

        # Rebuild categorized tree.
        self._visible = []
        self._node_by_script_index = {}

        categories = [
            "Data Quality",
            "Reporting",
            "Library Health",
            "Tag Import/Export",
            "Master Data Management",
        ]

        grouped: dict[str, list[int]] = {c: [] for c in categories}

        for i, item in enumerate(self._scripts):
            if not self._matches_filter(item, tokens):
                continue
            cat = self._category_for_filename(item.info.filename, str(item.info.path))
            if cat not in grouped:
                grouped[cat] = []
            grouped[cat].append(i)

        # Add categories with at least one script.
        for cat in categories:
            indices = grouped.get(cat) or []
            show_reporting_tool = False
            show_colsel_tool = False
            if cat == "Reporting":
                action_hay = "artist graph explorer reporting"
                show_reporting_tool = all(t in action_hay for t in tokens)

            if cat == "Tag Import/Export":
                action_hay = "configure columns column selections matrix toml config"
                show_colsel_tool = all(t in action_hay for t in tokens)

            if not indices and not show_reporting_tool and not show_colsel_tool:
                continue

            cat_node = tree.root.add(cat, data=("category", cat))

            # In-app tools (not subprocess scripts).
            if cat == "Reporting" and show_reporting_tool:
                cat_node.add("Artist Graph Explorer", data=("action", "artist_graph"))
                cat_node.add("Artist Similarity Map (browser)", data=("action", "artist_map"))

            if cat == "Tag Import/Export" and show_colsel_tool:
                cat_node.add("Configure Column Selections (TOML)", data=("action", "column_matrix"))

            for script_index in indices:
                item = self._scripts[script_index]
                label = self._format_script_label(item)
                node = cat_node.add(label, data=script_index)
                self._visible.append(script_index)
                self._node_by_script_index[script_index] = node

            # Expand groups by default (show everything).
            try:
                cat_node.expand()
            except Exception:
                pass

        if not self._visible:
            self._current_script_index = None
            self._current = None
            self.sub_title = "No scripts match the filter"
            doc_widget = self.query_one("#doc", Static)
            doc_widget.update("(No matching scripts.)")
            return

        # Try to keep current selection if still visible.
        if self._current_script_index is not None and self._current_script_index in self._node_by_script_index:
            node = self._node_by_script_index[self._current_script_index]
            try:
                expand_to_node(node)
                tree.select_node(node)
                tree.cursor_node = node
                tree.scroll_to_node(node)
            except Exception:
                pass
            self._set_selected_by_script_index(self._current_script_index)
            return

        if select_first_if_needed:
            # Startup UX: select the first script leaf and show its docstring.
            # All categories are expanded by default, so this is always visible.
            first_script_index = self._visible[0]
            node = self._node_by_script_index.get(first_script_index)
            if node is not None:
                try:
                    expand_to_node(node)
                    tree.select_node(node)
                    tree.cursor_node = node
                    tree.scroll_to_node(node)
                except Exception:
                    pass
            self._set_selected_by_script_index(first_script_index)

    def _set_selected_by_script_index(self, script_index: int) -> None:
        if script_index < 0 or script_index >= len(self._scripts):
            return
        self._current_script_index = script_index
        self._current = self._scripts[script_index]
        self.post_message(ScriptSelected(self._current))

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        data = event.node.data
        if isinstance(data, int):
            self._current_action_id = None
            self._set_selected_by_script_index(data)
            return
        if isinstance(data, tuple) and len(data) == 2 and data[0] == "action":
            self._current_script_index = None
            self._current = None
            action_id = str(data[1])
            self._current_action_id = action_id
            self.sub_title = f"Tool: {action_id}"
            doc_widget = self.query_one("#doc", Static)
            if action_id == "artist_graph":
                doc_widget.update(
                    "Artist Graph Explorer\n\n"
                    "Browse an endless artist/person graph built from alib.\n"
                    "Similarity blends album-level co-occurrence + shared credits roles + shared genre/style tags, with MBID-aware unification when available.\n\n"
                    "Keys: Enter to navigate, b to go back, Esc to return."
                )
            elif action_id == "artist_map":
                doc_widget.update(
                    "Artist Similarity Map (browser)\n\n"
                    "Generate a music-map style, pan/zoom similarity map in your browser, limited to artists present in your library.\n"
                    "Starts from a high-degree seed; click nodes to focus; as you pan toward edges, it expands into new connected artists.\n\n"
                    "Similarity blends album-level co-occurrence + shared credits roles + shared genre/style tags, with MBID-aware unification when available."
                )
            elif action_id == "column_matrix":
                doc_widget.update(
                    "Configure Column Selections (TOML)\n\n"
                    "Edit column membership lists in tagminder.toml via a matrix grid (keep/dedupe/critical/multi).\n"
                    "Edits apply only on Save; a timestamped .bak-* backup is written next to tagminder.toml for easy rollback.\n\n"
                    "Keys: arrows to move, Space/Enter to toggle, s to save, Esc to return."
                )
            else:
                doc_widget.update("(Unknown tool.)")
            return
        if isinstance(data, tuple) and len(data) == 2 and data[0] == "category":
            self._current_script_index = None
            self._current = None
            self._current_action_id = None
            category = str(data[1])
            self.sub_title = f"Group: {category}"
            doc_widget = self.query_one("#doc", Static)
            doc_widget.update(self._category_overview(category))

            # Clear selection label to avoid implying a runnable script.
            return

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        # Selection only. Running is handled in on_key/action_run_selected or double-click detection.
        data = event.node.data
        
        # Detect double-click: same node selected within 300ms.
        current_time = time.time()
        is_double_click = (
            self._last_selected_node is event.node 
            and (current_time - self._last_selected_time) < 0.3
        )
        self._last_selected_node = event.node
        self._last_selected_time = current_time
        
        if isinstance(data, int):
            self._current_action_id = None
            self._set_selected_by_script_index(data)
            if is_double_click:
                # Execute the script on double-click.
                self.action_run_selected()
        elif isinstance(data, tuple) and len(data) == 2 and data[0] == "action":
            self._current_script_index = None
            self._current = None
            action_id = str(data[1])
            self._current_action_id = action_id
            if is_double_click:
                # Launch the action on double-click.
                self._run_action(action_id)
        elif isinstance(data, tuple) and len(data) == 2 and data[0] == "category":
            self._current_script_index = None
            self._current = None
            self._current_action_id = None
            category = str(data[1])
            self.sub_title = f"Group: {category}"
            doc_widget = self.query_one("#doc", Static)
            doc_widget.update(self._category_overview(category))
        try:
            event.tree.focus()
        except Exception:
            pass

    def on_key(self, event: events.Key) -> None:
        # Only run on Enter when the script list has focus.
        if event.key == "enter":
            focused = self.focused
            if getattr(focused, "id", None) == "script_tree":
                # Only run scripts when the cursor is on a leaf node.
                # If a category is selected, let the Tree handle Enter (expand/collapse).
                try:
                    tree = self.query_one("#script_tree", Tree)
                    node = tree.cursor_node
                    if node is not None:
                        data = getattr(node, "data", None)
                        if isinstance(data, int):
                            self.action_run_selected()
                            event.stop()
                            return
                        if isinstance(data, tuple) and len(data) == 2 and data[0] == "action":
                            self._run_action(str(data[1]))
                            event.stop()
                            return
                except Exception:
                    pass

    def on_input_changed(self, event: Input.Changed) -> None:
        # Filter box should live-update the script list.
        if getattr(event.input, "id", None) != "filter":
            return
        self._filter_text = (event.value or "").strip()
        self._refresh_script_tree(select_first_if_needed=True)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Send script input to the running subprocess's stdin."""
        if getattr(event.input, "id", None) != "script_input":
            return
        text = event.value
        event.input.value = ""  # Clear the input field for next response.
        if self._proc_stdin:
            try:
                self._proc_stdin.write((text + "\n").encode("utf-8"))
            except Exception:
                pass  # Subprocess may have already closed stdin.

    def on_script_selected(self, message: ScriptSelected) -> None:
        doc_widget = self.query_one("#doc", Static)
        script = message.script

        if script.docstring:
            doc_widget.update(script.docstring.strip())
        else:
            doc_widget.update("(No module docstring found.)")

        self.sub_title = f"Selected: {script.info.filename}"

    def action_clear_output(self) -> None:
        out = self.query_one("#output", RichLog)
        out.clear()
        self._output_lines.clear()

    def action_copy_output(self) -> None:
        text = "\n".join(self._output_lines).rstrip("\n")
        if not text:
            self.bell()
            return

        # Textual exposes clipboard support on App in most versions.
        copied = False
        try:
            copy_fn = getattr(self, "copy_to_clipboard", None)
            if callable(copy_fn):
                copy_fn(text)
                copied = True
        except Exception:
            copied = False

        out = self.query_one("#output", RichLog)
        if copied:
            out.write("\n[copied output to clipboard]\n")
        else:
            out.write("\n[copy failed: clipboard not available in this environment]\n")

    def action_focus_script_input(self) -> None:
        try:
            self.query_one("#script_input", Input).focus()
        except Exception:
            self.bell()

    def action_focus_script_tree(self) -> None:
        try:
            self.query_one("#script_tree", Tree).focus()
        except Exception:
            self.bell()

    def action_run_selected(self) -> None:
        # Run only the currently selected script.
        # (Use 'n' / action_run_next for run + advance.)
        self._advance_after_run = False

        if self._running_task and not self._running_task.done():
            self.bell()
            return

        if not self._current:
            if self._current_action_id:
                self._run_action(self._current_action_id)
                return
            self.bell()
            return

        self._running_task = asyncio.create_task(self._run_script(self._current))

    def _run_action(self, action_id: str) -> None:
        action_id = (action_id or "").strip()
        if not action_id:
            self.bell()
            return

        # Avoid running tools while a script/tool task is active.
        if self._running_task and not self._running_task.done():
            self.bell()
            return

        if action_id == "artist_graph":
            try:
                self.push_screen(ArtistGraphExplorerScreen())
            except Exception:
                self.bell()
            return
        if action_id == "artist_map":
            self._running_task = asyncio.create_task(self._run_artist_map_tool())
            return
        if action_id == "column_matrix":
            try:
                self.push_screen(ColumnSelectionMatrixScreen())
            except Exception:
                self.bell()
            return
        self.bell()

    def _write_output_line(self, text: str) -> None:
        out = self.query_one("#output", RichLog)
        out.write(text)
        self._output_lines.append(str(text))
        if len(self._output_lines) > self._output_line_cap:
            del self._output_lines[: len(self._output_lines) - self._output_line_cap]

    async def _run_artist_map_tool(self) -> None:
        self._write_output_line("\n=== Tool: artist_map ===")

        try:
            cache_dir = tm_config.get_cache_dir(default="/tmp")
        except Exception:
            cache_dir = "/tmp"

        out_dir = Path(cache_dir)
        out_latest = out_dir / "tagminder-artist-similarity-map.html"

        import time

        run_id = int(time.time() * 1000)
        out_path = out_dir / f"tagminder-artist-similarity-map.{run_id}.html"

        cached = getattr(self, "_artist_graph_cache", None)
        graph: tm_graph.WeightedGraph
        if isinstance(cached, tm_graph.WeightedGraph):
            graph = cached
        else:

            def build() -> tm_graph.WeightedGraph:
                db_path = tm_config.get_db_path(default=None)
                delimiter = tm_config.get_multivalue_delimiter()
                system_prefix = tm_config.get_system_prefix()
                return tm_graph.build_artist_similarity_graph(
                    db_path=db_path,
                    system_prefix=system_prefix,
                    delimiter=delimiter,
                )

            graph = await asyncio.to_thread(build)
            setattr(self, "_artist_graph_cache", graph)

        def write() -> Path:
            return tm_artist_map.write_artist_similarity_map_html(
                out_path=out_path,
                graph=graph,
                top_k_per_node=30,
                min_weight=1,
            )

        try:
            path = await asyncio.to_thread(write)
        except Exception as e:
            self._write_output_line(f"[artist_map] Failed to write map: {e}")
            return

        # Also update the stable path for convenience.
        try:
            import shutil

            shutil.copy2(path, out_latest)
        except Exception:
            pass

        url = path.resolve().as_uri()

        opened = False
        try:
            import webbrowser

            opened = bool(webbrowser.open(url))
        except Exception:
            opened = False

        if not opened:
            try:
                subprocess.Popen(["xdg-open", url])
                opened = True
            except Exception:
                opened = False

        self._write_output_line(f"[artist_map] Wrote: {path}")
        if opened:
            self._write_output_line("[artist_map] Opened in your browser.")
        else:
            self._write_output_line("[artist_map] Could not auto-open browser; open the HTML file manually.")

    def action_run_next(self) -> None:
        # Run selected, then advance selection to the next numbered step.
        self._advance_after_run = True

        if self._running_task and not self._running_task.done():
            self.bell()
            return

        if not self._current:
            self.bell()
            return

        self._running_task = asyncio.create_task(self._run_script(self._current))

    def _advance_selection_to_next_numbered(self) -> None:
        if self._current_script_index is None:
            return

        # Keep keyboard navigation working after advancing.
        try:
            self.query_one("#script_tree", Tree).focus()
        except Exception:
            pass

        current_item = self._scripts[self._current_script_index]
        current_step = self._parse_step_number(current_item.info.filename)

        if current_step is None:
            return

        # Choose the next step by numeric order among visible scripts.
        # The Tree display order is grouped by category and is not guaranteed to
        # match step order.
        candidates: list[tuple[int, int]] = []  # (step, script_index)
        for script_index in self._visible:
            step = self._parse_step_number(self._scripts[script_index].info.filename)
            if step is None:
                continue
            if step > current_step:
                candidates.append((step, script_index))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0])
        nxt_script_index = candidates[0][1]

        node = self._node_by_script_index.get(nxt_script_index)
        if node is not None:
            try:
                tree = self.query_one("#script_tree", Tree)
                try:
                    tree.focus()
                except Exception:
                    pass
                # All groups start collapsed; ensure the new leaf is visible.
                cur = getattr(node, "parent", None)
                while cur is not None and cur is not getattr(tree, "root", None):
                    try:
                        cur.expand()
                    except Exception:
                        pass
                    cur = getattr(cur, "parent", None)
                tree.select_node(node)
                tree.cursor_node = node
                tree.scroll_to_node(node)
            except Exception:
                pass

        self._set_selected_by_script_index(nxt_script_index)

    async def _run_script(self, script: ScriptItem) -> None:
        from datetime import datetime

        out = self.query_one("#output", RichLog)
        args_widget = self.query_one("#args", Input)

        raw_args = (args_widget.value or "").strip()
        script_args: list[str] = []
        if raw_args:
            try:
                script_args = shlex.split(raw_args)
            except ValueError as e:
                out.write(f"\n[args parse error] {e}\n")
                return
            if script_args and script_args[0] == "--":
                script_args = script_args[1:]

        cmd = [sys.executable, str(script.info.path), *script_args]
        out.write(f"\n=== Running: {script.info.filename} ===")
        out.write(f"CMD: {shlex.join(cmd)}\n")

        self._output_lines.append(f"=== Running: {script.info.filename} ===")
        self._output_lines.append(f"CMD: {shlex.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(_repo_root()),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        self._proc_stdin = proc.stdin

        async def pump(stream: asyncio.StreamReader | None, prefix: str) -> None:
            if stream is None:
                return
            while True:
                line = await stream.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                rendered = f"{prefix}{text}"
                out.write(self._render_output_line(script.info.filename, rendered))

                self._output_lines.append(rendered)
                if len(self._output_lines) > self._output_line_cap:
                    # Drop oldest lines to avoid unbounded memory growth.
                    del self._output_lines[: len(self._output_lines) - self._output_line_cap]

        await asyncio.gather(
            pump(proc.stdout, ""),
            pump(proc.stderr, "[stderr] "),
        )

        rc = await proc.wait()
        out.write(f"\n=== Exit code: {rc} ===\n")
        self._output_lines.append(f"=== Exit code: {rc} ===")

        # Close stdin and clear the reference.
        if self._proc_stdin:
            try:
                self._proc_stdin.close()
            except Exception:
                pass
        self._proc_stdin = None

        ts = datetime.now().strftime("%H:%M:%S")
        self._last_run[script.info.filename] = (int(rc), ts)

        # Refresh list labels to show last-run status.
        self._refresh_script_tree(select_first_if_needed=False)

        if self._advance_after_run:
            self._advance_after_run = False
            self._advance_selection_to_next_numbered()


def main() -> None:
    ScriptRunnerApp().run()


if __name__ == "__main__":
    main()
