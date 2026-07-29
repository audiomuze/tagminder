#!/usr/bin/env python3
"""zz-configure-column-selections-in-toml.py

Purpose:
	Interactive TUI editor for column-list membership in tagminder.toml.

	Shows a matrix grid:
	- Rows: column names (excluding system columns, e.g. '__*')
	- Columns: list memberships (keep/dedupe/critical/multi-value)

	Edits are applied in-memory and only written to disk on Save.
	On Save, a timestamped backup of tagminder.toml is created next to it so
	you can back out immediately.

Keys:
	Arrows     - move cursor
	Space/Enter- toggle membership cell
	s          - save (writes tagminder.toml + creates backup)
	q / Esc    - quit (no write)

This script is part of Tagminder.

SQLite tables referenced:
	- None

Author: audiomuze
Last updated: 2026-04-26
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import tomllib

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static


@dataclass(frozen=True)
class ListSpec:
	# TOML table header (e.g. "cleanup" or "reports.multi_value_tags_by_album")
	table: str
	# Key within that table (e.g. "keep_columns")
	key: str
	# UI label (short)
	label: str

	@property
	def path(self) -> str:
		return f"{self.table}.{self.key}"


_LIST_SPECS: list[ListSpec] = [
	ListSpec("cleanup", "keep_columns", "keep"),
	ListSpec("cleanup", "dedupe_columns", "dedupe"),
	ListSpec("reports.missing_critical_tags_by_album", "critical_columns", "critical"),
	ListSpec("reports.multi_value_tags_by_album", "tags", "multi"),
]


def _load_toml(path: Path) -> dict[str, Any]:
	with path.open("rb") as f:
		data = tomllib.load(f)
	if not isinstance(data, dict):
		raise ValueError(f"Top-level TOML must be a table: {path}")
	return data

def _resolve_default_config_path() -> Path:
	cwd_candidate = (Path.cwd() / "tagminder.toml").resolve()
	if cwd_candidate.exists():
		return cwd_candidate

	script_path = Path(__file__).resolve()
	checked: list[Path] = [cwd_candidate]
	for parent in script_path.parents:
		candidate = (parent / "tagminder.toml").resolve()
		checked.append(candidate)
		if candidate.exists():
			return candidate

	looked_in = "\n".join(f"- {path}" for path in checked)
	raise SystemExit(f"Config not found. Looked in:\n{looked_in}")


def _system_prefix(cfg: dict[str, Any]) -> str:
	cols = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
	if isinstance(cols, dict):
		sp = cols.get("system_prefix")
		if isinstance(sp, str) and sp:
			return sp
	return "__"


def _schema_columns(cfg: dict[str, Any]) -> list[str]:
	cols = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
	schema = cols.get("schema_columns") if isinstance(cols, dict) else None
	if isinstance(schema, list):
		out: list[str] = []
		seen: set[str] = set()
		for x in schema:
			if not isinstance(x, str):
				continue
			name = x.strip()
			if not name or name in seen:
				continue
			seen.add(name)
			out.append(name)
		return out
	return []


def _get_list(cfg: dict[str, Any], spec: ListSpec) -> list[str]:
	# Walk dotted table path.
	cur: Any = cfg
	for part in spec.table.split("."):
		if not isinstance(cur, dict):
			return []
		cur = cur.get(part)

	if not isinstance(cur, dict):
		return []
	val = cur.get(spec.key)
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


def _sanitize_col(name: str | None) -> str:
	return (name or "").strip()


def _backup_name(original: Path) -> Path:
	ts = datetime.now().strftime("%Y%m%d-%H%M%S")
	return original.with_name(original.name + f".bak-{ts}")


def _is_section_header(line: str) -> bool:
	s = line.strip()
	return s.startswith("[") and s.endswith("]") and not s.startswith("[[")


def _format_list_block(key: str, items: list[str], *, indent: str, item_indent: str) -> list[str]:
	lines: list[str] = []
	lines.append(f"{indent}{key} = [")
	for it in items:
		lines.append(f"{item_indent}\"{it}\",")
	lines.append(f"{indent}]")
	return lines


def _find_table_range(lines: list[str], table: str) -> tuple[int, int] | None:
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
		if _is_section_header(lines[j]):
			end = j
			break
	return start, end


def _replace_or_insert_list(
	*,
	text: str,
	table: str,
	key: str,
	items: list[str],
) -> str:
	lines = text.splitlines()
	rng = _find_table_range(lines, table)
	if rng is None:
		# Append new table at EOF.
		lines.append("")
		lines.append(f"[{table}]")
		indent = ""
		item_indent = "\t"
		lines.extend(_format_list_block(key, items, indent=indent, item_indent=item_indent))
		return "\n".join(lines) + ("\n" if text.endswith("\n") else "")

	start, end = rng

	# Look for an existing multiline list block: key = [ ... ]
	open_re = f"{key}"
	block_start = None
	block_end = None
	indent = ""
	item_indent = "\t"

	for i in range(start + 1, end):
		ln = lines[i]
		stripped = ln.lstrip(" \t")
		if not stripped.startswith(open_re):
			continue
		# key = [
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
			# Try to infer item indent from first item line.
			for j in range(i + 1, end):
				if "]" in lines[j]:
					break
				s2 = lines[j].strip()
				if not s2:
					continue
				item_indent = lines[j][: len(lines[j]) - len(lines[j].lstrip(" \t"))] or (indent + "\t")
				break
			# Find closing bracket line.
			for j in range(i + 1, end):
				if lines[j].strip().startswith("]"):
					block_end = j
					break
			break

	new_block = _format_list_block(key, items, indent=indent, item_indent=item_indent)

	if block_start is not None and block_end is not None:
		lines = [*lines[:block_start], *new_block, *lines[block_end + 1 :]]
		return "\n".join(lines) + ("\n" if text.endswith("\n") else "")

	# Insert at end of table (before next section header).
	insert_at = end
	# Keep a blank line separation if possible.
	prefix = []
	if insert_at > 0 and lines[insert_at - 1].strip():
		prefix = [""]
	lines = [*lines[:insert_at], *prefix, *new_block, *lines[insert_at:]]
	return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


class ColumnMatrixApp(App[None]):
	CSS = """
	Screen {
		layout: vertical;
	}

	#topbar {
		height: auto;
		padding: 0 1;
	}

	#filter {
		height: 3;
	}

	#status {
		height: auto;
		padding: 0 1;
		opacity: 0.85;
	}

	#table {
		height: 1fr;
	}
	"""

	BINDINGS = [
		Binding("q", "quit", "Quit"),
		Binding("escape", "quit", "Quit"),
		Binding("s", "save", "Save"),
		Binding("enter", "toggle", "Toggle"),
		Binding("space", "toggle", "Toggle"),
	]

	def __init__(self, *, config_path: Path) -> None:
		super().__init__()
		self._config_path = config_path
		self._cfg: dict[str, Any] = {}
		self._system_prefix = "__"
		self._schema: list[str] = []
		self._all_columns: list[str] = []
		self._filter_text: str = ""
		self._backup_path: Path | None = None

		# membership: spec.path -> set(columns)
		self._members: dict[str, set[str]] = {}
		# ordering: spec.path -> list(columns)
		self._order: dict[str, list[str]] = {}

	def compose(self) -> ComposeResult:
		yield Header()
		with Vertical(id="topbar"):
			yield Static("Configure column selections (tagminder.toml)")
			yield Input(placeholder="Filter columns…", id="filter")
			yield Static("Loading…", id="status")
		yield DataTable(id="table")
		yield Footer()

	def on_mount(self) -> None:
		self._load()
		self._build_table()
		self.query_one("#filter", Input).focus()

	def _load(self) -> None:
		self._cfg = _load_toml(self._config_path)
		self._system_prefix = _system_prefix(self._cfg)
		self._schema = _schema_columns(self._cfg)

		# Columns are primarily derived from schema_columns; union with any list items.
		schema_cols = [c for c in self._schema if not c.startswith(self._system_prefix)]
		all_set = set(schema_cols)

		for spec in _LIST_SPECS:
			items = _get_list(self._cfg, spec)
			items = [c for c in items if c and not c.startswith(self._system_prefix)]
			self._members[spec.path] = set(items)
			self._order[spec.path] = items
			all_set.update(items)

		# Keep schema order first; then extras (stable alphabetical).
		extras = sorted([c for c in all_set if c not in schema_cols])
		self._all_columns = [*schema_cols, *extras]

		status = self.query_one("#status", Static)
		status.update(
			f"Config: {self._config_path} | Columns: {len(self._all_columns):,d} | "
			+ " | ".join(
				f"{spec.label}={len(self._members.get(spec.path, set())):,d}" for spec in _LIST_SPECS
			)
		)

	def on_input_changed(self, event: Input.Changed) -> None:
		if getattr(event.input, "id", None) != "filter":
			return
		self._filter_text = (event.value or "").strip().lower()
		self._build_table()

	def _filtered_columns(self) -> list[str]:
		if not self._filter_text:
			return self._all_columns
		needle = self._filter_text
		return [c for c in self._all_columns if needle in c.lower()]

	def _build_table(self) -> None:
		table = self.query_one("#table", DataTable)
		table.clear(columns=True)

		# Columns
		table.add_column("column", key="col")
		for spec in _LIST_SPECS:
			table.add_column(spec.label, key=spec.path)

		# Rows
		for col in self._filtered_columns():
			row = [col]
			for spec in _LIST_SPECS:
				row.append("✓" if col in self._members.get(spec.path, set()) else "")
			table.add_row(*row, key=col)

		# Keep the cursor usable.
		try:
			table.cursor_type = "cell"
			table.focus()
		except Exception:
			pass

	def action_toggle(self) -> None:
		table = self.query_one("#table", DataTable)
		try:
			row_key = table.coordinate_to_cell_key(table.cursor_coordinate).row_key
			col_key = table.coordinate_to_cell_key(table.cursor_coordinate).column_key
		except Exception:
			return

		if not isinstance(row_key, str) or not row_key:
			return
		if not isinstance(col_key, str) or not col_key:
			return
		if col_key == "col":
			return

		members = self._members.setdefault(col_key, set())
		if row_key in members:
			members.remove(row_key)
		else:
			members.add(row_key)
			# Preserve order for newly-added items.
			order = self._order.setdefault(col_key, [])
			if row_key not in order:
				order.append(row_key)

		# Update the single cell (avoid full rebuild).
		table.update_cell(row_key, col_key, "✓" if row_key in members else "")

	def action_save(self) -> None:
		# Build list values in stable order (preserve original order; append new).
		list_values: dict[str, list[str]] = {}
		for spec in _LIST_SPECS:
			path = spec.path
			members = self._members.get(path, set())
			order = self._order.get(path, [])
			out = [c for c in order if c in members]
			# Any members not in order get appended in schema order (stable).
			missing = [c for c in self._all_columns if (c in members and c not in set(order))]
			out.extend(missing)
			# Deduplicate just in case.
			seen: set[str] = set()
			out2: list[str] = []
			for c in out:
				cc = _sanitize_col(c)
				if not cc or cc in seen:
					continue
				seen.add(cc)
				out2.append(cc)
			list_values[path] = out2

		# Read original TOML text.
		orig = self._config_path.read_text(encoding="utf-8")

		# Create backup once per session.
		if self._backup_path is None:
			self._backup_path = _backup_name(self._config_path)
			self._backup_path.write_text(orig, encoding="utf-8")

		new_text = orig
		for spec in _LIST_SPECS:
			items = list_values.get(spec.path, [])
			new_text = _replace_or_insert_list(
				text=new_text,
				table=spec.table,
				key=spec.key,
				items=items,
			)

		self._config_path.write_text(new_text, encoding="utf-8")

		status = self.query_one("#status", Static)
		status.update(
			f"Saved. Backup: {self._backup_path} | "
			+ " | ".join(
				f"{spec.label}={len(self._members.get(spec.path, set())):,d}" for spec in _LIST_SPECS
			)
		)


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		prog=Path(__file__).name,
		description="TUI matrix editor for tagminder.toml column selections.",
	)
	parser.add_argument(
		"--config",
		metavar="PATH",
		default=None,
		help="Path to tagminder.toml (default: repo root tagminder.toml)",
	)
	args = parser.parse_args(argv)

	config_path = Path(args.config).expanduser().resolve() if args.config else _resolve_default_config_path()
	if not config_path.exists():
		raise SystemExit(f"Config not found: {config_path}")

	app = ColumnMatrixApp(config_path=config_path)
	app.run()
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
