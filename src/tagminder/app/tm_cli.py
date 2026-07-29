"""

Purpose:
    A minimal command runner for Tagminder.

Design goals:
- Zero changes to the behavior of the existing ETL scripts.
- Discover scripts in the repository scripts/ tree and run them as subprocesses.
- Provide a stable surface area that a future TUI can call into.

Note: To pass arguments to the target script, put them after `--`.

This module is part of Tagminder.

SQLite tables referenced:
        - None

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


def _find_repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "tagminder.toml").exists() and (parent / "scripts").exists():
            return parent
    # Fallback for expected src/tagminder/app layout.
    return here.parents[3]


REPO_ROOT = _find_repo_root()
SCRIPTS_ROOT = REPO_ROOT / "scripts"


@dataclass(frozen=True)
class ScriptInfo:
    filename: str
    path: Path
    declared_name: str | None
    purpose: str | None


def _is_candidate_script(path: Path) -> bool:
    if path.suffix != ".py":
        return False
    if path.name.startswith("."):
        return False
    if path.name.startswith("__"):
        return False
    if not path.is_relative_to(SCRIPTS_ROOT):
        return False
    if path.name == Path(__file__).name:
        return False

    # Only show runnable scripts in the UI/CLI.
    # Policy:
    # - Include numbered pipeline scripts (e.g. 01-*.py)
    # - Include a small allowlist of standalone utilities
    # - Exclude helper modules (e.g. tm_*.py, main.py)
    allowlist = {
        "tags2db.py",
    }

    if path.name in allowlist:
        return True

    if path.name[0].isdigit():
        return True

    return False


def _extract_docstring(path: Path) -> str | None:
    try:
        source = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        source = path.read_text(encoding="utf-8", errors="replace")

    try:
        module = ast.parse(source)
    except SyntaxError:
        return None

    return ast.get_docstring(module)


def _parse_declared_name_and_purpose(docstring: str | None) -> tuple[str | None, str | None]:
    if not docstring:
        return None, None

    declared_name: str | None = None
    purpose: str | None = None

    lines = [line.rstrip() for line in docstring.splitlines()]

    # Common patterns in this repo:
    # - "Purpose:" then indented body
    for i, line in enumerate(lines):
        if purpose is None and line.lower().startswith("purpose:"):
            # Capture subsequent non-empty lines until the next section header.
            body: list[str] = []
            for j in range(i + 1, len(lines)):
                nxt = lines[j]
                if not nxt.strip():
                    if body:
                        body.append("")
                    continue
                # A crude section-header heuristic.
                if nxt.endswith(":") and len(nxt.split()) <= 4:
                    break
                body.append(nxt.strip())
            # Collapse whitespace/newlines; keep it short.
            purpose_text = " ".join([part for part in (" ".join(body)).split()])
            purpose = purpose_text or None

    return declared_name, purpose


def discover_scripts() -> list[ScriptInfo]:
    scripts: list[ScriptInfo] = []
    for path in sorted(SCRIPTS_ROOT.rglob("*.py")):
        if not _is_candidate_script(path):
            continue
        doc = _extract_docstring(path)
        declared_name, purpose = _parse_declared_name_and_purpose(doc)
        scripts.append(
            ScriptInfo(
                filename=path.name,
                path=path,
                declared_name=declared_name,
                purpose=purpose,
            )
        )
    return scripts


def _get_script_by_filename(filename: str) -> ScriptInfo:
    scripts = discover_scripts()
    normalized = filename.strip()

    for script in scripts:
        if script.filename == normalized:
            return script

    # Allow specifying without `.py`
    if not normalized.endswith(".py"):
        candidate = normalized + ".py"
        for script in scripts:
            if script.filename == candidate:
                return script

    available = ", ".join(s.filename for s in scripts)
    raise SystemExit(f"Unknown script '{filename}'. Available: {available}")


def cmd_list(verbose: bool) -> int:
    scripts = discover_scripts()
    if not verbose:
        for s in scripts:
            print(s.filename)
        return 0

    for s in scripts:
        label = s.declared_name or s.filename
        if s.purpose:
            print(f"{s.filename}\t{label}\t{s.purpose}")
        else:
            print(f"{s.filename}\t{label}")
    return 0


def cmd_describe(script_filename: str) -> int:
    script = _get_script_by_filename(script_filename)
    doc = _extract_docstring(script.path)

    print(script.filename)
    if script.purpose:
        print(f"Purpose: {script.purpose}")
    if doc:
        print("\n--- Docstring ---\n")
        print(doc.rstrip())

    return 0


def cmd_run(script_filename: str, script_args: Iterable[str]) -> int:
    script = _get_script_by_filename(script_filename)

    cmd = [sys.executable, str(script.path), *script_args]
    completed = subprocess.run(cmd, cwd=str(REPO_ROOT))
    return int(completed.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=Path(sys.argv[0]).name,
        description="List and run repository scripts (without changing them).",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List available scripts")
    p_list.add_argument(
        "--verbose",
        action="store_true",
        help="Include docstring-derived name/purpose (tab-separated)",
    )

    p_desc = sub.add_parser("describe", help="Show script description/docstring")
    p_desc.add_argument("script", help="Script filename")

    p_run = sub.add_parser("run", help="Run a script as a subprocess")
    p_run.add_argument("script", help="Script filename")
    p_run.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments to pass to the script (prefix with `--`)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list":
        return cmd_list(verbose=bool(args.verbose))

    if args.command == "describe":
        return cmd_describe(args.script)

    if args.command == "run":
        script_args = list(args.script_args)
        if script_args and script_args[0] == "--":
            script_args = script_args[1:]
        return cmd_run(args.script, script_args)

    raise SystemExit(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
