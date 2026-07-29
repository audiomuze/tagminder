#!/usr/bin/env python3
"""

Purpose:
    Canonical Tagminder launcher.

    This entrypoint launches the Textual TUI script runner.

    Intended usage:
        uv run --project <project_dir> <launcher_path>

This module is part of Tagminder.

SQLite tables referenced:
    - None

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

from tagminder.app import tm_tui

def main() -> None:
    tm_tui.main()


if __name__ == "__main__":
    main()
