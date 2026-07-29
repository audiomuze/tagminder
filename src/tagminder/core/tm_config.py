"""Tagminder configuration helpers.

Purpose:
    Centralize reading Tagminder configuration so scripts share consistent
    defaults (e.g., database path, delimiter settings, logging level).

    This module is intentionally lightweight (stdlib-only).

This module is part of Tagminder.

SQLite tables referenced:
    - None

Author: audiomuze
Last updated: 2026-04-15
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Sequence
import logging
import sys

import tomllib


def _find_default_config_path() -> Path:
    module_path = Path(__file__).resolve()
    for parent in module_path.parents:
        candidate = parent / "tagminder.toml"
        if candidate.exists():
            return candidate
    # Fallback to repo-root expectation for src layout.
    return module_path.parents[3] / "tagminder.toml"


_DEFAULT_CONFIG_PATH = _find_default_config_path()


@lru_cache(maxsize=1)
def _load_toml(path: Path) -> dict:
    with path.open("rb") as f:
        return tomllib.load(f)


def load_config(*, config_path: str | Path | None = None) -> dict:
    """Load `tagminder.toml` as a dict.

    Returns an empty dict if the file does not exist.
    """

    path = Path(config_path) if config_path is not None else _DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    return _load_toml(path)


def db_path_from_toml(*, default: str | None = None, config_path: str | Path | None = None) -> str | None:
    """Return `[db].path` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    db = cfg.get("db", {}) if isinstance(cfg, dict) else {}
    path = db.get("path") if isinstance(db, dict) else None
    if path:
        return str(path)
    return default


def _parse_db_override(argv: Sequence[str]) -> str | None:
    # Support: --db PATH, --db=PATH
    for i, token in enumerate(argv):
        if token == "--db":
            if i + 1 < len(argv):
                return argv[i + 1]
            return None
        if token.startswith("--db="):
            return token.split("=", 1)[1] or None
    return None


def get_db_path(
    *,
    argv: Sequence[str] | None = None,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str:
    """Resolve the database path.

    Precedence:
        1) CLI override via `--db` in argv
        2) `tagminder.toml` `[db].path`
        3) `default`

    Raises:
        ValueError if no path can be resolved.
    """

    argv = list(sys.argv[1:] if argv is None else argv)

    override = _parse_db_override(argv)
    if override:
        return override

    cfg_path = db_path_from_toml(default=default, config_path=config_path)
    if cfg_path:
        return cfg_path

    raise ValueError("No database path resolved (missing --db and [db].path)")


def master_data_db_path_from_toml(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str | None:
    """Return `[master_data].path` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    md_cfg = cfg.get("master_data", {}) if isinstance(cfg, dict) else {}
    path = md_cfg.get("path") if isinstance(md_cfg, dict) else None
    if path:
        return str(path)
    return default


def get_master_data_db_path(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str:
    """Resolve the master-data database path.

    Precedence:
        1) `tagminder.toml` `[master_data].path`
        2) `default`

    Raises:
        ValueError if no path can be resolved.
    """

    path = master_data_db_path_from_toml(default=default, config_path=config_path)
    if path:
        return path

    raise ValueError("No master-data database path resolved (missing [master_data].path)")


def multivalue_delimiter_from_toml(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str | None:
    """Return `[strings].multivalue_delimiter` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    strings_cfg = cfg.get("strings", {}) if isinstance(cfg, dict) else {}
    delim = strings_cfg.get("multivalue_delimiter") if isinstance(strings_cfg, dict) else None
    if isinstance(delim, str) and delim:
        return delim
    return default


def get_multivalue_delimiter(
    *,
    default: str | None = "\\\\",
    config_path: str | Path | None = None,
) -> str:
    """Resolve the multi-value delimiter.

    Raises ValueError if the delimiter can't be resolved.
    """

    delim = multivalue_delimiter_from_toml(default=default, config_path=config_path)
    if isinstance(delim, str) and delim:
        return delim
    raise ValueError("No multivalue delimiter resolved (missing [strings].multivalue_delimiter)")


def cache_dir_from_toml(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str | None:
    """Return `[paths].cache_dir` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    paths_cfg = cfg.get("paths", {}) if isinstance(cfg, dict) else {}
    cache_dir = paths_cfg.get("cache_dir") if isinstance(paths_cfg, dict) else None
    if cache_dir:
        return str(cache_dir)
    return default


def get_cache_dir(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str:
    """Resolve the cache directory.

    Raises ValueError if no cache dir can be resolved.
    """

    cache_dir = cache_dir_from_toml(default=default, config_path=config_path)
    if cache_dir:
        return cache_dir
    raise ValueError("No cache_dir resolved (missing [paths].cache_dir)")


def log_level_name_from_toml(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str | None:
    """Return `[logging].level` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    logging_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
    level_name = logging_cfg.get("level") if isinstance(logging_cfg, dict) else None
    if isinstance(level_name, str) and level_name:
        return level_name
    return default


def get_log_level(
    *,
    default: int = logging.INFO,
    config_path: str | Path | None = None,
) -> int:
    """Resolve logging level as a `logging` module constant.

    If `[logging].level` is missing or invalid, returns `default`.
    """

    level_name = log_level_name_from_toml(default=None, config_path=config_path)
    if not level_name:
        return int(default)

    level = getattr(logging, str(level_name).upper(), None)
    if isinstance(level, int):
        return level

    return int(default)


def system_prefix_from_toml(
    *,
    default: str | None = None,
    config_path: str | Path | None = None,
) -> str | None:
    """Return `[columns].system_prefix` from `tagminder.toml` (or `default`)."""

    cfg = load_config(config_path=config_path)
    cols_cfg = cfg.get("columns", {}) if isinstance(cfg, dict) else {}
    system_prefix = cols_cfg.get("system_prefix") if isinstance(cols_cfg, dict) else None
    if isinstance(system_prefix, str) and system_prefix:
        return system_prefix
    return default


def get_system_prefix(
    *,
    default: str = "__",
    config_path: str | Path | None = None,
) -> str:
    """Resolve system column prefix.

    Raises ValueError if the prefix can't be resolved.
    """

    system_prefix = system_prefix_from_toml(default=default, config_path=config_path)
    if isinstance(system_prefix, str) and system_prefix:
        return system_prefix
    raise ValueError("No system_prefix resolved (missing [columns].system_prefix)")
