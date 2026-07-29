"""Album path helpers.

Purpose:
    Provide a single source of truth for Tagminder's "album-root" folder logic.

Definitions:
    - album_root(dirpath):
        If the final path component looks like a disc subfolder (CD1, Disc 02, disc003),
        return its parent folder. Otherwise return the folder itself.

Notes:
    - This logic is used across multiple reports/dashboards to group multi-disc
      albums consistently.
    - We provide:
        - a pure-Python scalar function (for set-building and SQLite UDFs)
        - a Polars expression builder (vectorized; for Polars group-by pipelines)

This module is part of Tagminder.

Author: audiomuze
Last updated: 2026-04-18
"""

from __future__ import annotations

import re
import sqlite3


DISC_SUBFOLDER_RE_STR = r"^(?:cd|disc)\s*0*\d{1,3}$"
_DISC_SUBFOLDER_RE = re.compile(DISC_SUBFOLDER_RE_STR, flags=re.IGNORECASE)


def album_root(dirpath: str) -> str:
    """Return the album-root folder for a track folder path."""

    p = (dirpath or "").rstrip("/")
    if not p:
        return p

    leaf = p.rsplit("/", 1)[-1]
    if _DISC_SUBFOLDER_RE.match(leaf.strip()):
        parent = p.rsplit("/", 1)[0] if "/" in p else ""
        return parent or p

    return p


def register_sql_functions(conn: sqlite3.Connection, *, func_name: str = "album_root") -> None:
    """Register the album_root function for use in SQLite queries."""

    conn.create_function(str(func_name), 1, album_root)


def album_root_polars_expr(
    dir_col: str,
    *,
    out_col: str = "album_root",
):
    """Return a Polars expression that computes album_root from a directory column.

    This function imports Polars lazily so non-Polars scripts can still import
    this module.
    """

    import polars as pl  # local import by design

    clean = (
        pl.col(dir_col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars_end("/")
    )

    leaf = clean.str.extract(r"([^/]+)$", 1)
    # Use the same regex string as the scalar function.
    is_disc = leaf.str.to_lowercase().str.strip_chars().str.contains(DISC_SUBFOLDER_RE_STR)
    parent = clean.str.replace(r"/[^/]+$", "")

    return (
        pl.when(is_disc & (parent != ""))
        .then(parent)
        .otherwise(clean)
        .replace("", None)
        .alias(out_col)
    )
