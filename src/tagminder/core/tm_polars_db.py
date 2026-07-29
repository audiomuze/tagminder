"""SQLite → Polars read helpers.

Purpose:
    Centralize the common pattern of executing a SQLite query and converting
    the result into a Polars DataFrame, so scripts share consistent type
    handling and avoid duplicated boilerplate.

Policy:
    By default, most columns are returned as `pl.Utf8` (stringified), with:
        - `rowid` → `pl.Int64`
        - `__sqlmodded` → `pl.Int16` (NULL treated as 0)

    Scripts can pass `dtype_overrides` for columns that must remain numeric.

This module is part of Tagminder.

SQLite tables referenced:
    - (varies by caller query)

Author: audiomuze
Last updated: 2026-04-15
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

from tagminder.core import tm_db

def _to_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        try:
            return int(s)
        except ValueError:
            return 0
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return 0


def sqlite_to_polars(
    conn: sqlite3.Connection,
    query: str,
    *,
    params: Sequence[object] | None = None,
    dtype_overrides: Mapping[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Execute `query` and return results as a Polars DataFrame.

    Notes:
        - This is intentionally conservative: most values become strings.
        - Use `dtype_overrides` for numeric columns needed for later math.
    """

    dtype_overrides = dict(dtype_overrides or {})

    cursor = conn.cursor()
    if params is None:
        cursor.execute(query)
    else:
        cursor.execute(query, list(params))
    column_names = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    if not rows:
        # Preserve column names; default to Utf8 unless overridden.
        out: dict[str, pl.Series] = {}
        for name in column_names:
            dtype = dtype_overrides.get(name, pl.Utf8)
            out[name] = pl.Series(name=name, values=[], dtype=dtype)
        return pl.DataFrame(out)

    data: dict[str, Any] = {}
    for i, col_name in enumerate(column_names):
        col_data = [row[i] for row in rows]

        dtype = dtype_overrides.get(col_name)

        if dtype is None and col_name == "rowid":
            data[col_name] = pl.Series(
                name="rowid",
                values=[_to_int(x) for x in col_data],
                dtype=pl.Int64,
            )
            continue

        if dtype is None and col_name == "__sqlmodded":
            data[col_name] = pl.Series(
                name="__sqlmodded",
                values=[_to_int(x) for x in col_data],
                dtype=pl.Int16,
            )
            continue

        if dtype == pl.Int64:
            data[col_name] = pl.Series(
                name=col_name, values=[_to_int(x) for x in col_data], dtype=pl.Int64
            )
        elif dtype == pl.Int16:
            data[col_name] = pl.Series(
                name=col_name, values=[_to_int(x) for x in col_data], dtype=pl.Int16
            )
        elif dtype == pl.Float64:
            data[col_name] = pl.Series(
                name=col_name,
                values=[float(x) if x is not None else None for x in col_data],
                dtype=pl.Float64,
            )
        else:
            # Default: stringify everything else to keep downstream text ops stable.
            data[col_name] = pl.Series(
                name=col_name,
                values=[str(x) if x is not None else None for x in col_data],
                dtype=pl.Utf8,
            )

    return pl.DataFrame(data)


def read_table_columns(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    include_sqlmodded: bool = True,
) -> pl.DataFrame:
    """Read `rowid` (+ optional `__sqlmodded`) and the requested `columns` from `table`."""

    select_cols: list[str] = ["rowid"]
    if include_sqlmodded:
        select_cols.append("COALESCE(__sqlmodded, 0) AS __sqlmodded")

    select_cols.extend(tm_db.quote_ident(c) for c in columns)

    query = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {tm_db.quote_ident(table)}"
    )

    return sqlite_to_polars(conn, query)
