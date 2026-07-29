"""

Purpose:
    Small Polars typing utilities shared across Tagminder scripts.

Policy:
- `__sqlmodded` in Polars should be `pl.Int16`
- When ingesting `__sqlmodded`, treat NULL as 0

This module is part of Tagminder.

SQLite tables referenced:
    - None

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

import polars as pl


def series_rowid(values) -> pl.Series:
    return pl.Series(name="rowid", values=[int(v or 0) for v in values], dtype=pl.Int64)


def series_sqlmodded(values) -> pl.Series:
    return pl.Series(
        name="__sqlmodded", values=[int(v or 0) for v in values], dtype=pl.Int16
    )


def expr_sqlmodded(expr: pl.Expr) -> pl.Expr:
    return expr.fill_null(0).cast(pl.Int16)


_ALL_ZERO_RE = r"^0+$"


def expr_md5sig_is_invalid(md5sig: pl.Expr) -> pl.Expr:
    md5_trim = md5sig.cast(pl.Utf8).str.strip_chars()
    md5_s = md5_trim.fill_null("")
    md5_nohyphen = md5_s.str.replace_all("-", "", literal=True)
    return (
        md5sig.is_null()
        | (md5_s == "")
        | (md5_s == "0")
        | md5_nohyphen.str.contains(_ALL_ZERO_RE)
    )


def expr_tokens(expr: pl.Expr, *, delimiter: str) -> pl.Expr:
    """Tokenize a Tagminder multi-value text field into a unique list.

    Policy:
    - Split only on Tagminder's configured delimiter.
    - Strip whitespace per token.
    - Drop empty tokens.
    - Preserve first-seen order while uniquing.
    """

    return (
        expr.cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.split(delimiter)
        .list.eval(pl.element().str.strip_chars())
        .list.filter(pl.element().is_not_null() & (pl.element() != ""))
        .list.unique(maintain_order=True)
    )
