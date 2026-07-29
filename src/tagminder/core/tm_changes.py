"""

Purpose:
    Small helpers for computing field-level diffs and emitting changelog entries.

    These utilities are used by Tagminder scripts to ensure a consistent
    “only log what actually changed” pattern.

This module is part of Tagminder.

SQLite tables referenced:
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Callable, Iterable, Mapping
import sqlite3
from typing import Any

from tagminder.core import tm_db

def _default_normalize(value: Any) -> str | None:
    """Normalize values for stable comparisons and TEXT storage.

    - Preserve None and strings
    - Convert other scalars to strings (SQLite TEXT)
    """

    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def diff_fields(
    *,
    alib_path: str,
    old: Mapping[str, Any],
    new: Mapping[str, Any],
    fields: Iterable[str],
    timestamp: str,
    script: str,
    normalize: Callable[[Any], str | None] = _default_normalize,
    compare_normalize: Callable[[Any], str | None] | None = None,
) -> list[tm_db.ChangelogEntry]:
    """Build changelog entries for fields whose values changed.

    Args:
        alib_path: The stable identifier for the alib row (typically `alib.__path`).
        old: Mapping of original values (e.g. a Polars named row dict).
        new: Mapping of new values (e.g. a record dict).
        fields: Field names to compare.
        timestamp: ISO timestamp for changelog.
        script: Script identifier for changelog.
        normalize: Optional normalization function applied to both sides before
            comparison and logging.

    Returns:
        A list of `tm_db.ChangelogEntry` for changed fields, in the same order as
        `fields`.
    """

    entries: list[tm_db.ChangelogEntry] = []

    cmp = compare_normalize or normalize

    for field_name in fields:
        old_cmp = cmp(old.get(field_name))
        new_cmp = cmp(new.get(field_name))
        if old_cmp == new_cmp:
            continue

        old_value = normalize(old.get(field_name))
        new_value = normalize(new.get(field_name))

        entries.append(
            tm_db.ChangelogEntry(
                alib_path=alib_path,
                alib_column=field_name,
                old_value=old_value,
                new_value=new_value,
                timestamp=timestamp,
                script=script,
            )
        )

    return entries


def entries_from_changes(
    *,
    alib_path: str,
    changes: Iterable[tuple[str, Any, Any]],
    timestamp: str,
    script: str,
    normalize: Callable[[Any], str | None] = _default_normalize,
) -> list[tm_db.ChangelogEntry]:
    """Build changelog entries from already-computed changes.

    This helper does not attempt to detect changes; it simply formats the
    provided `(field, old_value, new_value)` tuples into `tm_db.ChangelogEntry`.
    """

    entries: list[tm_db.ChangelogEntry] = []
    for field_name, old_value_raw, new_value_raw in changes:
        entries.append(
            tm_db.ChangelogEntry(
                alib_path=alib_path,
                alib_column=field_name,
                old_value=normalize(old_value_raw),
                new_value=normalize(new_value_raw),
                timestamp=timestamp,
                script=script,
            )
        )
    return entries


def insert_changes(
    cursor: sqlite3.Cursor,
    *,
    alib_path: str,
    changes: Iterable[tuple[str, Any, Any]],
    timestamp: str,
    script: str,
    normalize: Callable[[Any], str | None] = _default_normalize,
) -> None:
    """Insert already-computed changes into `changelog`.

    This is a convenience wrapper around `entries_from_changes` +
    `tm_db.insert_changelog_entries`.
    """

    tm_db.insert_changelog_entries(
        cursor,
        entries_from_changes(
            alib_path=alib_path,
            changes=changes,
            timestamp=timestamp,
            script=script,
            normalize=normalize,
        ),
    )


@dataclass
class ChangelogBatch:
    """Accumulate changelog entries and insert them in one call.

    Use this inside a transaction to avoid per-row executemany overhead.
    """

    timestamp: str
    script: str
    normalize: Callable[[Any], str | None] = _default_normalize
    entries: list[tm_db.ChangelogEntry] = field(default_factory=list)

    def add(self, *, alib_path: str, changes: Iterable[tuple[str, Any, Any]]) -> None:
        self.entries.extend(
            entries_from_changes(
                alib_path=alib_path,
                changes=changes,
                timestamp=self.timestamp,
                script=self.script,
                normalize=self.normalize,
            )
        )

    def flush(self, cursor: sqlite3.Cursor) -> None:
        tm_db.insert_changelog_entries(cursor, self.entries)
        self.entries.clear()


def master_data_changelog_entries_from_changes(
    *,
    table_name: str,
    rowid: int,
    changes: Iterable[tuple[str, Any, Any]],
    timestamp: str,
    script: str,
    normalize: Callable[[Any], str | None] = _default_normalize,
) -> list[tm_db.MasterDataChangelogEntry]:
    """Build master_data_changelog entries from already-computed changes.

    This helper formats the provided `(column, old_value, new_value)` tuples 
    into `tm_db.MasterDataChangelogEntry`.
    """

    entries: list[tm_db.MasterDataChangelogEntry] = []
    for column_name, old_value_raw, new_value_raw in changes:
        entries.append(
            tm_db.MasterDataChangelogEntry(
                table_name=table_name,
                rowid=rowid,
                column_name=column_name,
                old_value=normalize(old_value_raw),
                new_value=normalize(new_value_raw),
                timestamp=timestamp,
                script=script,
            )
        )
    return entries


def insert_master_data_changes(
    cursor: sqlite3.Cursor,
    *,
    table_name: str,
    rowid: int,
    changes: Iterable[tuple[str, Any, Any]],
    timestamp: str,
    script: str,
    normalize: Callable[[Any], str | None] = _default_normalize,
) -> None:
    """Insert already-computed changes into `master_data_changelog`.

    This is a convenience wrapper around master_data_changelog_entries_from_changes +
    tm_db.insert_master_data_changelog_entries.
    """

    tm_db.insert_master_data_changelog_entries(
        cursor,
        master_data_changelog_entries_from_changes(
            table_name=table_name,
            rowid=rowid,
            changes=changes,
            timestamp=timestamp,
            script=script,
            normalize=normalize,
        ),
    )


@dataclass
class MasterDataChangelogBatch:
    """Accumulate master_data_changelog entries and insert them in one call.

    Use this inside a transaction to avoid per-row executemany overhead.
    """

    timestamp: str
    script: str
    normalize: Callable[[Any], str | None] = _default_normalize
    entries: list[tm_db.MasterDataChangelogEntry] = field(default_factory=list)

    def add(
        self,
        *,
        table_name: str,
        rowid: int,
        changes: Iterable[tuple[str, Any, Any]],
    ) -> None:
        self.entries.extend(
            master_data_changelog_entries_from_changes(
                table_name=table_name,
                rowid=rowid,
                changes=changes,
                timestamp=self.timestamp,
                script=self.script,
                normalize=self.normalize,
            )
        )

    def flush(self, cursor: sqlite3.Cursor) -> None:
        tm_db.insert_master_data_changelog_entries(cursor, self.entries)
        self.entries.clear()
