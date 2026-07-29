#!/usr/bin/env python3
"""tags2db: Import/Export tags <-> SQLite.

Purpose:
    This script is part of Tagminder.
    Import/Export audio file metadata tags to/from a SQLite database table `alib`.

Actions (subcommands):
    - import: scan folder trees, read tags from files, write/update rows in `alib`.
      Import modes (mutually exclusive):
        - Full import (default)
        - --new-files (only files not in database)
        - --modified-files (only files changed since last import)
        - --prunedb (only remove orphaned database entries)

    - export: write tags back to files from the database, restricted to rows whose
      `__path` is under a provided music directory.
        - Only non-`__*` columns are exported.
        - Empty/blank values remove the tag from the file.
        - Multi-value fields stored with the two-backslash delimiter (`\\`) are
          split back into lists.
        - `--touch-mtime` controls file mtime after writing tags:
            - preserve (default): restore `__file_mod_datetime_raw` when present
            - plus1: restore + 1 second (helps trigger some library rescans)
            - none: leave filesystem mtime as written by tag saving

    - housekeeping: database maintenance operations (e.g. `--dropnulls`).

Concurrency:
    For imports on single or multi-drive setups, the importer can assign dedicated
    worker pools per physical drive to maximize concurrent I/O and reduce disk
    contention.

SQLite tables referenced:
    - alib
    - sqlite_master (introspection)
    - pragma_table_info (introspection)

Author: audiomuze
Last updated: 2026-04-13
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import itertools
import logging
import multiprocessing
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import shutil
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from tagminder.core import tm_db
from tagminder.core import tm_config

class _RawDefaultsHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    """Argparse formatter that preserves newlines and shows defaults."""

from os import scandir

pl = None
audioinf = None


def _require_deps(*, need_polars: bool, need_audioinf: bool) -> None:
    """Import heavy optional dependencies only when needed.

    This is intentionally called AFTER CLI parsing so `-h/--help` works even if
    Polars/audioinf aren't installed.
    """

    global pl, audioinf

    if need_audioinf and audioinf is None:
        try:
            from tagminder.vendor import audioinf as _audioinf
        except ImportError:
            print(
                "Error: audioinf module not found. Please ensure audioinf is installed correctly.",
                file=sys.stderr,
            )
            sys.exit(1)
        audioinf = _audioinf

    if need_polars and pl is None:
        try:
            import polars as _pl
        except ImportError:
            print(
                "Error: polars module not found. Please install it with: pip install polars",
                file=sys.stderr,
            )
            sys.exit(1)
        pl = _pl

# --- Constants ---
AUDIO_EXTENSIONS = {".flac", ".wv", ".m4a", ".aiff", ".ape", ".mp3", ".ogg"}

# Multi-value tag encoding
#
# We deliberately store multi-value tags in SQLite as a single TEXT field delimited
# by TWO backslashes so the values stay easy to read/edit in table editors.
#
# Example:  Artist = "A\\B\\C"  (three values)
#
# Notes:
# - In Python strings, two literal backslashes are written as r"\\" or "\\\\".
# - In regex contexts, matching two literal backslashes requires r"\\\\".
MULTIVALUE_DELIM = r"\\"  # two literal backslashes
MULTIVALUE_DELIM_REGEX = r"\\\\"  # regex that matches two literal backslashes
TABLE_NAME = "alib"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# --- Canonical schema columns ---
# Source of truth: tagminder.toml [columns].schema_columns
_SCHEMA_TOML_FILENAME = "tagminder.toml"
_SCHEMA_COLUMNS_CACHE: list[str] | None = None


def _resolve_schema_toml_path() -> Path:
    """Resolve the canonical schema config path.

    Search order:
    1) current working directory
    2) script parent chain (repo-root friendly)
    """

    cwd_candidate = (Path.cwd() / _SCHEMA_TOML_FILENAME).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        candidate = parent / _SCHEMA_TOML_FILENAME
        if candidate.exists():
            return candidate

    raise RuntimeError(
        "Missing tagminder.toml. This script expects [columns].schema_columns to define the canonical schema. "
        f"Looked in cwd ({cwd_candidate}) and parents of {script_path}."
    )


def _get_schema_columns() -> list[str]:
    """Return the canonical ALIB column order.

    The schema is loaded from tagminder.toml so all scripts can share one source
    of truth.
    """

    global _SCHEMA_COLUMNS_CACHE
    if _SCHEMA_COLUMNS_CACHE is not None:
        return _SCHEMA_COLUMNS_CACHE

    toml_path = _resolve_schema_toml_path()

    data = tm_config.load_config(config_path=toml_path)

    cols = data.get("columns", {}).get("schema_columns")
    if not isinstance(cols, list) or not all(isinstance(c, str) for c in cols):
        raise RuntimeError(
            "Invalid tagminder.toml: expected [columns].schema_columns to be a list of strings"
        )

    seen: set[str] = set()
    dupes: set[str] = set()
    for c in cols:
        if c in seen:
            dupes.add(c)
        else:
            seen.add(c)

    if dupes:
        raise RuntimeError(
            "Invalid tagminder.toml: duplicate entries in [columns].schema_columns: "
            + ", ".join(sorted(dupes))
        )

    if cols.count("__path") != 1:
        raise RuntimeError(
            "Invalid tagminder.toml: [columns].schema_columns must contain '__path' exactly once"
        )
    if "__sqlmodded" not in seen:
        raise RuntimeError(
            "Invalid tagminder.toml: [columns].schema_columns must include '__sqlmodded'"
        )

    _SCHEMA_COLUMNS_CACHE = cols
    return cols


def _get_album_info_polars_schema() -> Dict[str, Any]:
    """Return a Polars schema for the canonical schema columns.

    Requires Polars. This is built lazily so CLI help works without Polars.
    """

    _require_deps(need_polars=True, need_audioinf=False)
    assert pl is not None
    schema = {col: pl.Utf8 for col in _get_schema_columns()}
    schema["__sqlmodded"] = pl.Int16
    return schema


# --- Helper Functions ---
def sanitize_value(value: Any) -> str:
    """
    Converts a tag value to a sanitized string. Handles lists by joining them.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(map(str, value))
    return str(value)


def tag_to_dict_raw(tag: Any) -> Dict[str, Any]:
    """
    Convert a puddlestuff Tag object to a dictionary,
    matching the behavior of the old script by only using dict(tag)
    and avoiding duplicate technical properties.

    Args:
        tag: The Tag object to convert

    Returns:
        Dictionary of tag names (lowercase) to values
    """
    # Get all tags from the Tag object
    tag_dict = dict(tag)

    # Always include the file path
    tag_dict["__path"] = tag.filepath

    # Clean and normalize the keys
    cleaned_dict = {}
    for k, v in tag_dict.items():
        # Remove quotes and convert to lowercase
        safe_k = k.replace('"', "").lower() if isinstance(k, str) else str(k).lower()

        # Convert list values to delimited strings
        if isinstance(v, list):
            cleaned_dict[safe_k] = MULTIVALUE_DELIM.join(map(str, v))
        else:
            cleaned_dict[safe_k] = v

    return cleaned_dict


def normalize_tag_dict_for_storage(tag_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a per-file tag dict for Parquet + SQLite.

    - Keeps keys unchanged
    - Ensures all non-internal values are strings or None (to match TEXT storage)
    - Ensures __sqlmodded is an int with default 0 (NULL treated as 0 in-memory)
    """
    normalized: Dict[str, Any] = {}
    for key, value in tag_dict.items():
        if key == "__sqlmodded":
            if value is None:
                normalized[key] = 0
            else:
                try:
                    normalized[key] = int(value)
                except Exception:
                    normalized[key] = 0
            continue

        if value is None:
            normalized[key] = None
        elif isinstance(value, list):
            normalized[key] = MULTIVALUE_DELIM.join(map(str, value))
        else:
            normalized[key] = str(value)

    if normalized.get("__sqlmodded") is None:
        normalized["__sqlmodded"] = 0
    return normalized


class ColumnOrderTracker:
    """Tracks column order: base schema order first, then first-seen new keys."""

    def __init__(self, base_order: List[str]):
        self._lock = threading.Lock()
        self.order: List[str] = list(base_order)
        self._seen: Set[str] = set(base_order)

    def update_from_dicts(self, dicts: List[Dict[str, Any]]) -> None:
        if not dicts:
            return
        with self._lock:
            for d in dicts:
                for key in d.keys():
                    if key not in self._seen:
                        self._seen.add(key)
                        self.order.append(key)


def _write_parquet_part_atomic(
    spool_dir: str, part_id: int, rows: List[Dict[str, Any]]
) -> str:
    """Write one Parquet part file atomically (tmp write then rename)."""
    final_path = os.path.join(spool_dir, f"part-{part_id:06d}.parquet")
    tmp_path = final_path + ".tmp"

    # CRITICAL: Polars' dict->DataFrame schema inference samples only the first
    # N rows by default. If a tag key appears later in the chunk, that column can
    # be silently dropped from the DataFrame (and thus from the Parquet file),
    # causing NULLs in SQLite on replay.
    #
    # Force inference over the full chunk so all keys observed anywhere in `rows`
    # become real columns in this part file.
    df = pl.DataFrame(rows, infer_schema_length=None)
    try:
        df.write_parquet(tmp_path, compression="zstd")
    except Exception:
        # Fallback for environments where zstd compression isn't available.
        df.write_parquet(tmp_path)
    try:
        os.replace(tmp_path, final_path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    return final_path


def _insert_parquet_spool_into_sqlite(
    dbpath: str, spool_dir: str, column_order: List[str]
) -> None:
    parquet_files = sorted(glob.glob(os.path.join(spool_dir, "part-*.parquet")))
    if not parquet_files:
        logging.info("No Parquet parts found to insert; skipping DB write.")
        return

    if "__path" not in column_order:
        raise ValueError("Missing required primary key column __path in column_order")

    quoted_columns = [f'"{col}"' for col in column_order]
    placeholders = ", ".join(["?"] * len(column_order))
    columns_str = ", ".join(quoted_columns)
    insert_sql = (
        f'INSERT OR REPLACE INTO "{TABLE_NAME}" ({columns_str}) VALUES ({placeholders})'
    )

    logging.info(
        f"Inserting {len(parquet_files)} Parquet parts into SQLite (columns={len(column_order)})..."
    )

    rows_inserted = 0
    parts_since_commit = 0
    commit_every_parts = 10

    with tm_db.connect(dbpath) as conn:
        create_and_migrate_db(dbpath, conn, column_order)
        cursor = conn.cursor()

        cursor.execute("BEGIN")
        for idx, parquet_path in enumerate(parquet_files, 1):
            df = pl.read_parquet(parquet_path)

            # Align columns and enforce storage types.
            select_exprs = []
            for col in column_order:
                if col == "__sqlmodded":
                    if col in df.columns:
                        sqlmodded_i16 = pl.col(col).fill_null(0).cast(pl.Int16)
                        select_exprs.append(
                            pl.when(sqlmodded_i16 == 0)
                            .then(pl.lit(None, dtype=pl.Int16))
                            .otherwise(sqlmodded_i16)
                            .alias(col)
                        )
                    else:
                        # Missing implies 0 in-memory; store NULL on disk for 0.
                        select_exprs.append(pl.lit(None, dtype=pl.Int16).alias(col))
                else:
                    if col in df.columns:
                        select_exprs.append(pl.col(col).cast(pl.Utf8).alias(col))
                    else:
                        select_exprs.append(pl.lit(None, dtype=pl.Utf8).alias(col))

            df_aligned = df.select(select_exprs)

            cursor.executemany(insert_sql, df_aligned.iter_rows())
            rows_inserted += len(df_aligned)
            parts_since_commit += 1

            if parts_since_commit >= commit_every_parts:
                conn.commit()
                cursor.execute("BEGIN")
                parts_since_commit = 0
                logging.info(
                    f"Inserted {rows_inserted} rows ({idx}/{len(parquet_files)} parts)"
                )

        conn.commit()
        logging.info(f"SQLite insert complete: inserted/updated {rows_inserted} rows")


def scantree(path: str) -> Iterator[str]:
    """
    Recursively yields file paths matching AUDIO_EXTENSIONS from a directory tree.
    Uses os.scandir for efficient directory listing.
    """
    try:
        for entry in scandir(path):
            if entry.is_dir(follow_symlinks=False):
                try:
                    yield from scantree(entry.path)
                except PermissionError:
                    logging.warning(
                        f"Permission denied accessing directory: {entry.path}"
                    )
                except OSError as e:
                    logging.warning(f"OS error accessing directory {entry.path}: {e}")
            elif entry.is_file(follow_symlinks=False):
                if os.path.splitext(entry.name)[1].lower() in AUDIO_EXTENSIONS:
                    yield entry.path
    except PermissionError:
        logging.warning(f"Permission denied scanning directory: {path}")
    except OSError as e:
        logging.warning(f"OS error scanning directory {path}: {e}")


def scan_single(path: str) -> Tuple[str, List[str]]:
    """
    Scans a single directory path and returns the path itself and a list of found audio files.
    This function is designed to be run in a separate thread.
    """
    try:
        logging.info(f"Scanning {path}...")
        files = list(scantree(path))
        logging.info(f"Finished scanning {path}. Found {len(files)} files.")
        return path, files
    except PermissionError:
        logging.error(f"Permission denied scanning directory: {path}")
        return path, []
    except OSError as e:
        logging.error(f"OS error scanning directory {path}: {e}")
        return path, []


def parallel_scantree(dirpaths: List[str], workers: int) -> Dict[str, List[str]]:
    """
    Scans multiple directory paths in parallel using a ThreadPoolExecutor.
    Returns a dictionary mapping each directory path to its list of audio files.
    """
    drive_files: Dict[str, List[str]] = {}
    # Using ThreadPoolExecutor for I/O-bound scanning is efficient as threads wait for disk.
    with ThreadPoolExecutor(
        max_workers=workers
    ) as executor:  # Corrected: use the 'workers' argument
        futures = {executor.submit(scan_single, path): path for path in dirpaths}
        for future in concurrent.futures.as_completed(futures):
            drive_path, files = future.result()
            drive_files[drive_path] = files
    return drive_files


def process_chunk_Optimised(
    filepaths: List[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Processes a chunk of filepaths, parses their tags using audioinf.Tag,
    and returns the list of parsed tags and statistics (processed/failed files) for the chunk.
    This function is designed to be run in a separate process.
    """
    # Python 3.14 may use the 'forkserver' multiprocessing start method on Linux,
    # meaning worker processes do not inherit imported modules / globals from the
    # parent process. Ensure audioinf is available in the worker.
    _require_deps(need_polars=False, need_audioinf=True)
    assert audioinf is not None

    tags_in_chunk = []
    chunk_stats = {"processed": 0, "failed": 0}
    for filepath in filepaths:
        try:
            # Check if file is readable before attempting to process
            if not os.access(filepath, os.R_OK):
                logging.warning(f"No read permission for file: {filepath}")
                chunk_stats["failed"] += 1
                continue

            info = audioinf.Tag(filepath)  # Reverted: Use audioinf.Tag
            parsed_tags = tag_to_dict_raw(info)
            tags_in_chunk.append(parsed_tags)
            chunk_stats["processed"] += 1
        except PermissionError:
            logging.warning(f"Permission denied reading file: {filepath}")
            chunk_stats["failed"] += 1
        except Exception as e:
            logging.warning(f"Failed to parse tags for {filepath}: {e}")
            chunk_stats["failed"] += 1
    return tags_in_chunk, chunk_stats


def clean_and_normalize_tags_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """Apply vectorized data cleaning and normalization to tag DataFrame.

    This replaces the individual tag cleaning that was done during dictionary creation.

    Args:
        df: DataFrame with raw tag values

    Returns:
        DataFrame with cleaned and normalized values
    """
    expressions = []

    for col in df.columns:
        col_dtype = df[col].dtype

        if col_dtype == pl.List:
            # Handle list columns - convert to string with double backslash delimiter
            cleaned_expr = (
                pl.when(pl.col(col).is_null())
                .then(None)
                .when(pl.col(col).list.len() == 0)
                .then(None)
                .otherwise(
                    pl.col(col)
                    .list.eval(pl.element().cast(pl.Utf8))
                    .list.join(MULTIVALUE_DELIM)  # Use double backslash as a deliberate separator
                )
                .alias(col)
            )
        else:
            # Handle non-list columns - ensure they're strings and clean
            cleaned_expr = (
                pl.when(pl.col(col).is_null())
                .then(None)
                .otherwise(pl.col(col).cast(pl.Utf8))
                .alias(col)
            )

            # Additional cleaning for string columns to remove empty strings
            if col_dtype in [pl.Utf8, pl.String]:
                cleaned_expr = (
                    pl.when(pl.col(col).is_null())
                    .then(None)
                    .when(pl.col(col).cast(pl.Utf8).str.strip_chars() == "")
                    .then(None)
                    .otherwise(pl.col(col).cast(pl.Utf8))
                    .alias(col)
                )

        expressions.append(cleaned_expr)

    return df.with_columns(expressions)


def build_dataframe_with_schema(all_tags: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Builds a Polars DataFrame from a list of tag dictionaries, enforcing a dynamic schema
    where all tag fields are treated as Utf8 to prevent type conflicts during creation.
    """
    if not all_tags:
        # Return an empty DataFrame with the correct schema if no tags are processed
        schema = _get_album_info_polars_schema()
        return pl.DataFrame({}, schema=schema)

    # 1. Discover all unique keys from all tags to prevent dropping unknown tags.
    #    Also, ensure all keys from the static schema are included for consistency.
    all_keys = set(_get_schema_columns())
    for tag_dict in all_tags:
        all_keys.update(tag_dict.keys())

    # 2. Per the request, create an ingestion schema where all columns are treated as pl.Utf8
    #    to prevent type errors on creation. The original schema's `Int64` for `__sqlmodded`
    #    is respected as it's an internal field, not a "tag".
    ingestion_schema = {key: pl.Utf8 for key in all_keys}
    if "__sqlmodded" in ingestion_schema:
        ingestion_schema["__sqlmodded"] = pl.Int16

    # 3. Pre-process the raw tag data. The primary goal is to convert list values
    #    into strings BEFORE they are passed to the DataFrame constructor. This avoids
    #    the `ComputeError: could not append value: [...] of type: list[str]`.
    pre_processed_tags = []
    for tag_dict in all_tags:
        processed_dict = {}
        # Iterate over all possible keys to ensure dictionaries have a consistent structure
        for key in all_keys:
            if key in tag_dict:
                value = tag_dict[key]
                if isinstance(value, list):
                    # Convert list to a delimited string.
                    processed_dict[key] = MULTIVALUE_DELIM.join(map(str, value))
                else:
                    processed_dict[key] = value
            else:
                processed_dict[key] = None

        # Ensure '__sqlmodded' has a default value if missing/None.
        if processed_dict.get("__sqlmodded") is None:
            processed_dict["__sqlmodded"] = 0

        pre_processed_tags.append(processed_dict)

    # 4. Create the DataFrame using the pre-processed data and the dynamically generated
    #    schema. This single step now correctly handles all known and unknown tags without type errors.
    df = pl.DataFrame(pre_processed_tags, schema=ingestion_schema)

    # 5. The original call to `clean_and_normalize_tags_vectorized` is no longer necessary
    #    as its work (list conversion, ensuring columns) is now done *before* DataFrame creation.
    return df


def create_and_migrate_db(
    dbpath: str, conn: sqlite3.Connection, df_columns: Optional[List[str]] = None
) -> None:
    cursor = conn.cursor()

    # 1. Get current columns (primary key first, then others in existing order)
    cursor.execute(f'SELECT name FROM pragma_table_info("{TABLE_NAME}") ORDER BY cid')
    existing_columns = [row[0] for row in cursor.fetchall()]

    # 2. Define required columns (schema columns first, then new ones)
    schema_columns = _get_schema_columns()
    required_columns = list(schema_columns)  # Preserves order
    if df_columns:
        required_columns += [
            col for col in df_columns if col not in schema_columns
        ]  # New tags appended

    # 3. Create table if missing (with perfect schema order)
    if not existing_columns:
        columns_sql = [
            f'"{col}" TEXT{" PRIMARY KEY" if col == "__path" else ""}'
            if col != "__sqlmodded"
            else f'"{col}" INTEGER'
            for col in required_columns
        ]

        cursor.execute(f'''
            CREATE TABLE "{TABLE_NAME}" (
                {", ".join(columns_sql)}
            )
        ''')
        conn.commit()
        return

    # 4. Just add missing columns (all as TEXT since new tags are pl.Utf8)
    missing_columns = [col for col in required_columns if col not in existing_columns]

    for col in missing_columns:
        try:
            cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN "{col}" TEXT')
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e):  # Ignore harmless duplicates
                raise

    conn.commit()


def _regenerate_audit_trigger(dbpath: str) -> None:
    """Regenerate the SQL audit trigger based on current alib schema.

    This trigger captures manual UPDATE statements to alib and writes per-column
    changes to the changelog table. It's called after import to ensure the trigger
    accommodates any new columns discovered during tag ingestion.

    The trigger:
    - Fires AFTER UPDATE on alib
    - For each user-facing column (excludes all __ prefix columns), generates an INSERT
      to changelog when the value changes
    - Captures old_value, new_value, timestamp, and marks source as 'TRIGGER_AUDIT'
    """
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    try:
        # Ensure changelog table exists before creating trigger
        tm_db.ensure_changelog_table(conn)

        # Get all columns from alib
        cursor.execute(f'PRAGMA table_info("{TABLE_NAME}")')
        all_columns = [row[1] for row in cursor.fetchall()]

        # Exclude all system columns (__ prefix) and primary key
        tracked_columns = [
            col
            for col in all_columns
            if not col.startswith("__")
        ]

        if not tracked_columns:
            logging.debug("No trackable columns in alib; audit trigger skipped")
            return

        # Generate INSERT statements for each column comparison
        insert_statements = []
        for col in tracked_columns:
            col_quoted = f'"{col}"'
            insert_statements.append(
                f"""    INSERT INTO changelog (alib_path, alib_column, old_value, new_value, timestamp, script)
    SELECT NEW.__path, '{col}', CAST(OLD.{col_quoted} AS TEXT), CAST(NEW.{col_quoted} AS TEXT), datetime('now'), 'TRIGGER_AUDIT'
    WHERE NEW.{col_quoted} != OLD.{col_quoted} OR (NEW.{col_quoted} IS NULL AND OLD.{col_quoted} IS NOT NULL) OR (NEW.{col_quoted} IS NOT NULL AND OLD.{col_quoted} IS NULL);"""
            )

        # Build complete trigger SQL
        trigger_sql = f"""DROP TRIGGER IF EXISTS capture_manual_alib_changes;
CREATE TRIGGER capture_manual_alib_changes
AFTER UPDATE ON "{TABLE_NAME}"
FOR EACH ROW
BEGIN
{chr(10).join(insert_statements)}
END;"""

        cursor.executescript(trigger_sql)
        conn.commit()
        logging.info(
            f"Audit trigger regenerated with {len(tracked_columns)} tracked columns"
        )

    except Exception as e:
        logging.error(f"Failed to regenerate audit trigger: {e}")
        raise
    finally:
        conn.close()


def housekeeping_dropnulls(dbpath: str) -> None:
    """Drop all-null non-schema columns by rebuilding the `alib` table.

    Keeps:
    - All columns listed in tagminder.toml `[columns].schema_columns` (even if all NULL)
    - Any other column that has at least one non-NULL value

    Column order in the rebuilt table follows the current on-disk order
    (PRAGMA table_info cid order) at the time housekeeping is run.

    After rebuilding, this function runs `VACUUM` to reclaim free pages.
    """

    def _quote_ident(name: str) -> str:
        # SQLite identifier quoting uses double-quotes; escape embedded quotes by doubling.
        return '"' + name.replace('"', '""') + '"'

    def _chunks(items: List[str], size: int) -> Iterator[List[str]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    schema_columns = _get_schema_columns()
    schema_cols = set(schema_columns)
    table = TABLE_NAME
    tmp_table = f"{TABLE_NAME}__dropnulls_tmp"
    old_table = f"{TABLE_NAME}__dropnulls_old"

    with tm_db.connect(dbpath) as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if cur.fetchone() is None:
            raise RuntimeError(f"Table {table!r} does not exist in {dbpath!r}")

        cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
        cols_info = cur.fetchall()
        if not cols_info:
            raise RuntimeError(f"Table {table!r} has no columns")

        # PRAGMA table_info returns rows: (cid, name, type, notnull, dflt_value, pk)
        existing_cols_in_order = [row[1] for row in sorted(cols_info, key=lambda r: r[0])]
        existing_col_set = set(existing_cols_in_order)
        if "__path" not in existing_cols_in_order:
            raise RuntimeError("Missing required primary key column '__path'")

        # Map existing column types (fallback to schema-known defaults).
        existing_types: Dict[str, str] = {}
        existing_pk: Set[str] = set()
        for cid, name, col_type, notnull, dflt_value, pk in cols_info:
            if pk:
                existing_pk.add(name)
            if col_type:
                existing_types[name] = str(col_type)

        def _default_sqlite_type(col: str) -> str:
            if col == "__sqlmodded":
                return "INTEGER"
            return "TEXT"

        # Determine which non-schema columns have any non-NULL values.
        non_schema_cols = [c for c in existing_cols_in_order if c not in schema_cols]
        non_null_cols: Set[str] = set()
        if non_schema_cols:
            # Chunk the aggregate queries to limit SQL statement size.
            # Each query scans the table once for its chunk.
            chunk_size = 50
            for chunk in _chunks(non_schema_cols, chunk_size):
                exprs = [
                    f"SUM(CASE WHEN {_quote_ident(c)} IS NOT NULL THEN 1 ELSE 0 END)"
                    for c in chunk
                ]
                sql = f"SELECT {', '.join(exprs)} FROM {_quote_ident(table)}"
                row = cur.execute(sql).fetchone()
                if row is None:
                    continue
                for col_name, count_val in zip(chunk, row):
                    if (count_val or 0) > 0:
                        non_null_cols.add(col_name)

        keep_set: Set[str] = (schema_cols & set(existing_cols_in_order)) | non_null_cols

        # Preserve existing column order; include any missing schema columns at the end.
        new_cols_in_order = [c for c in existing_cols_in_order if c in keep_set]
        missing_schema_cols = [c for c in schema_columns if c not in new_cols_in_order]
        if missing_schema_cols:
            new_cols_in_order.extend(missing_schema_cols)
            keep_set.update(missing_schema_cols)

        if "__path" not in new_cols_in_order:
            raise RuntimeError("Primary key '__path' must be retained")

        # Clean up any leftovers from a previously interrupted run.
        cur.execute(f"DROP TABLE IF EXISTS {_quote_ident(tmp_table)}")
        cur.execute(f"DROP TABLE IF EXISTS {_quote_ident(old_table)}")
        conn.commit()

        col_defs = []
        for col in new_cols_in_order:
            col_type = existing_types.get(col) or _default_sqlite_type(col)
            pk_sql = " PRIMARY KEY" if col == "__path" else ""
            col_defs.append(f"{_quote_ident(col)} {col_type}{pk_sql}")

        create_sql = (
            f"CREATE TABLE {_quote_ident(tmp_table)} (" + ", ".join(col_defs) + ")"
        )

        # Rebuild in a single transaction.
        cur.execute("BEGIN")
        try:
            cur.execute(create_sql)
            cols_csv = ", ".join(_quote_ident(c) for c in new_cols_in_order)
            select_csv = ", ".join(
                _quote_ident(c) if c in existing_col_set else f"NULL AS {_quote_ident(c)}"
                for c in new_cols_in_order
            )
            cur.execute(
                f"INSERT INTO {_quote_ident(tmp_table)} ({cols_csv}) "
                f"SELECT {select_csv} FROM {_quote_ident(table)} ORDER BY {_quote_ident('__path')}"
            )

            # Swap tables.
            cur.execute(f"ALTER TABLE {_quote_ident(table)} RENAME TO {_quote_ident(old_table)}")
            cur.execute(f"ALTER TABLE {_quote_ident(tmp_table)} RENAME TO {_quote_ident(table)}")
            cur.execute(f"DROP TABLE {_quote_ident(old_table)}")
            conn.commit()
        except Exception:
            conn.rollback()
            # Best-effort cleanup.
            try:
                cur.execute(f"DROP TABLE IF EXISTS {_quote_ident(tmp_table)}")
                cur.execute(f"DROP TABLE IF EXISTS {_quote_ident(old_table)}")
                conn.commit()
            except Exception:
                pass
            raise

        # Reclaim free pages. Without VACUUM, dropping columns leaves the DB file
        # at (roughly) its pre-housekeeping size.
        logging.info("Running VACUUM to reclaim database space...")
        vacuum_start = time.perf_counter()
        try:
            conn.execute("VACUUM")
        finally:
            vacuum_elapsed = time.perf_counter() - vacuum_start
            logging.info(f"VACUUM complete in {vacuum_elapsed:.2f}s")

    logging.info(
        f"Housekeeping dropping of null columns complete: kept {len(new_cols_in_order)}/{len(existing_cols_in_order)} columns"
    )


def filter_files_by_mode(
    dbpath: str,
    drive_files: Dict[str, List[str]],
    mode: str,  # "new" or "modified"
) -> Dict[str, List[str]]:
    """
    Filter files based on import mode.

    Args:
        dbpath: Path to SQLite database
        drive_files: Dictionary mapping drive paths to file lists
        mode: Either "new" (files not in DB) or "modified" (files in DB with newer mtime)

    Returns:
        Filtered dictionary with same structure
    """
    # Collect all files from all drives
    all_files = []
    for drive_path, files in drive_files.items():
        all_files.extend(files)

    if not all_files:
        return drive_files

    try:
        with tm_db.connect(dbpath) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if mode == "new":
                # Filter for files NOT in database
                filtered_files = filter_new_files(cursor, all_files)
            else:  # mode == "modified"
                # Filter for files IN database with newer modification time
                filtered_files = filter_modified_files(cursor, all_files)

            # Rebuild the drive_files dictionary with only filtered files
            filtered_drive_files = {}
            for drive_path, files in drive_files.items():
                filtered = [f for f in files if f in filtered_files]
                if filtered:
                    filtered_drive_files[drive_path] = filtered

                if files:  # Log filtering results per drive
                    original_count = len(files)
                    filtered_count = len(filtered)
                    removed_count = original_count - filtered_count
                    logging.info(
                        f"Drive {drive_path}: {filtered_count}/{original_count} files "
                        f"after filtering ({removed_count} removed)"
                    )

            return filtered_drive_files

    except sqlite3.Error as e:
        logging.error(f"Database error during filtering: {e}")
        # Fall back to processing all files
        logging.warning("Continuing with full import due to filtering error")
        return drive_files


def filter_new_files(cursor: sqlite3.Cursor, all_files: List[str]) -> Set[str]:
    """Return set of files that are NOT in the database."""
    # Start with all files as candidates
    candidates = set(all_files)

    # Remove files that exist in database
    batch_size = 500
    for i in range(0, len(all_files), batch_size):
        batch = all_files[i : i + batch_size]
        placeholders = ",".join(["?"] * len(batch))

        cursor.execute(
            f'''
            SELECT "__path" FROM "{TABLE_NAME}"
            WHERE "__path" IN ({placeholders})
        ''',
            batch,
        )

        existing_in_batch = {row[0] for row in cursor.fetchall()}
        candidates -= existing_in_batch

    return candidates


def filter_modified_files(cursor: sqlite3.Cursor, all_files: List[str]) -> Set[str]:
    """Return set of files that are in database AND have been modified since last import."""
    modified_files = set()

    # First, get current modification times for all files
    file_mtimes = {}
    for filepath in all_files:
        try:
            file_mtimes[filepath] = os.path.getmtime(filepath)
        except OSError:
            continue  # Skip files we can't access

    # Query database in batches
    batch_size = 500
    for i in range(0, len(all_files), batch_size):
        batch = all_files[i : i + batch_size]
        placeholders = ",".join(["?"] * len(batch))

        cursor.execute(
            f'''
            SELECT "__path", "__file_mod_datetime_raw"
            FROM "{TABLE_NAME}"
            WHERE "__path" IN ({placeholders})
        ''',
            batch,
        )

        for db_path, db_mtime_str in cursor.fetchall():
            if db_path in file_mtimes:
                current_mtime = file_mtimes[db_path]
                try:
                    # Compare with stored timestamp
                    db_mtime = float(db_mtime_str) if db_mtime_str else 0
                    if current_mtime > db_mtime:
                        modified_files.add(db_path)
                except (ValueError, TypeError):
                    # If database timestamp is invalid, treat as modified
                    modified_files.add(db_path)

    return modified_files


def prune_database_orphans(dbpath: str, existing_files: Set[str]) -> int:
    """
    Remove database entries for files that no longer exist on disk.

    Args:
        dbpath: Path to SQLite database
        existing_files: Set of file paths that currently exist on disk

    Returns:
        Number of orphaned records removed
    """
    try:
        with tm_db.connect(dbpath) as conn:
            cursor = conn.cursor()

            # Get all file paths from database
            cursor.execute(f'SELECT "__path" FROM "{TABLE_NAME}"')
            db_paths = {row[0] for row in cursor.fetchall()}

            # Find orphaned paths (in database but not on disk)
            orphaned_paths = db_paths - existing_files

            if not orphaned_paths:
                logging.info("No orphaned database entries found")
                return 0

            # Delete orphaned records in batches
            orphaned_list = list(orphaned_paths)
            batch_size = 500
            total_deleted = 0

            for i in range(0, len(orphaned_list), batch_size):
                batch = orphaned_list[i : i + batch_size]
                placeholders = ",".join(["?"] * len(batch))

                cursor.execute(
                    f'DELETE FROM "{TABLE_NAME}" WHERE "__path" IN ({placeholders})',
                    batch,
                )
                total_deleted += cursor.rowcount

            conn.commit()

            if total_deleted > 0:
                logging.info(
                    f"Database pruning: removed {total_deleted} orphaned entries"
                )
            else:
                logging.info("No orphaned database entries removed")

            return total_deleted

    except sqlite3.Error as e:
        logging.error(f"Database error during pruning: {e}")
        return 0
    except Exception as e:
        logging.error(f"Unexpected error during database pruning: {e}")
        return 0


def process_single_drive(
    drive_path: str,
    files: List[str],
    chunk_size: int,
    workers_per_drive: int,
    *,
    spool_dir: str,
    column_tracker: ColumnOrderTracker,
    part_id_counter: itertools.count,
    part_id_lock: threading.Lock,
) -> Dict[str, Any]:
    """
    Processes all files for a single drive in parallel using a dedicated ProcessPoolExecutor.
    This function is designed to be run by an outer ThreadPoolExecutor (e.g., drive_manager_executor)
    to manage concurrent processing of multiple drives.

    Args:
        drive_path (str): The path to the current drive/mount point.
        files (List[str]): A list of all audio file paths found on this drive.
        chunk_size (int): The number of files to process per chunk.
        workers_per_drive (int): The maximum number of worker processes to use for this specific drive's pool.

    Returns:
        Dict[str, Any]: Total statistics for this drive (e.g., "processed_files", "failed_files").

    Notes:
        This function spools each completed chunk to a Parquet part file under `spool_dir`
        and does not retain tag dictionaries in memory.
    """
    logging.info(
        f"Starting parallel processing for drive: {drive_path} with {len(files)} files"
    )

    # --- START OF CHANGE ---
    # Sort the files list for the current drive to improve locality of reference
    files.sort()
    # --- END OF CHANGE ---

    drive_total_stats = {"processed_files": 0, "failed_files": 0}

    # Calculate total chunks for progress tracking
    total_chunks = (len(files) + chunk_size - 1) // chunk_size
    completed_chunks = 0

    # Each drive gets its own ProcessPoolExecutor, ensuring dedicated workers
    # that focus their I/O on that specific physical disk.
    with ProcessPoolExecutor(max_workers=workers_per_drive) as executor:
        futures = []
        # Submit chunks of files from this drive to its dedicated pool
        for i in range(0, len(files), chunk_size):
            chunk = files[i : i + chunk_size]
            futures.append(executor.submit(process_chunk_Optimised, chunk))

        # Collect results from this drive's chunks as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                chunk_tags, chunk_stats = future.result()
                # Track columns in the order they materialize (base schema first).
                column_tracker.update_from_dicts(chunk_tags)

                # Normalize for stable TEXT storage and write this chunk to Parquet.
                normalized_rows = [
                    normalize_tag_dict_for_storage(d) for d in chunk_tags
                ]
                if normalized_rows:
                    with part_id_lock:
                        part_id = next(part_id_counter)
                    _write_parquet_part_atomic(spool_dir, part_id, normalized_rows)

                drive_total_stats["processed_files"] += chunk_stats["processed"]
                drive_total_stats["failed_files"] += chunk_stats["failed"]

                # Progress tracking
                completed_chunks += 1
                progress_percent = (completed_chunks / total_chunks) * 100
                logging.info(
                    f"Drive {drive_path}: {completed_chunks}/{total_chunks} chunks completed ({progress_percent:.1f}%)"
                )

            except Exception as e:
                logging.error(f"Error processing chunk for drive {drive_path}: {e}")
                # A rough estimate: if a chunk fails completely, assume all files in it failed
                drive_total_stats["failed_files"] += len(chunk)
                # Still increment completed chunks for progress tracking
                completed_chunks += 1
                progress_percent = (completed_chunks / total_chunks) * 100
                logging.info(
                    f"Drive {drive_path}: {completed_chunks}/{total_chunks} chunks completed ({progress_percent:.1f}%) - chunk failed"
                )

    logging.info(
        f"Finished processing drive: {drive_path}. Processed {drive_total_stats['processed_files']} files, failed {drive_total_stats['failed_files']}."
    )
    return drive_total_stats


def import_dir_optimised(
    dbpath: str,
    dirpaths: List[str],
    workers: Optional[int] = None,  # Named 'workers' to indicate workers PER DRIVE
    chunk_size: int = 4000,
    new_files: bool = False,  # Add new files only that do not already appear in the alib table
    modified_files: bool = False,  # Update database from modified files only (assumes external taggers are not preserving mod-time)
    prunedb: bool = False,  # Remove database entries for files no longer on disk
) -> None:
    """
    Optimised function to import audio metadata tags from multiple directories into a SQLite database.
    This version uses dedicated worker pools for each drive to enhance concurrent disk I/O.

    Args:
        dbpath (str): Path to the SQLite database file.
        dirpaths (List[str]): List of paths to the music directories (mount points).
        workers (Optional[int]): Number of worker processes to dedicate to each drive's processing pool.
                                 If None, it calculates workers as (total CPU cores) // (number of active drives).
        chunk_size (int): Number of files to process per chunk.
        new_files (bool): If True, only import files not already in the database.
        modified_files (bool): If True, only import files that exist in database and have been modified
                              (file modification time > stored __file_mod_datetime_raw).
        prunedb (bool): If True, remove database entries for files no longer found on disk (orphan cleanup).

    Note: --new-files, --modified-files and --prunedb are mutually exclusive. If all are False, all files are processed.
    """
    logging.info("Starting Optimised import process...")
    start_time = time.time()

    # Phase 1: Parallel Scan all directories to identify audio files on each drive.
    # This phase uses a ThreadPoolExecutor as scanning is I/O-bound.
    logging.info("Phase 1: Scanning directories in parallel...")
    # Determine the number of worker threads for scanning. Max out at 16 (common CPU core count) or number of drives.
    scan_threads = min(len(dirpaths), multiprocessing.cpu_count(), 16)
    drive_files = parallel_scantree(dirpaths, scan_threads)  # Pass scan_threads
    logging.info("Phase 1 Complete.")

    if new_files or modified_files:
        logging.info(
            f"Filtering files: {'new-files' if new_files else 'modified-files'} mode"
        )
        drive_files = filter_files_by_mode(
            dbpath=dbpath,
            drive_files=drive_files,
            mode="new" if new_files else "modified",
        )

    # If --prunedb is specified (mutually exclusive with import modes)
    if prunedb:
        logging.info("Prune-only mode: Only pruning database, no tag import...")

        # Collect all files that currently exist on disk
        all_existing_files = set()
        for files in drive_files.values():
            all_existing_files.update(files)

        # Prune database entries for files not found
        removed_count = prune_database_orphans(dbpath, all_existing_files)

        if removed_count > 0:
            logging.info(f"Pruning complete: removed {removed_count} orphaned entries")
        else:
            logging.info("Pruning complete: no orphaned entries found")

        end_time = time.time()
        logging.info(
            f"Prune-only operation finished in {end_time - start_time:.2f} seconds."
        )
        return  # Exit early, no tag processing

    total_files_to_process = sum(len(files) for files in drive_files.values())
    if total_files_to_process == 0:
        logging.info("No audio files found across all specified directories. Exiting.")
        return

    # Determine the number of worker processes to assign PER DRIVE.
    # This calculation aims to distribute available CPU cores efficiently among the active drives.
    num_cpu_cores = multiprocessing.cpu_count()
    active_drives_count = len(drive_files)

    if workers is None:
        # Default strategy: distribute CPU cores as evenly as possible among active drives.
        # Ensure at least 1 worker process per drive.
        workers_per_drive = max(1, num_cpu_cores // active_drives_count)
        logging.info(
            f"Auto-determining workers: {num_cpu_cores} CPU cores / {active_drives_count} drives = {workers_per_drive} workers per drive."
        )
    else:
        # If user explicitly specified workers, use that number for each drive's pool.
        workers_per_drive = max(1, workers)
        logging.info(
            f"Using user-specified {workers_per_drive} worker processes per drive."
        )

    # Phase 2: Process tags for each drive using its own dedicated ProcessPoolExecutor.
    # An outer ThreadPoolExecutor (drive_manager_executor) manages the concurrent launch
    # and monitoring of these per-drive ProcessPoolExecutors.
    logging.info("Phase 2: Processing tags in parallel (dedicated pool per drive)...")
    total_processed_files = 0
    total_failed_files = 0

    spool_dir = tempfile.mkdtemp(prefix="tagminder_parquet_spool_", dir="/tmp")
    logging.info(f"Spooling chunk tag data to Parquet in: {spool_dir}")

    column_tracker = ColumnOrderTracker(list(_get_schema_columns()))
    part_id_counter = itertools.count(0)
    part_id_lock = threading.Lock()

    # `drive_manager_executor` allows simultaneous execution of `process_single_drive` for multiple drives.
    # Its `max_workers` is set to the number of drives, enabling concurrent drive processing.
    with ThreadPoolExecutor(max_workers=active_drives_count) as drive_manager_executor:
        drive_processing_futures = []
        for drive_path, files in drive_files.items():
            if files:  # Only submit processing for drives that actually have files
                drive_processing_futures.append(
                    drive_manager_executor.submit(
                        process_single_drive,
                        drive_path,
                        files,
                        chunk_size,
                        workers_per_drive,
                        spool_dir=spool_dir,
                        column_tracker=column_tracker,
                        part_id_counter=part_id_counter,
                        part_id_lock=part_id_lock,
                    )
                )

        # Collect results from each drive's processing as they complete.
        for future in concurrent.futures.as_completed(drive_processing_futures):
            try:
                drive_stats = future.result()
                total_processed_files += drive_stats["processed_files"]
                total_failed_files += drive_stats["failed_files"]
            except Exception as e:
                logging.error(f"An error occurred in a drive's processing task: {e}")
                # Note: Exact failed file count from inner process might be lost on critical failure here.

    logging.info("Phase 2 Complete.")
    logging.info(
        f"Summary: Total files processed successfully: {total_processed_files}"
    )
    if total_failed_files > 0:
        logging.warning(f"Summary: Total files failed to process: {total_failed_files}")

    parquet_parts = sorted(glob.glob(os.path.join(spool_dir, "part-*.parquet")))
    if not parquet_parts:
        logging.info("No tags successfully processed across all drives. Exiting.")
        try:
            shutil.rmtree(spool_dir)
        except Exception:
            pass
        return

    # Phase 3: Write spooled Parquet parts to SQLite.
    logging.info("Phase 3: Writing Parquet spool to SQLite database...")
    success = False
    try:
        _insert_parquet_spool_into_sqlite(
            dbpath=dbpath,
            spool_dir=spool_dir,
            column_order=column_tracker.order,
        )
        success = True
    except sqlite3.Error as e:
        logging.error(f"SQLite database error during write operation: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during database write: {e}", exc_info=True
        )
        sys.exit(1)
    finally:
        if success:
            try:
                shutil.rmtree(spool_dir)
                logging.info("Parquet spool cleaned up")
            except Exception as e:
                logging.warning(f"Could not remove Parquet spool dir {spool_dir}: {e}")
        else:
            logging.warning(f"Keeping Parquet spool dir for inspection: {spool_dir}")

    logging.info("Phase 3 Complete.")

    end_time = time.time()
    logging.info(f"Import process finished in {end_time - start_time:.2f} seconds.")


def clean_values_vectorized(df: pl.DataFrame) -> pl.DataFrame:
    """Clean all values in DataFrame using vectorized operations.

    Important: this function must NOT rewrite the multi-value delimiter.
    The database encodes multi-value tags as strings delimited by ``\\`` (two
    backslashes) so the table stays human-editable in SQLite table editors.
    """
    string_cols = [col for col in df.columns if df[col].dtype == pl.Utf8]

    # Process each column individually
    for col in string_cols:
        # First handle empty strings by converting to None
        df = df.with_columns(
            pl.when(pl.col(col).is_null() | (pl.col(col).str.strip_chars() == ""))
            .then(None)
            .otherwise(pl.col(col))
            .alias(col)
        )
    return df


def build_path_filter_condition(dirpath: str) -> str:
    """Build a SQLite GLOB pattern for filtering by path prefix.

    Args:
        dirpath: Directory path to filter by

    Returns:
        A GLOB pattern string suitable for `WHERE __path GLOB ?`
    """
    # Normalize path and ensure it ends with separator
    normalized_path = os.path.normpath(dirpath)
    if not normalized_path.endswith(os.path.sep):
        normalized_path += os.path.sep

    # Use GLOB for efficient prefix matching.
    # Note: GLOB is case-sensitive.
    return f"{normalized_path}*"


def process_files(df: pl.DataFrame, preserve_mtime: bool) -> dict:
    stats = {"processed": 0, "errors": 0, "skipped": 0}
    tag_cols = [col for col in df.columns if not col.startswith("__")]

    for row in df.iter_rows(named=True):
        try:
            if not os.path.exists(row["__path"]):
                stats["skipped"] += 1
                continue

            tag = audioinf.Tag(row["__path"])
            for col in tag_cols:
                if (val := row[col]) is not None:
                    tag[col] = (
                        str(val).split(MULTIVALUE_DELIM)
                        if MULTIVALUE_DELIM in str(val)
                        else val
                    )

            tag.save()

            # Legacy helper retained for compatibility; export uses
            # process_files_with_directory_grouping which supports touch-mtime modes.
            if preserve_mtime:
                try:
                    mtime = float(row["__file_mod_datetime_raw"])
                    os.utime(row["__path"], times=(mtime, mtime))
                except Exception as e:
                    logging.warning(
                        f"Could not preserve mtime for {row['__path']}: {str(e)}"
                    )

            stats["processed"] += 1

        except Exception as e:
            stats["errors"] += 1
            logging.error(f"Failed {row['__path']}: {str(e)}")

    return stats


def export_db(dbpath: str, dirpath: str, touch_mtime: str = "preserve") -> None:
    """Export database to audio files using Optimised DataFrame operations with improved path handling."""
    try:
        # Connect to database
        logging.info(f"Reading database from {dbpath}...")
        conn = tm_db.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES)

        # Build path filter pattern (bound parameter to avoid quoting issues)
        path_glob = build_path_filter_condition(dirpath)

        # Query schema to build explicit schema for Polars
        schema_query = f"PRAGMA table_info({TABLE_NAME})"
        schema_df = pl.read_database(query=schema_query, connection=conn)
        table_schema = {col_name: pl.Utf8 for col_name in schema_df["name"]}

        # First, get just the paths to validate file existence
        path_query = f"""
        SELECT __path FROM {TABLE_NAME}
        WHERE __path GLOB ?
        ORDER BY __path
        """

        logging.info("Querying candidate file paths...")
        path_df = pl.read_database(
            query=path_query,
            connection=conn,
            execute_options={"parameters": [path_glob]},
            schema_overrides={"__path": pl.Utf8},
        )

        if path_df.is_empty():
            logging.warning(f"No database records found for directory: {dirpath}")
            conn.close()
            return

        total_candidates = len(path_df)
        logging.info(f"Found {total_candidates} database records matching path filter")

        # Pre-filter for existing files only
        logging.info("Validating file existence for candidate records...")
        candidate_paths = path_df.get_column("__path").to_list()
        existing_paths = []

        for filepath in candidate_paths:
            if os.path.exists(filepath):
                existing_paths.append(filepath)

        existing_count = len(existing_paths)
        skipped_count = total_candidates - existing_count

        if skipped_count > 0:
            logging.info(
                f"Skipped {skipped_count} database entries - files not found on disk"
            )

        if existing_count == 0:
            logging.warning(
                f"No files found on disk for any database records under: {dirpath}"
            )
            conn.close()
            return

        logging.info(f"Will process {existing_count} files that exist on disk")

        # Query records in batches to avoid SQL variable limit
        batch_size = 500  # Well below SQLite's default 999 variable limit
        total_batches = (existing_count + batch_size - 1) // batch_size
        all_dfs = []

        logging.info(f"Querying database in batches of {batch_size}...")
        for i in range(0, existing_count, batch_size):
            batch_paths = existing_paths[i : i + batch_size]
            placeholders = ",".join(["?"] * len(batch_paths))
            final_query = f"""
            SELECT * FROM {TABLE_NAME}
            WHERE __path IN ({placeholders})
            ORDER BY __path
            """

            batch_df = pl.read_database(
                query=final_query,
                connection=conn,
                execute_options={"parameters": batch_paths},
                schema_overrides=table_schema,
            )
            all_dfs.append(batch_df)

            # Log progress every 10 batches or on last batch
            if (i // batch_size) % 10 == 0 or (i + batch_size >= existing_count):
                logging.info(f"Processed batch {i // batch_size + 1}/{total_batches}")

        # Combine all batches into single DataFrame - already sorted by __path
        df = pl.concat(all_dfs)
        conn.close()

        logging.info(
            f"Loaded {len(df)} records for processing (sorted by __path for locality)"
        )

        # Vectorized data cleaning
        logging.info("Applying vectorized data cleaning...")
        df_cleaned = clean_values_vectorized(df)

        # Memory layout optimisation with columnar processing
        logging.info("Processing files with Optimised path handling...")
        stats = process_files_with_directory_grouping(
            df_cleaned,
            batch_size=1000,
            touch_mtime=touch_mtime,
        )

        logging.info(
            f"Export complete. Processed: {stats['processed']}, "
            f"Errors: {stats['errors']}, Skipped: {stats['skipped']}"
        )

    except Exception as e:
        logging.error(f"Error during export: {str(e)}", exc_info=True)
        if "conn" in locals():
            conn.close()
        raise


def process_files_with_directory_grouping(
    df: pl.DataFrame, batch_size: int = 1000, touch_mtime: str = "preserve"
) -> Dict[str, int]:
    """Alternative approach: Group by directory for even better locality.

    This version processes files directory by directory, which can be even more
    efficient for disk I/O patterns.
    """
    stats = {"processed": 0, "errors": 0, "skipped": 0}

    valid_touch_modes = {"preserve", "plus1", "none"}
    if touch_mtime not in valid_touch_modes:
        raise ValueError(
            f"Invalid touch_mtime mode: {touch_mtime!r}. Expected one of: {sorted(valid_touch_modes)}"
        )

    mtime_col = "__file_mod_datetime_raw"
    have_mtime_col = mtime_col in df.columns
    need_db_mtime = touch_mtime in {"preserve", "plus1"}
    if need_db_mtime and not have_mtime_col:
        logging.warning(
            f"touch-mtime={touch_mtime} but column {mtime_col} is missing; mtimes will not be set"
        )

    # Get exportable columns
    exportable_columns = [col for col in df.columns if not col.startswith("__")]
    logging.info(
        f"Will export {len(exportable_columns)} tag fields (excluding __ fields)"
    )

    # Ensure we have __dirpath column
    if "__dirpath" not in df.columns:
        # Extract directory from __path
        df = df.with_columns(
            pl.col("__path")
            .map_elements(lambda x: os.path.dirname(x), return_dtype=pl.Utf8)
            .alias("__dirpath")
        )

    # Use partition_by which returns a proper dict with string keys (not tuples)
    partitioned_list = df.partition_by("__dirpath", maintain_order=True)

    total_directories = len(partitioned_list)
    processed_directories = 0

    logging.info(f"Processing {len(df)} files across {total_directories} directories")

    # Process each directory group
    for dir_df in partitioned_list:
        # Extract the directory path from the first row of this partition
        dirpath = dir_df.select("__dirpath").item(0, 0)

        try:
            # Check if directory is accessible before processing
            if not os.path.exists(dirpath):
                logging.warning(f"Directory no longer exists: {dirpath}")
                stats["skipped"] += len(dir_df)
                continue

            if not os.access(dirpath, os.R_OK | os.W_OK):
                logging.warning(f"Insufficient permissions for directory: {dirpath}")
                stats["skipped"] += len(dir_df)
                continue

            # Process files in this directory
            filepaths = dir_df.get_column("__path").to_list()

            # Pre-extract tag data for this directory
            tag_columns = {}
            for col in exportable_columns:
                tag_columns[col] = dir_df.get_column(col).to_list()

            mtimes = None
            if need_db_mtime and have_mtime_col:
                mtimes = dir_df.get_column(mtime_col).to_list()

            # Process each file in the directory
            for i, filepath in enumerate(filepaths):
                try:
                    if not os.path.exists(filepath):
                        stats["skipped"] += 1
                        logging.warning(f"File not found, skipping: {filepath}")
                        continue

                    # Check file permissions
                    if not os.access(filepath, os.R_OK | os.W_OK):
                        logging.warning(
                            f"Insufficient permissions for file: {filepath}"
                        )
                        stats["errors"] += 1
                        continue

                    # Build tag dictionary
                    tag_values = {
                        col: tag_columns[col][i] for col in exportable_columns
                    }

                    # Process the file using full path
                    tag = audioinf.Tag(filepath)

                    # Update tags
                    for key, value in tag_values.items():
                        if value is None or (
                            isinstance(value, str) and value.strip() == ""
                        ):
                            if key in tag:
                                del tag[key]
                        elif isinstance(value, str) and MULTIVALUE_DELIM in value:
                            tag[key] = value.split(MULTIVALUE_DELIM)
                        # NOTE: Disabled on purpose.
                        #
                        # We store multi-value tags in SQLite as a single TEXT value delimited by
                        # TWO backslashes (MULTIVALUE_DELIM). Splitting on a *single* backslash can
                        # silently corrupt values that legitimately contain backslashes, and it also
                        # makes the storage format ambiguous for manual editing in table editors.
                        #
                        # If you ever need to support a legacy DB that used single-backslash as a
                        # delimiter, re-enable this block (ideally behind an explicit CLI flag).
                        # elif (
                        #     isinstance(value, str)
                        #     and "\\" in value
                        #     and not key.endswith("path")
                        # ):
                        #     tag[key] = value.split("\\")
                        else:
                            tag[key] = value

                    tag.save()

                    # Apply requested mtime behavior after writing tags.
                    if touch_mtime in {"preserve", "plus1"}:
                        if mtimes is not None:
                            try:
                                base_mtime = float(mtimes[i])
                                target_mtime = (
                                    base_mtime + 1.0 if touch_mtime == "plus1" else base_mtime
                                )
                                os.utime(filepath, times=(target_mtime, target_mtime))
                            except Exception as e:
                                logging.warning(
                                    f"Could not set mtime ({touch_mtime}) for {filepath}: {str(e)}"
                                )
                    else:
                        # touch_mtime == "none": leave filesystem mtime unchanged after tag.save().
                        pass

                    stats["processed"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    logging.error(f"Could not update {filepath}: {str(e)}")

            processed_directories += 1

            # Progress logging
            if (
                processed_directories % 100 == 0
                or processed_directories == total_directories
            ):
                logging.info(
                    f"Processed {processed_directories}/{total_directories} directories "
                    f"({stats['processed']} files so far)..."
                )
        except PermissionError:
            logging.error(f"Permission denied accessing directory: {dirpath}")
            stats["skipped"] += len(dir_df)
            continue
        except Exception as e:
            logging.error(f"Error processing directory {dirpath}: {str(e)}")
            continue

    return stats


def setup_logging(level: str) -> None:
    """Set up logging configuration.

    Args:
        level: Logging level
    """
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    try:
        level = getattr(logging, level.upper(), logging.INFO)
    except AttributeError:
        level = logging.INFO
        print(f"Invalid log level: {level}, defaulting to INFO", file=sys.stderr)

    logging.basicConfig(
        level=level, format=log_format, handlers=[logging.StreamHandler()]
    )


def main() -> None:
    """Main entry point with support for parallel processing and multiple directories."""
    parser = argparse.ArgumentParser(
        description="""
        Import/Export audio file tags to/from a SQLite database.

        Subcommands:
        - import: scan directories, read tags, write to the alib table
        - export: write tags back to files from the database
        - housekeeping: database maintenance operations

        Import modes (import subcommand, mutually exclusive):
        - Full import (default)
        - --new-files
        - --modified-files
        - --prunedb
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
          # Full import of all files
          %(prog)s import music.db /music/drive1 /music/drive2

          # Import only new files
          %(prog)s import --new-files music.db /music/new_albums/

          # Import only modified files
          %(prog)s import --modified-files music.db /music/library/

          # Export tags to files
          %(prog)s export music.db /music/library/

                    # Housekeeping: drop all-null non-schema columns (runs VACUUM)
                    %(prog)s housekeeping /path/to/db.sqlite --dropnulls

        For detailed help on specific commands:
            %(prog)s import -h
            %(prog)s export -h
                        %(prog)s housekeeping -h
        """,
    )
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    subparsers.required = True

    # Import subcommand
    import_parser = subparsers.add_parser(
        "import",
        help="Import audio files to database",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""
                Import audio metadata tags from directories into SQLite database.

                Use --new-files to import only files not already in database,
                or
                --modified-files to import only files already in database but
                changed on disk since last import.

                If neither is specified, all files are imported.
                """,
    )
    import_parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to SQLite database (default: tagminder.toml [db].path)",
    )
    import_parser.add_argument(
        "dbpath",
        nargs="?",
        default=None,
        help="Path to SQLite database (optional if tagminder.toml [db].path is set)",
    )

    import_parser.add_argument(
        "musicdirs",
        nargs="*",
        help="Paths to music directories to import (can specify multiple)",
    )

    import_parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes for tag processing PER DRIVE. "
        "If not specified, defaults to CPU count // number of active drives.",
    )

    import_parser.add_argument(
        "--chunk-size",
        type=int,
        default=4000,
        help="Number of files to process per chunk (for tag reading). Default is 4000.",
    )

    import_mode_group = import_parser.add_mutually_exclusive_group()

    import_mode_group.add_argument(
        "--new-files",
        action="store_true",
        help="Only import files not already in database",
    )

    import_mode_group.add_argument(
        "--modified-files",
        action="store_true",
        help="Only import files that have been modified since last import "
        "(compares file modification time with stored __file_mod_datetime_raw)",
    )

    import_mode_group.add_argument(
        "--prunedb",
        action="store_true",
        help="Remove database entries for files no longer found on disk (orphan cleanup)",
    )

    # Export subcommand
    export_parser = subparsers.add_parser(
        "export",
        help="Export database to audio files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="""
        Export metadata tags from database to audio files.
        Only files under the specified music directory are processed.
        """,
    )
    export_parser.add_argument(
        "--touch-mtime",
        choices=["preserve", "plus1", "none"],
        default="preserve",
        help="What to do with file modification time after writing tags. "
        "Default is preserve (restore from __file_mod_datetime_raw).",
    )
    export_parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to SQLite database (default: tagminder.toml [db].path)",
    )
    export_parser.add_argument(
        "dbpath",
        nargs="?",
        default=None,
        help="Path to SQLite database (optional if tagminder.toml [db].path is set)",
    )
    export_parser.add_argument(
        "musicdir",
        nargs="?",
        default=None,
        help="Path to music directory to export to",
    )

    # Housekeeping subcommand
    housekeeping_parser = subparsers.add_parser(
        "housekeeping",
        help="Database housekeeping",
        formatter_class=_RawDefaultsHelpFormatter,
        description=(
            "Housekeeping operations on the SQLite database.\n\n"
            "Use --dropnulls to rebuild the alib table, keeping:\n"
            "- all columns listed in tagminder.toml [columns].schema_columns\n"
            "- any other column that has at least one non-NULL value\n\n"
            "Column order is preserved as it exists at runtime.\n\n"
            "After rebuilding, this command runs VACUUM to reclaim space.\n\n"
            "Notes:\n"
            "- Options may appear before or after the db path."
        ),
        epilog=(
            "Examples:\n"
            "  %(prog)s /tmp/amg/staging.db --dropnulls\n"
            "  %(prog)s --dropnulls /tmp/amg/staging.db\n"
        ),
    )
    housekeeping_parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to SQLite database (default: tagminder.toml [db].path)",
    )
    housekeeping_parser.add_argument(
        "dbpath",
        nargs="?",
        default=None,
        help="Path to SQLite database (optional if tagminder.toml [db].path is set)",
    )
    housekeeping_parser.add_argument(
        "--dropnulls",
        action="store_true",
        help="Rebuild alib table dropping all-null non-schema columns; runs VACUUM",
    )

    # Common arguments
    for p in [import_parser, export_parser, housekeeping_parser]:
        p.add_argument(
            "--log",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            default="INFO",
            help="Log level",
        )

    try:
        args = parser.parse_args()
        setup_logging(args.log)

        def _resolve_dbpath() -> str:
            explicit = getattr(args, "db", None) or getattr(args, "dbpath", None)
            if explicit:
                return str(explicit)
            try:
                return tm_config.get_db_path(default=None)
            except ValueError as e:
                logging.error(str(e))
                logging.error("Set tagminder.toml [db].path or pass --db PATH / dbpath")
                raise SystemExit(2)

        # Validate paths
        if args.action == "import":
            _require_deps(need_polars=True, need_audioinf=True)

            musicdirs = list(args.musicdirs or [])
            # When --db is provided, any positional argument before musicdirs
            # is intended to be the first music directory, not a dbpath.
            if getattr(args, "db", None) is not None and getattr(args, "dbpath", None):
                musicdirs = [args.dbpath, *musicdirs]
                args.dbpath = None

            # Allow omitting the DB path entirely and passing only directories.
            # Example: tags2db.py import /music/drive1 /music/drive2
            # Argparse will otherwise interpret the first directory as dbpath.
            if (
                getattr(args, "db", None) is None
                and getattr(args, "dbpath", None)
                and os.path.isdir(args.dbpath)
            ):
                musicdirs = [args.dbpath, *musicdirs]
                args.dbpath = None

            if not musicdirs:
                logging.error("Error: No music directories specified for import")
                sys.exit(1)

            invalid_paths = [p for p in musicdirs if not os.path.exists(p)]
            if invalid_paths:
                logging.error(
                    f"Error: One or more specified music directories do not exist: {', '.join(invalid_paths)}"
                )
                sys.exit(1)

            dbpath = os.path.realpath(_resolve_dbpath())
            musicdirs = [os.path.realpath(p) for p in musicdirs]

            logging.info(f"Starting import operation on {len(musicdirs)} directories:")
            for i, path in enumerate(musicdirs, 1):
                logging.info(f"  {i}. {path}")

            if args.workers is not None and args.workers < 1:
                logging.warning(
                    "Warning: Worker count must be 1 or greater. Using default calculation for workers per drive."
                )
                args.workers = None  # Reset to None to trigger default calculation

            import_dir_optimised(
                dbpath=dbpath,
                dirpaths=musicdirs,
                workers=args.workers,
                chunk_size=args.chunk_size,
                new_files=args.new_files,
                modified_files=args.modified_files,
                prunedb=args.prunedb,
            )

            # Regenerate audit trigger to capture any new columns from import
            _regenerate_audit_trigger(dbpath)

        elif args.action == "export":
            _require_deps(need_polars=True, need_audioinf=True)

            musicdir_for_export = args.musicdir
            # Allow omitting the DB path and specifying only the music directory.
            # Example: tags2db.py export /music/library/
            if musicdir_for_export is None and getattr(args, "dbpath", None) and os.path.isdir(args.dbpath):
                musicdir_for_export = args.dbpath
                args.dbpath = None

            if not musicdir_for_export:
                logging.error("Error: Missing music directory for export")
                sys.exit(1)

            if not os.path.exists(musicdir_for_export):
                logging.error(
                    f"Error: Music directory for export does not exist: {musicdir_for_export}"
                )
                sys.exit(1)

            dbpath = os.path.realpath(_resolve_dbpath())
            musicdir_resolved = os.path.realpath(musicdir_for_export)

            logging.info(
                f"Starting export operation filtered by music directory: {musicdir_resolved}"
            )
            export_db(dbpath, musicdir_resolved, touch_mtime=args.touch_mtime)

        else:  # housekeeping
            dbpath = os.path.realpath(_resolve_dbpath())
            if not os.path.exists(dbpath):
                logging.error(f"Error: Database does not exist: {dbpath}")
                sys.exit(1)

            if args.dropnulls:
                logging.info("Starting housekeeping: dropnulls to remove non-schema all-null columns...")
                housekeeping_dropnulls(dbpath)
            else:
                logging.error(
                    "No housekeeping operation selected. Use --dropnulls (or run with -h for options)."
                )
                sys.exit(2)

    except Exception as e:
        logging.error(
            f"An unhandled error occurred during script execution: {str(e)}",
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
