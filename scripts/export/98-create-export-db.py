#!/usr/bin/env python3
""" 
Purpose:
    Create a separate export SQLite database containing an optimised `alib` table.
    The staging database is left untouched.

    The export table retains all system columns (prefixed with `__`) and only
    specified tag columns, reducing memory usage and improving export performance.

    The export database path is configured in `tagminder.toml` under
    `[export].db_path` and defaults to `tagminder_export.db`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog
    - sqlite_master (introspection)

Author: audiomuze
Last updated: 2026-04-13
"""

import argparse
import sqlite3
import sys
import logging
from typing import List
from pathlib import Path

from tagminder.core import tm_db
from tagminder.core import tm_config

def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """Get all column names from a table.

    Args:
        conn: SQLite connection
        table_name: Name of the table

    Returns:
        List of column names
    """
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def get_system_columns(columns: List[str]) -> List[str]:
    """Filter columns to return only system columns (prefixed with __).

    Args:
        columns: List of all column names

    Returns:
        List of system column names
    """
    return [col for col in columns if col.startswith("__")]


def read_tags_from_file(filepath: str) -> List[str]:
    """Read tag names from a text file (one per line).

    Args:
        filepath: Path to file containing tag names

    Returns:
        List of tag names

    Raises:
        FileNotFoundError: If file doesn't exist
        IOError: If file can't be read
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            tags = [
                line.strip()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
        return tags
    except FileNotFoundError:
        raise FileNotFoundError(f"Tags file not found: {filepath}")
    except IOError as e:
        raise IOError(f"Error reading tags file {filepath}: {e}")


def get_changelog_columns(
    conn: sqlite3.Connection, changelog_table: str = "changelog"
) -> List[str]:
    """Get distinct column names from changelog table if it exists and has data.

    Args:
        conn: SQLite connection
        changelog_table: Name of the changelog table (default: 'changelog')

    Returns:
        List of distinct column names from changelog, empty list if table doesn't exist or has no data
    """
    try:
        # Check if changelog table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (changelog_table,),
        )
        if not cursor.fetchone():
            return []

        # Check if table has data and get distinct column names
        # Canonical changelog schema uses `alib_column`.
        cursor = conn.execute(f"SELECT DISTINCT alib_column FROM {changelog_table}")
        columns = [
            row[0] for row in cursor.fetchall() if row[0]
        ]  # Filter out None/empty values

        return columns
    except sqlite3.Error:
        # If any error occurs (e.g., column doesn't exist), return empty list
        return []


def validate_tags(
    conn: sqlite3.Connection, tags_to_keep: List[str], table_name: str
) -> tuple[List[str], List[str]]:
    """Validate that specified tags exist in the table.

    Args:
        conn: SQLite connection
        tags_to_keep: List of tag names to validate
        table_name: Name of the table

    Returns:
        Tuple of (valid_tags, invalid_tags)
    """
    existing_columns = set(get_table_columns(conn, table_name))

    valid_tags = []
    invalid_tags = []

    for tag in tags_to_keep:
        if tag in existing_columns:
            valid_tags.append(tag)
        else:
            invalid_tags.append(tag)

    return valid_tags, invalid_tags


def optimise_table_columns(
    dbpath: str,
    tags_to_keep: List[str],
    table_name: str = "alib",
    export_db_path: str | Path | None = None,
    dry_run: bool = False,
    vacuum: bool = False,
) -> None:
    """Write an optimised export table into a new SQLite database.

    Args:
        dbpath: Path to staging SQLite database (left untouched)
        tags_to_keep: List of tag column names to retain
        table_name: Name of source table in staging DB (default: 'alib')
        export_db_path: Path to export SQLite DB to create
        dry_run: If True, show what would be done without making changes
        vacuum: If True, vacuum export database after writing

    Raises:
        sqlite3.Error: If database operations fail
        ValueError: If invalid parameters provided
    """
    import time

    if not dbpath or not Path(dbpath).exists():
        raise ValueError(f"Database file does not exist: {dbpath}")

    if not tags_to_keep:
        raise ValueError("No tags specified to keep")

    if export_db_path is None:
        raise ValueError("export_db_path is required")

    export_path = Path(export_db_path)
    try:
        staging_path_resolved = Path(dbpath).resolve()
        export_path_resolved = export_path.resolve()
    except Exception:
        staging_path_resolved = Path(dbpath)
        export_path_resolved = export_path

    if export_path_resolved == staging_path_resolved:
        raise ValueError(
            "Export DB path resolves to the staging DB path; refusing to overwrite staging DB: "
            f"{export_path_resolved}"
        )

    if export_path.exists():
        if export_path.is_dir():
            raise ValueError(f"Export DB path is a directory (expected file): {export_path}")

        if dry_run:
            logging.info(f"DRY RUN - Export DB already exists and would be overwritten: {export_path}")
        else:
            logging.info(f"Export DB already exists; overwriting: {export_path}")
            export_path.unlink()

            # Clean up common SQLite sidecar files that may exist from prior runs.
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(str(export_path) + suffix)
                try:
                    if sidecar.exists():
                        sidecar.unlink()
                except Exception:
                    # Best-effort cleanup; the main DB file is what matters.
                    pass

    try:
        # Get initial database size for comparison
        initial_size = Path(dbpath).stat().st_size

        # Connect to staging database read-only (do not modify it).
        # Avoid enabling WAL here: switching journal_mode to WAL can leave behind
        # *.db-wal / *.db-shm sidecar files that confuse users.
        staging = tm_db.connect(dbpath, read_only=True, wal=False)
        staging.row_factory = None
        try:
            staging.execute("PRAGMA query_only = ON")
        except sqlite3.Error:
            pass

        # Get current columns
        current_columns = get_table_columns(staging, table_name)
        if not current_columns:
            raise ValueError(f"Table '{table_name}' does not exist or has no columns")

        # Validate requested tags
        valid_tags, invalid_tags = validate_tags(staging, tags_to_keep, table_name)

        if invalid_tags:
            logging.warning(
                f"Invalid tag names (will be ignored): {', '.join(invalid_tags)}"
            )

        if not valid_tags:
            raise ValueError("None of the specified tags exist in the table")

        # Determine columns to keep
        system_columns = get_system_columns(current_columns)
        columns_to_keep = system_columns + valid_tags

        # Remove duplicates while preserving order
        columns_to_keep = list(dict.fromkeys(columns_to_keep))

        # Determine columns to drop
        columns_to_drop = [col for col in current_columns if col not in columns_to_keep]

        # Calculate estimated space savings
        estimated_reduction = len(columns_to_drop) / len(current_columns) * 100

        # Log what we're doing
        logging.info(f"Current table has {len(current_columns)} columns")
        logging.info(f"System columns (keeping): {len(system_columns)}")
        # logging.info(
        #     f"Tag columns to keep (i.e. columns with updated metadata): {len(valid_tags)}"
        # )
        # for tag in valid_tags:
        #     logging.info(f"  - {tag}")
        logging.info(
            f"Columns to drop (i.e no changes to be made to file metadata): {len(columns_to_drop)}"
        )
        logging.info(f"Estimated space reduction: {estimated_reduction:.1f}%")
        logging.info(f"Initial database size: {initial_size / (1024 * 1024):.1f} MB")
        logging.info(f"Export DB will be created at: {export_path}")

        if dry_run:
            logging.info("DRY RUN - Would drop these columns:")
            for col in columns_to_drop:
                logging.info(f"  - {col}")
            logging.info(f"Export table would have {len(columns_to_keep)} columns")
            if vacuum:
                logging.info("Would vacuum export database after writing")
            return

        logging.info("Starting export DB write...")
        export_start = time.time()

        # Export DB is an artifact; keep default (non-WAL) journaling to avoid
        # persistent sidecar files alongside the export DB.
        export_conn = tm_db.connect(str(export_path), wal=False)
        export_conn.row_factory = None
        try:
            export_conn.execute("BEGIN")

            # Create export table with only the columns we want.
            col_defs: list[str] = []
            for col in columns_to_keep:
                if col == "__path":
                    col_defs.append(f'"{col}" TEXT PRIMARY KEY')
                elif col == "__sqlmodded":
                    col_defs.append(f'"{col}" INTEGER')
                else:
                    col_defs.append(f'"{col}" TEXT')

            export_conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')

            # Determine whether changelog exists in staging.
            cursor = staging.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                ("changelog",),
            )
            changelog_exists = cursor.fetchone() is not None

            columns_list = ", ".join(f'"{col}"' for col in columns_to_keep)
            if changelog_exists:
                select_sql = f"""
                    SELECT {columns_list}
                    FROM "{table_name}"
                    WHERE "__path" IN (
                        SELECT DISTINCT alib_path FROM changelog
                        WHERE alib_path IS NOT NULL
                    )
                """.strip()
            else:
                select_sql = f'SELECT {columns_list} FROM "{table_name}"'

            placeholders = ", ".join(["?"] * len(columns_to_keep))
            insert_sql = f'INSERT INTO "{table_name}" ({columns_list}) VALUES ({placeholders})'

            cur = staging.execute(select_sql)
            batch_size = 2000
            total = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                export_conn.executemany(insert_sql, rows)
                total += len(rows)

            export_conn.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table_name}_path ON "{table_name}"("__path")'
            )

            export_conn.execute("COMMIT")

            export_time = time.time() - export_start
            logging.info(f"Export completed in {export_time:.1f} seconds")
            logging.info(f"Wrote {total:,} rows to export DB")

            if vacuum:
                logging.info("Vacuuming export database (this may take a while)...")
                vacuum_start = time.time()
                export_conn.execute("VACUUM")
                vacuum_time = time.time() - vacuum_start
                logging.info(f"Vacuum completed in {vacuum_time:.1f} seconds")

        except Exception as e:
            try:
                export_conn.execute("ROLLBACK")
            except Exception:
                pass
            raise sqlite3.Error(f"Failed to create export DB: {e}")
        finally:
            export_conn.close()

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        if "staging" in locals():
            staging.close()


def setup_logging(level: str) -> None:
    """Set up logging configuration.

    Args:
        level: Logging level
    """
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
    except AttributeError:
        log_level = logging.INFO
        print(f"Invalid log level: {level}, defaulting to INFO", file=sys.stderr)

    logging.basicConfig(
        level=log_level, format=log_format, handlers=[logging.StreamHandler()]
    )


def _export_db_path_from_config(staging_db_path: str) -> Path:
    cfg = tm_config.load_config()

    export_cfg = cfg.get("export", {}) if isinstance(cfg, dict) else {}
    name_or_path = export_cfg.get("db_path") if isinstance(export_cfg, dict) else None
    if not name_or_path:
        name_or_path = "tagminder_export.db"

    p = Path(str(name_or_path))
    if p.is_absolute() or p.parent != Path('.'):
        return p

    # If only a filename is provided, create it alongside the staging DB.
    return Path(staging_db_path).resolve().parent / p.name


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog=Path(sys.argv[0]).name,
        description="Create an export SQLite database containing an optimised alib table. "
        "The staging database is left untouched.",
        epilog="Examples:\n"
        "  %(prog)s --db staging.sqlite --keep title artist album\n"
        "  %(prog)s --db staging.sqlite --keep-file tags.txt --vacuum\n"
        "  %(prog)s --db staging.sqlite --dry-run  # auto-detect from changelog\n"
        "\nExport DB path is read from tagminder.toml [export].db_path (default: tagminder_export.db).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to staging SQLite database containing the source alib table (default: tagminder.toml [db].path)",
    )

    # Mutually exclusive group for specifying tags
    tag_group = parser.add_mutually_exclusive_group(required=False)
    tag_group.add_argument(
        "--keep",
        nargs="+",
        metavar="TAG",
        help="Tag column names to keep (space-separated). "
        "System columns (__prefixed) are always kept.",
    )
    tag_group.add_argument(
        "--keep-file",
        metavar="PATH",
        help="Text file with tag names to keep (one per line, # for comments)",
    )

    parser.add_argument(
        "--table",
        default=None,
        metavar="NAME",
        help="Source table name in the staging DB (default: tagminder.toml [export].table_name)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )

    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Vacuum export database after writing to reclaim disk space "
        "(slower but saves space)",
    )

    parser.add_argument(
        "--log",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level (default: %(default)s)",
    )

    # Auto-detection note
    parser.add_argument(
        "--version", action="version", version="%(prog)s 1.0 - Created by audiomuze"
    )

    try:
        args = parser.parse_args()
        setup_logging(args.log)

        cfg = tm_config.load_config()

        if not args.table:
            export_cfg = cfg.get("export", {}) if isinstance(cfg, dict) else {}
            table_name = export_cfg.get("table_name") if isinstance(export_cfg, dict) else None
            if not table_name:
                logging.error(
                    "No source table name resolved. Provide --table NAME or set tagminder.toml [export].table_name"
                )
                sys.exit(2)
            args.table = str(table_name)

        # Resolve staging DB path from CLI or tagminder.toml
        if not args.db:
            args.db = tm_config.db_path_from_toml(default=None)

        if not args.db:
            logging.error(
                "No staging DB path resolved. Provide --db PATH or set tagminder.toml [db].path"
            )
            sys.exit(2)

        # Validate database path
        if not Path(args.db).exists():
            logging.error(f"Database file does not exist: {args.db}")
            sys.exit(1)

        export_db_path = _export_db_path_from_config(args.db)

        # Get tags to keep
        if args.keep:
            tags_to_keep = args.keep
        elif args.keep_file:
            try:
                tags_to_keep = read_tags_from_file(args.keep_file)
            except (FileNotFoundError, IOError) as e:
                logging.error(str(e))
                sys.exit(1)
        else:
            # Auto-detect from changelog
            conn = tm_db.connect(args.db, read_only=True, wal=False)
            try:
                tags_to_keep = get_changelog_columns(conn)
                if not tags_to_keep:
                    logging.error(
                        "No changelog table found or no data in changelog, and no tags specified"
                    )
                    sys.exit(1)
                logging.info(
                    f"Auto-detected {len(tags_to_keep)} columns from changelog table"
                )
            finally:
                conn.close()

        if not tags_to_keep:
            logging.error("No tags specified to keep")
            sys.exit(1)

        # Remove duplicates while preserving order
        tags_to_keep = list(dict.fromkeys(tags_to_keep))

        logging.info(f"Creating export DB from table '{args.table}' in {args.db}")
        logging.info(f"Export DB: {export_db_path}")
        logging.info(
            f"Tags to keep (i.e. tags with changed metadata): {len(tags_to_keep)}"
        )
        for tag in tags_to_keep:
            logging.info(f"  - {tag}")

        if args.dry_run:
            logging.info("Running in DRY RUN mode - no changes will be made")

        # optimise_table_columns(
        #     dbpath=args.db,
        #     tags_to_keep=tags_to_keep,
        #     table_name=args.table,
        #     dry_run=args.dry_run
        # )

        optimise_table_columns(
            dbpath=args.db,
            tags_to_keep=tags_to_keep,
            table_name=args.table,
            export_db_path=export_db_path,
            dry_run=args.dry_run,
            vacuum=args.vacuum,
        )

        if not args.dry_run:
            logging.info("Export completed successfully")

    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
