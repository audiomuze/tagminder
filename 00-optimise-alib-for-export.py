#!/usr/bin/env python3
"""
Script Name: optimise_alib_columns.py

Purpose:
    optimises the alib SQLite table by dropping columns not needed for export.
    Retains all system columns (prefixed with __) and only specified tag columns.
    This reduces memory usage and improves export performance.

Usage:
    python optimise_alib_columns.py --db /path/to/db.sqlite --keep title artist album genre
    python optimise_alib_columns.py --db /path/to/db.sqlite --keep releasetype --dry-run
    python optimise_alib_columns.py --db /path/to/db.sqlite --keep-file tags_to_keep.txt

Author: audiomuze
Created: 2025-06-22
"""

import argparse
import sqlite3
import sys
import logging
from typing import List, Set
from pathlib import Path


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
    return [col for col in columns if col.startswith('__')]


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
        with open(filepath, 'r', encoding='utf-8') as f:
            tags = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return tags
    except FileNotFoundError:
        raise FileNotFoundError(f"Tags file not found: {filepath}")
    except IOError as e:
        raise IOError(f"Error reading tags file {filepath}: {e}")


def get_changelog_columns(conn: sqlite3.Connection, changelog_table: str = 'changelog') -> List[str]:
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
            (changelog_table,)
        )
        if not cursor.fetchone():
            return []

        # Check if table has data and get distinct column names
        cursor = conn.execute(f"SELECT DISTINCT column FROM {changelog_table}")
        columns = [row[0] for row in cursor.fetchall() if row[0]]  # Filter out None/empty values

        return columns
    except sqlite3.Error:
        # If any error occurs (e.g., column doesn't exist), return empty list
        return []


def validate_tags(conn: sqlite3.Connection, tags_to_keep: List[str], table_name: str) -> tuple[List[str], List[str]]:
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
    table_name: str = 'alib',
    dry_run: bool = False,
    vacuum: bool = False
) -> None:
    """optimise table by keeping only system columns and specified tag columns.

    Args:
        dbpath: Path to SQLite database
        tags_to_keep: List of tag column names to retain
        table_name: Name of table to optimise (default: 'alib')
        dry_run: If True, show what would be done without making changes
        vacuum: If True, vacuum database after optimization to reclaim space

    Raises:
        sqlite3.Error: If database operations fail
        ValueError: If invalid parameters provided
    """
    import time

    if not dbpath or not Path(dbpath).exists():
        raise ValueError(f"Database file does not exist: {dbpath}")

    if not tags_to_keep:
        raise ValueError("No tags specified to keep")

    try:
        # Get initial database size for comparison
        initial_size = Path(dbpath).stat().st_size

        # Connect to database
        conn = sqlite3.connect(dbpath)
        conn.execute("PRAGMA foreign_keys = OFF")  # Disable foreign keys for table operations

        # Get current columns
        current_columns = get_table_columns(conn, table_name)
        if not current_columns:
            raise ValueError(f"Table '{table_name}' does not exist or has no columns")

        # Validate requested tags
        valid_tags, invalid_tags = validate_tags(conn, tags_to_keep, table_name)

        if invalid_tags:
            logging.warning(f"Invalid tag names (will be ignored): {', '.join(invalid_tags)}")

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
        logging.info(f"Tag columns to keep: {len(valid_tags)} - {', '.join(valid_tags)}")
        logging.info(f"Columns to drop: {len(columns_to_drop)}")
        logging.info(f"Estimated space reduction: {estimated_reduction:.1f}%")
        logging.info(f"Initial database size: {initial_size / (1024*1024):.1f} MB")

        if dry_run:
            logging.info("DRY RUN - Would drop these columns:")
            for col in columns_to_drop:
                logging.info(f"  - {col}")
            logging.info(f"Final table would have {len(columns_to_keep)} columns")
            if vacuum:
                logging.info("Would vacuum database after optimization")
            return

        if not columns_to_drop:
            logging.info("No columns need to be dropped - table is already optimised")
            if vacuum:
                logging.info("Vacuuming database anyway to optimise layout...")
                vacuum_start = time.time()
                conn.execute("VACUUM")
                vacuum_time = time.time() - vacuum_start
                final_size = Path(dbpath).stat().st_size
                size_change = ((initial_size - final_size) / initial_size) * 100
                logging.info(f"Vacuum completed in {vacuum_time:.1f} seconds")
                logging.info(f"Database size change: {size_change:+.1f}% ({final_size / (1024*1024):.1f} MB)")
            return

        # Create optimised table using transaction
        logging.info("Starting table optimization...")
        optimise_start = time.time()

        conn.execute("BEGIN TRANSACTION")

        try:
            # Create new table with only the columns we want
            columns_sql = []
            for col in columns_to_keep:
                if col == "__path":
                    columns_sql.append(f'"{col}" TEXT PRIMARY KEY')
                else:
                    columns_sql.append(f'"{col}" TEXT')

            create_sql = f"CREATE TABLE {table_name}_optimised ({', '.join(columns_sql)})"
            conn.execute(create_sql)
            # Ensure index exists for changelog lookup
            conn.execute("CREATE INDEX IF NOT EXISTS idx_changelog_alib_rowid ON changelog(alib_rowid)")


            # Copy only changed rows to new table - creating a new table is not a problem because export relies on __path not rowid
            columns_list = ', '.join(f'"{col}"' for col in columns_to_keep)
            # copy_sql = f"INSERT INTO {table_name}_optimised SELECT {columns_list} FROM {table_name}"
            copy_sql = f"""
                INSERT INTO {table_name}_optimised
                SELECT {columns_list}
                FROM {table_name}
                WHERE rowid IN (
                    SELECT DISTINCT alib_rowid FROM changelog
                    WHERE alib_rowid IS NOT NULL
                )
            """
            conn.execute(copy_sql)

            # Replace old table with new one
            conn.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_all_records")
            conn.execute(f"ALTER TABLE {table_name}_optimised RENAME TO {table_name}")

            # Create index on __path for better performance
            conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_path ON {table_name}("__path")')

            conn.execute("COMMIT")

            optimise_time = time.time() - optimise_start
            logging.info(f"Table optimization completed in {optimise_time:.1f} seconds")
            logging.info(f"Successfully dropped {len(columns_to_drop)} columns")
            logging.info(f"Final table has {len(columns_to_keep)} columns")

            # Vacuum database if requested
            if vacuum:
                logging.info("Vacuuming database to reclaim space (this may take a while)...")
                vacuum_start = time.time()

                # Set pragmas for faster vacuum
                conn.execute("PRAGMA journal_mode = DELETE")  # Temporary for vacuum
                conn.execute("PRAGMA synchronous = NORMAL")

                conn.execute("VACUUM")

                # Restore optimal pragmas
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")

                vacuum_time = time.time() - vacuum_start

                # Calculate actual space savings
                final_size = Path(dbpath).stat().st_size
                size_reduction = ((initial_size - final_size) / initial_size) * 100
                space_saved = (initial_size - final_size) / (1024 * 1024)

                logging.info(f"Vacuum completed in {vacuum_time:.1f} seconds")
                logging.info(f"Space reclaimed: {space_saved:.1f} MB ({size_reduction:.1f}% reduction)")
                logging.info(f"Final database size: {final_size / (1024*1024):.1f} MB")

                total_time = optimise_time + vacuum_time
                logging.info(f"Total optimization time: {total_time:.1f} seconds")
            else:
                # Show potential space savings without vacuum
                logging.info("Note: Run with --vacuum to reclaim disk space")
                logging.info(f"Estimated space savings with vacuum: ~{estimated_reduction:.1f}%")

        except Exception as e:
            conn.execute("ROLLBACK")
            raise sqlite3.Error(f"Failed to optimise table: {e}")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def setup_logging(level: str) -> None:
    """Set up logging configuration.

    Args:
        level: Logging level
    """
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
    except AttributeError:
        log_level = logging.INFO
        print(f"Invalid log level: {level}, defaulting to INFO", file=sys.stderr)

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[logging.StreamHandler()]
    )


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='optimise alib table by dropping unused tag columns',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '--db',
        required=True,
        help='Path to SQLite database containing alib table'
    )

    # Mutually exclusive group for specifying tags
    tag_group = parser.add_mutually_exclusive_group(required=False)
    tag_group.add_argument(
        '--keep',
        nargs='+',
        help='Tag column names to keep (space-separated)'
    )
    tag_group.add_argument(
        '--keep-file',
        help='Path to text file containing tag names to keep (one per line)'
    )

    parser.add_argument(
        '--table',
        default='alib',
        help='Table name to optimise'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )

    parser.add_argument(
        '--log',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Log level'
    )

    parser.add_argument(
        '--vacuum',
        action='store_true',
        help='Vacuum database after optimization (reclaims disk space but takes time). Performance gains from exporting a much smaller table are beneficial if exporting many records'
    )


    try:
        args = parser.parse_args()
        setup_logging(args.log)

        # Validate database path
        if not Path(args.db).exists():
            logging.error(f"Database file does not exist: {args.db}")
            sys.exit(1)

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
            conn = sqlite3.connect(args.db)
            try:
                tags_to_keep = get_changelog_columns(conn)
                if not tags_to_keep:
                    logging.error("No changelog table found or no data in changelog, and no tags specified")
                    sys.exit(1)
                logging.info(f"Auto-detected {len(tags_to_keep)} columns from changelog table")
            finally:
                conn.close()

        if not tags_to_keep:
            logging.error("No tags specified to keep")
            sys.exit(1)

        # Remove duplicates while preserving order
        tags_to_keep = list(dict.fromkeys(tags_to_keep))

        logging.info(f"Optimizing table '{args.table}' in {args.db}")
        logging.info(f"Tags to keep: {', '.join(tags_to_keep)}")

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
                    dry_run=args.dry_run,
                    vacuum=args.vacuum
                )

        if not args.dry_run:
            logging.info("Optimization completed successfully")

    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
