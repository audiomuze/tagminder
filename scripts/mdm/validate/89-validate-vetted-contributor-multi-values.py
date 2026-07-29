"""
Purpose:
    Validate rows in _REF_vetted_contributors where status is NULL and
    replacement_val contains delimited items.

    For each row with delimited replacement_val:
    - Split by the double backslash delimiter
    - Strip/deduplicate tokens
    - Look up each token in contributors_unified_disambiguated.lpreferred__artist_name (lowercase)
    - If ALL tokens are found: set status = 1 (accepted)
    - If any token missing/empty: set status = 'empty_segments' (rejected)
    - Log all changes to master_data_changelog

This script is part of Tagminder.

SQLite tables referenced:
    - _REF_vetted_contributors
    - contributors_unified_disambiguated
    - master_data_changelog

Author: audiomuze
Last updated: 2026-05-10
"""

import polars as pl
import sqlite3
from typing import Set
import logging

from tagminder.core import tm_db
from tagminder.core import tm_changes
from tagminder.core import tm_config
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# ---------- Logging Setup ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------- Global Constants ----------
DELIMITER = tm_config.get_multivalue_delimiter()
EMPTY_SEGMENTS_CODE = "empty segments"
VALID_CODE = 1


# ---------- Database Helper Functions ----------


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the SQLite database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


# ---------- Validation Functions ----------


def load_entity_lookup(conn: sqlite3.Connection) -> Set[str]:
    """Load all entities from contributors_unified_disambiguated.lpreferred__artist_name into a set.
    
    Returns:
        Set of lowercase entity names for fast lookup
    """
    try:
        df = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT DISTINCT lpreferred__artist_name "
            "FROM contributors_unified_disambiguated "
            "WHERE lpreferred__artist_name IS NOT NULL",
        )
        return set(df["lpreferred__artist_name"].to_list())
    except Exception as e:
        logging.error(f"Failed to load entity lookup: {e}")
        return set()


def validate_delimited_value(
    replacement_val: str, entity_lookup: Set[str]
) -> tuple[bool, str | None, bool]:
    """Validate a delimited replacement_val against entity lookup.
    
    Args:
        replacement_val: Delimited string (may contain DELIMITER)
        entity_lookup: Set of valid lowercase entities
        
    Returns:
        (is_valid, error_code_or_none, has_missing_token)
        - (True, None) if all tokens are valid
        - (False, 'empty segments') if any token is empty
        - (False, None) if tokens are non-empty but one or more are not found
    """
    if not replacement_val or DELIMITER not in replacement_val:
        # Single-value rows are not processed in this pass
        return True, None, False
    
    # Split, strip, and deduplicate
    tokens = replacement_val.split(DELIMITER)
    seen = set()
    
    for token in tokens:
        stripped = token.strip()
        
        # Detect empty segments
        if not stripped:
            return False, EMPTY_SEGMENTS_CODE, False
        
        # Deduplicate (but still validate each unique token)
        if stripped in seen:
            continue
        seen.add(stripped)
        
        # Look up in entity set (use lowercase)
        lookup_key = stripped.lower()
        if lookup_key not in entity_lookup:
            return False, None, True
    
    # All tokens found and no empty segments
    return True, None, False


# ---------- Main Execution Function ----------


def main():
    """
    Main execution: validate _REF_vetted_contributors rows with null status
    and delimited replacement_val.
    """

    try:
        master_db_path = tm_config.get_master_data_db_path(
            default=tm_config.db_path_from_toml(default=None)
        )
        master_conn = tm_db.connect(master_db_path)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except ValueError as e:
        logging.error(f"Master-data DB path is not configured: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    try:
        # Ensure master_data_changelog table exists
        tm_db.ensure_master_data_changelog_table(master_conn)

        # Check required tables exist
        if not table_exists(master_conn, "_REF_vetted_contributors"):
            logging.info("_REF_vetted_contributors table does not exist")
            return

        if not table_exists(master_conn, "contributors_unified_disambiguated"):
            logging.info("contributors_unified_disambiguated table does not exist")
            return

        # Load entity lookup set
        logging.info("Loading entity lookup...")
        entity_lookup = load_entity_lookup(master_conn)
        logging.info(f"Loaded {len(entity_lookup)} entities for validation")

        if not entity_lookup:
            logging.warning("No entities found in contributors_unified_disambiguated")

        # Load candidates: status IS NULL AND replacement_val contains DELIMITER
        logging.info("Fetching candidate rows...")
        candidates = tm_polars_db.sqlite_to_polars(
            master_conn,
            f"""
            SELECT rowid, replacement_val
            FROM _REF_vetted_contributors
            WHERE status IS NULL AND replacement_val LIKE ? AND replacement_val IS NOT NULL
            ORDER BY rowid
            """,
            params=(f"%{DELIMITER}%",),
        )

        logging.info(f"Found {candidates.height} rows to validate")

        if candidates.height == 0:
            logging.info("No rows to validate")
            return

        # Validate each row
        timestamp = tm_db.utc_now_iso()
        script_name = tm_db.script_name()

        cursor = master_conn.cursor()
        updated_count = 0
        skipped_missing_count = 0
        with tm_db.transaction(master_conn):
            changelog = tm_changes.MasterDataChangelogBatch(
                timestamp=timestamp, script=script_name
            )

            for record in candidates.iter_rows(named=True):
                rowid = record["rowid"]
                replacement_val = record["replacement_val"]

                is_valid, error_code, has_missing_token = validate_delimited_value(
                    replacement_val, entity_lookup
                )

                # Status semantics:
                # - valid -> 1
                # - empty segments -> "empty segments"
                # - token(s) missing from contributors_unified_disambiguated -> leave NULL (unassessed)
                if has_missing_token:
                    skipped_missing_count += 1
                    continue
                new_status = VALID_CODE if is_valid else error_code

                # Update the row
                cursor.execute(
                    "UPDATE _REF_vetted_contributors SET status = ? WHERE rowid = ?",
                    (new_status, rowid),
                )

                # Log the change
                changelog.add(
                    table_name="_REF_vetted_contributors",
                    rowid=rowid,
                    changes=[("status", None, new_status)],
                )
                updated_count += 1

            changelog.flush(cursor)

        logging.info(
            "Validation complete for _REF_vetted_contributors: "
            f"candidates={candidates.height}, updated={updated_count}, "
            f"skipped_missing_tokens={skipped_missing_count}"
        )

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        raise
    finally:
        master_conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
