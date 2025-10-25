#!/usr/bin/env python3
"""
QNAP Database Partitioner

Creates three separate databases (qnap1.db, qnap2.db, qnap3.db) from a source database,
partitioning the alib table based on path prefixes (/qnap/qnap1, /qnap/qnap2, /qnap/qnap3).

Features:
- Uses SQL GLOB patterns for efficient filtering
- Atomic operations with transactions
- Source database protection with read-only access
- Comprehensive error handling and logging
- Progress tracking for large datasets
- Memory-efficient streaming operations

Usage:
    python qnap_partitioner.py source_database.db
"""

import sqlite3
import logging
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple
import time
from contextlib import contextmanager


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('qnap_partitioner.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class QNAPPartitioner:
    """
    Efficiently partitions a source database into QNAP-specific databases.

    This class automatically detects QNAP partitions from the path structure
    and creates separate databases for each partition found.
    """

    def __init__(self, source_db_path: str):
        """
        Initialize the partitioner with the source database path.

        Args:
            source_db_path: Path to the source SQLite database

        Raises:
            FileNotFoundError: If source database doesn't exist
            sqlite3.Error: If source database is invalid
        """
        self.source_db_path = Path(source_db_path)
        self.source_dir = self.source_db_path.parent

        # Validate source database exists and is readable
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"Source database not found: {source_db_path}")

        # Will be populated after detecting partitions
        self.partitions = {}

        logger.info(f"Initialized partitioner for source: {self.source_db_path}")

def detect_partitions(self) -> Set[str]:
    """
    Detect all unique partition names by analyzing the path structure between the 2nd and 3rd slashes.
    Works with any path format by finding the segment between the 2nd and 3rd forward slashes.

    Returns:
        Set of unique partition names found between 2nd and 3rd slashes in paths

    Raises:
        sqlite3.Error: If path analysis fails or no partitions found
    """
    with self.get_source_connection() as conn:
        try:
            # Get distinct segments between 2nd and 3rd slashes
            cursor = conn.execute("""
                WITH path_components AS (
                    SELECT
                        __path,
                        -- Find position of 2nd slash
                        instr(substr(__path, instr(__path, '/') + 1), '/') + instr(__path, '/') AS second_slash_pos,
                        -- Find position of 3rd slash
                        CASE
                            WHEN instr(substr(__path, instr(substr(__path, instr(__path, '/') + 1), '/') + 1), '/') > 0
                            THEN instr(substr(__path, instr(substr(__path, instr(__path, '/') + 1), '/') + 1), '/')
                                 + instr(substr(__path, instr(__path, '/') + 1), '/')
                                 + instr(__path, '/')
                            ELSE 0
                        END AS third_slash_pos
                    FROM alib
                    WHERE __path LIKE '%/%/%'  -- Must have at least 2 slashes
                )
                SELECT DISTINCT
                    substr(
                        __path,
                        second_slash_pos + 1,
                        CASE
                            WHEN third_slash_pos > 0 THEN third_slash_pos - second_slash_pos - 1
                            ELSE length(__path) - second_slash_pos
                        END
                    ) AS partition
                FROM path_components
                WHERE second_slash_pos > 0
                AND (
                    (third_slash_pos > 0 AND third_slash_pos > second_slash_pos + 1) OR
                    (third_slash_pos = 0 AND length(__path) > second_slash_pos + 1)
                )
                AND partition != ''
            """)

            partitions = {row['partition'] for row in cursor}

            if not partitions:
                raise sqlite3.Error("No partitions found in path structure")

            logger.info(f"Detected partitions: {', '.join(partitions)}")
            return partitions

        except sqlite3.Error as e:
            logger.error("Failed to detect partitions from path structure")
            raise

    def initialize_partitions(self) -> None:
        """
        Initialize partition configurations based on detected partitions.
        """
        partitions = self.detect_partitions()

        for partition in sorted(partitions):
            self.partitions[partition] = {
                'db_name': f"{partition}.db",
                'glob_pattern': f"/qnap/{partition}/*",
                'description': f"{partition} partition"
            }

        logger.info(f"Initialized {len(self.partitions)} partition configurations")

    @contextmanager
    def get_source_connection(self):
        """
        Context manager for read-only connection to source database.

        Yields:
            sqlite3.Connection: Read-only connection to source database
        """
        conn = None
        try:
            # Open in read-only mode to prevent accidental modifications
            conn = sqlite3.connect(f"file:{self.source_db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to source database: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_target_connection(self, target_db_path: Path):
        """
        Context manager for read-write connection to target database.

        Args:
            target_db_path: Path to target database

        Yields:
            sqlite3.Connection: Read-write connection to target database
        """
        conn = None
        try:
            conn = sqlite3.connect(target_db_path)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better performance
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance safety and performance
            conn.execute("PRAGMA temp_store=MEMORY")  # Use memory for temporary storage
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to target database {target_db_path}: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_source_schema(self) -> str:
        """
        Extract the CREATE TABLE statement for the alib table from source database.

        Returns:
            str: SQL CREATE TABLE statement for alib table

        Raises:
            sqlite3.Error: If alib table doesn't exist or schema can't be retrieved
        """
        with self.get_source_connection() as conn:
            cursor = conn.execute("""
                SELECT sql FROM sqlite_master
                WHERE type='table' AND name='alib'
            """)

            result = cursor.fetchone()
            if not result:
                raise sqlite3.Error("alib table not found in source database")

            return result[0]

    def get_record_counts(self) -> Dict[str, int]:
        """
        Get count of records for each partition to estimate progress.

        Returns:
            Dict[str, int]: Mapping of partition name to record count
        """
        counts = {}

        with self.get_source_connection() as conn:
            for partition_name, config in self.partitions.items():
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM alib
                    WHERE __path GLOB ?
                """, (config['glob_pattern'],))

                counts[partition_name] = cursor.fetchone()[0]

        return counts

    def create_partition_database(self, partition_name: str, config: Dict[str, str]) -> None:
        """
        Create a single partition database with filtered records.

        Args:
            partition_name: Name of the partition (e.g., 'qnap1')
            config: Configuration dictionary for this partition
        """
        target_db_path = self.source_dir / config['db_name']

        # Remove existing database if it exists
        if target_db_path.exists():
            target_db_path.unlink()
            logger.info(f"Removed existing database: {target_db_path}")

        # Get source table schema
        schema_sql = self.get_source_schema()

        logger.info(f"Creating {config['description']} database: {target_db_path}")

        with self.get_target_connection(target_db_path) as target_conn:
            # Create table with same schema as source
            target_conn.execute(schema_sql)

            # Copy indices and other objects if they exist
            with self.get_source_connection() as source_conn:
                # Copy indices
                index_cursor = source_conn.execute("""
                    SELECT sql FROM sqlite_master
                    WHERE type='index' AND tbl_name='alib' AND sql IS NOT NULL
                """)

                for index_row in index_cursor:
                    try:
                        target_conn.execute(index_row[0])
                    except sqlite3.Error as e:
                        logger.warning(f"Failed to create index: {e}")

                # Start transaction for data insertion
                target_conn.execute("BEGIN TRANSACTION")

                try:
                    # Use INSERT INTO ... SELECT for maximum efficiency
                    insert_sql = """
                        INSERT INTO alib
                        SELECT * FROM source_alib
                        WHERE __path GLOB ?
                    """

                    # Attach source database and perform bulk insert
                    target_conn.execute(f"ATTACH DATABASE '{self.source_db_path}' AS source_db")
                    target_conn.execute("CREATE TEMPORARY VIEW source_alib AS SELECT * FROM source_db.alib")

                    cursor = target_conn.execute(insert_sql, (config['glob_pattern'],))
                    records_inserted = cursor.rowcount

                    # Commit transaction
                    target_conn.execute("COMMIT")

                    # Clean up
                    target_conn.execute("DROP VIEW source_alib")
                    target_conn.execute("DETACH DATABASE source_db")

                    logger.info(f"Successfully created {config['description']}: {records_inserted:,} records")

                except Exception as e:
                    target_conn.execute("ROLLBACK")
                    raise e

    def create_all_partitions(self) -> None:
        """
        Create all partition databases with comprehensive error handling.
        """
        start_time = time.time()

        try:
            # First detect and initialize partitions
            self.initialize_partitions()

            # Get record counts for progress estimation
            logger.info("Analyzing source database...")
            record_counts = self.get_record_counts()

            total_records = sum(record_counts.values())
            logger.info(f"Source database contains {total_records:,} records across {len(self.partitions)} partitions")

            for partition_name, count in record_counts.items():
                logger.info(f"  {partition_name}: {count:,} records")

            # Create each partition database
            for partition_name, config in self.partitions.items():
                try:
                    self.create_partition_database(partition_name, config)
                except Exception as e:
                    logger.error(f"Failed to create {partition_name}: {e}")
                    raise

            # Log completion summary
            elapsed_time = time.time() - start_time
            logger.info(f"Partitioning completed successfully in {elapsed_time:.2f} seconds")
            logger.info(f"Created databases in: {self.source_dir}")

            # Verify created databases
            for config in self.partitions.values():
                db_path = self.source_dir / config['db_name']
                if db_path.exists():
                    size_mb = db_path.stat().st_size / (1024 * 1024)
                    logger.info(f"  {config['db_name']}: {size_mb:.2f} MB")

        except Exception as e:
            logger.error(f"Partitioning failed: {e}")
            raise

def main():
    """
    Main entry point for the script.

    Handles command line arguments and orchestrates the partitioning process.
    """
    if len(sys.argv) != 2:
        print("Usage: python qnap_partitioner.py <source_database.db>")
        print("\nExample: python qnap_partitioner.py /path/to/media_library.db")
        sys.exit(1)

    source_db_path = sys.argv[1]

    try:
        # Create partitioner instance
        partitioner = QNAPPartitioner(source_db_path)

        # Perform partitioning
        partitioner.create_all_partitions()

        logger.info("✅ Database partitioning completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        sys.exit(1)
    except sqlite3.Error as e:
        logger.error(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
