"""
Help text for droptags-polars.py script
"""

HELP_TEXT = """
droptags-polars.py - Remove unauthorized tags from music library database

DESCRIPTION:
    This script processes all records in the alib table and sets tag values to null
    for all unauthorized tags (i.e. any tag names that don't appear in the predefined
    fixed_columns list). All changes are logged to the changelog table for auditing.

    This is the de-facto way of cleaning up unwanted tags in your music collection
    and is part of the tagminder suite.

    The script uses Polars for efficient data processing and maintains a comprehensive
    audit trail of all modifications made to the database.

USAGE:
    python droptags-polars.py [OPTIONS]
    uv run droptags-polars.py [OPTIONS]

OPTIONS:
    -h, --help              Show this help message and exit
    -v, --verbose           Enable verbose logging (DEBUG level)
    -q, --quiet             Suppress all output except errors
    --dry-run               Show what would be changed without modifying the database
    --db-path PATH          Path to the SQLite database file
                           (default: /tmp/amg/dbtemplate.db)
    --backup                Create a backup of the database before making changes
    --backup-path PATH      Specify custom backup location
                           (default: {db_path}.backup.{timestamp})
    --show-fixed-columns    Display the list of authorized columns and exit
    --column-stats          Show statistics about unauthorized columns before cleanup
    --confirm               Require interactive confirmation before making changes
    --log-file PATH         Write log output to specified file in addition to console
    --max-changes N         Stop processing if more than N changes would be made
                           (safety limit, default: unlimited)

EXAMPLES:
    # Basic usage - clean up all unauthorized tags
    python droptags-polars.py

    # Dry run to see what would be changed
    python droptags-polars.py --dry-run --column-stats

    # Use custom database path with backup
    python droptags-polars.py --db-path /path/to/music.db --backup

    # Verbose mode with confirmation prompt
    python droptags-polars.py --verbose --confirm

    # Show which columns are considered authorized
    python droptags-polars.py --show-fixed-columns

AUTHORIZED COLUMNS:
    The script preserves the following types of columns:
    - System columns (prefixed with __)
    - Core music metadata (title, artist, album, etc.)
    - MusicBrainz identifiers
    - Audio analysis data
    - File system metadata
    - Custom tagminder fields

    Use --show-fixed-columns to see the complete list.

OUTPUT:
    - Summary of changes made
    - Statistics by column showing number of values removed
    - Total number of rows affected
    - All changes are logged to the changelog table with timestamps

SAFETY FEATURES:
    - Transactional updates (all-or-nothing)
    - Comprehensive change logging
    - Optional backup creation
    - Dry-run mode for testing
    - Confirmation prompts
    - Maximum change limits

EXIT CODES:
    0    Success
    1    General error
    2    Database connection error
    3    User cancelled operation
    4    Safety limit exceeded (max-changes)

NOTES:
    - The script modifies the 'sqlmodded' counter for each affected row
    - All changes are logged with timestamps in the 'changelog' table
    - The script is designed to be run safely multiple times
    - Large databases may take several minutes to process

    For more information about tagminder, visit: https://github.com/audiomuze/tagminder

AUTHOR:
    audiomuze

VERSION:
    1.0.0 (2025-04-21)
"""
