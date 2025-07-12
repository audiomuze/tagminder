# 01 droptags-polars.py

Remove unauthorized tags from music library database

## Synopsis

```bash
python droptags-polars.py [OPTIONS]
uv run droptags-polars.py [OPTIONS]
```

## Description

This script processes all records in the alib table and sets tag values to null for all unauthorized tags. Any tag names that don't appear in the predefined fixed_columns list are considered unauthorized and will be removed. All changes are logged to the changelog table for auditing purposes.

This is the standard method for cleaning up unwanted tags in your music collection and is part of the tagminder suite.

The script uses Polars for efficient data processing and maintains a comprehensive audit trail of all modifications made to the database.

## Options

| Option | Description |
|--------|-------------|
| `-h`, `--help` | Show help message and exit |
| `-v`, `--verbose` | Enable verbose logging (DEBUG level) |
| `-q`, `--quiet` | Suppress all output except errors |
| `--dry-run` | Show what would be changed without modifying the database |
| `--db-path PATH` | Path to the SQLite database file (default: /tmp/amg/dbtemplate.db) |
| `--backup` | Create a backup of the database before making changes |
| `--backup-path PATH` | Specify custom backup location |
| `--show-fixed-columns` | Display the list of authorized columns and exit |
| `--column-stats` | Show statistics about unauthorized columns before cleanup |
| `--confirm` | Require interactive confirmation before making changes |
| `--log-file PATH` | Write log output to specified file in addition to console |
| `--max-changes N` | Stop processing if more than N changes would be made |

## Examples

```bash
# Basic usage
python droptags-polars.py

# Dry run to preview changes
python droptags-polars.py --dry-run --column-stats

# Use custom database with backup
python droptags-polars.py --db-path /path/to/music.db --backup

# Verbose mode with confirmation
python droptags-polars.py --verbose --confirm

# Show authorized columns
python droptags-polars.py --show-fixed-columns
```

## Authorized Columns

The script preserves these column types:
- System columns (prefixed with `__`)
- Core music metadata (title, artist, album, etc.)
- MusicBrainz identifiers
- Audio analysis data
- File system metadata
- Custom tagminder fields

## Output

- Summary of changes made
- Statistics by column showing number of values removed
- Total number of rows affected
- All changes logged to changelog table with timestamps

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Database connection error |
| 3 | User cancelled operation |
| 4 | Safety limit exceeded |

## Notes

- Modifies the `sqlmodded` counter for each affected row
- All changes logged with timestamps in the `changelog` table
- Safe to run multiple times
- Large databases may take several minutes to process

## Author

audiomuze

## Version

1.0.0 (2025-04-21)
