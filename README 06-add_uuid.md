# 06-add_uuid.py

## Description

Generates and assigns UUID values to rows in the `alib` table that are missing `tagminder_uuid` values. The script identifies rows where the `tagminder_uuid` field is NULL or empty, generates new UUIDs for these rows, and updates the database with full change logging.

## Usage

```bash
python 06-add_uuid.py
```

## Command Line Options

This script currently accepts no command line arguments or options. All configuration is handled through hardcoded constants within the script.

## Configuration

The script uses the following hardcoded configuration:

- **Database Path**: `/tmp/amg/dbtemplate.db`
- **Target Table**: `alib`
- **UUID Column**: `tagminder_uuid`
- **Modification Counter**: `sqlmodded`

## What the Script Does

1. **Connects** to the SQLite database at the configured path
2. **Identifies** rows in the `alib` table where `tagminder_uuid` is NULL or empty
3. **Generates** new UUID4 values for all identified rows using vectorized operations
4. **Updates** the database with the new UUIDs
5. **Increments** the `sqlmodded` counter for each modified row
6. **Logs changes** to a `changelog` table with timestamps and script attribution
7. **Provides** detailed logging output during execution

## Database Requirements

The script expects:
- An existing SQLite database at the configured path
- An `alib` table with columns: `tagminder_uuid`, `sqlmodded`
- The script will automatically create a `changelog` table if it doesn't exist

## Output

The script provides informational logging including:
- Number of rows requiring UUID generation
- Sample of affected row IDs
- Confirmation of updates written to database
- Database connection status

## Dependencies

- `sqlite3` (Python standard library)
- `polars` - Used for efficient data processing
- `logging` (Python standard library)
- `uuid` (Python standard library)
- `datetime` (Python standard library)

## Exit Behavior

The script runs to completion and exits normally, database connections are properly closed in all cases.
