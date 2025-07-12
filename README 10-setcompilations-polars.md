# compilation-polars.

## Description

Detects and sets compilation flags for audio library entries based on directory path patterns. This script is part of the tagminder suite and uses Polars for optimized vectorized operations.

The script analyzes the `__dirpath` column in the `alib` table to identify compilation albums by examining the final directory segment in each path. It sets the `compilation` field to '1' for detected compilations and '0' for regular albums.

## Detection Logic

The script identifies compilation albums by checking if the last segment of the directory path starts with any of these patterns:

- `VA - ` (Various Artists prefix)
- `/VA/` (Various Artists directory)
- `Various Artists - `
- `/OST/` (Original Soundtrack directory)
- `OST - ` (Original Soundtrack prefix)

## Database Operations

- **Source Table**: `alib` 
- **Modified Columns**: `compilation`, `sqlmodded`
- **Changelog**: All changes are logged to the `changelog` table with timestamps
- **Transaction Safety**: Uses database transactions to ensure data integrity

## Usage

```bash
python compilation-polars.py
```

or with uv:

```bash
uv run compilation-polars.py
```

## Command Line Options

**Note**: This script currently does not accept command line arguments. All configuration is handled via constants defined in the script.

## Configuration

The following constants can be modified in the script source:

- `DB_PATH`: Database file path (default: `/tmp/amg/dbtemplate.db`)
- `COLUMNS`: Columns to track for changes (default: `["__dirpath", "compilation"]`)

## Output

The script provides detailed logging including:

- Number of rows processed
- Number of changes detected
- Sample compilation paths found
- Database update statistics
- Change logging confirmation

## Requirements

- Python 3.7+
- polars
- sqlite3 (built-in)
- logging (built-in)

## Examples

### Typical Output

```
2025-01-15 10:30:45 - INFO - Connecting to database...
2025-01-15 10:30:45 - INFO - Loaded 15847 rows
2025-01-15 10:30:45 - INFO - Found 8234 distinct directory paths
2025-01-15 10:30:45 - INFO - Found 1256 compilation paths
2025-01-15 10:30:45 - INFO - Detected 234 rows with changes
2025-01-15 10:30:45 - INFO - Writing 234 changed rows to database
2025-01-15 10:30:45 - INFO - Updated 234 rows and logged all changes.
```

### Sample Compilation Path Matches

The script will identify and flag these types of paths as compilations:

- `/music/Various Artists/VA - Best of 2024/`
- `/music/Compilations/OST - Movie Soundtrack/`
- `/music/Various Artists - Greatest Hits/`

## Performance

Optimized for speed using Polars vectorized expressions rather than row-by-row processing. Suitable for large audio libraries with hundreds of thousands of entries.

## Author

audiomuze  
Created: 2025-06-01