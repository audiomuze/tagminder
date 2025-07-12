# 02-cleantext-polars.py

## Description

A database text cleaning utility that processes all text columns in the 'alib' table of an SQLite database to remove spurious characters and formatting issues. Part of the tagminder project.

## What it does

The script performs the following operations:

- **Loads all text columns** from the 'alib' table (excluding specific columns)
- **Cleans each field** by:
  - Removing carriage returns (CR) and line feeds (LF)  
  - Converting empty strings to NULL values
  - Normalizing standalone apostrophes (`'` and `́`) to standard apostrophe (`'`)
- **Increments `sqlmodded` counter** for each column that gets modified
- **Writes only changed rows** back to the database
- **Logs all changes** to a 'changelog' table for audit purposes

## Usage

```bash
python cleantext-polars.py
```

or

```bash
uv run cleantext-polars.py
```

## Configuration

The script uses hardcoded configuration values:

- **Database path**: `/tmp/amg/dbtemplate.db`
- **Target table**: `alib`
- **Excluded columns**: `discogs_artist_url`, `lyrics`, `review`, `sqlmodded`, `unsyncedlyrics`
- **Auto-excluded**: Any column starting with `__`

## Output

The script provides logging output including:

- Number of usable columns discovered
- Total rows loaded
- Number of rows with detected changes
- Sample of changed row IDs
- Confirmation of successful updates

## Database Changes

### Updates to 'alib' table
- Modified text columns with cleaned values
- Incremented `sqlmodded` counter for changed rows

### Creates 'changelog' table
Records all changes with:
- `alib_rowid` - Reference to modified row
- `column` - Column name that was changed
- `old_value` - Original value
- `new_value` - Cleaned value  
- `timestamp` - UTC timestamp of change
- `script` - Name of script that made the change

## Requirements

- Python 3.8+
- polars
- sqlite3 (standard library)

## Author

audiomuze

## Created

2025-04-13