# 07-subtitles-polars.py

## Description

A database subtitle normalization tool that processes and standardizes subtitle entries in an SQLite database. The script identifies subtitle entries containing bracketed content (e.g., `[Live]`, `(Studio)`, `{HD}`) and normalizes them by:

- Removing duplicate entries within the same subtitle
- Applying consistent capitalization rules
- Handling special cases like redundant "Live" tags
- Standardizing bracket formats to square brackets `[...]`
- Joining multiple entries with a configurable delimiter

All changes are logged to a changelog table and the modification counter (`sqlmodded`) is incremented for tracking purposes.

## Usage

```bash
python subtitles-polars.py
```

## Command Line Options

**Note**: This script currently accepts no command line arguments. All configuration is handled through constants defined at the top of the script.

## Configuration

The script uses the following hardcoded configuration values:

- **Database Path**: `/tmp/amg/dbtemplate.db`
- **Delimiter**: `\\` (backslash separator for multiple subtitle entries)
- **Target Table**: `alib`
- **Required Columns**: `rowid`, `subtitle`, `sqlmodded`

## Database Requirements

### Input Table Structure
The script expects an `alib` table with the following columns:
- `rowid` (INTEGER) - Primary key identifier
- `subtitle` (TEXT) - Subtitle content to be normalized
- `sqlmodded` (INTEGER) - Modification counter (defaults to 0 if NULL)

### Output Changes
- **alib table**: Updated `subtitle` and `sqlmodded` columns
- **changelog table**: Created automatically to track all modifications with:
  - `alib_rowid` - Reference to modified row
  - `column` - Column name that was changed
  - `old_value` - Original value
  - `new_value` - Updated value
  - `timestamp` - ISO format timestamp of change
  - `script` - Name of script that made the change

## Processing Logic

### Normalization Rules
1. **Bracket Extraction**: Finds content within `()`, `[]`, `{}`, or `<>` brackets
2. **Deduplication**: Removes duplicate entries (case-insensitive comparison)
3. **Live Tag Handling**: Removes standalone `[Live]` tags when more descriptive live-related tags exist
4. **Capitalization**:
   - Capitalizes first word of each entry (unless already all uppercase)
   - Capitalizes letters following periods in abbreviations
5. **Format Standardization**: Converts all brackets to square bracket format `[...]`
6. **Delimiter Joining**: Combines multiple entries using the configured delimiter

### Example Transformations
- `(live) [HD Live]` → `[HD Live]`
- `{studio} (Studio)` → `[Studio]`
- `[live] (bbc news)` → `[Live]\\[BBC News]`

## Logging

The script provides detailed logging at INFO level including:
- Database connection status
- Number of rows processed
- Number of changes detected
- Sample of changed row IDs
- Update completion status

## Error Handling

- Database connection errors are handled with proper cleanup
- Transaction rollback on update failures
- Graceful handling of missing or NULL subtitle values

## Performance Notes

- Uses Polars for efficient DataFrame operations
- Processes data in memory before batch database updates
- Single transaction for all updates to ensure data consistency
- Vectorized operations where possible for optimal performance
