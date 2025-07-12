# 05-contributors-polars-optimised.py

## Description

A high-performance music library contributor normalization script that connects to a SQLite database to intelligently clean and standardize contributor names across multiple fields. The script uses surgical filtering to process only contributors that need normalization, avoiding unnecessary changes to entries that are already properly disambiguated.

## Purpose

- **Database Integration**: Connects to SQLite music library database (`alib` table) containing track metadata
- **Reference Dictionary**: Loads canonical contributor names from disambiguation table (`_REF_mb_disambiguated`)
- **Surgical Processing**: Filters out contributors already in disambiguation dictionary to improve efficiency
- **Multi-field Normalization**: Processes 13 contributor fields with intelligent formatting
- **Change Tracking**: Maintains complete audit trail of all modifications with timestamps
- **Performance Optimized**: Uses Polars DataFrames and vectorized operations for maximum efficiency

## Features

### Contributor Fields Processed
- `artist`, `composer`, `arranger`, `lyricist`, `writer`
- `albumartist`, `ensemble`, `performer`
- `conductor`, `producer`, `engineer`, `mixer`, `remixer`

### Intelligent Text Processing
- **Multi-contributor Handling**: Splits entries on double backslash (`\\`) delimiter
- **Smart Title Casing**: Preserves capitalization patterns, handles Roman numerals, possessives, and hyphenated names
- **Secondary Delimiters**: Processes semicolons, forward slashes, and commas (except before suffixes like Jr., Sr.)
- **Dictionary Lookup**: Maps lowercase variants to canonical forms from reference data
- **Deduplication**: Removes duplicate contributor names within entries

### Database Operations
- **Transactional Updates**: Uses database transactions for data integrity
- **Selective Updates**: Only modifies rows that actually changed
- **Modification Tracking**: Increments `sqlmodded` counter for each field changed
- **Changelog**: Logs all changes with timestamps to `changelog` table
- **Rollback Safety**: Transaction-based operations ensure database consistency

## Usage

```bash
python 05-contributors-polars-optimised.py
```

## Command Line Options

**Note**: This script currently accepts no command line arguments. All configuration is handled through internal constants.

## Configuration

The script uses hardcoded configuration values that can be modified in the source code:

### Database Settings
- **DB_PATH**: `/tmp/amg/dbtemplate.db` - Path to SQLite database file
- **SCRIPT_NAME**: `contributors-polars.py` - Identifier used in changelog entries

### Processing Settings
- **DELIMITER**: `\\\\` - Main delimiter for joining multiple contributors
- **SPLIT_PATTERN**: Regex pattern for splitting contributors on various delimiters while preserving suffixes

## Database Schema Requirements

### Required Tables
- **alib**: Main tracks table with contributor columns and `sqlmodded` counter
- **_REF_mb_disambiguated**: Reference table with `entity` (canonical) and `lentity` (lowercase) columns

### Auto-created Tables
- **changelog**: Automatically created to track all modifications with columns:
  - `alib_rowid`: Row ID of modified track
  - `column`: Name of modified column
  - `old_value`: Original value before change
  - `new_value`: New value after normalization
  - `timestamp`: ISO timestamp of change
  - `script`: Script name that made the change

## Performance Features

### Surgical Filtering
- Creates boolean masks to identify contributors already in disambiguation dictionary
- Filters dataset to only process tracks needing normalization
- Prevents unnecessary processing of already-clean data

### Vectorized Operations
- Uses Polars DataFrame operations for maximum performance
- Batch processing of contributor normalization
- Efficient change detection using mask-aware comparison

### Memory Efficiency
- Processes data in optimized chunks
- Selective column updating to minimize database I/O
- Conditional normalization to avoid redundant processing

## Output and Logging

### Console Output
- Connection and processing status messages
- Count of tracks processed and updated
- Performance metrics and completion status
- Error messages with full stack traces

### Database Logging
- All changes logged to `changelog` table with timestamps
- Modification counter (`sqlmodded`) incremented for audit trails
- Transactional logging ensures data consistency

## Error Handling

- Database transaction rollback on errors
- Comprehensive exception logging with stack traces
- Graceful connection cleanup in finally blocks
- Input validation and null value handling

## Dependencies

### Required Python Packages
- `polars` - High-performance DataFrame library
- `sqlite3` - SQLite database connectivity (built-in)
- `logging` - Logging functionality (built-in)
- `datetime` - Timestamp handling (built-in)
- `re` - Regular expression processing (built-in)

### System Requirements
- Python 3.8+
- SQLite database with required schema
- Sufficient memory for DataFrame operations
- Write permissions to database file

## Exit Codes

- **0**: Successful completion
- **1**: Database connection error, processing error, or configuration issue

## Example Output

```
2025-07-12 10:30:15,123 - INFO - Connecting to database: /tmp/amg/dbtemplate.db
2025-07-12 10:30:15,124 - INFO - Fetching contributors dictionary...
2025-07-12 10:30:15,145 - INFO - Loaded 15,432 disambiguated contributor entries
2025-07-12 10:30:15,146 - INFO - Fetching tracks data...
2025-07-12 10:30:15,890 - INFO - Creating contributor masks for surgical filtering...
2025-07-12 10:30:16,012 - INFO - Processing 8,765 tracks to validate...
2025-07-12 10:30:16,234 - INFO - Performing selective contributor normalization...
2025-07-12 10:30:17,567 - INFO - Found 1,234 tracks with changes
2025-07-12 10:30:18,123 - INFO - Updated 1,234 rows and logged all changes.
2025-07-12 10:30:18,124 - INFO - Successfully updated 1,234 tracks in the database
2025-07-12 10:30:18,125 - INFO - Database connection closed
```

## Notes

- Part of the `taglib` music library management system
- Designed for music metadata cleanup and standardization
- Optimized for large-scale database operations
- Maintains complete audit trail of all modifications
- Safe to run multiple times (idempotent for already-processed data)
