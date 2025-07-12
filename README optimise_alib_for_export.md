# 00-optimise-alib-for-export.py - Help Documentation

## Description

This script optimizes the alib SQLite table by dropping columns not needed for export. It retains all system columns (prefixed with `__`) and only specified tag columns, which reduces memory usage and improves export performance (only writing the exported columns to file tags, whilst leaving others intact).

The script is particularly useful when you have a large music library database with many tag columns, but only need to export a subset of those tags. By creating an optimized table with only the necessary columns, you can significantly reduce database size and improve export performance.

## Key Features

- **Selective Column Retention**: Keeps all system columns (prefixed with `__`) and user-specified tag columns
- **Auto-detection**: Can automatically detect which columns to keep based on changelog table data
- **Validation**: Validates that specified tags exist in the database before proceeding
- **Safe Operations**: Uses database transactions to ensure data integrity
- **Space Optimization**: Optional vacuum operation to reclaim disk space
- **Dry Run Mode**: Preview changes without making modifications
- **Comprehensive Logging**: Detailed progress reporting and error handling

## Usage Examples

```bash
# Keep specific tag columns
python optimise_alib_columns.py --db /path/to/music.sqlite --keep title artist album genre

# Keep tags from a file and vacuum the database
python optimise_alib_columns.py --db /path/to/music.sqlite --keep-file tags_to_keep.txt --vacuum

# Auto-detect columns from changelog (no --keep or --keep-file needed)
python optimise_alib_columns.py --db /path/to/music.sqlite --vacuum

# Preview changes without making them
python optimise_alib_columns.py --db /path/to/music.sqlite --keep title artist --dry-run

# Optimize a different table with verbose logging
python optimise_alib_columns.py --db /path/to/music.sqlite --table my_table --keep title artist --log DEBUG
```

## Command Line Options

### Required Arguments

**`--db PATH`**
- Path to the SQLite database containing the alib table
- Must be an existing, readable SQLite database file

### Tag Selection (Choose One)

**`--keep TAG [TAG ...]`**
- Space-separated list of tag column names to keep
- Example: `--keep title artist album genre year`
- Cannot be used with `--keep-file`

**`--keep-file PATH`**
- Path to text file containing tag names to keep (one per line)
- Lines starting with `#` are treated as comments and ignored
- Cannot be used with `--keep`
- Example file format:
  ```
  # Essential tags
  title
  artist
  album
  genre
  ```

**Auto-detection (Default)**
- If neither `--keep` nor `--keep-file` is specified, the script will automatically detect which columns to keep based on the changelog table
- Requires a changelog table with data to be present in the database

### Optional Arguments

**`--table NAME`**
- Name of the table to optimize (default: `alib`)
- Use this if your music library table has a different name

**`--dry-run`**
- Preview mode - shows what would be done without making any changes
- Useful for testing and verification before running the actual optimization
- Displays columns that would be dropped and final table structure

**`--vacuum`**
- Vacuum the database after optimization to reclaim disk space
- This operation can take significant time for large databases
- Provides actual space savings statistics
- Recommended for maximum space efficiency

**`--log LEVEL`**
- Set logging verbosity level
- Choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`
- Default: `INFO`
- Use `DEBUG` for detailed operation information

## How It Works

1. **Validation**: Checks database exists and specified tags are valid
2. **Analysis**: Determines which columns to keep (system + specified tags)
3. **Optimization**: Creates new table with only required columns
4. **Data Migration**: Copies data from original table to optimized table
5. **Replacement**: Replaces original table with optimized version
6. **Cleanup**: Optionally vacuums database to reclaim space

## System Columns

The script automatically preserves all system columns (those prefixed with `__`), which typically include:
- `__path` - File path (primary key)
- `__size` - File size
- `__mtime` - Modification time
- `__bitrate` - Audio bitrate
- And other system metadata

## Performance Benefits

- **Reduced Memory Usage**: Smaller tables use less RAM during operations
- **Faster Exports**: Fewer columns mean faster data transfer
- **Improved Query Performance**: Less data to scan and process
- **Space Savings**: Significant disk space reduction when vacuumed

## Safety Features

- **Transaction Safety**: All operations are wrapped in database transactions
- **Data Validation**: Verifies tag existence before proceeding
- **Backup Preservation**: Original table is renamed (not deleted) as `{table}_all_records`
- **Error Handling**: Comprehensive error checking and rollback on failure
- **Dry Run Mode**: Test operations without making changes

## Exit Codes

- `0`: Success
- `1`: Error (invalid arguments, database errors, etc.)
- `130`: User interruption (Ctrl+C)

## Notes

- The script creates indexes on the `__path` column for optimal performance
- If a changelog table exists, only records referenced in the changelog are copied
- Original table is preserved as `{table_name}_all_records` for safety
- Space reclamation requires the `--vacuum` option
- Foreign key constraints are temporarily disabled during table operations
