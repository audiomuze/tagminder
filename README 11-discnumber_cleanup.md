# discnumber-cleanup-polars.

## DESCRIPTION

A high-performance music library maintenance script that intelligently cleans up disc number metadata based on directory path analysis. This script is part of the tagminder suite and uses Polars for optimized vectorized operations.

The script performs three main cleanup operations:

1. **Removes empty disc data**: Filters out directory paths where all disc number entries are None/empty
2. **Filters disc/CD patterns**: Excludes paths matching common disc naming patterns (cdxx, discxx, cd xx, disc xx)
3. **Clears redundant disc numbers**: Sets disc numbers to None when all entries within a directory path are identical

All changes are tracked in a changelog table with full audit trail including timestamps and old/new values.

## SYNOPSIS

```bash
python discnumber-cleanup-polars.py
```

```bash
uv run discnumber-cleanup-polars.py
```

## OPTIONS

This script currently accepts no command line options. All configuration is handled through internal constants:

- **Database Path**: `/tmp/amg/dbtemplate.db` (hardcoded)
- **Target Columns**: `discnumber` (hardcoded)
- **Logging Level**: `INFO` (hardcoded)

## OPERATION DETAILS

### Input Data
- Reads from `alib` table in SQLite database
- Processes columns: `rowid`, `__dirpath`, `discnumber`, `sqlmodded`

### Processing Steps

1. **Data Filtering**
   - Removes directory paths containing no disc number data
   - Excludes paths matching regex patterns: `\bcd\s*\d+\b`, `\bdisc\s*\d+\b`

2. **Disc Number Normalization**
   - Identifies paths where all non-null disc numbers are identical
   - Sets these redundant disc numbers to `None`

3. **Change Tracking**
   - Increments `sqlmodded` counter for modified rows
   - Logs all changes to `changelog` table with full metadata

### Output
- Updates `alib` table with cleaned disc number data
- Creates audit trail in `changelog` table
- Provides detailed logging of all operations

## EXAMPLES

### Basic Usage
```bash
# Run with Python
python discnumber-cleanup-polars.py

# Run with uv
uv run discnumber-cleanup-polars.py
```

### Expected Log Output
```
2025-01-15 10:30:45 - INFO - Starting disc number cleanup process...
2025-01-15 10:30:45 - INFO - Connecting to database...
2025-01-15 10:30:45 - INFO - Loaded 15847 rows
2025-01-15 10:30:45 - INFO - Step 1: Filtering paths with no disc number data...
2025-01-15 10:30:45 - INFO - Removed 3421 rows from paths with no disc number data
2025-01-15 10:30:45 - INFO - Step 2: Filtering paths matching disc/cd patterns...
2025-01-15 10:30:45 - INFO - Removed 892 rows from paths matching disc/cd patterns
2025-01-15 10:30:45 - INFO - Step 3: Processing paths with identical disc numbers...
2025-01-15 10:30:45 - INFO - Found 156 paths with identical disc numbers that will be cleared
2025-01-15 10:30:45 - INFO - Detected 1247 rows with changes
2025-01-15 10:30:45 - INFO - Writing 1247 changed rows to database
2025-01-15 10:30:46 - INFO - Updated 1247 rows and logged all changes.
```

## FILES

- **Input**: `/tmp/amg/dbtemplate.db` - SQLite database containing music library metadata
- **Tables Modified**: 
  - `alib` - Main library table (disc numbers updated)
  - `changelog` - Audit trail table (change history logged)

## DEPENDENCIES

- `sqlite3` - Database connectivity
- `polars` - High-performance data processing
- `logging` - Operation logging
- `re` - Regular expression pattern matching
- `datetime` - Timestamp generation

## PERFORMANCE

Optimized for large datasets using Polars vectorized operations:
- Efficient groupby operations for path analysis
- Vectorized pattern matching
- Minimal database I/O with batch updates
- Memory-efficient processing of large music libraries

## AUTHOR

audiomuze

## SEE ALSO

Part of the tagminder music library management suite.