# Genre Cleanup Script Help

## NAME
`09-fix-genres-polars-optimised-handles-nulls.py` - Optimized genre and style tag cleanup for music library databases

## SYNOPSIS
```bash
python 09-fix-genres-polars-optimised-handles-nulls.py
```

## DESCRIPTION
This script performs intelligent cleanup and standardization of genre and style tags in a music library database. It uses fuzzy string matching with pre-filtering optimizations to efficiently process large datasets while maintaining data integrity.

### What the script does:
1. **Connects to SQLite database** at `/tmp/amg/dbtemplate.db`
2. **Loads valid genre tags** from the `_REF_genres` reference table
3. **Extracts all unique tags** from both `genre` and `style` columns in the `alib` table
4. **Applies intelligent pre-filtering** using hard-coded replacements and exact matches
5. **Performs fuzzy string matching** using string_grouper for remaining unmatched tags
6. **Processes database records** in optimized chunks using vectorized operations
7. **Updates genre and style fields** with cleaned/standardized values
8. **Maintains detailed changelog** of all changes made
9. **Handles null values** properly throughout the process

### Key Features:
- **Optimized performance**: Uses intelligent pre-filtering to minimize expensive fuzzy matching operations
- **Caching system**: Caches fuzzy matching results to avoid recomputation
- **Vectorized operations**: Uses Polars for high-performance data processing
- **Null-safe**: Properly handles NULL values in database fields
- **Change tracking**: Maintains comprehensive changelog of all modifications
- **Memory efficient**: Processes large datasets in configurable chunks
- **SQLite optimized**: Applies database-specific performance optimizations

## COMMAND LINE OPTIONS
**Note**: This script currently does not accept command line arguments. All configuration is done via constants in the script.

## CONFIGURATION
The script uses the following hard-coded configuration constants:

- `DB_PATH`: `/tmp/amg/dbtemplate.db` - SQLite database path
- `ALIB_TABLE`: `alib` - Main table containing genre/style data
- `REF_VALIDATION_TABLE`: `_REF_genres` - Reference table with valid genre names
- `DELIMITER`: `\\\\` - Tag separator used in genre/style fields
- `CHUNK_SIZE`: `100000` - Number of records processed per batch
- `BATCH_SIZE`: `10000` - Number of updates per database transaction
- `CACHE_DIR`: `/tmp/amg_cache` - Directory for caching fuzzy match results
- `SIMILARITY_THRESHOLD`: `0.95` - Minimum similarity score for fuzzy matching
- `NUM_CORES`: `12` - Number of CPU cores for parallel processing

## HARD-CODED REPLACEMENTS
The script includes a comprehensive mapping of common genre variations to standardized forms:

- `acoustic` → `Singer/Songwriter`
- `alternative` → `Adult Alternative Pop/Rock`
- `metal` → `Heavy Metal`
- `rock` → `Pop/Rock`
- `world` → `International`
- And many more...

## OUTPUT
The script provides detailed logging output including:
- Number of valid tags loaded
- Unique tags found in database
- Pre-filtering statistics (exact matches vs fuzzy candidates)
- Processing progress with row counts
- Final statistics: processed rows, updated rows, execution time, and processing rate

## DATABASE REQUIREMENTS
- SQLite database with `alib` table containing:
  - `rowid` (INTEGER): Primary key
  - `__path` (TEXT): File path
  - `genre` (TEXT): Genre tags (nullable)
  - `style` (TEXT): Style tags (nullable)
  - `sqlmodded` (INTEGER): Modification counter
- Reference table `_REF_genres` with:
  - `genre_name` (TEXT): Valid genre names
- Automatically creates `changelog` table for tracking changes

## DEPENDENCIES
- `polars` - High-performance DataFrame library
- `string_grouper` - Fuzzy string matching
- `pandas` - Data processing (used by string_grouper)
- `sqlite3` - Database connectivity
- Standard library: `logging`, `datetime`, `os`, `pickle`, `hashlib`, `concurrent.futures`, `multiprocessing`, `re`, `functools`

## PERFORMANCE NOTES
- Optimized for systems with 64GB RAM and 12+ CPU cores
- Uses aggressive caching to avoid repeated fuzzy matching
- Intelligent pre-filtering reduces string_grouper workload by ~80-90%
- Vectorized operations with Polars provide significant performance gains
- Memory-mapped database access for faster I/O

## CHANGELOG TABLE
All changes are tracked in the `changelog` table with:
- `alib_rowid`: Reference to modified record
- `column`: Changed column name ('genre' or 'style')
- `old_value`: Original value (may be NULL)
- `new_value`: New value (may be NULL)
- `timestamp`: ISO timestamp of change
- `script`: Script name for attribution

## EXIT STATUS
- `0` - Success
- `1` - Error (with detailed logging)

## EXAMPLES
```bash
# Run the script (no options required)
python 09-fix-genres-polars-optimised-handles-nulls.py

# Example output:
# Processed: 1,234,567 rows
# Updated: 45,678 rows
# Time: 12.34 minutes
# Rate: 1,667.2 rows/sec
```

## NOTES
- The script is designed to be run multiple times safely
- Existing changes are tracked via the `sqlmodded` counter
- Cache files are stored in `/tmp/amg_cache` for reuse
- All database operations are atomic with proper rollback on errors