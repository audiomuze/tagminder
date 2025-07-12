# Release Type Normalizer

## Description

The Release Type Normalizer is a music library management tool that processes all records in your audio library database and normalizes `releasetype` tag values to ensure consistent categorization across your collection.

## What it does

1. **Normalizes existing release types** - Maps inconsistent release type values from music taggers (like Picard) to standardized user-preferred values using a comprehensive mapping table
2. **Assigns release types to untagged albums** - Automatically categorizes albums that lack release type information based on intelligent heuristics
3. **Logs all changes** - Maintains a complete audit trail of modifications in the changelog table
4. **Enables cleaner browsing** - Provides consistent release type categorization for improved navigation in music servers like Lyrion

## Usage

```bash
python 12-release_type_normalizer.py
```

or with uv:

```bash
uv run 12-release_type_normalizer.py
```

## Command Line Options

This script currently accepts no command line arguments. All configuration is handled through constants defined within the script.

## Configuration

The script uses hardcoded configuration values:

- **Database Path**: `/tmp/amg/dbtemplate.db`
- **Script Name**: `"release-type-normalizer.py"` (for changelog tracking)
- **Logging Level**: `INFO`

## Processing Logic

### Stage 1: Release Type Normalization
- Processes existing non-null release type values
- Applies two-stage vectorized mapping:
  - **Multi-value mappings**: Direct string replacement for complex release types containing delimiters
  - **Single-value mappings**: Simple lookup for basic release types
- Uses case-insensitive matching

### Stage 2: Assignment for Null Values
For albums without release type information, assigns categories based on:

1. **Singles**: ≤3 tracks per directory (excluding Classical/Jazz genres)
2. **Extended Play**: 4-6 tracks per directory (excluding Classical/Jazz genres)
3. **Soundtrack**: Directories containing '/OST' in the path
4. **Studio Album**: >6 tracks per directory OR Classical/Jazz genres (default for remaining)

## Supported Release Type Mappings

The script includes an extensive mapping table that normalizes various release type formats to standardized categories including:

- Studio Album
- Live Album
- Extended Play
- Single
- Soundtrack
- Greatest Hits & Anthologies
- Remix
- Demos, Soundboards & Bootlegs
- Box Set
- Mixtape/Street
- Various Artists Compilation

## Database Requirements

- SQLite database with `alib` table containing music metadata
- Required columns: `rowid`, `releasetype`, `__dirpath`, `genre`, `sqlmodded`
- Creates `changelog` table automatically if it doesn't exist

## Output

The script provides detailed logging including:
- Number of tracks processed
- Stage-by-stage processing statistics
- Assignment logic results
- Database update confirmations
- Error handling and reporting

## Dependencies

- `polars` - For high-performance data processing
- `sqlite3` - For database operations
- `logging` - For operation tracking
- `datetime` - For timestamp generation

## Notes

- All operations are performed using vectorized operations for optimal performance
- The script maintains referential integrity and provides complete audit trails
- No backup is created automatically - ensure you backup your database before running
- Part of the tagminder toolkit for music library management
