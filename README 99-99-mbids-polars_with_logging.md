# mbids-polars. - MusicBrainz ID Processing Script

## Overview

This script processes all records in the `alib` database table and updates MusicBrainz IDs (MBIDs) for music contributors. It ensures that contributor tags and their corresponding MBID tags are properly synchronized and ordered.

## Purpose

The script updates the following MBID relationships:
- `artist` → `musicbrainz_artistid`
- `albumartist` → `musicbrainz_albumartistid`
- `composer` → `musicbrainz_composerid`
- `engineer` → `musicbrainz_engineerid`
- `producer` → `musicbrainz_producerid`

### Key Features

- **Adds missing MBIDs**: Where contributor names exist but MBIDs are missing
- **Maintains order**: Ensures MBIDs appear in the same order as contributor names
- **Handles missing matches**: Inserts empty strings (`''`) for contributors without MBIDs in MusicBrainz
- **Comprehensive logging**: Tracks all changes with detailed statistics
- **Change tracking**: Logs all modifications to a `changelog` table
- **High performance**: Uses Polars for vectorized operations and efficient data processing

## Usage

```bash
python mbids-polars.py
```

or with uv:

```bash
uv run mbids-polars.py
```

## Command Line Options

Currently, the script does not accept command line arguments. Configuration is handled through the script's internal variables:

### Internal Configuration

The following parameters can be modified in the script:

- **Database Path**: Hardcoded to `/tmp/amg/dbtemplate.db`
- **Chunking Mode**: Set via `use_chunking` parameter in `main()` function
  - `use_chunking=False`: Process entire database at once (default, recommended for Polars)
  - `use_chunking=True`: Process in chunks of 50,000 rows (useful for memory-constrained systems)
- **Batch Size**: Updates are written in batches of 1,000-5,000 records
- **Logging Level**: Set to `INFO` by default

## Prerequisites

### Database Requirements

The script expects a SQLite database with the following tables:

1. **`alib`** - Main music library table containing:
   - `rowid`: Primary key
   - `artist`, `albumartist`, `composer`, `engineer`, `producer`: Contributor fields
   - `musicbrainz_artistid`, `musicbrainz_albumartistid`, etc.: MBID fields
   - `sqlmodded`: Modification counter (optional)

2. **`_REF_mb_disambiguated`** - Reference table containing:
   - `entity`: Normalized contributor name
   - `mbid`: Corresponding MusicBrainz ID

### Python Dependencies

```
polars
sqlite3 (standard library)
logging (standard library)
unicodedata (standard library)
numpy
```

## Processing Logic

### Data Normalization

The script applies consistent normalization to all contributor names:
- Converts to lowercase
- Removes leading/trailing whitespace
- Removes double quotes
- Normalizes Unicode characters (diacritics)
- Normalizes internal whitespace

### MBID Matching

1. Splits multi-value contributor fields using `\\\\` delimiter
2. Normalizes each contributor name
3. Looks up corresponding MBID in reference table
4. Joins MBIDs back with `\\\\` delimiter
5. Inserts empty string (`''`) for unmatched contributors

### Change Tracking

The script distinguishes between:
- **Additions**: New MBIDs added to previously empty fields
- **Corrections**: Existing MBIDs updated with different values

All changes are logged to the `changelog` table with timestamps.

## Output

### Statistics Display

The script provides comprehensive statistics including:
- Total number of changes made
- Breakdown of additions vs corrections
- Per-field statistics for each contributor type

### Changelog Table

A `changelog` table is created/updated with:
- `alib_rowid`: ID of the modified record
- `column`: Name of the column changed
- `old_value`: Previous value
- `new_value`: New value
- `timestamp`: When the change was made
- `script`: Name of the script that made the change

## Performance Notes

- **Vectorized Operations**: Uses Polars for high-performance data processing
- **Memory Efficient**: Optimized for large datasets
- **Batch Processing**: Database updates are batched for efficiency
- **Transaction Safety**: All operations are wrapped in database transactions

## Error Handling

- Comprehensive error logging
- Automatic transaction rollback on errors
- Type validation for database fields
- Graceful handling of missing or malformed data

## Part of tagminder Suite

This script is part of the tagminder music metadata management system and serves as the de-facto method for ensuring contributors and their associated MBIDs are accurately recorded throughout your music collection.

---

**Author**: audiomuze  
**Created**: 2025-04-18  
**Updated**: 2025-06-01