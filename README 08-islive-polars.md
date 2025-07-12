# islive-polars.py

## Description

Normalize 'live' markers from title into subtitle and set the live flag in a music library database. This script is part of the tagminder suite and uses Polars for optimized vectorized operations.

## What it does

The script processes music library records to standardize live performance markers:

1. **Removes live markers from titles and albums** - Strips patterns like `(live)`, `[live]`, `{live}`, `<live>`, or `- live` from the end of title and album fields
2. **Adds standardized live marker to subtitle** - Appends `[Live]` to subtitle if no live marker is already present
3. **Sets live flag** - Updates the `live` field to `'1'` for affected records
4. **Tracks modifications** - Increments `sqlmodded` counter and logs all changes to a changelog table
5. **Processes only changed records** - Only writes back rows that were actually modified

## Usage

```bash
python islive-polars.py
```

or with uv:

```bash
uv run islive-polars.py
```

## Command Line Options

This script currently accepts no command line arguments. All configuration is handled through internal constants.

## Configuration

The script uses these hardcoded configuration values:

- **Database Path**: `/tmp/amg/dbtemplate.db`
- **Target Columns**: `title`, `subtitle`, `album`, `live`
- **Live Pattern Detection**: Case-insensitive regex matching bracketed live markers at end of strings
- **Subtitle Format**: Standardized `[Live]` format

## Database Requirements

The script expects:

- SQLite database with an `alib` table
- Required columns: `rowid`, `title`, `subtitle`, `album`, `live`, `sqlmodded`
- Creates a `changelog` table if it doesn't exist for tracking modifications

## Output

The script logs its progress and:

- Reports number of rows loaded
- Shows count of rows with detected changes
- Logs sample rowids of changed records
- Confirms number of rows updated
- Creates detailed changelog entries for all modifications

## Examples

### Typical transformations:

**Title normalization:**
- `"Song Title (Live)"` → `"Song Title"`
- `"Track Name [live]"` → `"Track Name"`
- `"Performance - live"` → `"Performance"`

**Subtitle updates:**
- `null` → `"[Live]"`
- `"Studio Version"` → `"Studio Version [Live]"`
- `"Live Recording"` → `"Live Recording"` (no change - already contains 'live')

**Live flag:**
- `live` field set to `'1'` for all processed records

## Notes

- Uses Polars for high-performance vectorized operations
- Only processes and writes back rows that actually changed
- Maintains complete audit trail in changelog table
- Case-insensitive pattern matching for live markers
- Preserves existing live markers in subtitles to avoid duplication
