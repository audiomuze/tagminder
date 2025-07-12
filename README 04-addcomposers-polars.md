# addcomposers-polars.py

## Description

Adds composer metadata to tracks based on artist and title matching against the composer occurrence with the most instances in your collection.

This script identifies tracks in your music library that are missing composer information and attempts to fill in those gaps by finding other versions of the same track (matched by normalized title and artist) that do have composer metadata. It uses a majority-vote approach to determine the most likely composer for each track.

The script is part of the tagminder toolkit and serves as the de-facto method for adding composer tags to tracks where the same composition appears elsewhere in your library with existing composer metadata.

## Usage

```bash
python addcomposers-polars.py
```

or with uv:

```bash
uv run addcomposers-polars.py
```

## Command Line Options

This script currently accepts no command line arguments or options. All configuration is handled through constants defined in the script.

## Configuration

The script uses the following hardcoded configuration:

- **Database Path**: `/tmp/amg/dbtemplate.db`
- **Log Level**: INFO level logging to console

## How It Works

1. **Data Loading**: Retrieves all tracks from the `alib` table including title, composer, artist, and albumartist fields
2. **Normalization**: 
   - Normalizes track titles by removing live performance indicators and special characters
   - Splits artist and albumartist fields on common separators (`;`, `,`, `/`, `&`, `\\`, ` and `)
3. **Composer Inference**: 
   - Groups tracks by normalized title and individual artist names
   - For each group, identifies the most frequently occurring composer
   - Creates a lookup table of inferred composers
4. **Propagation**: 
   - Applies inferred composer data to tracks missing composer information
   - Updates the `sqlmodded` counter for modified records
5. **Change Logging**: 
   - Records all changes in a `changelog` table with timestamps
   - Commits changes to the database

## Database Requirements

The script expects a SQLite database with:

- **`alib` table** containing columns: `rowid`, `sqlmodded`, `title`, `composer`, `artist`, `albumartist`
- **`changelog` table** (created automatically if missing) for tracking modifications

## Output

The script provides console logging showing:
- Number of rows loaded from the database
- Number of composer groups inferred via majority vote
- Number of composer fields updated and logged
- Connection status messages

## Dependencies

- `polars` - For high-performance data processing
- `sqlite3` - For database operations (Python standard library)
- `logging` - For console output (Python standard library)
- `re` - For text normalization (Python standard library)
- `datetime` - For timestamp generation (Python standard library)

## Author

audiomuze

## Created

2025-04-21