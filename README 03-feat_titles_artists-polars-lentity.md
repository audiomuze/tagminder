# 03-feat_titles_artists-polars-lentity.py

## NAME
03-feat_titles_artists-polars-lentity.py - Music library metadata processor for titles, artists, and featured performers

## SYNOPSIS
```bash
python 03-feat_titles_artists-polars-lentity.py
uv run 03-feat_titles_artists-polars-lentity.py
```

## DESCRIPTION
This script processes music library records from the 'alib' table to clean and standardize title, subtitle, artist, and live performance information. It performs intelligent metadata normalization by processing bracketed content in titles and standardizing artist name formatting.

### Key Processing Features:

**Artist Name Standardization:**
- Compares artist names against a disambiguation reference table (`_REF_mb_disambiguated`)
- Corrects case mismatches using canonical artist names
- Processes featured artist notation (feat., featuring, with, w/) and standardizes with `\\` delimiter

**Title Bracket Processing:**
- Extracts bracketed suffixes from titles `[content]`, `(content)`, `{content}`, `<content>`
- Intelligently categorizes bracketed content into:
  - **Featured Artists**: Moves to artist field with proper delimiter
  - **Live Performance**: Sets live flag and moves venue info to subtitle
  - **Subtitle Content**: Moves version info (remixes, remasters, demos, etc.) to subtitle field

**Recognized Patterns:**
- **Feature Prefixes**: `with`, `w/`, `feat`, `feat.`, `featuring`
- **Live Indicators**: `live` (followed by venue/event information)
- **Subtitle Prefixes**: `remix`, `rmx`, `remaster`, `remastered`, `demo`, `outtake`, `alt`, `alternate`, `alt.`, `mix`, `early mix`, `instrumental`, `bonus`, `radio`, `reprise`, `unplugged`, `acoustic`, `electric`, `akoesties`, `orchestral`, `piano`, `dj`
- **Trailing Matches**: `mix`, `session`, `demos`, `remaster`, `remastered`, `remix`

## DATABASE REQUIREMENTS
- SQLite database with `alib` table containing columns: `title`, `subtitle`, `artist`, `live`, `sqlmodded`
- Reference table `_REF_mb_disambiguated` with columns: `entity`, `lentity`
- Database path hardcoded as: `/tmp/amg/dbtemplate.db`

## COMMAND LINE OPTIONS
This script currently accepts no command line options. All configuration is handled through internal constants:

- `DB_PATH`: Database file location (hardcoded)
- `DELIM`: Artist delimiter character (`\\`)
- `SCRIPT_NAME`: Used for changelog tracking

## OUTPUT
The script processes the database in-place and provides logging output showing:
- Number of tracks loaded for processing
- Number of disambiguated artist references loaded
- Number of rows modified
- Sample of changed record IDs

## CHANGE TRACKING
All modifications are logged to a `changelog` table with:
- Record ID (`alib_rowid`)
- Modified column name
- Old and new values
- Timestamp (UTC)
- Script name for audit trail

## EXAMPLES

### Example Input Processing:
```
Title: "Song Name (feat. Artist Name)"
Artist: "Main Artist"
```

### Example Output:
```
Title: "Song Name"
Artist: "Main Artist\\Artist Name"
```

### Live Performance Processing:
```
Title: "Song Name (Live at Venue)"
Live: "0"
```

### Becomes:
```
Title: "Song Name"
Subtitle: "[Live at Venue]"
Live: "1"
```

## FILES
- Input: `/tmp/amg/dbtemplate.db` (SQLite database)
- Output: Modified database with changelog entries

## AUTHOR
audiomuze

## CREATED
2025-04-26

## SEE ALSO
This script is part of the tagminder suite for music library management.