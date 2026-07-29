# MusicBrainz Harvest Scripts (Master Data)

This folder contains one-shot harvest scripts that stream canonical MusicBrainz data from mbdump and load SQLite tables used by Tagminder master-data workflows.

These scripts are designed to be run directly with uv. They are not part of the numbered ETL pipeline shown by tm-cli list.

## Scope Contract

The harvesters in this folder are scoped to canonical entity and relationship metadata only.

Included:
- artist canonical entities
- artist-artist relationships
- work canonical entities (works, aliases, ISWCs, language, artist roles, work-work relationships — all in one pass)
- Wikidata music identity enrichment
- contributor disambiguation and export
- AllMusic lookup export for contributor MNIDs missing from `amg_artists`

Not part of canonical work-identification flow:
- recording canonical entities (MBID->recording-id bridge)
- recording-work relationships (recording->work bridge)

Excluded by contract:
- release-layer metadata and non-work serving entities (tracks, releases, release-groups)

Work-identification core:
- canonical_works_metadata

## Prerequisites

- Python environment synced via uv
- MusicBrainz dump archive available locally (mbdump.tar.bz2)
- writable SQLite output path(s)

## Configuration Contract

Configuration is loaded from [harvest_master_data.toml](../../../harvest_master_data.toml).

Resolution order used by these scripts:
1. current working directory
2. parent directories of the script path
3. fail with FileNotFoundError

Required keys:
- [musicbrainz].dump_archive
- [musicbrainz].contributors_db

## How To Run

Run from repo root (recommended):

```bash
uv run python scripts/mdm/harvest/harvest_mb_artists.py
uv run python scripts/mdm/harvest/harvest_mb_artist_relationships.py
uv run python scripts/mdm/harvest/harvest_mb_works.py
uv run python scripts/mdm/harvest/harvest_wikimedia.py
uv run python scripts/mdm/harvest/emit_contributors.py
uv run python scripts/mdm/harvest/amg_todo_list.py
uv run python scripts/mdm/harvest/check_master_data_readiness.py --strict
```

Recording bridge scripts (excluded from canonical run order):

```bash
uv run python scripts/mdm/harvest/harvest_mb_recordings.py
uv run python scripts/mdm/harvest/harvest_mb_recording_work_relationships.py
```

## Order Contract

Recommended order:
1. harvest_mb_artists.py
2. harvest_mb_artist_relationships.py
3. harvest_mb_works.py
4. harvest_wikimedia.py
5. emit_contributors.py
6. amg_todo_list.py
7. check_master_data_readiness.py --strict

Strict dependencies:
- Run harvest_mb_artists.py before harvest_mb_artist_relationships.py.
- Run harvest_mb_artists.py before harvest_mb_works.py if you want contributor display names in role columns.
- Run harvest_mb_artists.py and harvest_wikimedia.py before emit_contributors.py.

Recording bridge scripts are maintained in this folder but intentionally excluded from the canonical work-identification run order.

## Output Tables By Script

harvest_mb_artists.py
- musicbrainz_artists

harvest_mb_artist_relationships.py
- musicbrainz_artist_relationships
- musicbrainz_artist_relationship_attributes

harvest_mb_works.py  *(single-pass tar scan; consolidates works, aliases, ISWCs, language, roles, and work-work relationships)*
- canonical_works_metadata

harvest_wikimedia.py
- wikidata_music_identity

emit_contributors.py
- contributors_unified_disambiguated
- contributors_unified_namesakes
- wikimedia_data_quality_issues
- unmatched_wikidata_music_identity
- unmatched_amg_artists
- EXCEPTION_wikidata_music_identity_mbid_not_in_musicbrainz_artists_review

amg_todo_list.py
- amg_lookups.tsv (TSV export; overwritten on each run)

amg_todo_list.py output contract
- Purpose: backlog of AllMusic MNIDs referenced by contributor outputs but missing from `amg_artists`.
- Scans all three MNID sources from both `contributors_unified_disambiguated` and `contributors_unified_namesakes`:
	- `allmusic_mnid`
	- `musicbrainz_allmusic_mnid`
	- `wikimedia_allmusic_mnid`
- Emits one unified TSV with source identifiers:
	- `mnid_source`
	- `source_table`
	- `allmusic_mnid`
	- `allmusic_artist`
	- `allmusic_url`
	- `allmusic_genres_json`
	- `allmusic_styles_json`

harvest_mb_recordings.py  *(recording bridge — not in canonical run order)*
- musicbrainz_recordings

harvest_mb_recording_work_relationships.py  *(recording bridge — not in canonical run order)*
- musicbrainz_recording_work_relationships
- musicbrainz_recording_work_relationship_attributes
- musicbrainz_work_work_relationship_attributes

harvest_mb_recording_work_relationships.py
- musicbrainz_recording_work_relationships
- musicbrainz_recording_work_relationship_attributes

build_mb_work_lookup.py
- canonical_works_lookup

## Idempotency / Replace Behavior

Each harvester drops and recreates its own output tables on each run, then repopulates from source dump streams.

Implication:
- Re-running a script replaces that script's table set completely.
- If you need cross-table consistency for a domain, run both scripts for that domain in sequence.

## Quick Validation Queries

Use the database path from [harvest_master_data.toml](../../../harvest_master_data.toml) ([musicbrainz].contributors_db).

```bash
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_artists', COUNT(*) FROM musicbrainz_artists;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_artist_relationships', COUNT(*) FROM musicbrainz_artist_relationships;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_artist_relationship_attributes', COUNT(*) FROM musicbrainz_artist_relationship_attributes;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_works', COUNT(*) FROM musicbrainz_works;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_recordings', COUNT(*) FROM musicbrainz_recordings;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_aliases', COUNT(*) FROM musicbrainz_work_aliases;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_identifiers', COUNT(*) FROM musicbrainz_work_identifiers;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_artist_relationships', COUNT(*) FROM musicbrainz_work_artist_relationships;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_artist_relationship_attributes', COUNT(*) FROM musicbrainz_work_artist_relationship_attributes;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_work_relationships', COUNT(*) FROM musicbrainz_work_work_relationships;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_work_work_relationship_attributes', COUNT(*) FROM musicbrainz_work_work_relationship_attributes;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_recording_work_relationships', COUNT(*) FROM musicbrainz_recording_work_relationships;"
sqlite3 /tmp/amg/master-data.db "SELECT 'musicbrainz_recording_work_relationship_attributes', COUNT(*) FROM musicbrainz_recording_work_relationship_attributes;"
sqlite3 /tmp/amg/master-data.db "SELECT 'canonical_works_lookup', COUNT(*) FROM canonical_works_lookup;"
```

## Notes

- Relationship tables include a normalized attributes table plus a single per-edge attributes_json cache column.
- The design avoids duplicating equivalent JSON payloads across multiple tables.
- The flattened table `canonical_works_lookup` is a serving layer for fast vectorized enrichment joins; canonical truth remains in normalized tables.
