# Tagminder Python Script Audit (Docstrings)

Generated: 2026-04-15T13:15:14.552949+00:00

Scope:
- Repo-owned Python files in the workspace root (excluding `.venv/`, `.git/`, `__pycache__/`)
- `audioinf/` is treated as vendored/third-party-like code and excluded from docstring policy checks

## Policy
- Required docstring fields: `Purpose:`, `Tagminder`, `Author:`, `Last updated:`, `SQLite tables referenced:`
- Docstrings must not embed their own filename (because filenames may change)

## Results
- OK: 30/30 non-`audioinf/` Python files comply

## Files (non-audioinf)
- 01-null-unauthorised-tags.py — SQLite tables: alib, changelog
- 02-clean-text-fields.py — SQLite tables: alib, changelog
- 03-normalize-title-artist-features.py — SQLite tables: alib, contributors_unified_disambiguated, changelog
- 04-merge-songwriter-fields-into-composer.py — SQLite tables: alib, changelog
- 05-infer-composers-from-library.py — SQLite tables: alib, changelog
- 06-normalize-contributors.py — SQLite tables: alib, contributors_unified_disambiguated, contributors_unified_namesakes, changelog
- 07-apply-vetted-contributor-mappings.py — SQLite tables: alib, _REF_vetted_contributors, changelog, sqlite_master (introspection)
- 08-normalize-subtitles.py — SQLite tables: alib, changelog
- 09-normalize-live-markers.py — SQLite tables: alib, changelog
- 10-normalize-genres-and-styles.py — SQLite tables: alib, _REF_genres, changelog
- 11-enrich-genres-using-artist-genre-norms.py — SQLite tables: alib, contributors_unified_disambiguated, changelog
- 12-detect-compilations.py — SQLite tables: alib, changelog
- 13-cleanup-discnumber.py — SQLite tables: alib, changelog
- 14-normalize-releasetype.py — SQLite tables: alib, changelog
- 15-contributor-similarity-analysis.py — SQLite tables: alib, _REF_vetted_contributors, _REF_contributors_workspace, sqlite_master (introspection)
- 16-populate-track-uuid.py — SQLite tables: alib, changelog
- 17-dedupe-columns.py — SQLite tables: alib, changelog
- 18-populate-musicbrainz-ids.py — SQLite tables: alib, contributors_unified_disambiguated, contributors_unified_namesakes, _TMP_alib_updates, changelog, sqlite_master (introspection)
- 98-create-export-db.py — SQLite tables: alib, changelog, sqlite_master (introspection)
- 98-rename-files-and-folders.py — SQLite tables: alib, changelog
- 99-reset-sqlmodded.py — SQLite tables: alib
- tags2db.py — SQLite tables: alib, sqlite_master (introspection), pragma_table_info (introspection)
- tm.py — SQLite tables: None
- tm_changes.py — SQLite tables: changelog
- tm_cli.py — SQLite tables: None
- tm_config.py — SQLite tables: None
- tm_db.py — SQLite tables: alib, changelog, sqlite_master
- tm_polars.py — SQLite tables: None
- tm_polars_db.py — SQLite tables: (varies by caller query)
- tm_run.py — SQLite tables: changelog (optional; schema ensure), sqlite_master (introspection; optional)
- tm_tui.py — SQLite tables: None

## audioinf package
- Files under `audioinf/` were not modified by this audit.
