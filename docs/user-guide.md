# Tagminder User Guide

This guide is the main operating document for day-to-day Tagminder use.

Use this with [README.md](../README.md):

- Read [README.md](../README.md) once for orientation.
- Use this guide for actual workflow execution.

## Guide Navigation

Choose your path based on what you are doing now:

1. First-ever run: [First Run Workflow](#first-run-workflow)
2. Regular refresh run: [Routine Workflow](#routine-workflow)
3. MusicBrainz enrichment setup/run: [Enrichment Workflow](#enrichment-workflow)
4. Export tags to files: [Export Workflow](#export-workflow)
5. Problems or unexpected results: [Troubleshooting Workflow](#troubleshooting-workflow)

Related specialist docs:

- Master-data setup: [scripts/mdm/harvest/README.md](../scripts/mdm/harvest/README.md)
- Contributor emit details: [docs/emit_contributors.README.md](emit_contributors.README.md)

## Workflow Model

Tagminder is intentionally staged:

1. Import file metadata into staging DB (`alib`)
2. Run cleanup/enrichment scripts in DB
3. Review changes in reports/changelog
4. Build export DB
5. Export back to files only when satisfied

Safety rule:

- No file tags are written until you run export.

## First Run Workflow

### Goal

Get a safe baseline run completed on a test subset.

### Steps

1. Configure paths in [tagminder.toml](../tagminder.toml): `[db].path`, `[master_data].path`, `[export].db_path`
2. Install runtime dependencies:

```bash
uv python install 3.14
uv sync
```

3. Import test subset:

```bash
uv run python scripts/ingest/tags2db.py import /music
```

4. Run a minimal cleanup set:

```bash
uv run python scripts/pipeline/02-clean-text-fields.py
uv run python scripts/pipeline/03-normalize-title-artist-features.py
uv run python scripts/pipeline/13-cleanup-discnumber.py
```

5. Generate baseline reports:

```bash
uv run python scripts/reports/92-report-library-health.py
uv run python scripts/reports/93-report-track-sequence-anomalies-by-album.py
```

6. If results are acceptable, build export DB:

```bash
uv run python scripts/export/98-create-export-db.py
```

### Success checks

- `alib` has rows
- `changelog` shows expected field changes
- report tables (`_INF_*`) are populated when issues exist
- no direct file mutation has happened yet

## Routine Workflow

### Goal

Refresh library metadata and apply repeatable normalization safely.

### Steps

1. Import with incremental mode:

```bash
uv run python scripts/ingest/tags2db.py import --modified-files /music
```

or

```bash
uv run python scripts/ingest/tags2db.py import --new-files /music
```

2. Run chosen pipeline steps (typically in numeric order for enabled steps).
3. Run health/snapshot reports.
4. Inspect `changelog` and report outputs.
5. Build export DB and export only when ready.

## Enrichment Workflow

### Goal

Enable contributor/MBID/work-level enrichment using master data.

### Steps

1. Follow [scripts/mdm/harvest/README.md](../scripts/mdm/harvest/README.md) to build/refresh master data.
2. Run readiness check:

```bash
uv run python scripts/mdm/harvest/check_master_data_readiness.py --strict
```

3. Run enrichment pipeline steps (for example 06, 11, 18, 22 as appropriate).
4. Review candidate/review tables and logs before export.

### Work inference tables

- `work_inference_candidates`: draft suggestions to review.
- `user_vetted_works`: your approved canonical work associations used for consistency across future runs.

## Export Workflow

### Goal

Apply approved metadata updates back to audio files.

### Steps

1. Build export DB:

```bash
uv run python scripts/export/98-create-export-db.py --db /tmp/tagminder-staging.db
```

2. Export to files:

```bash
uv run python scripts/ingest/tags2db.py export --db /tmp/tagminder-export.db /music
```

3. Optional reset after completion:

```bash
uv run python scripts/export/99-reset-sqlmodded.py --db /tmp/tagminder-staging.db
```

### Export behavior note

- Export writes one file at a time intentionally (sequential file writes) for predictability and safer file-level error handling.

## Import Performance Workflow

### Single-drive import

- Start with moderate workers and chunk size.
- Increase workers first only if storage and system remain responsive.

### Multi-drive import

You can ingest multiple directories in one run:

```bash
uv run python scripts/ingest/tags2db.py import /mnt/music_drive_1 /mnt/music_drive_2
```

Important:

- `--workers` is per drive.
- Effective parallelism scales with active drives.
- Tagminder does not verify physical-device separation; multiple sources on the same physical device can increase thrashing and slow ingestion.

## Troubleshooting Workflow

Start here when behavior is unexpected.

1. Verify DB path in [tagminder.toml](../tagminder.toml).
2. Confirm `alib` exists and has rows.
3. Confirm command targeted intended DB.
4. For enrichment issues, run strict readiness check.
5. Re-run on a smaller subset to isolate behavior.

Also see quick troubleshooting in [README.md](../README.md#troubleshooting-common-first-run-issues).

## Document Cross-Links

- Project overview and quickstart: [README.md](../README.md)
- Master-data harvesting: [scripts/mdm/harvest/README.md](../scripts/mdm/harvest/README.md)
- Contributor emit details: [docs/emit_contributors.README.md](emit_contributors.README.md)
- Script audit status: [docs/SCRIPT_AUDIT.md](SCRIPT_AUDIT.md)
- Backlog and future considerations: [docs/backlog.md](backlog.md)
