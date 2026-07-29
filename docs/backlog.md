# Tagminder — Backlog and Current State (Metadata Quality)

This file captures two things:
- what Tagminder already does today (implemented checks/capabilities)
- what is still outstanding (prioritized backlog)

Scope is intentionally **offline, dataset-only** checks using fields already retained (see `tagminder.toml` `[cleanup].keep_columns`).

Guiding principles:
- **No online lookups** required.
- Prefer checks that are **high-signal**, **actionable**, and **cheap to compute** over 800k+ rows.
- Where possible, phrase checks as: **Condition → Why it matters → Suggested remediation**.

---

## Current state snapshot (implemented)

### Existing reporting/quality checks in pipeline

- [x] **Track sequence anomaly reporting exists**
  - Implemented by `scripts/reports/93-report-track-sequence-anomalies-by-album.py`.
  - Covers missing/invalid/duplicate/gapped track numbering behavior at album/disc scope.

- [x] **Missing critical tags reporting exists**
  - Implemented by `scripts/reports/94-report-missing-critical-tags-by-album.py`.
  - Includes ReplayGain coverage checks used for album-level quality visibility.

- [x] **ReplayGain coverage consistency**
  - Implemented via `scripts/reports/94-report-missing-critical-tags-by-album.py` by including ReplayGain fields in critical-column checks.

- [x] **Duplicate detection reports exist**
  - Implemented by `scripts/reports/96-report-duplicate-tracks-all.py` and `scripts/reports/97-report-duplicate-albums.py`.
  - Supports duplicate discovery at both track and album levels.

### Contributor MBID policy hardening (step 18)

- [x] **Chunk/full semantics aligned (idempotency-first)**
  - Step 18 behavior now preserves deterministic outcomes regardless of processing mode.

- [x] **All synthetic outcomes persisted to decisions table**
  - Synthetic assignments are written to `_USR_disambiguation_decisions`.

- [x] **Synthetic rows not written to main disambiguated lookup table**
  - Policy enforced: do not insert synthetic IDs into `contributors_unified_disambiguated`.

- [x] **Decision provenance captured**
  - `_USR_disambiguation_decisions` includes `decision_source` (with migration support).

### Documentation and workflow clarity

- [x] **Primary user guide added and linked**
  - `docs/user-guide.md` created and cross-linked with README for navigation.

- [x] **Import performance guidance expanded**
  - Multi-drive concurrent ingest guidance documented, including anti-thrashing notes.

- [x] **Export behavior made explicit**
  - README documents that export is intentionally serialized (one file at a time).

### Operational reliability

- [x] **No persistent SQLite WAL sidecars after TUI exit**
  - Implemented in the TUI quit path (best-effort WAL checkpoint/truncate + switch back to `journal_mode=DELETE`).
  - Why: for users treating the staging DB as the metadata master/backup, leftover WAL sidecars are confusing and can feel like an unclean shutdown.

- [x] **Cross-database metadata sync by `track_uuid`**
  - Implemented via `scripts/export/98-sync-metadata-by-track-uuid.py`.
  - Applies selected metadata updates from source DB to target DB by `track_uuid`, updates only changed values, increments `__sqlmodded`, and writes target changelog entries.

- [x] **Synthetic MBID retirement workflow**
  - Implemented via `scripts/pipeline/23-retire-synthetic-mbids.py`.
  - Uses normalized contributor **name + context** matching (no name-only auto replacement), dry-run by default, and apply mode for confirmed replacements.
  - When applying, synthetic->real replacements are propagated across all `musicbrainz_*id` columns in `alib`, `__sqlmodded` is incremented, `changelog` is written, and `_USR_disambiguation_decisions` is updated.

---

## Outstanding backlog

### P0 — High impact / high signal (remaining)

### 1) Track/disc numbering contradictions
**Core identity fields:** `track`, `tracknumber`, `disc`, `discnumber`.

- [ ] **Track/disc numbering contradictions (beyond existing sequence anomaly report)**
  - Flag when `track` and `tracknumber` disagree materially.
  - Flag when `disc` is present but `discnumber` is missing (or vice versa), or non-numeric.
  - Why: breaks ordering, multi-disc grouping, and player UI expectations.

### 2) Date sanity + contradictions
**Applies to:** `year`, `date`, `releasedate`, `originaldate`, `originalyear`, `originalreleasedate`, `recording_date`, `recordingstartdate`, `recordingenddate`, `performancedate`.

- [ ] **Cross-field contradictions**
  - Examples: `year` conflicts with year extracted from `releasedate`; `originalyear` > `year`.
  - Why: breaks chronology, "original vs reissue" logic, and library browse.

- [ ] **Invalid or placeholder dates**
  - Flag clearly invalid values, impossible ranges, placeholders (e.g., `0000`, epoch-like placeholders if you use them).
  - Why: placeholders pollute sorting and can be mistaken for real data.

- [ ] **Recording range sanity**
  - Flag when `recordingenddate` < `recordingstartdate`, or when ranges exist but the main `recording_date` conflicts.

---

### P1 — Medium impact / high leverage (remaining)

### 3) Classical/work-structure completeness
**Applies to:** `work`, `movement`, `part`, `composer`, `conductor`, `orchestra`, `ensemble`, `performer`, `movementname` (if retained).

- [ ] **Work/movement coherence**
  - Flag when `movement` is present but `work` is missing.
  - Flag when `work` is present but `composer` is missing.

- [ ] **Movement field drift**
  - If both `movement` and `movementname` exist in your retained dataset, flag divergence (one present without the other, or conflicting values).
  - Why: classical browsing depends on stable work/movement semantics.

- [ ] **Ensemble/orchestra/performer overlap anomalies**
  - Flag when identical entities appear across multiple of `ensemble`, `orchestra`, `performer` in ways that violate your chosen tagging model.

### 4) Lyrics + explicit policy enforcement
**Applies to:** `lyrics`, `unsyncedlyrics`, `explicit`.

- [ ] **Unsynced lyrics leftover**
  - If your policy is "move `unsyncedlyrics` → `lyrics` when `lyrics` empty", flag rows where `unsyncedlyrics` remains populated after the cleanup stage.

- [ ] **Explicit value domain consistency**
  - Flag mixed representations (e.g., `0/1` mixed with `Clean/Explicit` or vendor-specific codes).

---

### P2 — Lower severity / long-term library health (remaining)

### 5) Taxonomy drift (genre/style/mood/theme)
**Applies to:** `genre`, `style`, `mood`, `theme`.

- [ ] **Over-broad genre residue**
  - Track counts of generic buckets (e.g., `Pop`, `Pop/Rock`, `Jazz`, `Classical`) post-enrichment.
  - Why: tells you whether enrichment/normalization is paying off.

- [ ] **Style-in-genre leakage**
  - If you sometimes merge style→genre, flag records where `style` is empty but `genre` looks like it contains many style tokens (or vice versa).

### 6) ID integrity / collisions (offline checks)
**Applies to:** `isrc`, `upc`, `barcode`, `asin`, `catalog`, `catalognumber` (if present), `musicbrainz_*`, `acoustid_*`, `discogs_*`, `songkong_id`, `roonid`, `itunesalbumid`, `itunesartistid`.

- [ ] **ISRC collisions**
  - Flag when the same `isrc` appears across materially different `title/artist` combinations.
  - Why: indicates tag collisions or mis-assignment.

- [ ] **Discogs URL/ID inconsistencies**
  - Flag when `discogs_release_url` is present but `discogs_release_id` is missing (or malformed), and similarly for master release.

### 7) Audio/file-level anomalies (system columns)
**Applies to:** `__md5sig`, `__length_seconds`, `__file_size_bytes`, plus ReplayGain fields.

- [ ] **Audio-stream content hashing for formats without embedded MD5**
  - Consider implementing an audio-stream MD5 (or similar stable digest) for file formats that don't natively provide an embedded content hash.
  - Implementation approach: Rust Symphonia decoder wrapped via PyO3, producing a deterministic digest over decoded PCM frames.
  - Why: enables "exact duplicates by audio content" detection beyond FLAC/WavPack embedded MD5 coverage.

---

### Tooling / UX (operational, remaining)

## Notes / Implementation hints (non-code)

- Prefer generating a report with:
  - counts per issue type
  - top examples (sample `__path` + key fields)
  - a stable issue key so you can track "fixed vs remaining" over time
- For date checks, decide whether you allow partial dates (`YYYY`, `YYYY-MM`) and treat them consistently.

---

## Deferred notes (kept for implementation planning)

- Synthetic-retirement logic should remain opt-in and review-first (no automatic replacements).
- Cross-database sync should always be changeloged and idempotent across reruns.


