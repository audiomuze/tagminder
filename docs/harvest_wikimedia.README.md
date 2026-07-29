# harvest_wikimedia.py — Wikidata Identity Ingestion

Loads Wikidata music identity records by streaming `latest-all.json.gz` or `latest-all.json.bz2` into a normalized SQLite table for use as a reconciliation hub by downstream scripts (e.g., `harvest_mb_artists.py`).

**Input**: Wikidata entity JSON dump (`latest-all.json.gz` or `latest-all.json.bz2`) (required)  
**Output**: Single normalized SQLite table (`wikidata_music_identity`) with Wikidata, MusicBrainz, and AllMusic identity mappings

---

## Quick Start

### 1. Prerequisites

- **Python 3.10+** with `uv` package manager
- **Wikidata entity JSON dump** file (`latest-all.json.gz` or `latest-all.json.bz2`)

### 2. Configuration

Create `harvest_master_data.toml` with:

```toml
[wikimedia]
all_json_gz = "/path/to/latest-all.json.bz2"     # required; Wikidata entity JSON dump (.gz or .bz2)
wikimedia_db = "/path/to/wikimedia.db"           # required; output database
target_table = "wikidata_music_identity"         # optional; defaults to above
label_language = "en"                            # optional; label/alias language
apple_music_artist_id_property = "P2850"         # optional; override if needed
wikipedia_base_url = "https://en.wikipedia.org/" # optional; controls wikipedia_url extraction
```

Configuration discovery behavior:

- The script first looks for `harvest_master_data.toml` in the current working directory.
- If not found there, it looks for `harvest_master_data.toml` in the same directory as `harvest_wikimedia.py`.
- If not found in either location, it fails with `FileNotFoundError`.

### 3. Run the script

```bash
uv run harvest_wikimedia.py
```

Logs will show:

```text
Run config: Stream from /path/to/latest-all.json.bz2 (... GiB), db=/path/to/wikimedia.db, table=wikidata_music_identity, label_language=en, batch_size=10,000
  progress: ... entities scanned, ... candidates retained, ... rows written
  scan complete: ... entities scanned, ... candidates retained in ...s
  wrote ... rows into 'wikidata_music_identity' in ...s (total ...s)
```

---

## Wikidata Entity JSON Dump

Download from Wikimedia dumps (example):

`https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz`

You can also use a `.bz2` variant if you maintain one locally (for example, `latest-all.json.bz2`).

Save to the path configured in `[wikimedia].all_json_gz`.

### Truthy Statement Selection

The script does not approximate truthy data. It reconstructs truthy statement selection from `latest-all.json.gz`/`.bz2` per property using statement `rank`:

1. Claims with `rank = "deprecated"` are ignored.
2. If any claim for a property has `rank = "preferred"`, only preferred claims are retained.
3. Otherwise, all `rank = "normal"` claims are retained.

This is intended to yield the same retained truthy statements for the extracted properties as the truthy dump, while using the entity JSON source format.

### Common Wikidata Properties

| Property | Code | Description |
|----------|------|-------------|
| MusicBrainz ID | P434 | Artist identifier in MusicBrainz |
| AllMusic ID | P1728 | Artist identifier in AllMusic |
| VIAF ID | P214 | Virtual International Authority File |
| ISNI | P213 | International Standard Name Identifier |
| Sex or gender | P21 | Gender identity classification |

### Property-to-Column Mapping

Definitive extraction mapping used by `harvest_wikimedia.py`:

| Wikidata Property | Code | SQLite Column |
|---|---|---|
| MusicBrainz artist ID | P434 | `mbid` |
| AllMusic artist ID | P1728 | `allmusic_mnid` |
| Songkick artist ID | P3478 | `songkick_artist_id` |
| Apple music artist ID (configurable) | configurable (`apple_music_artist_id_property`) | `apple_music_artist_id` |
| Discogs artist ID | P1953 | `discogs_artist_id` |
| Spotify artist ID | P2205 | `spotify_artist_id` |
| Last.fm artist ID | P3192 | `lastfm_artist_id` |
| YouTube channel ID | P2397 | `youtube_channel_id` |
| ISNI | P213 | `isni` |
| VIAF ID | P214 | `viaf_id` |
| Sex or gender | P21 | `gender` |
| Official website | P856 | `official_website` |
| Instance of | P31 | `instance_of_wikidata_ids` |
| Occupation | P106 | `occupation_wikidata_ids` |
| Country of citizenship | P27 | `citizenship_wikidata_ids` |
| Country of origin | P495 | `origin_country_wikidata_ids` |
| Place of birth | P19 | `place_of_birth_wikidata_id` |
| Place of death | P20 | `place_of_death_wikidata_id` |
| Date of birth | P569 | `date_of_birth` |
| Date of death | P570 | `date_of_death` |
| Inception | P571 | `inception` |
| Dissolved, abolished, or demolished date | P576 | `dissolved` |
| Genre | P136 | `genre_wikidata_ids` |
| Instrument | P1303 | `instrument_wikidata_ids` |
| Member of | P463 | `member_of_wikidata_ids` |

Derived URL columns:

| Source | SQLite Column | Notes |
|---|---|---|
| Wikidata item URI | `wikidata_url` | Direct copy of `wikidata_uri` |
| MusicBrainz ID | `musicbrainz_url` | `https://musicbrainz.org/artist/{mbid}` |
| AllMusic ID | `allmusic_url` | `https://www.allmusic.com/artist/{allmusic_mnid}` |
| Discogs artist ID | `discogs_url` | `https://www.discogs.com/artist/{discogs_artist_id}` |
| Spotify artist ID | `spotify_url` | `https://open.spotify.com/artist/{spotify_artist_id}` |
| Songkick artist ID | `songkick_url` | `https://www.songkick.com/artists/{songkick_artist_id}` |
| Wikipedia sitelink | `wikipedia_url` | Derived from configured `wikipedia_base_url` host and entity sitelinks |
| Apple artist ID | `apple_lookup_url` | iTunes lookup URL |

Label and alias extraction:

| JSON Section | SQLite Column | Notes |
|---|---|---|
| `labels[language].value` | `wikidata_label` | Uses `label_language` setting |
| `aliases[language][].value` | `wikidata_aliases` | JSON array in `label_language` |

Gender normalization:

- `P21 = Q6581097` -> `male`
- `P21 = Q6581072` -> `female`
- Any other or missing `P21` -> `not applicable`

---

## Data Model

### Input JSON Format

The script streams one entity at a time from `latest-all.json.gz`/`.bz2`, reconstructs truthy statements locally from claim ranks, and retains rows when either MusicBrainz ID (`P434`) or AllMusic ID (`P1728`) exists.

### Output Table

Single table named `wikidata_music_identity` (configurable):

| Column | Type | Notes |
|--------|------|-------|
| wikidata_uri | TEXT PRIMARY KEY | Wikidata URI (e.g., `http://www.wikidata.org/entity/Q123`) |
| wikidata_id | TEXT | Wikidata QID (e.g., `Q123`) |
| wikidata_label | TEXT | Human-readable label in configured language |
| wikidata_aliases | TEXT | JSON array of aliases in configured language |
| mbid | TEXT | MusicBrainz artist ID |
| allmusic_mnid | TEXT | AllMusic artist ID |
| songkick_artist_id | TEXT | Songkick artist ID (P3478) |
| apple_music_artist_id | TEXT | Apple/iTunes artist ID (configurable property) |
| discogs_artist_id | TEXT | Discogs artist ID |
| spotify_artist_id | TEXT | Spotify artist ID |
| lastfm_artist_id | TEXT | Last.fm artist ID |
| youtube_channel_id | TEXT | YouTube channel ID |
| isni | TEXT | ISNI identifier |
| viaf_id | TEXT | VIAF identifier |
| gender | TEXT | Normalized value: `male`, `female`, or `not applicable` |
| official_website | TEXT | Official website URL |
| instance_of_wikidata_ids | TEXT | JSON array of instance-of QIDs |
| occupation_wikidata_ids | TEXT | JSON array of occupation QIDs |
| citizenship_wikidata_ids | TEXT | JSON array of citizenship QIDs |
| origin_country_wikidata_ids | TEXT | JSON array of origin-country QIDs |
| place_of_birth_wikidata_id | TEXT | Place-of-birth QID |
| place_of_death_wikidata_id | TEXT | Place-of-death QID |
| date_of_birth | TEXT | Truthy date literal |
| date_of_death | TEXT | Truthy date literal |
| inception | TEXT | Truthy date literal |
| dissolved | TEXT | Truthy date literal |
| genre_wikidata_ids | TEXT | JSON array of genre QIDs |
| instrument_wikidata_ids | TEXT | JSON array of instrument QIDs |
| member_of_wikidata_ids | TEXT | JSON array of membership QIDs |
| wikidata_url | TEXT | Duplicate of `wikidata_uri` for link convenience |
| musicbrainz_url | TEXT | Derived MusicBrainz artist URL |
| allmusic_url | TEXT | Derived AllMusic artist URL |
| discogs_url | TEXT | Derived Discogs artist URL |
| spotify_url | TEXT | Derived Spotify artist URL |
| songkick_url | TEXT | Derived Songkick artist URL |
| wikipedia_url | TEXT | URL from configured Wikipedia base host |
| apple_lookup_url | TEXT | Derived iTunes lookup API URL |
| source_dump | TEXT | Dump filename used for extraction |
| extracted_utc | TEXT | Extraction timestamp (UTC) |

The table is dropped and recreated on each run (full rebuild semantics).

---

## Rebuild Semantics

Each run of `harvest_wikimedia.py`:

1. Drops the existing target table (if present).
2. Recreates it with fresh schema.
3. Streams entities from `latest-all.json.gz` or `latest-all.json.bz2`.
4. Applies exact truthy rank selection locally per extracted property.
5. Writes retained rows to SQLite in batches.

This ensures the output table always reflects the current dump extraction, with no accumulation of stale records.

---

## Integration with harvest_mb_artists.py

The output table from this script (`wikidata_music_identity`) is used by `harvest_mb_artists.py` during Step 7 (artist identity alignment):

- Reconciliation: Cross-checks MusicBrainz ↔ Wikidata ↔ AllMusic identity consistency
- Conflict detection: Identifies MBID/QID/MNID mismatches across sources
- Alignment scoring: Computes confidence that artist records refer to the same entity

---

## Troubleshooting

### Entity JSON dump not found

```text
FileNotFoundError: Entity JSON dump not found: /path/to/latest-all.json.bz2
```

Solution: Download `latest-all.json.gz` (or use your local `.bz2` copy) and set `[wikimedia].all_json_gz` correctly.

### Config file missing

Solution: Ensure `harvest_master_data.toml` exists in the current directory (or in the script directory) and check `[wikimedia]` is present.

### Empty extraction result

If no entities contain MBID or AllMusic ID, the table will be created but empty:

```text
Run config: Stream from /path/to/latest-all.json.bz2 (... GiB), db=/path/to/wikimedia.db, table=wikidata_music_identity, label_language=en, batch_size=10,000
  scan complete: ... entities scanned, 0 candidates retained in ...s
  wrote 0 rows into 'wikidata_music_identity' in ...s (total ...s)
```

---

## Performance Notes

- Parsing model: streaming gzip entity JSON in a single pass
- Truthy handling: reconstructed from claim `rank` values per property
- SQLite writes: batched `executemany()` inserts
- Memory profile: bounded by batch size rather than retained entity count
- Full rebuild: no incremental merging; each run is a clean slate

---

## Example Workflow

1. Download entity dump:
  Fetch `latest-all.json.gz` from Wikimedia dumps, or point to your local `.bz2` copy.
2. Configure paths in `harvest_master_data.toml`:

```toml
[wikimedia]
all_json_gz = "/data/latest-all.json.bz2"
wikimedia_db = "/data/wikimedia.db"
target_table = "wikidata_music_identity"
```

3. Run ingestion:

```bash
uv run harvest_wikimedia.py
```
