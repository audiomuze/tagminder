"""
Purpose:
    Process all records in `alib` and set tag values to NULL for unauthorised tags
    (i.e. any tagnames that don't appear in the allowlist from tagminder.toml
    `[cleanup].keep_columns`; system columns prefixed with `__` are always retained).

    Logs changes to `changelog`.

This script is part of Tagminder.

SQLite tables referenced:
    - alib
    - changelog

Author: audiomuze
Last updated: 2026-04-13
"""

import logging
import sqlite3
from typing import Dict, List, Tuple

import polars as pl

from tagminder.core import tm_db
from tagminder.core import tm_config
from tagminder.core import tm_polars_db
from tagminder.core import tm_run
# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
def merge_columns_before_cleanup(
    df: pl.DataFrame,
) -> Tuple[pl.DataFrame, List[Tuple[int, str, str | None, str | None]]]:
    """
    Merge selected columns before cleanup.

    Policy:
        - All merges use the configured multi-value delimiter from tagminder.toml
          ([strings].multivalue_delimiter).
        - Merges de-duplicate by case-insensitive exact match per delimited segment.

    iTunes advisory -> explicit:
        - itunesadvisory == 1 => explicit = '1'
        - itunesadvisory == 0 or 2 => explicit = NULL
        - itunesadvisory is NULL => leave explicit untouched

    Free-text merge:
        - description, comment -> review: append as delimited segments, de-dupe by
          case-insensitive exact segment match.

        Lyrics move:
                - If lyrics is NULL/empty and unsyncedlyrics is non-empty, move the value:
                    set lyrics = unsyncedlyrics, then set unsyncedlyrics = NULL.

    Returns updated dataframe and list of merge changes for logging.
    """
    merge_changes: List[Tuple[int, str, str | None, str | None]] = []
    df_updated = df.clone()

    cfg = tm_config.load_config()
    strings_cfg = cfg.get("strings", {}) if isinstance(cfg, dict) else {}
    delimiter = strings_cfg.get("multivalue_delimiter") if isinstance(strings_cfg, dict) else None
    if not isinstance(delimiter, str) or not delimiter:
        raise ValueError("Missing tagminder.toml [strings].multivalue_delimiter")

    def _norm_cell(value: object) -> str | None:
        if value is None:
            return None
        s = str(value).strip()
        return s

    def _split(value: object) -> list[str]:
        s = _norm_cell(value)
        if s is None or s == "":
            return []
        parts = [p.strip() for p in s.split(delimiter)]
        return [p for p in parts if p]

    def _merge_dedupe(existing_value: object, incoming_values: list[object]) -> str | None:
        existing_norm = _norm_cell(existing_value)
        existing_parts = _split(existing_value)
        incoming_parts: list[str] = []
        for v in incoming_values:
            incoming_parts.extend(_split(v))

        if not existing_parts and not incoming_parts:
            # Preserve None vs empty-string when there is no meaningful content.
            return None if existing_norm is None else ("" if existing_norm == "" else existing_norm)

        seen: set[str] = set()
        merged: list[str] = []
        for p in existing_parts + incoming_parts:
            key = p.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(p)

        if not merged:
            return None if existing_norm is None else ""

        return delimiter.join(merged)

    def _apply_merge(*, target: str, sources: list[str]) -> None:
        nonlocal df_updated

        source_cols = [c for c in sources if c in df_updated.columns]
        if not source_cols:
            return

        if target not in df_updated.columns:
            df_updated = df_updated.with_columns(pl.lit(None, dtype=pl.Utf8).alias(target))

        rowids = df_updated.get_column("rowid").to_list()
        target_vals = df_updated.get_column(target).to_list()
        source_vals = [df_updated.get_column(c).to_list() for c in source_cols]

        new_vals: list[str | None] = []
        for i in range(len(rowids)):
            incoming = [sv[i] for sv in source_vals]
            merged = _merge_dedupe(target_vals[i], incoming)
            old_norm = _norm_cell(target_vals[i])
            new_norm = _norm_cell(merged)
            # Preserve explicit None vs empty-string decisions from _merge_dedupe
            if old_norm == new_norm:
                new_vals.append(target_vals[i] if isinstance(target_vals[i], str) or target_vals[i] is None else str(target_vals[i]))
                continue

            new_vals.append(merged)
            merge_changes.append((int(rowids[i]), target, old_norm, new_norm))

        df_updated = df_updated.with_columns(pl.Series(name=target, values=new_vals, dtype=pl.Utf8))

    def _split_involvedpeople_entries(raw: str) -> list[str]:
        """Split involvedpeople into person/role entries.

        Expected format (as observed/confirmed):
            - entries delimited by '-'
            - within each entry: 'person, RoleA, RoleB'

        Hyphen gotcha: avoid splitting hyphenated names when there is no comma on
        both sides of the hyphen.
        """
        s = raw.strip()
        if not s:
            return []

        # Common formatting: spaces around the delimiter.
        if " - " in s:
            return [p.strip() for p in s.split(" - ") if p.strip()]

        # Fallback: split only at hyphens that appear to separate two structured
        # entries (i.e., a comma exists on both sides).
        entries: list[str] = []
        start = 0
        for idx, ch in enumerate(s):
            if ch != "-":
                continue
            left = s[start:idx]
            right = s[idx + 1 :]
            if "," in left and "," in right:
                piece = left.strip()
                if piece:
                    entries.append(piece)
                start = idx + 1

        tail = s[start:].strip()
        if tail:
            entries.append(tail)

        return entries

    def _extract_involvedpeople_fanout(value: object) -> dict[str, list[str]]:
        """Parse involvedpeople and return role-derived people by target column."""
        s = _norm_cell(value)
        if s is None or s == "":
            return {}

        role_to_targets: dict[str, tuple[str, ...]] = {
            "mainartist": ("albumartist",),
            "composerlyricist": ("composer", "lyricist"),
        }

        people_by_target: dict[str, list[str]] = {}
        seen_by_target: dict[str, set[str]] = {}

        for entry in _split_involvedpeople_entries(s):
            tokens = [t.strip() for t in entry.split(",") if t.strip()]
            if len(tokens) < 2:
                continue

            person = tokens[0]
            roles = tokens[1:]
            if not person:
                continue

            for role in roles:
                role_key = role.casefold().replace(" ", "")
                targets = role_to_targets.get(role_key)
                if not targets:
                    continue
                for target in targets:
                    seen = seen_by_target.setdefault(target, set())
                    person_key = person.casefold()
                    if person_key in seen:
                        continue
                    seen.add(person_key)
                    people_by_target.setdefault(target, []).append(person)

        return people_by_target

    def _apply_involvedpeople_role_fanout() -> None:
        nonlocal df_updated

        if "involvedpeople" not in df_updated.columns:
            return

        targets = ["albumartist", "composer", "lyricist"]
        for target in targets:
            if target not in df_updated.columns:
                df_updated = df_updated.with_columns(
                    pl.lit(None, dtype=pl.Utf8).alias(target)
                )

        rowids = df_updated.get_column("rowid").to_list()
        involved_vals = df_updated.get_column("involvedpeople").to_list()
        current_by_target = {t: df_updated.get_column(t).to_list() for t in targets}
        new_by_target: dict[str, list[str | None]] = {t: list(vals) for t, vals in current_by_target.items()}

        for i in range(len(rowids)):
            fanout = _extract_involvedpeople_fanout(involved_vals[i])
            if not fanout:
                continue

            for target, people in fanout.items():
                if target not in new_by_target:
                    continue
                merged = _merge_dedupe(new_by_target[target][i], people)
                old_norm = _norm_cell(new_by_target[target][i])
                new_norm = _norm_cell(merged)
                if old_norm == new_norm:
                    continue

                new_by_target[target][i] = merged
                merge_changes.append((int(rowids[i]), target, old_norm, new_norm))

        for target in targets:
            df_updated = df_updated.with_columns(
                pl.Series(name=target, values=new_by_target[target], dtype=pl.Utf8)
            )

    def _apply_involvedpeople2_key_value_merge() -> None:
        """Merge many role columns into involvedpeople2 as `key:value` segments.

        - Segments are delimited by the configured multi-value delimiter.
        - De-dupe is case-insensitive by exact segment match.
        - Only columns present in the dataframe are considered.
        - Values within each source cell are split on the same delimiter.

        Example segment:
            "bass guitar:John Paul Jones"
        """
        nonlocal df_updated

        # Column names are the keys exactly as they appear in the source.
        source_cols = [
            "a_r",
            "accordion",
            "acoustic guitar",
            "adapter",
            "art_direction",
            "artwork",
            "assistant_engineer",
            "background vocals",
            "band",
            "banjo",
            "bass",
            "bass guitar",
            "bass_electric",
            "bass_programming",
            "bass_upright",
            "bass_vocal",
            "bassguitar",
            "cajon",
            "cello",
            "chimes",
            "choir_chorus",
            "chorusconductor",
            "clarinet_bass",
            "composerlyricist",
            "concept",
            "design",
            "development",
            "dobro",
            "drum_programming",
            "drumkit",
            "drums",
            "editing",
            "editorial_research",
            "executive_producer",
            "fender_rhodes",
            "fiddle",
            "flute",
            "graphic_design",
            "guitar",
            "guitar (acoustic)",
            "guitar (any type)",
            "guitar_acoustic",
            "guitar_baritone",
            "guitar_bass",
            "guitar_electric",
            "guitar_steel",
            "hammond_b3",
            "harmonium",
            "harmony vocals",
            "horn",
            "horn_arrangements",
            "keyboards",
            "layout",
            "lead vocals",
            "liner_notes",
            "management",
            "mandolin",
            "mastering",
            "mastering_engineer",
            "mellotron",
            "mixing",
            "mixing engineer",
            "mixing_engineer",
            "musicpublisher",
            "oboe",
            "orchestration",
            "organ",
            "other instrument",
            "pedal_steel",
            "percussion",
            "photography",
            "piano",
            "programmer",
            "programming",
            "project_manager",
            "recording",
            "recording engineer",
            "recording_assistant",
            "remastering",
            "restoration",
            "rhythmprogrammer",
            "sax_alto",
            "sax_tenor",
            "slide_guitar",
            "sound engineer",
            "special_effects",
            "strings",
            "stringsconductor",
            "studio_assistant",
            "synthesizer",
            "synthesizer_pads",
            "synthesizer_programming",
            "tracking engineer",
            "trombone",
            "trumpet",
            "tuba",
            "viola",
            "violin",
            "vocal_engineer",
            "vocal_harmony",
            "vocal_producer",
            "vocalist",
            "vocals",
            "vocals_background",
            "workarranger",
        ]

        present_sources = [c for c in source_cols if c in df_updated.columns]
        if not present_sources:
            return

        if "involvedpeople2" not in df_updated.columns:
            df_updated = df_updated.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("involvedpeople2")
            )

        rowids = df_updated.get_column("rowid").to_list()
        target_vals = df_updated.get_column("involvedpeople2").to_list()
        source_vals = {c: df_updated.get_column(c).to_list() for c in present_sources}

        new_vals: list[str | None] = []
        for i in range(len(rowids)):
            incoming_segments: list[str] = []
            for key in present_sources:
                for v in _split(source_vals[key][i]):
                    incoming_segments.append(f"{key}:{v}")

            merged = _merge_dedupe(target_vals[i], incoming_segments)
            old_norm = _norm_cell(target_vals[i])
            new_norm = _norm_cell(merged)
            if old_norm == new_norm:
                new_vals.append(
                    target_vals[i]
                    if isinstance(target_vals[i], str) or target_vals[i] is None
                    else str(target_vals[i])
                )
                continue

            new_vals.append(merged)
            merge_changes.append((int(rowids[i]), "involvedpeople2", old_norm, new_norm))

        df_updated = df_updated.with_columns(
            pl.Series(name="involvedpeople2", values=new_vals, dtype=pl.Utf8)
        )

    # Merge rules (caseless segment de-dupe)
    _apply_merge(target="personnel", sources=["studiopersonnel", "main_personnel"])
    _apply_merge(target="artist", sources=["featured_artist", "studiopersonnel"])
    _apply_merge(target="songkong_id", sources=["songkongid"])
    _apply_merge(target="composer", sources=["author", "songwriter"])
    _apply_merge(target="albumartist", sources=["album artist", "primary_artist", "musician"])

    # Requested tag normalizations
    _apply_merge(target="album_dr", sources=["album dynamic range", "dynamic range"])
    _apply_merge(target="catalog", sources=["catalog#", "mcn", "catalognumber"])

    # involvedpeople structured fan-out (role-based)
    _apply_involvedpeople_role_fanout()

    # involvedpeople2 aggregation of many role columns (key:value segments)
    _apply_involvedpeople2_key_value_merge()

    _apply_merge(target="originaldate", sources=["original date"])
    _apply_merge(target="originalreleasedate", sources=["original release date"])
    _apply_merge(target="originalyear", sources=["origyear"])
    _apply_merge(target="version", sources=["albumversion", "release"])
    _apply_merge(target="releasetype", sources=["musicbrainz album type", "musicbrainz_albumtype"])

    # Newly requested merges
    _apply_merge(target="label", sources=["music publisher", "discogs_label"])
    _apply_merge(target="producer", sources=["co-producer", "additionalproducer"])
    _apply_merge(target="remixer", sources=["remixedby"])
    _apply_merge(
        target="engineer",
        sources=[
            "recording engineer",
            "mixing engineer",
            "mastering engineer",
            "vocal engineer",
            "additional programming engineer",
        ],
    )

    # Free-text merge into review
    _apply_merge(target="review", sources=["description", "comment"])

    # unsyncedlyrics -> lyrics move (NOT a merge)
    if "unsyncedlyrics" in df_updated.columns:
        if "lyrics" not in df_updated.columns:
            df_updated = df_updated.with_columns(pl.lit(None, dtype=pl.Utf8).alias("lyrics"))

        rowids = df_updated.get_column("rowid").to_list()
        lyrics_vals = df_updated.get_column("lyrics").to_list()
        unsynced_vals = df_updated.get_column("unsyncedlyrics").to_list()

        new_lyrics: list[str | None] = list(lyrics_vals)
        new_unsynced: list[str | None] = list(unsynced_vals)

        for i in range(len(rowids)):
            lyric_norm = _norm_cell(lyrics_vals[i])
            uns_norm = _norm_cell(unsynced_vals[i])

            if (lyric_norm is None or lyric_norm == "") and (uns_norm is not None and uns_norm != ""):
                # Move into lyrics
                if lyric_norm != uns_norm:
                    merge_changes.append((int(rowids[i]), "lyrics", lyric_norm, uns_norm))
                new_lyrics[i] = uns_norm

                # Clear unsyncedlyrics
                if uns_norm is not None:
                    merge_changes.append((int(rowids[i]), "unsyncedlyrics", uns_norm, None))
                new_unsynced[i] = None

        df_updated = df_updated.with_columns(
            [
                pl.Series(name="lyrics", values=new_lyrics, dtype=pl.Utf8),
                pl.Series(name="unsyncedlyrics", values=new_unsynced, dtype=pl.Utf8),
            ]
        )

    # itunesadvisory -> explicit conditional set (NOT a merge)
    if "itunesadvisory" in df_updated.columns:
        if "explicit" not in df_updated.columns:
            df_updated = df_updated.with_columns(pl.lit(None, dtype=pl.Utf8).alias("explicit"))

        rowids = df_updated.get_column("rowid").to_list()
        advisories = df_updated.get_column("itunesadvisory").to_list()
        explicits = df_updated.get_column("explicit").to_list()

        new_explicits: list[str | None] = list(explicits)
        for i in range(len(rowids)):
            adv = advisories[i]
            adv_norm = _norm_cell(adv)
            if adv_norm is None or adv_norm == "":
                continue  # NULL advisory => leave explicit untouched

            # itunesadvisory: 0=None/unknown, 1=explicit, 2=clean
            if adv_norm == "1":
                desired: str | None = "1"
            elif adv_norm in {"0", "2"}:
                desired = None
            else:
                continue

            old_norm = _norm_cell(explicits[i])
            new_norm = _norm_cell(desired)
            if old_norm == new_norm:
                continue

            new_explicits[i] = desired
            merge_changes.append((int(rowids[i]), "explicit", old_norm, new_norm))

        df_updated = df_updated.with_columns(
            pl.Series(name="explicit", values=new_explicits, dtype=pl.Utf8)
        )

    return df_updated, merge_changes


def cleanup_dataframe(
    df: pl.DataFrame, fixed_columns: List[str]
) -> Tuple[
    Dict[int, int],
    List[Tuple[int, str, str, None]],
    List[Tuple[str, int]],
    Dict[str, int],
]:
    fixed_set = set(fixed_columns)
    columns_to_drop = [
        col
        for col in df.columns
        if col not in fixed_set and col not in ("rowid", "__sqlmodded")
    ]

    change_log: List[Tuple[int, str, str, None]] = []
    null_updates: List[Tuple[str, int]] = []
    rowid_mod_map: Dict[int, int] = {}
    changes_by_column: Dict[str, int] = {}

    for col in columns_to_drop:
        series = df[col]
        for rowid, value in zip(df["rowid"], series):
            if value is not None:
                rowid_int = int(rowid)
                change_log.append((rowid_int, col, value, None))
                null_updates.append((col, rowid_int))
                rowid_mod_map[rowid_int] = rowid_mod_map.get(rowid_int, 0) + 1
                changes_by_column[col] = changes_by_column.get(col, 0) + 1

    return rowid_mod_map, change_log, null_updates, changes_by_column


def write_updates_to_db(
    conn: sqlite3.Connection,
    rowid_mod_map: Dict[int, int],
    change_log: List[Tuple[int, str, str, None]],
    null_updates: List[Tuple[str, int]],
    merge_changes: List[Tuple[int, str, str | None, str | None]],
) -> int:
    cursor = conn.cursor()

    # Include merge changes in the total updated rows count
    all_updated_rowids = set(rowid_mod_map.keys())
    all_updated_rowids.update(rowid for rowid, _, _, _ in merge_changes)
    total_updated_rows = len(all_updated_rowids)

    if total_updated_rows == 0:
        return 0

    tm_db.ensure_changelog_table(conn)
    timestamp = tm_db.utc_now_iso()
    script_name = tm_db.script_name()

    path_by_rowid = tm_db.fetch_paths_by_rowid(conn, list(all_updated_rowids))

    with tm_db.transaction(conn):
        # 1. Apply merge changes first
        for rowid, column, old_value, new_value in merge_changes:
            cursor.execute(
                f'UPDATE alib SET "{column}" = ? WHERE rowid = ?', (new_value, rowid)
            )

        # 2. Nullify unwanted values
        updates_by_column: Dict[str, List[int]] = {}
        for col, rowid in null_updates:
            updates_by_column.setdefault(col, []).append(rowid)

        for col, rowids in updates_by_column.items():
            cursor.executemany(
                f'UPDATE alib SET "{col}" = NULL WHERE rowid = ?',
                [(rowid,) for rowid in rowids],
            )

        # 3. Update __sqlmodded for drop changes
        if rowid_mod_map:
            cursor.executemany(
                "UPDATE alib SET __sqlmodded = COALESCE(__sqlmodded, 0) + ? WHERE rowid = ?",
                [(mod_count, rowid) for rowid, mod_count in rowid_mod_map.items()],
            )

        # 4. Update __sqlmodded for merge changes (increment by 1 for each merge)
        merge_mod_counts: Dict[int, int] = {}
        for rowid, _, _, _ in merge_changes:
            merge_mod_counts[rowid] = merge_mod_counts.get(rowid, 0) + 1

        if merge_mod_counts:
            cursor.executemany(
                "UPDATE alib SET __sqlmodded = COALESCE(__sqlmodded, 0) + ? WHERE rowid = ?",
                [(mod_count, rowid) for rowid, mod_count in merge_mod_counts.items()],
            )

        # 5. Write changelog for both merge and drop changes
        def _norm(v):
            if v is None:
                return None
            return v if isinstance(v, str) else str(v)

        entries: list[tm_db.ChangelogEntry] = []

        for rowid, column, old_value, new_value in merge_changes:
            entries.append(
                tm_db.ChangelogEntry(
                    alib_path=path_by_rowid.get(int(rowid), str(rowid)),
                    alib_column=column,
                    old_value=_norm(old_value),
                    new_value=_norm(new_value),
                    timestamp=timestamp,
                    script=script_name,
                )
            )

        for rowid, col, old, new in change_log:
            entries.append(
                tm_db.ChangelogEntry(
                    alib_path=path_by_rowid.get(int(rowid), str(rowid)),
                    alib_column=col,
                    old_value=_norm(old),
                    new_value=_norm(new),
                    timestamp=timestamp,
                    script=script_name,
                )
            )

        tm_db.insert_changelog_entries(cursor, entries)

    return total_updated_rows


def _load_keep_columns_from_config() -> tuple[list[str], list[str]]:
    cfg = tm_config.load_config()
    cleanup_cfg = cfg.get("cleanup", {}) if isinstance(cfg, dict) else {}
    keep_columns = (
        cleanup_cfg.get("keep_columns") if isinstance(cleanup_cfg, dict) else None
    )

    if keep_columns is None:
        raise RuntimeError(
            "Missing tagminder.toml [cleanup].keep_columns (required by 01-null-unauthorised-tags.py)"
        )
    if not isinstance(keep_columns, list) or not all(isinstance(c, str) for c in keep_columns):
        raise RuntimeError(
            "Invalid tagminder.toml: expected [cleanup].keep_columns to be a list of strings"
        )

    # De-dupe while preserving order
    seen: set[str] = set()
    keep_dedup: list[str] = []
    for c in keep_columns:
        if c in seen:
            continue
        seen.add(c)
        keep_dedup.append(c)

    schema_columns = (
        cfg.get("columns", {}).get("schema_columns")
        if isinstance(cfg, dict) and isinstance(cfg.get("columns"), dict)
        else None
    )
    schema_set: set[str] = (
        set(schema_columns)
        if isinstance(schema_columns, list)
        and all(isinstance(c, str) for c in schema_columns)
        else set()
    )

    extras = [c for c in keep_dedup if not c.startswith("__") and c not in schema_set]
    return keep_dedup, extras


def main():
    fixed_columns, extras = _load_keep_columns_from_config()
    if extras:
        logging.warning(
            "[cleanup].keep_columns includes %d column(s) not present in [columns].schema_columns:",
            len(extras),
        )
        for c in extras:
            logging.warning("  - %s", c)

    try:
        conn, _, _, _ = tm_run.open_db(ensure_changelog=True, require_exists=True)
    except FileNotFoundError as e:
        logging.error(f"Database file does not exist: {e}")
        return
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        return

    if not tm_db.table_exists(conn, "alib"):
        logging.error("Required table 'alib' not found in database")
        conn.close()
        return

    try:
        logging.info("Fetching all data from alib table...")

        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(alib)")
        all_columns = [row[1] for row in cursor.fetchall()]
        data_columns = [
            col
            for col in all_columns
            if not col.startswith("__") and col != "__sqlmodded"
        ]

        column_clause = ", ".join(f'"{col}"' for col in data_columns)
        query = f"""
            SELECT rowid, COALESCE(__sqlmodded, 0) AS __sqlmodded, {column_clause}
            FROM alib
        """

        tracks_df = tm_polars_db.sqlite_to_polars(conn, query)
        logging.info(
            f"Loaded DataFrame with {tracks_df.height} rows and {len(tracks_df.columns)} columns"
        )

        # Always retain system columns (prefix '__') without requiring config entries.
        fixed_set = set(fixed_columns)
        for col in tracks_df.columns:
            if col.startswith("__") and col not in fixed_set:
                fixed_columns.append(col)
                fixed_set.add(col)

        logging.info("Processing column merges...")
        tracks_df, merge_changes = merge_columns_before_cleanup(tracks_df)
        if merge_changes:
            merge_counts: Dict[str, int] = {}
            for _, column, _, _ in merge_changes:
                merge_counts[column] = merge_counts.get(column, 0) + 1
            logging.info(f"Merged {len(merge_changes)} values:")
            for column, count in merge_counts.items():
                logging.info(f"  - {column}: {count} merges")

        logging.info("Cleaning up dataframe...")
        rowid_mod_map, change_log, null_updates, changes_by_column = cleanup_dataframe(
            tracks_df, fixed_columns
        )
        total_rows_changed = len(rowid_mod_map)
        logging.info(f"Total number of rows with drop changes: {total_rows_changed}")
        logging.info("Number of drop changes by column:")
        for col, count in changes_by_column.items():
            logging.info(f"  - {col}: {count}")

        if total_rows_changed > 0 or merge_changes:
            logging.info("Writing updates back to database...")
            all_updated_rowids = set(rowid_mod_map.keys())
            all_updated_rowids.update(rowid for rowid, _, _, _ in merge_changes)
            logging.info(f"Rows flagged for update: {len(all_updated_rowids)}")
            updated_count = write_updates_to_db(
                conn, rowid_mod_map, change_log, null_updates, merge_changes
            )
            total_changes = len(change_log) + len(merge_changes)
            logging.info(
                f"Successfully updated {updated_count} rows in the database and logged {total_changes} changes."
            )
        else:
            logging.info("No changes detected, database not updated.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
    finally:
        conn.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()
