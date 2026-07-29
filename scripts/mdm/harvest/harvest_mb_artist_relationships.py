"""Extract MusicBrainz artist-to-artist relationships from mbdump.tar.bz2.

This script is intentionally scoped to artist↔artist relationship ingestion.
It writes relationship edges and relationship attributes into the same
contributors SQLite database used by harvest_mb_artists.py.

Inputs (from mbdump.tar.bz2):
- mbdump/l_artist_artist
- mbdump/link
- mbdump/link_type
- mbdump/link_attribute (optional)
- mbdump/link_attribute_type (optional)
- mbdump/link_attribute_text_value (optional)

Outputs:
- musicbrainz_artist_relationships
- musicbrainz_artist_relationship_attributes

Both output tables are dropped/recreated on each run.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import sqlite3
import tarfile
import time
from pathlib import Path

from tagminder.core import tm_config

log = logging.getLogger("harvest_mb_artist_relationships")

MASTER_CONFIG_FILE = "harvest_master_data.toml"
EDGE_CHUNK_SIZE = 50_000
ATTR_CHUNK_SIZE = 100_000
PROGRESS_LOG_INTERVAL = 500_000

REL_TABLE = "musicbrainz_artist_relationships"
ATTR_TABLE = "musicbrainz_artist_relationship_attributes"


def _is_nullish(value: str | None) -> bool:
    if value is None:
        return True
    text = value.strip()
    return text == "" or text == r"\N"


def _to_int(value: str | None) -> int | None:
    if _is_nullish(value):
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _to_bool_int(value: str | None) -> int:
    if _is_nullish(value):
        return 0
    text = str(value).strip().lower()
    return 1 if text in {"1", "t", "true", "y", "yes"} else 0


def _clean_text(value: str | None) -> str | None:
    if _is_nullish(value):
        return None
    return str(value).strip()


def _looks_uuid(value: str | None) -> bool:
    if _is_nullish(value):
        return False
    return bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            str(value).strip(),
        )
    )


def _resolve_master_config_path() -> Path:
    cwd_candidate = (Path.cwd() / MASTER_CONFIG_FILE).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    script_path = Path(__file__).resolve()
    checked: list[Path] = [cwd_candidate]
    for parent in script_path.parents:
        candidate = (parent / MASTER_CONFIG_FILE).resolve()
        checked.append(candidate)
        if candidate.exists():
            return candidate

    looked_in = "\n".join(f"- {path}" for path in checked)
    raise FileNotFoundError(
        f"{MASTER_CONFIG_FILE} not found. Looked in:\n{looked_in}"
    )


def _load_musicbrainz_paths() -> tuple[str, str]:
    config_path = _resolve_master_config_path()
    cfg = tm_config.load_config(config_path=config_path)

    config_dir = config_path.parent
    mb_raw = cfg.get("musicbrainz") if isinstance(cfg, dict) else None
    mb_cfg = mb_raw if isinstance(mb_raw, dict) else {}

    tar_candidate = str(mb_cfg.get("dump_archive", "")).strip()
    if not tar_candidate:
        raise FileNotFoundError(
            "MusicBrainz dump_archive path not found.\n"
            "Set [musicbrainz].dump_archive in harvest_master_data.toml."
        )
    tar_path = Path(tar_candidate).expanduser()
    if not tar_path.is_absolute():
        tar_path = (config_dir / tar_path).resolve()

    db_candidate = str(mb_cfg.get("contributors_db", "")).strip() or "master-data.db"
    db_path = Path(db_candidate).expanduser()
    if not db_path.is_absolute():
        db_path = (config_dir / db_path).resolve()

    return str(tar_path), str(db_path)


def _stream_all_members(
    tar_archive: str,
    wanted: set[str],
) -> dict[str, io.BytesIO]:
    """Single-pass stream scan of tar archive. Returns wanted members as in-memory BytesIO buffers."""
    buffers: dict[str, io.BytesIO] = {}
    with tarfile.open(tar_archive, "r:bz2") as tf:
        for member in tf:
            if member.name not in wanted:
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            buffers[member.name] = io.BytesIO(f.read())
            if len(buffers) == len(wanted):
                break
    return buffers


def _text_stream(buf: io.BytesIO) -> io.TextIOWrapper:
    """Convert BytesIO buffer to UTF-8 TextIOWrapper for csv reader."""
    buf.seek(0)
    return io.TextIOWrapper(buf, encoding="utf-8")


def _parse_link_type_row(row: list[str]) -> tuple[int | None, str | None, str | None, str | None]:
    # Observed mbdump/link_type layout (16 fields):
    # 0=id, 1=parent, 2=child_order, 3=gid,
    # 4=entity_type0, 5=entity_type1,
    # 6=name, 7=description,
    # 8=link_phrase, 9=reverse_link_phrase, 10=long_link_phrase, ...
    type_id = _to_int(row[0] if len(row) > 0 else None)
    if type_id is None:
        return (None, None, None, None)

    if len(row) >= 11 and _looks_uuid(row[3]):
        return (
            type_id,
            _clean_text(row[6]),
            _clean_text(row[8]),
            _clean_text(row[9]),
        )

    return (
        type_id,
        _clean_text(row[6] if len(row) > 6 else None),
        _clean_text(row[8] if len(row) > 8 else None),
        _clean_text(row[9] if len(row) > 9 else None),
    )


def _parse_link_attribute_type_name(row: list[str]) -> tuple[int | None, str | None]:
    # Observed mbdump/link_attribute_type layout (8 fields):
    # 0=id, 1=parent, 2=root, 3=child_order, 4=gid, 5=name, 6=description, 7=last_updated
    attr_type_id = _to_int(row[0] if len(row) > 0 else None)
    if attr_type_id is None:
        return (None, None)

    if len(row) >= 6 and _looks_uuid(row[4] if len(row) > 4 else None):
        return (attr_type_id, _clean_text(row[5]))

    name = _clean_text(row[5] if len(row) > 5 else None)
    if name is None:
        name = _clean_text(row[3] if len(row) > 3 else None)
    return (attr_type_id, name)


def _create_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {ATTR_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {REL_TABLE}")

    cursor.execute(
        f"""
        CREATE TABLE {REL_TABLE} (
            edge_id INTEGER PRIMARY KEY,
            l_artist_artist_id INTEGER UNIQUE NOT NULL,
            link_id INTEGER NOT NULL,
            from_artist_id INTEGER NOT NULL,
            to_artist_id INTEGER NOT NULL,
            link_order INTEGER,
            entity0_credit TEXT,
            entity1_credit TEXT,
            link_type_id INTEGER,
            relationship_name TEXT,
            relationship_phrase_forward TEXT,
            relationship_phrase_reverse TEXT,
            start_year INTEGER,
            start_month INTEGER,
            start_day INTEGER,
            end_year INTEGER,
            end_month INTEGER,
            end_day INTEGER,
            is_ended INTEGER NOT NULL,
            attributes_json TEXT,
            source_dump TEXT NOT NULL,
            extracted_utc TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE {ATTR_TABLE} (
            edge_attr_id INTEGER PRIMARY KEY,
            edge_id INTEGER NOT NULL,
            l_artist_artist_id INTEGER NOT NULL,
            link_id INTEGER NOT NULL,
            attribute_type_id INTEGER,
            attribute_name TEXT,
            attribute_text_value TEXT,
            credited_as TEXT,
            source_dump TEXT NOT NULL,
            extracted_utc TEXT NOT NULL
        )
        """
    )

    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_from_artist ON {REL_TABLE}(from_artist_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_to_artist ON {REL_TABLE}(to_artist_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_link ON {REL_TABLE}(link_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_type ON {REL_TABLE}(link_type_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_dates ON {REL_TABLE}(start_year, end_year)")

    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_edge ON {ATTR_TABLE}(edge_id)")
    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_link ON {ATTR_TABLE}(link_id)")
    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_atype ON {ATTR_TABLE}(attribute_type_id)")


def harvest_pipeline() -> None:
    tar_archive, db_file = _load_musicbrainz_paths()

    tar_path = Path(tar_archive)
    if not tar_path.exists():
        raise FileNotFoundError(f"MusicBrainz dump archive not found: {tar_archive}")

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    t_total = time.perf_counter()
    source_dump = Path(tar_archive).name
    extracted_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    log.info("[1/7] Single-pass archive scan: %s", tar_archive)
    t_total = time.perf_counter()
    
    wanted = {"mbdump/link_type", "mbdump/link", "mbdump/l_artist_artist", 
              "mbdump/link_attribute_type", "mbdump/link_attribute", "mbdump/link_attribute_text_value"}
    buffers = _stream_all_members(tar_archive, wanted)
    
    for required in ("mbdump/link_type", "mbdump/link", "mbdump/l_artist_artist"):
        if required not in buffers:
            raise RuntimeError(f"{required} not found in archive")

    log.info("[2/7] Loading relationship type dictionary (mbdump/link_type)...")
    link_type_map: dict[int, tuple[str | None, str | None, str | None]] = {}
    type_rows = 0
    link_type_stream = _text_stream(buffers["mbdump/link_type"])
    for row in csv.reader(link_type_stream, delimiter="\t"):
        type_id, name, fwd, rev = _parse_link_type_row(row)
        if type_id is None:
            continue
        link_type_map[type_id] = (name, fwd, rev)
        type_rows += 1
    log.info("  loaded %d link_type rows", type_rows)

    log.info("[3/7] Loading link bridge (mbdump/link)...")
    link_map: dict[int, tuple[int | None, int | None, int | None, int | None, int | None, int | None, int | None, int | None, int]] = {}
    link_rows = 0
    link_stream = _text_stream(buffers["mbdump/link"])
    for row in csv.reader(link_stream, delimiter="\t"):
        if len(row) < 2:
            continue
        link_id = _to_int(row[0])
        if link_id is None:
            continue

        # Common MB link table layout:
        # id, link_type, begin_y, begin_m, begin_d, end_y, end_m, end_d, attribute_count, created, ended, ...
        link_type_id = _to_int(row[1] if len(row) > 1 else None)
        begin_y = _to_int(row[2] if len(row) > 2 else None)
        begin_m = _to_int(row[3] if len(row) > 3 else None)
        begin_d = _to_int(row[4] if len(row) > 4 else None)
        end_y = _to_int(row[5] if len(row) > 5 else None)
        end_m = _to_int(row[6] if len(row) > 6 else None)
        end_d = _to_int(row[7] if len(row) > 7 else None)
        ended = _to_bool_int(row[10] if len(row) > 10 else None)

        link_map[link_id] = (
            link_type_id,
            begin_y,
            begin_m,
            begin_d,
            end_y,
            end_m,
            end_d,
            _to_int(row[8] if len(row) > 8 else None),
            ended,
        )
        link_rows += 1
    log.info("  loaded %d link rows", link_rows)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    _create_tables(cursor)

    log.info("[4/7] Streaming artist↔artist edges (mbdump/l_artist_artist)...")
    edge_insert_sql = f"""
        INSERT INTO {REL_TABLE} (
            l_artist_artist_id,
            link_id,
            from_artist_id,
            to_artist_id,
            link_order,
            entity0_credit,
            entity1_credit,
            link_type_id,
            relationship_name,
            relationship_phrase_forward,
            relationship_phrase_reverse,
            start_year,
            start_month,
            start_day,
            end_year,
            end_month,
            end_day,
            is_ended,
            attributes_json,
            source_dump,
            extracted_utc
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """

    edge_chunk: list[tuple[object, ...]] = []
    processed_edges = 0
    skipped_missing_link = 0

    l_art_art_stream = _text_stream(buffers["mbdump/l_artist_artist"])
    for row in csv.reader(l_art_art_stream, delimiter="\t"):
        if len(row) < 4:
            continue

        l_artist_artist_id = _to_int(row[0])
        link_id = _to_int(row[1])
        from_artist_id = _to_int(row[2])
        to_artist_id = _to_int(row[3])
        if (
            l_artist_artist_id is None
            or link_id is None
            or from_artist_id is None
            or to_artist_id is None
        ):
            continue

        link_info = link_map.get(link_id)
        if link_info is None:
            skipped_missing_link += 1
            continue

        link_type_id, begin_y, begin_m, begin_d, end_y, end_m, end_d, _attr_count, ended = link_info
        type_name, fwd_phrase, rev_phrase = link_type_map.get(link_type_id or -1, (None, None, None))

        edge_chunk.append(
            (
                l_artist_artist_id,
                link_id,
                from_artist_id,
                to_artist_id,
                _to_int(row[4] if len(row) > 4 else None),
                _clean_text(row[7] if len(row) > 7 else None),
                _clean_text(row[8] if len(row) > 8 else None),
                link_type_id,
                type_name,
                fwd_phrase,
                rev_phrase,
                begin_y,
                begin_m,
                begin_d,
                end_y,
                end_m,
                end_d,
                ended,
                None,
                source_dump,
                extracted_utc,
            )
        )

        if len(edge_chunk) >= EDGE_CHUNK_SIZE:
            cursor.executemany(edge_insert_sql, edge_chunk)
            processed_edges += len(edge_chunk)
            if processed_edges % PROGRESS_LOG_INTERVAL == 0:
                log.info("  progress: inserted %d relationship edges", processed_edges)
            edge_chunk = []

    if edge_chunk:
        cursor.executemany(edge_insert_sql, edge_chunk)
        processed_edges += len(edge_chunk)

    log.info(
        "  inserted %d relationship edges (skipped_missing_link=%d)",
        processed_edges,
        skipped_missing_link,
    )

    log.info("[5/7] Loading relationship attribute dictionaries (optional mbdump/link_attribute_type, mbdump/link_attribute_text_value)...")
    attribute_type_name: dict[int, str | None] = {}
    if "mbdump/link_attribute_type" in buffers:
        attr_type_stream = _text_stream(buffers["mbdump/link_attribute_type"])
        for row in csv.reader(attr_type_stream, delimiter="\t"):
            atype_id, atype_name = _parse_link_attribute_type_name(row)
            if atype_id is None:
                continue
            attribute_type_name[atype_id] = atype_name
    log.info("  loaded %d attribute types", len(attribute_type_name))

    attribute_text_value: dict[int, str | None] = {}
    if "mbdump/link_attribute_text_value" in buffers:
        attr_text_stream = _text_stream(buffers["mbdump/link_attribute_text_value"])
        for row in csv.reader(attr_text_stream, delimiter="\t"):
            if len(row) < 2:
                continue
            text_value_id = _to_int(row[0])
            if text_value_id is None:
                continue
            attribute_text_value[text_value_id] = _clean_text(row[1])
    log.info("  loaded %d attribute text values", len(attribute_text_value))

    log.info("[6/7] Streaming and attaching relationship attributes (optional mbdump/link_attribute)...")
    edge_by_link: dict[int, list[tuple[int, int]]] = {}
    for edge_id, l_artist_artist_id, link_id in cursor.execute(
        f"SELECT edge_id, l_artist_artist_id, link_id FROM {REL_TABLE}"
    ):
        edge_by_link.setdefault(int(link_id), []).append((int(edge_id), int(l_artist_artist_id)))

    attr_stream = None
    inserted_attrs = 0
    attr_chunk: list[tuple[object, ...]] = []

    if "mbdump/link_attribute" in buffers:
        attr_stream = _text_stream(buffers["mbdump/link_attribute"])
        attr_insert_sql = f"""
            INSERT INTO {ATTR_TABLE} (
                edge_id,
                l_artist_artist_id,
                link_id,
                attribute_type_id,
                attribute_name,
                attribute_text_value,
                credited_as,
                source_dump,
                extracted_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for row in csv.reader(attr_stream, delimiter="\t"):
            if len(row) < 3:
                continue

            link_id = _to_int(row[1])
            attribute_type_id = _to_int(row[2])
            if link_id is None:
                continue

            edge_refs = edge_by_link.get(link_id)
            if not edge_refs:
                continue

            # Common MB layout: id, link, attribute_type, attribute_text_value, credited_as
            text_value = _clean_text(row[3] if len(row) > 3 else None)
            text_value_id = _to_int(row[3] if len(row) > 3 else None)
            if text_value is None and text_value_id is not None:
                text_value = attribute_text_value.get(text_value_id)

            credited_as = _clean_text(row[4] if len(row) > 4 else None)
            attr_name = attribute_type_name.get(attribute_type_id or -1)

            for edge_id, l_artist_artist_id in edge_refs:
                attr_chunk.append(
                    (
                        edge_id,
                        l_artist_artist_id,
                        link_id,
                        attribute_type_id,
                        attr_name,
                        text_value,
                        credited_as,
                        source_dump,
                        extracted_utc,
                    )
                )

            if len(attr_chunk) >= ATTR_CHUNK_SIZE:
                cursor.executemany(attr_insert_sql, attr_chunk)
                inserted_attrs += len(attr_chunk)
                attr_chunk = []

        if attr_chunk:
            cursor.executemany(attr_insert_sql, attr_chunk)
            inserted_attrs += len(attr_chunk)

    # Backfill attributes_json to keep edge table self-contained for direct Polars reads.
    cursor.execute(
        f"""
        WITH agg AS (
            SELECT
                edge_id,
                json_group_array(
                    json_object(
                        'attribute_type_id', attribute_type_id,
                        'attribute_name', attribute_name,
                        'attribute_text_value', attribute_text_value,
                        'credited_as', credited_as
                    )
                ) AS attrs_json
            FROM {ATTR_TABLE}
            GROUP BY edge_id
        )
        UPDATE {REL_TABLE}
        SET attributes_json = (
            SELECT attrs_json FROM agg WHERE agg.edge_id = {REL_TABLE}.edge_id
        )
        WHERE edge_id IN (SELECT edge_id FROM agg)
        """
    )

    log.info("  inserted %d relationship attributes", inserted_attrs)

    log.info("[7/7] Finalizing...")
    conn.commit()
    conn.close()

    log.info(
        "Harvest complete: %s edges, %s attributes (%.1fs)",
        processed_edges,
        inserted_attrs,
        time.perf_counter() - t_total,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        harvest_pipeline()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        raise SystemExit(1)
    except KeyboardInterrupt:
        log.warning("Harvest aborted by user.")
        raise SystemExit(130)