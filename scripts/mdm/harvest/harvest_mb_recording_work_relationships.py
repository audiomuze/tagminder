"""Extract MusicBrainz recording-to-work relationships from mbdump.tar.bz2.

This script is intentionally scoped to recording↔work relationship ingestion.
It writes relationship edges and relationship attributes into the same
master-data SQLite database used by the other MusicBrainz harvest scripts.

Inputs (from mbdump.tar.bz2):
- mbdump/l_recording_work
- mbdump/link
- mbdump/link_type
- mbdump/link_attribute (optional)
- mbdump/link_attribute_type (optional)
- mbdump/link_attribute_text_value (optional)

Outputs:
- musicbrainz_recording_work_relationships
- musicbrainz_recording_work_relationship_attributes

Both output tables are dropped/recreated on each run.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import sqlite3
import tarfile
import time
from pathlib import Path

from tagminder.core import tm_config

log = logging.getLogger("harvest_mb_recording_work_relationships")

MASTER_CONFIG_FILE = "harvest_master_data.toml"
EDGE_CHUNK_SIZE = 50_000
ATTR_CHUNK_SIZE = 100_000

REL_TABLE = "musicbrainz_recording_work_relationships"
ATTR_TABLE = "musicbrainz_recording_work_relationship_attributes"


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
    # 0=id, 1=parent, 2=root, 3=child_order, 4=gid, 5=name, 6=description, ...
    type_id = _to_int(row[0] if len(row) > 0 else None)
    if type_id is None:
        return (None, None)

    if len(row) >= 6 and _looks_uuid(row[4] if len(row) > 4 else None):
        return (type_id, _clean_text(row[5]))

    name = _clean_text(row[5] if len(row) > 5 else None)
    if name is None:
        name = _clean_text(row[3] if len(row) > 3 else None)
    return (type_id, name)


def _create_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {ATTR_TABLE}")
    cursor.execute(f"DROP TABLE IF EXISTS {REL_TABLE}")

    cursor.execute(
        f"""
        CREATE TABLE {REL_TABLE} (
            edge_id INTEGER PRIMARY KEY,
            l_recording_work_id INTEGER UNIQUE NOT NULL,
            link_id INTEGER NOT NULL,
            recording_id INTEGER NOT NULL,
            work_id INTEGER NOT NULL,
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
            l_recording_work_id INTEGER NOT NULL,
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

    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_recording ON {REL_TABLE}(recording_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_work ON {REL_TABLE}(work_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_link ON {REL_TABLE}(link_id)")
    cursor.execute(f"CREATE INDEX idx_{REL_TABLE}_type ON {REL_TABLE}(link_type_id)")

    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_edge ON {ATTR_TABLE}(edge_id)")
    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_link ON {ATTR_TABLE}(link_id)")
    cursor.execute(f"CREATE INDEX idx_{ATTR_TABLE}_atype ON {ATTR_TABLE}(attribute_type_id)")


def _load_link_maps(
    buffers: dict[str, io.BytesIO],
) -> tuple[
    dict[int, tuple[int | None, int | None, int | None, int | None, int | None, int | None, int | None, int]],
    dict[int, tuple[str | None, str | None, str | None]],
]:
    link_type_map: dict[int, tuple[str | None, str | None, str | None]] = {}
    for row in csv.reader(_text_stream(buffers["mbdump/link_type"]), delimiter="\t"):
        type_id, name, phrase_fwd, phrase_rev = _parse_link_type_row(row)
        if type_id is None:
            continue
        link_type_map[type_id] = (name, phrase_fwd, phrase_rev)

    link_map: dict[int, tuple[int | None, int | None, int | None, int | None, int | None, int | None, int | None, int]] = {}
    for row in csv.reader(_text_stream(buffers["mbdump/link"]), delimiter="\t"):
        link_id = _to_int(row[0] if len(row) > 0 else None)
        if link_id is None:
            continue

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
            ended,
        )

    return link_map, link_type_map


def _stream_edges(
    buffers: dict[str, io.BytesIO],
    cursor: sqlite3.Cursor,
    *,
    source_dump: str,
    extracted_utc: str,
    link_map: dict[int, tuple[int | None, int | None, int | None, int | None, int | None, int | None, int | None, int]],
    link_type_map: dict[int, tuple[str | None, str | None, str | None]],
) -> int:
    insert_sql = f"""
        INSERT INTO {REL_TABLE} (
            l_recording_work_id,
            link_id,
            recording_id,
            work_id,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    edge_chunk: list[tuple[object, ...]] = []
    inserted = 0

    for row in csv.reader(_text_stream(buffers["mbdump/l_recording_work"]), delimiter="\t"):
        if len(row) < 4:
            continue

        l_row_id = _to_int(row[0])
        link_id = _to_int(row[1])
        recording_id = _to_int(row[2])
        work_id = _to_int(row[3])
        if l_row_id is None or link_id is None or recording_id is None or work_id is None:
            continue

        link_info = link_map.get(link_id)
        if link_info is None:
            continue

        (
            link_type_id,
            begin_y,
            begin_m,
            begin_d,
            end_y,
            end_m,
            end_d,
            ended,
        ) = link_info
        type_name, phrase_fwd, phrase_rev = link_type_map.get(link_type_id or -1, (None, None, None))

        edge_chunk.append(
            (
                l_row_id,
                link_id,
                recording_id,
                work_id,
                _to_int(row[4] if len(row) > 4 else None),
                _clean_text(row[5] if len(row) > 5 else None),
                _clean_text(row[6] if len(row) > 6 else None),
                link_type_id,
                type_name,
                phrase_fwd,
                phrase_rev,
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
            cursor.executemany(insert_sql, edge_chunk)
            inserted += len(edge_chunk)
            edge_chunk = []

    if edge_chunk:
        cursor.executemany(insert_sql, edge_chunk)
        inserted += len(edge_chunk)

    return inserted


def _load_attribute_dicts(
    buffers: dict[str, io.BytesIO],
) -> tuple[
    dict[int, str | None],
    dict[tuple[int, int], str | None],
    dict[tuple[int, int], str | None],
]:
    attr_type_name: dict[int, str | None] = {}
    if "mbdump/link_attribute_type" in buffers:
        for row in csv.reader(_text_stream(buffers["mbdump/link_attribute_type"]), delimiter="\t"):
            type_id, name = _parse_link_attribute_type_name(row)
            if type_id is None:
                continue
            attr_type_name[type_id] = name

    attr_text_map: dict[tuple[int, int], str | None] = {}
    if "mbdump/link_attribute_text_value" in buffers:
        for row in csv.reader(_text_stream(buffers["mbdump/link_attribute_text_value"]), delimiter="\t"):
            link_id = _to_int(row[0] if len(row) > 0 else None)
            attr_type_id = _to_int(row[1] if len(row) > 1 else None)
            if link_id is None or attr_type_id is None:
                continue
            attr_text_map[(link_id, attr_type_id)] = _clean_text(row[2] if len(row) > 2 else None)

    attr_credit_map: dict[tuple[int, int], str | None] = {}
    if "mbdump/link_attribute_credit" in buffers:
        for row in csv.reader(_text_stream(buffers["mbdump/link_attribute_credit"]), delimiter="\t"):
            link_id = _to_int(row[0] if len(row) > 0 else None)
            attr_type_id = _to_int(row[1] if len(row) > 1 else None)
            if link_id is None or attr_type_id is None:
                continue
            attr_credit_map[(link_id, attr_type_id)] = _clean_text(row[2] if len(row) > 2 else None)

    return attr_type_name, attr_text_map, attr_credit_map


def _attach_attributes(
    buffers: dict[str, io.BytesIO],
    cursor: sqlite3.Cursor,
    *,
    source_dump: str,
    extracted_utc: str,
    attr_type_name: dict[int, str | None],
    attr_text_map: dict[tuple[int, int], str | None],
    attr_credit_map: dict[tuple[int, int], str | None],
) -> int:
    edge_by_link: dict[int, list[tuple[int, int]]] = {}
    for edge_id, l_row_id, link_id in cursor.execute(
        f"SELECT edge_id, l_recording_work_id, link_id FROM {REL_TABLE}"
    ):
        edge_by_link.setdefault(int(link_id), []).append((int(edge_id), int(l_row_id)))

    if "mbdump/link_attribute" not in buffers:
        return 0

    attr_insert = f"""
        INSERT INTO {ATTR_TABLE} (
            edge_id,
            l_recording_work_id,
            link_id,
            attribute_type_id,
            attribute_name,
            attribute_text_value,
            credited_as,
            source_dump,
            extracted_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    chunk: list[tuple[object, ...]] = []
    inserted = 0
    for row in csv.reader(_text_stream(buffers["mbdump/link_attribute"]), delimiter="\t"):
        if len(row) < 2:
            continue

        link_id = _to_int(row[0])
        attr_type_id = _to_int(row[1])
        if link_id is None:
            continue

        refs = edge_by_link.get(link_id)
        if not refs:
            continue

        text_value = attr_text_map.get((link_id, attr_type_id or -1))
        credited_as = attr_credit_map.get((link_id, attr_type_id or -1))
        attr_name = attr_type_name.get(attr_type_id or -1)

        for edge_id, l_row_id in refs:
            chunk.append(
                (
                    edge_id,
                    l_row_id,
                    link_id,
                    attr_type_id,
                    attr_name,
                    text_value,
                    credited_as,
                    source_dump,
                    extracted_utc,
                )
            )

        if len(chunk) >= ATTR_CHUNK_SIZE:
            cursor.executemany(attr_insert, chunk)
            inserted += len(chunk)
            chunk = []

    if chunk:
        cursor.executemany(attr_insert, chunk)
        inserted += len(chunk)

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

    return inserted


def harvest_pipeline() -> None:
    tar_archive, db_file = _load_musicbrainz_paths()

    tar_path = Path(tar_archive)
    if not tar_path.exists():
        raise FileNotFoundError(f"MusicBrainz dump archive not found: {tar_archive}")

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    t_total = time.perf_counter()
    source_dump = Path(tar_archive).name
    extracted_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    wanted = {
        "mbdump/link_type",
        "mbdump/link",
        "mbdump/l_recording_work",
        "mbdump/link_attribute_type",
        "mbdump/link_attribute_text_value",
        "mbdump/link_attribute_credit",
        "mbdump/link_attribute",
    }

    log.info("[1/5] Single-pass archive scan: %s", tar_archive)
    buffers = _stream_all_members(tar_archive, wanted)
    log.info("  loaded members: %s", sorted(buffers.keys()))

    for required in ("mbdump/link_type", "mbdump/link", "mbdump/l_recording_work"):
        if required not in buffers:
            raise RuntimeError(f"{required} not found in archive")

    log.info("[2/5] Loading link/link_type dictionaries...")
    link_map, link_type_map = _load_link_maps(buffers)
    log.info("  loaded dictionaries: link=%d link_type=%d", len(link_map), len(link_type_map))

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    _create_tables(cursor)

    log.info("[3/5] Streaming recording\u2194work edges (mbdump/l_recording_work)...")
    inserted_edges = _stream_edges(
        buffers,
        cursor,
        source_dump=source_dump,
        extracted_utc=extracted_utc,
        link_map=link_map,
        link_type_map=link_type_map,
    )
    log.info("  inserted %d recording-work edges", inserted_edges)

    log.info("[4/5] Attaching relationship attributes and JSON cache...")
    attr_type_name, attr_text_map, attr_credit_map = _load_attribute_dicts(buffers)
    inserted_attrs = _attach_attributes(
        buffers,
        cursor,
        source_dump=source_dump,
        extracted_utc=extracted_utc,
        attr_type_name=attr_type_name,
        attr_text_map=attr_text_map,
        attr_credit_map=attr_credit_map,
    )
    log.info("  inserted %d recording-work attributes", inserted_attrs)

    log.info("[5/5] Finalizing...")
    conn.commit()
    conn.close()

    log.info(
        "Harvest complete: recording-work edges=%d, recording-work attrs=%d (%.1fs)",
        inserted_edges,
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
