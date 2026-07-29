"""Build canonical MusicBrainz work lookup directly from mbdump in one tar pass.

This script consolidates semantics from:
- __harvest_mb_works.py
- __harvest_mb_work_relationships.py
- __build_mb_work_lookup.py

It writes only one output table:
- canonical_works_metadata

No intermediate SQLite tables are created.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import tarfile
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import polars as pl

from tagminder.core import tm_config

log = logging.getLogger("harvest_mb_work_lookup")
MASTER_CONFIG_FILE = "harvest_master_data.toml"
LOOKUP_TABLE = "canonical_works_metadata"

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


def _normalize_text(value: str | None) -> str:
    if _is_nullish(value):
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.lower().replace('"', "")
    return " ".join(text.split())


def _looks_uuid(value: str | None) -> bool:
    if _is_nullish(value):
        return False
    return bool(
        re.fullmatch(
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            str(value).strip(),
        )
    )


def _mv_sorted_list(values: set[str] | list[str] | tuple[str, ...], delimiter: str) -> str | None:
    if not values:
        return None
    clean = sorted({str(v).strip() for v in values if str(v).strip()})
    if not clean:
        return None
    return delimiter.join(clean)


def _parse_link_type_row(row: list[str]) -> tuple[int | None, str | None, str | None, str | None]:
    type_id = _to_int(row[0] if len(row) > 0 else None)
    if type_id is None:
        return (None, None, None, None)

    # Observed current mbdump/link_type layout (16 fields):
    # 0=id, 1=parent, 2=child_order, 3=gid,
    # 4=entity_type0, 5=entity_type1,
    # 6=name, 7=description,
    # 8=link_phrase, 9=reverse_link_phrase, 10=long_link_phrase, ...
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
    type_id = _to_int(row[0] if len(row) > 0 else None)
    if type_id is None:
        return (None, None)

    # Observed current mbdump/link_attribute_type layout (8 fields):
    # 0=id, 1=parent, 2=root, 3=child_order, 4=gid, 5=name, 6=description, ...
    if len(row) >= 6 and _looks_uuid(row[4] if len(row) > 4 else None):
        return (type_id, _clean_text(row[5]))

    name = _clean_text(row[5] if len(row) > 5 else None)
    if name is None:
        name = _clean_text(row[3] if len(row) > 3 else None)
    return (type_id, name)


# Strict parser contract for mbdump table readers in this file:
# 1) Evidence-first only: column mappings must come from observed archive rows or dump DDL.
# 2) No heuristic fallback columns in production parsing paths.
# 3) Fail fast on unexpected row width/shape; do not silently coerce.
# 4) Keep parse rules table-specific; avoid cross-table assumptions.
# 5) Any schema change must update the inline table layout note with a concrete example row.
def _parse_work_type_row(row: list[str]) -> tuple[int | None, str | None]:
    # Observed in current mbdump/work_type rows:
    #   0=id, 1=name, 2=parent(\\N), 3=child_order, 4=description, 5=gid
    # Example from archive:
    #   29\tMusical\t\\N\t2\t...\t9ca5e067-acf7-3cd6-baa4-92bf1975bf24
    type_id = _to_int(row[0] if len(row) > 0 else None)
    if type_id is None:
        return (None, None)

    if len(row) < 2:
        raise RuntimeError(
            f"Unexpected mbdump/work_type row layout (expected >=2 columns, got {len(row)}): {row!r}"
        )

    return (type_id, _clean_text(row[1]))


def _parse_language_row(row: list[str]) -> tuple[int | None, str | None, str | None]:
    lang_id = _to_int(row[0] if len(row) > 0 else None)
    if lang_id is None:
        return (None, None, None)

    iso_code = _clean_text(row[3] if len(row) > 3 else None)
    if iso_code is None:
        iso_code = _clean_text(row[1] if len(row) > 1 else None)

    name = _clean_text(row[4] if len(row) > 4 else None)
    if name is None:
        name = _clean_text(row[2] if len(row) > 2 else None)

    return (lang_id, iso_code, name)


def _derive_explicit_role_labels(
    relationship_name: str | None,
    phrase_forward: str | None,
    phrase_reverse: str | None,
    attrs: list[dict[str, object]],
) -> set[str]:
    labels: set[str] = set()

    rel = _clean_text(relationship_name)
    if rel:
        labels.add(rel)

    fwd = _clean_text(phrase_forward)
    rev = _clean_text(phrase_reverse)
    if not labels and fwd:
        labels.add(fwd)
    if not labels and rev:
        labels.add(rev)

    # If no role label is present on link_type, use attribute names as a fallback surface.
    if not labels:
        for attr in attrs:
            name = _clean_text(str(attr.get("attribute_name") or ""))
            if name:
                labels.add(name)

    return labels


def _mv_role_pairs(role_map: dict[str, set[str]], delimiter: str) -> str | None:
    if not role_map:
        return None

    tokens: set[str] = set()
    for role in sorted(role_map.keys()):
        clean_role = role.strip()
        if not clean_role:
            continue
        for value in sorted({v.strip() for v in role_map.get(role, set()) if v and v.strip()}):
            tokens.add(f"{clean_role}:{value}")

    return _mv_sorted_list(tokens, delimiter)


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
    raise FileNotFoundError(f"Master config {MASTER_CONFIG_FILE} not found. Checked: {checked}")


def _load_musicbrainz_paths() -> tuple[str, str]:
    config_path = _resolve_master_config_path()
    cfg = tm_config.load_config(config_path=config_path)

    config_dir = config_path.parent
    mb_raw = cfg.get("musicbrainz") if isinstance(cfg, dict) else None
    mb_cfg = mb_raw if isinstance(mb_raw, dict) else {}

    tar_candidate = str(mb_cfg.get("dump_archive", "")).strip()
    if not tar_candidate:
        raise FileNotFoundError(
            "MusicBrainz dump_archive path not found. "
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


def harvest_pipeline() -> None:
    tar_archive, db_file = _load_musicbrainz_paths()
    tar_path = Path(tar_archive)
    if not tar_path.exists():
        raise FileNotFoundError(f"MusicBrainz dump archive not found: {tar_archive}")

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    source_dump = tar_path.name
    extracted_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    mv_delim = tm_config.get_multivalue_delimiter()

    log.info("[1/6] Single-pass tar scan and extraction...")

    link_type_map: dict[int, tuple[str | None, str | None, str | None]] = {}
    link_map: dict[int, tuple[int | None, int]] = {}
    attr_type_name: dict[int, str | None] = {}
    link_attr_types_by_link: dict[int, set[int]] = defaultdict(set)
    link_attr_text_by_key: dict[tuple[int, int], str | None] = {}
    link_attr_credit_by_key: dict[tuple[int, int], str | None] = {}

    work_type_map: dict[int, str | None] = {}
    language_map: dict[int, tuple[str | None, str | None]] = {}
    artist_name_by_id: dict[int, str] = {}
    artist_mbid_by_id: dict[int, str] = {}

    works_records: list[dict[str, object]] = []
    alias_records: list[dict[str, object]] = []
    iswc_records: list[dict[str, object]] = []

    work_lang_choice: dict[int, tuple[int, int]] = {}

    artist_work_refs: list[tuple[int, int, int]] = []
    work_work_refs: list[tuple[int, int, int]] = []

    target_members = {
        "mbdump/link_type",
        "mbdump/link",
        "mbdump/link_attribute_type",
        "mbdump/link_attribute_text_value",
        "mbdump/link_attribute_credit",
        "mbdump/link_attribute",
        "mbdump/work_type",
        "mbdump/language",
        "mbdump/artist",
        "mbdump/work",
        "mbdump/work_language",
        "mbdump/work_alias",
        "mbdump/iswc",
        "mbdump/l_artist_work",
        "mbdump/l_work_work",
    }
    found_members: set[str] = set()

    with tarfile.open(tar_archive, "r:*") as tar:
        for member in tar:
            name = member.name
            if name not in target_members:
                continue

            extracted = tar.extractfile(member)
            if extracted is None:
                continue

            log.info("    processing %s...", name.replace("mbdump/", ""))
            stream = io.TextIOWrapper(extracted, encoding="utf-8")
            reader = csv.reader(stream, delimiter="\t")

            if name == "mbdump/link_type":
                for row in reader:
                    type_id, rel_name, phrase_fwd, phrase_rev = _parse_link_type_row(row)
                    if type_id is None:
                        continue
                    link_type_map[type_id] = (rel_name, phrase_fwd, phrase_rev)

            elif name == "mbdump/link":
                for row in reader:
                    link_id = _to_int(row[0] if len(row) > 0 else None)
                    if link_id is None:
                        continue
                    link_type_id = _to_int(row[1] if len(row) > 1 else None)
                    ended = _to_bool_int(row[10] if len(row) > 10 else None)
                    link_map[link_id] = (link_type_id, ended)

            elif name == "mbdump/link_attribute_type":
                for row in reader:
                    attr_type_id, attr_name = _parse_link_attribute_type_name(row)
                    if attr_type_id is None:
                        continue
                    attr_type_name[attr_type_id] = attr_name

            elif name == "mbdump/link_attribute_text_value":
                for row in reader:
                    link_id = _to_int(row[0] if len(row) > 0 else None)
                    attr_type_id = _to_int(row[1] if len(row) > 1 else None)
                    if link_id is None or attr_type_id is None:
                        continue
                    link_attr_text_by_key[(link_id, attr_type_id)] = _clean_text(row[2] if len(row) > 2 else None)

            elif name == "mbdump/link_attribute_credit":
                for row in reader:
                    link_id = _to_int(row[0] if len(row) > 0 else None)
                    attr_type_id = _to_int(row[1] if len(row) > 1 else None)
                    if link_id is None or attr_type_id is None:
                        continue
                    link_attr_credit_by_key[(link_id, attr_type_id)] = _clean_text(row[2] if len(row) > 2 else None)

            elif name == "mbdump/link_attribute":
                for row in reader:
                    if len(row) < 2:
                        continue
                    link_id = _to_int(row[0])
                    attr_type_id = _to_int(row[1])
                    if link_id is None or attr_type_id is None:
                        continue
                    link_attr_types_by_link[link_id].add(attr_type_id)

            elif name == "mbdump/work_type":
                for row in reader:
                    work_type_id, work_type_name = _parse_work_type_row(row)
                    if work_type_id is None:
                        continue
                    work_type_map[work_type_id] = work_type_name

            elif name == "mbdump/language":
                for row in reader:
                    lang_id, lang_code, lang_name = _parse_language_row(row)
                    if lang_id is None:
                        continue
                    language_map[lang_id] = (lang_code, lang_name)

            elif name == "mbdump/artist":
                for row in reader:
                    artist_id = _to_int(row[0] if len(row) > 0 else None)
                    artist_mbid = _clean_text(row[1] if len(row) > 1 else None)
                    artist_name = _clean_text(row[2] if len(row) > 2 else None)
                    if artist_id is None:
                        continue
                    if artist_name is not None:
                        artist_name_by_id[artist_id] = artist_name
                    if artist_mbid is not None:
                        artist_mbid_by_id[artist_id] = artist_mbid

            elif name == "mbdump/work":
                for row in reader:
                    if len(row) < 3:
                        continue
                    work_id = _to_int(row[0])
                    if work_id is None:
                        continue
                    work_type_id = _to_int(row[3] if len(row) > 3 else None)
                    works_records.append(
                        {
                            "work_id": work_id,
                            "musicbrainz_workid": _clean_text(row[1] if len(row) > 1 else None),
                            "work_title": _clean_text(row[2] if len(row) > 2 else None),
                            "work_type_id": work_type_id,
                            "work_type_name": None,
                            "work_disambiguation": _clean_text(row[4] if len(row) > 4 else None),
                            "source_dump": source_dump,
                            "extracted_utc": extracted_utc,
                        }
                    )

            elif name == "mbdump/work_language":
                for row in reader:
                    if len(row) < 2:
                        continue
                    work_id = _to_int(row[0])
                    lang_id = _to_int(row[1] if len(row) > 1 else None)
                    if work_id is None or lang_id is None:
                        continue
                    is_primary = _to_bool_int(row[2] if len(row) > 2 else None)
                    existing = work_lang_choice.get(work_id)
                    if existing is None or (is_primary == 1 and existing[1] == 0):
                        work_lang_choice[work_id] = (lang_id, is_primary)

            elif name == "mbdump/work_alias":
                for row in reader:
                    work_id = _to_int(row[1] if len(row) > 1 else None)
                    alias = _clean_text(row[2] if len(row) > 2 else None)
                    if work_id is None or alias is None:
                        continue
                    alias_records.append({"work_id": work_id, "alias": alias})

            elif name == "mbdump/iswc":
                for row in reader:
                    work_id = _to_int(row[1] if len(row) > 1 else None)
                    iswc_value = _clean_text(row[2] if len(row) > 2 else None)
                    if work_id is None or iswc_value is None:
                        continue
                    iswc_records.append({"work_id": work_id, "iswc": iswc_value})

            elif name == "mbdump/l_artist_work":
                for row in reader:
                    if len(row) < 4:
                        continue
                    link_id = _to_int(row[1])
                    artist_id = _to_int(row[2])
                    work_id = _to_int(row[3])
                    if link_id is None or artist_id is None or work_id is None:
                        continue
                    artist_work_refs.append((link_id, artist_id, work_id))

            elif name == "mbdump/l_work_work":
                for row in reader:
                    if len(row) < 4:
                        continue
                    link_id = _to_int(row[1])
                    from_work_id = _to_int(row[2])
                    to_work_id = _to_int(row[3])
                    if link_id is None or from_work_id is None or to_work_id is None:
                        continue
                    work_work_refs.append((link_id, from_work_id, to_work_id))

            found_members.add(name)
            if found_members == target_members:
                log.info("    all target members collected; stopping tar scan early")
                break

    log.info("[2/6] Resolving metadata maps...")

    for rec in works_records:
        wt_id = rec["work_type_id"]
        rec["work_type_name"] = _clean_text(work_type_map.get(wt_id or -1))

    work_title_by_id: dict[int, str] = {}
    work_mbid_by_id: dict[int, str] = {}
    for rec in works_records:
        wid = rec.get("work_id")
        title = rec.get("work_title")
        if isinstance(wid, int) and isinstance(title, str) and title.strip():
            work_title_by_id[wid] = title.strip()
        mbid = rec.get("musicbrainz_workid")
        if isinstance(wid, int) and isinstance(mbid, str) and mbid.strip():
            work_mbid_by_id[wid] = mbid.strip()

    resolved_work_lang: list[dict[str, object]] = []
    for work_id, (lang_id, _is_primary) in work_lang_choice.items():
        lang_code, lang_name = language_map.get(lang_id, (None, None))
        resolved_work_lang.append(
            {
                "work_id": work_id,
                "language_id": lang_id,
                "language_code": lang_code,
                "language_name": lang_name,
            }
        )

    attrs_by_link: dict[int, list[dict[str, object]]] = {}
    all_link_ids_with_attrs = set(link_attr_types_by_link.keys()) | {k[0] for k in link_attr_text_by_key.keys()} | {k[0] for k in link_attr_credit_by_key.keys()}
    for link_id in all_link_ids_with_attrs:
        resolved_attrs: list[dict[str, object]] = []
        attr_type_ids = set(link_attr_types_by_link.get(link_id, set()))
        attr_type_ids.update(k[1] for k in link_attr_text_by_key.keys() if k[0] == link_id)
        attr_type_ids.update(k[1] for k in link_attr_credit_by_key.keys() if k[0] == link_id)
        for attr_type_id in sorted(attr_type_ids):
            resolved_attrs.append(
                {
                    "attribute_type_id": attr_type_id,
                    "attribute_name": attr_type_name.get(attr_type_id or -1),
                    "attribute_text_value": link_attr_text_by_key.get((link_id, attr_type_id)),
                    "credited_as": link_attr_credit_by_key.get((link_id, attr_type_id)),
                }
            )
        attrs_by_link[link_id] = resolved_attrs

    log.info("[3/6] Aggregating relationship roles and lineage...")

    role_label_to_names_by_work: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    role_label_to_mbids_by_work: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for link_id, artist_id, work_id in artist_work_refs:
        link_type_id, _ended = link_map.get(link_id, (None, 0))
        rel_name, phrase_fwd, phrase_rev = link_type_map.get(link_type_id or -1, (None, None, None))
        roles = _derive_explicit_role_labels(rel_name, phrase_fwd, phrase_rev, attrs_by_link.get(link_id, []))
        if not roles:
            continue

        artist_name = artist_name_by_id.get(artist_id)
        artist_mbid = artist_mbid_by_id.get(artist_id)
        for role in roles:
            if artist_name:
                role_label_to_names_by_work[work_id][role].add(artist_name)
            if artist_mbid:
                role_label_to_mbids_by_work[work_id][role].add(artist_mbid)

    related_work_ids_by_work: dict[int, set[int]] = defaultdict(set)
    related_work_relname_by_work: dict[int, set[str]] = defaultdict(set)

    for link_id, from_work_id, to_work_id in work_work_refs:
        link_type_id, _ended = link_map.get(link_id, (None, 0))
        rel_name, _fwd, _rev = link_type_map.get(link_type_id or -1, (None, None, None))

        related_work_ids_by_work[from_work_id].add(to_work_id)
        related_work_ids_by_work[to_work_id].add(from_work_id)
        if rel_name:
            related_work_relname_by_work[from_work_id].add(rel_name)
            related_work_relname_by_work[to_work_id].add(rel_name)

    log.info("[4/6] Building Polars frames and aggregates...")

    works_df = pl.DataFrame(
        works_records,
        schema={
            "work_id": pl.Int32,
            "musicbrainz_workid": pl.Utf8,
            "work_title": pl.Utf8,
            "work_type_id": pl.Int32,
            "work_type_name": pl.Utf8,
            "work_disambiguation": pl.Utf8,
            "source_dump": pl.Utf8,
            "extracted_utc": pl.Utf8,
        },
    ).with_columns(pl.col("work_type_name").cast(pl.Categorical))

    work_lang_df = (
        pl.DataFrame(
            resolved_work_lang,
            schema={
                "work_id": pl.Int32,
                "language_id": pl.Int32,
                "language_code": pl.Utf8,
                "language_name": pl.Utf8,
            },
        )
        if resolved_work_lang
        else pl.DataFrame(schema={"work_id": pl.Int32, "language_id": pl.Int32, "language_code": pl.Utf8, "language_name": pl.Utf8})
    ).with_columns(
        pl.col("language_code").cast(pl.Categorical),
        pl.col("language_name").cast(pl.Categorical),
    )

    aliases_df = (
        pl.DataFrame(alias_records, schema={"work_id": pl.Int32, "alias": pl.Utf8})
        if alias_records
        else pl.DataFrame(schema={"work_id": pl.Int32, "alias": pl.Utf8})
    )
    iswc_df = (
        pl.DataFrame(iswc_records, schema={"work_id": pl.Int32, "iswc": pl.Utf8})
        if iswc_records
        else pl.DataFrame(schema={"work_id": pl.Int32, "iswc": pl.Utf8})
    )

    role_records: list[dict[str, object]] = []
    all_role_work_ids = (
        set(role_label_to_names_by_work.keys())
        | set(role_label_to_mbids_by_work.keys())
    )
    for work_id in all_role_work_ids:
        names_map = role_label_to_names_by_work.get(work_id, {})
        mbids_map = role_label_to_mbids_by_work.get(work_id, {})

        rec: dict[str, object] = {"work_id": work_id}
        rec["musicbrainz_work_role_artist_names"] = _mv_role_pairs(names_map, mv_delim)
        rec["musicbrainz_work_role_artist_mbids"] = _mv_role_pairs(mbids_map, mv_delim)
        role_records.append(rec)

    roles_schema: dict[str, pl.DataType] = {
        "work_id": pl.Int32,
        "musicbrainz_work_role_artist_names": pl.Utf8,
        "musicbrainz_work_role_artist_mbids": pl.Utf8,
    }

    roles_df = (
        pl.DataFrame(role_records, schema=roles_schema)
        if role_records
        else pl.DataFrame(schema=roles_schema)
    )

    related_records = [
        {
            "work_id": work_id,
            "related_work_titles": _mv_sorted_list(
                {work_title_by_id[rid] for rid in ids if rid in work_title_by_id}, mv_delim
            ),
            "related_work_mbids": _mv_sorted_list(
                {work_mbid_by_id[rid] for rid in ids if rid in work_mbid_by_id}, mv_delim
            ),
            "related_work_relationship_names": _mv_sorted_list(related_work_relname_by_work.get(work_id, set()), mv_delim),
        }
        for work_id, ids in related_work_ids_by_work.items()
    ]
    related_df = (
        pl.DataFrame(
            related_records,
            schema={
                "work_id": pl.Int32,
                "related_work_titles": pl.Utf8,
                "related_work_mbids": pl.Utf8,
                "related_work_relationship_names": pl.Utf8,
            },
        )
        if related_records
        else pl.DataFrame(schema={"work_id": pl.Int32, "related_work_titles": pl.Utf8, "related_work_mbids": pl.Utf8, "related_work_relationship_names": pl.Utf8})
    )

    aliases_agg = (
        aliases_df.with_columns(
            pl.col("alias").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("alias_norm")
        )
        .group_by("work_id")
        .agg(
            pl.col("alias").drop_nulls().unique().sort().implode().list.join(mv_delim).alias("aliases"),
            pl.col("alias_norm").drop_nulls().filter(pl.col("alias_norm") != "").unique().sort().implode().list.join(mv_delim).alias("alias_norms"),
        )
        if not aliases_df.is_empty()
        else pl.DataFrame(schema={"work_id": pl.Int32, "aliases": pl.Utf8, "alias_norms": pl.Utf8})
    )

    iswc_agg = (
        iswc_df.group_by("work_id")
        .agg(pl.col("iswc").drop_nulls().unique().sort().implode().list.join(mv_delim).alias("iswc"))
        if not iswc_df.is_empty()
        else pl.DataFrame(schema={"work_id": pl.Int32, "iswc": pl.Utf8})
    )

    log.info("[5/6] Materializing final lookup rows...")

    final_df = (
        works_df
        .join(work_lang_df, on="work_id", how="left")
        .join(aliases_agg, on="work_id", how="left")
        .join(iswc_agg, on="work_id", how="left")
        .join(roles_df, on="work_id", how="left")
        .join(related_df, on="work_id", how="left")
        .with_columns(
            pl.col("work_title").map_elements(_normalize_text, return_dtype=pl.Utf8).alias("work_title_norm")
        )
        .with_columns(
            pl.struct(["work_title_norm", "alias_norms"]).map_elements(
                lambda s: _mv_sorted_list(
                    set(
                        ([s["work_title_norm"]] if s["work_title_norm"] else [])
                        + ([x for x in (s["alias_norms"] or "").split(mv_delim) if x] if s["alias_norms"] else [])
                    ),
                    mv_delim,
                ),
                return_dtype=pl.Utf8,
            ).alias("all_title_norm_tokens")
        )
    )

    required_nullable_cols = (
        "musicbrainz_workid",
        "musicbrainz_work_role_artist_names",
        "musicbrainz_work_role_artist_mbids",
        "related_work_titles",
        "related_work_mbids",
        "related_work_relationship_names",
        "aliases",
        "iswc",
    )
    for col_name in required_nullable_cols:
        if col_name not in final_df.columns:
            final_df = final_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col_name))

    final_df = final_df.select(
        [
            "work_id",
            "musicbrainz_workid",
            "work_title",
            "work_title_norm",
            "work_type_name",
            "language_name",
            "work_disambiguation",
            "aliases",
            "iswc",
            "all_title_norm_tokens",
            "musicbrainz_work_role_artist_names",
            "musicbrainz_work_role_artist_mbids",
            "related_work_titles",
            "related_work_mbids",
            "related_work_relationship_names",
            "source_dump",
            "extracted_utc",
        ]
    ).with_columns(
        pl.col("work_type_name").cast(pl.Utf8),
        pl.col("language_name").cast(pl.Utf8),
    )

    db_uri = f"sqlite:///{db_file}"
    final_df.write_database(
        table_name=LOOKUP_TABLE,
        connection=db_uri,
        if_table_exists="replace",
        engine="adbc",
    )

    log.info("[6/6] Wrote %s: %d rows (%.1fs)", LOOKUP_TABLE, len(final_df), time.perf_counter() - t0)


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
