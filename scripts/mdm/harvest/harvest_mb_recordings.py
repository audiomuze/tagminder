"""Extract canonical MusicBrainz recording metadata from mbdump.tar.bz2.

This script is intentionally scoped to the minimal recording identity data
needed to bridge library recording MBIDs into harvested recording→work
relationships.

Inputs (from mbdump.tar.bz2):
- mbdump/recording

Outputs (in the master-data SQLite database):
- musicbrainz_recordings

The output table is dropped/recreated on each run.
"""

from __future__ import annotations

import csv
import argparse
import io
import logging
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from tagminder.core import tm_config
from tagminder.core import tm_db

log = logging.getLogger("harvest_mb_recordings")

MASTER_CONFIG_FILE = "harvest_master_data.toml"
RECORDINGS_TABLE = "musicbrainz_recordings"
INSERT_BATCH_SIZE = 50_000
PROGRESS_LOG_INTERVAL = 500_000
COMMIT_INTERVAL = 500_000


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
    raise FileNotFoundError(f"{MASTER_CONFIG_FILE} not found. Looked in:\n{looked_in}")


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


def _open_recording_stream_via_tar(tar_path: Path) -> tuple[subprocess.Popen[bytes], io.TextIOWrapper]:
    command = ["tar", "-xOjf", str(tar_path), "mbdump/recording"]
    process: subprocess.Popen[bytes] = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None:
        process.kill()
        raise RuntimeError("Failed to open tar stdout stream for mbdump/recording")
    stream = io.TextIOWrapper(process.stdout, encoding="utf-8", errors="replace")
    return process, stream


def _parse_recording_row(row: list[str]) -> tuple[int | None, str | None, str | None]:
    # Observed mbdump/recording layout (9 fields):
    # 0=id, 1=gid, 2=name, 3=artist_credit, 4=length, 5=comment,
    # 6=edits_pending, 7=last_updated, 8=video
    # Example: 12431434\t86c6dd66-0da8-4b0f-8b78-ec9d4f12c5c4\tHere to Go\t...
    recording_id = _to_int(row[0] if len(row) > 0 else None)
    if recording_id is None:
        return (None, None, None)
    if len(row) < 3:
        raise RuntimeError(
            f"Unexpected mbdump/recording row layout (expected >=3 columns, got {len(row)}): {row!r}"
        )
    return (recording_id, _clean_text(row[1]), _clean_text(row[2]))


def _create_table(cursor: sqlite3.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {RECORDINGS_TABLE}")
    cursor.execute(
        f"""
        CREATE TABLE {RECORDINGS_TABLE} (
            recording_id INTEGER PRIMARY KEY,
            recording_mbid TEXT NOT NULL,
            title TEXT,
            source_dump TEXT NOT NULL,
            extracted_utc TEXT NOT NULL
        )
        """
    )
    cursor.execute(f"CREATE INDEX idx_{RECORDINGS_TABLE}_mbid ON {RECORDINGS_TABLE}(recording_mbid)")


def harvest_pipeline() -> None:
    tar_path_str, db_file = _load_musicbrainz_paths()
    tar_path = Path(tar_path_str)
    if not tar_path.exists():
        raise FileNotFoundError(f"MusicBrainz dump archive not found: {tar_path}")

    csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

    start = time.perf_counter()
    log.info("Starting recordings harvest from %s", tar_path)
    conn = tm_db.connect(db_file)
    cursor = conn.cursor()
    count = 0

    try:
        tar_proc, stream = _open_recording_stream_via_tar(tar_path)
        log.info("Streaming mbdump/recording via tar subprocess")

        _create_table(cursor)
        conn.commit()

        extracted_utc = tm_db.utc_now_iso()
        source_dump = tar_path.name
        insert_sql = f"""
            INSERT INTO {RECORDINGS_TABLE}
            (recording_id, recording_mbid, title, source_dump, extracted_utc)
            VALUES (?, ?, ?, ?, ?)
        """

        rows: list[tuple[int, str, str | None, str, str]] = []
        raw_count = 0
        next_log_at = PROGRESS_LOG_INTERVAL
        since_commit = 0

        reader = csv.reader(
            stream,
            delimiter="\t",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        for raw_row in reader:
            raw_count += 1
            recording_id, recording_mbid, title = _parse_recording_row(raw_row)
            if recording_id is None or not recording_mbid:
                continue

            rows.append((recording_id, recording_mbid, title, source_dump, extracted_utc))
            count += 1

            if len(rows) >= INSERT_BATCH_SIZE:
                cursor.executemany(insert_sql, rows)
                since_commit += len(rows)
                rows.clear()

                if count >= next_log_at:
                    elapsed = max(time.perf_counter() - start, 1e-6)
                    rate = count / elapsed
                    log.info(
                        "Loaded %d recordings from %d raw rows (%.0f rows/s)",
                        count,
                        raw_count,
                        rate,
                    )
                    next_log_at += PROGRESS_LOG_INTERVAL

            if since_commit >= COMMIT_INTERVAL:
                conn.commit()
                log.info("Committed %d loaded recordings", count)
                since_commit = 0

        if rows:
            cursor.executemany(insert_sql, rows)
            since_commit += len(rows)

        if since_commit:
            conn.commit()
            log.info("Committed %d loaded recordings", count)

        stream.close()
        stderr_output = b""
        if tar_proc.stderr is not None:
            stderr_output = tar_proc.stderr.read()
            tar_proc.stderr.close()
        return_code = tar_proc.wait()
        if return_code != 0:
            stderr_text = stderr_output.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"tar subprocess failed ({return_code}): {stderr_text}")
    finally:
        conn.close()

    elapsed = time.perf_counter() - start
    log.info("Wrote %d recordings to %s in %.1fs", count, RECORDINGS_TABLE, elapsed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract canonical MusicBrainz recording metadata into master-data SQLite.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parse_args()
    harvest_pipeline()


if __name__ == "__main__":
    main()