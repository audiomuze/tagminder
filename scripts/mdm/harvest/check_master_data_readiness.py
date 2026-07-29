"""Validate master-data database table readiness and print remediation commands.

Purpose:
    Check whether required master-data tables exist in the configured
    master-data database and report exactly which harvest scripts to run
    when tables are missing.

This script is intentionally non-mutating.
It does not create tables and does not auto-run harvest scripts.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

from tagminder.core import tm_config
from tagminder.core import tm_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("check_master_data_readiness")


@dataclass(frozen=True)
class Requirement:
    table: str
    owner: str
    remediation: str


REQUIREMENTS: tuple[Requirement, ...] = (
    Requirement(
        table="musicbrainz_artists",
        owner="harvest_mb_artists.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_artists.py",
    ),
    Requirement(
        table="musicbrainz_artist_relationships",
        owner="harvest_mb_artist_relationships.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_artist_relationships.py",
    ),
    Requirement(
        table="musicbrainz_artist_relationship_attributes",
        owner="harvest_mb_artist_relationships.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_artist_relationships.py",
    ),
    Requirement(
        table="musicbrainz_work_artist_relationships",
        owner="harvest_mb_works.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_works.py",
    ),
    Requirement(
        table="musicbrainz_work_work_relationships",
        owner="harvest_mb_works.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_works.py",
    ),
    Requirement(
        table="musicbrainz_work_url_relationships",
        owner="harvest_mb_works.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_works.py",
    ),
    Requirement(
        table="musicbrainz_work_relationship_attributes",
        owner="harvest_mb_works.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/harvest_mb_works.py",
    ),
    Requirement(
        table="contributors_unified_disambiguated",
        owner="emit_contributors.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/emit_contributors.py",
    ),
    Requirement(
        table="contributors_unified_namesakes",
        owner="emit_contributors.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/mdm/harvest/emit_contributors.py",
    ),
    Requirement(
        table="_REF_vetted_contributors",
        owner="curation",
        remediation="Populate curated mappings into _REF_vetted_contributors (no harvest auto-bootstrap).",
    ),
    Requirement(
        table="_REF_genres",
        owner="curation",
        remediation="Populate curated validation values into _REF_genres (expects column genre_name).",
    ),
    Requirement(
        table="_REF_contributors_workspace",
        owner="15-contributor-similarity-analysis.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/pipeline/15-contributor-similarity-analysis.py --db /tmp/amg/tagminder-staging.db",
    ),
    Requirement(
        table="master_data_changelog",
        owner="core helper",
        remediation="This table is auto-created by scripts that write master-data changelog entries.",
    ),
    Requirement(
        table="_USR_disambiguation_decisions",
        owner="18-populate-musicbrainz-ids.py",
        remediation="uv run --project /home/x/tm /home/x/tm/scripts/pipeline/18-populate-musicbrainz-ids.py --db /tmp/amg/tagminder-staging.db",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check master-data table readiness and print remediation commands.",
    )
    parser.add_argument(
        "--db",
        default=tm_config.master_data_db_path_from_toml(default=None),
        help="Path to master-data SQLite database (default: tagminder.toml [master_data].path)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 when any required table is missing.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.db:
        raise SystemExit("No master-data DB path resolved: set tagminder.toml [master_data].path or pass --db PATH")

    conn = tm_db.connect(args.db)
    try:
        print("Master Data Readiness")
        print("=====================")
        print(f"DB: {args.db}")

        missing: list[Requirement] = []
        present_count = 0

        for req in REQUIREMENTS:
            exists = tm_db.table_exists(conn, req.table)
            if exists:
                present_count += 1
                print(f"[OK] {req.table}")
            else:
                missing.append(req)
                print(f"[MISSING] {req.table} (owner: {req.owner})")

        print("\nSummary")
        print("-------")
        print(f"Present: {present_count}")
        print(f"Missing: {len(missing)}")

        if missing:
            print("\nRemediation")
            print("-----------")
            emitted: set[str] = set()
            for req in missing:
                if req.remediation in emitted:
                    continue
                emitted.add(req.remediation)
                print(f"- {req.remediation}")

        if args.strict and missing:
            raise SystemExit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
