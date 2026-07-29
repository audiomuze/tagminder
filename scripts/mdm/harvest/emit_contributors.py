"""Emit unified contributor rows from MusicBrainz, AllMusic, and Wikimedia.

This script is intentionally merge-only. It does not parse dump archives.
It expects upstream source tables/databases to already exist.

Sources:
- musicbrainz_artists (from harvest_mb_artists.py)
- amg_artists, amg_artist_genres, amg_artist_styles (optional)
- wikidata_music_identity (optional, configurable)

Output:
- contributors_unified_disambiguated
- contributors_unified_namesakes

All overlapping fields are source-prefixed to preserve provenance and support
alignment/conflict analysis.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from pathlib import Path

import polars as pl

from tagminder.core import tm_config
from tagminder.core import tm_polars_db
log = logging.getLogger("emit_contributors")

DISAMBIGUATED_TABLE = "contributors_unified_disambiguated"
NAMESAKES_TABLE = "contributors_unified_namesakes"
DEFAULT_MB_TABLE = "musicbrainz_artists"
DEFAULT_WD_TABLE = "wikidata_music_identity"
WD_ONLY_MISSING_MB_REVIEW_TABLE = (
    "EXCEPTION_wikidata_music_identity_mbid_not_in_musicbrainz_artists_review"
)
WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE = "wikimedia_data_quality_issues"
UNMATCHED_WD_TABLE = "unmatched_wikidata_music_identity"
UNMATCHED_AMG_TABLE = "unmatched_amg_artists"
MASTER_CONFIG_FILE = "harvest_master_data.toml"


def _normalize_exact_name_expr(column: str | pl.Expr) -> pl.Expr:
    expr = pl.col(column) if isinstance(column, str) else column
    return (
        pl.when(expr.is_null() | (expr.cast(pl.Utf8).str.strip_chars() == ""))
        .then(None)
        .otherwise(
            expr.cast(pl.Utf8)
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .str.to_lowercase()
        )
    )


def _extract_year_expr(column: str | pl.Expr) -> pl.Expr:
    expr = pl.col(column) if isinstance(column, str) else column
    return (
        pl.when(expr.is_null() | (expr.cast(pl.Utf8).str.strip_chars() == ""))
        .then(None)
        .otherwise(expr.cast(pl.Utf8).str.extract(r"^(\d{4})", 1).cast(pl.Int64))
    )


def _parse_amg_active_window_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("active_start_year"),
            pl.lit(None, dtype=pl.Int64).alias("active_end_year"),
        )

    return df.with_columns(
        pl.when(pl.col("active").is_null() | (pl.col("active").str.strip_chars() == ""))
        .then(None)
        .otherwise(pl.col("active").cast(pl.Utf8).str.extract(r"^(\d{4})", 1).cast(pl.Int64))
        .alias("active_start_decade"),
        pl.when(pl.col("active").is_null() | (pl.col("active").str.strip_chars() == ""))
        .then(None)
        .otherwise(
            pl.col("active")
            .cast(pl.Utf8)
            .str.extract(r"^\d{4}s(?:\s*[-–]\s*(\d{4})s)?$", 1)
            .cast(pl.Int64)
        )
        .alias("active_end_decade"),
    ).with_columns(
        pl.col("active_start_decade").alias("active_start_year"),
        pl.when(pl.col("active_end_decade").is_null())
        .then(pl.col("active_start_decade"))
        .otherwise(pl.col("active_end_decade"))
        .alias("active_end_decade_filled"),
    ).with_columns(
        pl.col("active_start_year"),
        (pl.col("active_end_decade_filled") + 9).alias("active_end_year"),
    ).drop(["active_start_decade", "active_end_decade", "active_end_decade_filled"])


def _mb_gender_text_expr(column: str | pl.Expr) -> pl.Expr:
    expr = pl.col(column) if isinstance(column, str) else column
    return (
        pl.when(expr == 1)
        .then(pl.lit("male"))
        .when(expr == 2)
        .then(pl.lit("female"))
        .otherwise(None)
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


def _resolve_path(raw_value: str, config_dir: Path) -> str:
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        path = (config_dir / path).resolve()
    return str(path)


def _table_exists(conn: sqlite3.Connection, db_alias: str, table_name: str) -> bool:
    cur = conn.execute(
        f"SELECT 1 FROM {db_alias}.sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _log_output_table_stats(cursor: sqlite3.Cursor, output_table: str) -> None:
    # Set-related summary metrics requested by user.
    three_way = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_allmusic_row=1 AND has_wikimedia_row=1"
    ).fetchone()[0]

    mb_am = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_allmusic_row=1"
    ).fetchone()[0]
    mb_wd = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_wikimedia_row=1"
    ).fetchone()[0]
    am_wd = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_allmusic_row=1 AND has_wikimedia_row=1"
    ).fetchone()[0]

    mb_only = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_allmusic_row=0 AND has_wikimedia_row=0"
    ).fetchone()[0]
    am_only = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=0 AND has_allmusic_row=1 AND has_wikimedia_row=0"
    ).fetchone()[0]
    wd_only = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=0 AND has_allmusic_row=0 AND has_wikimedia_row=1"
    ).fetchone()[0]

    total_rows = cursor.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]

    log.info("Unified table ready: %s rows=%d", output_table, total_rows)
    log.info("Set stats: intersection(all 3)=%d", three_way)
    log.info("Set stats: intersection(musicbrainz,allmusic)=%d", mb_am)
    log.info("Set stats: intersection(musicbrainz,wikimedia)=%d", mb_wd)
    log.info("Set stats: intersection(allmusic,wikimedia)=%d", am_wd)
    log.info("Set stats: only musicbrainz=%d", mb_only)
    log.info("Set stats: only allmusic=%d", am_only)
    log.info("Set stats: only wikimedia=%d", wd_only)


def _ensure_preferred_name_columns(cursor: sqlite3.Cursor, source_table: str) -> None:
    for col in ("preferred__artist_name", "lpreferred__artist_name"):
        try:
            cursor.execute(f"ALTER TABLE {source_table} ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            # Column already exists.
            pass

    cursor.execute(
        f"""
        UPDATE {source_table}
        SET preferred__artist_name = CASE
            WHEN allmusic_artist IS NOT NULL AND TRIM(allmusic_artist) <> '' THEN TRIM(allmusic_artist)
            WHEN musicbrainz_artist_name IS NOT NULL AND TRIM(musicbrainz_artist_name) <> '' THEN TRIM(musicbrainz_artist_name)
            ELSE NULL
        END
        """
    )
    cursor.execute(
        f"""
        UPDATE {source_table}
        SET lpreferred__artist_name = CASE
            WHEN preferred__artist_name IS NULL OR TRIM(preferred__artist_name) = '' THEN NULL
            ELSE LOWER(TRIM(preferred__artist_name))
        END
        """
    )


def _emit_split_unified_tables(
    cursor: sqlite3.Cursor,
    *,
    source_table: str,
    disambiguated_table: str,
    namesakes_table: str,
) -> None:
    cursor.execute("DROP TABLE IF EXISTS __contributors_unified_name_counts")
    cursor.execute(
        f"""
        CREATE TEMP TABLE __contributors_unified_name_counts AS
        SELECT lpreferred__artist_name, COUNT(*) AS c
        FROM {source_table}
        WHERE lpreferred__artist_name IS NOT NULL
        GROUP BY lpreferred__artist_name
        HAVING COUNT(*) > 1
        """
    )

    cursor.execute(f"DROP TABLE IF EXISTS {disambiguated_table}")
    cursor.execute(
        f"""
        CREATE TABLE {disambiguated_table} AS
        SELECT *
        FROM {source_table}
        WHERE lpreferred__artist_name IS NULL
           OR lpreferred__artist_name NOT IN (
                SELECT lpreferred__artist_name FROM __contributors_unified_name_counts
           )
        """
    )

    cursor.execute(f"DROP TABLE IF EXISTS {namesakes_table}")
    cursor.execute(
        f"""
        CREATE TABLE {namesakes_table} AS
        SELECT *
        FROM {source_table}
        WHERE lpreferred__artist_name IN (
            SELECT lpreferred__artist_name FROM __contributors_unified_name_counts
        )
        """
    )

    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{disambiguated_table}_lpreferred ON {disambiguated_table}(lpreferred__artist_name)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{disambiguated_table}_mbid ON {disambiguated_table}(merge_key_mbid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{namesakes_table}_lpreferred ON {namesakes_table}(lpreferred__artist_name)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{namesakes_table}_mbid ON {namesakes_table}(merge_key_mbid)"
    )


def _build_norm_tables_for_diagnostics(
    cursor: sqlite3.Cursor,
    *,
    mb_table: str,
    wd_table: str,
    has_allmusic: bool,
    has_wikimedia: bool,
) -> None:
    conn = cursor.connection
    cursor.execute("DROP TABLE IF EXISTS mb_norm_t")
    mb_source_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            artist_id,
            mbid,
            allmusic_mnid,
            wikidata_id
        FROM {mb_table}
        """,
        dtype_overrides={"artist_id": pl.Int64()},
    )
    mb_norm_df = mb_source_df.with_columns(
        pl.when(pl.col("mbid").is_null())
        .then(None)
        .otherwise(
            pl.when(pl.col("mbid").str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col("mbid").str.strip_chars().str.to_lowercase())
        )
        .alias("mbid_n"),
        pl.when(pl.col("allmusic_mnid").is_null())
        .then(None)
        .otherwise(
            pl.when(pl.col("allmusic_mnid").str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col("allmusic_mnid").str.strip_chars().str.to_lowercase())
        )
        .alias("mnid_n"),
        pl.when(pl.col("wikidata_id").is_null())
        .then(None)
        .otherwise(
            pl.when(pl.col("wikidata_id").str.strip_chars() == "")
            .then(None)
            .otherwise(pl.col("wikidata_id").str.strip_chars().str.to_uppercase())
        )
        .alias("qid_n"),
    ).select(["artist_id", "mbid_n", "mnid_n", "qid_n"])

    cursor.execute(
        """
        CREATE TEMP TABLE mb_norm_t (
            artist_id INTEGER,
            mbid_n TEXT,
            mnid_n TEXT,
            qid_n TEXT
        )
        """
    )
    if mb_norm_df.height:
        cursor.executemany(
            "INSERT INTO mb_norm_t (artist_id, mbid_n, mnid_n, qid_n) VALUES (?, ?, ?, ?)",
            mb_norm_df.iter_rows(),
        )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_norm_artist ON mb_norm_t(artist_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_norm_mbid ON mb_norm_t(mbid_n)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_norm_mnid ON mb_norm_t(mnid_n)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_norm_qid ON mb_norm_t(qid_n)")

    cursor.execute("DROP TABLE IF EXISTS amg_rollup_t")
    if has_allmusic:
        has_amg_genres = (
            cursor.execute(
                "SELECT 1 FROM amg.sqlite_master WHERE type='table' AND name='amg_artist_genres'"
            ).fetchone()
            is not None
        )
        has_amg_styles = (
            cursor.execute(
                "SELECT 1 FROM amg.sqlite_master WHERE type='table' AND name='amg_artist_styles'"
            ).fetchone()
            is not None
        )

        amg_artists_df = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT
                mnid,
                artist_input,
                allmusic_artist,
                allmusic_url,
                name_similarity,
                active,
                born_date,
                born_place,
                biography_html,
                enrichment_status,
                first_seen_utc,
                last_seen_utc,
                last_enriched_utc,
                last_source_mode,
                raw_payload_json
            FROM amg.amg_artists
            """,
            dtype_overrides={"name_similarity": pl.Float64()},
        )

        cursor.execute("DROP TABLE IF EXISTS amg_genres_t")
        if has_amg_genres:
            amg_genres_df = tm_polars_db.sqlite_to_polars(
                conn,
                "SELECT mnid, value FROM amg.amg_artist_genres",
            )
            amg_genres_rollup_df = (
                amg_genres_df
                .group_by("mnid")
                .agg(pl.col("value"))
                .with_columns(
                    pl.col("value")
                    .map_elements(
                        lambda vals: json.dumps(vals.to_list() if hasattr(vals, "to_list") else vals),
                        return_dtype=pl.Utf8,
                    )
                    .alias("genres_json"),
                    pl.col("value").list.len().cast(pl.Int64).alias("genre_count"),
                )
                .select(["mnid", "genres_json", "genre_count"])
            )
            cursor.execute(
                """
                CREATE TEMP TABLE amg_genres_t (
                    mnid TEXT,
                    genres_json TEXT,
                    genre_count INTEGER
                )
                """
            )
            if amg_genres_rollup_df.height:
                cursor.executemany(
                    "INSERT INTO amg_genres_t (mnid, genres_json, genre_count) VALUES (?, ?, ?)",
                    amg_genres_rollup_df.select(["mnid", "genres_json", "genre_count"]).iter_rows(),
                )
        else:
            cursor.execute(
                """
                CREATE TEMP TABLE amg_genres_t AS
                SELECT
                    CAST(NULL AS TEXT) AS mnid,
                    CAST(NULL AS TEXT) AS genres_json,
                    CAST(NULL AS INTEGER) AS genre_count
                WHERE 0
                """
            )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_amg_genres_mnid ON amg_genres_t(mnid)")

        cursor.execute("DROP TABLE IF EXISTS amg_styles_t")
        if has_amg_styles:
            amg_styles_df = tm_polars_db.sqlite_to_polars(
                conn,
                "SELECT mnid, value FROM amg.amg_artist_styles",
            )
            amg_styles_rollup_df = (
                amg_styles_df
                .group_by("mnid")
                .agg(pl.col("value"))
                .with_columns(
                    pl.col("value")
                    .map_elements(
                        lambda vals: json.dumps(vals.to_list() if hasattr(vals, "to_list") else vals),
                        return_dtype=pl.Utf8,
                    )
                    .alias("styles_json"),
                    pl.col("value").list.len().cast(pl.Int64).alias("style_count"),
                )
                .select(["mnid", "styles_json", "style_count"])
            )
            cursor.execute(
                """
                CREATE TEMP TABLE amg_styles_t (
                    mnid TEXT,
                    styles_json TEXT,
                    style_count INTEGER
                )
                """
            )
            if amg_styles_rollup_df.height:
                cursor.executemany(
                    "INSERT INTO amg_styles_t (mnid, styles_json, style_count) VALUES (?, ?, ?)",
                    amg_styles_rollup_df.select(["mnid", "styles_json", "style_count"]).iter_rows(),
                )
        else:
            cursor.execute(
                """
                CREATE TEMP TABLE amg_styles_t AS
                SELECT
                    CAST(NULL AS TEXT) AS mnid,
                    CAST(NULL AS TEXT) AS styles_json,
                    CAST(NULL AS INTEGER) AS style_count
                WHERE 0
                """
            )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_amg_styles_mnid ON amg_styles_t(mnid)")

        amg_genres_join_df = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT mnid, genres_json, genre_count FROM amg_genres_t",
            dtype_overrides={"genre_count": pl.Int64()},
        )
        amg_styles_join_df = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT mnid, styles_json, style_count FROM amg_styles_t",
            dtype_overrides={"style_count": pl.Int64()},
        )

        # Dataframe-first rollup: normalize keys and join pre-aggregated genre/style payloads.
        amg_rollup_df = (
            amg_artists_df
            .join(amg_genres_join_df, on="mnid", how="left")
            .join(amg_styles_join_df, on="mnid", how="left")
            .with_columns(
                pl.when(pl.col("mnid").is_null())
                .then(None)
                .otherwise(
                    pl.when(pl.col("mnid").str.strip_chars() == "")
                    .then(None)
                    .otherwise(pl.col("mnid").str.strip_chars().str.to_lowercase())
                )
                .alias("mnid_n")
            )
            .select(
                [
                    "mnid",
                    "mnid_n",
                    "artist_input",
                    "allmusic_artist",
                    "allmusic_url",
                    "name_similarity",
                    "active",
                    "born_date",
                    "born_place",
                    "biography_html",
                    "enrichment_status",
                    "first_seen_utc",
                    "last_seen_utc",
                    "last_enriched_utc",
                    "last_source_mode",
                    "raw_payload_json",
                    "genres_json",
                    "styles_json",
                    "genre_count",
                    "style_count",
                ]
            )
        )

        cursor.execute(
            """
            CREATE TEMP TABLE amg_rollup_t (
                mnid TEXT,
                mnid_n TEXT,
                artist_input TEXT,
                allmusic_artist TEXT,
                allmusic_url TEXT,
                name_similarity REAL,
                active TEXT,
                born_date TEXT,
                born_place TEXT,
                biography_html TEXT,
                enrichment_status TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_enriched_utc TEXT,
                last_source_mode TEXT,
                raw_payload_json TEXT,
                genres_json TEXT,
                styles_json TEXT,
                genre_count INTEGER,
                style_count INTEGER
            )
            """
        )
        if amg_rollup_df.height:
            cursor.executemany(
                """
                INSERT INTO amg_rollup_t (
                    mnid,
                    mnid_n,
                    artist_input,
                    allmusic_artist,
                    allmusic_url,
                    name_similarity,
                    active,
                    born_date,
                    born_place,
                    biography_html,
                    enrichment_status,
                    first_seen_utc,
                    last_seen_utc,
                    last_enriched_utc,
                    last_source_mode,
                    raw_payload_json,
                    genres_json,
                    styles_json,
                    genre_count,
                    style_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                amg_rollup_df.iter_rows(),
            )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_amg_rollup_mnid_n ON amg_rollup_t(mnid_n)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_amg_rollup_mnid ON amg_rollup_t(mnid)")
    else:
        cursor.execute(
            """
            CREATE TEMP TABLE amg_rollup_t AS
            SELECT
                CAST(NULL AS TEXT) AS mnid,
                CAST(NULL AS TEXT) AS mnid_n,
                CAST(NULL AS TEXT) AS artist_input,
                CAST(NULL AS TEXT) AS allmusic_artist,
                CAST(NULL AS TEXT) AS allmusic_url,
                CAST(NULL AS REAL) AS name_similarity,
                CAST(NULL AS TEXT) AS active,
                CAST(NULL AS TEXT) AS born_date,
                CAST(NULL AS TEXT) AS born_place,
                CAST(NULL AS TEXT) AS biography_html,
                CAST(NULL AS TEXT) AS enrichment_status,
                CAST(NULL AS TEXT) AS first_seen_utc,
                CAST(NULL AS TEXT) AS last_seen_utc,
                CAST(NULL AS TEXT) AS last_enriched_utc,
                CAST(NULL AS TEXT) AS last_source_mode,
                CAST(NULL AS TEXT) AS raw_payload_json,
                CAST(NULL AS TEXT) AS genres_json,
                CAST(NULL AS TEXT) AS styles_json,
                CAST(NULL AS INTEGER) AS genre_count,
                CAST(NULL AS INTEGER) AS style_count
            WHERE 0
            """
        )

    cursor.execute("DROP TABLE IF EXISTS wd_norm_t")
    if has_wikimedia:
        conn = cursor.connection
        wd_source_df = tm_polars_db.sqlite_to_polars(
            conn,
            f"""
            SELECT
                rowid AS wd_rowid,
                wikidata_uri,
                wikidata_id,
                wikidata_label,
                wikidata_aliases,
                mbid,
                allmusic_mnid,
                songkick_artist_id,
                apple_music_artist_id,
                discogs_artist_id,
                spotify_artist_id,
                lastfm_artist_id,
                youtube_channel_id,
                isni,
                viaf_id,
                official_website,
                gender,
                instance_of_wikidata_ids,
                occupation_wikidata_ids,
                citizenship_wikidata_ids,
                origin_country_wikidata_ids,
                place_of_birth_wikidata_id,
                place_of_death_wikidata_id,
                date_of_birth,
                date_of_death,
                inception,
                dissolved,
                genre_wikidata_ids,
                instrument_wikidata_ids,
                member_of_wikidata_ids,
                wikidata_url,
                musicbrainz_url,
                allmusic_url,
                discogs_url,
                spotify_url,
                songkick_url,
                wikipedia_url,
                apple_lookup_url,
                source_dump,
                extracted_utc
            FROM wd.{wd_table}
            """,
            dtype_overrides={"wd_rowid": pl.Int64()},
        )

        wd_norm_df = wd_source_df.with_columns(
            pl.when(pl.col("mbid").is_null())
            .then(None)
            .otherwise(
                pl.when(pl.col("mbid").str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col("mbid").str.strip_chars().str.to_lowercase())
            )
            .alias("mbid_n"),
            pl.when(pl.col("allmusic_mnid").is_null())
            .then(None)
            .otherwise(
                pl.when(pl.col("allmusic_mnid").str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col("allmusic_mnid").str.strip_chars().str.to_lowercase())
            )
            .alias("mnid_n"),
            pl.when(
                pl.col("wikidata_id").is_not_null() & (pl.col("wikidata_id").str.strip_chars() != "")
            )
            .then(pl.col("wikidata_id"))
            .when(pl.col("wikidata_uri").is_null() | (pl.col("wikidata_uri").str.strip_chars() == ""))
            .then(None)
            .otherwise(
                pl.when(pl.col("wikidata_uri").str.contains("Q"))
                .then(pl.col("wikidata_uri").str.extract(r"(Q.*)$", 1))
                .otherwise(pl.col("wikidata_uri"))
            )
            .str.to_uppercase()
            .alias("qid_n"),
        ).select(
            [
                "wd_rowid",
                "mbid_n",
                "mnid_n",
                "qid_n",
                "wikidata_uri",
                "wikidata_id",
                "wikidata_label",
                "wikidata_aliases",
                "mbid",
                "allmusic_mnid",
                "songkick_artist_id",
                "apple_music_artist_id",
                "discogs_artist_id",
                "spotify_artist_id",
                "lastfm_artist_id",
                "youtube_channel_id",
                "isni",
                "viaf_id",
                "official_website",
                "gender",
                "instance_of_wikidata_ids",
                "occupation_wikidata_ids",
                "citizenship_wikidata_ids",
                "origin_country_wikidata_ids",
                "place_of_birth_wikidata_id",
                "place_of_death_wikidata_id",
                "date_of_birth",
                "date_of_death",
                "inception",
                "dissolved",
                "genre_wikidata_ids",
                "instrument_wikidata_ids",
                "member_of_wikidata_ids",
                "wikidata_url",
                "musicbrainz_url",
                "allmusic_url",
                "discogs_url",
                "spotify_url",
                "songkick_url",
                "wikipedia_url",
                "apple_lookup_url",
                "source_dump",
                "extracted_utc",
            ]
        )

        cursor.execute(
            """
            CREATE TEMP TABLE wd_norm_t (
                wd_rowid INTEGER,
                mbid_n TEXT,
                mnid_n TEXT,
                qid_n TEXT,
                wikidata_uri TEXT,
                wikidata_id TEXT,
                wikidata_label TEXT,
                wikidata_aliases TEXT,
                mbid TEXT,
                allmusic_mnid TEXT,
                songkick_artist_id TEXT,
                apple_music_artist_id TEXT,
                discogs_artist_id TEXT,
                spotify_artist_id TEXT,
                lastfm_artist_id TEXT,
                youtube_channel_id TEXT,
                isni TEXT,
                viaf_id TEXT,
                official_website TEXT,
                gender TEXT,
                instance_of_wikidata_ids TEXT,
                occupation_wikidata_ids TEXT,
                citizenship_wikidata_ids TEXT,
                origin_country_wikidata_ids TEXT,
                place_of_birth_wikidata_id TEXT,
                place_of_death_wikidata_id TEXT,
                date_of_birth TEXT,
                date_of_death TEXT,
                inception TEXT,
                dissolved TEXT,
                genre_wikidata_ids TEXT,
                instrument_wikidata_ids TEXT,
                member_of_wikidata_ids TEXT,
                wikidata_url TEXT,
                musicbrainz_url TEXT,
                allmusic_url TEXT,
                discogs_url TEXT,
                spotify_url TEXT,
                songkick_url TEXT,
                wikipedia_url TEXT,
                apple_lookup_url TEXT,
                source_dump TEXT,
                extracted_utc TEXT
            )
            """
        )

        if wd_norm_df.height:
            cursor.executemany(
                """
                INSERT INTO wd_norm_t (
                    wd_rowid,
                    mbid_n,
                    mnid_n,
                    qid_n,
                    wikidata_uri,
                    wikidata_id,
                    wikidata_label,
                    wikidata_aliases,
                    mbid,
                    allmusic_mnid,
                    songkick_artist_id,
                    apple_music_artist_id,
                    discogs_artist_id,
                    spotify_artist_id,
                    lastfm_artist_id,
                    youtube_channel_id,
                    isni,
                    viaf_id,
                    official_website,
                    gender,
                    instance_of_wikidata_ids,
                    occupation_wikidata_ids,
                    citizenship_wikidata_ids,
                    origin_country_wikidata_ids,
                    place_of_birth_wikidata_id,
                    place_of_death_wikidata_id,
                    date_of_birth,
                    date_of_death,
                    inception,
                    dissolved,
                    genre_wikidata_ids,
                    instrument_wikidata_ids,
                    member_of_wikidata_ids,
                    wikidata_url,
                    musicbrainz_url,
                    allmusic_url,
                    discogs_url,
                    spotify_url,
                    songkick_url,
                    wikipedia_url,
                    apple_lookup_url,
                    source_dump,
                    extracted_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                wd_norm_df.iter_rows(),
            )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wd_norm_mbid ON wd_norm_t(mbid_n)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wd_norm_mnid ON wd_norm_t(mnid_n)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wd_norm_qid ON wd_norm_t(qid_n)")


def _log_diagnostics(
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
    has_allmusic: bool,
    has_wikimedia: bool,
) -> None:
    mb_total = cursor.execute("SELECT COUNT(*) FROM mb_norm_t").fetchone()[0]
    mb_with_mbid = cursor.execute("SELECT COUNT(*) FROM mb_norm_t WHERE mbid_n IS NOT NULL").fetchone()[0]
    mb_with_mnid = cursor.execute("SELECT COUNT(*) FROM mb_norm_t WHERE mnid_n IS NOT NULL").fetchone()[0]
    mb_with_qid = cursor.execute("SELECT COUNT(*) FROM mb_norm_t WHERE qid_n IS NOT NULL").fetchone()[0]

    log.info(
        "Diagnostics MB key presence: total=%d mbid_non_null=%d (%.2f%%) mnid_non_null=%d (%.2f%%) qid_non_null=%d (%.2f%%)",
        mb_total,
        mb_with_mbid,
        (100.0 * mb_with_mbid / mb_total) if mb_total else 0.0,
        mb_with_mnid,
        (100.0 * mb_with_mnid / mb_total) if mb_total else 0.0,
        mb_with_qid,
        (100.0 * mb_with_qid / mb_total) if mb_total else 0.0,
    )

    if has_allmusic:
        am_total = cursor.execute("SELECT COUNT(*) FROM amg_rollup_t").fetchone()[0]
        am_with_mnid = cursor.execute("SELECT COUNT(*) FROM amg_rollup_t WHERE mnid_n IS NOT NULL").fetchone()[0]
        log.info(
            "Diagnostics AllMusic key presence: total=%d mnid_non_null=%d (%.2f%%)",
            am_total,
            am_with_mnid,
            (100.0 * am_with_mnid / am_total) if am_total else 0.0,
        )

        mb_am_mnid_overlap = cursor.execute(
            """
            SELECT COUNT(DISTINCT mb.artist_id)
            FROM mb_norm_t mb
            INNER JOIN amg_rollup_t am ON mb.mnid_n = am.mnid_n
            WHERE mb.mnid_n IS NOT NULL
            """
        ).fetchone()[0]
        log.info("Diagnostics pair overlap by key: MB<->AllMusic via mnid=%d", mb_am_mnid_overlap)

        am_mnid_not_in_mb = cursor.execute(
            """
            SELECT COUNT(*)
            FROM amg_rollup_t am
            WHERE am.mnid_n IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM mb_norm_t mb WHERE mb.mnid_n = am.mnid_n
              )
            """
        ).fetchone()[0]
        log.info("Diagnostics unmatched slice: AllMusic mnid not present in MB=%d", am_mnid_not_in_mb)
    else:
        log.info("Diagnostics AllMusic key presence: source unavailable")

    if has_wikimedia:
        wd_total = cursor.execute("SELECT COUNT(*) FROM wd_norm_t").fetchone()[0]
        wd_with_mbid = cursor.execute("SELECT COUNT(*) FROM wd_norm_t WHERE mbid_n IS NOT NULL").fetchone()[0]
        wd_with_mnid = cursor.execute("SELECT COUNT(*) FROM wd_norm_t WHERE mnid_n IS NOT NULL").fetchone()[0]
        wd_with_qid = cursor.execute("SELECT COUNT(*) FROM wd_norm_t WHERE qid_n IS NOT NULL").fetchone()[0]

        log.info(
            "Diagnostics Wikimedia key presence: total=%d mbid_non_null=%d (%.2f%%) mnid_non_null=%d (%.2f%%) qid_non_null=%d (%.2f%%)",
            wd_total,
            wd_with_mbid,
            (100.0 * wd_with_mbid / wd_total) if wd_total else 0.0,
            wd_with_mnid,
            (100.0 * wd_with_mnid / wd_total) if wd_total else 0.0,
            wd_with_qid,
            (100.0 * wd_with_qid / wd_total) if wd_total else 0.0,
        )

        mb_wd_mbid_overlap = cursor.execute(
            """
            SELECT COUNT(DISTINCT mb.artist_id)
            FROM mb_norm_t mb
            INNER JOIN wd_norm_t wd ON mb.mbid_n = wd.mbid_n
            WHERE mb.mbid_n IS NOT NULL
            """
        ).fetchone()[0]
        mb_wd_qid_overlap = cursor.execute(
            """
            SELECT COUNT(DISTINCT mb.artist_id)
            FROM mb_norm_t mb
            INNER JOIN wd_norm_t wd ON mb.qid_n = wd.qid_n
            WHERE mb.qid_n IS NOT NULL
            """
        ).fetchone()[0]
        log.info("Diagnostics pair overlap by key: MB<->Wikimedia via mbid=%d", mb_wd_mbid_overlap)
        log.info("Diagnostics pair overlap by key: MB<->Wikimedia via qid=%d", mb_wd_qid_overlap)

    else:
        log.info("Diagnostics Wikimedia key presence: source unavailable")

    mb_missing_mnid_and_qid = cursor.execute(
        "SELECT COUNT(*) FROM mb_norm_t WHERE mnid_n IS NULL AND qid_n IS NULL"
    ).fetchone()[0]
    log.info("Diagnostics unmatched slice: MB rows with NULL mnid and NULL qid=%d", mb_missing_mnid_and_qid)

    mb_missing_allmusic = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_allmusic_row=0"
    ).fetchone()[0]
    mb_with_wikimedia_missing_allmusic = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_wikimedia_row=1 AND has_allmusic_row=0"
    ).fetchone()[0]
    mb_missing_wikimedia = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE has_musicbrainz_row=1 AND has_wikimedia_row=0"
    ).fetchone()[0]
    log.info(
        "Final-state MB gaps: missing_allmusic=%d missing_wikimedia=%d with_wikimedia_but_missing_allmusic=%d",
        mb_missing_allmusic,
        mb_missing_wikimedia,
        mb_with_wikimedia_missing_allmusic,
    )

    conn = cursor.connection
    mb_baseline_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT artist_id, mnid_n, qid_n
        FROM mb_norm_t
        """,
        dtype_overrides={"artist_id": pl.Int64()},
    )
    unified_mb_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT contributor_row_id, has_allmusic_row, merge_key_wikidata_id
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
        """,
        dtype_overrides={"contributor_row_id": pl.Int64(), "has_allmusic_row": pl.Int64()},
    )

    enrichment_df = unified_mb_df.join(
        mb_baseline_df,
        left_on="contributor_row_id",
        right_on="artist_id",
        how="inner",
    ).with_columns(
        ((pl.col("has_allmusic_row") == 1) & pl.col("mnid_n").is_null()).alias("added_allmusic"),
        (
            pl.col("merge_key_wikidata_id").is_not_null()
            & (pl.col("merge_key_wikidata_id").str.strip_chars() != "")
            & pl.col("qid_n").is_null()
        ).alias("added_qid")
    )

    allmusic_added_vs_mb = int(
        enrichment_df.select(pl.col("added_allmusic").cast(pl.Int64).sum().alias("c")).item(0, "c") or 0
    )
    qid_added_vs_mb = int(
        enrichment_df.select(pl.col("added_qid").cast(pl.Int64).sum().alias("c")).item(0, "c") or 0
    )

    log.info(
        "Enrichment added: allmusic_matches_added_vs_musicbrainz=%d qid_matches_added_vs_musicbrainz=%d",
        allmusic_added_vs_mb,
        qid_added_vs_mb,
    )

    wd_quality_count = 0
    wd_exception_count = 0
    wd_unmatched_count = 0
    if has_wikimedia:
        wd_quality_count = cursor.execute(
            f"SELECT COUNT(*) FROM {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}"
        ).fetchone()[0]
        wd_exception_count = cursor.execute(
            f"SELECT COUNT(*) FROM {WD_ONLY_MISSING_MB_REVIEW_TABLE}"
        ).fetchone()[0]
        wd_unmatched_count = cursor.execute(
            f"SELECT COUNT(*) FROM {UNMATCHED_WD_TABLE}"
        ).fetchone()[0]

    amg_unmatched_count = 0
    if has_allmusic:
        amg_unmatched_count = cursor.execute(
            f"SELECT COUNT(*) FROM {UNMATCHED_AMG_TABLE}"
        ).fetchone()[0]

    log.info(
        "Residual tables: wikimedia_quality=%d wikimedia_exception=%d wikimedia_unmatched=%d allmusic_unmatched=%d",
        wd_quality_count,
        wd_exception_count,
        wd_unmatched_count,
        amg_unmatched_count,
    )


def _build_staged_mb_wd_match_table(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    mb_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT artist_id, mbid_n, qid_n, mnid_n FROM mb_norm_t",
        dtype_overrides={"artist_id": pl.Int64()},
    )
    wd_all_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid, mbid_n, qid_n, mnid_n FROM wd_norm_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    wd_dq_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_data_quality_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    wd_exception_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_exception_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    wd_df = (
        wd_all_df
        .join(wd_dq_df, on="wd_rowid", how="anti")
        .join(wd_exception_df, on="wd_rowid", how="anti")
    )

    # Keep high-cardinality text keys compact during repeated join/sort stages.
    mb_df = mb_df.with_columns(
        pl.col("mbid_n").cast(pl.Categorical),
        pl.col("qid_n").cast(pl.Categorical),
        pl.col("mnid_n").cast(pl.Categorical),
    )
    wd_df = wd_df.with_columns(
        pl.col("mbid_n").cast(pl.Categorical),
        pl.col("qid_n").cast(pl.Categorical),
        pl.col("mnid_n").cast(pl.Categorical),
    )

    mb_res = mb_df
    wd_res = wd_df
    staged_matches: list[pl.DataFrame] = []

    # Stage 1 contract: match by MBID first, then QID, then allow MNID matching
    # only for remaining Wikimedia rows that have no MBID.
    stage_specs = (
        ("mbid_n", "mbid", False),
        ("qid_n", "qid", False),
        ("mnid_n", "mnid", True),
    )

    for key_col, stage_name, wd_requires_null_mbid in stage_specs:
        mb_side = (
            mb_res
            .filter(pl.col(key_col).is_not_null())
            .sort([key_col, "artist_id"])
            .with_columns(pl.col("artist_id").cum_count().over(key_col).alias("__rn"))
            .select(["artist_id", key_col, "__rn"])
        )
        wd_source = wd_res.filter(pl.col(key_col).is_not_null())
        if wd_requires_null_mbid:
            wd_source = wd_source.filter(pl.col("mbid_n").is_null())
        wd_side = (
            wd_source
            .sort([key_col, "wd_rowid"])
            .with_columns(pl.col("wd_rowid").cum_count().over(key_col).alias("__rn"))
            .select(["wd_rowid", key_col, "__rn"])
        )

        stage_matches = (
            mb_side
            .join(wd_side, on=[key_col, "__rn"], how="inner")
            .select(["artist_id", "wd_rowid"])
            .rename({"artist_id": "mb_artist_id"})
            .with_columns(pl.lit(stage_name).alias("match_stage"))
        )

        if stage_matches.height:
            staged_matches.append(stage_matches)
            mb_res = mb_res.join(
                stage_matches.select(pl.col("mb_artist_id").alias("artist_id")),
                on="artist_id",
                how="anti",
            )
            wd_res = wd_res.join(
                stage_matches.select("wd_rowid"),
                on="wd_rowid",
                how="anti",
            )

        # After MBID matching and before QID/MNID matching, quarantine ambiguous
        # duplicated QIDs from remaining Wikimedia rows.
        if stage_name == "mbid":
            dup_qid_keys = (
                wd_res
                .filter(pl.col("qid_n").is_not_null())
                .group_by("qid_n")
                .len()
                .filter(pl.col("len") > 1)
                .select("qid_n")
            )

            dq_qid_count = 0
            if dup_qid_keys.height:
                dq_rows = (
                    wd_res
                    .join(dup_qid_keys, on="qid_n", how="inner")
                    .select("wd_rowid")
                    .unique()
                )
                dq_qid_count = dq_rows.height
                if dq_qid_count:
                    cursor.executemany(
                        "INSERT OR IGNORE INTO wd_data_quality_rowids_t (wd_rowid) VALUES (?)",
                        dq_rows.select("wd_rowid").iter_rows(),
                    )
                    cursor.executemany(
                        "INSERT OR IGNORE INTO wd_data_quality_reasons_t (wd_rowid, reason) VALUES (?, ?)",
                        [
                            (int(row[0]), "Duplicated QID")
                            for row in dq_rows.select("wd_rowid").iter_rows()
                        ],
                    )
                    wd_res = wd_res.join(dq_rows, on="wd_rowid", how="anti")
            log.info(
                "Wikimedia quality gate: quarantined %d rows for duplicated QID before QID stage",
                dq_qid_count,
            )

        # After MBID/QID matching and before MNID matching, quarantine ambiguous
        # duplicated MNIDs from remaining Wikimedia rows.
        if stage_name == "qid":
            dup_mnid_keys = (
                wd_res
                .filter(pl.col("mnid_n").is_not_null())
                .group_by("mnid_n")
                .len()
                .filter(pl.col("len") > 1)
                .select("mnid_n")
            )

            dq_mnid_count = 0
            if dup_mnid_keys.height:
                dq_rows = (
                    wd_res
                    .join(dup_mnid_keys, on="mnid_n", how="inner")
                    .select("wd_rowid")
                    .unique()
                )
                dq_mnid_count = dq_rows.height
                if dq_mnid_count:
                    cursor.executemany(
                        "INSERT OR IGNORE INTO wd_data_quality_rowids_t (wd_rowid) VALUES (?)",
                        dq_rows.select("wd_rowid").iter_rows(),
                    )
                    cursor.executemany(
                        "INSERT OR IGNORE INTO wd_data_quality_reasons_t (wd_rowid, reason) VALUES (?, ?)",
                        [
                            (int(row[0]), "Duplicated AllMusic MNID")
                            for row in dq_rows.select("wd_rowid").iter_rows()
                        ],
                    )
                    wd_res = wd_res.join(dq_rows, on="wd_rowid", how="anti")
            log.info(
                "Wikimedia quality gate: quarantined %d rows for duplicated AllMusic MNID before MNID stage",
                dq_mnid_count,
            )

    if staged_matches:
        all_matches = pl.concat(staged_matches, how="vertical")
    else:
        all_matches = pl.DataFrame(
            {
                "mb_artist_id": pl.Series([], dtype=pl.Int64),
                "wd_rowid": pl.Series([], dtype=pl.Int64),
                "match_stage": pl.Series([], dtype=pl.Utf8),
            }
        )

    cursor.execute("DROP TABLE IF EXISTS mb_wd_match_t")
    cursor.execute(
        """
        CREATE TEMP TABLE mb_wd_match_t (
            mb_artist_id INTEGER NOT NULL,
            wd_rowid INTEGER NOT NULL,
            match_stage TEXT NOT NULL
        )
        """
    )

    if all_matches.height:
        cursor.executemany(
            "INSERT INTO mb_wd_match_t (mb_artist_id, wd_rowid, match_stage) VALUES (?, ?, ?)",
            all_matches.select(["mb_artist_id", "wd_rowid", "match_stage"]).iter_rows(),
        )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_wd_match_mb_artist ON mb_wd_match_t(mb_artist_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_wd_match_wd_rowid ON mb_wd_match_t(wd_rowid)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mb_wd_match_stage ON mb_wd_match_t(match_stage)")


def _refresh_wikimedia_exception_tables(
    cursor: sqlite3.Cursor,
    *,
    wd_table: str,
    mb_table: str,
    has_wikimedia: bool,
) -> None:
    """Build durable exception tables for Wikimedia rows with MBIDs missing from MB."""
    cursor.execute(f"DROP TABLE IF EXISTS {WD_ONLY_MISSING_MB_REVIEW_TABLE}")

    if not has_wikimedia:
        cursor.execute(
            f"""
            CREATE TABLE {WD_ONLY_MISSING_MB_REVIEW_TABLE} (
                wikidata_uri TEXT,
                wikidata_id TEXT,
                wikidata_label TEXT,
                wikidata_aliases TEXT,
                mbid TEXT,
                allmusic_mnid TEXT,
                songkick_artist_id TEXT,
                apple_music_artist_id TEXT,
                discogs_artist_id TEXT,
                spotify_artist_id TEXT,
                lastfm_artist_id TEXT,
                youtube_channel_id TEXT,
                isni TEXT,
                viaf_id TEXT,
                official_website TEXT,
                gender TEXT,
                instance_of_wikidata_ids TEXT,
                occupation_wikidata_ids TEXT,
                citizenship_wikidata_ids TEXT,
                origin_country_wikidata_ids TEXT,
                place_of_birth_wikidata_id TEXT,
                place_of_death_wikidata_id TEXT,
                date_of_birth TEXT,
                date_of_death TEXT,
                inception TEXT,
                dissolved TEXT,
                genre_wikidata_ids TEXT,
                instrument_wikidata_ids TEXT,
                member_of_wikidata_ids TEXT,
                wikidata_url TEXT,
                musicbrainz_url TEXT,
                allmusic_url TEXT,
                discogs_url TEXT,
                spotify_url TEXT,
                songkick_url TEXT,
                wikipedia_url TEXT,
                apple_lookup_url TEXT,
                source_dump TEXT,
                extracted_utc TEXT
            )
            """
        )
        log.info(
            "Exception table refreshed: %s rows=0 (wikimedia source unavailable)",
            WD_ONLY_MISSING_MB_REVIEW_TABLE,
        )
        return

    conn = cursor.connection
    source_cols = [
        row[1]
        for row in cursor.execute("PRAGMA table_info(wd_norm_t)").fetchall()
        if row[1] not in {"wd_rowid", "mbid_n", "mnid_n", "qid_n"}
    ]
    source_col_defs = ",\n            ".join(f"{col} TEXT" for col in source_cols)
    cursor.execute(
        f"""
        CREATE TABLE {WD_ONLY_MISSING_MB_REVIEW_TABLE} (
            {source_col_defs}
        )
        """
    )

    wd_source_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"SELECT wd_rowid, {', '.join(source_cols)} FROM wd_norm_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    wd_norm_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid, mbid_n FROM wd_norm_t WHERE mbid_n IS NOT NULL",
        dtype_overrides={"wd_rowid": pl.Int64()},
    ).with_columns(pl.col("mbid_n").cast(pl.Categorical))
    dq_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_data_quality_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    mb_present_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT DISTINCT LOWER(TRIM(mbid)) AS mbid_n
        FROM {mb_table}
        WHERE mbid IS NOT NULL
          AND TRIM(mbid) <> ''
        """,
    ).with_columns(pl.col("mbid_n").cast(pl.Categorical))

    review_rowids_df = (
        wd_norm_df
        .join(dq_df, on="wd_rowid", how="anti")
        .join(mb_present_df, on="mbid_n", how="anti")
        .select("wd_rowid")
    )
    review_df = wd_source_df.join(review_rowids_df, on="wd_rowid", how="inner").select(source_cols)

    if review_df.height:
        insert_cols = ", ".join(source_cols)
        placeholders = ", ".join(["?"] * len(source_cols))
        cursor.executemany(
            f"INSERT INTO {WD_ONLY_MISSING_MB_REVIEW_TABLE} ({insert_cols}) VALUES ({placeholders})",
            review_df.iter_rows(),
        )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{WD_ONLY_MISSING_MB_REVIEW_TABLE}_mbid "
        f"ON {WD_ONLY_MISSING_MB_REVIEW_TABLE}(mbid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{WD_ONLY_MISSING_MB_REVIEW_TABLE}_qid "
        f"ON {WD_ONLY_MISSING_MB_REVIEW_TABLE}(wikidata_id)"
    )

    review_count = cursor.execute(
        f"SELECT COUNT(*) FROM {WD_ONLY_MISSING_MB_REVIEW_TABLE}"
    ).fetchone()[0]
    log.info(
        "Exception table ready: %s rows=%d",
        WD_ONLY_MISSING_MB_REVIEW_TABLE,
        review_count,
    )


def _refresh_unmatched_wikimedia_table(
    cursor: sqlite3.Cursor,
    *,
    wd_table: str,
    has_wikimedia: bool,
) -> None:
    """Persist non-erroneous Wikimedia rows that were not matched to MB."""
    cursor.execute(f"DROP TABLE IF EXISTS {UNMATCHED_WD_TABLE}")

    if not has_wikimedia:
        cursor.execute(
            f"""
            CREATE TABLE {UNMATCHED_WD_TABLE} (
                wikidata_uri TEXT,
                wikidata_id TEXT,
                wikidata_label TEXT,
                wikidata_aliases TEXT,
                mbid TEXT,
                allmusic_mnid TEXT,
                songkick_artist_id TEXT,
                apple_music_artist_id TEXT,
                discogs_artist_id TEXT,
                spotify_artist_id TEXT,
                lastfm_artist_id TEXT,
                youtube_channel_id TEXT,
                isni TEXT,
                viaf_id TEXT,
                official_website TEXT,
                gender TEXT,
                instance_of_wikidata_ids TEXT,
                occupation_wikidata_ids TEXT,
                citizenship_wikidata_ids TEXT,
                origin_country_wikidata_ids TEXT,
                place_of_birth_wikidata_id TEXT,
                place_of_death_wikidata_id TEXT,
                date_of_birth TEXT,
                date_of_death TEXT,
                inception TEXT,
                dissolved TEXT,
                genre_wikidata_ids TEXT,
                instrument_wikidata_ids TEXT,
                member_of_wikidata_ids TEXT,
                wikidata_url TEXT,
                musicbrainz_url TEXT,
                allmusic_url TEXT,
                discogs_url TEXT,
                spotify_url TEXT,
                songkick_url TEXT,
                wikipedia_url TEXT,
                apple_lookup_url TEXT,
                source_dump TEXT,
                extracted_utc TEXT
            )
            """
        )
        log.info("Unmatched table ready: %s rows=0 (wikimedia source unavailable)", UNMATCHED_WD_TABLE)
        return

    conn = cursor.connection
    source_cols = [
        row[1]
        for row in cursor.execute("PRAGMA table_info(wd_norm_t)").fetchall()
        if row[1] not in {"wd_rowid", "mbid_n", "mnid_n", "qid_n"}
    ]
    source_col_defs = ",\n            ".join(f"{col} TEXT" for col in source_cols)
    cursor.execute(
        f"""
        CREATE TABLE {UNMATCHED_WD_TABLE} (
            {source_col_defs}
        )
        """
    )

    wd_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"SELECT wd_rowid, {', '.join(source_cols)} FROM wd_norm_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    dq_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_data_quality_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    ex_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_exception_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    matched_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM mb_wd_match_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )

    unmatched_df = (
        wd_df
        .join(dq_df, on="wd_rowid", how="anti")
        .join(ex_df, on="wd_rowid", how="anti")
        .join(matched_df, on="wd_rowid", how="anti")
        .select(source_cols)
    )

    if unmatched_df.height:
        insert_cols = ", ".join(source_cols)
        placeholders = ", ".join(["?"] * len(source_cols))
        cursor.executemany(
            f"INSERT INTO {UNMATCHED_WD_TABLE} ({insert_cols}) VALUES ({placeholders})",
            unmatched_df.iter_rows(),
        )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNMATCHED_WD_TABLE}_qid "
        f"ON {UNMATCHED_WD_TABLE}(wikidata_id)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNMATCHED_WD_TABLE}_mbid "
        f"ON {UNMATCHED_WD_TABLE}(mbid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNMATCHED_WD_TABLE}_mnid "
        f"ON {UNMATCHED_WD_TABLE}(allmusic_mnid)"
    )

    unmatched_count = cursor.execute(f"SELECT COUNT(*) FROM {UNMATCHED_WD_TABLE}").fetchone()[0]
    log.info("Unmatched table ready: %s rows=%d", UNMATCHED_WD_TABLE, unmatched_count)


def _refresh_unmatched_amg_table(cursor: sqlite3.Cursor, *, has_allmusic: bool) -> None:
    """Persist AllMusic rows not allocated to unified output rows in the current run."""
    cursor.execute(f"DROP TABLE IF EXISTS {UNMATCHED_AMG_TABLE}")

    if not has_allmusic:
        cursor.execute(
            f"""
            CREATE TABLE {UNMATCHED_AMG_TABLE} (
                reason TEXT NOT NULL,
                emitted_utc TEXT NOT NULL,
                mnid TEXT,
                artist_input TEXT,
                allmusic_artist TEXT,
                allmusic_url TEXT,
                name_similarity REAL,
                active TEXT,
                born_date TEXT,
                born_place TEXT,
                biography_html TEXT,
                enrichment_status TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_enriched_utc TEXT,
                last_source_mode TEXT,
                raw_payload_json TEXT,
                genres_json TEXT,
                styles_json TEXT,
                genre_count INTEGER,
                style_count INTEGER
            )
            """
        )
        log.info("Unmatched table ready: %s rows=0 (allmusic source unavailable)", UNMATCHED_AMG_TABLE)
        return

    cursor.execute(
        f"""
        CREATE TABLE {UNMATCHED_AMG_TABLE} (
            reason TEXT NOT NULL,
            emitted_utc TEXT NOT NULL,
            mnid TEXT,
            artist_input TEXT,
            allmusic_artist TEXT,
            allmusic_url TEXT,
            name_similarity REAL,
            active TEXT,
            born_date TEXT,
            born_place TEXT,
            biography_html TEXT,
            enrichment_status TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            last_enriched_utc TEXT,
            last_source_mode TEXT,
            raw_payload_json TEXT,
            genres_json TEXT,
            styles_json TEXT,
            genre_count INTEGER,
            style_count INTEGER
        )
        """
    )

    conn = cursor.connection
    amg_remaining_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT
            mnid,
            artist_input,
            allmusic_artist,
            allmusic_url,
            name_similarity,
            active,
            born_date,
            born_place,
            biography_html,
            enrichment_status,
            first_seen_utc,
            last_seen_utc,
            last_enriched_utc,
            last_source_mode,
            raw_payload_json,
            genres_json,
            styles_json,
            genre_count,
            style_count
        FROM amg_remaining_t
        """,
        dtype_overrides={
            "name_similarity": pl.Float64(),
            "genre_count": pl.Int64(),
            "style_count": pl.Int64(),
        },
    )
    unmatched_df = amg_remaining_df.with_columns(
        pl.lit("no_match_in_unified_or_wd_residual").alias("reason"),
        pl.lit(time.strftime("%Y-%m-%d %H:%M:%S")).alias("emitted_utc"),
    ).select(
        [
            "reason",
            "emitted_utc",
            "mnid",
            "artist_input",
            "allmusic_artist",
            "allmusic_url",
            "name_similarity",
            "active",
            "born_date",
            "born_place",
            "biography_html",
            "enrichment_status",
            "first_seen_utc",
            "last_seen_utc",
            "last_enriched_utc",
            "last_source_mode",
            "raw_payload_json",
            "genres_json",
            "styles_json",
            "genre_count",
            "style_count",
        ]
    )
    if unmatched_df.height:
        cursor.executemany(
            f"""
            INSERT INTO {UNMATCHED_AMG_TABLE} (
                reason,
                emitted_utc,
                mnid,
                artist_input,
                allmusic_artist,
                allmusic_url,
                name_similarity,
                active,
                born_date,
                born_place,
                biography_html,
                enrichment_status,
                first_seen_utc,
                last_seen_utc,
                last_enriched_utc,
                last_source_mode,
                raw_payload_json,
                genres_json,
                styles_json,
                genre_count,
                style_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            unmatched_df.iter_rows(),
        )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNMATCHED_AMG_TABLE}_mnid ON {UNMATCHED_AMG_TABLE}(mnid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{UNMATCHED_AMG_TABLE}_reason ON {UNMATCHED_AMG_TABLE}(reason)"
    )

    unmatched_count = cursor.execute(f"SELECT COUNT(*) FROM {UNMATCHED_AMG_TABLE}").fetchone()[0]
    log.info("Unmatched table ready: %s rows=%d", UNMATCHED_AMG_TABLE, unmatched_count)


def _apply_mb_wd_mnid_bridge_enrichment_phase(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
) -> int:
    target_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            contributor_row_id,
            wikimedia_allmusic_mnid,
            musicbrainz_allmusic_mnid
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
          AND has_wikimedia_row = 1
          AND has_allmusic_row = 0
          AND wikimedia_allmusic_mnid IS NOT NULL
          AND TRIM(wikimedia_allmusic_mnid) <> ''
        """,
        dtype_overrides={"contributor_row_id": pl.Int64()},
    ).with_columns(
        _normalize_exact_name_expr(pl.col("wikimedia_allmusic_mnid")).alias("mnid_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_allmusic_mnid")).alias("musicbrainz_allmusic_mnid_n"),
    )

    if target_df.is_empty():
        return 0

    amg_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT
            rowid AS amg_rowid,
            mnid_n,
            artist_input,
            allmusic_artist,
            allmusic_url,
            name_similarity,
            active,
            born_date,
            born_place,
            biography_html,
            enrichment_status,
            first_seen_utc,
            last_seen_utc,
            last_enriched_utc,
            last_source_mode,
            raw_payload_json,
            genres_json,
            styles_json,
            genre_count,
            style_count
        FROM amg_remaining_t
        WHERE mnid_n IS NOT NULL
          AND TRIM(mnid_n) <> ''
        """,
        dtype_overrides={
            "amg_rowid": pl.Int64(),
            "name_similarity": pl.Float64(),
            "genre_count": pl.Int64(),
            "style_count": pl.Int64(),
        },
    )

    if amg_df.is_empty():
        return 0

    target_side = (
        target_df
        .filter(pl.col("mnid_n").is_not_null() & (pl.col("mnid_n") != ""))
        .sort(["mnid_n", "contributor_row_id"])
        .with_columns(pl.col("contributor_row_id").cum_count().over("mnid_n").alias("__rn"))
    )
    amg_side = (
        amg_df
        .sort(["mnid_n", "amg_rowid"])
        .with_columns(pl.col("amg_rowid").cum_count().over("mnid_n").alias("__rn"))
    )

    bridge_match_df = (
        target_side
        .join(amg_side, on=["mnid_n", "__rn"], how="inner")
        .select(
            [
                "contributor_row_id",
                "amg_rowid",
                "mnid_n",
                "allmusic_artist",
                "allmusic_url",
                "artist_input",
                "name_similarity",
                "active",
                "born_date",
                "born_place",
                "biography_html",
                "enrichment_status",
                "first_seen_utc",
                "last_seen_utc",
                "last_enriched_utc",
                "last_source_mode",
                "raw_payload_json",
                "genres_json",
                "styles_json",
                "genre_count",
                "style_count",
            ]
        )
    )

    if bridge_match_df.is_empty():
        return 0

    update_sql = f"""
        UPDATE {output_table}
        SET
            merge_key_allmusic_mnid = ?,
            has_allmusic_row = 1,
            allmusic_mnid = ?,
            allmusic_artist = ?,
            allmusic_url = ?,
            allmusic_artist_input = ?,
            allmusic_name_similarity = ?,
            allmusic_active = ?,
            allmusic_born_date = ?,
            allmusic_born_place = ?,
            allmusic_biography_html = ?,
            allmusic_enrichment_status = ?,
            allmusic_first_seen_utc = ?,
            allmusic_last_seen_utc = ?,
            allmusic_last_enriched_utc = ?,
            allmusic_last_source_mode = ?,
            allmusic_raw_payload_json = ?,
            allmusic_genres_json = ?,
            allmusic_styles_json = ?,
            allmusic_genre_count = ?,
            allmusic_style_count = ?,
            aligns_mnid_musicbrainz_allmusic = NULL,
            record_origin = 'mb_seed_amg_enriched_via_wd_bridge',
            merge_updated_utc = CURRENT_TIMESTAMP
        WHERE contributor_row_id = ?
    """

    cursor.executemany(
        update_sql,
        bridge_match_df.select(
            [
                pl.col("mnid_n").alias("merge_key_allmusic_mnid"),
                pl.col("mnid_n").alias("allmusic_mnid"),
                pl.col("allmusic_artist"),
                pl.col("allmusic_url"),
                pl.col("artist_input"),
                pl.col("name_similarity"),
                pl.col("active"),
                pl.col("born_date"),
                pl.col("born_place"),
                pl.col("biography_html"),
                pl.col("enrichment_status"),
                pl.col("first_seen_utc"),
                pl.col("last_seen_utc"),
                pl.col("last_enriched_utc"),
                pl.col("last_source_mode"),
                pl.col("raw_payload_json"),
                pl.col("genres_json"),
                pl.col("styles_json"),
                pl.col("genre_count"),
                pl.col("style_count"),
                pl.col("contributor_row_id"),
            ]
        ).iter_rows(),
    )

    cursor.executemany(
        "DELETE FROM amg_remaining_t WHERE rowid = ?",
        bridge_match_df.select("amg_rowid").unique().iter_rows(),
    )

    log.info(
        "AllMusic bridge enrichment via WD MNID: matched=%d",
        bridge_match_df.height,
    )
    return bridge_match_df.height


def _apply_wikimedia_exact_name_fallback_phase(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
) -> int:
    source_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            rowid AS wd_rowid,
            wikidata_uri,
            wikidata_id,
            wikidata_label,
            wikidata_aliases,
            mbid,
            allmusic_mnid,
            songkick_artist_id,
            apple_music_artist_id,
            discogs_artist_id,
            spotify_artist_id,
            lastfm_artist_id,
            youtube_channel_id,
            isni,
            viaf_id,
            official_website,
            gender,
            instance_of_wikidata_ids,
            occupation_wikidata_ids,
            citizenship_wikidata_ids,
            origin_country_wikidata_ids,
            place_of_birth_wikidata_id,
            place_of_death_wikidata_id,
            date_of_birth,
            date_of_death,
            inception,
            dissolved,
            genre_wikidata_ids,
            instrument_wikidata_ids,
            member_of_wikidata_ids,
            wikidata_url,
            musicbrainz_url,
            allmusic_url,
            discogs_url,
            spotify_url,
            songkick_url,
            wikipedia_url,
            apple_lookup_url,
            source_dump,
            extracted_utc
        FROM {UNMATCHED_WD_TABLE}
        """,
        dtype_overrides={"wd_rowid": pl.Int64()},
    ).with_columns(
        _normalize_exact_name_expr(pl.col("wikidata_label")).alias("name_n"),
        _normalize_exact_name_expr(pl.col("mbid")).alias("mbid_n"),
        _normalize_exact_name_expr(pl.col("allmusic_mnid")).alias("mnid_n"),
        _normalize_exact_name_expr(pl.col("wikidata_id")).alias("qid_n"),
        pl.when(pl.col("gender").is_null() | (pl.col("gender").str.strip_chars() == ""))
        .then(None)
        .otherwise(pl.col("gender").str.strip_chars().str.to_lowercase())
        .alias("gender_n"),
    )

    source_unique_df = (
        source_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )

    source_match_df = source_df.join(source_unique_df, on="name_n", how="inner")

    if source_unique_df.is_empty():
        return 0

    target_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            contributor_row_id,
            musicbrainz_artist_name,
            musicbrainz_mbid,
            musicbrainz_allmusic_mnid,
            musicbrainz_wikidata_id,
            musicbrainz_begin_date_year,
            musicbrainz_end_date_year,
            musicbrainz_gender,
            merge_key_wikidata_id
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
          AND has_wikimedia_row = 0
          AND musicbrainz_artist_name IS NOT NULL
          AND TRIM(musicbrainz_artist_name) <> ''
        """,
        dtype_overrides={
            "contributor_row_id": pl.Int64(),
            "musicbrainz_begin_date_year": pl.Int64(),
            "musicbrainz_end_date_year": pl.Int64(),
            "musicbrainz_gender": pl.Int64(),
        },
    ).with_columns(
        _normalize_exact_name_expr(pl.col("musicbrainz_artist_name")).alias("name_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_mbid")).alias("musicbrainz_mbid_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_allmusic_mnid")).alias("musicbrainz_allmusic_mnid_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_wikidata_id")).alias("musicbrainz_wikidata_id_n"),
        _mb_gender_text_expr(pl.col("musicbrainz_gender")).alias("musicbrainz_gender_n"),
    )

    target_unique_df = (
        target_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )

    match_df = (
        target_df
        .join(target_unique_df, on="name_n", how="inner")
        .join(source_match_df, on="name_n", how="inner")
    )

    if match_df.is_empty():
        return 0

    match_df = match_df.with_columns(
        pl.when(pl.col("gender_n").is_not_null() & pl.col("musicbrainz_gender_n").is_not_null())
        .then(pl.col("gender_n") == pl.col("musicbrainz_gender_n"))
        .otherwise(True)
        .alias("gender_ok"),
        pl.when(
            pl.col("musicbrainz_wikidata_id_n").is_not_null()
            & pl.col("qid_n").is_not_null()
        )
        .then(pl.col("musicbrainz_wikidata_id_n") == pl.col("qid_n"))
        .otherwise(True)
        .alias("qid_ok"),
        pl.when(pl.col("musicbrainz_mbid_n").is_not_null() & pl.col("mbid_n").is_not_null())
        .then(pl.col("musicbrainz_mbid_n") == pl.col("mbid_n"))
        .otherwise(True)
        .alias("mbid_ok"),
        pl.when(
            pl.col("musicbrainz_allmusic_mnid_n").is_not_null()
            & pl.col("mnid_n").is_not_null()
        )
        .then(pl.col("musicbrainz_allmusic_mnid_n") == pl.col("mnid_n"))
        .otherwise(True)
        .alias("mnid_ok"),
    )

    match_df = match_df.filter(
        pl.col("gender_ok") & pl.col("qid_ok") & pl.col("mbid_ok") & pl.col("mnid_ok")
    )

    if match_df.is_empty():
        return 0

    conflict_reason_df = match_df.with_columns(
        pl.when(
            pl.col("musicbrainz_mbid_n").is_not_null()
            & pl.col("mbid_n").is_not_null()
            & (pl.col("musicbrainz_mbid_n") != pl.col("mbid_n"))
        )
        .then(pl.lit("mbid_conflict; "))
        .otherwise(pl.lit(""))
        .alias("mbid_conflict_txt"),
        pl.when(
            pl.col("musicbrainz_allmusic_mnid_n").is_not_null()
            & pl.col("mnid_n").is_not_null()
            & (pl.col("musicbrainz_allmusic_mnid_n") != pl.col("mnid_n"))
        )
        .then(pl.lit("mnid_conflict; "))
        .otherwise(pl.lit(""))
        .alias("mnid_conflict_txt"),
        pl.when(
            pl.col("musicbrainz_wikidata_id_n").is_not_null()
            & pl.col("qid_n").is_not_null()
            & (pl.col("musicbrainz_wikidata_id_n") != pl.col("qid_n"))
        )
        .then(pl.lit("qid_conflict; "))
        .otherwise(pl.lit(""))
        .alias("qid_conflict_txt"),
    ).with_columns(
        pl.concat_str(["mbid_conflict_txt", "mnid_conflict_txt", "qid_conflict_txt"]).str.strip_chars().alias("conflict_reason")
    )

    match_df = conflict_reason_df

    update_sql = f"""
        UPDATE {output_table}
        SET
            merge_key_wikidata_id = ?,
            has_wikimedia_row = 1,
            wikimedia_wikidata_uri = ?,
            wikimedia_wikidata_id = ?,
            wikimedia_wikidata_label = ?,
            wikimedia_wikidata_aliases_json = ?,
            wikimedia_mbid = ?,
            wikimedia_allmusic_mnid = ?,
            wikimedia_songkick_artist_id = ?,
            wikimedia_apple_music_artist_id = ?,
            wikimedia_discogs_artist_id = ?,
            wikimedia_spotify_artist_id = ?,
            wikimedia_lastfm_artist_id = ?,
            wikimedia_youtube_channel_id = ?,
            wikimedia_isni = ?,
            wikimedia_viaf_id = ?,
            wikimedia_official_website = ?,
            wikimedia_gender = ?,
            wikimedia_instance_of_wikidata_ids_json = ?,
            wikimedia_occupation_wikidata_ids_json = ?,
            wikimedia_citizenship_wikidata_ids_json = ?,
            wikimedia_origin_country_wikidata_ids_json = ?,
            wikimedia_place_of_birth_wikidata_id = ?,
            wikimedia_place_of_death_wikidata_id = ?,
            wikimedia_date_of_birth = ?,
            wikimedia_date_of_death = ?,
            wikimedia_inception = ?,
            wikimedia_dissolved = ?,
            wikimedia_genre_wikidata_ids_json = ?,
            wikimedia_instrument_wikidata_ids_json = ?,
            wikimedia_member_of_wikidata_ids_json = ?,
            wikimedia_url = ?,
            wikimedia_musicbrainz_url = ?,
            wikimedia_allmusic_url = ?,
            wikimedia_discogs_url = ?,
            wikimedia_spotify_url = ?,
            wikimedia_songkick_url = ?,
            wikimedia_wikipedia_url = ?,
            wikimedia_apple_lookup_url = ?,
            wikimedia_source_dump = ?,
            wikimedia_extracted_utc = ?,
            aligns_mbid_musicbrainz_wikimedia = ?,
            aligns_mnid_musicbrainz_wikimedia = ?,
            aligns_qid_musicbrainz_wikimedia = ?,
            aligns_qid_all_sources = ?,
            conflict_reason = COALESCE(NULLIF(conflict_reason, ''), ?)
        WHERE contributor_row_id = ?
    """

    update_params = match_df.select(
        [
            pl.col("qid_n"),
            pl.col("wikidata_uri"),
            pl.col("wikidata_id"),
            pl.col("wikidata_label"),
            pl.col("wikidata_aliases"),
            pl.col("mbid_n"),
            pl.col("mnid_n"),
            pl.col("songkick_artist_id"),
            pl.col("apple_music_artist_id"),
            pl.col("discogs_artist_id"),
            pl.col("spotify_artist_id"),
            pl.col("lastfm_artist_id"),
            pl.col("youtube_channel_id"),
            pl.col("isni"),
            pl.col("viaf_id"),
            pl.col("official_website"),
            pl.col("gender"),
            pl.col("instance_of_wikidata_ids"),
            pl.col("occupation_wikidata_ids"),
            pl.col("citizenship_wikidata_ids"),
            pl.col("origin_country_wikidata_ids"),
            pl.col("place_of_birth_wikidata_id"),
            pl.col("place_of_death_wikidata_id"),
            pl.col("date_of_birth"),
            pl.col("date_of_death"),
            pl.col("inception"),
            pl.col("dissolved"),
            pl.col("genre_wikidata_ids"),
            pl.col("instrument_wikidata_ids"),
            pl.col("member_of_wikidata_ids"),
            pl.col("wikidata_url"),
            pl.col("musicbrainz_url"),
            pl.col("allmusic_url"),
            pl.col("discogs_url"),
            pl.col("spotify_url"),
            pl.col("songkick_url"),
            pl.col("wikipedia_url"),
            pl.col("apple_lookup_url"),
            pl.col("source_dump"),
            pl.col("extracted_utc"),
            pl.when(pl.col("mbid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_mbid_musicbrainz_wikimedia"),
            pl.when(pl.col("mnid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_mnid_musicbrainz_wikimedia"),
            pl.when(pl.col("qid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_qid_musicbrainz_wikimedia"),
            pl.when(pl.col("qid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_qid_all_sources"),
            pl.col("conflict_reason").alias("conflict_reason"),
            pl.col("contributor_row_id"),
        ]
    ).iter_rows()
    cursor.executemany(update_sql, update_params)

    matched_rowids_df = match_df.select("wd_rowid").unique()
    if matched_rowids_df.height:
        cursor.executemany(
            f"DELETE FROM {UNMATCHED_WD_TABLE} WHERE rowid = ?",
            matched_rowids_df.iter_rows(),
        )

    log.info(
        "Wikimedia exact-name fallback: matched=%d gender_supported=%d",
        match_df.height,
        int(match_df.filter(pl.col("gender_n").is_not_null() & pl.col("musicbrainz_gender_n").is_not_null()).height),
    )
    return match_df.height


def _apply_allmusic_exact_name_fallback_phase(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
) -> int:
    source_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT
            rowid AS amg_rowid,
            mnid,
            artist_input,
            allmusic_artist,
            allmusic_url,
            name_similarity,
            active,
            born_date,
            born_place,
            biography_html,
            enrichment_status,
            first_seen_utc,
            last_seen_utc,
            last_enriched_utc,
            last_source_mode,
            raw_payload_json,
            genres_json,
            styles_json,
            genre_count,
            style_count
        FROM amg_remaining_t
        """,
        dtype_overrides={"amg_rowid": pl.Int64(), "name_similarity": pl.Float64(), "genre_count": pl.Int64(), "style_count": pl.Int64()},
    ).with_columns(
        _normalize_exact_name_expr(pl.coalesce([pl.col("allmusic_artist"), pl.col("artist_input")])).alias("name_n"),
        _normalize_exact_name_expr(pl.col("mnid")).alias("mnid_n"),
        _extract_year_expr(pl.col("born_date")).alias("born_year"),
    )

    source_unique_df = (
        source_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )

    source_match_df = source_df.join(source_unique_df, on="name_n", how="inner")

    if source_unique_df.is_empty():
        return 0

    target_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            contributor_row_id,
            musicbrainz_artist_name,
            musicbrainz_mbid,
            musicbrainz_allmusic_mnid,
            musicbrainz_begin_date_year,
            musicbrainz_end_date_year,
            merge_key_allmusic_mnid,
            has_wikimedia_row,
            wikimedia_allmusic_mnid,
            wikimedia_date_of_birth
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
          AND has_allmusic_row = 0
          AND musicbrainz_artist_name IS NOT NULL
          AND TRIM(musicbrainz_artist_name) <> ''
        """,
        dtype_overrides={
            "contributor_row_id": pl.Int64(),
            "musicbrainz_begin_date_year": pl.Int64(),
            "musicbrainz_end_date_year": pl.Int64(),
            "has_wikimedia_row": pl.Int64(),
        },
    ).with_columns(
        _normalize_exact_name_expr(pl.col("musicbrainz_artist_name")).alias("name_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_mbid")).alias("musicbrainz_mbid_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_allmusic_mnid")).alias("musicbrainz_allmusic_mnid_n"),
        _normalize_exact_name_expr(pl.col("merge_key_allmusic_mnid")).alias("merge_key_allmusic_mnid_n"),
        _normalize_exact_name_expr(pl.col("wikimedia_allmusic_mnid")).alias("wikimedia_allmusic_mnid_n"),
        _extract_year_expr(pl.col("wikimedia_date_of_birth")).alias("wikimedia_birth_year"),
    )

    target_unique_df = (
        target_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )

    match_df = (
        target_df
        .join(target_unique_df, on="name_n", how="inner")
        .join(source_match_df, on="name_n", how="inner")
    )

    if match_df.is_empty():
        return 0

    # Avoid overlap with names already linked to AllMusic anywhere in the merged output.
    # This prevents re-linking names where a deterministic AMG linkage already exists.
    existing_allmusic_name_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT DISTINCT musicbrainz_artist_name
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
          AND has_allmusic_row = 1
          AND musicbrainz_artist_name IS NOT NULL
          AND TRIM(musicbrainz_artist_name) <> ''
        """,
    ).with_columns(
        _normalize_exact_name_expr(pl.col("musicbrainz_artist_name")).alias("name_n")
    ).select("name_n")

    overlap_excluded = 0
    if existing_allmusic_name_df.height:
        before_overlap_filter = match_df.height
        match_df = match_df.join(existing_allmusic_name_df, on="name_n", how="anti")
        overlap_excluded = before_overlap_filter - match_df.height

    if match_df.is_empty():
        return 0

    match_df = match_df.with_columns(
        pl.col("musicbrainz_begin_date_year").alias("mb_begin_year"),
        pl.col("musicbrainz_end_date_year").fill_null(pl.col("musicbrainz_begin_date_year")).alias("mb_end_year"),
    )

    match_df = _parse_amg_active_window_df(match_df)
    match_df = match_df.with_columns(
        pl.when(
            pl.col("musicbrainz_begin_date_year").is_not_null()
            & pl.col("born_year").is_not_null()
        )
        .then(pl.col("musicbrainz_begin_date_year") == pl.col("born_year"))
        .otherwise(True)
        .alias("born_year_ok"),
        pl.when(
            pl.col("musicbrainz_begin_date_year").is_not_null()
            & pl.col("active_start_year").is_not_null()
            & pl.col("active_end_year").is_not_null()
        )
        .then(
            (pl.col("active_start_year") <= pl.col("mb_end_year"))
            & (pl.col("active_end_year") >= pl.col("musicbrainz_begin_date_year"))
        )
        .otherwise(True)
        .alias("active_ok"),
        pl.when(
            (pl.col("has_wikimedia_row") == 1)
            & pl.col("wikimedia_allmusic_mnid_n").is_not_null()
            & pl.col("mnid_n").is_not_null()
        )
        .then(pl.col("wikimedia_allmusic_mnid_n") == pl.col("mnid_n"))
        .otherwise(True)
        .alias("wd_mnid_ok"),
        pl.when(
            (pl.col("has_wikimedia_row") == 1)
            & pl.col("wikimedia_birth_year").is_not_null()
            & pl.col("born_year").is_not_null()
        )
        .then(pl.col("wikimedia_birth_year") == pl.col("born_year"))
        .otherwise(True)
        .alias("wd_birth_year_ok"),
    ).filter(
        pl.col("born_year_ok")
        & pl.col("active_ok")
        & pl.col("wd_mnid_ok")
        & pl.col("wd_birth_year_ok")
    )

    if match_df.is_empty():
        return 0

    match_df = match_df.with_columns(
        pl.when(
            pl.col("musicbrainz_allmusic_mnid_n").is_not_null()
            & pl.col("mnid_n").is_not_null()
        )
        .then(pl.col("musicbrainz_allmusic_mnid_n") == pl.col("mnid_n"))
        .otherwise(True)
        .alias("mnid_ok"),
    ).filter(pl.col("mnid_ok"))

    if match_df.is_empty():
        return 0

    update_sql = f"""
        UPDATE {output_table}
        SET
            merge_key_allmusic_mnid = ?,
            has_allmusic_row = 1,
            allmusic_mnid = ?,
            allmusic_artist = ?,
            allmusic_url = ?,
            allmusic_artist_input = ?,
            allmusic_name_similarity = ?,
            allmusic_active = ?,
            allmusic_born_date = ?,
            allmusic_born_place = ?,
            allmusic_biography_html = ?,
            allmusic_enrichment_status = ?,
            allmusic_first_seen_utc = ?,
            allmusic_last_seen_utc = ?,
            allmusic_last_enriched_utc = ?,
            allmusic_last_source_mode = ?,
            allmusic_raw_payload_json = ?,
            allmusic_genres_json = ?,
            allmusic_styles_json = ?,
            allmusic_genre_count = ?,
            allmusic_style_count = ?,
            aligns_mnid_musicbrainz_allmusic = ?,
            conflict_reason = COALESCE(NULLIF(conflict_reason, ''), ?)
        WHERE contributor_row_id = ?
    """

    update_params = match_df.select(
        [
            pl.col("mnid_n").alias("merge_key_allmusic_mnid"),
            pl.col("mnid_n").alias("allmusic_mnid"),
            pl.col("allmusic_artist"),
            pl.col("allmusic_url"),
            pl.col("artist_input"),
            pl.col("name_similarity"),
            pl.col("active"),
            pl.col("born_date"),
            pl.col("born_place"),
            pl.col("biography_html"),
            pl.col("enrichment_status"),
            pl.col("first_seen_utc"),
            pl.col("last_seen_utc"),
            pl.col("last_enriched_utc"),
            pl.col("last_source_mode"),
            pl.col("raw_payload_json"),
            pl.col("genres_json"),
            pl.col("styles_json"),
            pl.col("genre_count"),
            pl.col("style_count"),
            pl.when(pl.col("mnid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_mnid_musicbrainz_allmusic"),
            pl.when(
                pl.col("musicbrainz_allmusic_mnid_n").is_not_null()
                & pl.col("mnid_n").is_not_null()
                & (pl.col("musicbrainz_allmusic_mnid_n") != pl.col("mnid_n"))
            ).then(pl.lit("mnid_conflict; ")).otherwise(pl.lit("")).alias("conflict_reason"),
            pl.col("contributor_row_id"),
        ]
    ).iter_rows()
    cursor.executemany(update_sql, update_params)

    matched_rowids_df = match_df.select("amg_rowid").unique()
    if matched_rowids_df.height:
        cursor.executemany(
            "DELETE FROM amg_remaining_t WHERE rowid = ?",
            matched_rowids_df.iter_rows(),
        )

    log.info(
        "AllMusic exact-name fallback: matched=%d overlap_excluded=%d born_year_supported=%d active_supported=%d wd_mnid_supported=%d wd_birth_year_supported=%d",
        match_df.height,
        overlap_excluded,
        int(match_df.filter(pl.col("born_year").is_not_null()).height),
        int(match_df.filter(pl.col("active_start_year").is_not_null() & pl.col("active_end_year").is_not_null()).height),
        int(match_df.filter((pl.col("has_wikimedia_row") == 1) & pl.col("wikimedia_allmusic_mnid_n").is_not_null()).height),
        int(match_df.filter((pl.col("has_wikimedia_row") == 1) & pl.col("wikimedia_birth_year").is_not_null()).height),
    )
    return match_df.height


def _apply_allmusic_terminal_lower_name_phase(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
) -> int:
    """Final AMG pass: exact normalized lowercase name match for remaining MB-only rows."""
    source_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT
            rowid AS amg_rowid,
            mnid,
            artist_input,
            allmusic_artist,
            allmusic_url,
            name_similarity,
            active,
            born_date,
            born_place,
            biography_html,
            enrichment_status,
            first_seen_utc,
            last_seen_utc,
            last_enriched_utc,
            last_source_mode,
            raw_payload_json,
            genres_json,
            styles_json,
            genre_count,
            style_count
        FROM amg_remaining_t
        """,
        dtype_overrides={"amg_rowid": pl.Int64(), "name_similarity": pl.Float64(), "genre_count": pl.Int64(), "style_count": pl.Int64()},
    ).with_columns(
        _normalize_exact_name_expr(pl.coalesce([pl.col("allmusic_artist"), pl.col("artist_input")])).alias("name_n"),
        _normalize_exact_name_expr(pl.col("mnid")).alias("mnid_n"),
    )

    target_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT
            contributor_row_id,
            musicbrainz_artist_name,
            musicbrainz_allmusic_mnid
        FROM {output_table}
        WHERE has_musicbrainz_row = 1
          AND has_allmusic_row = 0
          AND musicbrainz_artist_name IS NOT NULL
          AND TRIM(musicbrainz_artist_name) <> ''
        """,
        dtype_overrides={"contributor_row_id": pl.Int64()},
    ).with_columns(
        _normalize_exact_name_expr(pl.col("musicbrainz_artist_name")).alias("name_n"),
        _normalize_exact_name_expr(pl.col("musicbrainz_allmusic_mnid")).alias("musicbrainz_allmusic_mnid_n"),
    )

    if source_df.is_empty() or target_df.is_empty():
        return 0

    # Strictly unique lowercase names on both sides to keep this terminal pass deterministic.
    source_unique_df = (
        source_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )
    target_unique_df = (
        target_df
        .filter(pl.col("name_n").is_not_null())
        .group_by("name_n")
        .len()
        .filter(pl.col("len") == 1)
        .select("name_n")
    )

    if source_unique_df.is_empty() or target_unique_df.is_empty():
        return 0

    match_df = (
        target_df
        .join(target_unique_df, on="name_n", how="inner")
        .join(source_df.join(source_unique_df, on="name_n", how="inner"), on="name_n", how="inner")
        .with_columns(
            pl.when(
                pl.col("musicbrainz_allmusic_mnid_n").is_not_null()
                & pl.col("mnid_n").is_not_null()
            )
            .then(pl.col("musicbrainz_allmusic_mnid_n") == pl.col("mnid_n"))
            .otherwise(True)
            .alias("mnid_ok")
        )
        .filter(pl.col("mnid_ok"))
    )

    if match_df.is_empty():
        return 0

    update_sql = f"""
        UPDATE {output_table}
        SET
            merge_key_allmusic_mnid = ?,
            has_allmusic_row = 1,
            allmusic_mnid = ?,
            allmusic_artist = ?,
            allmusic_url = ?,
            allmusic_artist_input = ?,
            allmusic_name_similarity = ?,
            allmusic_active = ?,
            allmusic_born_date = ?,
            allmusic_born_place = ?,
            allmusic_biography_html = ?,
            allmusic_enrichment_status = ?,
            allmusic_first_seen_utc = ?,
            allmusic_last_seen_utc = ?,
            allmusic_last_enriched_utc = ?,
            allmusic_last_source_mode = ?,
            allmusic_raw_payload_json = ?,
            allmusic_genres_json = ?,
            allmusic_styles_json = ?,
            allmusic_genre_count = ?,
            allmusic_style_count = ?,
            aligns_mnid_musicbrainz_allmusic = ?,
            record_origin = 'mb_seed_amg_terminal_lower_name',
            merge_updated_utc = CURRENT_TIMESTAMP
        WHERE contributor_row_id = ?
    """

    cursor.executemany(
        update_sql,
        match_df.select(
            [
                pl.col("mnid_n").alias("merge_key_allmusic_mnid"),
                pl.col("mnid_n").alias("allmusic_mnid"),
                pl.col("allmusic_artist"),
                pl.col("allmusic_url"),
                pl.col("artist_input"),
                pl.col("name_similarity"),
                pl.col("active"),
                pl.col("born_date"),
                pl.col("born_place"),
                pl.col("biography_html"),
                pl.col("enrichment_status"),
                pl.col("first_seen_utc"),
                pl.col("last_seen_utc"),
                pl.col("last_enriched_utc"),
                pl.col("last_source_mode"),
                pl.col("raw_payload_json"),
                pl.col("genres_json"),
                pl.col("styles_json"),
                pl.col("genre_count"),
                pl.col("style_count"),
                pl.when(pl.col("mnid_ok")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("aligns_mnid_musicbrainz_allmusic"),
                pl.col("contributor_row_id"),
            ]
        ).iter_rows(),
    )

    matched_rowids_df = match_df.select("amg_rowid").unique()
    if matched_rowids_df.height:
        cursor.executemany(
            "DELETE FROM amg_remaining_t WHERE rowid = ?",
            matched_rowids_df.iter_rows(),
        )

    log.info(
        "AllMusic terminal lower-name pass: matched=%d",
        match_df.height,
    )
    return match_df.height


def _apply_allmusic_allocation_phase(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
    has_wikimedia: bool,
) -> None:
    """Apply AMG enrichment and residual allocation using explicit stage boundaries."""
    cursor.execute("DROP TABLE IF EXISTS amg_remaining_t")
    cursor.execute("CREATE TEMP TABLE amg_remaining_t AS SELECT * FROM amg_rollup_t")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_amg_remaining_mnid_n ON amg_remaining_t(mnid_n)")

    cursor.execute(f"DROP TABLE IF EXISTS {output_table}_mb_seed_enriched_t")
    cursor.execute(
        f"""
        CREATE TEMP TABLE {output_table}_mb_seed_enriched_t AS
        SELECT
            t.contributor_row_id,
            t.merge_key_mbid,
            t.merge_key_allmusic_mnid,
            t.merge_key_wikidata_id,
            t.has_musicbrainz_row,
            CASE WHEN a.mnid_n IS NOT NULL THEN 1 ELSE t.has_allmusic_row END AS has_allmusic_row,
            t.has_wikimedia_row,
            t.musicbrainz_artist_id,
            t.musicbrainz_mbid,
            t.musicbrainz_artist_name,
            t.musicbrainz_begin_date_year,
            t.musicbrainz_begin_date_month,
            t.musicbrainz_begin_date_day,
            t.musicbrainz_end_date_year,
            t.musicbrainz_end_date_month,
            t.musicbrainz_end_date_day,
            t.musicbrainz_type,
            t.musicbrainz_area,
            t.musicbrainz_gender,
            t.musicbrainz_disambiguation,
            t.musicbrainz_ended,
            t.musicbrainz_wikidata_uri,
            t.musicbrainz_wikidata_id,
            t.musicbrainz_allmusic_mnid,
            t.musicbrainz_source_dump,
            t.musicbrainz_extracted_utc,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.mnid ELSE t.allmusic_mnid END AS allmusic_mnid,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.allmusic_artist ELSE t.allmusic_artist END AS allmusic_artist,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.allmusic_url ELSE t.allmusic_url END AS allmusic_url,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.artist_input ELSE t.allmusic_artist_input END AS allmusic_artist_input,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.name_similarity ELSE t.allmusic_name_similarity END AS allmusic_name_similarity,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.active ELSE t.allmusic_active END AS allmusic_active,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.born_date ELSE t.allmusic_born_date END AS allmusic_born_date,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.born_place ELSE t.allmusic_born_place END AS allmusic_born_place,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.biography_html ELSE t.allmusic_biography_html END AS allmusic_biography_html,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.enrichment_status ELSE t.allmusic_enrichment_status END AS allmusic_enrichment_status,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.first_seen_utc ELSE t.allmusic_first_seen_utc END AS allmusic_first_seen_utc,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.last_seen_utc ELSE t.allmusic_last_seen_utc END AS allmusic_last_seen_utc,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.last_enriched_utc ELSE t.allmusic_last_enriched_utc END AS allmusic_last_enriched_utc,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.last_source_mode ELSE t.allmusic_last_source_mode END AS allmusic_last_source_mode,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.raw_payload_json ELSE t.allmusic_raw_payload_json END AS allmusic_raw_payload_json,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.genres_json ELSE t.allmusic_genres_json END AS allmusic_genres_json,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.styles_json ELSE t.allmusic_styles_json END AS allmusic_styles_json,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.genre_count ELSE t.allmusic_genre_count END AS allmusic_genre_count,
            CASE WHEN a.mnid_n IS NOT NULL THEN a.style_count ELSE t.allmusic_style_count END AS allmusic_style_count,
            t.wikimedia_wikidata_uri,
            t.wikimedia_wikidata_id,
            t.wikimedia_wikidata_label,
            t.wikimedia_wikidata_aliases_json,
            t.wikimedia_mbid,
            t.wikimedia_allmusic_mnid,
            t.wikimedia_songkick_artist_id,
            t.wikimedia_apple_music_artist_id,
            t.wikimedia_discogs_artist_id,
            t.wikimedia_spotify_artist_id,
            t.wikimedia_lastfm_artist_id,
            t.wikimedia_youtube_channel_id,
            t.wikimedia_isni,
            t.wikimedia_viaf_id,
            t.wikimedia_official_website,
            t.wikimedia_gender,
            t.wikimedia_instance_of_wikidata_ids_json,
            t.wikimedia_occupation_wikidata_ids_json,
            t.wikimedia_citizenship_wikidata_ids_json,
            t.wikimedia_origin_country_wikidata_ids_json,
            t.wikimedia_place_of_birth_wikidata_id,
            t.wikimedia_place_of_death_wikidata_id,
            t.wikimedia_date_of_birth,
            t.wikimedia_date_of_death,
            t.wikimedia_inception,
            t.wikimedia_dissolved,
            t.wikimedia_genre_wikidata_ids_json,
            t.wikimedia_instrument_wikidata_ids_json,
            t.wikimedia_member_of_wikidata_ids_json,
            t.wikimedia_url,
            t.wikimedia_musicbrainz_url,
            t.wikimedia_allmusic_url,
            t.wikimedia_discogs_url,
            t.wikimedia_spotify_url,
            t.wikimedia_songkick_url,
            t.wikimedia_wikipedia_url,
            t.wikimedia_apple_lookup_url,
            t.wikimedia_source_dump,
            t.wikimedia_extracted_utc,
            t.aligns_mbid_musicbrainz_wikimedia,
            CASE
                WHEN a.mnid_n IS NULL THEN t.aligns_mnid_musicbrainz_allmusic
                WHEN t.merge_key_allmusic_mnid IS NULL THEN NULL
                WHEN t.merge_key_allmusic_mnid = a.mnid_n THEN 1
                ELSE 0
            END AS aligns_mnid_musicbrainz_allmusic,
            t.aligns_mnid_musicbrainz_wikimedia,
            t.aligns_qid_musicbrainz_wikimedia,
            t.aligns_mnid_allmusic_wikimedia,
            t.aligns_mbid_all_sources,
            t.aligns_mnid_all_sources,
            t.aligns_qid_all_sources,
            t.conflict_reason,
            CASE
                WHEN a.mnid_n IS NOT NULL THEN 'mb_seed_amg_enriched_existing'
                ELSE t.record_origin
            END AS record_origin,
            t.merge_created_utc,
            CURRENT_TIMESTAMP AS merge_updated_utc
        FROM {output_table} t
        LEFT JOIN amg_remaining_t a
            ON t.merge_key_allmusic_mnid = a.mnid_n
        """
    )

    cursor.execute(f"DELETE FROM {output_table}")
    cursor.execute(f"INSERT INTO {output_table} SELECT * FROM {output_table}_mb_seed_enriched_t")

    enriched_row_count = cursor.execute(
        f"SELECT COUNT(*) FROM {output_table} WHERE record_origin = 'mb_seed_amg_enriched_existing'"
    ).fetchone()[0]

    matched_existing_mnids_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"""
        SELECT DISTINCT merge_key_allmusic_mnid AS mnid_n
        FROM {output_table}
        WHERE merge_key_allmusic_mnid IS NOT NULL
        """,
    )
    if matched_existing_mnids_df.height:
        cursor.executemany(
            "DELETE FROM amg_remaining_t WHERE mnid_n = ?",
            matched_existing_mnids_df.iter_rows(),
        )

    wd_rows_consumed = 0
    merged_rows_inserted = 0
    if has_wikimedia:
        amg_remaining_df = tm_polars_db.sqlite_to_polars(
            conn,
            """
            SELECT rowid AS amg_rowid, mnid_n
            FROM amg_remaining_t
            WHERE mnid_n IS NOT NULL
            """,
            dtype_overrides={"amg_rowid": pl.Int64()},
        ).with_columns(pl.col("mnid_n").cast(pl.Categorical))
        wd_unmatched_df = tm_polars_db.sqlite_to_polars(
            conn,
            f"""
            SELECT
                rowid AS unmatched_rowid,
                allmusic_mnid
            FROM {UNMATCHED_WD_TABLE}
            """,
            dtype_overrides={"unmatched_rowid": pl.Int64()},
        )

        wd_side = (
            wd_unmatched_df
            .with_columns(
                pl.col("allmusic_mnid")
                .str.strip_chars()
                .str.to_lowercase()
                .cast(pl.Categorical)
                .alias("mnid_n")
            )
            .filter(pl.col("mnid_n").is_not_null() & (pl.col("mnid_n") != ""))
            .sort(["mnid_n", "unmatched_rowid"])
            .with_columns(pl.col("unmatched_rowid").cum_count().over("mnid_n").alias("__rn"))
            .select(["unmatched_rowid", "mnid_n", "__rn"])
        )
        amg_side = (
            amg_remaining_df
            .sort(["mnid_n", "amg_rowid"])
            .with_columns(pl.col("amg_rowid").cum_count().over("mnid_n").alias("__rn"))
            .select(["mnid_n", "__rn"])
        )
        wd_amg_match_pairs_df = (
            wd_side
            .join(amg_side, on=["mnid_n", "__rn"], how="inner")
            .select(["unmatched_rowid", "mnid_n"])
        )

        cursor.execute("DROP TABLE IF EXISTS wd_amg_match_pairs_t")
        cursor.execute(
            """
            CREATE TEMP TABLE wd_amg_match_pairs_t (
                unmatched_rowid INTEGER NOT NULL,
                mnid_n TEXT NOT NULL
            )
            """
        )

        merged_rows_inserted = wd_amg_match_pairs_df.height
        if merged_rows_inserted:
            cursor.executemany(
                "INSERT INTO wd_amg_match_pairs_t (unmatched_rowid, mnid_n) VALUES (?, ?)",
                wd_amg_match_pairs_df.select(["unmatched_rowid", "mnid_n"]).iter_rows(),
            )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_wd_amg_match_pairs_rowid ON wd_amg_match_pairs_t(unmatched_rowid)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_wd_amg_match_pairs_mnid ON wd_amg_match_pairs_t(mnid_n)"
        )

        if merged_rows_inserted:
            cursor.execute(
                f"""
                INSERT INTO {output_table}
                SELECT
                    NULL AS contributor_row_id,
                    LOWER(NULLIF(TRIM(u.mbid), '')) AS merge_key_mbid,
                    a.mnid_n AS merge_key_allmusic_mnid,
                    UPPER(
                        CASE
                            WHEN u.wikidata_id IS NOT NULL AND TRIM(u.wikidata_id) <> '' THEN u.wikidata_id
                            WHEN u.wikidata_uri IS NULL OR TRIM(u.wikidata_uri) = '' THEN NULL
                            WHEN INSTR(u.wikidata_uri, 'Q') > 0 THEN SUBSTR(u.wikidata_uri, INSTR(u.wikidata_uri, 'Q'))
                            ELSE u.wikidata_uri
                        END
                    ) AS merge_key_wikidata_id,
                    0 AS has_musicbrainz_row,
                    1 AS has_allmusic_row,
                    1 AS has_wikimedia_row,
                    NULL AS musicbrainz_artist_id,
                    NULL AS musicbrainz_mbid,
                    NULL AS musicbrainz_artist_name,
                    NULL AS musicbrainz_begin_date_year,
                    NULL AS musicbrainz_begin_date_month,
                    NULL AS musicbrainz_begin_date_day,
                    NULL AS musicbrainz_end_date_year,
                    NULL AS musicbrainz_end_date_month,
                    NULL AS musicbrainz_end_date_day,
                    NULL AS musicbrainz_type,
                    NULL AS musicbrainz_area,
                    NULL AS musicbrainz_gender,
                    NULL AS musicbrainz_disambiguation,
                    NULL AS musicbrainz_ended,
                    NULL AS musicbrainz_wikidata_uri,
                    NULL AS musicbrainz_wikidata_id,
                    NULL AS musicbrainz_allmusic_mnid,
                    NULL AS musicbrainz_source_dump,
                    NULL AS musicbrainz_extracted_utc,
                    a.mnid AS allmusic_mnid,
                    a.allmusic_artist AS allmusic_artist,
                    a.allmusic_url AS allmusic_url,
                    a.artist_input AS allmusic_artist_input,
                    a.name_similarity AS allmusic_name_similarity,
                    a.active AS allmusic_active,
                    a.born_date AS allmusic_born_date,
                    a.born_place AS allmusic_born_place,
                    a.biography_html AS allmusic_biography_html,
                    a.enrichment_status AS allmusic_enrichment_status,
                    a.first_seen_utc AS allmusic_first_seen_utc,
                    a.last_seen_utc AS allmusic_last_seen_utc,
                    a.last_enriched_utc AS allmusic_last_enriched_utc,
                    a.last_source_mode AS allmusic_last_source_mode,
                    a.raw_payload_json AS allmusic_raw_payload_json,
                    a.genres_json AS allmusic_genres_json,
                    a.styles_json AS allmusic_styles_json,
                    a.genre_count AS allmusic_genre_count,
                    a.style_count AS allmusic_style_count,
                    u.wikidata_uri AS wikimedia_wikidata_uri,
                    u.wikidata_id AS wikimedia_wikidata_id,
                    u.wikidata_label AS wikimedia_wikidata_label,
                    u.wikidata_aliases AS wikimedia_wikidata_aliases_json,
                    u.mbid AS wikimedia_mbid,
                    u.allmusic_mnid AS wikimedia_allmusic_mnid,
                    u.songkick_artist_id AS wikimedia_songkick_artist_id,
                    u.apple_music_artist_id AS wikimedia_apple_music_artist_id,
                    u.discogs_artist_id AS wikimedia_discogs_artist_id,
                    u.spotify_artist_id AS wikimedia_spotify_artist_id,
                    u.lastfm_artist_id AS wikimedia_lastfm_artist_id,
                    u.youtube_channel_id AS wikimedia_youtube_channel_id,
                    u.isni AS wikimedia_isni,
                    u.viaf_id AS wikimedia_viaf_id,
                    u.official_website AS wikimedia_official_website,
                    u.gender AS wikimedia_gender,
                    u.instance_of_wikidata_ids AS wikimedia_instance_of_wikidata_ids_json,
                    u.occupation_wikidata_ids AS wikimedia_occupation_wikidata_ids_json,
                    u.citizenship_wikidata_ids AS wikimedia_citizenship_wikidata_ids_json,
                    u.origin_country_wikidata_ids AS wikimedia_origin_country_wikidata_ids_json,
                    u.place_of_birth_wikidata_id AS wikimedia_place_of_birth_wikidata_id,
                    u.place_of_death_wikidata_id AS wikimedia_place_of_death_wikidata_id,
                    u.date_of_birth AS wikimedia_date_of_birth,
                    u.date_of_death AS wikimedia_date_of_death,
                    u.inception AS wikimedia_inception,
                    u.dissolved AS wikimedia_dissolved,
                    u.genre_wikidata_ids AS wikimedia_genre_wikidata_ids_json,
                    u.instrument_wikidata_ids AS wikimedia_instrument_wikidata_ids_json,
                    u.member_of_wikidata_ids AS wikimedia_member_of_wikidata_ids_json,
                    u.wikidata_url AS wikimedia_url,
                    u.musicbrainz_url AS wikimedia_musicbrainz_url,
                    u.allmusic_url AS wikimedia_allmusic_url,
                    u.discogs_url AS wikimedia_discogs_url,
                    u.spotify_url AS wikimedia_spotify_url,
                    u.songkick_url AS wikimedia_songkick_url,
                    u.wikipedia_url AS wikimedia_wikipedia_url,
                    u.apple_lookup_url AS wikimedia_apple_lookup_url,
                    u.source_dump AS wikimedia_source_dump,
                    u.extracted_utc AS wikimedia_extracted_utc,
                    NULL AS aligns_mbid_musicbrainz_wikimedia,
                    NULL AS aligns_mnid_musicbrainz_allmusic,
                    NULL AS aligns_mnid_musicbrainz_wikimedia,
                    NULL AS aligns_qid_musicbrainz_wikimedia,
                    CASE
                        WHEN LOWER(NULLIF(TRIM(u.allmusic_mnid), '')) IS NULL OR a.mnid_n IS NULL THEN NULL
                        WHEN LOWER(NULLIF(TRIM(u.allmusic_mnid), '')) = a.mnid_n THEN 1
                        ELSE 0
                    END AS aligns_mnid_allmusic_wikimedia,
                    NULL AS aligns_mbid_all_sources,
                    NULL AS aligns_mnid_all_sources,
                    NULL AS aligns_qid_all_sources,
                    NULL AS conflict_reason,
                    'wd_amg_merged_from_residual' AS record_origin,
                    CURRENT_TIMESTAMP AS merge_created_utc,
                    CURRENT_TIMESTAMP AS merge_updated_utc
                FROM wd_amg_match_pairs_t p
                INNER JOIN {UNMATCHED_WD_TABLE} u ON u.rowid = p.unmatched_rowid
                INNER JOIN amg_remaining_t a ON a.mnid_n = p.mnid_n
                """
            )

            matched_rowids_df = wd_amg_match_pairs_df.select("unmatched_rowid").unique()
            wd_rows_consumed = matched_rowids_df.height
            if wd_rows_consumed:
                cursor.executemany(
                    f"DELETE FROM {UNMATCHED_WD_TABLE} WHERE rowid = ?",
                    matched_rowids_df.iter_rows(),
                )

            matched_mnids_df = wd_amg_match_pairs_df.select("mnid_n").unique()
            if matched_mnids_df.height:
                cursor.executemany(
                    "DELETE FROM amg_remaining_t WHERE mnid_n = ?",
                    matched_mnids_df.iter_rows(),
                )

        _apply_mb_wd_mnid_bridge_enrichment_phase(
            conn,
            cursor,
            output_table=output_table,
        )

        _apply_wikimedia_exact_name_fallback_phase(
            conn,
            cursor,
            output_table=output_table,
        )

    _apply_allmusic_exact_name_fallback_phase(
        conn,
        cursor,
        output_table=output_table,
    )

    _apply_allmusic_terminal_lower_name_phase(
        conn,
        cursor,
        output_table=output_table,
    )

    _refresh_unmatched_amg_table(cursor, has_allmusic=True)

    if not has_wikimedia:
        wd_rows_consumed = 0

    amg_remaining_count = cursor.execute("SELECT COUNT(*) FROM amg_remaining_t").fetchone()[0]
    wd_remaining_count = (
        cursor.execute(f"SELECT COUNT(*) FROM {UNMATCHED_WD_TABLE}").fetchone()[0]
        if has_wikimedia
        else 0
    )
    log.info(
        "AllMusic allocation: enriched_existing=%d wd_amg_merged=%d wd_consumed=%d amg_parked=%d wd_residual_remaining=%d",
        enriched_row_count,
        merged_rows_inserted,
        wd_rows_consumed,
        amg_remaining_count,
        wd_remaining_count,
    )


def _promote_residual_rows_into_unified(
    cursor: sqlite3.Cursor,
    *,
    output_table: str,
    has_allmusic: bool,
    has_wikimedia: bool,
) -> None:
    """Promote residual unmatched rows into unified output before name-based split."""
    wd_promoted = 0
    amg_promoted = 0

    if has_wikimedia:
        wd_promoted = cursor.execute(
            f"SELECT COUNT(*) FROM {UNMATCHED_WD_TABLE}"
        ).fetchone()[0]
        if wd_promoted:
            cursor.execute(
                f"""
                INSERT INTO {output_table}
                SELECT
                    NULL AS contributor_row_id,
                    LOWER(NULLIF(TRIM(u.mbid), '')) AS merge_key_mbid,
                    LOWER(NULLIF(TRIM(u.allmusic_mnid), '')) AS merge_key_allmusic_mnid,
                    UPPER(
                        CASE
                            WHEN u.wikidata_id IS NOT NULL AND TRIM(u.wikidata_id) <> '' THEN u.wikidata_id
                            WHEN u.wikidata_uri IS NULL OR TRIM(u.wikidata_uri) = '' THEN NULL
                            WHEN INSTR(u.wikidata_uri, 'Q') > 0 THEN SUBSTR(u.wikidata_uri, INSTR(u.wikidata_uri, 'Q'))
                            ELSE u.wikidata_uri
                        END
                    ) AS merge_key_wikidata_id,
                    0 AS has_musicbrainz_row,
                    0 AS has_allmusic_row,
                    1 AS has_wikimedia_row,
                    NULL AS musicbrainz_artist_id,
                    NULL AS musicbrainz_mbid,
                    NULL AS musicbrainz_artist_name,
                    NULL AS musicbrainz_begin_date_year,
                    NULL AS musicbrainz_begin_date_month,
                    NULL AS musicbrainz_begin_date_day,
                    NULL AS musicbrainz_end_date_year,
                    NULL AS musicbrainz_end_date_month,
                    NULL AS musicbrainz_end_date_day,
                    NULL AS musicbrainz_type,
                    NULL AS musicbrainz_area,
                    NULL AS musicbrainz_gender,
                    NULL AS musicbrainz_disambiguation,
                    NULL AS musicbrainz_ended,
                    NULL AS musicbrainz_wikidata_uri,
                    NULL AS musicbrainz_wikidata_id,
                    NULL AS musicbrainz_allmusic_mnid,
                    NULL AS musicbrainz_source_dump,
                    NULL AS musicbrainz_extracted_utc,
                    NULL AS allmusic_mnid,
                    NULL AS allmusic_artist,
                    NULL AS allmusic_url,
                    NULL AS allmusic_artist_input,
                    NULL AS allmusic_name_similarity,
                    NULL AS allmusic_active,
                    NULL AS allmusic_born_date,
                    NULL AS allmusic_born_place,
                    NULL AS allmusic_biography_html,
                    NULL AS allmusic_enrichment_status,
                    NULL AS allmusic_first_seen_utc,
                    NULL AS allmusic_last_seen_utc,
                    NULL AS allmusic_last_enriched_utc,
                    NULL AS allmusic_last_source_mode,
                    NULL AS allmusic_raw_payload_json,
                    NULL AS allmusic_genres_json,
                    NULL AS allmusic_styles_json,
                    NULL AS allmusic_genre_count,
                    NULL AS allmusic_style_count,
                    u.wikidata_uri AS wikimedia_wikidata_uri,
                    u.wikidata_id AS wikimedia_wikidata_id,
                    u.wikidata_label AS wikimedia_wikidata_label,
                    u.wikidata_aliases AS wikimedia_wikidata_aliases_json,
                    u.mbid AS wikimedia_mbid,
                    u.allmusic_mnid AS wikimedia_allmusic_mnid,
                    u.songkick_artist_id AS wikimedia_songkick_artist_id,
                    u.apple_music_artist_id AS wikimedia_apple_music_artist_id,
                    u.discogs_artist_id AS wikimedia_discogs_artist_id,
                    u.spotify_artist_id AS wikimedia_spotify_artist_id,
                    u.lastfm_artist_id AS wikimedia_lastfm_artist_id,
                    u.youtube_channel_id AS wikimedia_youtube_channel_id,
                    u.isni AS wikimedia_isni,
                    u.viaf_id AS wikimedia_viaf_id,
                    u.official_website AS wikimedia_official_website,
                    u.gender AS wikimedia_gender,
                    u.instance_of_wikidata_ids AS wikimedia_instance_of_wikidata_ids_json,
                    u.occupation_wikidata_ids AS wikimedia_occupation_wikidata_ids_json,
                    u.citizenship_wikidata_ids AS wikimedia_citizenship_wikidata_ids_json,
                    u.origin_country_wikidata_ids AS wikimedia_origin_country_wikidata_ids_json,
                    u.place_of_birth_wikidata_id AS wikimedia_place_of_birth_wikidata_id,
                    u.place_of_death_wikidata_id AS wikimedia_place_of_death_wikidata_id,
                    u.date_of_birth AS wikimedia_date_of_birth,
                    u.date_of_death AS wikimedia_date_of_death,
                    u.inception AS wikimedia_inception,
                    u.dissolved AS wikimedia_dissolved,
                    u.genre_wikidata_ids AS wikimedia_genre_wikidata_ids_json,
                    u.instrument_wikidata_ids AS wikimedia_instrument_wikidata_ids_json,
                    u.member_of_wikidata_ids AS wikimedia_member_of_wikidata_ids_json,
                    u.wikidata_url AS wikimedia_url,
                    u.musicbrainz_url AS wikimedia_musicbrainz_url,
                    u.allmusic_url AS wikimedia_allmusic_url,
                    u.discogs_url AS wikimedia_discogs_url,
                    u.spotify_url AS wikimedia_spotify_url,
                    u.songkick_url AS wikimedia_songkick_url,
                    u.wikipedia_url AS wikimedia_wikipedia_url,
                    u.apple_lookup_url AS wikimedia_apple_lookup_url,
                    u.source_dump AS wikimedia_source_dump,
                    u.extracted_utc AS wikimedia_extracted_utc,
                    NULL AS aligns_mbid_musicbrainz_wikimedia,
                    NULL AS aligns_mnid_musicbrainz_allmusic,
                    NULL AS aligns_mnid_musicbrainz_wikimedia,
                    NULL AS aligns_qid_musicbrainz_wikimedia,
                    NULL AS aligns_mnid_allmusic_wikimedia,
                    NULL AS aligns_mbid_all_sources,
                    NULL AS aligns_mnid_all_sources,
                    NULL AS aligns_qid_all_sources,
                    NULL AS conflict_reason,
                    'wd_residual_promoted' AS record_origin,
                    CURRENT_TIMESTAMP AS merge_created_utc,
                    CURRENT_TIMESTAMP AS merge_updated_utc
                FROM {UNMATCHED_WD_TABLE} u
                """
            )

    if has_allmusic:
        amg_promoted = cursor.execute(
            "SELECT COUNT(*) FROM amg_remaining_t"
        ).fetchone()[0]
        if amg_promoted:
            cursor.execute(
                f"""
                INSERT INTO {output_table}
                SELECT
                    NULL AS contributor_row_id,
                    NULL AS merge_key_mbid,
                    LOWER(NULLIF(TRIM(a.mnid), '')) AS merge_key_allmusic_mnid,
                    NULL AS merge_key_wikidata_id,
                    0 AS has_musicbrainz_row,
                    1 AS has_allmusic_row,
                    0 AS has_wikimedia_row,
                    NULL AS musicbrainz_artist_id,
                    NULL AS musicbrainz_mbid,
                    NULL AS musicbrainz_artist_name,
                    NULL AS musicbrainz_begin_date_year,
                    NULL AS musicbrainz_begin_date_month,
                    NULL AS musicbrainz_begin_date_day,
                    NULL AS musicbrainz_end_date_year,
                    NULL AS musicbrainz_end_date_month,
                    NULL AS musicbrainz_end_date_day,
                    NULL AS musicbrainz_type,
                    NULL AS musicbrainz_area,
                    NULL AS musicbrainz_gender,
                    NULL AS musicbrainz_disambiguation,
                    NULL AS musicbrainz_ended,
                    NULL AS musicbrainz_wikidata_uri,
                    NULL AS musicbrainz_wikidata_id,
                    NULL AS musicbrainz_allmusic_mnid,
                    NULL AS musicbrainz_source_dump,
                    NULL AS musicbrainz_extracted_utc,
                    a.mnid AS allmusic_mnid,
                    a.allmusic_artist AS allmusic_artist,
                    a.allmusic_url AS allmusic_url,
                    a.artist_input AS allmusic_artist_input,
                    a.name_similarity AS allmusic_name_similarity,
                    a.active AS allmusic_active,
                    a.born_date AS allmusic_born_date,
                    a.born_place AS allmusic_born_place,
                    a.biography_html AS allmusic_biography_html,
                    a.enrichment_status AS allmusic_enrichment_status,
                    a.first_seen_utc AS allmusic_first_seen_utc,
                    a.last_seen_utc AS allmusic_last_seen_utc,
                    a.last_enriched_utc AS allmusic_last_enriched_utc,
                    a.last_source_mode AS allmusic_last_source_mode,
                    a.raw_payload_json AS allmusic_raw_payload_json,
                    a.genres_json AS allmusic_genres_json,
                    a.styles_json AS allmusic_styles_json,
                    a.genre_count AS allmusic_genre_count,
                    a.style_count AS allmusic_style_count,
                    NULL AS wikimedia_wikidata_uri,
                    NULL AS wikimedia_wikidata_id,
                    NULL AS wikimedia_wikidata_label,
                    NULL AS wikimedia_wikidata_aliases_json,
                    NULL AS wikimedia_mbid,
                    NULL AS wikimedia_allmusic_mnid,
                    NULL AS wikimedia_songkick_artist_id,
                    NULL AS wikimedia_apple_music_artist_id,
                    NULL AS wikimedia_discogs_artist_id,
                    NULL AS wikimedia_spotify_artist_id,
                    NULL AS wikimedia_lastfm_artist_id,
                    NULL AS wikimedia_youtube_channel_id,
                    NULL AS wikimedia_isni,
                    NULL AS wikimedia_viaf_id,
                    NULL AS wikimedia_official_website,
                    NULL AS wikimedia_gender,
                    NULL AS wikimedia_instance_of_wikidata_ids_json,
                    NULL AS wikimedia_occupation_wikidata_ids_json,
                    NULL AS wikimedia_citizenship_wikidata_ids_json,
                    NULL AS wikimedia_origin_country_wikidata_ids_json,
                    NULL AS wikimedia_place_of_birth_wikidata_id,
                    NULL AS wikimedia_place_of_death_wikidata_id,
                    NULL AS wikimedia_date_of_birth,
                    NULL AS wikimedia_date_of_death,
                    NULL AS wikimedia_inception,
                    NULL AS wikimedia_dissolved,
                    NULL AS wikimedia_genre_wikidata_ids_json,
                    NULL AS wikimedia_instrument_wikidata_ids_json,
                    NULL AS wikimedia_member_of_wikidata_ids_json,
                    NULL AS wikimedia_url,
                    NULL AS wikimedia_musicbrainz_url,
                    NULL AS wikimedia_allmusic_url,
                    NULL AS wikimedia_discogs_url,
                    NULL AS wikimedia_spotify_url,
                    NULL AS wikimedia_songkick_url,
                    NULL AS wikimedia_wikipedia_url,
                    NULL AS wikimedia_apple_lookup_url,
                    NULL AS wikimedia_source_dump,
                    NULL AS wikimedia_extracted_utc,
                    NULL AS aligns_mbid_musicbrainz_wikimedia,
                    NULL AS aligns_mnid_musicbrainz_allmusic,
                    NULL AS aligns_mnid_musicbrainz_wikimedia,
                    NULL AS aligns_qid_musicbrainz_wikimedia,
                    NULL AS aligns_mnid_allmusic_wikimedia,
                    NULL AS aligns_mbid_all_sources,
                    NULL AS aligns_mnid_all_sources,
                    NULL AS aligns_qid_all_sources,
                    'no_match_in_unified_or_wd_residual' AS conflict_reason,
                    'amg_residual_promoted' AS record_origin,
                    CURRENT_TIMESTAMP AS merge_created_utc,
                    CURRENT_TIMESTAMP AS merge_updated_utc
                FROM amg_remaining_t a
                """
            )

    log.info(
        "Residual promotion: wd_promoted=%d amg_promoted=%d",
        wd_promoted,
        amg_promoted,
    )


def _build_wd_exception_rowids_table(cursor: sqlite3.Cursor, *, has_wikimedia: bool) -> None:
    cursor.execute("DROP TABLE IF EXISTS wd_exception_rowids_t")
    if not has_wikimedia:
        cursor.execute("CREATE TEMP TABLE wd_exception_rowids_t (wd_rowid INTEGER PRIMARY KEY)")
        return

    conn = cursor.connection
    wd_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT wd_rowid, mbid_n
        FROM wd_norm_t
        WHERE mbid_n IS NOT NULL
        """,
        dtype_overrides={"wd_rowid": pl.Int64()},
    ).with_columns(pl.col("mbid_n").cast(pl.Categorical))
    dq_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid FROM wd_data_quality_rowids_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    mb_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT DISTINCT mbid_n FROM mb_norm_t WHERE mbid_n IS NOT NULL",
    ).with_columns(pl.col("mbid_n").cast(pl.Categorical))

    exception_df = (
        wd_df
        .join(dq_df, on="wd_rowid", how="anti")
        .join(mb_df, on="mbid_n", how="anti")
        .select("wd_rowid")
    )

    cursor.execute("CREATE TEMP TABLE wd_exception_rowids_t (wd_rowid INTEGER PRIMARY KEY)")
    if exception_df.height:
        cursor.executemany(
            "INSERT INTO wd_exception_rowids_t (wd_rowid) VALUES (?)",
            exception_df.select("wd_rowid").iter_rows(),
        )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wd_exception_rowids ON wd_exception_rowids_t(wd_rowid)")


def _build_wd_data_quality_rowids_table(cursor: sqlite3.Cursor, *, has_wikimedia: bool) -> None:
    cursor.execute("DROP TABLE IF EXISTS wd_data_quality_rowids_t")
    cursor.execute("DROP TABLE IF EXISTS wd_data_quality_reasons_t")
    if not has_wikimedia:
        cursor.execute("CREATE TEMP TABLE wd_data_quality_rowids_t (wd_rowid INTEGER PRIMARY KEY)")
        cursor.execute(
            "CREATE TEMP TABLE wd_data_quality_reasons_t (wd_rowid INTEGER PRIMARY KEY, reason TEXT NOT NULL)"
        )
        return

    cursor.execute("CREATE TEMP TABLE wd_data_quality_rowids_t (wd_rowid INTEGER PRIMARY KEY)")
    cursor.execute(
        "CREATE TEMP TABLE wd_data_quality_reasons_t (wd_rowid INTEGER PRIMARY KEY, reason TEXT NOT NULL)"
    )

    conn = cursor.connection
    wd_df = tm_polars_db.sqlite_to_polars(
        conn,
        """
        SELECT wd_rowid, mbid_n
        FROM wd_norm_t
        WHERE mbid_n IS NOT NULL
        """,
        dtype_overrides={"wd_rowid": pl.Int64()},
    ).with_columns(pl.col("mbid_n").cast(pl.Categorical))

    dup_mbid_keys = (
        wd_df
        .group_by("mbid_n")
        .len()
        .filter(pl.col("len") > 1)
        .select("mbid_n")
    )
    dq_df = wd_df.join(dup_mbid_keys, on="mbid_n", how="inner").select("wd_rowid").unique()

    if dq_df.height:
        cursor.executemany(
            "INSERT INTO wd_data_quality_rowids_t (wd_rowid) VALUES (?)",
            dq_df.select("wd_rowid").iter_rows(),
        )
        cursor.executemany(
            "INSERT INTO wd_data_quality_reasons_t (wd_rowid, reason) VALUES (?, ?)",
            [(int(row[0]), "Duplicated MBID") for row in dq_df.select("wd_rowid").iter_rows()],
        )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wd_data_quality_rowids ON wd_data_quality_rowids_t(wd_rowid)")
    dq_mbid_count = cursor.execute(
        "SELECT COUNT(*) FROM wd_data_quality_reasons_t WHERE reason = 'Duplicated MBID'"
    ).fetchone()[0]
    log.info(
        "Wikimedia quality gate: quarantined %d rows for duplicated MBID before MBID stage",
        dq_mbid_count,
    )


def _refresh_wikimedia_data_quality_issues_table(
    cursor: sqlite3.Cursor,
    *,
    wd_table: str,
    has_wikimedia: bool,
) -> None:
    """Persist quarantined Wikimedia source rows that fail pre-match quality checks."""
    cursor.execute(f"DROP TABLE IF EXISTS {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}")

    if not has_wikimedia:
        cursor.execute(
            f"""
            CREATE TABLE {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE} (
                reason TEXT NOT NULL,
                wikidata_uri TEXT,
                wikidata_id TEXT,
                wikidata_label TEXT,
                wikidata_aliases TEXT,
                mbid TEXT,
                allmusic_mnid TEXT,
                songkick_artist_id TEXT,
                apple_music_artist_id TEXT,
                discogs_artist_id TEXT,
                spotify_artist_id TEXT,
                lastfm_artist_id TEXT,
                youtube_channel_id TEXT,
                isni TEXT,
                viaf_id TEXT,
                official_website TEXT,
                gender TEXT,
                instance_of_wikidata_ids TEXT,
                occupation_wikidata_ids TEXT,
                citizenship_wikidata_ids TEXT,
                origin_country_wikidata_ids TEXT,
                place_of_birth_wikidata_id TEXT,
                place_of_death_wikidata_id TEXT,
                date_of_birth TEXT,
                date_of_death TEXT,
                inception TEXT,
                dissolved TEXT,
                genre_wikidata_ids TEXT,
                instrument_wikidata_ids TEXT,
                member_of_wikidata_ids TEXT,
                wikidata_url TEXT,
                musicbrainz_url TEXT,
                allmusic_url TEXT,
                discogs_url TEXT,
                spotify_url TEXT,
                songkick_url TEXT,
                wikipedia_url TEXT,
                apple_lookup_url TEXT,
                source_dump TEXT,
                extracted_utc TEXT
            )
            """
        )
        log.info(
            "Wikimedia data quality table ready: %s rows=0 (wikimedia source unavailable)",
            WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE,
        )
        return

    conn = cursor.connection
    source_cols = [
        row[1]
        for row in cursor.execute("PRAGMA table_info(wd_norm_t)").fetchall()
        if row[1] not in {"wd_rowid", "mbid_n", "mnid_n", "qid_n"}
    ]
    source_col_defs = ",\n            ".join(f"{col} TEXT" for col in source_cols)
    cursor.execute(
        f"""
        CREATE TABLE {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE} (
            reason TEXT NOT NULL,
            {source_col_defs}
        )
        """
    )

    wd_df = tm_polars_db.sqlite_to_polars(
        conn,
        f"SELECT wd_rowid, {', '.join(source_cols)} FROM wd_norm_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )
    reasons_df = tm_polars_db.sqlite_to_polars(
        conn,
        "SELECT wd_rowid, reason FROM wd_data_quality_reasons_t",
        dtype_overrides={"wd_rowid": pl.Int64()},
    )

    dq_issues_df = reasons_df.join(wd_df, on="wd_rowid", how="inner").select(["reason", *source_cols])
    if dq_issues_df.height:
        insert_cols = ", ".join(["reason", *source_cols])
        placeholders = ", ".join(["?"] * (len(source_cols) + 1))
        cursor.executemany(
            f"INSERT INTO {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE} ({insert_cols}) VALUES ({placeholders})",
            dq_issues_df.iter_rows(),
        )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}_reason "
        f"ON {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}(reason)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}_mbid "
        f"ON {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}(mbid)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}_qid "
        f"ON {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}(wikidata_id)"
    )

    dq_count = cursor.execute(
        f"SELECT COUNT(*) FROM {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}"
    ).fetchone()[0]
    bad_dup_mbid_null_count = cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE}
        WHERE reason = 'Duplicated MBID'
          AND NULLIF(TRIM(COALESCE(mbid, '')), '') IS NULL
        """
    ).fetchone()[0]
    log.info(
        "Wikimedia data quality table ready: %s rows=%d",
        WIKIMEDIA_DATA_QUALITY_ISSUES_TABLE,
        dq_count,
    )
    if bad_dup_mbid_null_count:
        log.warning(
            "Wikimedia quality gate anomaly: %d rows tagged Duplicated MBID have null/blank mbid",
            bad_dup_mbid_null_count,
        )


def _load_paths() -> dict[str, str]:
    config_path = _resolve_master_config_path()
    cfg = tm_config.load_config(config_path=config_path)

    config_dir = config_path.parent

    mb_cfg_raw = cfg.get("musicbrainz") if isinstance(cfg, dict) else None
    mb_cfg = mb_cfg_raw if isinstance(mb_cfg_raw, dict) else {}

    wd_cfg_raw = cfg.get("wikimedia") if isinstance(cfg, dict) else None
    wd_cfg = wd_cfg_raw if isinstance(wd_cfg_raw, dict) else {}

    allmusic_cfg_raw = cfg.get("allmusic") if isinstance(cfg, dict) else None
    allmusic_cfg = allmusic_cfg_raw if isinstance(allmusic_cfg_raw, dict) else {}

    emit_cfg_raw = cfg.get("emit_contributors") if isinstance(cfg, dict) else None
    emit_cfg = emit_cfg_raw if isinstance(emit_cfg_raw, dict) else {}

    sqlite_cfg_raw = emit_cfg.get("sqlite")
    sqlite_cfg = sqlite_cfg_raw if isinstance(sqlite_cfg_raw, dict) else {}

    contributors_db_candidate = str(mb_cfg.get("contributors_db", "")).strip()
    if not contributors_db_candidate:
        contributors_db_candidate = "master-data.db"
    if not contributors_db_candidate:
        raise ValueError("Could not resolve contributors DB path from TOML or defaults")

    allmusic_candidate = str(allmusic_cfg.get("metadata_db", "")).strip()

    wikimedia_candidate = str(wd_cfg.get("wikimedia_db", "")).strip()

    disambiguated_table = DISAMBIGUATED_TABLE
    namesakes_table = NAMESAKES_TABLE
    mb_table = str(emit_cfg.get("musicbrainz_table", DEFAULT_MB_TABLE)).strip() or DEFAULT_MB_TABLE
    wd_table = str(wd_cfg.get("target_table", DEFAULT_WD_TABLE)).strip() or DEFAULT_WD_TABLE

    busy_timeout_ms = int(sqlite_cfg.get("busy_timeout_ms", 5000))
    journal_mode = str(sqlite_cfg.get("journal_mode", "WAL")).strip().upper() or "WAL"
    synchronous = str(sqlite_cfg.get("synchronous", "NORMAL")).strip().upper() or "NORMAL"
    temp_store = str(sqlite_cfg.get("temp_store", "MEMORY")).strip().upper() or "MEMORY"
    cache_size_kib = int(sqlite_cfg.get("cache_size_kib", 1048576))
    run_optimize = bool(sqlite_cfg.get("run_optimize", True))

    paths = {
        "contributors_db": _resolve_path(contributors_db_candidate, config_dir),
        "allmusic_db": _resolve_path(allmusic_candidate, config_dir) if allmusic_candidate else None,
        "wikimedia_db": _resolve_path(wikimedia_candidate, config_dir) if wikimedia_candidate else None,
        "disambiguated_table": disambiguated_table,
        "namesakes_table": namesakes_table,
        "mb_table": mb_table,
        "wd_table": wd_table,
        "sqlite_busy_timeout_ms": str(busy_timeout_ms),
        "sqlite_journal_mode": journal_mode,
        "sqlite_synchronous": synchronous,
        "sqlite_temp_store": temp_store,
        "sqlite_cache_size_kib": str(cache_size_kib),
        "sqlite_run_optimize": "1" if run_optimize else "0",
    }
    return paths


def emit_contributors(*, analyze_only: bool = False) -> None:
    paths = _load_paths()
    contributors_db = str(paths["contributors_db"])
    allmusic_db = paths["allmusic_db"]
    wikimedia_db = paths["wikimedia_db"]
    output_table = "__contributors_unified_emit_work"
    disambiguated_table = str(paths["disambiguated_table"])
    namesakes_table = str(paths["namesakes_table"])
    mb_table = str(paths["mb_table"])
    wd_table = str(paths["wd_table"])
    sqlite_busy_timeout_ms = int(str(paths["sqlite_busy_timeout_ms"]))
    sqlite_journal_mode = str(paths["sqlite_journal_mode"])
    sqlite_synchronous = str(paths["sqlite_synchronous"])
    sqlite_temp_store = str(paths["sqlite_temp_store"])
    sqlite_cache_size_kib = int(str(paths["sqlite_cache_size_kib"]))
    sqlite_run_optimize = bool(int(str(paths["sqlite_run_optimize"])))

    db_path = Path(contributors_db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    t_total = time.perf_counter()
    conn = sqlite3.connect(contributors_db)
    cursor = conn.cursor()

    has_allmusic = False
    amg_attached = False
    if allmusic_db and Path(allmusic_db).exists():
        cursor.execute("ATTACH DATABASE ? AS amg", (allmusic_db,))
        amg_attached = True
        has_allmusic = _table_exists(conn, "amg", "amg_artists")
        if not has_allmusic:
            log.warning("AllMusic DB attached but table is missing: %s (amg_artists)", allmusic_db)
    elif allmusic_db:
        log.warning("AllMusic DB configured but not found: %s", allmusic_db)

    has_wikimedia = False
    wd_attached = False
    if wikimedia_db and Path(wikimedia_db).exists():
        cursor.execute("ATTACH DATABASE ? AS wd", (wikimedia_db,))
        wd_attached = True
        has_wikimedia = _table_exists(conn, "wd", wd_table)
        if not has_wikimedia:
            log.warning("Wikimedia DB attached but table is missing: %s (%s)", wikimedia_db, wd_table)
    elif wikimedia_db:
        log.warning("Wikimedia DB configured but not found: %s", wikimedia_db)

    if analyze_only:
        if not _table_exists(conn, "main", disambiguated_table) or not _table_exists(conn, "main", namesakes_table):
            if wd_attached:
                cursor.execute("DETACH DATABASE wd")
            if amg_attached:
                cursor.execute("DETACH DATABASE amg")
            conn.close()
            raise FileNotFoundError(
                "Analyze-only mode requires existing output tables: "
                f"{disambiguated_table}, {namesakes_table}. "
                "Run emit mode first to build it."
            )
        cursor.execute("DROP TABLE IF EXISTS __contributors_unified_analyze")
        cursor.execute(
            f"""
            CREATE TEMP TABLE __contributors_unified_analyze AS
            SELECT * FROM {disambiguated_table}
            UNION ALL
            SELECT * FROM {namesakes_table}
            """
        )
        _log_output_table_stats(cursor, "__contributors_unified_analyze")
        _build_norm_tables_for_diagnostics(
            cursor,
            mb_table=mb_table,
            wd_table=wd_table,
            has_allmusic=has_allmusic,
            has_wikimedia=has_wikimedia,
        )
        _build_wd_data_quality_rowids_table(cursor, has_wikimedia=has_wikimedia)
        _build_wd_exception_rowids_table(cursor, has_wikimedia=has_wikimedia)
        if has_wikimedia:
            _build_staged_mb_wd_match_table(conn, cursor)
        _log_diagnostics(
            cursor,
            output_table="__contributors_unified_analyze",
            has_allmusic=has_allmusic,
            has_wikimedia=has_wikimedia,
        )
        cursor.execute("DROP TABLE IF EXISTS mb_norm_t")
        cursor.execute("DROP TABLE IF EXISTS amg_rollup_t")
        cursor.execute("DROP TABLE IF EXISTS amg_genres_t")
        cursor.execute("DROP TABLE IF EXISTS amg_styles_t")
        cursor.execute("DROP TABLE IF EXISTS wd_norm_t")
        cursor.execute("DROP TABLE IF EXISTS wd_data_quality_rowids_t")
        cursor.execute("DROP TABLE IF EXISTS wd_data_quality_reasons_t")
        cursor.execute("DROP TABLE IF EXISTS wd_exception_rowids_t")
        cursor.execute("DROP TABLE IF EXISTS mb_wd_match_t")
        cursor.execute("DROP TABLE IF EXISTS amg_remaining_t")
        cursor.execute("DROP TABLE IF EXISTS wd_amg_match_pairs_t")
        cursor.execute("DROP TABLE IF EXISTS wd_amg_matched_rowids_t")
        cursor.execute("DROP TABLE IF EXISTS wd_amg_matched_mnids_t")
        cursor.execute("DROP TABLE IF EXISTS __contributors_unified_name_counts")
        cursor.execute("DROP TABLE IF EXISTS __contributors_unified_analyze")
        cursor.execute(f"DROP TABLE IF EXISTS {output_table}_mb_seed_enriched_t")
        conn.commit()
        if wd_attached:
            cursor.execute("DETACH DATABASE wd")
        if amg_attached:
            cursor.execute("DETACH DATABASE amg")
        conn.close()
        log.info("Analyze-only complete (%.1fs)", time.perf_counter() - t_total)
        return

    log.info(
        "SQLite tuning: busy_timeout_ms=%d journal_mode=%s synchronous=%s temp_store=%s cache_size_kib=%d optimize=%s",
        sqlite_busy_timeout_ms,
        sqlite_journal_mode,
        sqlite_synchronous,
        sqlite_temp_store,
        sqlite_cache_size_kib,
        "yes" if sqlite_run_optimize else "no",
    )

    pragmas = [
        f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}",
        f"PRAGMA journal_mode = {sqlite_journal_mode}",
        f"PRAGMA synchronous = {sqlite_synchronous}",
        f"PRAGMA temp_store = {sqlite_temp_store}",
        f"PRAGMA cache_size = {-sqlite_cache_size_kib}",
    ]
    if sqlite_run_optimize:
        pragmas.append("PRAGMA optimize")

    for pragma in pragmas:
        try:
            cursor.execute(pragma)
        except sqlite3.Error:
            continue

    if not _table_exists(conn, "main", mb_table):
        raise FileNotFoundError(
            f"Required table not found in contributors DB: {mb_table}. "
            "Run harvest_mb_artists.py first."
        )

    log.info(
        "Emit run: contributors_db=%s work_table=%s emit_tables=(%s,%s) mb_table=%s has_allmusic=%s has_wikimedia=%s",
        contributors_db,
        output_table,
        disambiguated_table,
        namesakes_table,
        mb_table,
        "yes" if has_allmusic else "no",
        "yes" if has_wikimedia else "no",
    )

    t_stage = time.perf_counter()
    _build_norm_tables_for_diagnostics(
        cursor,
        mb_table=mb_table,
        wd_table=wd_table,
        has_allmusic=has_allmusic,
        has_wikimedia=has_wikimedia,
    )
    _build_wd_data_quality_rowids_table(cursor, has_wikimedia=has_wikimedia)
    _build_wd_exception_rowids_table(cursor, has_wikimedia=has_wikimedia)
    log.info("Stage timing: build normalized temp tables %.2fs", time.perf_counter() - t_stage)

    cursor.execute(f"DROP TABLE IF EXISTS {output_table}")
    cursor.execute(
        f"""
        CREATE TEMP TABLE {output_table} (
            contributor_row_id INTEGER PRIMARY KEY,
            merge_key_mbid TEXT,
            merge_key_allmusic_mnid TEXT,
            merge_key_wikidata_id TEXT,
            has_musicbrainz_row INTEGER NOT NULL,
            has_allmusic_row INTEGER NOT NULL,
            has_wikimedia_row INTEGER NOT NULL,
            musicbrainz_artist_id INTEGER,
            musicbrainz_mbid TEXT,
            musicbrainz_artist_name TEXT,
            musicbrainz_begin_date_year INTEGER,
            musicbrainz_begin_date_month INTEGER,
            musicbrainz_begin_date_day INTEGER,
            musicbrainz_end_date_year INTEGER,
            musicbrainz_end_date_month INTEGER,
            musicbrainz_end_date_day INTEGER,
            musicbrainz_type INTEGER,
            musicbrainz_area INTEGER,
            musicbrainz_gender INTEGER,
            musicbrainz_disambiguation TEXT,
            musicbrainz_ended INTEGER,
            musicbrainz_wikidata_uri TEXT,
            musicbrainz_wikidata_id TEXT,
            musicbrainz_allmusic_mnid TEXT,
            musicbrainz_source_dump TEXT,
            musicbrainz_extracted_utc TEXT,
            allmusic_mnid TEXT,
            allmusic_artist TEXT,
            allmusic_url TEXT,
            allmusic_artist_input TEXT,
            allmusic_name_similarity REAL,
            allmusic_active TEXT,
            allmusic_born_date TEXT,
            allmusic_born_place TEXT,
            allmusic_biography_html TEXT,
            allmusic_enrichment_status TEXT,
            allmusic_first_seen_utc TEXT,
            allmusic_last_seen_utc TEXT,
            allmusic_last_enriched_utc TEXT,
            allmusic_last_source_mode TEXT,
            allmusic_raw_payload_json TEXT,
            allmusic_genres_json TEXT,
            allmusic_styles_json TEXT,
            allmusic_genre_count INTEGER,
            allmusic_style_count INTEGER,
            wikimedia_wikidata_uri TEXT,
            wikimedia_wikidata_id TEXT,
            wikimedia_wikidata_label TEXT,
            wikimedia_wikidata_aliases_json TEXT,
            wikimedia_mbid TEXT,
            wikimedia_allmusic_mnid TEXT,
            wikimedia_songkick_artist_id TEXT,
            wikimedia_apple_music_artist_id TEXT,
            wikimedia_discogs_artist_id TEXT,
            wikimedia_spotify_artist_id TEXT,
            wikimedia_lastfm_artist_id TEXT,
            wikimedia_youtube_channel_id TEXT,
            wikimedia_isni TEXT,
            wikimedia_viaf_id TEXT,
            wikimedia_official_website TEXT,
            wikimedia_gender TEXT,
            wikimedia_instance_of_wikidata_ids_json TEXT,
            wikimedia_occupation_wikidata_ids_json TEXT,
            wikimedia_citizenship_wikidata_ids_json TEXT,
            wikimedia_origin_country_wikidata_ids_json TEXT,
            wikimedia_place_of_birth_wikidata_id TEXT,
            wikimedia_place_of_death_wikidata_id TEXT,
            wikimedia_date_of_birth TEXT,
            wikimedia_date_of_death TEXT,
            wikimedia_inception TEXT,
            wikimedia_dissolved TEXT,
            wikimedia_genre_wikidata_ids_json TEXT,
            wikimedia_instrument_wikidata_ids_json TEXT,
            wikimedia_member_of_wikidata_ids_json TEXT,
            wikimedia_url TEXT,
            wikimedia_musicbrainz_url TEXT,
            wikimedia_allmusic_url TEXT,
            wikimedia_discogs_url TEXT,
            wikimedia_spotify_url TEXT,
            wikimedia_songkick_url TEXT,
            wikimedia_wikipedia_url TEXT,
            wikimedia_apple_lookup_url TEXT,
            wikimedia_source_dump TEXT,
            wikimedia_extracted_utc TEXT,
            aligns_mbid_musicbrainz_wikimedia INTEGER,
            aligns_mnid_musicbrainz_allmusic INTEGER,
            aligns_mnid_musicbrainz_wikimedia INTEGER,
            aligns_qid_musicbrainz_wikimedia INTEGER,
            aligns_mnid_allmusic_wikimedia INTEGER,
            aligns_mbid_all_sources INTEGER,
            aligns_mnid_all_sources INTEGER,
            aligns_qid_all_sources INTEGER,
            conflict_reason TEXT,
            record_origin TEXT NOT NULL,
            merge_created_utc TEXT,
            merge_updated_utc TEXT
        )
        """
    )


    cursor.execute(f"DROP TABLE IF EXISTS {output_table}_temp")

    if has_wikimedia:
        t_stage = time.perf_counter()
        _build_staged_mb_wd_match_table(conn, cursor)
        _refresh_wikimedia_data_quality_issues_table(
            cursor,
            wd_table=wd_table,
            has_wikimedia=has_wikimedia,
        )
        stage_counts = dict(
            cursor.execute(
                "SELECT match_stage, COUNT(*) FROM mb_wd_match_t GROUP BY match_stage"
            ).fetchall()
        )
        total_pairs = cursor.execute("SELECT COUNT(*) FROM mb_wd_match_t").fetchone()[0]
        log.info(
            "Stage timing: staged MB<->Wikimedia matcher %.2fs (pairs=%d mbid=%d qid=%d mnid=%d)",
            time.perf_counter() - t_stage,
            total_pairs,
            int(stage_counts.get("mbid", 0)),
            int(stage_counts.get("qid", 0)),
            int(stage_counts.get("mnid", 0)),
        )

    t_stage = time.perf_counter()
    if has_wikimedia:
        cursor.execute(
            f"""
            CREATE TEMP TABLE {output_table}_temp AS
            SELECT
                mb.artist_id AS contributor_row_id,
                mbn.mbid_n AS merge_key_mbid,
                mbn.mnid_n AS merge_key_allmusic_mnid,
                mbn.qid_n AS merge_key_wikidata_id,
                1 AS has_musicbrainz_row,
                0 AS has_allmusic_row,
                CASE WHEN w.wd_rowid IS NOT NULL THEN 1 ELSE 0 END AS has_wikimedia_row,
                mb.artist_id AS musicbrainz_artist_id,
                mb.mbid AS musicbrainz_mbid,
                mb.artist_name AS musicbrainz_artist_name,
                mb.begin_date_year AS musicbrainz_begin_date_year,
                mb.begin_date_month AS musicbrainz_begin_date_month,
                mb.begin_date_day AS musicbrainz_begin_date_day,
                mb.end_date_year AS musicbrainz_end_date_year,
                mb.end_date_month AS musicbrainz_end_date_month,
                mb.end_date_day AS musicbrainz_end_date_day,
                mb.type AS musicbrainz_type,
                mb.area AS musicbrainz_area,
                mb.gender AS musicbrainz_gender,
                mb.disambiguation AS musicbrainz_disambiguation,
                mb.ended AS musicbrainz_ended,
                mb.wikidata_uri AS musicbrainz_wikidata_uri,
                mb.wikidata_id AS musicbrainz_wikidata_id,
                mb.allmusic_mnid AS musicbrainz_allmusic_mnid,
                mb.source_dump AS musicbrainz_source_dump,
                mb.extracted_utc AS musicbrainz_extracted_utc,
                NULL AS allmusic_mnid,
                NULL AS allmusic_artist,
                NULL AS allmusic_url,
                NULL AS allmusic_artist_input,
                NULL AS allmusic_name_similarity,
                NULL AS allmusic_active,
                NULL AS allmusic_born_date,
                NULL AS allmusic_born_place,
                NULL AS allmusic_biography_html,
                NULL AS allmusic_enrichment_status,
                NULL AS allmusic_first_seen_utc,
                NULL AS allmusic_last_seen_utc,
                NULL AS allmusic_last_enriched_utc,
                NULL AS allmusic_last_source_mode,
                NULL AS allmusic_raw_payload_json,
                NULL AS allmusic_genres_json,
                NULL AS allmusic_styles_json,
                NULL AS allmusic_genre_count,
                NULL AS allmusic_style_count,
                w.wikidata_uri AS wikimedia_wikidata_uri,
                w.wikidata_id AS wikimedia_wikidata_id,
                w.wikidata_label AS wikimedia_wikidata_label,
                w.wikidata_aliases AS wikimedia_wikidata_aliases_json,
                w.mbid AS wikimedia_mbid,
                w.allmusic_mnid AS wikimedia_allmusic_mnid,
                w.songkick_artist_id AS wikimedia_songkick_artist_id,
                w.apple_music_artist_id AS wikimedia_apple_music_artist_id,
                w.discogs_artist_id AS wikimedia_discogs_artist_id,
                w.spotify_artist_id AS wikimedia_spotify_artist_id,
                w.lastfm_artist_id AS wikimedia_lastfm_artist_id,
                w.youtube_channel_id AS wikimedia_youtube_channel_id,
                w.isni AS wikimedia_isni,
                w.viaf_id AS wikimedia_viaf_id,
                w.official_website AS wikimedia_official_website,
                w.gender AS wikimedia_gender,
                w.instance_of_wikidata_ids AS wikimedia_instance_of_wikidata_ids_json,
                w.occupation_wikidata_ids AS wikimedia_occupation_wikidata_ids_json,
                w.citizenship_wikidata_ids AS wikimedia_citizenship_wikidata_ids_json,
                w.origin_country_wikidata_ids AS wikimedia_origin_country_wikidata_ids_json,
                w.place_of_birth_wikidata_id AS wikimedia_place_of_birth_wikidata_id,
                w.place_of_death_wikidata_id AS wikimedia_place_of_death_wikidata_id,
                w.date_of_birth AS wikimedia_date_of_birth,
                w.date_of_death AS wikimedia_date_of_death,
                w.inception AS wikimedia_inception,
                w.dissolved AS wikimedia_dissolved,
                w.genre_wikidata_ids AS wikimedia_genre_wikidata_ids_json,
                w.instrument_wikidata_ids AS wikimedia_instrument_wikidata_ids_json,
                w.member_of_wikidata_ids AS wikimedia_member_of_wikidata_ids_json,
                w.wikidata_url AS wikimedia_url,
                w.musicbrainz_url AS wikimedia_musicbrainz_url,
                w.allmusic_url AS wikimedia_allmusic_url,
                w.discogs_url AS wikimedia_discogs_url,
                w.spotify_url AS wikimedia_spotify_url,
                w.songkick_url AS wikimedia_songkick_url,
                w.wikipedia_url AS wikimedia_wikipedia_url,
                w.apple_lookup_url AS wikimedia_apple_lookup_url,
                w.source_dump AS wikimedia_source_dump,
                w.extracted_utc AS wikimedia_extracted_utc,
                CASE WHEN mbn.mbid_n IS NULL OR w.mbid_n IS NULL THEN NULL
                    WHEN mbn.mbid_n = w.mbid_n THEN 1 ELSE 0 END AS aligns_mbid_musicbrainz_wikimedia,
                NULL AS aligns_mnid_musicbrainz_allmusic,
                CASE WHEN mbn.mnid_n IS NULL OR w.mnid_n IS NULL THEN NULL
                    WHEN mbn.mnid_n = w.mnid_n THEN 1 ELSE 0 END AS aligns_mnid_musicbrainz_wikimedia,
                CASE WHEN mbn.qid_n IS NULL OR w.qid_n IS NULL THEN NULL
                    WHEN mbn.qid_n = w.qid_n THEN 1 ELSE 0 END AS aligns_qid_musicbrainz_wikimedia,
                NULL AS aligns_mnid_allmusic_wikimedia,
                NULL AS aligns_mbid_all_sources,
                NULL AS aligns_mnid_all_sources,
                CASE WHEN mbn.qid_n IS NULL OR w.qid_n IS NULL THEN NULL
                    WHEN mbn.qid_n = w.qid_n THEN 1 ELSE 0 END AS aligns_qid_all_sources,
                TRIM(
                    (CASE WHEN mbn.mbid_n IS NOT NULL AND w.mbid_n IS NOT NULL AND mbn.mbid_n <> w.mbid_n THEN 'mbid_conflict; ' ELSE '' END) ||
                    (CASE WHEN mbn.mnid_n IS NOT NULL AND w.mnid_n IS NOT NULL AND mbn.mnid_n <> w.mnid_n THEN 'mnid_conflict; ' ELSE '' END) ||
                    (CASE WHEN mbn.qid_n IS NOT NULL AND w.qid_n IS NOT NULL AND mbn.qid_n <> w.qid_n THEN 'qid_conflict; ' ELSE '' END)
                ) AS conflict_reason,
                'mb_seed' AS record_origin,
                CURRENT_TIMESTAMP AS merge_created_utc,
                CURRENT_TIMESTAMP AS merge_updated_utc
            FROM {mb_table} mb
            LEFT JOIN mb_norm_t mbn ON mbn.artist_id = mb.artist_id
            LEFT JOIN mb_wd_match_t mwd ON mwd.mb_artist_id = mb.artist_id
            LEFT JOIN wd_norm_t w ON w.wd_rowid = mwd.wd_rowid
            """
        )
    else:
        cursor.execute(
            f"""
            CREATE TEMP TABLE {output_table}_temp AS
            SELECT
                mb.artist_id AS contributor_row_id,
                mbn.mbid_n AS merge_key_mbid,
                mbn.mnid_n AS merge_key_allmusic_mnid,
                mbn.qid_n AS merge_key_wikidata_id,
                1 AS has_musicbrainz_row,
                0 AS has_allmusic_row,
                0 AS has_wikimedia_row,
                mb.artist_id AS musicbrainz_artist_id,
                mb.mbid AS musicbrainz_mbid,
                mb.artist_name AS musicbrainz_artist_name,
                mb.begin_date_year AS musicbrainz_begin_date_year,
                mb.begin_date_month AS musicbrainz_begin_date_month,
                mb.begin_date_day AS musicbrainz_begin_date_day,
                mb.end_date_year AS musicbrainz_end_date_year,
                mb.end_date_month AS musicbrainz_end_date_month,
                mb.end_date_day AS musicbrainz_end_date_day,
                mb.type AS musicbrainz_type,
                mb.area AS musicbrainz_area,
                mb.gender AS musicbrainz_gender,
                mb.disambiguation AS musicbrainz_disambiguation,
                mb.ended AS musicbrainz_ended,
                mb.wikidata_uri AS musicbrainz_wikidata_uri,
                mb.wikidata_id AS musicbrainz_wikidata_id,
                mb.allmusic_mnid AS musicbrainz_allmusic_mnid,
                mb.source_dump AS musicbrainz_source_dump,
                mb.extracted_utc AS musicbrainz_extracted_utc,
                NULL AS allmusic_mnid,
                NULL AS allmusic_artist,
                NULL AS allmusic_url,
                NULL AS allmusic_artist_input,
                NULL AS allmusic_name_similarity,
                NULL AS allmusic_active,
                NULL AS allmusic_born_date,
                NULL AS allmusic_born_place,
                NULL AS allmusic_biography_html,
                NULL AS allmusic_enrichment_status,
                NULL AS allmusic_first_seen_utc,
                NULL AS allmusic_last_seen_utc,
                NULL AS allmusic_last_enriched_utc,
                NULL AS allmusic_last_source_mode,
                NULL AS allmusic_raw_payload_json,
                NULL AS allmusic_genres_json,
                NULL AS allmusic_styles_json,
                NULL AS allmusic_genre_count,
                NULL AS allmusic_style_count,
                NULL AS wikimedia_wikidata_uri,
                NULL AS wikimedia_wikidata_id,
                NULL AS wikimedia_wikidata_label,
                NULL AS wikimedia_wikidata_aliases_json,
                NULL AS wikimedia_mbid,
                NULL AS wikimedia_allmusic_mnid,
                NULL AS wikimedia_songkick_artist_id,
                NULL AS wikimedia_apple_music_artist_id,
                NULL AS wikimedia_discogs_artist_id,
                NULL AS wikimedia_spotify_artist_id,
                NULL AS wikimedia_lastfm_artist_id,
                NULL AS wikimedia_youtube_channel_id,
                NULL AS wikimedia_isni,
                NULL AS wikimedia_viaf_id,
                NULL AS wikimedia_official_website,
                NULL AS wikimedia_gender,
                NULL AS wikimedia_instance_of_wikidata_ids_json,
                NULL AS wikimedia_occupation_wikidata_ids_json,
                NULL AS wikimedia_citizenship_wikidata_ids_json,
                NULL AS wikimedia_origin_country_wikidata_ids_json,
                NULL AS wikimedia_place_of_birth_wikidata_id,
                NULL AS wikimedia_place_of_death_wikidata_id,
                NULL AS wikimedia_date_of_birth,
                NULL AS wikimedia_date_of_death,
                NULL AS wikimedia_inception,
                NULL AS wikimedia_dissolved,
                NULL AS wikimedia_genre_wikidata_ids_json,
                NULL AS wikimedia_instrument_wikidata_ids_json,
                NULL AS wikimedia_member_of_wikidata_ids_json,
                NULL AS wikimedia_url,
                NULL AS wikimedia_musicbrainz_url,
                NULL AS wikimedia_allmusic_url,
                NULL AS wikimedia_discogs_url,
                NULL AS wikimedia_spotify_url,
                NULL AS wikimedia_songkick_url,
                NULL AS wikimedia_wikipedia_url,
                NULL AS wikimedia_apple_lookup_url,
                NULL AS wikimedia_source_dump,
                NULL AS wikimedia_extracted_utc,
                NULL AS aligns_mbid_musicbrainz_wikimedia,
                NULL AS aligns_mnid_musicbrainz_allmusic,
                NULL AS aligns_mnid_musicbrainz_wikimedia,
                NULL AS aligns_qid_musicbrainz_wikimedia,
                NULL AS aligns_mnid_allmusic_wikimedia,
                NULL AS aligns_mbid_all_sources,
                NULL AS aligns_mnid_all_sources,
                NULL AS aligns_qid_all_sources,
                NULL AS conflict_reason,
                'mb_seed' AS record_origin,
                CURRENT_TIMESTAMP AS merge_created_utc,
                CURRENT_TIMESTAMP AS merge_updated_utc
            FROM {mb_table} mb
            LEFT JOIN mb_norm_t mbn ON mbn.artist_id = mb.artist_id
            """
        )

    log.info("Stage timing: build %s_temp %.2fs", output_table, time.perf_counter() - t_stage)

    t_stage = time.perf_counter()
    cursor.execute(f"INSERT INTO {output_table} SELECT * FROM {output_table}_temp")
    log.info("Stage timing: insert into %s %.2fs", output_table, time.perf_counter() - t_stage)

    t_stage = time.perf_counter()
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_mbid ON {output_table}(merge_key_mbid)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_mnid ON {output_table}(merge_key_allmusic_mnid)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_qid ON {output_table}(merge_key_wikidata_id)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_has_mb ON {output_table}(has_musicbrainz_row)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_has_am ON {output_table}(has_allmusic_row)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_has_wd ON {output_table}(has_wikimedia_row)")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{output_table}_origin ON {output_table}(record_origin)")
    log.info("Stage timing: create output indexes %.2fs", time.perf_counter() - t_stage)

    _refresh_wikimedia_exception_tables(
        cursor,
        wd_table=wd_table,
        mb_table=mb_table,
        has_wikimedia=has_wikimedia,
    )
    _refresh_unmatched_wikimedia_table(
        cursor,
        wd_table=wd_table,
        has_wikimedia=has_wikimedia,
    )

    if has_allmusic:
        _apply_allmusic_allocation_phase(
            conn,
            cursor,
            output_table=output_table,
            has_wikimedia=has_wikimedia,
        )
    else:
        _refresh_unmatched_amg_table(cursor, has_allmusic=False)

    _promote_residual_rows_into_unified(
        cursor,
        output_table=output_table,
        has_allmusic=has_allmusic,
        has_wikimedia=has_wikimedia,
    )

    _ensure_preferred_name_columns(cursor, output_table)
    _emit_split_unified_tables(
        cursor,
        source_table=output_table,
        disambiguated_table=disambiguated_table,
        namesakes_table=namesakes_table,
    )

    conn.commit()

    disambiguated_count = cursor.execute(
        f"SELECT COUNT(*) FROM {disambiguated_table}"
    ).fetchone()[0]
    namesakes_count = cursor.execute(
        f"SELECT COUNT(*) FROM {namesakes_table}"
    ).fetchone()[0]
    log.info(
        "Unified split ready: %s rows=%d, %s rows=%d",
        disambiguated_table,
        disambiguated_count,
        namesakes_table,
        namesakes_count,
    )

    _log_output_table_stats(cursor, output_table)
    _log_diagnostics(
        cursor,
        output_table=output_table,
        has_allmusic=has_allmusic,
        has_wikimedia=has_wikimedia,
    )

    cursor.execute("DROP TABLE IF EXISTS mb_norm_t")
    cursor.execute("DROP TABLE IF EXISTS amg_rollup_t")
    cursor.execute("DROP TABLE IF EXISTS amg_genres_t")
    cursor.execute("DROP TABLE IF EXISTS amg_styles_t")
    cursor.execute("DROP TABLE IF EXISTS wd_norm_t")
    cursor.execute("DROP TABLE IF EXISTS wd_data_quality_rowids_t")
    cursor.execute("DROP TABLE IF EXISTS wd_data_quality_reasons_t")
    cursor.execute("DROP TABLE IF EXISTS wd_exception_rowids_t")
    cursor.execute("DROP TABLE IF EXISTS mb_wd_match_t")
    cursor.execute("DROP TABLE IF EXISTS amg_remaining_t")
    cursor.execute("DROP TABLE IF EXISTS wd_amg_match_pairs_t")
    cursor.execute("DROP TABLE IF EXISTS wd_amg_matched_rowids_t")
    cursor.execute("DROP TABLE IF EXISTS wd_amg_matched_mnids_t")
    cursor.execute("DROP TABLE IF EXISTS __contributors_unified_name_counts")
    cursor.execute(f"DROP TABLE IF EXISTS {output_table}_mb_seed_enriched_t")
    cursor.execute(f"DROP TABLE IF EXISTS {output_table}")
    conn.commit()
    if wd_attached:
        cursor.execute("DETACH DATABASE wd")
    if amg_attached:
        cursor.execute("DETACH DATABASE amg")

    conn.commit()
    conn.close()

    log.info("Emit complete (%.1fs)", time.perf_counter() - t_total)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Emit split unified contributor tables, or analyze stats from existing output tables."
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Skip rebuild and report analytics/stats from existing output table only.",
    )
    args = parser.parse_args()

    emit_contributors(analyze_only=args.analyze_only)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        main()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        raise SystemExit(1)
    except KeyboardInterrupt:
        log.warning("Emit aborted by user.")
        raise SystemExit(130)
