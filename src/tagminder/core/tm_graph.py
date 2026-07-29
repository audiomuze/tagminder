"""Graph-building helpers for Tagminder.

Purpose:
    Provide small, reusable graph constructors over the staging SQLite library
    (`alib`) using only system columns + keep_columns.

This module is part of Tagminder.

SQLite tables referenced:
    - alib

Author: audiomuze
Last updated: 2026-04-19
"""

from __future__ import annotations

from dataclasses import dataclass
import re

import polars as pl

from tagminder.core import tm_album
from tagminder.core import tm_db
from tagminder.core import tm_polars
from tagminder.core import tm_polars_db

@dataclass(frozen=True)
class WeightedGraph:
    """Simple adjacency list graph with integer edge weights."""

    nodes: list[str]
    adjacency: dict[str, list[tuple[str, int]]]


_UUID_PATTERN = r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"


def _null_list_like(list_expr: pl.Expr) -> pl.Expr:
    """Return a list of nulls with the same length as list_expr."""

    return pl.int_ranges(0, list_expr.list.len()).list.eval(pl.lit(None))


def _explode_entities(
    df: pl.DataFrame,
    *,
    album_root_col: str,
    name_col: str,
    delimiter: str,
    mbid_col: str | None = None,
) -> pl.DataFrame:
    """Explode a multi-value entity column into long rows.

    Returns columns:
        album_root, name, mbid

    If mbid_col is provided, attempt positional alignment when list lengths
    match; otherwise mbid values are null.
    """

    name_list = tm_polars.expr_tokens(pl.col(name_col), delimiter=delimiter).alias("_name_list")

    if mbid_col:
        mbid_list_raw = tm_polars.expr_tokens(pl.col(mbid_col), delimiter=delimiter).alias("_mbid_list_raw")
        name_len = pl.col("_name_list").list.len()
        mbid_len = pl.col("_mbid_list_raw").list.len()
        mbid_list = (
            pl.when(mbid_len == name_len)
            .then(pl.col("_mbid_list_raw"))
            .otherwise(_null_list_like(pl.col("_name_list")))
            .alias("_mbid_list")
        )
        base = (
            df.select(
                [
                    pl.col(album_root_col),
                    name_list,
                    mbid_list_raw,
                ]
            )
            .with_columns(mbid_list)
            .select([pl.col(album_root_col), pl.col("_name_list"), pl.col("_mbid_list")])
        )
        exploded = base.explode(["_name_list", "_mbid_list"]).rename(
            {"_name_list": "name", "_mbid_list": "mbid"}
        )
    else:
        base = df.select([pl.col(album_root_col), name_list])
        exploded = base.explode(["_name_list"]).with_columns(pl.lit(None).alias("mbid")).rename(
            {"_name_list": "name"}
        )

    return (
        exploded
        .with_columns(
            [
                pl.col("name").cast(pl.Utf8, strict=False).str.strip_chars(),
                pl.col("mbid").cast(pl.Utf8, strict=False).str.strip_chars(),
            ]
        )
        .filter(pl.col("name").is_not_null() & (pl.col("name") != ""))
    )


def build_artist_similarity_graph(
    *,
    db_path: str,
    system_prefix: str,
    delimiter: str,
    include_credit_roles: bool = True,
    include_genre_style_proximity: bool = True,
    min_tag_df: int = 2,
    max_tag_df: int = 50,
) -> WeightedGraph:
    """Build an enriched similarity graph for library artists.

    Nodes:
        - Artists present in the library (from albumartist + artist), with
          optional MusicBrainz-ID-based unification when IDs are available.

    Similarity signals (enabled by default):
        1) Album-level co-occurrence: albumartist ↔ artist per album_root
           (weight = distinct album_roots supporting the link).
        2) Credit-role proximity: artists share producers/engineers/mixers/
           remixers/composers (bounded by feature document frequency).
        3) Genre/style proximity: artists share genre/style tags (bounded by
           tag document frequency).

    Rationale:
        Credit-role tokens and tags are treated as *features* that add weight
        to artist↔artist edges; we do not emit producers/composers/etc as
        separate graph nodes to keep the map performant.
    """

    dir_col = f"{system_prefix}dirpath"

    # Artists are the only graph nodes.
    artist_cols: list[tuple[str, str | None]] = [
        ("albumartist", "musicbrainz_albumartistid"),
        ("artist", "musicbrainz_artistid"),
    ]

    credit_role_cols: list[str] = []
    if include_credit_roles:
        credit_role_cols = [
            "producer",
            "engineer",
            "mixer",
            "remixer",
            "composer",
        ]

    # Select only columns that exist in the current DB.
    conn = tm_db.connect(db_path, read_only=True, wal=False)
    try:
        df_cols = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT name FROM pragma_table_info('alib')",
        )
        existing_cols = set(df_cols["name"].to_list()) if "name" in df_cols.columns else set()

        wanted_cols: list[str] = [dir_col]

        for c, mb in artist_cols:
            if c in existing_cols:
                wanted_cols.append(c)
            if mb and mb in existing_cols:
                wanted_cols.append(mb)

        for c in credit_role_cols:
            if c in existing_cols:
                wanted_cols.append(c)

        if include_genre_style_proximity:
            for c in ("genre", "style"):
                if c in existing_cols:
                    wanted_cols.append(c)

        wanted_cols = sorted(set(wanted_cols), key=wanted_cols.index)

        df = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT " + ", ".join(tm_db.quote_ident(c) for c in wanted_cols) + " FROM alib",
        )
    finally:
        conn.close()

    if df.is_empty() or dir_col not in df.columns:
        return WeightedGraph(nodes=[], adjacency={})

    df = df.with_columns(tm_album.album_root_polars_expr(dir_col, out_col="album_root")).drop_nulls(
        ["album_root"]
    )

    # Build long artist table: album_root, name, mbid.
    artist_parts: list[pl.DataFrame] = []
    for name_col, mbid_col in artist_cols:
        if name_col not in df.columns:
            continue
        mbid_col2 = mbid_col if (mbid_col and mbid_col in df.columns) else None
        artist_parts.append(
            _explode_entities(
                df,
                album_root_col="album_root",
                name_col=name_col,
                mbid_col=mbid_col2,
                delimiter=delimiter,
            )
        )

    if not artist_parts:
        return WeightedGraph(nodes=[], adjacency={})

    artists_long = pl.concat(artist_parts, how="vertical")

    # MBID-aware unification into a stable artist label.
    artists_long = artists_long.with_columns(
        pl.when(
            pl.col("mbid").is_not_null()
            & (pl.col("mbid") != "")
            & pl.col("mbid").str.contains(_UUID_PATTERN)
        )
        .then(pl.concat_str([pl.lit("mbid:"), pl.col("mbid")]))
        .otherwise(pl.col("name"))
        .alias("artist_id")
    )

    mbid_labels = (
        artists_long.filter(pl.col("artist_id").str.starts_with("mbid:"))
        .group_by("artist_id")
        .agg(pl.col("name").mode().first().alias("label"))
    )

    artists_long = (
        artists_long.join(mbid_labels, on="artist_id", how="left")
        .with_columns(pl.coalesce([pl.col("label"), pl.col("name")]).alias("artist"))
    )

    # Recompute per-album unique albumartist and artist membership separately
    # (to preserve the albumartist ↔ artist structure used by the original graph).
    albumartists = (
        _explode_entities(
            df,
            album_root_col="album_root",
            name_col="albumartist",
            mbid_col=("musicbrainz_albumartistid" if "musicbrainz_albumartistid" in df.columns else None),
            delimiter=delimiter,
        )
        if "albumartist" in df.columns
        else pl.DataFrame({"album_root": [], "name": [], "mbid": []})
    )
    trackartists = (
        _explode_entities(
            df,
            album_root_col="album_root",
            name_col="artist",
            mbid_col=("musicbrainz_artistid" if "musicbrainz_artistid" in df.columns else None),
            delimiter=delimiter,
        )
        if "artist" in df.columns
        else pl.DataFrame({"album_root": [], "name": [], "mbid": []})
    )

    def _unify_artist(d: pl.DataFrame) -> pl.DataFrame:
        if d.is_empty():
            return d.with_columns(pl.lit(None).alias("artist_id"), pl.lit(None).alias("artist")).select(
                ["album_root", "artist"]
            )
        dd = d.with_columns(
            pl.when(
                pl.col("mbid").is_not_null()
                & (pl.col("mbid") != "")
                & pl.col("mbid").str.contains(_UUID_PATTERN)
            )
            .then(pl.concat_str([pl.lit("mbid:"), pl.col("mbid")]))
            .otherwise(pl.col("name"))
            .alias("artist_id")
        ).join(mbid_labels, on="artist_id", how="left").with_columns(
            pl.coalesce([pl.col("label"), pl.col("name")]).alias("artist")
        )
        return (
            dd.select(["album_root", "artist"])  # keep labels only
            .filter(pl.col("artist").is_not_null() & (pl.col("artist") != ""))
            .unique(subset=["album_root", "artist"])
        )

    aa_m = _unify_artist(albumartists)
    ar_m = _unify_artist(trackartists)

    if aa_m.is_empty() or ar_m.is_empty():
        return WeightedGraph(nodes=[], adjacency={})

    pairs = (
        aa_m.join(ar_m, on="album_root", how="inner", suffix="_b")
        .rename({"artist": "src", "artist_b": "dst"})
        .filter(pl.col("src") != pl.col("dst"))
        .unique(subset=["album_root", "src", "dst"])
        .group_by(["src", "dst"])
        .len()
        .rename({"len": "w"})
    )

    # Start an edge-weight dict (undirected, canonical order).
    edge_w: dict[tuple[str, str], int] = {}
    for src, dst, w in pairs.select(["src", "dst", "w"]).iter_rows():
        a = str(src)
        b = str(dst)
        if not a or not b or a == b:
            continue
        # canonicalize (undirected)
        if a.lower() <= b.lower():
            key = (a, b)
        else:
            key = (b, a)
        edge_w[key] = edge_w.get(key, 0) + int(w)

    # Credit-role proximity: artists share role-person tokens.
    if include_credit_roles and credit_role_cols:
        # album_root -> artists (union of albumartist+artist)
        artists_by_album = (
            pl.concat([aa_m, ar_m], how="vertical")
            .unique(subset=["album_root", "artist"])
            .rename({"artist": "artist"})
        )

        for role_col in credit_role_cols:
            if role_col not in df.columns:
                continue
            role_people = (
                df.select(
                    [
                        pl.col("album_root"),
                        tm_polars.expr_tokens(pl.col(role_col), delimiter=delimiter).alias("role"),
                    ]
                )
                .explode("role")
                .with_columns(pl.col("role").cast(pl.Utf8, strict=False).str.strip_chars())
                .filter(pl.col("role").is_not_null() & (pl.col("role") != ""))
                .unique(subset=["album_root", "role"])
            )

            if role_people.is_empty():
                continue

            role_artist = (
                artists_by_album.join(role_people, on="album_root", how="inner")
                .select(["role", "artist"])
                .unique(subset=["role", "artist"])
            )
            if role_artist.is_empty():
                continue

            groups = (
                role_artist.group_by("role")
                .agg(
                    [
                        pl.col("artist").unique().alias("artists"),
                        pl.col("artist").n_unique().alias("df"),
                    ]
                )
                .filter((pl.col("df") >= int(min_tag_df)) & (pl.col("df") <= int(max_tag_df)))
            )

            for _role, artists, _dfv in groups.select(["role", "artists", "df"]).iter_rows():
                arts = [str(a) for a in (artists or []) if a]
                if len(arts) < 2:
                    continue
                arts.sort(key=lambda s: s.lower())
                for i in range(len(arts)):
                    a = arts[i]
                    for j in range(i + 1, len(arts)):
                        b = arts[j]
                        edge_w[(a, b)] = edge_w.get((a, b), 0) + 1

    # Genre/style proximity: add +1 per shared tag within df bounds.
    if include_genre_style_proximity and ("genre" in df.columns or "style" in df.columns):
        artist_by_album = (
            pl.concat([aa_m, ar_m], how="vertical").unique(subset=["album_root", "artist"])
        )

        tag_frames: list[pl.DataFrame] = []
        for tag_col in ("genre", "style"):
            if tag_col not in df.columns:
                continue
            tags = (
                df.select(
                    [
                        pl.col("album_root"),
                        tm_polars.expr_tokens(pl.col(tag_col), delimiter=delimiter).alias("tag"),
                    ]
                )
                .explode("tag")
                .with_columns(pl.col("tag").cast(pl.Utf8, strict=False).str.strip_chars())
                .filter(pl.col("tag").is_not_null() & (pl.col("tag") != ""))
                .unique(subset=["album_root", "tag"])
            )
            if not tags.is_empty():
                tag_frames.append(tags)

        if tag_frames:
            tags_by_album = pl.concat(tag_frames, how="vertical").unique(subset=["album_root", "tag"])
            artist_tags = (
                artist_by_album.join(tags_by_album, on="album_root", how="inner")
                .select(["tag", "artist"])
                .unique(subset=["tag", "artist"])
            )

            if not artist_tags.is_empty():
                tag_groups = (
                    artist_tags.group_by("tag")
                    .agg(
                        [
                            pl.col("artist").unique().alias("artists"),
                            pl.col("artist").n_unique().alias("df"),
                        ]
                    )
                    .filter((pl.col("df") >= int(min_tag_df)) & (pl.col("df") <= int(max_tag_df)))
                )

                for _tag, artists, _dfv in tag_groups.select(["tag", "artists", "df"]).iter_rows():
                    arts = [str(a) for a in (artists or []) if a]
                    if len(arts) < 2:
                        continue
                    arts.sort(key=lambda s: s.lower())
                    for i in range(len(arts)):
                        a = arts[i]
                        for j in range(i + 1, len(arts)):
                            b = arts[j]
                            edge_w[(a, b)] = edge_w.get((a, b), 0) + 1

    if not edge_w:
        return WeightedGraph(nodes=[], adjacency={})

    # Build nodes and adjacency from edge_w.
    nodes_set: set[str] = set()
    adjacency: dict[str, list[tuple[str, int]]] = {}
    for (a, b), w in edge_w.items():
        if w <= 0:
            continue
        nodes_set.add(a)
        nodes_set.add(b)
        adjacency.setdefault(a, []).append((b, int(w)))
        adjacency.setdefault(b, []).append((a, int(w)))

    nodes = sorted(nodes_set, key=lambda s: s.lower())

    for s, neigh in adjacency.items():
        neigh.sort(key=lambda t: (-int(t[1]), str(t[0]).lower()))

    return WeightedGraph(nodes=nodes, adjacency=adjacency)


def build_albumartist_artist_graph(
    *,
    db_path: str,
    system_prefix: str,
    delimiter: str,
) -> WeightedGraph:
    """Build a person graph using albumartist↔artist co-occurrence per album.

    Definition:
        For each album (album_root), connect each albumartist token to each
        track-artist token observed on that album. Edge weight is the number of
        distinct album_root values supporting the relationship.

    Notes:
        - This intentionally uses album-level deduplication (not track count).
        - This is a pragmatic "artist exploration" seed graph.
    """

    dir_col = f"{system_prefix}dirpath"

    conn = tm_db.connect(db_path, read_only=True, wal=False)
    try:
        # Keep the query tiny; most work happens in Polars.
        df = tm_polars_db.sqlite_to_polars(
            conn,
            "SELECT "
            + ", ".join(
                [
                    tm_db.quote_ident(dir_col),
                    tm_db.quote_ident("albumartist"),
                    tm_db.quote_ident("artist"),
                ]
            )
            + " FROM alib",
        )
    finally:
        conn.close()

    if df.is_empty() or dir_col not in df.columns:
        return WeightedGraph(nodes=[], adjacency={})

    df = df.with_columns(
        [
            tm_album.album_root_polars_expr(dir_col, out_col="album_root"),
            tm_polars.expr_tokens(pl.col("albumartist"), delimiter=delimiter).alias("aa_tok"),
            tm_polars.expr_tokens(pl.col("artist"), delimiter=delimiter).alias("ar_tok"),
        ]
    ).drop_nulls(["album_root"])

    # Per-album unique token membership.
    aa = (
        df.select([pl.col("album_root"), pl.col("aa_tok")])
        .explode("aa_tok")
        .drop_nulls(["aa_tok"])
        .with_columns(pl.col("aa_tok").cast(pl.Utf8, strict=False).str.strip_chars())
        .filter(pl.col("aa_tok") != "")
        .unique(subset=["album_root", "aa_tok"])
    )
    ar = (
        df.select([pl.col("album_root"), pl.col("ar_tok")])
        .explode("ar_tok")
        .drop_nulls(["ar_tok"])
        .with_columns(pl.col("ar_tok").cast(pl.Utf8, strict=False).str.strip_chars())
        .filter(pl.col("ar_tok") != "")
        .unique(subset=["album_root", "ar_tok"])
    )

    if aa.is_empty() or ar.is_empty():
        return WeightedGraph(nodes=[], adjacency={})

    # Cross product within album_root yields edges. De-dupe per album_root.
    edges = (
        aa.join(ar, on="album_root", how="inner")
        .rename({"aa_tok": "src", "ar_tok": "dst"})
        .filter(pl.col("src") != pl.col("dst"))
        .unique(subset=["album_root", "src", "dst"])
        .group_by(["src", "dst"])
        .len()
        .rename({"len": "w"})
    )

    if edges.is_empty():
        return WeightedGraph(nodes=[], adjacency={})

    # Symmetrize so browsing works from either side.
    edges2 = pl.concat(
        [
            edges.select([pl.col("src"), pl.col("dst"), pl.col("w")]),
            edges.select([pl.col("dst").alias("src"), pl.col("src").alias("dst"), pl.col("w")]),
        ]
    )

    nodes = (
        pl.concat([
            edges2.select(pl.col("src").alias("n")),
            edges2.select(pl.col("dst").alias("n")),
        ])
        .unique()
        .sort("n")
    )["n"].to_list()

    # Convert to an adjacency mapping for fast interactive browsing.
    adjacency: dict[str, list[tuple[str, int]]] = {}
    for src, dst, w in edges2.select(["src", "dst", "w"]).iter_rows():
        s = str(src)
        d = str(dst)
        ww = int(w)
        adjacency.setdefault(s, []).append((d, ww))

    # Pre-sort neighbor lists for stable UX.
    for s, neigh in adjacency.items():
        neigh.sort(key=lambda t: (-int(t[1]), str(t[0]).lower()))

    return WeightedGraph(nodes=nodes, adjacency=adjacency)
