"""Browser-based artist similarity map (offline HTML).

Purpose:
    Generate a spatial-layout graph experience (music-map style) for artists
    present in the indexed library.

Approach:
    - Build a weighted artist graph from the staging DB (via tm_graph).
    - Emit a self-contained HTML file that:
        * renders a force-directed map on a Canvas
        * supports pan/zoom + click-to-focus
        * progressively expands neighbors as you move toward the viewport edge

This module is part of Tagminder.

SQLite tables referenced:
    - alib

Author: audiomuze
Last updated: 2026-04-19
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from tagminder.core import tm_graph

def _trim_graph(
    graph: tm_graph.WeightedGraph,
    *,
    top_k_per_node: int,
    min_weight: int,
) -> tuple[list[str], list[list[list[int]]]]:
    """Return (names, adjacency) using compact integer IDs.

    adjacency[id] = [[neighbor_id, weight], ...]
    """

    names = list(graph.nodes)
    id_by_name = {n: i for i, n in enumerate(names)}

    adj: list[list[list[int]]] = [[] for _ in range(len(names))]
    for src, neigh in graph.adjacency.items():
        sid = id_by_name.get(src)
        if sid is None:
            continue

        kept: list[list[int]] = []
        for dst, w in neigh:
            if int(w) < int(min_weight):
                continue
            did = id_by_name.get(dst)
            if did is None or did == sid:
                continue
            kept.append([int(did), int(w)])
            if len(kept) >= int(top_k_per_node):
                break
        adj[sid] = kept

    return names, adj


def write_artist_similarity_map_html(
    *,
    out_path: Path,
    graph: tm_graph.WeightedGraph,
  top_k_per_node: int = 30,
    min_weight: int = 1,
) -> Path:
    """Write a self-contained HTML similarity map and return its path."""

    out_path.parent.mkdir(parents=True, exist_ok=True)

    names, adj = _trim_graph(graph, top_k_per_node=top_k_per_node, min_weight=min_weight)

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    payload = {
        "names": names,
        "adj": adj,
        "top_k_per_node": int(top_k_per_node),
        "min_weight": int(min_weight),
        "generated_at": generated_at,
    }

    data_json = json.dumps(payload, ensure_ascii=False)

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Tagminder — Artist Similarity Map</title>
  <style>
    :root {{ color-scheme: dark light; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif; }}

    .wrap {{ height: 100%; display: grid; grid-template-rows: auto 1fr; }}
    .toolbar {{
      display: flex; gap: 10px; align-items: center;
      padding: 10px 12px; border-bottom: 1px solid rgba(127,127,127,0.25);
    }}
    .toolbar input {{
      width: min(520px, 60vw);
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid rgba(127,127,127,0.35);
      background: transparent;
      color: inherit;
      outline: none;
    }}
    .toolbar button {{
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid rgba(127,127,127,0.35);
      background: transparent;
      color: inherit;
      cursor: pointer;
    }}
    .hint {{ opacity: 0.75; font-size: 12px; }}
    #stamp {{ margin-left: auto; text-align: right; max-width: min(45vw, 560px); }}

    #stage {{ position: relative; overflow: hidden; }}
    canvas {{ width: 100%; height: 100%; display: block; }}

    .tooltip {{
      position: absolute;
      pointer-events: none;
      padding: 6px 8px;
      border-radius: 8px;
      border: 1px solid rgba(127,127,127,0.35);
      background: rgba(20, 20, 20, 0.90);
      color: #f0f0f0;
      font-size: 12px;
      opacity: 0;
      transform: translate(10px, 10px);
      max-width: 520px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"toolbar\">
      <input id=\"q\" type=\"text\" placeholder=\"Search artist/person… (press Enter)\" />
      <button id=\"random\" type=\"button\">Random focus</button>
      <button id=\"reset\" type=\"button\">Reset view</button>
      <div class=\"hint\">Drag to pan, wheel to zoom, double-click a node/label to focus (and center).</div>
      <div id=\"stamp\" class=\"hint\"></div>
    </div>
    <div id=\"stage\">
      <canvas id=\"c\"></canvas>
      <div id=\"tip\" class=\"tooltip\"></div>
    </div>
  </div>

  <script src=\"https://cdn.jsdelivr.net/npm/d3@7\"></script>
  <script>
  const DATA = {data_json};

  // Visible build stamp so it’s obvious which file you’re looking at.
  const stamp = document.getElementById('stamp');
  if (stamp) {{
    const g = DATA.generated_at || 'unknown';
    const topk = (DATA.top_k_per_node ?? '?');
    const minw = (DATA.min_weight ?? '?');
    stamp.textContent = 'build ' + g + '  |  nodes ' + DATA.names.length + '  |  top_k ' + topk + '  |  min_w ' + minw;
  }}

  const NAMES = DATA.names;
  const ADJ = DATA.adj;

  const stage = document.getElementById('stage');
  const canvas = document.getElementById('c');
  const tip = document.getElementById('tip');
  const input = document.getElementById('q');

  const ctx = canvas.getContext('2d');

  // Layout + label tuning (computed from viewport).
  const BASE_LABEL_PX = 11;
  let LAYOUT = {{
    s: 1.0,
    linkBase: 64,
    linkVar: 320,
    charge: -160,
    collisionPad: 26,
  }};

  // Cache label widths measured at BASE_LABEL_PX.
  const labelWidthPx = new Map();
  function getLabelWidthPx(n) {{
    const key = n.id;
    if (labelWidthPx.has(key)) return labelWidthPx.get(key);
    ctx.save();
    ctx.font = `${{BASE_LABEL_PX}}px system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif`;
    const w = Math.max(0, (ctx.measureText(n.name || '').width || 0));
    ctx.restore();
    labelWidthPx.set(key, w);
    return w;
  }}

  function computeLayout() {{
    const rect = stage.getBoundingClientRect();
    const dpr = Math.max(1, window.devicePixelRatio || 1);
    const area = Math.max(1, rect.width * rect.height);
    // Scale roughly with viewport pixel area; clamped for stability.
    let s = Math.sqrt((area * dpr) / (1100 * 700));
    s = Math.max(0.9, Math.min(3.2, s));

    // With a small live-node budget (~30), we want more separation so labels
    // remain readable and nodes don’t clump into a blob.
    const density = Math.max(1.0, Math.min(1.65, 45 / Math.max(20, PRUNE_TO)));

    LAYOUT = {{
      s: s,
      // Keep links longer on larger screens; still inversely related to weight.
      linkBase: 70 * s * density,
      linkVar: 380 * s * density,
      // Strong repulsion for label readability.
      charge: -220 * Math.pow(s, 1.12) * Math.pow(density, 1.25),
      collisionPad: 30 * s * density,
    }};
  }}

  function resize() {{
    const rect = stage.getBoundingClientRect();
    const dpr = Math.max(1, window.devicePixelRatio || 1);
    canvas.width = Math.floor(rect.width * dpr);
    canvas.height = Math.floor(rect.height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    STAGE_W = rect.width;
    STAGE_H = rect.height;
    computeLayout();
    applyLayout(false);
    clampNodesToViewport();
  }}
  window.addEventListener('resize', () => {{ resize(); redraw(); }});

  // World -> screen transform via d3-zoom.
  let transform = d3.zoomIdentity;

  // Cached stage size in CSS pixels (updated on resize).
  let STAGE_W = 0;
  let STAGE_H = 0;

  // Track drag direction so we can expand/contract on mouse release.
  let _dragStartTransform = null;
  let _suppressZoomEnd = false;

  const zoom = d3.zoom()
    .scaleExtent([0.08, 8])
    .on('start', (event) => {{
      _dragStartTransform = event.transform;
    }})
    .on('zoom', (event) => {{
      transform = event.transform;
      applyLayout(false);
      redraw();
    }})
    .on('end', (event) => {{
      try {{
        if (_suppressZoomEnd) return;
        if (_dragStartTransform) {{
          const dx = (event.transform.x - _dragStartTransform.x) || 0;
          const dy = (event.transform.y - _dragStartTransform.y) || 0;
          // Dragging moves the content; blank space opens in the opposite direction.
          maybeAutoAdapt(-dx, -dy);
        }}

        // If the user panned so far that nothing is visible,
        // snap back to something sensible (keep zoom level).
        ensureSomethingVisible();
      }} finally {{
        _dragStartTransform = null;
      }}
    }});

  d3.select(canvas).call(zoom);
  // Disable d3's default double-click zoom so dblclick can be used for selection.
  d3.select(canvas).on('dblclick.zoom', null);

  // Simulation (only for currently loaded nodes)
  const forceCharge = d3.forceManyBody();
  const forceLink = d3.forceLink([]).id(d => d.id);
  const forceCollide = d3.forceCollide();

  const sim = d3.forceSimulation([])
    .force('charge', forceCharge)
    .force('link', forceLink)
    .force('center', d3.forceCenter(0, 0))
    .force('collision', forceCollide);

  // Keep nodes within the visible viewport to avoid “lost off-screen” drift.
  // Implemented as a soft constraint force (nudges velocity), not a hard clamp.
  function forceConstrainToViewport() {{
    let _nodes = [];
    function force(alpha) {{
      const k = Math.max(0.001, transform.k || 1);
      const w = Math.max(1, STAGE_W || stage.getBoundingClientRect().width);
      const h = Math.max(1, STAGE_H || stage.getBoundingClientRect().height);
      const padPx = 26;
      const padW = padPx / k;

      // Convert screen-space viewport bounds to world coordinates.
      const minX = (0 - transform.x) / k + padW;
      const maxX = (w - transform.x) / k - padW;
      const minY = (0 - transform.y) / k + padW;
      const maxY = (h - transform.y) / k - padW;

      if (!(minX < maxX && minY < maxY)) return;

      // Strength tuned to be noticeable but not jittery.
      const s = 0.22 * (alpha || 1);
      for (const n of _nodes) {{
        if (!Number.isFinite(n.x) || !Number.isFinite(n.y)) continue;

        const rr = (typeof n.r === 'number') ? n.r : 0;
        const loX = minX + rr;
        const hiX = maxX - rr;
        const loY = minY + rr;
        const hiY = maxY - rr;

        if (n.x < loX) n.vx += (loX - n.x) * s;
        else if (n.x > hiX) n.vx -= (n.x - hiX) * s;

        if (n.y < loY) n.vy += (loY - n.y) * s;
        else if (n.y > hiY) n.vy -= (n.y - hiY) * s;
      }}
    }}
    force.initialize = function(nodes) {{ _nodes = nodes || []; }};
    return force;
  }}

  sim.force('constrain', forceConstrainToViewport());

  let nodes = [];
  let links = [];

  // Currently focused (central) node id.
  let focusId = null;

  // Visual emphasis: top-N most related neighbors of the focused node.
  const FOCUS_TOP_N = 10;
  let focusTopSet = new Set();

  function recomputeFocusTop() {{
    focusTopSet = new Set();
    if (focusId === null) return;
    const arr = (ADJ[focusId] || []).slice();
    // Ensure it's sorted strongest-first.
    arr.sort((a, b) => (b[1] || 0) - (a[1] || 0));
    for (let i = 0; i < Math.min(FOCUS_TOP_N, arr.length); i++) {{
      focusTopSet.add(arr[i][0]);
    }}
  }}

  function visualRadius(n) {{
    if (focusId !== null && focusTopSet.has(n.id)) return n.r + 2.2;
    return n.r;
  }}

  // Screen-space hitboxes for labels drawn in the last redraw.
  // Each entry has: id, x0, y0, x1, y1
  let lastLabelHitboxes = [];

  function applyLayout(restart = true) {{
    const k = Math.max(0.001, transform.k || 1);

    // Make spacing feel stable in *screen space* across zoom levels.
    // When zoomed out (k small) things otherwise clump; when zoomed in (k large)
    // repulsion/collision can become too aggressive.
    const invK = 1 / k;
    const repelScale = Math.max(0.35, Math.min(2.2, invK));

    // Hard floor in screen pixels so neighbors can’t collapse into a blob.
    // (Converted to world units via invK below.)
    const MIN_LINK_PX = 185;

    forceCharge.strength(LAYOUT.charge * repelScale);
    forceLink
      .distance(l => {{
        const px = (LAYOUT.linkBase + LAYOUT.linkVar / Math.max(1, l.w));
        return Math.max(MIN_LINK_PX, px) * invK;
      }})
      .strength(0.11);

    // Key idea: labels are drawn at roughly constant *screen* size, so their
    // world-space footprint scales as 1/k. Inflate collision radius by label
    // width to bias the sim toward non-overlapping names.
    forceCollide
      .radius(d => d.r + (LAYOUT.collisionPad * invK) + (getLabelWidthPx(d) * invK) * 0.92)
      .strength(0.92);

    if (restart) {{
      sim.alpha(0.55).restart();
    }}
  }}

  const nodeById = new Map();
  // How many neighbors we've expanded for a given node id.
  const expandedCount = new Map();

  // Live set size controls (keep the map responsive/readable).
  // With the current UX (no link rendering + labels), the map is most legible
  // with a small live set. Keep a little headroom above PRUNE_TO so expansions
  // can add, then prune back.
  const PRUNE_TO = 30;
  const MAX_NODES = 40;

  // Auto-adapt tuning (conservative to avoid runaway growth).
  const AUTO_ADAPT_MIN_DRAG_PX = 70;
  const AUTO_EXPAND_LIMIT_PER_NODE = 5;
  const AUTO_EXPAND_MAX_SOURCES = 3;
  const AUTO_BLANK_THRESHOLD_PX = 140;
  const AUTO_SNAP_VISIBLE_MARGIN_PX = 40;

  function degree(id) {{
    const a = ADJ[id];
    return a ? a.length : 0;
  }}

  function nodeRadius(id) {{
    // sqrt-ish scaling; keep compact.
    const d = degree(id);
    return Math.max(3.0, Math.min(9.5, 2.7 + Math.sqrt(d)));
  }}

  function ensureNode(id) {{
    if (nodeById.has(id)) return nodeById.get(id);
    const n = {{ id, name: NAMES[id], r: nodeRadius(id), x: (Math.random()-0.5)*40, y: (Math.random()-0.5)*40 }};
    nodeById.set(id, n);
    nodes.push(n);
    return n;
  }}

  function ensureLink(a, b, w) {{
    // Keep a canonical key to avoid duplicates.
    const key = a < b ? `${{a}}-${{b}}` : `${{b}}-${{a}}`;
    if (ensureLink._seen.has(key)) return;
    ensureLink._seen.add(key);
    links.push({{ source: a, target: b, w }});
  }}
  ensureLink._seen = new Set();

  function expandFrom(id, limit = 20) {{
    // Returns how many *new* nodes were introduced.
    const adj = ADJ[id] || [];
    let i = expandedCount.get(id) || 0;

    let newAdded = 0;
    // Scan through adjacency until we find enough truly-new nodes (or exhaust list).
    for (; i < adj.length; i++) {{
      const [nbr, w] = adj[i];
      const existed = nodeById.has(nbr);

      if (!existed && nodes.length >= MAX_NODES) {{
        pruneOffscreen(PRUNE_TO);
        if (nodes.length >= MAX_NODES) break;
      }}

      ensureNode(nbr);
      ensureLink(id, nbr, w);

      if (!existed) {{
        newAdded += 1;
        if (newAdded >= limit) {{
          i += 1;
          break;
        }}
      }}
    }}

    expandedCount.set(id, Math.min(adj.length, i));
    return newAdded;
  }}

  function pruneOffscreen(targetCount = PRUNE_TO) {{
    if (nodes.length <= targetCount) return;

    const rect = stage.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;
    const margin = Math.max(600, Math.min(w, h) * 1.25);

    // Always keep focus.
    const keep = new Set();
    if (focusId !== null) keep.add(focusId);

    // Keep nodes near the viewport.
    const candidates = [];
    for (const n of nodes) {{
      if (focusId !== null && n.id === focusId) continue;
      const sx = n.x * transform.k + transform.x;
      const sy = n.y * transform.k + transform.y;
      const dx = (sx < -margin) ? (-margin - sx) : ((sx > w + margin) ? (sx - (w + margin)) : 0);
      const dy = (sy < -margin) ? (-margin - sy) : ((sy > h + margin) ? (sy - (h + margin)) : 0);
      const off2 = dx*dx + dy*dy;
      if (off2 === 0) {{
        keep.add(n.id);
      }} else {{
        candidates.push([off2, n.id]);
      }}
    }}

    // Prefer removing the farthest-offscreen nodes first.
    candidates.sort((a, b) => b[0] - a[0]);

    const remove = new Set();
    let current = nodes.length;
    for (const [_off2, id] of candidates) {{
      if (current <= targetCount) break;
      if (keep.has(id)) continue;
      remove.add(id);
      current -= 1;
    }}

    if (remove.size === 0) return;

    // Filter nodes.
    nodes = nodes.filter(n => !remove.has(n.id));
    for (const id of remove) {{
      nodeById.delete(id);
      expandedCount.delete(id);
    }}

    // Filter links.
    links = links.filter(l => !remove.has((typeof l.source === 'object') ? l.source.id : l.source)
                           && !remove.has((typeof l.target === 'object') ? l.target.id : l.target));

    // Rebuild seen-link set (simpler + reliable).
    ensureLink._seen.clear();
    for (const l of links) {{
      const a = (typeof l.source === 'object') ? l.source.id : l.source;
      const b = (typeof l.target === 'object') ? l.target.id : l.target;
      const key = a < b ? `${{a}}-${{b}}` : `${{b}}-${{a}}`;
      ensureLink._seen.add(key);
    }}

    sim.nodes(nodes);
    sim.force('link').links(links);
    applyLayout(true);
  }}

  function pruneByDirection(dirX, dirY, targetCount = PRUNE_TO) {{
    // Remove nodes farthest in the trailing direction (screen-space projection).
    if (nodes.length <= targetCount) return;
    const rect = stage.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;
    const k = Math.max(0.001, transform.k || 1);

    // Keep focus.
    const keep = new Set();
    if (focusId !== null) keep.add(focusId);

    const scored = [];
    for (const n of nodes) {{
      if (focusId !== null && n.id === focusId) continue;
      const sx = n.x * k + transform.x;
      const sy = n.y * k + transform.y;
      // Prefer pruning nodes that are also well outside the viewport.
      const off = (sx < -60 || sy < -60 || sx > (w + 60) || sy > (h + 60));
      const proj = sx * dirX + sy * dirY;
      scored.push([off ? 0 : 1, proj, n.id]);
    }}

    // Sort: offscreen first, then smallest projection (most trailing) first.
    scored.sort((a, b) => (a[0] - b[0]) || (a[1] - b[1]));

    const remove = new Set();
    let current = nodes.length;
    for (const [_offRank, _proj, id] of scored) {{
      if (current <= targetCount) break;
      if (keep.has(id)) continue;
      remove.add(id);
      current -= 1;
    }}

    if (remove.size === 0) return;

    nodes = nodes.filter(n => !remove.has(n.id));
    for (const id of remove) {{
      nodeById.delete(id);
      expandedCount.delete(id);
    }}

    links = links.filter(l => !remove.has((typeof l.source === 'object') ? l.source.id : l.source)
                           && !remove.has((typeof l.target === 'object') ? l.target.id : l.target));

    ensureLink._seen.clear();
    for (const l of links) {{
      const a = (typeof l.source === 'object') ? l.source.id : l.source;
      const b = (typeof l.target === 'object') ? l.target.id : l.target;
      const key = a < b ? `${{a}}-${{b}}` : `${{b}}-${{a}}`;
      ensureLink._seen.add(key);
    }}

    sim.nodes(nodes);
    sim.force('link').links(links);
    applyLayout(true);
  }}

  function pruneByDistanceToFocus(targetCount = PRUNE_TO) {{
    // Remove nodes farthest from the focused node in world space.
    // This keeps the map uncluttered once we hit the steady-state node budget.
    if (nodes.length <= targetCount) return;

    // Keep focus and its top-N neighbors.
    const keep = new Set();
    if (focusId !== null) keep.add(focusId);
    for (const id of focusTopSet) keep.add(id);

    let fx = 0;
    let fy = 0;
    if (focusId !== null) {{
      const fn = nodeById.get(focusId);
      if (fn && Number.isFinite(fn.x) && Number.isFinite(fn.y)) {{
        fx = fn.x;
        fy = fn.y;
      }}
    }}

    const scored = [];
    for (const n of nodes) {{
      if (keep.has(n.id)) continue;
      const dx = (n.x || 0) - fx;
      const dy = (n.y || 0) - fy;
      const d2 = dx*dx + dy*dy;
      scored.push([d2, n.id]);
    }}

    // Remove farthest first.
    scored.sort((a, b) => b[0] - a[0]);

    const remove = new Set();
    let current = nodes.length;
    for (const [_d2, id] of scored) {{
      if (current <= targetCount) break;
      remove.add(id);
      current -= 1;
    }}

    if (remove.size === 0) return;

    nodes = nodes.filter(n => !remove.has(n.id));
    for (const id of remove) {{
      nodeById.delete(id);
      expandedCount.delete(id);
    }}

    links = links.filter(l => !remove.has((typeof l.source === 'object') ? l.source.id : l.source)
                           && !remove.has((typeof l.target === 'object') ? l.target.id : l.target));

    ensureLink._seen.clear();
    for (const l of links) {{
      const a = (typeof l.source === 'object') ? l.source.id : l.source;
      const b = (typeof l.target === 'object') ? l.target.id : l.target;
      const key = a < b ? `${{a}}-${{b}}` : `${{b}}-${{a}}`;
      ensureLink._seen.add(key);
    }}

    sim.nodes(nodes);
    sim.force('link').links(links);
    applyLayout(true);
  }}

  function ensureSomethingVisible() {{
    if (!nodes || nodes.length === 0) return;
    const rect = stage.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;
    const k = Math.max(0.001, transform.k || 1);
    const m = AUTO_SNAP_VISIBLE_MARGIN_PX;

    let anyVisible = false;
    for (const n of nodes) {{
      const sx = n.x * k + transform.x;
      const sy = n.y * k + transform.y;
      if (sx >= -m && sx <= (w + m) && sy >= -m && sy <= (h + m)) {{
        anyVisible = true;
        break;
      }}
    }}
    if (anyVisible) return;

    // Anchor choice: focused node if available, else nearest node to the viewport center.
    let anchor = null;
    if (focusId !== null) {{
      const fn = nodeById.get(focusId);
      if (fn) anchor = fn;
    }}
    if (!anchor) {{
      const cx = w / 2;
      const cy = h / 2;
      let best = null;
      let bestD2 = Infinity;
      for (const n of nodes) {{
        const sx = n.x * k + transform.x;
        const sy = n.y * k + transform.y;
        const dx = sx - cx;
        const dy = sy - cy;
        const d2 = dx*dx + dy*dy;
        if (d2 < bestD2) {{ best = n; bestD2 = d2; }}
      }}
      anchor = best;
    }}
    if (!anchor) return;

    const cx = w / 2;
    const cy = h / 2;

    // Programmatic zoom transform triggers zoom events; suppress end-handler logic.
    _suppressZoomEnd = true;
    try {{
      transform = d3.zoomIdentity.translate(cx - anchor.x * k, cy - anchor.y * k).scale(k);
      d3.select(canvas).call(zoom.transform, transform);
    }} finally {{
      _suppressZoomEnd = false;
    }}
  }}

  function maybeAutoAdapt(dirX, dirY) {{
    // Expand in the direction of blank space after a pan/drag.
    const mag = Math.hypot(dirX, dirY);
    if (mag < AUTO_ADAPT_MIN_DRAG_PX) return;
    dirX /= mag;
    dirY /= mag;

    const rect = stage.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;
    const k = Math.max(0.001, transform.k || 1);

    // How much blank space exists in the direction of travel?
    const corners = [
      [0, 0], [w, 0], [0, h], [w, h],
    ];
    let boundaryProj = -Infinity;
    for (const [cx, cy] of corners) {{
      boundaryProj = Math.max(boundaryProj, cx * dirX + cy * dirY);
    }}

    // Only consider nodes near the viewport when deciding “blankness”; otherwise
    // a far-offscreen outlier can suppress expansion even if the viewport is empty.
    const viewMargin = 120;
    let maxNodeProj = -Infinity;
    for (const n of nodes) {{
      const sx = n.x * k + transform.x;
      const sy = n.y * k + transform.y;
      if (sx < -viewMargin || sy < -viewMargin || sx > (w + viewMargin) || sy > (h + viewMargin)) continue;
      maxNodeProj = Math.max(maxNodeProj, sx * dirX + sy * dirY);
    }}
    if (!Number.isFinite(maxNodeProj)) {{
      // Fallback: if nothing is near the viewport, use overall.
      for (const n of nodes) {{
        const sx = n.x * k + transform.x;
        const sy = n.y * k + transform.y;
        maxNodeProj = Math.max(maxNodeProj, sx * dirX + sy * dirY);
      }}
    }}

    if ((boundaryProj - maxNodeProj) < AUTO_BLANK_THRESHOLD_PX) return;

    // Make room if needed (prune trailing side first).
    if (nodes.length >= MAX_NODES - (AUTO_EXPAND_MAX_SOURCES * AUTO_EXPAND_LIMIT_PER_NODE)) {{
      pruneByDirection(-dirX, -dirY, PRUNE_TO);
      pruneOffscreen(PRUNE_TO);
    }}

    // Pick a few frontier nodes (those most advanced in the direction of travel).
    const candidates = [];
    for (const n of nodes) {{
      const adj = ADJ[n.id] || [];
      const start = expandedCount.get(n.id) || 0;
      if (start >= adj.length) continue;
      const sx = n.x * k + transform.x;
      const sy = n.y * k + transform.y;
      if (sx < -viewMargin || sy < -viewMargin || sx > (w + viewMargin) || sy > (h + viewMargin)) continue;
      const proj = sx * dirX + sy * dirY;
      candidates.push([proj, n.id]);
    }}
    candidates.sort((a, b) => b[0] - a[0]);

    // Expand enough to make a perceptible dent in the blank area.
    const blankPx = Math.max(0, boundaryProj - maxNodeProj);
    const targetNew = Math.max(20, Math.min(110, Math.round(blankPx / 170) * 20));

    const beforeCount = nodes.length;
    let sourcesUsed = 0;
    let newTotal = 0;
    for (const [_proj, id] of candidates) {{
      if (nodes.length >= MAX_NODES) break;
      newTotal += expandFrom(id, AUTO_EXPAND_LIMIT_PER_NODE);
      sourcesUsed += 1;
      if (sourcesUsed >= AUTO_EXPAND_MAX_SOURCES && newTotal >= targetNew) break;
      if (sourcesUsed >= Math.max(AUTO_EXPAND_MAX_SOURCES, Math.ceil(targetNew / AUTO_EXPAND_LIMIT_PER_NODE))) {{
        if (newTotal >= targetNew) break;
      }}
    }}

    // Keep the steady-state node budget stable: add in the direction of blank space,
    // and drop nodes on the trailing extremity.
    if (nodes.length > PRUNE_TO) {{
      const added = nodes.length - beforeCount;
      if (added > 0) {{
        pruneByDirection(-dirX, -dirY, PRUNE_TO);
      }} else {{
        // If nothing could be expanded, avoid directional pruning.
        pruneOffscreen(PRUNE_TO);
      }}
    }}

    if (nodes.length !== beforeCount) {{
      sim.nodes(nodes);
      sim.force('link').links(links);
      sim.alpha(0.55).restart();
    }}
  }}

  function seed(id) {{
    nodes = [];
    links = [];
    nodeById.clear();
    expandedCount.clear();
    ensureLink._seen.clear();

    focusId = id;
    recomputeFocusTop();

    ensureNode(id);
    expandFrom(id, 20);

    sim.nodes(nodes);
    sim.force('link').links(links);
    applyLayout(true);
    sim.alpha(1).restart();
    recenterAndPinFocus(id);
  }}

  function centerOn(id) {{
    const n = nodeById.get(id);
    if (!n) return;
    const rect = stage.getBoundingClientRect();
    const cx = rect.width / 2;
    const cy = rect.height / 2;
    const k = Math.max(0.001, transform.k || 1);
    // screen = world*k + t; so center => t = (c - world*k)
    transform = d3.zoomIdentity.translate(cx - n.x * k, cy - n.y * k).scale(k);
    d3.select(canvas).call(zoom.transform, transform);
  }}

  function recenterAndPinFocus(id) {{
    // Release any previous focus pin.
    if (focusId !== null) {{
      const prev = nodeById.get(focusId);
      if (prev) {{ prev.fx = null; prev.fy = null; }}
    }}

    focusId = id;
    recomputeFocusTop();

    const fn = nodeById.get(id);
    if (!fn) {{
      centerOn(id);
      return;
    }}

    // Shift the entire layout so the focused node becomes (0,0) in world space.
    const dx = fn.x || 0;
    const dy = fn.y || 0;
    for (const n of nodes) {{
      if (Number.isFinite(n.x)) n.x -= dx;
      if (Number.isFinite(n.y)) n.y -= dy;
      if (n.fx !== null && n.fx !== undefined) n.fx -= dx;
      if (n.fy !== null && n.fy !== undefined) n.fy -= dy;
    }}

    fn.x = 0;
    fn.y = 0;
    fn.fx = 0;
    fn.fy = 0;

    // Put world origin at the visual center.
    const rect = stage.getBoundingClientRect();
    const cx = rect.width / 2;
    const cy = rect.height / 2;
    const k = Math.max(0.001, transform.k || 1);
    transform = d3.zoomIdentity.translate(cx, cy).scale(k);
    d3.select(canvas).call(zoom.transform, transform);
  }}

  function redraw() {{
    const rect = stage.getBoundingClientRect();
    const w = rect.width;
    const h = rect.height;

    ctx.clearRect(0, 0, w, h);

    ctx.save();
    ctx.translate(transform.x, transform.y);
    ctx.scale(transform.k, transform.k);

    // Links intentionally not rendered (too busy). Layout still uses link forces.

    // Nodes (non-focused)
    ctx.globalAlpha = 0.95;
    for (const n of nodes) {{
      if (focusId !== null && n.id === focusId) continue;
      ctx.beginPath();
      const isTop = (focusId !== null) && focusTopSet.has(n.id);
      // Muted base color so the focused node is obvious.
      ctx.fillStyle = isTop ? 'rgba(255, 236, 165, 0.82)' : 'rgba(31, 119, 180, 0.40)';
      ctx.arc(n.x, n.y, visualRadius(n), 0, Math.PI * 2);
      ctx.fill();
    }}

    // Focused node (draw last so it sits on top)
    if (focusId !== null) {{
      const fn = nodeById.get(focusId);
      if (fn) {{
        ctx.beginPath();
        // Focused node: softer golden-yellow (less glaring than hot pink).
        ctx.fillStyle = 'rgba(255, 204, 0, 0.92)';
        ctx.arc(fn.x, fn.y, fn.r + 4.0, 0, Math.PI * 2);
        ctx.fill();

        ctx.lineWidth = 3.8 / Math.max(1, transform.k);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.98)';
        ctx.stroke();

        // Outer halo ring for extra contrast.
        ctx.beginPath();
        ctx.arc(fn.x, fn.y, fn.r + 7.5, 0, Math.PI * 2);
        ctx.lineWidth = 2.0 / Math.max(1, transform.k);
        ctx.strokeStyle = 'rgba(255, 204, 0, 0.55)';
        ctx.stroke();
      }}
    }}

    // Labels (names)
    // Guarantee: labels never overlap (we cull labels that would overlap).
    const k = Math.max(0.001, transform.k || 1);
    const fs = BASE_LABEL_PX / k;  // keep roughly constant in screen pixels
    ctx.font = `${{fs}}px system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, sans-serif`;
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    ctx.globalAlpha = 0.92;
    ctx.lineWidth = 3 / k;
    ctx.strokeStyle = 'rgba(0, 0, 0, 0.60)';
    ctx.fillStyle = 'rgba(245, 245, 245, 0.92)';

    const cell = 80;
    const grid = new Map();
    lastLabelHitboxes = [];
    function key(cx, cy) {{ return `${{cx}},${{cy}}`; }}
    function addBox(b) {{
      const x0 = Math.floor(b.x0 / cell);
      const x1 = Math.floor(b.x1 / cell);
      const y0 = Math.floor(b.y0 / cell);
      const y1 = Math.floor(b.y1 / cell);
      for (let gx = x0; gx <= x1; gx++) {{
        for (let gy = y0; gy <= y1; gy++) {{
          const kk = key(gx, gy);
          if (!grid.has(kk)) grid.set(kk, []);
          grid.get(kk).push(b);
        }}
      }}
    }}
    function overlaps(b) {{
      const x0 = Math.floor(b.x0 / cell);
      const x1 = Math.floor(b.x1 / cell);
      const y0 = Math.floor(b.y0 / cell);
      const y1 = Math.floor(b.y1 / cell);
      for (let gx = x0; gx <= x1; gx++) {{
        for (let gy = y0; gy <= y1; gy++) {{
          const kk = key(gx, gy);
          const arr = grid.get(kk);
          if (!arr) continue;
          for (const o of arr) {{
            if (!(b.x1 < o.x0 || b.x0 > o.x1 || b.y1 < o.y0 || b.y0 > o.y1)) return true;
          }}
        }}
      }}
      return false;
    }}

    function drawLabel(n) {{
      const sx = n.x * k + transform.x;
      const sy = n.y * k + transform.y;
      const wpx = getLabelWidthPx(n);
      const hpx = BASE_LABEL_PX;
      const x0 = sx + (n.r * k) + 4;
      const y0 = sy - (hpx / 2) - 2;
      const b = {{ x0, y0, x1: x0 + wpx + 4, y1: y0 + hpx + 4 }};

      // Only consider labels near/on screen.
      if (b.x1 < -40 || b.y1 < -40 || b.x0 > (w + 40) || b.y0 > (h + 40)) return;
      if (overlaps(b)) return;
      addBox(b);

      lastLabelHitboxes.push({{ id: n.id, x0: b.x0, y0: b.y0, x1: b.x1, y1: b.y1 }});

      const xw = n.x + n.r + (3.5 / k);
      const yw = n.y;
      ctx.strokeText(n.name, xw, yw);
      ctx.fillText(n.name, xw, yw);
    }}

    // Focus label first so it always wins.
    if (focusId !== null) {{
      const fn = nodeById.get(focusId);
      if (fn) drawLabel(fn);
    }}

    for (const n of nodes) {{
      if (focusId !== null && n.id === focusId) continue;
      drawLabel(n);
    }}

    ctx.restore();
  }}

  sim.on('tick', redraw);

  // Hit-testing for hover/click.
  function worldFromClient(clientX, clientY) {{
    const rect = stage.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    const wx = (x - transform.x) / transform.k;
    const wy = (y - transform.y) / transform.k;
    return [wx, wy, x, y];
  }}

  function nearestNodeAtScreen(sx, sy) {{
    // Screen-space hit testing keeps selection reliable at any zoom.
    let best = null;
    let bestD2 = Infinity;
    const k = Math.max(0.001, transform.k || 1);
    for (const n of nodes) {{
      const nx = n.x * k + transform.x;
      const ny = n.y * k + transform.y;
      const dx = sx - nx;
      const dy = sy - ny;
      const rPx = Math.max(10, (visualRadius(n) * k) + 6);
      const d2 = dx*dx + dy*dy;
      if (d2 <= rPx*rPx && d2 < bestD2) {{ best = n; bestD2 = d2; }}
    }}
    return best;
  }}

  canvas.addEventListener('mousemove', (e) => {{
    const [_wx, _wy, sx, sy] = worldFromClient(e.clientX, e.clientY);
    const n = nearestNodeAtScreen(sx, sy);
    if (!n) {{ tip.style.opacity = 0; return; }}
    tip.style.opacity = 1;
    tip.style.left = `${{sx}}px`;
    tip.style.top = `${{sy}}px`;
    tip.textContent = `${{n.name}}  (${{degree(n.id)}} links)`;
  }});

  canvas.addEventListener('mouseleave', () => {{ tip.style.opacity = 0; }});

  canvas.addEventListener('dblclick', (e) => {{
    e.preventDefault();
    const [_wx, _wy, sx, sy] = worldFromClient(e.clientX, e.clientY);
    let n = nearestNodeAtScreen(sx, sy);
    if (!n) {{
      // Fallback: allow double-clicking on the label text region.
      for (const b of lastLabelHitboxes) {{
        if (sx >= b.x0 && sx <= b.x1 && sy >= b.y0 && sy <= b.y1) {{
          n = nodeById.get(b.id);
          break;
        }}
      }}
    }}
    if (!n) return;

    // Keep expansions bounded: only expand from the selected node.
    // With a small live-node budget, prefer smaller bursts.
    expandFrom(n.id, 12);
    recenterAndPinFocus(n.id);

    // Keep a steady node budget once we reach it.
    if (nodes.length > PRUNE_TO) pruneByDistanceToFocus(PRUNE_TO);

    sim.nodes(nodes);
    sim.force('link').links(links);
    applyLayout(true);
    sim.alpha(0.85).restart();
    redraw();
  }});

  // Search + seed.
  function findIdBySubstring(q) {{
    const s = (q || '').trim().toLowerCase();
    if (!s) return null;
    // Prefer exact (case-insensitive) match first.
    for (let i = 0; i < NAMES.length; i++) {{
      if (NAMES[i] && NAMES[i].toLowerCase() === s) return i;
    }}
    // Otherwise first substring match.
    for (let i = 0; i < NAMES.length; i++) {{
      if (NAMES[i] && NAMES[i].toLowerCase().includes(s)) return i;
    }}
    return null;
  }}

  function seedDefault() {{
    // Pick a high-degree node as a visually rich starting point.
    let best = 0;
    let bestDeg = -1;
    for (let i = 0; i < NAMES.length; i++) {{
      const d = degree(i);
      if (d > bestDeg) {{ best = i; bestDeg = d; }}
    }}
    seed(best);
  }}

  document.getElementById('reset').addEventListener('click', () => {{
    d3.select(canvas).call(zoom.transform, d3.zoomIdentity);
    seedDefault();
  }});

  document.getElementById('random').addEventListener('click', () => {{
    // Choose from top-degree slice so it isn't a dead end.
    const degs = [];
    for (let i = 0; i < NAMES.length; i++) degs.push([degree(i), i]);
    degs.sort((a,b) => b[0]-a[0]);
    const slice = degs.slice(0, Math.min(2000, degs.length));
    const pick = slice[Math.floor(Math.random() * slice.length)][1];
    seed(pick);
  }});

  input.addEventListener('keydown', (e) => {{
    if (e.key !== 'Enter') return;
    const id = findIdBySubstring(input.value);
    if (id === null) return;
    seed(id);
  }});

  // Init
  resize();
  seedDefault();
  </script>
</body>
</html>"""

    out_path.write_text(html, encoding="utf-8")
    return out_path
