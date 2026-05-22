#!/usr/bin/env python3
"""
Local web UI for inspecting authoring simulation SQLite databases.

Usage:
    python3 scripts/authoring_sim_ui.py --db /path/to/authoring-sim.sqlite
    AUTHORING_SIM_DB=/var/lib/subtensor/chains/bittensor/authoring-sim.sqlite uvicorn scripts.authoring_sim_ui:asgi_app --host 0.0.0.0 --port 8787
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


DEFAULT_LIMIT = 200
MAX_LIMIT = 1000


def clamp_limit(value: str | None, default: int = DEFAULT_LIMIT) -> int:
    if value is None:
        return default
    try:
        return max(1, min(MAX_LIMIT, int(value)))
    except ValueError:
        return default


def to_jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    return value


class Db:
    def __init__(self, path: Path):
        self.path = path.resolve()

    def connect(self) -> sqlite3.Connection:
        uri_path = urllib.parse.quote(str(self.path), safe="/:")
        conn = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        return conn

    def rows(
        self, sql: str, params: tuple[Any, ...] = (), limit: int | None = None
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            if limit is not None:
                params = (*params, limit)
            return [
                {key: to_jsonable(row[key]) for key in row.keys()}
                for row in conn.execute(sql, params).fetchall()
            ]

    def one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            if row is None:
                return {}
            return {key: to_jsonable(row[key]) for key in row.keys()}

    def scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        with self.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return None if row is None else row[0]


class App:
    def __init__(self, db: Db):
        self.db = db

    def summary(self) -> dict[str, Any]:
        counts = {
            name: self.db.scalar(f"SELECT COUNT(*) FROM {name}") or 0
            for name in (
                "sim_blocks",
                "extrinsics",
                "extrinsic_events",
                "pool_views",
                "pool_view_members",
                "chain_events",
                "sim_timeline_events",
                "writer_stats",
            )
        }

        latest_time = self.db.scalar(
            """
            SELECT MAX(event_time_ms) FROM (
                SELECT event_time_ms FROM sim_blocks
                UNION ALL SELECT event_time_ms FROM extrinsic_events
                UNION ALL SELECT event_time_ms FROM pool_views
                UNION ALL SELECT event_time_ms FROM chain_events
                UNION ALL SELECT event_time_ms FROM sim_timeline_events
                UNION ALL SELECT event_time_ms FROM writer_stats
            )
            """
        )

        return {
            "db_path": str(self.db.path),
            "counts": counts,
            "latest_event_time_ms": latest_time,
            "latest_writer": self.db.one(
                "SELECT * FROM writer_stats ORDER BY event_time_ms DESC, seq DESC LIMIT 1"
            ),
            "recent_event_kinds": self.db.rows(
                """
                SELECT event_kind, COUNT(*) AS count, MAX(event_time_ms) AS last_time_ms
                FROM (
                    SELECT event_time_ms, event_kind FROM extrinsic_events
                    UNION ALL SELECT event_time_ms, event_kind FROM chain_events
                    UNION ALL SELECT event_time_ms, event_kind FROM sim_timeline_events
                )
                GROUP BY event_kind
                ORDER BY last_time_ms DESC
                LIMIT 20
                """
            ),
            "top_lifetimes": self.db.rows(
                """
                SELECT
                    ev.tx_hash,
                    COALESCE(x.classification, 'unknown') AS classification,
                    COALESCE(x.last_status, 'unknown') AS last_status,
                    COUNT(ev.seq) AS event_count,
                    MIN(ev.event_time_ms) AS first_time_ms,
                    MAX(ev.event_time_ms) AS last_time_ms,
                    MAX(ev.event_time_ms) - MIN(ev.event_time_ms) AS lifetime_ms
                FROM extrinsic_events ev
                LEFT JOIN extrinsics x ON x.tx_hash = ev.tx_hash
                GROUP BY ev.tx_hash
                ORDER BY lifetime_ms DESC, last_time_ms DESC
                LIMIT 20
                """
            ),
        }

    def timeline(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        kind = first(query, "kind")
        where = []
        params: list[Any] = []
        if kind:
            where.append("event_kind = ?")
            params.append(kind)
        clause = f"WHERE {' AND '.join(where)}" if where else ""

        rows = self.db.rows(
            f"""
            SELECT * FROM (
                SELECT event_time_ms, 'chain' AS source, event_kind, NULL AS tx_hash,
                    block_number, block_hash AS parent_hash, NULL AS view_id, details_json
                FROM chain_events
                UNION ALL
                SELECT event_time_ms, 'tx' AS source, event_kind, tx_hash,
                    block_number, parent_hash, view_id, details_json
                FROM extrinsic_events
                UNION ALL
                SELECT event_time_ms, 'sim' AS source, event_kind, NULL AS tx_hash,
                    NULL AS block_number, parent_hash, NULL AS view_id, details_json
                FROM sim_timeline_events
            )
            {clause}
            ORDER BY event_time_ms DESC
            LIMIT ?
            """,
            tuple(params),
            limit,
        )
        return {"rows": rows, "limit": limit}

    def extrinsics(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        search = first(query, "q")
        status = first(query, "status")
        classification = first(query, "classification")
        where = []
        params: list[Any] = []
        if search:
            where.append("(x.tx_hash LIKE ? OR x.details_json LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like])
        if status:
            where.append("x.last_status = ?")
            params.append(status)
        if classification:
            where.append("x.classification = ?")
            params.append(classification)
        clause = f"WHERE {' AND '.join(where)}" if where else ""

        rows = self.db.rows(
            f"""
            SELECT
                x.tx_hash,
                x.first_seen_time_ms,
                x.first_seen_source,
                x.encoded_len,
                x.classification,
                x.details_json,
                x.last_status,
                x.updated_time_ms,
                COUNT(ev.seq) AS event_count,
                MIN(ev.event_time_ms) AS first_event_time_ms,
                MAX(ev.event_time_ms) AS last_event_time_ms,
                COALESCE(MAX(ev.event_time_ms) - MIN(ev.event_time_ms), 0) AS lifetime_ms
            FROM extrinsics x
            LEFT JOIN extrinsic_events ev ON ev.tx_hash = x.tx_hash
            {clause}
            GROUP BY x.tx_hash
            ORDER BY x.updated_time_ms DESC
            LIMIT ?
            """,
            tuple(params),
            limit,
        )
        return {"rows": rows, "limit": limit}

    def extrinsic(self, query: dict[str, list[str]]) -> dict[str, Any]:
        tx_hash = first(query, "hash")
        if not tx_hash:
            raise ValueError("missing hash")
        row = self.db.one("SELECT * FROM extrinsics WHERE tx_hash = ?", (tx_hash,))
        events = self.db.rows(
            """
            SELECT * FROM extrinsic_events
            WHERE tx_hash = ?
            ORDER BY event_time_ms ASC, seq ASC
            LIMIT ?
            """,
            (tx_hash,),
            MAX_LIMIT,
        )
        views = self.db.rows(
            """
            SELECT m.*, v.event_time_ms, v.slot, v.parent_number, v.reason
            FROM pool_view_members m
            JOIN pool_views v ON v.view_id = m.view_id
            WHERE m.tx_hash = ?
            ORDER BY v.event_time_ms ASC, m.section ASC, m.ordinal ASC
            LIMIT ?
            """,
            (tx_hash,),
            MAX_LIMIT,
        )
        return {"row": row, "events": events, "views": views}

    def pool_views(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        rows = self.db.rows(
            """
            SELECT *
            FROM pool_views
            ORDER BY event_time_ms DESC
            LIMIT ?
            """,
            (),
            limit,
        )
        return {"rows": rows, "limit": limit}

    def pool_view(self, query: dict[str, list[str]]) -> dict[str, Any]:
        view_id = first(query, "view_id")
        if not view_id:
            raise ValueError("missing view_id")
        view = self.db.one("SELECT * FROM pool_views WHERE view_id = ?", (view_id,))
        members = self.db.rows(
            """
            SELECT m.*, x.classification, x.last_status, x.details_json
            FROM pool_view_members m
            LEFT JOIN extrinsics x ON x.tx_hash = m.tx_hash
            WHERE m.view_id = ?
            ORDER BY m.section ASC, m.ordinal ASC
            LIMIT ?
            """,
            (view_id,),
            MAX_LIMIT,
        )
        return {"view": view, "members": members}

    def blocks(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        rows = self.db.rows(
            """
            SELECT *
            FROM sim_blocks
            ORDER BY event_time_ms DESC, id DESC
            LIMIT ?
            """,
            (),
            limit,
        )
        return {"rows": rows, "limit": limit}


def dispatch(app: App, path: str, query: dict[str, list[str]]) -> tuple[HTTPStatus, str, bytes]:
    try:
        if path == "/":
            return html_response(INDEX_HTML)
        if path == "/api/summary":
            return json_response(app.summary())
        if path == "/api/timeline":
            return json_response(app.timeline(query))
        if path == "/api/extrinsics":
            return json_response(app.extrinsics(query))
        if path == "/api/extrinsic":
            return json_response(app.extrinsic(query))
        if path == "/api/pool-views":
            return json_response(app.pool_views(query))
        if path == "/api/pool-view":
            return json_response(app.pool_view(query))
        if path == "/api/blocks":
            return json_response(app.blocks(query))
        return json_response({"error": "not found"}, HTTPStatus.NOT_FOUND)
    except sqlite3.OperationalError as error:
        return json_response({"error": str(error)}, HTTPStatus.SERVICE_UNAVAILABLE)
    except ValueError as error:
        return json_response({"error": str(error)}, HTTPStatus.BAD_REQUEST)
    except Exception as error:  # Keep the UI useful while debugging schema changes.
        return json_response({"error": str(error)}, HTTPStatus.INTERNAL_SERVER_ERROR)


def html_response(html: str, status: HTTPStatus = HTTPStatus.OK) -> tuple[HTTPStatus, str, bytes]:
    return status, "text/html; charset=utf-8", html.encode("utf-8")


def json_response(
    payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK
) -> tuple[HTTPStatus, str, bytes]:
    return (
        status,
        "application/json",
        json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )


def first(query: dict[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    return None if not values else values[0]


class Handler(BaseHTTPRequestHandler):
    app: App

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        status, content_type, body = dispatch(self.app, parsed.path, query)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}")


class AsgiApp:
    def __init__(self, app: App):
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await self.handle_lifespan(receive, send)
            return

        if scope["type"] != "http":
            await self.send_response(
                send, *json_response({"error": "unsupported scope"}, HTTPStatus.BAD_REQUEST)
            )
            return

        if scope.get("method") != "GET":
            await self.send_response(
                send, *json_response({"error": "method not allowed"}, HTTPStatus.METHOD_NOT_ALLOWED)
            )
            return

        query_string = scope.get("query_string", b"").decode("utf-8")
        query = urllib.parse.parse_qs(query_string)
        status, content_type, body = dispatch(self.app, scope.get("path", "/"), query)
        await self.send_response(send, status, content_type, body)

    async def handle_lifespan(self, receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def send_response(
        self, send: Any, status: HTTPStatus, content_type: str, body: bytes
    ) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": int(status),
                "headers": [
                    (b"content-type", content_type.encode("utf-8")),
                    (b"cache-control", b"no-store"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def create_asgi_app() -> AsgiApp:
    db_path = Path(os.environ.get("AUTHORING_SIM_DB", "authoring-sim.sqlite"))
    return AsgiApp(App(Db(db_path)))


asgi_app = create_asgi_app()


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Authoring Sim DB Viewer</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1020;
      --panel: #121a2f;
      --panel-2: #17223d;
      --text: #e8eefc;
      --muted: #9eabc6;
      --accent: #7cc4ff;
      --good: #70e0a3;
      --warn: #ffd166;
      --bad: #ff7b8a;
      --border: #243252;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 20px;
      border-bottom: 1px solid var(--border);
      background: rgba(11, 16, 32, 0.95);
      backdrop-filter: blur(8px);
    }
    h1 { margin: 0; font-size: 18px; }
    button, input, select {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel-2);
      color: var(--text);
      padding: 8px 10px;
    }
    button { cursor: pointer; }
    button.active { border-color: var(--accent); color: var(--accent); }
    main { padding: 20px; }
    .tabs, .controls { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 12px; }
    .card {
      border: 1px solid var(--border);
      border-radius: 12px;
      background: var(--panel);
      padding: 14px;
      min-width: 0;
    }
    .metric { font-size: 24px; font-weight: 700; margin-top: 4px; }
    .muted { color: var(--muted); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .section { margin-top: 18px; }
    .hidden { display: none; }
    table {
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--panel);
    }
    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    th { color: var(--muted); font-weight: 600; background: var(--panel-2); }
    tr:hover td { background: rgba(124, 196, 255, 0.06); }
    td.wrap { white-space: normal; max-width: 520px; word-break: break-word; }
    .pill {
      display: inline-block;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 2px 8px;
      background: var(--panel-2);
      color: var(--muted);
    }
    .pill.good { color: var(--good); }
    .pill.warn { color: var(--warn); }
    .pill.bad { color: var(--bad); }
    .details {
      white-space: pre-wrap;
      word-break: break-word;
      max-height: 360px;
      overflow: auto;
      padding: 12px;
      background: #080c18;
      border: 1px solid var(--border);
      border-radius: 10px;
    }
    @media (max-width: 1000px) {
      .grid { grid-template-columns: repeat(2, minmax(140px, 1fr)); }
      header { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Authoring Sim DB Viewer</h1>
      <div id="db-path" class="muted mono"></div>
    </div>
    <div class="tabs">
      <button data-tab="dashboard" class="active">Dashboard</button>
      <button data-tab="timeline">Timeline</button>
      <button data-tab="extrinsics">Extrinsics</button>
      <button data-tab="pool">Pool Views</button>
      <button data-tab="blocks">Blocks</button>
      <button id="refresh">Refresh</button>
      <label class="muted"><input id="auto-refresh" type="checkbox" checked> live</label>
    </div>
  </header>

  <main>
    <div id="error" class="card hidden"></div>

    <section id="tab-dashboard">
      <div id="metrics" class="grid"></div>
      <div class="section grid" style="grid-template-columns: 1fr 1fr;">
        <div class="card">
          <h3>Recent Event Kinds</h3>
          <div id="event-kinds"></div>
        </div>
        <div class="card">
          <h3>Longest Observed Lifetimes</h3>
          <div id="top-lifetimes"></div>
        </div>
      </div>
      <div class="section card">
        <h3>Writer Status</h3>
        <pre id="writer" class="details"></pre>
      </div>
    </section>

    <section id="tab-timeline" class="hidden">
      <div class="controls section">
        <input id="timeline-kind" placeholder="filter event_kind">
        <select id="timeline-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option></select>
        <button id="load-timeline">Load</button>
      </div>
      <div id="timeline-table" class="section"></div>
    </section>

    <section id="tab-extrinsics" class="hidden">
      <div class="controls section">
        <input id="extrinsic-q" placeholder="hash / address / details">
        <input id="extrinsic-status" placeholder="last_status">
        <input id="extrinsic-class" placeholder="classification">
        <select id="extrinsic-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option></select>
        <button id="load-extrinsics">Search</button>
      </div>
      <div id="extrinsics-table" class="section"></div>
      <div class="section card">
        <h3>Selected Extrinsic</h3>
        <div id="extrinsic-detail" class="muted">Click a transaction hash to inspect its lifetime.</div>
      </div>
    </section>

    <section id="tab-pool" class="hidden">
      <div class="controls section">
        <select id="pool-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option></select>
        <button id="load-pool">Load</button>
      </div>
      <div id="pool-table" class="section"></div>
      <div class="section card">
        <h3>Selected Pool View</h3>
        <div id="pool-detail" class="muted">Click a view id to inspect ready/future ordering.</div>
      </div>
    </section>

    <section id="tab-blocks" class="hidden">
      <div class="controls section">
        <select id="blocks-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option></select>
        <button id="load-blocks">Load</button>
      </div>
      <div id="blocks-table" class="section"></div>
    </section>
  </main>

<script>
const state = { tab: "dashboard" };
const $ = (id) => document.getElementById(id);

function fmtTime(ms) {
  if (ms === null || ms === undefined) return "";
  const d = new Date(Number(ms));
  if (Number.isNaN(d.getTime())) return String(ms);
  const pad = (value, size = 2) => String(value).padStart(size, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.` +
    `${pad(d.getMilliseconds(), 3)}`;
}

function short(value, n = 14) {
  if (!value) return "";
  const s = String(value);
  return s.length <= n * 2 ? s : `${s.slice(0, n)}...${s.slice(-n)}`;
}

function parseJsonMaybe(value) {
  if (!value) return value;
  try { return JSON.parse(value); } catch (_) { return value; }
}

function pretty(value) {
  return JSON.stringify(parseJsonMaybe(value), null, 2);
}

function setError(error) {
  const el = $("error");
  if (!error) {
    el.classList.add("hidden");
    el.textContent = "";
    return;
  }
  el.classList.remove("hidden");
  el.textContent = error;
}

async function api(path) {
  const res = await fetch(path);
  const data = await res.json();
  if (!res.ok || data.error) throw new Error(data.error || res.statusText);
  return data;
}

function table(headers, rows, render) {
  if (!rows.length) return `<div class="card muted">No rows.</div>`;
  return `<table><thead><tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr></thead><tbody>${
    rows.map(row => `<tr>${render(row)}</tr>`).join("")
  }</tbody></table>`;
}

function pill(value) {
  const s = String(value || "");
  const cls = /error|drop|invalid|fail/i.test(s) ? "bad" : /ready|success|import/i.test(s) ? "good" : "";
  return `<span class="pill ${cls}">${escapeHtml(s)}</span>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  })[c]);
}

async function loadDashboard() {
  const data = await api("/api/summary");
  $("db-path").textContent = data.db_path;
  const c = data.counts;
  $("metrics").innerHTML = Object.entries(c).map(([name, count]) => `
    <div class="card"><div class="muted">${name}</div><div class="metric">${count}</div></div>
  `).join("") + `
    <div class="card"><div class="muted">latest event</div><div class="metric" style="font-size:16px">${fmtTime(data.latest_event_time_ms)}</div></div>
  `;
  $("event-kinds").innerHTML = table(["kind", "count", "last"], data.recent_event_kinds, r =>
    `<td>${pill(r.event_kind)}</td><td>${r.count}</td><td>${fmtTime(r.last_time_ms)}</td>`);
  $("top-lifetimes").innerHTML = table(["hash", "status", "events", "lifetime"], data.top_lifetimes, r =>
    `<td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${pill(r.last_status)}</td><td>${r.event_count}</td><td>${r.lifetime_ms} ms</td>`);
  $("writer").textContent = pretty(data.latest_writer || {});
}

async function loadTimeline() {
  const qs = new URLSearchParams();
  qs.set("limit", $("timeline-limit").value);
  if ($("timeline-kind").value) qs.set("kind", $("timeline-kind").value);
  const data = await api(`/api/timeline?${qs}`);
  $("timeline-table").innerHTML = table(["time", "source", "kind", "tx", "block", "view", "details"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td>${r.source}</td><td>${pill(r.event_kind)}</td><td class="mono">${r.tx_hash ? `<button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button>` : ""}</td><td>${r.block_number ?? ""}</td><td class="mono">${r.view_id ? `<button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 10)}</button>` : ""}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
}

async function loadExtrinsics() {
  const qs = new URLSearchParams();
  qs.set("limit", $("extrinsic-limit").value);
  if ($("extrinsic-q").value) qs.set("q", $("extrinsic-q").value);
  if ($("extrinsic-status").value) qs.set("status", $("extrinsic-status").value);
  if ($("extrinsic-class").value) qs.set("classification", $("extrinsic-class").value);
  const data = await api(`/api/extrinsics?${qs}`);
  $("extrinsics-table").innerHTML = table(["hash", "class", "status", "events", "lifetime", "updated", "details"], data.rows, r =>
    `<td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${r.classification}</td><td>${pill(r.last_status)}</td><td>${r.event_count}</td><td>${r.lifetime_ms} ms</td><td>${fmtTime(r.updated_time_ms)}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
}

async function selectExtrinsic(hash) {
  state.tab = "extrinsics";
  showTab("extrinsics");
  const data = await api(`/api/extrinsic?hash=${encodeURIComponent(hash)}`);
  $("extrinsic-detail").innerHTML = `
    <div class="muted">row</div><pre class="details">${escapeHtml(pretty(data.row))}</pre>
    <h4>Events</h4>
    ${table(["time", "kind", "slot", "block", "view", "details"], data.events, r =>
      `<td>${fmtTime(r.event_time_ms)}</td><td>${pill(r.event_kind)}</td><td>${r.slot ?? ""}</td><td>${r.block_number ?? ""}</td><td class="mono">${r.view_id ? `<button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 10)}</button>` : ""}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`)}
    <h4>Pool View Membership</h4>
    ${table(["time", "section", "ordinal", "slot", "reason"], data.views, r =>
      `<td>${fmtTime(r.event_time_ms)}</td><td>${r.section}</td><td>${r.ordinal}</td><td>${r.slot ?? ""}</td><td>${r.reason}</td>`)}
  `;
}

async function loadPoolViews() {
  const qs = new URLSearchParams();
  qs.set("limit", $("pool-limit").value);
  const data = await api(`/api/pool-views?${qs}`);
  $("pool-table").innerHTML = table(["time", "view", "slot", "parent", "ready", "future", "reason"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td class="mono"><button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 12)}</button></td><td>${r.slot ?? ""}</td><td>${r.parent_number}</td><td>${r.ready_count}</td><td>${r.future_count}</td><td>${r.reason}</td>`);
}

async function selectPoolView(viewId) {
  state.tab = "pool";
  showTab("pool");
  const data = await api(`/api/pool-view?view_id=${encodeURIComponent(viewId)}`);
  $("pool-detail").innerHTML = `
    <div class="muted">view</div><pre class="details">${escapeHtml(pretty(data.view))}</pre>
    <h4>Members</h4>
    ${table(["section", "ordinal", "hash", "priority", "class", "status", "provides"], data.members, r =>
      `<td>${r.section}</td><td>${r.ordinal}</td><td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${r.priority ?? ""}</td><td>${r.classification ?? ""}</td><td>${pill(r.last_status)}</td><td class="wrap">${escapeHtml(r.provides_json)}</td>`)}
  `;
}

async function loadBlocks() {
  const qs = new URLSearchParams();
  qs.set("limit", $("blocks-limit").value);
  const data = await api(`/api/blocks?${qs}`);
  $("blocks-table").innerHTML = table(["time", "slot", "parent", "block", "duration", "result", "error"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td>${r.slot}</td><td>${r.parent_number}</td><td>${r.block_number ?? ""}</td><td>${r.duration_ms} ms</td><td>${pill(r.result)}</td><td class="wrap">${escapeHtml(r.error ?? "")}</td>`);
}

function showTab(tab) {
  state.tab = tab;
  for (const name of ["dashboard", "timeline", "extrinsics", "pool", "blocks"]) {
    $(`tab-${name}`).classList.toggle("hidden", name !== tab);
    document.querySelector(`[data-tab="${name}"]`).classList.toggle("active", name === tab);
  }
  refresh();
}

async function refresh() {
  setError(null);
  try {
    if (state.tab === "dashboard") await loadDashboard();
    if (state.tab === "timeline") await loadTimeline();
    if (state.tab === "extrinsics") await loadExtrinsics();
    if (state.tab === "pool") await loadPoolViews();
    if (state.tab === "blocks") await loadBlocks();
  } catch (error) {
    setError(error.message);
  }
}

for (const btn of document.querySelectorAll("[data-tab]")) {
  btn.addEventListener("click", () => showTab(btn.dataset.tab));
}
$("refresh").addEventListener("click", refresh);
$("load-timeline").addEventListener("click", loadTimeline);
$("load-extrinsics").addEventListener("click", loadExtrinsics);
$("load-pool").addEventListener("click", loadPoolViews);
$("load-blocks").addEventListener("click", loadBlocks);
setInterval(() => { if ($("auto-refresh").checked) refresh(); }, 5000);
refresh();
</script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("authoring-sim.sqlite"),
        help="Path to the authoring simulation SQLite database.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host.")
    parser.add_argument("--port", type=int, default=8787, help="Bind port.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.db.exists():
        raise SystemExit(f"database does not exist: {args.db}")

    Handler.app = App(Db(args.db))
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"Authoring sim DB viewer: {url}")
    print(f"Reading: {args.db.resolve()}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
