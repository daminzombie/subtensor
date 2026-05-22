#!/usr/bin/env python3
"""
Local web UI for inspecting event export SQLite databases.

Usage:
    python3 scripts/event_export_ui.py --db /path/to/event-export.sqlite
    python3 scripts/event_export_ui.py --db /path/to/event-export.sqlite --retention-minutes 30 --prune-interval-seconds 180
    EVENT_EXPORT_DB=/var/lib/subtensor/chains/bittensor/event-export.sqlite uvicorn scripts.event_export_ui:asgi_app --host 0.0.0.0 --port 8787
    EVENT_EXPORT_RETENTION_MINUTES=30 EVENT_EXPORT_PRUNE_INTERVAL_SECONDS=180 uvicorn scripts.event_export_ui:asgi_app --host 0.0.0.0 --port 8787
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


DEFAULT_LIMIT = 200
MAX_LIMIT = 5000
DEFAULT_PRUNE_INTERVAL_SECONDS = 180
DEFAULT_RETENTION_MINUTES = 5
DEFAULT_PRUNE_BATCH_SIZE = 5_000
DEFAULT_DB_PATH = Path("/var/lib/subtensor/chains/bittensor/event-export.sqlite")
DEFAULT_VIEW_TIMELINE_BLOCKS = 10


def clamp_limit(value: str | None, default: int = DEFAULT_LIMIT) -> int:
    if value is None:
        return default
    try:
        return max(1, min(MAX_LIMIT, int(value)))
    except ValueError:
        return default


def parse_int(value: str | None, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def to_jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    return value


def details_value(row: dict[str, Any], key: str) -> Any:
    try:
        details = json.loads(row.get("details_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    return details.get(key) if isinstance(details, dict) else None


class Db:
    def __init__(self, path: Path):
        self.path = path.resolve()

    def connect(self, readonly: bool = True, timeout: float = 1.0) -> sqlite3.Connection:
        uri_path = urllib.parse.quote(str(self.path), safe="/:")
        mode = "ro" if readonly else "rw"
        conn = sqlite3.connect(f"file:{uri_path}?mode={mode}", uri=True, timeout=timeout)
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

    def prune_before(self, cutoff_time_ms: int, batch_size: int) -> int:
        with self.connect(readonly=False, timeout=0.25) as conn:
            conn.execute("PRAGMA busy_timeout = 250")
            cursor = conn.execute(
                """
                DELETE FROM events
                WHERE seq IN (
                    SELECT seq
                    FROM events
                    WHERE event_time_ms < ?
                    ORDER BY event_time_ms
                    LIMIT ?
                )
                """,
                (cutoff_time_ms, batch_size),
            )
            deleted = cursor.rowcount if cursor.rowcount is not None else 0
            return deleted


def is_sqlite_lock_error(error: sqlite3.OperationalError) -> bool:
    message = str(error).lower()
    return "locked" in message or "busy" in message


def prune_once(db: Db, retention_minutes: int, batch_size: int) -> int:
    cutoff_time_ms = int(time.time() * 1000) - retention_minutes * 60_000
    return db.prune_before(cutoff_time_ms, batch_size)


def run_prune_loop(
    db: Db,
    retention_minutes: int,
    interval_seconds: int,
    batch_size: int,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(interval_seconds):
        try:
            deleted = prune_once(db, retention_minutes, batch_size)
            if deleted:
                print(
                    f"Pruned {deleted} events older than {retention_minutes} minutes "
                    f"from {db.path}"
                )
        except sqlite3.OperationalError as error:
            if not is_sqlite_lock_error(error):
                print(f"Skipped event DB prune: {error}")
        except Exception as error:
            print(f"Event DB prune failed: {error}")


class App:
    def __init__(self, db: Db):
        self.db = db

    def summary(self) -> dict[str, Any]:
        counts = {
            "events": self.db.scalar("SELECT COUNT(*) FROM events") or 0,
            "tx_events": self.db.scalar("SELECT COUNT(*) FROM events WHERE tx_hash IS NOT NULL")
            or 0,
            "chain_events": self.db.scalar("SELECT COUNT(*) FROM events WHERE source = 'chain'")
            or 0,
            "pool_events": self.db.scalar("SELECT COUNT(*) FROM events WHERE source = 'pool'")
            or 0,
            "timeline_events": self.db.scalar("SELECT COUNT(*) FROM events WHERE source = 'timeline'")
            or 0,
        }

        latest_time = self.db.scalar("SELECT MAX(event_time_ms) FROM events")

        return {
            "db_path": str(self.db.path),
            "counts": counts,
            "latest_event_time_ms": latest_time,
            "latest_writer": self.db.one(
                "SELECT * FROM events WHERE source = 'writer' ORDER BY event_time_ms DESC, seq DESC LIMIT 1"
            ),
            "recent_event_kinds": self.db.rows(
                """
                SELECT event_kind, COUNT(*) AS count, MAX(event_time_ms) AS last_time_ms
                FROM events
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
                FROM events ev
                LEFT JOIN (
                    SELECT tx_hash, MAX(classification) AS classification, MAX(status) AS last_status
                    FROM events
                    WHERE tx_hash IS NOT NULL
                    GROUP BY tx_hash
                ) x ON x.tx_hash = ev.tx_hash
                WHERE ev.tx_hash IS NOT NULL
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
            SELECT *, details_json AS tx_details_json
            FROM events
            {clause}
            ORDER BY event_time_ms DESC
            LIMIT ?
            """,
            tuple(params),
            limit,
        )
        return {"rows": rows, "limit": limit}

    def tx_timeline(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        tx_hash = first(query, "hash")
        where = ["tx_hash IS NOT NULL"]
        params: list[Any] = []
        if tx_hash:
            where.append("tx_hash = ?")
            params.append(tx_hash)
        clause = f"WHERE {' AND '.join(where)}"
        rows = self.db.rows(
            f"""
            SELECT *, details_json AS tx_details_json
            FROM events
            {clause}
            ORDER BY event_time_ms DESC, seq DESC
            LIMIT ?
            """,
            tuple(params),
            limit,
        )
        return {"rows": rows, "limit": limit}

    def view_timeline(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"), MAX_LIMIT)
        blocks = parse_int(first(query, "blocks"), DEFAULT_VIEW_TIMELINE_BLOCKS)
        parent_block = parse_int(first(query, "parent_block"))
        build_block = parse_int(first(query, "build_block"))
        from_block = parse_int(first(query, "from_block"))
        to_block = parse_int(first(query, "to_block"))
        parent_expr = """
            COALESCE(
                CAST(json_extract(details_json, '$.parent_block_number') AS INTEGER),
                CAST(json_extract(details_json, '$.parent_number') AS INTEGER),
                block_number - 1
            )
        """
        build_expr = """
            COALESCE(
                CAST(json_extract(details_json, '$.build_block_number') AS INTEGER),
                CAST(json_extract(details_json, '$.view_block_number') AS INTEGER),
                block_number
            )
        """

        if any(value is not None for value in (parent_block, build_block, from_block, to_block)):
            where = []
            params: list[Any] = []
            if parent_block is not None:
                where.append("parent_block_number = ?")
                params.append(parent_block)
            if build_block is not None:
                where.append("build_block_number = ?")
                params.append(build_block)
            if from_block is not None:
                where.append("parent_block_number >= ?")
                params.append(from_block)
            if to_block is not None:
                where.append("parent_block_number <= ?")
                params.append(to_block)
            rows = self.db.rows(
                f"""
                WITH views AS (
                    SELECT
                        *,
                        {parent_expr} AS parent_block_number,
                        {build_expr} AS build_block_number
                    FROM events
                    WHERE event_kind = 'pool_view_created'
                )
                SELECT *
                FROM views
                WHERE {' AND '.join(where)}
                ORDER BY parent_block_number DESC, event_time_ms DESC, seq DESC
                LIMIT ?
                """,
                tuple(params),
                limit,
            )
        else:
            rows = self.db.rows(
                f"""
                WITH views AS (
                    SELECT
                        *,
                        {parent_expr} AS parent_block_number,
                        {build_expr} AS build_block_number
                    FROM events
                    WHERE event_kind = 'pool_view_created'
                ),
                recent_parent_blocks AS (
                    SELECT DISTINCT parent_block_number
                    FROM views
                    WHERE parent_block_number IS NOT NULL
                    ORDER BY parent_block_number DESC
                    LIMIT ?
                )
                SELECT *
                FROM views
                WHERE parent_block_number IN (
                    SELECT parent_block_number FROM recent_parent_blocks
                )
                ORDER BY parent_block_number DESC, event_time_ms DESC, seq DESC
                LIMIT ?
                """,
                (max(1, blocks or DEFAULT_VIEW_TIMELINE_BLOCKS),),
                limit,
            )
        for row in rows:
            members = self.db.rows(
                """
                SELECT tx_hash, insertion_id, status AS section, details_json
                FROM events
                WHERE event_kind = 'pool_view_member' AND view_id = ?
                ORDER BY
                    CASE status WHEN 'ready' THEN 0 WHEN 'future' THEN 1 ELSE 2 END,
                    CAST(json_extract(details_json, '$.ordinal') AS INTEGER),
                    seq
                LIMIT ?
                """,
                (row.get("view_id"),),
                MAX_LIMIT,
            )
            row["members_json"] = json.dumps(
                [
                    {
                        "tx_hash": member.get("tx_hash"),
                        "insertion_id": member.get("insertion_id"),
                        "section": member.get("section"),
                        "ordinal": details_value(member, "ordinal"),
                    }
                    for member in members
                ]
            )
        return {
            "rows": rows,
            "limit": limit,
            "blocks": blocks,
            "parent_block": parent_block,
            "build_block": build_block,
            "from_block": from_block,
            "to_block": to_block,
        }

    def extrinsics(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = clamp_limit(first(query, "limit"))
        search = first(query, "q")
        status = first(query, "status")
        classification = first(query, "classification")
        where = ["x.tx_hash IS NOT NULL"]
        params: list[Any] = []
        if search:
            where.append("(x.tx_hash LIKE ? OR x.details_json LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like])
        if status:
            where.append("x.status = ?")
            params.append(status)
        if classification:
            where.append("x.classification = ?")
            params.append(classification)
        clause = f"WHERE {' AND '.join(where)}" if where else ""

        rows = self.db.rows(
            f"""
            SELECT
                x.tx_hash,
                MIN(x.event_time_ms) AS first_seen_time_ms,
                MIN(x.source) AS first_seen_source,
                NULL AS encoded_len,
                MAX(x.classification) AS classification,
                (
                    SELECT details_json FROM events latest
                    WHERE latest.tx_hash = x.tx_hash
                    ORDER BY latest.event_time_ms DESC, latest.seq DESC
                    LIMIT 1
                ) AS details_json,
                MAX(x.status) AS last_status,
                MAX(x.event_time_ms) AS updated_time_ms,
                COUNT(x.seq) AS event_count,
                MIN(x.event_time_ms) AS first_event_time_ms,
                MAX(x.event_time_ms) AS last_event_time_ms,
                COALESCE(MAX(x.event_time_ms) - MIN(x.event_time_ms), 0) AS lifetime_ms
            FROM events x
            {clause}
            GROUP BY x.tx_hash
            ORDER BY updated_time_ms DESC
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
        row = self.db.one(
            """
            SELECT *, details_json AS tx_details_json
            FROM events
            WHERE tx_hash = ?
            ORDER BY event_time_ms DESC, seq DESC
            LIMIT 1
            """,
            (tx_hash,),
        )
        events = self.db.rows(
            """
            SELECT *
            FROM events
            WHERE tx_hash = ?
            ORDER BY event_time_ms ASC, seq ASC
            LIMIT ?
            """,
            (tx_hash,),
            MAX_LIMIT,
        )
        views = self.db.rows(
            """
            SELECT *
            FROM events
            WHERE tx_hash = ? AND view_id IS NOT NULL
            ORDER BY event_time_ms ASC, seq ASC
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
            FROM events
            WHERE event_kind = 'pool_view_created'
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
        view = self.db.one(
            "SELECT * FROM events WHERE event_kind = 'pool_view_created' AND view_id = ?",
            (view_id,),
        )
        members = self.db.rows(
            """
            SELECT *
            FROM events
            WHERE event_kind = 'pool_view_member' AND view_id = ?
            ORDER BY seq ASC
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
            FROM events
            WHERE source = 'chain' AND event_kind = 'block_import'
            ORDER BY event_time_ms DESC, seq DESC
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
        if path == "/api/tx-timeline":
            return json_response(app.tx_timeline(query))
        if path == "/api/view-timeline":
            return json_response(app.view_timeline(query))
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
    def __init__(
        self,
        app: App,
        retention_minutes: int,
        prune_interval_seconds: int,
        prune_batch_size: int,
    ):
        self.app = app
        self.retention_minutes = retention_minutes
        self.prune_interval_seconds = prune_interval_seconds
        self.prune_batch_size = prune_batch_size
        self._prune_task: asyncio.Task[Any] | None = None

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
                self.start_pruner()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.stop_pruner()
                await send({"type": "lifespan.shutdown.complete"})
                return

    def start_pruner(self) -> None:
        if self.retention_minutes <= 0 or self.prune_interval_seconds <= 0:
            return
        if self._prune_task is None or self._prune_task.done():
            self._prune_task = asyncio.create_task(self.prune_loop())

    async def stop_pruner(self) -> None:
        if self._prune_task is None:
            return
        self._prune_task.cancel()
        try:
            await self._prune_task
        except asyncio.CancelledError:
            pass

    async def prune_loop(self) -> None:
        while True:
            await asyncio.sleep(self.prune_interval_seconds)
            try:
                deleted = await asyncio.to_thread(
                    prune_once,
                    self.app.db,
                    self.retention_minutes,
                    self.prune_batch_size,
                )
                if deleted:
                    print(
                        f"Pruned {deleted} events older than {self.retention_minutes} "
                        f"minutes from {self.app.db.path}"
                    )
            except sqlite3.OperationalError as error:
                if not is_sqlite_lock_error(error):
                    print(f"Skipped event DB prune: {error}")
            except Exception as error:
                print(f"Event DB prune failed: {error}")

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
    db_path = Path(os.environ.get("EVENT_EXPORT_DB", DEFAULT_DB_PATH))
    retention_minutes = int(
        os.environ.get("EVENT_EXPORT_RETENTION_MINUTES", DEFAULT_RETENTION_MINUTES)
    )
    prune_interval_seconds = int(
        os.environ.get(
            "EVENT_EXPORT_PRUNE_INTERVAL_SECONDS", DEFAULT_PRUNE_INTERVAL_SECONDS
        )
    )
    prune_batch_size = int(
        os.environ.get("EVENT_EXPORT_PRUNE_BATCH_SIZE", DEFAULT_PRUNE_BATCH_SIZE)
    )
    return AsgiApp(
        App(Db(db_path)),
        retention_minutes,
        prune_interval_seconds,
        prune_batch_size,
    )


asgi_app = create_asgi_app()


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Event Export DB Viewer</title>
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
      <h1>Event Export DB Viewer</h1>
      <div id="db-path" class="muted mono"></div>
    </div>
    <div class="tabs">
      <button data-tab="dashboard" class="active">Dashboard</button>
      <button data-tab="timeline">Event Timeline</button>
      <button data-tab="txTimeline">Tx Lifetime Timeline</button>
      <button data-tab="viewTimeline">View Timeline</button>
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
      <div class="card section muted">
        <strong>Slot</strong> is the consensus time slot, not chain height. Use the
        <strong>block #</strong> column for imported, announced, and simulated block numbers.
      </div>
      <div class="controls section">
        <input id="timeline-kind" placeholder="filter event_kind">
        <select id="timeline-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option><option>5000</option></select>
        <button id="load-timeline">Load</button>
      </div>
      <div id="timeline-table" class="section"></div>
    </section>

    <section id="tab-txTimeline" class="hidden">
      <div class="controls section">
        <input id="tx-timeline-hash" placeholder="optional tx hash">
        <select id="tx-timeline-limit"><option>100</option><option selected>200</option><option>500</option><option>1000</option><option>5000</option></select>
        <button id="load-tx-timeline">Load</button>
      </div>
      <div id="tx-timeline-table" class="section"></div>
    </section>

    <section id="tab-viewTimeline" class="hidden">
      <div class="controls section">
        <input id="view-timeline-blocks" placeholder="latest parent blocks" value="10">
        <input id="view-timeline-parent-block" placeholder="parent block N">
        <input id="view-timeline-build-block" placeholder="build block N+1">
        <input id="view-timeline-from-block" placeholder="from parent block">
        <input id="view-timeline-to-block" placeholder="to parent block">
        <select id="view-timeline-limit"><option>100</option><option>200</option><option>500</option><option>1000</option><option selected>5000</option></select>
        <button id="load-view-timeline">Load</button>
      </div>
      <div id="view-timeline-table" class="section"></div>
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

function detailsField(row, field) {
  const details = parseJsonMaybe(row?.details_json);
  if (!details || typeof details !== "object") return "";
  return details[field] ?? "";
}

function txDetailsField(row, field) {
  const details = parseJsonMaybe(row?.tx_details_json ?? row?.details_json);
  if (!details || typeof details !== "object") return "";
  return details[field] ?? "";
}

function timelineDetails(row) {
  const details = parseJsonMaybe(row?.details_json);
  return details && typeof details === "object" ? details : {};
}

function timelineBlockNumber(row) {
  return row.block_number ?? timelineDetails(row).block_number ?? "";
}

function timelineBlockHash(row) {
  return row.block_hash ?? timelineDetails(row).block_hash ?? "";
}

function timelineElapsed(row) {
  const elapsed = timelineDetails(row).elapsed_ms;
  return elapsed === undefined || elapsed === null ? "" : `${elapsed} ms`;
}

function addressCell(value) {
  if (!value) return "";
  const s = String(value);
  return `<span class="mono" title="${escapeHtml(s)}">${escapeHtml(short(s, 10))}</span>`;
}

function hashCell(value, n = 10) {
  if (!value) return "";
  const s = String(value);
  return `<span class="mono" title="${escapeHtml(s)}">${escapeHtml(short(s, n))}</span>`;
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
  $("timeline-table").innerHTML = table(["time", "source", "kind", "slot", "block #", "block hash", "tx", "insertion", "status", "view", "details"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td>${r.source}</td><td>${pill(r.event_kind)}</td><td>${r.slot ?? ""}</td><td>${timelineBlockNumber(r)}</td><td>${hashCell(timelineBlockHash(r))}</td><td class="mono">${r.tx_hash ? `<button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button>` : ""}</td><td>${r.insertion_id ?? ""}</td><td>${r.status ? pill(r.status) : ""}</td><td class="mono">${r.view_id ? `<button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 10)}</button>` : ""}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
}

async function loadTxTimeline() {
  const qs = new URLSearchParams();
  qs.set("limit", $("tx-timeline-limit").value);
  if ($("tx-timeline-hash").value) qs.set("hash", $("tx-timeline-hash").value);
  const data = await api(`/api/tx-timeline?${qs}`);
  $("tx-timeline-table").innerHTML = table(["time", "tx", "event", "block #", "view", "section", "ordinal", "insertion", "from", "to", "details"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${pill(r.event_kind)}</td><td>${r.block_number ?? detailsField(r, "view_block_number") ?? ""}</td><td class="mono">${r.view_id ? `<button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 10)}</button>` : ""}</td><td>${r.status ?? detailsField(r, "section")}</td><td>${detailsField(r, "ordinal")}</td><td>${r.insertion_id ?? detailsField(r, "insertion_id") ?? ""}</td><td>${addressCell(txDetailsField(r, "from"))}</td><td>${addressCell(txDetailsField(r, "to"))}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
}

async function loadViewTimeline() {
  const qs = new URLSearchParams();
  qs.set("limit", $("view-timeline-limit").value);
  if ($("view-timeline-blocks").value) qs.set("blocks", $("view-timeline-blocks").value);
  if ($("view-timeline-parent-block").value) qs.set("parent_block", $("view-timeline-parent-block").value);
  if ($("view-timeline-build-block").value) qs.set("build_block", $("view-timeline-build-block").value);
  if ($("view-timeline-from-block").value) qs.set("from_block", $("view-timeline-from-block").value);
  if ($("view-timeline-to-block").value) qs.set("to_block", $("view-timeline-to-block").value);
  const data = await api(`/api/view-timeline?${qs}`);
  $("view-timeline-table").innerHTML = table(["time", "view", "reason", "trigger tx", "parent block", "build block", "parent hash", "ready", "future", "tx hash / insertion id pairs"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td class="mono"><button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 12)}</button></td><td>${pill(detailsField(r, "reason") ?? r.status ?? "")}</td><td class="mono">${detailsField(r, "trigger_tx_hash") ? `<button onclick="selectExtrinsic('${escapeHtml(detailsField(r, "trigger_tx_hash"))}')">${short(detailsField(r, "trigger_tx_hash"))}</button>` : ""}</td><td>${r.parent_block_number ?? detailsField(r, "parent_block_number") ?? detailsField(r, "parent_number") ?? ""}</td><td>${r.build_block_number ?? detailsField(r, "build_block_number") ?? r.block_number ?? detailsField(r, "view_block_number") ?? ""}</td><td>${hashCell(r.parent_hash)}</td><td>${detailsField(r, "ready_count")}</td><td>${detailsField(r, "future_count")}</td><td class="wrap"><pre>${escapeHtml(pretty(r.members_json))}</pre></td>`);
}

async function loadExtrinsics() {
  const qs = new URLSearchParams();
  qs.set("limit", $("extrinsic-limit").value);
  if ($("extrinsic-q").value) qs.set("q", $("extrinsic-q").value);
  if ($("extrinsic-status").value) qs.set("status", $("extrinsic-status").value);
  if ($("extrinsic-class").value) qs.set("classification", $("extrinsic-class").value);
  const data = await api(`/api/extrinsics?${qs}`);
  $("extrinsics-table").innerHTML = table(["hash", "class", "from", "to", "status", "events", "lifetime", "updated", "details"], data.rows, r =>
    `<td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${r.classification}</td><td>${addressCell(detailsField(r, "from"))}</td><td>${addressCell(detailsField(r, "to"))}</td><td>${pill(r.last_status)}</td><td>${r.event_count}</td><td>${r.lifetime_ms} ms</td><td>${fmtTime(r.updated_time_ms)}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
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
    ${table(["time", "section", "ordinal", "insertion", "view block"], data.views, r =>
      `<td>${fmtTime(r.event_time_ms)}</td><td>${r.status ?? detailsField(r, "section")}</td><td>${detailsField(r, "ordinal")}</td><td>${r.insertion_id ?? detailsField(r, "insertion_id") ?? ""}</td><td>${r.block_number ?? detailsField(r, "view_block_number") ?? ""}</td>`)}
  `;
}

async function loadPoolViews() {
  const qs = new URLSearchParams();
  qs.set("limit", $("pool-limit").value);
  const data = await api(`/api/pool-views?${qs}`);
  $("pool-table").innerHTML = table(["time", "view", "slot", "parent", "ready", "future", "reason"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td class="mono"><button onclick="selectPoolView('${escapeHtml(r.view_id)}')">${short(r.view_id, 12)}</button></td><td>${r.slot ?? ""}</td><td>${detailsField(r, "parent_number")}</td><td>${detailsField(r, "ready_count")}</td><td>${detailsField(r, "future_count")}</td><td>${r.status ?? detailsField(r, "reason")}</td>`);
}

async function selectPoolView(viewId) {
  state.tab = "pool";
  showTab("pool");
  const data = await api(`/api/pool-view?view_id=${encodeURIComponent(viewId)}`);
  $("pool-detail").innerHTML = `
    <div class="muted">view</div><pre class="details">${escapeHtml(pretty(data.view))}</pre>
    <h4>Members</h4>
    ${table(["section", "ordinal", "hash", "priority", "status", "details"], data.members, r =>
      `<td>${detailsField(r, "section")}</td><td>${detailsField(r, "ordinal")}</td><td class="mono"><button onclick="selectExtrinsic('${escapeHtml(r.tx_hash)}')">${short(r.tx_hash)}</button></td><td>${detailsField(r, "priority")}</td><td>${pill(r.status)}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`)}
  `;
}

async function loadBlocks() {
  const qs = new URLSearchParams();
  qs.set("limit", $("blocks-limit").value);
  const data = await api(`/api/blocks?${qs}`);
  $("blocks-table").innerHTML = table(["time", "kind", "block #", "block hash", "new best", "origin", "details"], data.rows, r =>
    `<td>${fmtTime(r.event_time_ms)}</td><td>${pill(r.event_kind)}</td><td>${r.block_number ?? ""}</td><td>${hashCell(r.block_hash)}</td><td>${detailsField(r, "is_new_best")}</td><td>${r.status ?? ""}</td><td class="wrap"><pre>${escapeHtml(pretty(r.details_json))}</pre></td>`);
}

function showTab(tab) {
  state.tab = tab;
  for (const name of ["dashboard", "timeline", "txTimeline", "viewTimeline", "extrinsics", "pool", "blocks"]) {
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
    if (state.tab === "txTimeline") await loadTxTimeline();
    if (state.tab === "viewTimeline") await loadViewTimeline();
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
$("load-tx-timeline").addEventListener("click", loadTxTimeline);
$("load-view-timeline").addEventListener("click", loadViewTimeline);
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
        default=DEFAULT_DB_PATH,
        help="Path to the event export SQLite database.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host.")
    parser.add_argument("--port", type=int, default=8787, help="Bind port.")
    parser.add_argument(
        "--retention-minutes",
        type=int,
        default=DEFAULT_RETENTION_MINUTES,
        help=(
            "Keep only events newer than this many minutes. "
            "Set to 0 to disable pruning."
        ),
    )
    parser.add_argument(
        "--prune-interval-seconds",
        type=int,
        default=DEFAULT_PRUNE_INTERVAL_SECONDS,
        help="How often to delete old events while the server is running.",
    )
    parser.add_argument(
        "--prune-batch-size",
        type=int,
        default=DEFAULT_PRUNE_BATCH_SIZE,
        help="Maximum number of old event rows to delete per prune pass.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.db.exists():
        raise SystemExit(f"database does not exist: {args.db}")

    db = Db(args.db)
    Handler.app = App(db)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    stop_pruner = threading.Event()
    pruner: threading.Thread | None = None
    url = f"http://{args.host}:{args.port}"
    print(f"Event export DB viewer: {url}")
    print(f"Reading: {args.db.resolve()}")
    if args.retention_minutes > 0 and args.prune_interval_seconds > 0:
        print(
            "Pruning events older than "
            f"{args.retention_minutes} minutes every "
            f"{args.prune_interval_seconds} seconds"
        )
        pruner = threading.Thread(
            target=run_prune_loop,
            args=(
                db,
                args.retention_minutes,
                args.prune_interval_seconds,
                args.prune_batch_size,
                stop_pruner,
            ),
            daemon=True,
        )
        pruner.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        stop_pruner.set()
        if pruner is not None:
            pruner.join(timeout=1.0)
        server.server_close()


if __name__ == "__main__":
    main()
