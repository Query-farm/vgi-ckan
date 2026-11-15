#!/usr/bin/env python3
"""Strict record/replay server for the CKAN Action API, used by every test.

Running CKAN itself needs Solr, PostgreSQL, Redis and CKAN (several GB), so the
tests replay real responses recorded once from public portals instead. Each
fixture file in ``test/fixtures/`` is one portal mounted under a path prefix:

=====================  ==========================  ==============
prefix                 recorded from               CKAN
=====================  ==========================  ==============
``/demo``              https://demo.ckan.org        2.11.5
``/canada``            https://open.canada.ca/data  2.10.8
``/ireland``           https://data.gov.ie          2.9.16
``/qld``               https://www.data.qld.gov.au  (hidden; has datastore_search_sql)
``/private``           hand-written                 token-protected portal
=====================  ==========================  ==============

so the worker is pointed at e.g. ``http://127.0.0.1:<port>/ireland``.

**Replay is strict.** A request is answered only if the exact method, path and
query string (parameters sorted) was recorded; anything else gets HTTP 501 and
a CKAN-shaped error naming the request, so a changed request fails the test
instead of silently passing.

**Credentials.** A fixture with a ``token`` demands ``Authorization: <token>``
(else CKAN's 403 Authorization Error). Every other fixture rejects any request
that *carries* an ``Authorization`` header with HTTP 400, so a token leaking
to the wrong portal fails loudly.

Switches: ``MOCK_FLAKY=1`` answers the first request for every key with
``429`` + ``Retry-After: 0`` (exercises the retry path). ``GET /__requests``
returns the request log; ``POST /__reset`` clears it.

Recording (needs network; tiny volumes, run once by a maintainer)::

    python3 test/mock_api/server.py --record     # forwards unrecorded requests upstream, saves them

Usage::

    python3 test/mock_api/server.py               # prints PORT:<n>
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
USER_AGENT = "vgi-ckan-test-recorder/0.1 (+https://github.com/Query-farm/vgi-ckan)"


def canonical(path: str, query: str) -> str:
    """The replay key: path plus the query parameters sorted (values kept verbatim)."""
    pairs = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    qs = urllib.parse.urlencode(pairs, quote_via=urllib.parse.quote)
    return path + (f"?{qs}" if qs else "")


class Portal:
    """One fixture file: a recorded portal mounted at a prefix."""

    def __init__(self, file: Path) -> None:
        self.file = file
        self.data: dict[str, Any] = json.loads(file.read_text())
        self.prefix: str = self.data["prefix"]
        self.upstream: str | None = self.data.get("upstream")
        self.token: str | None = self.data.get("token")
        self.interactions: dict[str, dict[str, Any]] = self.data.setdefault("interactions", {})

    def save(self) -> None:
        self.data["interactions"] = dict(sorted(self.interactions.items()))
        self.file.write_text(json.dumps(self.data, indent=1, ensure_ascii=False, sort_keys=False) + "\n")


class State:
    """Mounted portals, flaky bookkeeping and the request log."""

    def __init__(self, record: bool, fixtures: Path) -> None:
        self.portals = [Portal(f) for f in sorted(fixtures.glob("*.json"))]
        self.record = record
        self.flaky = os.environ.get("MOCK_FLAKY") == "1"
        self.seen: set[str] = set()
        self.log: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def portal_for(self, path: str) -> Portal | None:
        for p in self.portals:
            if path == p.prefix or path.startswith(p.prefix + "/"):
                return p
        return None


def _ckan_error(etype: str, message: str) -> dict[str, Any]:
    return {"help": "", "success": False, "error": {"__type": etype, "message": message}}


class Handler(BaseHTTPRequestHandler):
    """Answers GET requests from the fixtures."""

    state: State

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("MOCK_VERBOSE"):
            sys.stderr.write(fmt % args + "\n")

    def _send(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
        raw = (body if isinstance(body, str) else json.dumps(body)).encode()  # str bodies are sent verbatim
        self.send_response(status)
        self.send_header("Content-Type", "application/json;charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:
        if self.path == "/__reset":
            with self.state.lock:
                self.state.log.clear()
                self.state.seen.clear()
            self._send(200, {"ok": True})
            return
        self._send(405, _ckan_error("Method Not Allowed", "the worker only issues GET requests"))

    def do_GET(self) -> None:
        parts = urllib.parse.urlsplit(self.path)
        if parts.path == "/__requests":
            with self.state.lock:
                self._send(200, list(self.state.log))
            return
        portal = self.state.portal_for(parts.path)
        auth = self.headers.get("Authorization")
        entry = {"path": parts.path, "query": parts.query, "auth": bool(auth)}
        with self.state.lock:
            self.state.log.append(entry)
        if portal is None:
            if auth:
                self._send(400, _ckan_error("Unexpected Credential", "a token was sent to an unknown portal"))
                return
            self._send(404, "<html>Not Found</html>")  # like a web server in front of no CKAN
            return
        if portal.token:
            if auth != portal.token:
                self._send(403, _ckan_error("Authorization Error", "Access denied: a valid API token is required"))
                return
        elif auth:
            self._send(400, _ckan_error("Unexpected Credential", f"{portal.prefix} is public; no token expected"))
            return
        key = canonical(parts.path[len(portal.prefix) :], parts.query)
        if self.state.flaky and key not in self.state.seen:
            with self.state.lock:
                self.state.seen.add(key)
            self._send(429, _ckan_error("Rate Limited", "slow down"), {"Retry-After": "0"})
            return
        hit = portal.interactions.get(key)
        if hit is None and self.state.record and portal.upstream:
            hit = self._record(portal, key)
        if hit is None:
            self._send(501, _ckan_error("Unrecorded Request", f"no fixture for GET {portal.prefix}{key}"))
            return
        body = hit["body"]
        text = json.dumps(body, ensure_ascii=False) if hit.get("json", True) else str(body)
        self._send(int(hit["status"]), text)

    def _record(self, portal: Portal, key: str) -> dict[str, Any]:
        url = portal.upstream + key
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                status, raw = r.status, r.read()
        except urllib.error.HTTPError as err:
            status, raw = err.code, err.read()
        text = raw.decode("utf-8", errors="replace")
        try:
            hit: dict[str, Any] = {"status": status, "body": json.loads(text)}
        except ValueError:
            hit = {"status": status, "json": False, "body": text}
        with self.state.lock:
            portal.interactions[key] = hit
            portal.data["recorded_at"] = dt.datetime.now(dt.UTC).date().isoformat()
            portal.save()
        sys.stderr.write(f"recorded {status} {url} ({len(raw)} bytes)\n")
        return hit


def main() -> None:
    """Parse arguments and serve until killed."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--record", action="store_true", help="forward unrecorded requests upstream and save them")
    ap.add_argument("--fixtures", type=Path, default=FIXTURES, help="directory of fixture files")
    args = ap.parse_args()
    Handler.state = State(args.record, args.fixtures)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
