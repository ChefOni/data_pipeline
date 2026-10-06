#!/usr/bin/env python3
"""
Config-driven incremental sync pipeline.

Add endpoints by editing sources.yaml. No code changes needed.
Add new kinds of data sources (CSV, database, webhook...) by writing a Source
subclass and registering it in SOURCE_TYPES (see bottom of the Source section).

  pip install requests pyyaml
  python pipeline.py --config sources.yaml              # loop forever
  python pipeline.py --config sources.yaml --once       # one pass over everything
  python pipeline.py --config sources.yaml --status     # show what has synced
  python pipeline.py --config sources.yaml --only my_juno/candidates --full
"""
import argparse
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import requests
import yaml

log = logging.getLogger("pipeline")
stop = False


# ---------- helpers ----------

def utcnow():
    return datetime.now(timezone.utc)


def dig(obj, path):
    """Read a dotted path like 'meta.next_cursor'. Empty path returns obj itself."""
    if not path:
        return obj
    for part in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def expand_env(value):
    """Replace ${VAR} with environment variables so secrets stay out of the YAML."""
    if isinstance(value, str):
        def sub(m):
            if m.group(1) not in os.environ:
                raise SystemExit(f"Missing environment variable: {m.group(1)}")
            return os.environ[m.group(1)]
        return re.sub(r"\$\{(\w+)\}", sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


# ---------- sources ----------

class Source:
    """Interface every data source implements. fetch() yields dict records."""

    def __init__(self, cfg):
        self.cfg = cfg

    def fetch(self, ep, since):
        raise NotImplementedError


class HttpSource(Source):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.session = requests.Session()
        self.auth_params = {}
        auth = cfg.get("auth") or {}
        kind = auth.get("type")
        if kind == "bearer":
            self.session.headers["Authorization"] = f"Bearer {auth['token']}"
        elif kind == "header":
            self.session.headers[auth["name"]] = auth["value"]
        elif kind == "query":
            self.auth_params[auth["name"]] = auth["value"]
        self.session.headers.update(cfg.get("headers") or {})

    def _get(self, path, params, max_retries):
        url = self.cfg["base_url"].rstrip("/") + "/" + path.lstrip("/")
        for attempt in range(max_retries):
            backoff = min(60, 2 ** attempt) + random.random()
            try:
                r = self.session.get(url, params=params, timeout=30)
            except (requests.ConnectionError, requests.Timeout) as e:
                log.warning("Network error on %s (%s), retry in %.1fs", path, e, backoff)
                time.sleep(backoff)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = float(r.headers.get("Retry-After", backoff))
                log.warning("HTTP %s on %s, retry in %.1fs", r.status_code, path, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()  # other 4xx: fail fast (bad token, bad params)
            return r.json()
        raise RuntimeError(f"Gave up on {path} after {max_retries} attempts")

    def fetch(self, ep, since):
        pg = ep.get("pagination") or {"type": "none"}
        kind = pg.get("type", "none")
        size = pg.get("page_size", 100)
        page, cursor = 1, None
        while True:
            params = dict(ep.get("params") or {})
            params.update(self.auth_params)
            if since and ep.get("since_param"):
                params[ep["since_param"]] = since
            if kind in ("page", "cursor"):
                params[pg.get("size_param", "limit")] = size
            if kind == "page":
                params[pg.get("page_param", "page")] = page
            elif kind == "cursor" and cursor:
                params[pg.get("cursor_param", "cursor")] = cursor

            body = self._get(ep["path"], params, ep.get("max_retries", 6))
            items = dig(body, ep.get("items_path", ""))
            if items is None:
                items = []
            yield from items

            if kind == "page":
                if len(items) < size:
                    return
                page += 1
            elif kind == "cursor":
                cursor = dig(body, pg.get("cursor_path", ""))
                if not cursor:
                    return
            else:
                return


# To add a new kind of source later: subclass Source, implement fetch(), register it.
# Then in YAML use `type: csv`, with whatever settings your class reads from self.cfg.
SOURCE_TYPES = {"http": HttpSource}


# ---------- storage ----------

def connect(path):
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS records(
            source TEXT, endpoint TEXT, id TEXT,
            payload TEXT NOT NULL, content_hash TEXT NOT NULL,
            first_seen_at TEXT NOT NULL, last_changed_at TEXT NOT NULL,
            PRIMARY KEY (source, endpoint, id));
        CREATE TABLE IF NOT EXISTS state(
            source TEXT, endpoint TEXT, last_sync TEXT, last_attempt_at TEXT,
            PRIMARY KEY (source, endpoint));
        CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, endpoint TEXT,
            started_at TEXT, finished_at TEXT, fetched INTEGER, inserted INTEGER,
            updated INTEGER, unchanged INTEGER, status TEXT, error TEXT);
        """
    )
    return con


def get_state(con, source, endpoint):
    return con.execute(
        "SELECT last_sync, last_attempt_at FROM state WHERE source=? AND endpoint=?",
        (source, endpoint),
    ).fetchone() or (None, None)


def upsert(con, source, endpoint, rid, rec, now):
    payload = json.dumps(rec, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode()).hexdigest()
    row = con.execute(
        "SELECT content_hash FROM records WHERE source=? AND endpoint=? AND id=?",
        (source, endpoint, rid),
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO records VALUES (?,?,?,?,?,?,?)",
            (source, endpoint, rid, payload, digest, now, now),
        )
        return "inserted"
    if row[0] != digest:
        con.execute(
            "UPDATE records SET payload=?, content_hash=?, last_changed_at=? "
            "WHERE source=? AND endpoint=? AND id=?",
            (payload, digest, now, source, endpoint, rid),
        )
        return "updated"
    return "unchanged"


# ---------- sync ----------

def sync_endpoint(con, source_obj, sname, ep, full=False):
    ename = ep["name"]
    started = utcnow()
    now = started.isoformat()
    last_sync, _ = get_state(con, sname, ename)

    since = None
    if last_sync and not full and ep.get("since_param"):
        overlap = timedelta(seconds=ep.get("overlap_seconds", 120))
        since = (datetime.fromisoformat(last_sync) - overlap).isoformat()

    counts = {"fetched": 0, "inserted": 0, "updated": 0, "unchanged": 0}
    run_id = con.execute(
        "INSERT INTO runs(source, endpoint, started_at, status) VALUES(?,?,?,'running')",
        (sname, ename, now),
    ).lastrowid
    con.commit()
    log.info("[%s/%s] sync started (since=%s)", sname, ename, since or "beginning")

    new_last_sync, status, error = None, "ok", None
    try:
        for rec in source_obj.fetch(ep, since):
            rid = dig(rec, ep.get("id_field", "id"))
            if rid is None:
                log.warning("[%s/%s] record without id skipped", sname, ename)
                continue
            counts["fetched"] += 1
            counts[upsert(con, sname, ename, str(rid), rec, now)] += 1
            if counts["fetched"] % 500 == 0:
                con.commit()
        new_last_sync = now  # advance checkpoint only after a fully successful run
    except Exception as e:
        status, error = "failed", str(e)
        log.exception("[%s/%s] sync failed", sname, ename)

    con.execute(
        "INSERT INTO state(source, endpoint, last_sync, last_attempt_at) VALUES(?,?,?,?) "
        "ON CONFLICT(source, endpoint) DO UPDATE SET "
        "last_attempt_at=excluded.last_attempt_at, "
        "last_sync=COALESCE(excluded.last_sync, state.last_sync)",
        (sname, ename, new_last_sync, now),
    )
    con.execute(
        "UPDATE runs SET finished_at=?, fetched=?, inserted=?, updated=?, unchanged=?, "
        "status=?, error=? WHERE id=?",
        (utcnow().isoformat(), counts["fetched"], counts["inserted"], counts["updated"],
         counts["unchanged"], status, error, run_id),
    )
    con.commit()
    log.info("[%s/%s] %s %s", sname, ename, status, counts)


def is_due(con, sname, ep):
    _, last_attempt = get_state(con, sname, ep["name"])
    if not last_attempt:
        return True
    interval = ep.get("interval_seconds", 300)
    return utcnow() >= datetime.fromisoformat(last_attempt) + timedelta(seconds=interval)


def print_status(con, jobs):
    print(f"{'endpoint':35} {'rows':>8} {'last sync':>28}  last run")
    for sname, ep in jobs:
        rows = con.execute(
            "SELECT COUNT(*) FROM records WHERE source=? AND endpoint=?", (sname, ep["name"])
        ).fetchone()[0]
        last_sync, _ = get_state(con, sname, ep["name"])
        run = con.execute(
            "SELECT status, fetched, inserted, updated FROM runs "
            "WHERE source=? AND endpoint=? ORDER BY id DESC LIMIT 1",
            (sname, ep["name"]),
        ).fetchone()
        run_txt = f"{run[0]} (fetched {run[1]}, new {run[2]}, changed {run[3]})" if run else "never run"
        print(f"{sname + '/' + ep['name']:35} {rows:>8} {str(last_sync or '-'):>28}  {run_txt}")


# ---------- config + main ----------

def load_config(path):
    with open(path) as f:
        cfg = expand_env(yaml.safe_load(f))
    defaults = cfg.get("defaults") or {}
    seen, jobs, sources = set(), [], {}
    for s in cfg.get("sources", []):
        name = s["name"]
        if name in sources:
            raise SystemExit(f"Duplicate source name: {name}")
        stype = s.get("type", "http")
        if stype not in SOURCE_TYPES:
            raise SystemExit(f"Unknown source type '{stype}' for source '{name}'")
        sources[name] = SOURCE_TYPES[stype](s)
        for ep in s.get("endpoints", []):
            merged = {**defaults, **ep}
            merged["pagination"] = {**(defaults.get("pagination") or {}), **(ep.get("pagination") or {})}
            if "name" not in merged or "path" not in merged:
                raise SystemExit(f"Endpoint in '{name}' needs both 'name' and 'path'")
            key = (name, merged["name"])
            if key in seen:
                raise SystemExit(f"Duplicate endpoint: {name}/{merged['name']}")
            seen.add(key)
            jobs.append((name, merged))
    return cfg, sources, jobs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--once", action="store_true", help="run every selected endpoint once and exit")
    ap.add_argument("--full", action="store_true", help="ignore checkpoints on the first pass")
    ap.add_argument("--only", action="append", help="limit to source/endpoint (repeatable)")
    ap.add_argument("--status", action="store_true", help="print sync status and exit")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg, sources, jobs = load_config(args.config)
    if args.only:
        jobs = [j for j in jobs if f"{j[0]}/{j[1]['name']}" in args.only]
        if not jobs:
            raise SystemExit("--only matched no endpoints")

    con = connect(cfg.get("database", "pipeline.db"))
    if args.status:
        print_status(con, jobs)
        return

    def handle_stop(signum, frame):
        global stop
        stop = True
        log.info("Shutdown requested, finishing current sync...")

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    full_pending = {(s, e["name"]) for s, e in jobs} if args.full else set()
    log.info("Pipeline running with %d endpoint(s)", len(jobs))
    while not stop:
        for sname, ep in jobs:
            if stop:
                break
            if args.once or is_due(con, sname, ep):
                key = (sname, ep["name"])
                sync_endpoint(con, sources[sname], sname, ep, full=key in full_pending)
                full_pending.discard(key)
        if args.once:
            break
        time.sleep(1)


if __name__ == "__main__":
    main()