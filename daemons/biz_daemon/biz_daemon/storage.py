"""SQLite state: the Finnhub universe cache and the per-scrape snapshot log.

The snapshot table is the velocity substrate — whether a ticker is
accelerating across scrapes is answered later, off these rows. We do not
build velocity logic now; we persist faithfully so it is available.

Cost telemetry is folded into the snapshot payload by the orchestrator
*before* this module writes to disk, so a write failure never loses the
Haiku cost record from the returned object.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS universe_cache (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    symbols_json  TEXT NOT NULL,
    source        TEXT NOT NULL,
    fetched_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    scrape_ts     INTEGER PRIMARY KEY,
    payload_json  TEXT NOT NULL,
    cost_json     TEXT NOT NULL,
    created_at    INTEGER NOT NULL
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection (WAL, autocommit). Caller owns the lifecycle."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if absent. Idempotent."""
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def read_cached_universe(
    conn: sqlite3.Connection, *, ttl_s: int, now: int
) -> set[str] | None:
    """Return the cached symbol set if present and within TTL, else None."""
    row = conn.execute(
        "SELECT symbols_json, fetched_at FROM universe_cache WHERE id = 1"
    ).fetchone()
    if row is None:
        return None
    if now - int(row["fetched_at"]) > ttl_s:
        return None
    return set(json.loads(row["symbols_json"]))


def write_cached_universe(
    conn: sqlite3.Connection, *, symbols: set[str], source: str, now: int
) -> None:
    symbols_json = json.dumps(sorted(symbols), separators=(",", ":"))
    with transaction(conn):
        conn.execute(
            "INSERT INTO universe_cache (id, symbols_json, source, fetched_at) "
            "VALUES (1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET "
            "symbols_json=excluded.symbols_json, source=excluded.source, "
            "fetched_at=excluded.fetched_at",
            (symbols_json, source, now),
        )


def persist_snapshot(
    conn: sqlite3.Connection,
    *,
    scrape_ts: int,
    payload: dict[str, Any],
    cost: dict[str, Any],
    now: int,
) -> None:
    """Persist one scrape. One row per scrape_ts; re-runs overwrite."""
    with transaction(conn):
        conn.execute(
            "INSERT INTO snapshots (scrape_ts, payload_json, cost_json, created_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(scrape_ts) DO UPDATE SET "
            "payload_json=excluded.payload_json, cost_json=excluded.cost_json, "
            "created_at=excluded.created_at",
            (
                scrape_ts,
                json.dumps(payload, separators=(",", ":")),
                json.dumps(cost, separators=(",", ":")),
                now,
            ),
        )
