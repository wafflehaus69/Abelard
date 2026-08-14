"""Per-source health and watermark state.

Phase 1 owns only `source_health`. The `opportunities` ledger is Phase 2 --
distribution-first means the schema is written against observed data, not
against a prediction, so it does not exist yet.

WATERMARK DISCIPLINE (the whole point of this module)
-----------------------------------------------------
Transcribed from the FIXED News Watch behavior at
`news_watch_daemon/src/news_watch_daemon/scrape/orchestrator.py:337-362`, not
from its original footgun. Three rules, and the third is the one that bites:

  ok WITH items  -> advance the watermark to the NEWEST INGESTED ITEM's
                    timestamp. Not to `now`.
  ok with ZERO items -> record the attempt; watermark HOLDS.
  error / rate-limited -> record the attempt; watermark HOLDS; failure count++

Advancing to `now` on an empty or failed poll silently skips every item that
was published inside the skipped window. There is no error, no gap, no null --
just a permanently missing slice of history. Advancing only to the newest item
actually ingested means a window that returned nothing stays re-pollable.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from . import config

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_health (
    source                      TEXT PRIMARY KEY,
    last_attempt_unix           INTEGER,
    last_successful_fetch_unix  INTEGER,   -- THE WATERMARK
    consecutive_failures        INTEGER NOT NULL DEFAULT 0,
    last_status                 TEXT,
    last_detail                 TEXT,
    last_item_count             INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scan_log (
    scan_id        TEXT PRIMARY KEY,
    started_unix   INTEGER NOT NULL,
    finished_unix  INTEGER,
    sources_ok     INTEGER NOT NULL DEFAULT 0,
    sources_empty  INTEGER NOT NULL DEFAULT 0,
    sources_error  INTEGER NOT NULL DEFAULT 0,
    items_total    INTEGER NOT NULL DEFAULT 0
);
"""


@dataclass
class SourceHealth:
    source: str
    last_attempt_unix: int | None = None
    last_successful_fetch_unix: int | None = None
    consecutive_failures: int = 0
    last_status: str | None = None
    last_detail: str | None = None
    last_item_count: int = 0


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the scout DB with the schema applied."""
    path = db_path or config.DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()
    return conn


def get_health(conn: sqlite3.Connection, source: str) -> SourceHealth:
    row = conn.execute(
        "SELECT * FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    if row is None:
        return SourceHealth(source=source)
    return SourceHealth(
        source=row["source"],
        last_attempt_unix=row["last_attempt_unix"],
        last_successful_fetch_unix=row["last_successful_fetch_unix"],
        consecutive_failures=row["consecutive_failures"],
        last_status=row["last_status"],
        last_detail=row["last_detail"],
        last_item_count=row["last_item_count"],
    )


def since_unix_for_source(
    conn: sqlite3.Connection, source: str, default_lookback_s: int, now_unix: int
) -> int:
    """The lower bound for this source's next poll.

    First run has no watermark, so it falls back to a bounded lookback rather
    than to 0 -- an unbounded first fetch against a 910-row directory is a
    different operation than an incremental poll, and should be a deliberate
    backfill rather than an accident of a missing row.
    """
    health = get_health(conn, source)
    if health.last_successful_fetch_unix is None:
        return max(0, now_unix - default_lookback_s)
    return health.last_successful_fetch_unix


def record_source_result(
    conn: sqlite3.Connection,
    source: str,
    *,
    now_unix: int,
    status: str,
    item_count: int,
    ingested_high_watermark_unix: int | None,
    detail: str = "",
) -> None:
    """Persist one source's outcome, applying the watermark rules.

    `ingested_high_watermark_unix` is the newest timestamp among items actually
    ingested this poll, or None when the source publishes no usable timestamp.
    A source with no timestamps can never advance its watermark -- correct, and
    the reason `since` is a filter hint rather than a correctness guarantee.
    """
    advance = (
        status == "ok"
        and item_count > 0
        and ingested_high_watermark_unix is not None
    )

    current = get_health(conn, source)
    failures = 0 if status in ("ok", "empty") else current.consecutive_failures + 1
    watermark = (
        ingested_high_watermark_unix
        if advance
        else current.last_successful_fetch_unix   # HOLDS
    )

    conn.execute(
        """
        INSERT INTO source_health(
            source, last_attempt_unix, last_successful_fetch_unix,
            consecutive_failures, last_status, last_detail, last_item_count)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(source) DO UPDATE SET
            last_attempt_unix=excluded.last_attempt_unix,
            last_successful_fetch_unix=excluded.last_successful_fetch_unix,
            consecutive_failures=excluded.consecutive_failures,
            last_status=excluded.last_status,
            last_detail=excluded.last_detail,
            last_item_count=excluded.last_item_count
        """,
        (source, now_unix, watermark, failures, status, detail[:500], item_count),
    )
    conn.commit()


def start_scan(conn: sqlite3.Connection, scan_id: str, now_unix: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO scan_log(scan_id, started_unix) VALUES(?,?)",
        (scan_id, now_unix),
    )
    conn.commit()


def finish_scan(
    conn: sqlite3.Connection,
    scan_id: str,
    *,
    finished_unix: int,
    ok: int,
    empty: int,
    error: int,
    items: int,
) -> None:
    conn.execute(
        "UPDATE scan_log SET finished_unix=?, sources_ok=?, sources_empty=?, "
        "sources_error=?, items_total=? WHERE scan_id=?",
        (finished_unix, ok, empty, error, items, scan_id),
    )
    conn.commit()


__all__ = [
    "SCHEMA_VERSION",
    "SourceHealth",
    "connect",
    "get_health",
    "since_unix_for_source",
    "record_source_result",
    "start_scan",
    "finish_scan",
]
