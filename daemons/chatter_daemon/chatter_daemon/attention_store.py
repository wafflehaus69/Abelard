"""ATTENTION rolling store (Order 8 Phase 2) — 14-day hot per-scan counts, the
velocity substrate for off-watchlist discovery.

A SEPARATE table in chatter's existing state DB — NOT the Order-7 watchlist baseline:
different scope (universe-wide, threshold-gated vs watchlist-bounded) and different
retention (14-day-hot-then-prune vs unbounded trailing). Mirrors `baseline.py`'s SQLite
plumbing (WAL, autocommit, ON CONFLICT, exclude-current `read_baseline`). Per-scan
timestamped counts keyed `(ticker, source, scan_ts)` — NOT per-event rows.

PRUNE = ROLL-UP-TO-COLD, never plain delete: events past the 14-day window aggregate to
a compact per-day/ticker/source `DayRollup`, get archived to cold storage, and ONLY
THEN leave the hot table. The caller archives before deleting, so nothing is lost.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone

from .baseline import Baseline, transaction
from .schema import DayRollup

HOT_WINDOW_DAYS = 14
_DAY_S = 24 * 60 * 60

_SCHEMA = """
CREATE TABLE IF NOT EXISTS attention_observations (
    ticker  TEXT    NOT NULL,
    source  TEXT    NOT NULL,
    scan_ts INTEGER NOT NULL,
    count   INTEGER NOT NULL,
    PRIMARY KEY (ticker, source, scan_ts)
);
CREATE INDEX IF NOT EXISTS idx_attn_key_ts
    ON attention_observations (ticker, source, scan_ts DESC);
CREATE INDEX IF NOT EXISTS idx_attn_ts ON attention_observations (scan_ts);
"""


def init_attention_table(conn: sqlite3.Connection) -> None:
    """Create the attention_observations table if absent. Idempotent."""
    conn.executescript(_SCHEMA)


def hot_cutoff(now: int, *, hot_days: int = HOT_WINDOW_DAYS) -> int:
    """Timestamp boundary: events with `scan_ts < hot_cutoff(now)` are past the hot
    window and prunable."""
    return int(now) - hot_days * _DAY_S


def append_observation(
    conn: sqlite3.Connection, *, ticker: str, source: str, scan_ts: int, count: int
) -> None:
    """Append one per-scan count. One row per key+ts; a re-run at the same ts
    overwrites (idempotent)."""
    with transaction(conn):
        conn.execute(
            "INSERT INTO attention_observations (ticker, source, scan_ts, count) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(ticker, source, scan_ts) DO UPDATE SET count=excluded.count",
            (ticker, source, int(scan_ts), int(count)),
        )


def read_baseline(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    source: str,
    window: int,
    now: int,
    max_age_s: int | None = None,
) -> Baseline:
    """Trailing baseline over the last `window` observations strictly BEFORE `now`
    (the velocity substrate; current scan excluded). Sample std (n-1); 0.0 when n<2."""
    params: list[object] = [ticker, source, int(now)]
    age_clause = ""
    if max_age_s is not None:
        age_clause = " AND scan_ts >= ?"
        params.append(int(now) - int(max_age_s))
    params.append(int(window))
    rows = conn.execute(
        "SELECT count FROM attention_observations "
        "WHERE ticker=? AND source=? AND scan_ts < ?"
        + age_clause
        + " ORDER BY scan_ts DESC LIMIT ?",
        params,
    ).fetchall()

    counts = [int(r["count"]) for r in rows]
    n = len(counts)
    if n == 0:
        return Baseline(0, 0.0, 0.0)
    mean = sum(counts) / n
    if n < 2:
        return Baseline(n, round(mean, 4), 0.0)
    var = sum((c - mean) ** 2 for c in counts) / (n - 1)
    return Baseline(n, round(mean, 4), round(math.sqrt(var), 4))


def collect_prunable_rollups(conn: sqlite3.Connection, *, cutoff: int) -> list[DayRollup]:
    """Roll up every hot event with `scan_ts < cutoff` into per-(day, ticker, source)
    summaries. Pure read — deletion is a separate, archive-gated step."""
    rows = conn.execute(
        "SELECT ticker, source, scan_ts, count FROM attention_observations "
        "WHERE scan_ts < ? ORDER BY ticker, source, scan_ts",
        (int(cutoff),),
    ).fetchall()

    groups: dict[tuple[str, str, str], list[int]] = {}
    for r in rows:
        day = datetime.fromtimestamp(int(r["scan_ts"]), tz=timezone.utc).strftime("%Y-%m-%d")
        groups.setdefault((day, r["ticker"], r["source"]), []).append(int(r["count"]))

    out: list[DayRollup] = []
    for (day, ticker, source), counts in sorted(groups.items()):
        out.append(
            DayRollup(
                day=day,
                ticker=ticker,
                source=source,
                scans=len(counts),
                total_count=sum(counts),
                max_count=max(counts),
            )
        )
    return out


def delete_pruned(conn: sqlite3.Connection, *, cutoff: int) -> int:
    """Delete hot events with `scan_ts < cutoff`. Call ONLY after the rollups are
    safely archived to cold storage. Returns the row count removed."""
    with transaction(conn):
        cur = conn.execute(
            "DELETE FROM attention_observations WHERE scan_ts < ?", (int(cutoff),)
        )
        return cur.rowcount


__all__ = [
    "HOT_WINDOW_DAYS",
    "append_observation",
    "collect_prunable_rollups",
    "delete_pruned",
    "hot_cutoff",
    "init_attention_table",
    "read_baseline",
]
