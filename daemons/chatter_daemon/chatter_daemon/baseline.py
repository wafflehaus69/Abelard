"""SQLite baseline store (Order 7) — trailing count observations, the substrate the
anomaly layer z-scores against.

Mirrors BizDaemon's `storage.py` (WAL, autocommit, `INSERT ... ON CONFLICT`). One
observation row per `(watchlist, ticker, source, canonical_unix)` carrying that
scan's count. The trailing baseline (mean mu, std sigma over the last K observations,
optionally bounded to the last D days) is computed per `(watchlist, ticker, source)`.

ORDERING INVARIANT: `read_baseline` excludes the current scan — it reads rows with
`canonical_unix < now`. The orchestrator reads the baseline, computes the anomaly,
THEN appends the current scan, so a scan never sits in its own baseline (and a re-run
at the same timestamp is idempotent: it overwrites its row and still z-scores against
the strictly-prior history).

Fail loud: an unwritable DB path raises `BaselineError` at connect.
"""

from __future__ import annotations

import math
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    watchlist      TEXT    NOT NULL,
    ticker         TEXT    NOT NULL,
    source         TEXT    NOT NULL,
    canonical_unix INTEGER NOT NULL,
    count          INTEGER NOT NULL,
    PRIMARY KEY (watchlist, ticker, source, canonical_unix)
);
CREATE INDEX IF NOT EXISTS idx_obs_key_ts
    ON observations (watchlist, ticker, source, canonical_unix DESC);
"""


class BaselineError(RuntimeError):
    """Raised when the baseline DB cannot be opened or written."""


@dataclass(frozen=True)
class Baseline:
    """Trailing stats for one `(watchlist, ticker, source)`, EXCLUDING the current
    scan. `n` is the number of prior observations; `std` is the SAMPLE std (n-1),
    0.0 when n < 2 or every observation is identical (the anomaly layer guards on it).
    """

    n: int
    mean: float
    std: float


def connect(db_path: Path) -> sqlite3.Connection:
    """Open a SQLite connection (WAL, autocommit). Fail loud on an unwritable path."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    except (OSError, sqlite3.Error) as exc:
        raise BaselineError(f"cannot open baseline DB at {db_path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create the observations table if absent. Idempotent."""
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


def append_observation(
    conn: sqlite3.Connection,
    *,
    watchlist: str,
    ticker: str,
    source: str,
    canonical_unix: int,
    count: int,
) -> None:
    """Append one count observation. One row per key+ts; a re-run at the same ts
    overwrites (idempotent)."""
    with transaction(conn):
        conn.execute(
            "INSERT INTO observations (watchlist, ticker, source, canonical_unix, count) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(watchlist, ticker, source, canonical_unix) "
            "DO UPDATE SET count=excluded.count",
            (watchlist, ticker, source, int(canonical_unix), int(count)),
        )


def read_baseline(
    conn: sqlite3.Connection,
    *,
    watchlist: str,
    ticker: str,
    source: str,
    window: int,
    now: int,
    max_age_s: int | None = None,
) -> Baseline:
    """Trailing baseline over the last `window` observations strictly BEFORE `now`.

    `max_age_s`, when set, additionally bounds the lookback to `[now - max_age_s, now)`
    (the "last D days" knob). Returns `Baseline(0, 0.0, 0.0)` when there is no prior
    history.
    """
    params: list[object] = [watchlist, ticker, source, int(now)]
    age_clause = ""
    if max_age_s is not None:
        age_clause = " AND canonical_unix >= ?"
        params.append(int(now) - int(max_age_s))
    params.append(int(window))
    rows = conn.execute(
        "SELECT count FROM observations "
        "WHERE watchlist=? AND ticker=? AND source=? AND canonical_unix < ?"
        + age_clause
        + " ORDER BY canonical_unix DESC LIMIT ?",
        params,
    ).fetchall()

    counts = [int(r["count"]) for r in rows]
    n = len(counts)
    if n == 0:
        return Baseline(0, 0.0, 0.0)
    mean = sum(counts) / n
    if n < 2:
        return Baseline(n, round(mean, 4), 0.0)
    var = sum((c - mean) ** 2 for c in counts) / (n - 1)  # sample variance
    return Baseline(n, round(mean, 4), round(math.sqrt(var), 4))


__all__ = [
    "Baseline",
    "BaselineError",
    "append_observation",
    "connect",
    "init_db",
    "read_baseline",
    "transaction",
]
