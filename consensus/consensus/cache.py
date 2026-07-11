"""On-disk raw-response cache (SQLite).

Every live upstream fetch is written here verbatim, keyed by
``(source, endpoint, params, fetch_ts)``. Two reasons, both from the spec:

  - **Rule 1 / audit trail.** A signal must trace back to raw fetched records
    stored on disk. This table *is* that record — the parsed model objects are
    derived views, this is ground truth.
  - **§M0 backtest replay.** The harness replays from this store; ``latest(...,
    as_of=T)`` returns the most recent response fetched at or before ``T``, which
    is how the consensus replay enforces "no lookahead" — you can only see what
    had actually been fetched by the as-of instant.

Append-only: a new fetch never overwrites an old one, so history is preserved.

Postgres-swap note (spec §1): the schema is deliberately plain (TEXT/INTEGER,
no SQLite-only column types). Swapping to Postgres means changing the
``INTEGER PRIMARY KEY`` to ``BIGSERIAL`` and the connection factory; the queries
are standard SQL.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import CacheError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_responses (
    id          INTEGER PRIMARY KEY,
    source      TEXT NOT NULL,
    endpoint    TEXT NOT NULL,
    params_json TEXT NOT NULL,
    fetch_ts    TEXT NOT NULL,
    http_status INTEGER,
    body_json   TEXT NOT NULL,
    row_count   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_raw_lookup
    ON raw_responses (source, endpoint, params_json, fetch_ts);
"""


def canonical_params(params: dict[str, Any] | None) -> str:
    """Deterministic JSON encoding of request params, so the same logical request
    keys to the same cache row regardless of dict ordering."""
    return json.dumps(params or {}, sort_keys=True, separators=(",", ":"), default=str)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class CachedResponse:
    source: str
    endpoint: str
    params: dict[str, Any]
    fetch_ts: str
    http_status: int | None
    body: Any
    row_count: int | None


class RawCache:
    """A thin SQLite wrapper around the raw-response log. Use as a context manager
    or call :meth:`close` explicitly."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.path))
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        except (sqlite3.Error, OSError) as exc:
            raise CacheError(f"cannot open cache at {self.path}: {exc}") from exc

    # -- write ---------------------------------------------------------------

    def store(
        self,
        *,
        source: str,
        endpoint: str,
        params: dict[str, Any] | None,
        body: Any,
        http_status: int | None,
        fetch_ts: str | None = None,
    ) -> int:
        """Append one raw response. Returns the new row id."""
        ts = fetch_ts or _now_iso()
        row_count = len(body) if isinstance(body, (list, tuple)) else None
        try:
            body_json = json.dumps(body, separators=(",", ":"), default=str)
        except (TypeError, ValueError) as exc:
            raise CacheError(f"response body not JSON-serialisable: {exc}") from exc
        try:
            cur = self._conn.execute(
                "INSERT INTO raw_responses "
                "(source, endpoint, params_json, fetch_ts, http_status, body_json, row_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (source, endpoint, canonical_params(params), ts, http_status, body_json, row_count),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise CacheError(f"cache write failed: {exc}") from exc
        return int(cur.lastrowid)

    # -- read ----------------------------------------------------------------

    def latest(
        self,
        *,
        source: str,
        endpoint: str,
        params: dict[str, Any] | None,
        as_of: str | None = None,
    ) -> CachedResponse | None:
        """Most recent cached response for this exact request. With ``as_of`` (an
        ISO-8601 UTC string), the most recent fetched at or before that instant —
        the no-lookahead read the backtest uses. Returns ``None`` if nothing
        matches (caller decides whether that is a loud error)."""
        key = canonical_params(params)
        sql = (
            "SELECT source, endpoint, params_json, fetch_ts, http_status, body_json, row_count "
            "FROM raw_responses WHERE source=? AND endpoint=? AND params_json=?"
        )
        args: list[Any] = [source, endpoint, key]
        if as_of is not None:
            sql += " AND fetch_ts <= ?"
            args.append(as_of)
        sql += " ORDER BY fetch_ts DESC, id DESC LIMIT 1"
        try:
            row = self._conn.execute(sql, args).fetchone()
        except sqlite3.Error as exc:
            raise CacheError(f"cache read failed: {exc}") from exc
        if row is None:
            return None
        try:
            body = json.loads(row[5])
        except (json.JSONDecodeError, TypeError) as exc:
            raise CacheError(f"corrupt cached body for {source}{endpoint}: {exc}") from exc
        return CachedResponse(
            source=row[0],
            endpoint=row[1],
            params=json.loads(row[2]),
            fetch_ts=row[3],
            http_status=row[4],
            body=body,
            row_count=row[6],
        )

    def count(self) -> int:
        """Total rows in the cache (all sources/endpoints)."""
        try:
            return int(self._conn.execute("SELECT COUNT(*) FROM raw_responses").fetchone()[0])
        except sqlite3.Error as exc:
            raise CacheError(f"cache count failed: {exc}") from exc

    def stats(self) -> dict[str, Any]:
        """Cache observability (addendum v1.1 §2.6): size on disk, total rows,
        and per-source row counts with oldest/newest fetch_ts.

        The cache is append-only by design — it is the Rule-1 audit trail and
        the backtest replay substrate, so there is NO eviction. If size becomes
        a problem the answer is partitioning, never deletion.
        """
        try:
            total = int(self._conn.execute("SELECT COUNT(*) FROM raw_responses").fetchone()[0])
            per_source = self._conn.execute(
                "SELECT source, COUNT(*), MIN(fetch_ts), MAX(fetch_ts) "
                "FROM raw_responses GROUP BY source ORDER BY source"
            ).fetchall()
        except sqlite3.Error as exc:
            raise CacheError(f"cache stats failed: {exc}") from exc
        try:
            size_bytes = self.path.stat().st_size
        except OSError:
            size_bytes = None
        return {
            "path": str(self.path),
            "size_bytes": size_bytes,
            "total_rows": total,
            "sources": [
                {"source": s, "rows": n, "oldest_fetch_ts": lo, "newest_fetch_ts": hi}
                for s, n, lo, hi in per_source
            ],
        }

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass

    def __enter__(self) -> "RawCache":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
