"""L2 forward-archive tape (addendum v1.2 §1-2) — the collector's persistent record.

The tape is the L2 layer of the three-layer data model: an append-only,
deduplicated record of every fill the collector observes in target-category
markets, from collector-start forward. Unlike the M1 RawCache (which stores
whole responses), the tape preserves every raw RECORD verbatim (the ``raw``
column) with poll provenance — including records that fail to parse
(``parse_ok=0``): L2 is an archive, so upstream records are never dropped,
only marked unusable.

Rule 1 applied to time: intervals the collector could not observe are recorded
in ``l2_gaps`` as declared gaps — never bridged, never interpolated. A reader
spanning a gap sees the gap.

Dedupe model (measured 2026-07-12, see reference memo):
  - data-api aggregates fills per (tx, taker): txHash collisions are rare
    (~0.5% worst observed) and transient.
  - fill_key = sha256 over the raw record's identifying fields
    (transactionHash, proxyWallet, asset, side, price, size, timestamp),
    hashed from the RAW JSON values for fidelity.
  - Within one response page, identical tuples are REAL distinct fills →
    stored with an occurrence suffix (``#2``, ``#3``...). Across polls, an
    identical tuple is presumed to be the same fill (deduped). The residual
    ambiguity (identical fills split across polls) is counted via the
    ``dupe_records`` instrumentation, not guessed at.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from .errors import CacheError

_SCHEMA = """
CREATE TABLE IF NOT EXISTS l2_trades (
    fill_key         TEXT PRIMARY KEY,
    condition_id     TEXT,
    proxy_wallet     TEXT,
    side             TEXT,
    asset            TEXT,
    outcome          TEXT,
    price            REAL,
    size             REAL,
    timestamp        INTEGER,
    transaction_hash TEXT,
    slug             TEXT,
    parse_ok         INTEGER NOT NULL DEFAULT 1,
    lane             TEXT NOT NULL,
    first_seen_poll  INTEGER NOT NULL,
    raw              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_l2_trades_market_ts ON l2_trades(condition_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_l2_trades_wallet_ts ON l2_trades(proxy_wallet, timestamp);
CREATE INDEX IF NOT EXISTS idx_l2_trades_ts ON l2_trades(timestamp);

CREATE TABLE IF NOT EXISTS l2_markets (
    condition_id    TEXT PRIMARY KEY,
    slug            TEXT,
    question        TEXT,
    tags            TEXT,
    source          TEXT NOT NULL,             -- 'enumeration' | 'stray'
    adopted_ts      INTEGER NOT NULL,
    active          INTEGER NOT NULL DEFAULT 1,
    end_date        TEXT,
    tier            TEXT NOT NULL DEFAULT 'dormant',  -- 'hot' | 'quiet' | 'dormant'
    hot_until_ts    INTEGER NOT NULL DEFAULT 0,
    last_polled_ts  INTEGER NOT NULL DEFAULT 0,
    newest_fill_ts  INTEGER NOT NULL DEFAULT 0,
    last_new_fills  INTEGER NOT NULL DEFAULT 0,
    close_seen_ts   INTEGER NOT NULL DEFAULT 0    -- when enumeration first saw it closed
);

CREATE TABLE IF NOT EXISTS l2_polls (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    invoked_ts       INTEGER NOT NULL,
    lane             TEXT NOT NULL,                -- 'market' | 'global' | 'enumeration'
    condition_id     TEXT,
    pages            INTEGER NOT NULL DEFAULT 0,
    raw_records      INTEGER NOT NULL DEFAULT 0,
    new_records      INTEGER NOT NULL DEFAULT 0,
    dupe_records     INTEGER NOT NULL DEFAULT 0,
    skipped_records  INTEGER NOT NULL DEFAULT 0,
    unparsed_records INTEGER NOT NULL DEFAULT 0,
    overlap_found    INTEGER NOT NULL DEFAULT 0,
    gap_declared     INTEGER NOT NULL DEFAULT 0,
    error            TEXT
);

CREATE TABLE IF NOT EXISTS l2_gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    lane         TEXT NOT NULL,
    condition_id TEXT,
    lo_ts        INTEGER,
    hi_ts        INTEGER,
    declared_ts  INTEGER NOT NULL,
    reason       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS l2_strays (
    condition_id  TEXT PRIMARY KEY,
    first_seen_ts INTEGER NOT NULL,
    fill_count    INTEGER NOT NULL DEFAULT 0,
    resolved      INTEGER NOT NULL DEFAULT 0    -- 1 once enumerator adjudicated it
);

CREATE TABLE IF NOT EXISTS l2_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

# Raw-record fields that identify a fill (hashed from RAW values, not parsed).
_KEY_FIELDS = ("transactionHash", "proxyWallet", "asset", "side", "price", "size", "timestamp")


def fill_key_base(raw: dict[str, Any]) -> str:
    """Deterministic identity hash of one raw fill record."""
    ident = json.dumps([raw.get(f) for f in _KEY_FIELDS], sort_keys=False, default=str)
    return hashlib.sha256(ident.encode()).hexdigest()


class TapeStore:
    """SQLite persistence for the L2 tape. Append-only by convention: nothing
    here updates or deletes a stored fill or a declared gap."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(str(self.path))
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_SCHEMA)
            self._migrate()
            self._conn.commit()
        except sqlite3.Error as exc:
            raise CacheError(f"tape open failed at {self.path}: {exc}") from exc

    def _migrate(self) -> None:
        """Additive column migrations for tapes created by earlier schema
        versions. Never destructive — the tape is the archive."""
        for table, column, decl in (
            ("l2_markets", "close_seen_ts", "INTEGER NOT NULL DEFAULT 0"),
            ("l2_polls", "skipped_records", "INTEGER NOT NULL DEFAULT 0"),
            ("l2_polls", "unparsed_records", "INTEGER NOT NULL DEFAULT 0"),
        ):
            cols = {r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")

    def close(self) -> None:
        self._conn.close()

    # -- fills ---------------------------------------------------------------

    def store_page(
        self,
        raw_records: list[Any],
        *,
        lane: str,
        poll_id: int,
        parsed_by: Any,  # Callable[[dict], model|None] — Trade.from_api
        restrict_condition_ids: set[str] | None = None,
        occurrence: dict[str, int] | None = None,
    ) -> dict[str, int]:
        """Insert one response page of raw fill records.

        Returns counters: ``raw`` (records seen), ``new`` (inserted),
        ``overlap`` (already stored), ``dupes`` (identical tuples within this
        walk, stored as distinct fills), ``skipped`` (outside restrict set),
        ``unparsed`` (stored with parse_ok=0 — archived, never dropped).

        ``occurrence`` scopes the identical-tuple disambiguation: pass one dict
        across all pages of a single walk so a multi-fill tx straddling a page
        boundary is still stored as two fills, not conflated with a cross-poll
        dupe. Omitting it scopes to this page only.
        """
        counts = {"raw": len(raw_records), "new": 0, "overlap": 0, "dupes": 0,
                  "skipped": 0, "unparsed": 0}
        if occurrence is None:
            occurrence = {}
        try:
            for raw in raw_records:
                if not isinstance(raw, dict):
                    # Malformed upstream element (null/string/number). L2 is an
                    # archive: store it raw with parse_ok=0 — never drop.
                    counts["unparsed"] += 1
                    base = hashlib.sha256(
                        json.dumps(raw, sort_keys=True, default=str).encode()
                    ).hexdigest()
                    occurrence[base] = occurrence.get(base, 0) + 1
                    key = base if occurrence[base] == 1 else f"{base}#{occurrence[base]}"
                    cur = self._conn.execute(
                        "INSERT OR IGNORE INTO l2_trades (fill_key, parse_ok, lane,"
                        " first_seen_poll, raw) VALUES (?,0,?,?,?)",
                        (key, lane, poll_id, json.dumps(raw, default=str)),
                    )
                    counts["new" if cur.rowcount else "overlap"] += 1
                    continue
                cid = raw.get("conditionId")
                if restrict_condition_ids is not None and cid not in restrict_condition_ids:
                    counts["skipped"] += 1
                    continue
                base = fill_key_base(raw)
                occurrence[base] = occurrence.get(base, 0) + 1
                key = base if occurrence[base] == 1 else f"{base}#{occurrence[base]}"
                if occurrence[base] > 1:
                    counts["dupes"] += 1

                parsed = parsed_by(raw)
                if parsed is None:
                    counts["unparsed"] += 1
                row = (
                    key,
                    cid,
                    (raw.get("proxyWallet") or "").lower() or None,
                    parsed.side if parsed else None,
                    raw.get("asset"),
                    parsed.outcome if parsed else None,
                    parsed.price if parsed else None,
                    parsed.size if parsed else None,
                    parsed.timestamp if parsed else None,
                    raw.get("transactionHash"),
                    raw.get("slug"),
                    1 if parsed else 0,
                    lane,
                    poll_id,
                    json.dumps(raw, sort_keys=True),
                )
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO l2_trades (fill_key, condition_id, proxy_wallet,"
                    " side, asset, outcome, price, size, timestamp, transaction_hash, slug,"
                    " parse_ok, lane, first_seen_poll, raw)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    row,
                )
                if cur.rowcount:
                    counts["new"] += 1
                else:
                    counts["overlap"] += 1
            self._conn.commit()
        except sqlite3.Error as exc:
            raise CacheError(f"tape store_page failed: {exc}") from exc
        return counts

    def has_fills(self, condition_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM l2_trades WHERE condition_id = ? LIMIT 1", (condition_id,)
        ).fetchone()
        return row is not None

    def newest_fill_ts(self, condition_id: str | None = None) -> int | None:
        if condition_id is None:
            row = self._conn.execute("SELECT MAX(timestamp) FROM l2_trades").fetchone()
        else:
            row = self._conn.execute(
                "SELECT MAX(timestamp) FROM l2_trades WHERE condition_id = ?", (condition_id,)
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    # -- markets ---------------------------------------------------------------

    def upsert_market(
        self,
        condition_id: str,
        *,
        slug: str | None,
        question: str | None,
        tags: str,
        source: str,
        now_ts: int,
        closed: bool = False,
        end_date: str | None = None,
    ) -> bool:
        """Insert a market if unknown (returns True); refresh mutable metadata
        if known (returns False). Tier/poll state is never reset here.

        A closed signal does NOT deactivate the market directly — it stamps
        ``close_seen_ts`` so the collector keeps polling through a drain window
        (the fills between the last poll and close are exactly the data that
        matters on a resolving market). Deactivation happens in the collector's
        drain sweep. A market seen open again clears the stamp (re-listing,
        enumeration flicker)."""
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO l2_markets"
            " (condition_id, slug, question, tags, source, adopted_ts, active, end_date,"
            "  close_seen_ts)"
            " VALUES (?,?,?,?,?,?,1,?,?)",
            (condition_id, slug, question, tags, source, now_ts, end_date,
             now_ts if closed else 0),
        )
        inserted = bool(cur.rowcount)
        if not inserted:
            if closed:
                # Keep the FIRST close sighting; keep active until drained.
                self._conn.execute(
                    "UPDATE l2_markets SET slug=?, question=?, tags=?, end_date=?,"
                    " close_seen_ts = CASE WHEN close_seen_ts = 0 THEN ? ELSE close_seen_ts END"
                    " WHERE condition_id=?",
                    (slug, question, tags, end_date, now_ts, condition_id),
                )
            else:
                self._conn.execute(
                    "UPDATE l2_markets SET slug=?, question=?, tags=?, end_date=?,"
                    " active=1, close_seen_ts=0 WHERE condition_id=?",
                    (slug, question, tags, end_date, condition_id),
                )
        self._conn.commit()
        return inserted

    def deactivate_drained(self, *, now_ts: int, drain_seconds: int) -> list[str]:
        """Deactivate markets whose close was seen at least ``drain_seconds``
        ago (they stayed in the poll rotation through the drain window, so the
        tail of fills around resolution was captured). Returns the ids."""
        rows = self._conn.execute(
            "SELECT condition_id FROM l2_markets"
            " WHERE active = 1 AND close_seen_ts > 0 AND close_seen_ts <= ?",
            (now_ts - drain_seconds,),
        ).fetchall()
        ids = [r[0] for r in rows]
        if ids:
            self._conn.executemany(
                "UPDATE l2_markets SET active = 0 WHERE condition_id = ?",
                [(i,) for i in ids],
            )
            self._conn.commit()
        return ids

    def markets(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        q = "SELECT * FROM l2_markets"
        if active_only:
            q += " WHERE active = 1"
        cur = self._conn.execute(q)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def update_market_poll_state(
        self,
        condition_id: str,
        *,
        tier: str,
        hot_until_ts: int,
        last_polled_ts: int,
        newest_fill_ts: int,
        last_new_fills: int,
    ) -> None:
        self._conn.execute(
            "UPDATE l2_markets SET tier=?, hot_until_ts=?, last_polled_ts=?,"
            " newest_fill_ts=MAX(newest_fill_ts, ?), last_new_fills=? WHERE condition_id=?",
            (tier, hot_until_ts, last_polled_ts, newest_fill_ts, last_new_fills, condition_id),
        )
        self._conn.commit()

    def promote_to_hot(self, condition_id: str, *, hot_until_ts: int) -> None:
        self._conn.execute(
            "UPDATE l2_markets SET tier='hot', hot_until_ts=MAX(hot_until_ts, ?)"
            " WHERE condition_id=?",
            (hot_until_ts, condition_id),
        )
        self._conn.commit()

    # -- polls / gaps / strays --------------------------------------------------

    def open_poll(self, *, invoked_ts: int, lane: str, condition_id: str | None = None) -> int:
        cur = self._conn.execute(
            "INSERT INTO l2_polls (invoked_ts, lane, condition_id) VALUES (?,?,?)",
            (invoked_ts, lane, condition_id),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def close_poll(self, poll_id: int, **fields: Any) -> None:
        allowed = {"pages", "raw_records", "new_records", "dupe_records",
                   "skipped_records", "unparsed_records",
                   "overlap_found", "gap_declared", "error"}
        unknown = set(fields) - allowed
        if unknown:
            raise CacheError(f"close_poll: unknown fields {unknown}")
        sets = ", ".join(f"{k}=?" for k in fields)
        self._conn.execute(
            f"UPDATE l2_polls SET {sets} WHERE id=?", (*fields.values(), poll_id)
        )
        self._conn.commit()

    def declare_gap(
        self,
        *,
        lane: str,
        condition_id: str | None,
        lo_ts: int | None,
        hi_ts: int | None,
        declared_ts: int,
        reason: str,
    ) -> int:
        """Record an interval the collector could not observe. Loud by design:
        callers must also log it. Never bridged, never deleted."""
        cur = self._conn.execute(
            "INSERT INTO l2_gaps (lane, condition_id, lo_ts, hi_ts, declared_ts, reason)"
            " VALUES (?,?,?,?,?,?)",
            (lane, condition_id, lo_ts, hi_ts, declared_ts, reason),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def record_stray(self, condition_id: str, *, now_ts: int, fills: int) -> None:
        self._conn.execute(
            "INSERT INTO l2_strays (condition_id, first_seen_ts, fill_count)"
            " VALUES (?,?,?)"
            " ON CONFLICT(condition_id) DO UPDATE SET fill_count = fill_count + ?",
            (condition_id, now_ts, fills, fills),
        )
        self._conn.commit()

    def unresolved_strays(self) -> list[str]:
        return [r[0] for r in self._conn.execute(
            "SELECT condition_id FROM l2_strays WHERE resolved = 0"
        ).fetchall()]

    def resolve_stray(self, condition_id: str) -> None:
        self._conn.execute(
            "UPDATE l2_strays SET resolved = 1 WHERE condition_id = ?", (condition_id,)
        )
        self._conn.commit()

    # -- meta / stats -------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM l2_meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO l2_meta (key, value) VALUES (?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._conn.commit()

    def stats(self) -> dict[str, Any]:
        try:
            fills = self._conn.execute("SELECT COUNT(*) FROM l2_trades").fetchone()[0]
            unparsed = self._conn.execute(
                "SELECT COUNT(*) FROM l2_trades WHERE parse_ok = 0"
            ).fetchone()[0]
            markets = self._conn.execute("SELECT COUNT(*) FROM l2_markets").fetchone()[0]
            gaps = self._conn.execute("SELECT COUNT(*) FROM l2_gaps").fetchone()[0]
            polls = self._conn.execute("SELECT COUNT(*) FROM l2_polls").fetchone()[0]
            strays = self._conn.execute(
                "SELECT COUNT(*) FROM l2_strays WHERE resolved = 0"
            ).fetchone()[0]
            span = self._conn.execute(
                "SELECT MIN(timestamp), MAX(timestamp) FROM l2_trades"
            ).fetchone()
        except sqlite3.Error as exc:
            raise CacheError(f"tape stats failed: {exc}") from exc
        try:
            size_bytes = self.path.stat().st_size
        except OSError:
            size_bytes = None
        return {
            "path": str(self.path),
            "size_bytes": size_bytes,
            "fills": fills,
            "fills_unparsed": unparsed,
            "markets": markets,
            "polls": polls,
            "gaps_declared": gaps,
            "unresolved_strays": strays,
            "oldest_fill_ts": span[0],
            "newest_fill_ts": span[1],
        }
