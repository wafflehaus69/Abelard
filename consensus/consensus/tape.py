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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import CacheError

# SQLite bind-variable cap is 999 (older builds); chunk market-id IN lists well
# under it. The tracked roster is ~15k, so window reads chunk the id set.
_IN_CHUNK = 900

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
    close_seen_ts   INTEGER NOT NULL DEFAULT 0,   -- when enumeration first saw it closed
    resolution      TEXT                          -- raw gamma outcome JSON (who won); NULL until swept
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
    presumed_records INTEGER NOT NULL DEFAULT 0,
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
    resolved      INTEGER NOT NULL DEFAULT 0,   -- 1 once enumerator adjudicated it
    attempts      INTEGER NOT NULL DEFAULT 0    -- failed "unknown to gamma" lookups
);
CREATE INDEX IF NOT EXISTS idx_l2_strays_pending
    ON l2_strays(resolved, attempts, first_seen_ts);

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
            # WAL + these pragmas are the collector's write-throughput budget.
            # The tape is a commit-heavy, index-probe-heavy append log that had
            # grown to ~7.8 GB / 4.3M rows by 2026-07-19, at which point per-pass
            # INSERT-OR-IGNORE dedup against the four indexes went disk-bound and
            # passes ballooned from ~4 min to ~32 min.
            #   - busy_timeout: tolerate a concurrent writer (a stale-lock
            #     takeover can briefly overlap two passes) instead of erroring
            #     out with "database is locked".
            #   - synchronous=NORMAL: safe under WAL (a crash can lose only the
            #     last commit, never corrupt) and removes most fsync stalls; the
            #     collector re-walks and re-captures on the next pass anyway.
            #   - cache_size=-262144: 256 MB page cache (vs the 2 MB default) so
            #     the hot index working set stays resident within a pass.
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA cache_size=-262144")
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
            ("l2_polls", "presumed_records", "INTEGER NOT NULL DEFAULT 0"),
            ("l2_strays", "attempts", "INTEGER NOT NULL DEFAULT 0"),
            ("l2_markets", "resolution", "TEXT"),
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
        skip_below_ts: int | None = None,
    ) -> dict[str, int]:
        """Insert one response page of raw fill records.

        Returns counters: ``raw`` (records seen), ``new`` (inserted),
        ``overlap`` (already stored), ``dupes`` (identical tuples within this
        walk, stored as distinct fills), ``skipped`` (outside restrict set),
        ``unparsed`` (stored with parse_ok=0 — archived, never dropped),
        ``presumed_stored`` (below ``skip_below_ts`` — already on the tape by
        the market-lane contiguity invariant, so not re-processed).

        ``occurrence`` scopes the identical-tuple disambiguation: pass one dict
        across all pages of a single walk so a multi-fill tx straddling a page
        boundary is still stored as two fills, not conflated with a cross-poll
        dupe. Omitting it scopes to this page only.

        ``skip_below_ts`` is a dedup fast-path for the market lane ONLY: a record
        whose raw timestamp is strictly below it is already stored (the walk that
        set the frontier reached down through this timestamp; below the frontier
        the tape is contiguous), so its hash/parse/insert is skipped as pure
        redundant work. The caller sets it to ``frontier − late-arrival margin``
        so the band just under the frontier is STILL re-inserted — a fill
        data-api indexes late lands there and is captured, never dropped. The
        global lane (no per-market frontier) must never pass this.
        """
        counts = {"raw": len(raw_records), "new": 0, "overlap": 0, "dupes": 0,
                  "skipped": 0, "unparsed": 0, "presumed_stored": 0}
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
                if skip_below_ts is not None:
                    ts = raw.get("timestamp")
                    if isinstance(ts, int) and ts < skip_below_ts:
                        # Below the frontier margin: already on the tape. Skip
                        # the hash/parse/insert — this is the redundant-work
                        # elimination. (Records at/above the margin still fall
                        # through to INSERT-OR-IGNORE, catching late arrivals.)
                        counts["presumed_stored"] += 1
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

    # -- read helpers (M10 scan + data-sufficiency readout) --------------------
    # All pure reads: no commit, no writes. Consumers (M10, the supply readout)
    # are read-only clients and must never call the write/upsert/deactivate path.

    def fills_in_window(
        self,
        *,
        lo_ts: int,
        hi_ts: int,
        condition_ids: set[str] | None = None,
        parsed_only: bool = True,
        include_raw: bool = False,
    ) -> list[dict[str, Any]]:
        """Return l2_trades rows with ``lo_ts <= timestamp <= hi_ts`` as column
        dicts, optionally restricted to a market set and to parsed fills. The
        primary fill reader — powers the M10 window scan and roster/edge material.

        Unparsed rows carry NULL timestamp so the range predicate already excludes
        them; ``parsed_only`` adds the explicit ``parse_ok=1`` for clarity. A fill
        is one row keyed by fill_key regardless of which lane inserted it (dedup by
        INSERT-OR-IGNORE), and within-tx real duplicates keep their #2/#3 suffix —
        so callers must NOT filter by lane and should include those rows. The
        market set is chunked under SQLite's bind-variable cap and the merged
        result re-sorted by timestamp."""
        cols = ("fill_key, condition_id, proxy_wallet, side, asset, outcome, price,"
                " size, timestamp, transaction_hash, slug, parse_ok, lane")
        if include_raw:
            cols += ", raw"
        where = "timestamp >= ? AND timestamp <= ?"
        if parsed_only:
            where += " AND parse_ok = 1"
        try:
            if condition_ids is None:
                cur = self._conn.execute(
                    f"SELECT {cols} FROM l2_trades WHERE {where} ORDER BY timestamp",
                    (lo_ts, hi_ts),
                )
                names = [d[0] for d in cur.description]
                return [dict(zip(names, r)) for r in cur.fetchall()]
            ids = list(condition_ids)
            rows: list[dict[str, Any]] = []
            names: list[str] = []
            for i in range(0, len(ids), _IN_CHUNK):
                chunk = ids[i:i + _IN_CHUNK]
                ph = ",".join("?" * len(chunk))
                cur = self._conn.execute(
                    f"SELECT {cols} FROM l2_trades WHERE {where}"
                    f" AND condition_id IN ({ph})",
                    (lo_ts, hi_ts, *chunk),
                )
                names = [d[0] for d in cur.description]
                rows.extend(dict(zip(names, r)) for r in cur.fetchall())
            rows.sort(key=lambda r: r["timestamp"] if r["timestamp"] is not None else 0)
            return rows
        except sqlite3.Error as exc:
            raise CacheError(f"tape fills_in_window failed: {exc}") from exc

    def gaps_overlapping(
        self, *, lo_ts: int, hi_ts: int, condition_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return l2_gaps rows whose declared interval intersects ``[lo_ts,
        hi_ts]``. NULL lo_ts means open-below (-inf), NULL hi_ts open-above
        (+inf). Global-lane gaps (condition_id IS NULL) are ALWAYS returned — a
        global gap can bias any window. The coverage-integrity gate: a reader
        must never silently scan across a declared gap; overlaps are stamped into
        the output as caveats (Rule 1)."""
        try:
            cur = self._conn.execute(
                "SELECT id, lane, condition_id, lo_ts, hi_ts, declared_ts, reason"
                " FROM l2_gaps WHERE (lo_ts IS NULL OR lo_ts <= ?)"
                " AND (hi_ts IS NULL OR hi_ts >= ?)",
                (hi_ts, lo_ts),
            )
            names = [d[0] for d in cur.description]
            gaps = [dict(zip(names, r)) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            raise CacheError(f"tape gaps_overlapping failed: {exc}") from exc
        if condition_ids is None:
            return gaps
        return [g for g in gaps
                if g["condition_id"] is None or g["condition_id"] in condition_ids]

    def market_supply_counts(self, *, now_ts: int | None = None) -> dict[str, int]:
        """One-query resolved-supply aggregate over l2_markets. ``close_seen`` and
        ``drained`` UNDERCOUNT true resolutions — enumeration queries gamma with
        closed=false, so whole-event closures are caught only by the resolution
        sweep — hence ``end_date_passed`` is surfaced as the complementary
        heuristic (ISO end_date sorts chronologically for a lexical compare)."""
        try:
            counts = {
                "total": self._conn.execute("SELECT COUNT(*) FROM l2_markets").fetchone()[0],
                "active": self._conn.execute(
                    "SELECT COUNT(*) FROM l2_markets WHERE active=1").fetchone()[0],
                "close_seen": self._conn.execute(
                    "SELECT COUNT(*) FROM l2_markets WHERE close_seen_ts>0").fetchone()[0],
                "drained": self._conn.execute(
                    "SELECT COUNT(*) FROM l2_markets WHERE active=0").fetchone()[0],
                "resolved_outcome": self._conn.execute(
                    "SELECT COUNT(*) FROM l2_markets"
                    " WHERE resolution IS NOT NULL AND resolution != ''").fetchone()[0],
                "end_date_passed": 0,
            }
            if now_ts is not None:
                iso_now = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ")
                counts["end_date_passed"] = self._conn.execute(
                    "SELECT COUNT(*) FROM l2_markets"
                    " WHERE end_date IS NOT NULL AND end_date != '' AND end_date < ?",
                    (iso_now,),
                ).fetchone()[0]
        except sqlite3.Error as exc:
            raise CacheError(f"tape market_supply_counts failed: {exc}") from exc
        return counts

    def wallet_fill_counts(
        self, *, lo_ts: int | None = None, hi_ts: int | None = None, min_fills: int = 1,
    ) -> list[dict[str, Any]]:
        """Per-wallet fill tallies (roster raw-material sizing): proxy_wallet,
        n_fills, first_ts, last_ts. Optionally windowed. NOTE: this is
        participation volume, not skill — the winning outcome needed for realized
        edge is not on the tape until the resolution sweep persists it."""
        q = ("SELECT proxy_wallet, COUNT(*) AS n_fills, MIN(timestamp) AS first_ts,"
             " MAX(timestamp) AS last_ts FROM l2_trades"
             " WHERE proxy_wallet IS NOT NULL AND parse_ok = 1")
        params: list[Any] = []
        if lo_ts is not None:
            q += " AND timestamp >= ?"; params.append(lo_ts)
        if hi_ts is not None:
            q += " AND timestamp <= ?"; params.append(hi_ts)
        q += " GROUP BY proxy_wallet HAVING COUNT(*) >= ? ORDER BY n_fills DESC"
        params.append(min_fills)
        try:
            cur = self._conn.execute(q, params)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            raise CacheError(f"tape wallet_fill_counts failed: {exc}") from exc

    def polls(
        self, *, lane: str | None = None, lo_ts: int | None = None, hi_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read l2_polls rows (previously write-only). Poll cadence per lane over
        a window is the collector's liveness/coverage record."""
        q = "SELECT * FROM l2_polls WHERE 1=1"
        params: list[Any] = []
        if lane is not None:
            q += " AND lane = ?"; params.append(lane)
        if lo_ts is not None:
            q += " AND invoked_ts >= ?"; params.append(lo_ts)
        if hi_ts is not None:
            q += " AND invoked_ts <= ?"; params.append(hi_ts)
        q += " ORDER BY invoked_ts"
        try:
            cur = self._conn.execute(q, params)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            raise CacheError(f"tape polls failed: {exc}") from exc

    def fill_histogram(
        self, *, bucket_seconds: int, lo_ts: int | None = None, hi_ts: int | None = None,
    ) -> list[tuple[int, int]]:
        """(bucket_start_ts, fill_count) over parsed fills, bucketed by
        ``bucket_seconds`` — activity density over time."""
        if bucket_seconds <= 0:
            raise CacheError("fill_histogram: bucket_seconds must be > 0")
        q = ("SELECT (timestamp / ?) * ? AS bucket, COUNT(*) FROM l2_trades"
             " WHERE parse_ok = 1 AND timestamp IS NOT NULL")
        params: list[Any] = [bucket_seconds, bucket_seconds]
        if lo_ts is not None:
            q += " AND timestamp >= ?"; params.append(lo_ts)
        if hi_ts is not None:
            q += " AND timestamp <= ?"; params.append(hi_ts)
        q += " GROUP BY bucket ORDER BY bucket"
        try:
            return [(int(r[0]), int(r[1])) for r in self._conn.execute(q, params).fetchall()]
        except sqlite3.Error as exc:
            raise CacheError(f"tape fill_histogram failed: {exc}") from exc

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

    def record_resolution(self, condition_id: str, *, resolution: str, now_ts: int) -> bool:
        """Persist a tracked market's RESOLUTION outcome (raw gamma outcome JSON —
        who won) and stamp close_seen_ts if not already set. Written by the
        collector's closed-market sweep, which catches whole-event closures that
        the open (closed=false) enumeration never sees (adversarial scout
        2026-07-20). Returns True if a row was updated. The confirmation pass
        reads the winning side from ``resolution`` (Rule 1: raw values, parsed at
        analysis time) instead of re-fetching thousands of markets."""
        cur = self._conn.execute(
            "UPDATE l2_markets SET resolution=?,"
            " close_seen_ts = CASE WHEN close_seen_ts = 0 THEN ? ELSE close_seen_ts END"
            " WHERE condition_id=?",
            (resolution, now_ts, condition_id),
        )
        self._conn.commit()
        return bool(cur.rowcount)

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
                   "skipped_records", "unparsed_records", "presumed_records",
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

    def strays_pending_adjudication(self, *, limit: int | None = None) -> list[tuple[str, int]]:
        """Unresolved strays to adjudicate this pass, as (condition_id, attempts).
        MOST-attempted first (then oldest): a stray closest to the give-up
        threshold is processed first so it reaches abandonment and leaves the
        pool, which then admits newer strays. Ordering least-attempted-first
        instead let a steady influx of fresh strays perpetually outrank the ones
        marching toward give-up, so the table grew without bound even though the
        per-run ``limit`` kept pass duration flat (observed 2026-07-20). A real
        target market is still adopted by the enumeration tag-page walk, so
        de-prioritizing brand-new strays here costs no coverage."""
        q = ("SELECT condition_id, attempts FROM l2_strays WHERE resolved = 0"
             " ORDER BY attempts DESC, first_seen_ts ASC")
        params: tuple[Any, ...] = ()
        if limit is not None:
            q += " LIMIT ?"
            params = (limit,)
        return [(r[0], r[1]) for r in self._conn.execute(q, params).fetchall()]

    def bump_stray_attempt(self, condition_id: str) -> None:
        self._conn.execute(
            "UPDATE l2_strays SET attempts = attempts + 1 WHERE condition_id = ?",
            (condition_id,),
        )
        self._conn.commit()

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
