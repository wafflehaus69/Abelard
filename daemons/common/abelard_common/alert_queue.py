"""abelard_queue — durable alert queue between daemons and Abelard.

GATE 2 decision (2026-07-14): daemons never dispatch externally. They
enqueue structured JSON alerts here; Abelard consumes the queue, owns
the materiality decision (push vs suppress), and is the only component
that talks to an external channel. This module is the shared primitive
both sides import.

Architectural rules encoded here:

  - **Enqueue is the commit point.** ``enqueue()`` commits the row
    before returning; a daemon that got a QueueItem back knows the
    alert is durable regardless of what happens downstream.
  - **Append-only.** Items are never deleted and ``payload_json`` is
    never rewritten. Interpretation and dispatch mutate *status*
    columns only.
  - **Status machine:** ``pending -> interpreted -> dispatched`` for
    pushed items; ``pending -> suppressed`` for suppressed ones (the
    interpret step and the suppress verdict collapse into one
    transition — there is no dispatch leg to wait for). Illegal
    transitions raise ``QueueError`` — fail loud, never mask.
  - **Idempotent enqueue.** ``dedupe_key`` is UNIQUE; re-enqueueing an
    existing key returns the stored item untouched (created=False).
    A daemon retry loop can never double-insert.
  - **No double-push.** Dispatch is claim-based: ``claim_for_dispatch``
    stamps ``claimed_at_unix`` before any network I/O. A *known*
    failure clears the claim (safe to retry). A claim with no recorded
    failure and no dispatched stamp is a crash-window item
    ("unconfirmed") — it is never auto-retried; ``unconfirmed()``
    surfaces it for a manual ``reset_claim``.
  - **Journal every decision.** ``mark_interpreted`` writes a
    ``decision_journal`` row in the same transaction as the status
    flip — the calibration record Mando reviews.

Common-library discipline: the constructor takes an explicit
``db_path`` (no env resolution here — call sites own that), and the
module has no I/O beyond SQLite. Stdlib only.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

SCHEMA_VERSION = 1

VALID_STATUSES = ("pending", "interpreted", "dispatched", "suppressed")
VALID_DECISIONS = ("push", "suppress")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_unix     INTEGER NOT NULL,
    source              TEXT    NOT NULL,
    kind                TEXT    NOT NULL,
    topic_key           TEXT    NOT NULL,
    dedupe_key          TEXT    NOT NULL UNIQUE,
    payload_json        TEXT    NOT NULL,
    status              TEXT    NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','interpreted','dispatched','suppressed')),
    decision            TEXT
        CHECK (decision IS NULL OR decision IN ('push','suppress')),
    decided_by          TEXT,
    decision_reason     TEXT,
    interpreted_at_unix INTEGER,
    claimed_at_unix     INTEGER,
    dispatch_attempts   INTEGER NOT NULL DEFAULT 0,
    last_dispatch_error TEXT,
    dispatched_at_unix  INTEGER,
    dispatch_channel    TEXT
);

CREATE TABLE IF NOT EXISTS decision_journal (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_unix    INTEGER NOT NULL,
    item_id    INTEGER NOT NULL REFERENCES queue_items(id),
    decision   TEXT    NOT NULL,
    decided_by TEXT    NOT NULL,
    reason     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS queue_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_items_status
    ON queue_items (status);
CREATE INDEX IF NOT EXISTS idx_queue_items_cooldown
    ON queue_items (source, kind, topic_key, interpreted_at_unix);
"""


class QueueError(RuntimeError):
    """Illegal transition, malformed input, or storage failure. Fail loud."""


@dataclass(frozen=True)
class QueueItem:
    """One alert as stored. Immutable snapshot of a row."""

    id: int
    created_at_unix: int
    source: str
    kind: str
    topic_key: str
    dedupe_key: str
    payload: dict[str, Any]
    status: str
    decision: Optional[str]
    decided_by: Optional[str]
    decision_reason: Optional[str]
    interpreted_at_unix: Optional[int]
    claimed_at_unix: Optional[int]
    dispatch_attempts: int
    last_dispatch_error: Optional[str]
    dispatched_at_unix: Optional[int]
    dispatch_channel: Optional[str]


@dataclass(frozen=True)
class JournalEntry:
    """One push/suppress decision as journaled — the calibration record."""

    id: int
    ts_unix: int
    item_id: int
    decision: str
    decided_by: str
    reason: str


def _row_to_item(row: sqlite3.Row) -> QueueItem:
    return QueueItem(
        id=row["id"],
        created_at_unix=row["created_at_unix"],
        source=row["source"],
        kind=row["kind"],
        topic_key=row["topic_key"],
        dedupe_key=row["dedupe_key"],
        payload=json.loads(row["payload_json"]),
        status=row["status"],
        decision=row["decision"],
        decided_by=row["decided_by"],
        decision_reason=row["decision_reason"],
        interpreted_at_unix=row["interpreted_at_unix"],
        claimed_at_unix=row["claimed_at_unix"],
        dispatch_attempts=row["dispatch_attempts"],
        last_dispatch_error=row["last_dispatch_error"],
        dispatched_at_unix=row["dispatched_at_unix"],
        dispatch_channel=row["dispatch_channel"],
    )


class AlertQueue:
    """SQLite-backed durable alert queue. WAL mode — daemons enqueue
    while the consumer reads, cross-process, without lock contention.

    ``now_fn`` is injectable for tests; production uses wall-clock.
    """

    def __init__(
        self,
        db_path: Path | str,
        *,
        now_fn: Callable[[], int] | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._now = now_fn or (lambda: int(time.time()))
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, timeout=10.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._conn:
            self._conn.executescript(_SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO queue_meta (key, value) VALUES "
                "('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "AlertQueue":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ---------- enqueue (daemon side) ----------

    def enqueue(
        self,
        *,
        source: str,
        kind: str,
        topic_key: str,
        dedupe_key: str,
        payload: Mapping[str, Any],
    ) -> tuple[QueueItem, bool]:
        """Persist one alert. Returns (item, created).

        Commit point: when this returns, the row is durable. Idempotent
        on ``dedupe_key`` — an existing key returns the stored item
        with created=False and does NOT overwrite its payload.
        """
        for label, value in (
            ("source", source), ("kind", kind),
            ("topic_key", topic_key), ("dedupe_key", dedupe_key),
        ):
            if not isinstance(value, str) or not value.strip():
                raise QueueError(f"enqueue: {label} must be a non-empty string")
        try:
            payload_json = json.dumps(dict(payload), ensure_ascii=False,
                                      sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise QueueError(f"enqueue: payload not JSON-serializable: {exc}") from exc

        with self._conn:
            cur = self._conn.execute(
                "INSERT INTO queue_items "
                "(created_at_unix, source, kind, topic_key, dedupe_key, payload_json) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (dedupe_key) DO NOTHING",
                (self._now(), source, kind, topic_key, dedupe_key, payload_json),
            )
            created = cur.rowcount == 1
        item = self._by_dedupe_key(dedupe_key)
        if item is None:  # pragma: no cover — UNIQUE row must exist post-insert
            raise QueueError(f"enqueue: row vanished for dedupe_key {dedupe_key!r}")
        return item, created

    # ---------- interpretation (Abelard side) ----------

    def mark_interpreted(
        self,
        item_id: int,
        *,
        decision: str,
        decided_by: str,
        reason: str,
    ) -> QueueItem:
        """Record the materiality verdict. Legal only from ``pending``.

        decision='push'     -> status 'interpreted' (awaits dispatch)
        decision='suppress' -> status 'suppressed'  (terminal)

        The decision_journal row is written in the same transaction —
        a verdict can never exist without its calibration record.
        """
        if decision not in VALID_DECISIONS:
            raise QueueError(f"mark_interpreted: decision must be one of "
                             f"{VALID_DECISIONS}, got {decision!r}")
        if not decided_by.strip() or not reason.strip():
            raise QueueError("mark_interpreted: decided_by and reason are required")
        new_status = "interpreted" if decision == "push" else "suppressed"
        now = self._now()
        with self._conn:
            cur = self._conn.execute(
                "UPDATE queue_items SET status = ?, decision = ?, decided_by = ?, "
                "decision_reason = ?, interpreted_at_unix = ? "
                "WHERE id = ? AND status = 'pending'",
                (new_status, decision, decided_by, reason, now, item_id),
            )
            if cur.rowcount != 1:
                current = self._status_of(item_id)
                raise QueueError(
                    f"mark_interpreted: item {item_id} not in 'pending' "
                    f"(current: {current})"
                )
            self._conn.execute(
                "INSERT INTO decision_journal (ts_unix, item_id, decision, "
                "decided_by, reason) VALUES (?, ?, ?, ?, ?)",
                (now, item_id, decision, decided_by, reason),
            )
        return self._require(item_id)

    # ---------- dispatch (Abelard side) ----------

    def claim_for_dispatch(self, item_id: int) -> bool:
        """Stamp the claim BEFORE any network I/O. Returns False when the
        item is not claimable (wrong state, or already claimed — which
        includes crash-window unconfirmed items)."""
        with self._conn:
            cur = self._conn.execute(
                "UPDATE queue_items SET claimed_at_unix = ?, "
                "dispatch_attempts = dispatch_attempts + 1 "
                "WHERE id = ? AND status = 'interpreted' AND decision = 'push' "
                "AND claimed_at_unix IS NULL",
                (self._now(), item_id),
            )
            return cur.rowcount == 1

    def mark_dispatched(self, item_id: int, *, channel: str) -> QueueItem:
        """Confirm delivery. Legal only from a claimed 'interpreted' item."""
        if not channel.strip():
            raise QueueError("mark_dispatched: channel is required")
        with self._conn:
            cur = self._conn.execute(
                "UPDATE queue_items SET status = 'dispatched', "
                "dispatched_at_unix = ?, dispatch_channel = ? "
                "WHERE id = ? AND status = 'interpreted' "
                "AND claimed_at_unix IS NOT NULL",
                (self._now(), channel, item_id),
            )
            if cur.rowcount != 1:
                current = self._status_of(item_id)
                raise QueueError(
                    f"mark_dispatched: item {item_id} not a claimed "
                    f"'interpreted' item (current: {current})"
                )
        return self._require(item_id)

    def record_dispatch_failure(self, item_id: int, *, error: str) -> QueueItem:
        """A KNOWN transport failure — clear the claim (safe to retry
        later) and keep the error for the surface report."""
        if not error.strip():
            raise QueueError("record_dispatch_failure: error text is required")
        with self._conn:
            cur = self._conn.execute(
                "UPDATE queue_items SET claimed_at_unix = NULL, "
                "last_dispatch_error = ? "
                "WHERE id = ? AND status = 'interpreted' "
                "AND claimed_at_unix IS NOT NULL",
                (error, item_id),
            )
            if cur.rowcount != 1:
                current = self._status_of(item_id)
                raise QueueError(
                    f"record_dispatch_failure: item {item_id} not a claimed "
                    f"'interpreted' item (current: {current})"
                )
        return self._require(item_id)

    def reset_claim(self, item_id: int) -> QueueItem:
        """MANUAL operator action for a crash-window (unconfirmed) item —
        clears the claim so dispatch may retry. Only meaningful after a
        human verified the message did not actually deliver."""
        with self._conn:
            cur = self._conn.execute(
                "UPDATE queue_items SET claimed_at_unix = NULL "
                "WHERE id = ? AND status = 'interpreted' "
                "AND claimed_at_unix IS NOT NULL",
                (item_id,),
            )
            if cur.rowcount != 1:
                current = self._status_of(item_id)
                raise QueueError(
                    f"reset_claim: item {item_id} not a claimed 'interpreted' "
                    f"item (current: {current})"
                )
        return self._require(item_id)

    # ---------- queries ----------

    def items(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> list[QueueItem]:
        if status is not None and status not in VALID_STATUSES:
            raise QueueError(f"items: unknown status {status!r}")
        sql = "SELECT * FROM queue_items"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY id ASC LIMIT ?"
        params.append(limit)
        return [_row_to_item(r) for r in self._conn.execute(sql, params)]

    def dispatchable(self) -> list[QueueItem]:
        """Pushed, unclaimed items — the dispatch work list."""
        rows = self._conn.execute(
            "SELECT * FROM queue_items WHERE status = 'interpreted' "
            "AND decision = 'push' AND claimed_at_unix IS NULL "
            "ORDER BY id ASC"
        )
        return [_row_to_item(r) for r in rows]

    def unconfirmed(self) -> list[QueueItem]:
        """Crash-window items: claimed, no recorded failure, never
        confirmed dispatched. NEVER auto-retried — surface these."""
        rows = self._conn.execute(
            "SELECT * FROM queue_items WHERE status = 'interpreted' "
            "AND claimed_at_unix IS NOT NULL AND last_dispatch_error IS NULL "
            "ORDER BY id ASC"
        )
        return [_row_to_item(r) for r in rows]

    def recent_push_exists(
        self,
        *,
        source: str,
        kind: str,
        topic_key: str,
        within_s: int,
        now_unix: Optional[int] = None,
    ) -> bool:
        """Cooldown probe: was a push decided for this (source, kind,
        topic_key) within the window? Used by the consumer's explicit
        cooldown rule."""
        now = self._now() if now_unix is None else now_unix
        row = self._conn.execute(
            "SELECT 1 FROM queue_items WHERE source = ? AND kind = ? "
            "AND topic_key = ? AND decision = 'push' "
            "AND interpreted_at_unix >= ? LIMIT 1",
            (source, kind, topic_key, now - within_s),
        ).fetchone()
        return row is not None

    def journal(self, *, limit: int = 100) -> list[JournalEntry]:
        rows = self._conn.execute(
            "SELECT * FROM decision_journal ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [
            JournalEntry(
                id=r["id"], ts_unix=r["ts_unix"], item_id=r["item_id"],
                decision=r["decision"], decided_by=r["decided_by"],
                reason=r["reason"],
            )
            for r in rows
        ]

    def counts(self) -> dict[str, int]:
        out = {s: 0 for s in VALID_STATUSES}
        for r in self._conn.execute(
            "SELECT status, COUNT(*) AS n FROM queue_items GROUP BY status"
        ):
            out[r["status"]] = r["n"]
        return out

    def get(self, item_id: int) -> Optional[QueueItem]:
        row = self._conn.execute(
            "SELECT * FROM queue_items WHERE id = ?", (item_id,)
        ).fetchone()
        return _row_to_item(row) if row else None

    # ---------- internals ----------

    def _by_dedupe_key(self, dedupe_key: str) -> Optional[QueueItem]:
        row = self._conn.execute(
            "SELECT * FROM queue_items WHERE dedupe_key = ?", (dedupe_key,)
        ).fetchone()
        return _row_to_item(row) if row else None

    def _status_of(self, item_id: int) -> str:
        row = self._conn.execute(
            "SELECT status, claimed_at_unix FROM queue_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        if row is None:
            return "no such item"
        claimed = "claimed" if row["claimed_at_unix"] is not None else "unclaimed"
        return f"{row['status']}/{claimed}"

    def _require(self, item_id: int) -> QueueItem:
        item = self.get(item_id)
        if item is None:  # pragma: no cover — callers just updated the row
            raise QueueError(f"item {item_id} vanished mid-transition")
        return item


__all__ = [
    "SCHEMA_VERSION",
    "VALID_DECISIONS",
    "VALID_STATUSES",
    "AlertQueue",
    "JournalEntry",
    "QueueError",
    "QueueItem",
]
