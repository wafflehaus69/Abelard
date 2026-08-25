"""SQLite ledger for FDU.

Storage discipline, because it was an explicit instruction: **no source
documents are retained.** Per-firm ADV PDFs average 1.98 MB and the corpus is
~49 GB; they are fetched, parsed in memory, reduced to the handful of facts
below, and discarded. What lands on disk is structured rows measured in tens of
megabytes for the whole corpus.

Three shapes:
  ``firm``        current state, one row per CRD, upserted
  ``firm_change`` APPEND-ONLY log of what moved and when
  ``adv_detail``  facts extracted from the per-firm document

``firm_change`` is append-only for the same reason scout's verdict table is: the
publisher keeps ~8 days of feeds, so our change log is the ONLY history that
will ever exist. A row that can be overwritten is not a record.

I-3 note, load-bearing: Schedule A lists named individuals. ``adv_detail``
stores ownership STRUCTURE -- counts and the ownership-percentage code multiset
-- and no names, no addresses, no per-person rows. Firm-level lead generation
does not require identifying people, and the moment it stores them it has
become the per-person dossier the invariant forbids.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import fields as dataclass_fields
from pathlib import Path

from . import config
from .errors import LedgerError
from .feed import FirmRecord

SCHEMA_VERSION = 1

_FIRM_COLUMNS: dict[str, str] = {
    "crd": "TEXT PRIMARY KEY",
    "source_feed": "TEXT NOT NULL",
    "legal_name": "TEXT",
    "business_name": "TEXT",
    "sec_number": "TEXT",
    "umbrella": "TEXT",
    "rgstn_type": "TEXT",
    "rgstn_status": "TEXT",
    "rgstn_date": "TEXT",
    "filing_date": "TEXT",
    "form_version": "TEXT",
    "city": "TEXT",
    "state": "TEXT",
    "country": "TEXT",
    "postal_code": "TEXT",
    "total_employees": "INTEGER",
    "advisory_employees": "INTEGER",
    "aum_total": "INTEGER",
    "aum_discretionary": "INTEGER",
    "aum_non_discretionary": "INTEGER",
    "accounts_total": "INTEGER",
    "clients_hnw": "INTEGER",
    "disciplinary_flag": "TEXT",
    "notice_states": "TEXT",
    "absence_reason": "TEXT",
    "change_key": "TEXT NOT NULL",
    "first_seen_unix": "INTEGER NOT NULL",
    "last_seen_unix": "INTEGER NOT NULL",
    "last_changed_unix": "INTEGER",
    "snapshot_date": "TEXT",
}

_CHANGE_COLUMNS: dict[str, str] = {
    "change_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "crd": "TEXT NOT NULL",
    "observed_unix": "INTEGER NOT NULL",
    "from_snapshot": "TEXT",
    "to_snapshot": "TEXT",
    "field": "TEXT NOT NULL",
    "old_value": "TEXT",
    "new_value": "TEXT",
    "run_id": "TEXT NOT NULL",
}

_ADV_COLUMNS: dict[str, str] = {
    "crd": "TEXT PRIMARY KEY",
    "fetched_unix": "INTEGER NOT NULL",
    "doc_bytes": "INTEGER",
    "doc_pages": "INTEGER",
    "form_filed_at": "TEXT",
    "amendment_type": "TEXT",
    # Successions -- Schedule D Section 4. The Item 4 checkbox itself is NOT
    # recoverable from the document text (no AcroForm fields, checkbox state is
    # drawn not written), so presence of Section 4 content is the signal.
    "section4_filed": "INTEGER",
    "succession_detail": "TEXT",
    "succession_count": "INTEGER",
    "succession_acquired_names": "TEXT",
    "succession_acquired_crds": "TEXT",
    "succession_is_self": "INTEGER",
    # Ownership STRUCTURE only -- never names. See module docstring.
    "direct_owner_count": "INTEGER",
    "indirect_owner_count": "INTEGER",
    "ownership_codes": "TEXT",
    "max_ownership_code": "TEXT",
    "control_person_count": "INTEGER",
    "extract_status": "TEXT NOT NULL",
    "extract_note": "TEXT",
}

#: The monthly CSV view. Kept in its OWN table rather than merged into `firm`
#: because the two sources have different cadences and different authority: the
#: daily feed is the change detector, this is a monthly enrichment. Merging them
#: would make it impossible to say which source a value came from, which is the
#: mistake E6 is about -- know which layer you are reading.
_MONTHLY_COLUMNS: dict[str, str] = {
    "crd": "TEXT PRIMARY KEY",
    "observed_unix": "INTEGER NOT NULL",
    "source_file": "TEXT",
    "acquired_name": "TEXT",
    "acquired_sec_no": "TEXT",
    "acquired_crd": "TEXT",
    "acquired_count": "TEXT",
    "is_self_succession": "INTEGER",
    "latest_filing": "TEXT",
    "sec_status": "TEXT",
    "sec_status_date": "TEXT",
    "relying_advisers": "TEXT",
    "control_related": "TEXT",
    "common_control": "TEXT",
}

#: B1 snapshot store. One row per (crd, snapshot_date) is far too wide for 196
#: snapshots x ~14k firms, so this holds snapshot METADATA and the transition
#: events carry the movement. Raw ZIPs are preserved on disk separately.
_SNAPSHOT_COLUMNS: dict[str, str] = {
    "snapshot_date": "TEXT",
    "source_file": "TEXT PRIMARY KEY",
    "era": "TEXT",
    "n_columns": "INTEGER",
    "n_rows": "INTEGER",
    "skipped_rows": "INTEGER",
    "absent_fields": "TEXT",
    "ingested_unix": "INTEGER NOT NULL",
}

#: B2 transition events. APPEND-ONLY, and unified with the live change log by a
#: provenance column. This is the reconstructed history; nothing overwrites it.
_TRANSITION_COLUMNS: dict[str, str] = {
    "event_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "crd": "TEXT NOT NULL",
    "event_type": "TEXT NOT NULL",
    "field": "TEXT",
    "old_value": "TEXT",
    "new_value": "TEXT",
    "snapshot_from": "TEXT",
    "snapshot_to": "TEXT",
    "interval_months": "INTEGER",
    "spans_gap": "INTEGER",
    "source_file": "TEXT",
    "provenance": "TEXT NOT NULL",
}

_RUN_COLUMNS: dict[str, str] = {
    "run_id": "TEXT PRIMARY KEY",
    "started_unix": "INTEGER NOT NULL",
    "kind": "TEXT NOT NULL",
    "snapshot_date": "TEXT",
    "firms_seen": "INTEGER",
    "firms_changed": "INTEGER",
    "firms_added": "INTEGER",
    "firms_removed": "INTEGER",
    "docs_fetched": "INTEGER",
    "fetch_calls": "INTEGER",
    "fetch_bytes": "INTEGER",
    "llm_calls": "INTEGER NOT NULL DEFAULT 0",
    "llm_cost_usd": "REAL NOT NULL DEFAULT 0.0",
    "status": "TEXT",
    "note": "TEXT",
}


def _ddl(table: str, cols: dict[str, str]) -> str:
    body = ",\n  ".join(f"{name} {decl}" for name, decl in cols.items())
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {body}\n)"


def connect(path: Path | None = None) -> sqlite3.Connection:
    p = path or config.db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    apply_schema(conn)
    return conn


def _add_missing_columns(conn: sqlite3.Connection, table: str, cols: dict[str, str]) -> None:
    """Additive migration.

    ``CREATE TABLE IF NOT EXISTS`` is a no-op on an existing table, so adding a
    name to the column dict without this leaves live databases missing it and
    every write against that column fails at runtime rather than at deploy.
    """
    have = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if not have:
        return
    for name, decl in cols.items():
        if name in have:
            continue
        if "PRIMARY KEY" in decl.upper() or "AUTOINCREMENT" in decl.upper():
            continue
        safe = decl.replace(" NOT NULL", "")
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {safe}")


def apply_schema(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
        for table, cols in (
            ("firm", _FIRM_COLUMNS),
            ("firm_change", _CHANGE_COLUMNS),
            ("adv_detail", _ADV_COLUMNS),
            ("monthly", _MONTHLY_COLUMNS),
            ("snapshot", _SNAPSHOT_COLUMNS),
            ("transition_events", _TRANSITION_COLUMNS),
            ("run", _RUN_COLUMNS),
        ):
            conn.execute(_ddl(table, cols))
            _add_missing_columns(conn, table, cols)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_crd ON firm_change(crd, observed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_field ON firm_change(field, observed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_firm_changed ON firm(last_changed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_firm_state ON firm(state, rgstn_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_monthly_succ ON monthly(is_self_succession)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tr_crd ON transition_events(crd, snapshot_to)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tr_type ON transition_events(event_type, snapshot_to)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tr_prov ON transition_events(provenance)")
        cur = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        if cur is None:
            conn.execute("INSERT INTO schema_version(version) VALUES (?)", (SCHEMA_VERSION,))
    except sqlite3.Error as exc:
        raise LedgerError(f"schema application failed: {exc}") from exc


# --------------------------------------------------------------------------
# Writes
# --------------------------------------------------------------------------


#: Columns that are DERIVED, not stored state. They are written for query
#: convenience and must never be assigned back onto a record on reload --
#: ``absence_reason`` is a property with no setter, and re-assigning
#: bookkeeping columns would make a round-trip look like a change.
_DERIVED_COLUMNS = frozenset(
    {"absence_reason", "change_key", "first_seen_unix", "last_seen_unix",
     "last_changed_unix", "snapshot_date"}
)


def load_firms(conn: sqlite3.Connection) -> dict[str, FirmRecord]:
    """Rehydrate current firm state for diffing against a fresh snapshot."""
    settable = {f.name for f in dataclass_fields(FirmRecord)} - _DERIVED_COLUMNS
    out: dict[str, FirmRecord] = {}
    for row in conn.execute("SELECT * FROM firm"):
        rec = FirmRecord(crd=row["crd"], source_feed=row["source_feed"])
        for name in _FIRM_COLUMNS:
            if name in ("crd", "source_feed", "notice_states"):
                continue
            if name in settable:
                setattr(rec, name, row[name])
        raw = row["notice_states"]
        rec.notice_states = tuple(json.loads(raw)) if raw else ()
        out[rec.crd] = rec
    return out


def upsert_firm(conn: sqlite3.Connection, rec: FirmRecord, *, change_key: str,
                now_unix: int, snapshot_date: str, changed: bool) -> None:
    payload = {
        "crd": rec.crd,
        "source_feed": rec.source_feed,
        "notice_states": json.dumps(list(rec.notice_states)),
        "absence_reason": rec.absence_reason,
        "change_key": change_key,
        "last_seen_unix": now_unix,
        "snapshot_date": snapshot_date,
    }
    for name in _FIRM_COLUMNS:
        if name in payload or name in ("first_seen_unix", "last_changed_unix"):
            continue
        if hasattr(rec, name):
            payload[name] = getattr(rec, name)
    cols = list(payload) + ["first_seen_unix", "last_changed_unix"]
    vals = [payload[c] for c in payload] + [now_unix, now_unix if changed else None]
    updates = ", ".join(f"{c}=excluded.{c}" for c in payload if c != "crd")
    extra = ", last_changed_unix=excluded.last_changed_unix" if changed else ""
    conn.execute(
        f"INSERT INTO firm ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(crd) DO UPDATE SET {updates}{extra}",
        vals,
    )


def record_changes(conn: sqlite3.Connection, crd: str, moved: dict, *, now_unix: int,
                   from_snapshot: str | None, to_snapshot: str, run_id: str) -> None:
    """Append the movement. Never updates; never deletes."""
    conn.executemany(
        "INSERT INTO firm_change (crd, observed_unix, from_snapshot, to_snapshot, field, "
        "old_value, new_value, run_id) VALUES (?,?,?,?,?,?,?,?)",
        [
            (crd, now_unix, from_snapshot, to_snapshot, field,
             json.dumps(old, default=str), json.dumps(new, default=str), run_id)
            for field, (old, new) in sorted(moved.items())
        ],
    )


def start_run(conn: sqlite3.Connection, run_id: str, kind: str, now_unix: int) -> None:
    """Open the run row BEFORE any work.

    Cost telemetry is persisted ahead of results so a crash mid-run still
    leaves a record that the work (and any spend) happened.
    """
    conn.execute(
        "INSERT OR REPLACE INTO run (run_id, started_unix, kind, status) VALUES (?,?,?,?)",
        (run_id, now_unix, kind, "running"),
    )


def finish_run(conn: sqlite3.Connection, run_id: str, **fields) -> None:
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE run SET {sets} WHERE run_id=?", [*fields.values(), run_id])


def upsert_adv_detail(conn: sqlite3.Connection, detail: dict) -> None:
    cols = [c for c in _ADV_COLUMNS if c in detail]
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "crd")
    conn.execute(
        f"INSERT INTO adv_detail ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(crd) DO UPDATE SET {updates}",
        [detail[c] for c in cols],
    )


def upsert_monthly(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Upsert monthly-CSV rows. Returns the count written."""
    if not rows:
        return 0
    cols = [c for c in _MONTHLY_COLUMNS if c in rows[0]]
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "crd")
    conn.executemany(
        f"INSERT INTO monthly ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(crd) DO UPDATE SET {updates}",
        [[r.get(c) for c in cols] for r in rows],
    )
    return len(rows)


def record_snapshot(conn: sqlite3.Connection, meta: dict) -> None:
    cols = [c for c in _SNAPSHOT_COLUMNS if c in meta]
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "source_file")
    conn.execute(
        f"INSERT INTO snapshot ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
        f"ON CONFLICT(source_file) DO UPDATE SET {updates}",
        [meta[c] for c in cols],
    )


def append_transitions(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Append transition events. Never updates, never deletes.

    The archive keeps ~8 days of live feeds and the published monthly files can
    be re-uploaded, so this table is the only durable record of what moved.
    """
    if not rows:
        return 0
    cols = list(_TRANSITION_COLUMNS)
    cols.remove("event_id")
    conn.executemany(
        f"INSERT INTO transition_events ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' * len(cols))})",
        [[r.get(c) for c in cols] for r in rows],
    )
    return len(rows)


def ingested_files(conn: sqlite3.Connection) -> set:
    return {r["source_file"] for r in conn.execute("SELECT source_file FROM snapshot")}
