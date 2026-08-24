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
            ("run", _RUN_COLUMNS),
        ):
            conn.execute(_ddl(table, cols))
            _add_missing_columns(conn, table, cols)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_crd ON firm_change(crd, observed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_field ON firm_change(field, observed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_firm_changed ON firm(last_changed_unix)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_firm_state ON firm(state, rgstn_status)")
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
