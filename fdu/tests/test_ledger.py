"""Ledger tests: append-only history, additive migration, absence semantics."""

from __future__ import annotations

import pytest

from fdu_daemon import ledger, normalize
from fdu_daemon.feed import FirmRecord


def _rec(**kw) -> FirmRecord:
    base = dict(crd="123", source_feed="sec", legal_name="EXAMPLE ADVISORS LLC",
                filing_date="2026-03-04", total_employees=8, aum_total=1_000_000,
                rgstn_type="Registered", rgstn_status="APPROVED")
    base.update(kw)
    return FirmRecord(**base)


@pytest.fixture
def conn():
    c = ledger.connect()
    yield c
    c.close()


def test_schema_applies_and_is_idempotent(conn):
    ledger.apply_schema(conn)
    ledger.apply_schema(conn)
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"firm", "firm_change", "adv_detail", "run", "schema_version"} <= tables


def test_upsert_then_reload_roundtrips(conn):
    rec = _rec(notice_states=("GA", "NY"))
    ledger.upsert_firm(conn, rec, change_key=normalize.change_key(rec),
                       now_unix=100, snapshot_date="2026-08-21", changed=True)
    back = ledger.load_firms(conn)
    assert set(back) == {"123"}
    assert back["123"].notice_states == ("GA", "NY")
    assert back["123"].total_employees == 8


def test_reloaded_record_has_same_change_key(conn):
    """Round-tripping must not itself look like a change.

    If it did, every firm would appear to move on every scan.
    """
    rec = _rec(notice_states=("MT", "GA", "HI"))
    key = normalize.change_key(rec)
    ledger.upsert_firm(conn, rec, change_key=key, now_unix=100,
                       snapshot_date="2026-08-21", changed=True)
    back = ledger.load_firms(conn)["123"]
    assert normalize.change_key(back) == key
    assert normalize.diff_fields(back, rec) == {}


def test_changes_are_append_only(conn):
    ledger.record_changes(conn, "123", {"filing_date": ("2026-03-04", "2026-08-19")},
                          now_unix=100, from_snapshot="2026-08-14",
                          to_snapshot="2026-08-21", run_id="r1")
    ledger.record_changes(conn, "123", {"filing_date": ("2026-08-19", "2026-08-20")},
                          now_unix=200, from_snapshot="2026-08-21",
                          to_snapshot="2026-08-22", run_id="r2")
    rows = conn.execute("SELECT * FROM firm_change ORDER BY observed_unix").fetchall()
    assert len(rows) == 2, "history must accumulate, never overwrite"
    assert rows[0]["run_id"] == "r1"


def test_no_delete_path_in_ledger_source():
    """There must be no DELETE against the history tables."""
    import inspect

    src = inspect.getsource(ledger)
    lowered = src.lower()
    assert "delete from firm_change" not in lowered
    assert "drop table" not in lowered


def test_absence_reason_distinguishes_era(conn):
    era = _rec(crd="900", rgstn_type="ERA", total_employees=None)
    assert era.absence_reason == "era_partial_form"
    reported = _rec(crd="901", total_employees=0)
    assert reported.absence_reason is None


def test_additive_migration_adds_new_column(conn):
    conn.execute("ALTER TABLE firm DROP COLUMN clients_hnw")
    ledger._add_missing_columns(conn, "firm", ledger._FIRM_COLUMNS)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(firm)")}
    assert "clients_hnw" in cols


def test_adv_detail_upsert_is_idempotent(conn):
    row = {"crd": "123", "fetched_unix": 1, "direct_owner_count": 2,
           "ownership_codes": "B,E", "extract_status": "ok"}
    ledger.upsert_adv_detail(conn, row)
    ledger.upsert_adv_detail(conn, {**row, "fetched_unix": 2, "direct_owner_count": 3})
    rows = conn.execute("SELECT * FROM adv_detail").fetchall()
    assert len(rows) == 1
    assert rows[0]["direct_owner_count"] == 3


def test_run_row_opens_before_work(conn):
    ledger.start_run(conn, "runX", "scan", 100)
    row = conn.execute("SELECT * FROM run WHERE run_id='runX'").fetchone()
    assert row["status"] == "running"
    assert row["llm_calls"] == 0
    assert row["llm_cost_usd"] == 0.0
    ledger.finish_run(conn, "runX", status="ok", firms_seen=10)
    row = conn.execute("SELECT * FROM run WHERE run_id='runX'").fetchone()
    assert row["status"] == "ok" and row["firms_seen"] == 10
