"""CD-FRONTIER-CLOSE F3 — the guard audit, pinned against a REAL snapshot build.

Inconsistent guards are how the 2026-09-12 defect leaked: buckets refused a
one-member latest point while the total had no floor, and the constant panel
had a floor but ended its window wherever any single member had reached. These
tests build an actual snapshot from synthetic XBRL facts — not a hand-written
dict — so "every published series declares its guard" is checked against what
the code really publishes.
"""
from datetime import date, datetime, timezone

import pytest

from capex_daemon import guards, snapshot, trend, universe
from capex_daemon.facts_api import ApiFact

CAPEX = "PaymentsToAcquirePropertyPlantAndEquipment"
QUARTERS = [(y, q) for y in (2023, 2024, 2025, 2026) for q in (1, 2, 3, 4)]
THROUGH_Q2 = QUARTERS[:QUARTERS.index((2026, 2)) + 1]
NOW = int(datetime(2026, 9, 21, tzinfo=timezone.utc).timestamp())

# ORCL is ~10% of the dollars, as live.
HYPERS = {"0000000001": ("AMZN", 40.0), "0000000002": ("GOOGL", 30.0),
          "0000000003": ("MSFT", 28.0), "0000000004": ("META", 22.0),
          "0000000005": ("ORCL", 13.0)}


def _q_bounds(y, q):
    start = date(y, 3 * q - 2, 1)
    end = {1: date(y, 3, 31), 2: date(y, 6, 30), 3: date(y, 9, 30), 4: date(y, 12, 31)}[q]
    return start.isoformat(), end.isoformat(), (end - start).days


def _facts(base, quarters):
    out = []
    for i, (y, q) in enumerate(quarters):
        s, e, d = _q_bounds(y, q)
        out.append(ApiFact(concept=CAPEX, taxonomy="us-gaap", unit="USD",
                           value=base * 1e9 * (1 + 0.05 * i), period_start=s,
                           period_end=e, duration_days=d, form="10-Q", filed=e,
                           frame=None))
    return out


def _build(orcl_ahead=True, only=None):
    roster, indexed = {}, {}
    for cik, (tick, base) in HYPERS.items():
        if only and tick not in only:
            continue
        roster[cik] = universe.Entity(cik, tick, "hyperscaler", "", "")
        qs = THROUGH_Q2 + ([(2026, 3)] if (orcl_ahead and tick == "ORCL") else [])
        indexed[cik] = {CAPEX: _facts(base, qs)}
    return snapshot.build(roster, indexed, now_unix=NOW)


@pytest.fixture(scope="module")
def snap():
    return _build()


# --- the audit is complete, structurally ------------------------------------

def test_every_published_path_declares_its_guard(snap):
    """A new series cannot ship without saying what stands on it."""
    assert guards.undeclared(snap) == []


def test_every_declared_guard_is_a_floor_a_reason_or_metadata():
    for path, g in guards.GUARDS.items():
        if g["kind"] == guards.META:
            continue
        has_floor = isinstance(g.get("floor"), int) and g.get("enforced")
        has_reason = bool(g.get("no_floor_because"))
        assert has_floor or has_reason, path
        assert not (has_floor and has_reason), path     # exactly one, never both


def test_every_matched_sum_carries_the_same_floor_and_the_frontier_gate():
    """The asymmetry that leaked: buckets had 2, the total had none."""
    sums = {p: g for p, g in guards.GUARDS.items() if g["kind"] == guards.MATCHED_SUM}
    assert set(sums) == {"total", "buckets.*", "panel.issuance_ttm", "suppliers.combined"}
    assert {g["floor"] for g in sums.values()} == {trend.MIN_BUCKET_MEMBERS}
    assert all(g["frontier"] for g in sums.values())


def test_every_constant_panel_is_frontier_gated_too():
    consts = [g for g in guards.GUARDS.values() if g["kind"] == guards.CONSTANT_LEVEL]
    assert consts and all(g["frontier"] and g["floor"] == trend.MIN_BUCKET_MEMBERS
                          for g in consts)


# --- the live defect, end to end through a real build -----------------------

def test_the_real_build_holds_orcls_lone_quarter(snap):
    t = snap["total"]
    assert t["latest_quarter"] == "2026Q2" and t["member_count"] == 5
    assert [r["q"] for r in t["partial_frontier"]] == ["2026Q3"]
    assert snap["buckets"]["hyperscaler"]["state"] != trend.STATE_INSUFFICIENT_MEMBERSHIP


def test_the_constant_panel_is_not_emptied_by_one_member_ahead(snap):
    """Live since 2026-09-12: every candidate window had to end at ORCL's lone
    2026Q3, common membership was one name, and the front-page composite read
    "no constant-membership panel"."""
    cap = snap["panel"]["constant"]["capex"]
    assert cap["members"], "constant panel came back empty"
    assert cap["last_quarter"] == "2026Q2"
    assert "ORCL" in cap["members"]


def test_the_constant_panel_still_reaches_a_fully_reported_quarter():
    cap = _build(orcl_ahead=False)["panel"]["constant"]["capex"]
    assert cap["members"] and cap["last_quarter"] == "2026Q2"


# --- the floors themselves ----------------------------------------------------

def test_a_one_member_total_is_refused_not_published():
    """Buckets always refused this; the total did not, until F3."""
    one = _build(orcl_ahead=False, only={"AMZN"})
    assert one["total"]["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP
    assert one["total"]["floor"]["status"] == trend.STATE_INSUFFICIENT_MEMBERSHIP


def test_two_members_clear_the_floor():
    two = _build(orcl_ahead=False, only={"AMZN", "GOOGL"})
    assert two["total"]["floor"]["status"] == "OK"
    assert two["total"]["state"] != trend.STATE_INSUFFICIENT_MEMBERSHIP


def test_the_floor_reads_the_latest_point_only():
    """History keeps its early one-name quarters, each labelled with its count."""
    v = trend.membership_floor({"2010Q1": ["MSFT"], "2026Q2": ["A", "B", "C"]})
    assert v["status"] == "OK" and v["members"] == 3 and v["quarter"] == "2026Q2"


def test_an_empty_series_is_below_the_floor():
    assert trend.membership_floor({})["status"] == trend.STATE_INSUFFICIENT_MEMBERSHIP


def test_the_thesis_withholds_a_one_issuer_credit_direction():
    snap = _build(orcl_ahead=False)
    snap["panel"]["issuance_ttm"] = [{"q": "2026Q1", "value": 1.0, "members": 1},
                                     {"q": "2026Q2", "value": 9.0, "members": 1}]
    snap["panel"]["issuance_floor"] = {"status": trend.STATE_INSUFFICIENT_MEMBERSHIP,
                                       "members": 1, "min_members": 2}
    line = snapshot.thesis_line(snap)
    assert "credit issuance is withheld" in line
    assert "credit issuance is rising" not in line


# --- the table the order asks for ---------------------------------------------

def test_the_audit_table_lists_every_non_metadata_guard():
    rows = guards.table()
    paths = {r[0] for r in rows}
    assert "total" in paths and "panel.breadth_series" in paths
    assert all(r[4] for r in rows)          # every row states its enforcement or reason
