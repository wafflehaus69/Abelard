"""The aggregate frontier: an aggregate's newest point is the newest quarter its
MEMBERSHIP has reached, not the newest quarter any member reached.

Measured live 2026-09-21. Oracle's fiscal quarter ended 2026-08-31, so it aligned
to calendar 2026Q3 roughly six weeks before the calendar-year filers. Every
matched sum then published 2026Q3 as its latest point with one member:

    total panel   $614.18B, 22 members  ->  $75.66B, 1 member ("and falling")
    hyperscaler   INSUFFICIENT-MEMBERSHIP — the whole bucket went dark
    cross-check   367.2%, NVDA over ORCL alone
    composition   "2026Q3 AMZN, GOOGL, META, MSFT left"
    A3            NVDA's genuine one-quarter-ahead read silenced

It stood nine days. Ruled by Mando 2026-09-21: gate the frontier on the
already-ratified COVERAGE_FLOOR.
"""
from datetime import date

import pytest

from capex_daemon import snapshot, trend

# Roughly the live shares: ORCL is ~9.8% of hyperscaler dollars.
BASES = {"AMZN": 40.0, "GOOGL": 30.0, "MSFT": 28.0, "META": 22.0, "ORCL": 13.0}
QUARTERS = ["{}Q{}".format(y, q) for y in (2023, 2024, 2025, 2026) for q in (1, 2, 3, 4)]
THROUGH_Q2 = QUARTERS[:QUARTERS.index("2026Q2") + 1]

# ORCL reached calendar 2026Q3 before the quarter had even ended.
AS_OF = date(2026, 9, 21)


def _members(extra=None, through=THROUGH_Q2):
    m = {}
    for t, base in BASES.items():
        m[t] = {q: base * (1 + 0.05 * i) for i, q in enumerate(through)}
    for t, qs in (extra or {}).items():
        for q in qs:
            m[t][q] = BASES[t] * 1.9
    return m


def _orcl_ahead():
    return _members(extra={"ORCL": ["2026Q3"]})


# --- the ruling ---------------------------------------------------------------

def test_the_floor_is_the_ratified_coverage_floor_not_a_copy():
    """An alias, so the two can never drift apart."""
    assert trend.FRONTIER_COVERAGE_FLOOR is trend.COVERAGE_FLOOR
    assert trend.FRONTIER_COVERAGE_FLOOR == 0.95


# --- the live defect, reproduced and fixed -------------------------------------

def test_one_member_ahead_does_not_become_the_aggregates_latest_point():
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q2"
    assert "2026Q3" not in bt.ttm and "2026Q3" not in bt.membership
    assert len(bt.membership["2026Q2"]) == 5


def test_the_held_quarter_is_published_beside_the_series_with_its_facts():
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    assert len(bt.partial) == 1
    row = bt.partial[0]
    assert row["q"] == "2026Q3" and row["prior_q"] == "2026Q2"
    assert row["members"] == ["ORCL"]
    assert row["member_count"] == 1 and row["prior_member_count"] == 5
    assert row["coverage"] == pytest.approx(13.0 / 133.0, abs=0.005)   # ~9.8%
    assert row["missing"] == ["AMZN", "GOOGL", "META", "MSFT"]
    assert row["ttm"] is not None          # what ORCL alone would have read
    assert row["publishes_by"] == "2026-12-29"


def test_no_phantom_exits_at_the_held_quarter():
    """Live, this published "2026Q3 AMZN, GOOGL, META, MSFT left" — four calendar
    filers who simply report Q3 in late October."""
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    assert not [e for e in bt.composition_events if e[1] == "2026Q3"]


def test_the_ladder_never_classifies_a_held_quarter():
    """Classification reads `.yoy`. A one-member YoY (ORCL's own +176%, live)
    must never feed the panel's state machine, so it cannot set a state or
    trigger a transition, and so it can never alert."""
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    assert "2026Q3" not in bt.yoy


def test_the_total_is_gated_the_same_way():
    tt = trend.full_panel_trend(_orcl_ahead(), as_of=AS_OF)
    assert max(tt.yoy, key=trend._cq_sort) == "2026Q2"
    assert [r["q"] for r in tt.partial] == ["2026Q3"]


# --- the gate opens when it should ----------------------------------------------

def test_a_fully_reported_quarter_publishes():
    everyone = _members(extra={t: ["2026Q3"] for t in BASES})
    bt = trend.bucket_trend("hyperscaler", everyone, as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q3"
    assert bt.partial == []


def test_an_arrival_never_lowers_coverage():
    """A new name reporting the latest quarter adds to it; it does not dilute
    the prior quarter's dollars that coverage is measured against."""
    m = _members(extra={t: ["2026Q3"] for t in BASES})
    m["NEWCO"] = {q: 5.0 for q in QUARTERS[QUARTERS.index("2025Q2"):]}
    bt = trend.bucket_trend("hyperscaler", m, as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q3"
    assert bt.partial == []


def test_ninety_five_percent_is_enough():
    """Everyone but the smallest name has reported: that is a filing season
    nearly over, not a partial quarter. ORCL is ~9.8%, so drop someone smaller."""
    m = _members(extra={t: ["2026Q3"] for t in BASES})
    m["TINY"] = {q: 1.0 for q in THROUGH_Q2}          # ~0.7% of dollars, not yet in
    bt = trend.bucket_trend("hyperscaler", m, as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q3"


# --- the escape, so a real departure cannot freeze an aggregate forever --------

def test_past_the_last_filing_deadline_the_quarter_publishes_anyway():
    """90 days after quarter end is the latest regular deadline for any periodic
    report. Past it, the missing names are behind on filing, not pending, and
    the aggregate moves on without them — the pre-existing behaviour for a
    genuine departure, which then shows as a composition event."""
    late = date(2026, 12, 29)
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=late)
    assert "2026Q3" in bt.yoy
    assert bt.partial == []


def test_one_day_before_the_deadline_it_is_still_held():
    bt = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=date(2026, 12, 28))
    assert "2026Q3" not in bt.yoy


# --- walk-back discipline ---------------------------------------------------------

def test_only_trailing_quarters_are_ever_trimmed():
    """A historical dip is a composition change, already published as one — not
    a filing season in progress. Amazon's 2017Q1 exit covered 78% of the prior
    quarter and must stay in history."""
    m = _members(extra={t: ["2026Q3"] for t in BASES})
    for q in ("2024Q3", "2024Q4"):            # AMZN absent mid-history
        del m["AMZN"][q]
    bt = trend.bucket_trend("hyperscaler", m, as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q3"
    assert bt.partial == []


def test_two_trailing_partial_quarters_are_both_held():
    """The case that broke the first cut. It compared each quarter with its
    immediate predecessor, so 2026Q4 (ORCL alone) was judged against 2026Q3
    (ORCL alone) — 100% coverage — and the one-member tail published anyway.
    A held quarter must never be the reference that legitimises its successor."""
    m = _members(extra={"ORCL": ["2026Q3", "2026Q4"]})
    bt = trend.bucket_trend("hyperscaler", m, as_of=AS_OF)
    assert max(bt.yoy, key=trend._cq_sort) == "2026Q2"
    assert [r["q"] for r in bt.partial] == ["2026Q3", "2026Q4"]
    # both measured against the last ACCEPTED quarter, not against each other
    assert [r["prior_q"] for r in bt.partial] == ["2026Q2", "2026Q2"]
    assert all(r["coverage"] < 0.15 for r in bt.partial)


def test_the_published_series_is_always_a_prefix():
    """Once a quarter is held, everything after it is held — never a hole."""
    m = _members(extra={"ORCL": ["2026Q3", "2026Q4"]})
    bt = trend.bucket_trend("hyperscaler", m, as_of=AS_OF)
    qs = sorted(bt.yoy, key=trend._cq_sort)
    idx = [trend._cq_index(q) for q in qs]
    assert idx == list(range(idx[0], idx[0] + len(idx)))


def test_the_result_depends_on_as_of_not_on_the_wall_clock():
    """The snapshot passes the scan's own timestamp, so a rebuild reproduces."""
    a = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    b = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)
    assert a.yoy == b.yoy and a.partial == b.partial


# --- every matched sum takes the same gate ------------------------------------------

def test_the_credit_leg_and_supplier_leg_are_gated_too():
    ttm, membership, partial = trend.matched_ttm_published(_orcl_ahead(), as_of=AS_OF)
    assert max(ttm, key=trend._cq_sort) == "2026Q2"
    assert [r["q"] for r in partial] == ["2026Q3"]
    assert partial[0]["ttm"] is not None


def test_a3_sees_the_supplier_ahead_again():
    """With ORCL's lone 2026Q3 counted as the demand frontier, NVDA's genuine
    2026Q3 read was no longer 'ahead' and A3 went silent. Gating the hyperscaler
    series restores the demand frontier to 2026Q2."""
    hyper = trend.bucket_trend("hyperscaler", _orcl_ahead(), as_of=AS_OF)

    class Leg:
        quarters = {"2025Q3": 100.0, "2026Q2": 200.0, "2026Q3": 260.0}

    fr = snapshot._supplier_frontier({"NVDA": Leg()}, hyper)
    assert fr["demand_frontier"] == "2026Q2"
    assert [r["q"] for r in fr["rows"]] == ["2026Q3"]


# --- the daily line states it, every day, in the same place ---------------------------

def test_the_thesis_clause_names_the_held_quarter():
    total = {"latest_quarter": "2026Q2",
             "partial_frontier": [{"q": "2026Q3", "prior_q": "2026Q2",
                                   "member_count": 1, "prior_member_count": 22,
                                   "coverage": 0.091}]}
    clause = snapshot.frontier_clause(total)
    assert "stands at 2026Q2" in clause
    assert "2026Q3 is partial" in clause and "1 of 22" in clause and "9%" in clause


def test_the_thesis_clause_is_present_when_nothing_is_held():
    """Silence is explicit: the clause never disappears, it says so."""
    clause = snapshot.frontier_clause({"latest_quarter": "2026Q2", "partial_frontier": []})
    assert clause == "The panel stands at 2026Q2; no later quarter is partially reported"


def test_the_renderers_share_one_list_of_held_quarters():
    snap = {"total": {"latest_quarter": "2026Q2",
                      "partial_frontier": [{"q": "2026Q3", "member_count": 1}]},
            "buckets": {"hyperscaler": {"latest_quarter": "2026Q2",
                                        "partial_frontier": [{"q": "2026Q3"}]},
                        "builder": {"latest_quarter": "2026Q2", "partial_frontier": []}},
            "panel": {}, "suppliers": {}}
    rows = snapshot.partial_frontier_rows(snap)
    assert [(r["series"], r["q"]) for r in rows] == [
        ("TOTAL PANEL", "2026Q3"), ("bucket:hyperscaler", "2026Q3")]


# --- both renderers print the held quarter, from the same rows -----------------

def _snap_with_held_quarter():
    from .test_charts import _fake_snapshot
    snap = _fake_snapshot()
    held = [{"q": "2026Q3", "prior_q": "2026Q2", "members": ["ORCL"],
             "member_count": 1, "prior_member_count": 22, "coverage": 0.091,
             "missing": ["AMZN", "GOOGL", "META", "MSFT"], "ttm": 75.66e9,
             "publishes_by": "2026-12-29"}]
    snap["total"]["latest_quarter"] = "2026Q2"
    snap["total"]["partial_frontier"] = held
    return snap


def test_the_dashboard_states_the_held_quarter():
    from capex_daemon import dashboard
    html = dashboard.view_aggregate(_snap_with_held_quarter())
    assert "Partially reported" in html
    assert "2026Q3</b> is partial: 1 of 22 members (ORCL)" in html
    assert "Waiting on AMZN, GOOGL, META, MSFT" in html
    assert "2026-12-29" in html


def test_the_dashboard_is_silent_when_nothing_is_held():
    from capex_daemon import dashboard
    from .test_charts import _fake_snapshot
    assert "Partially reported" not in dashboard.view_aggregate(_fake_snapshot())


def test_the_pdf_states_the_held_quarter_too(tmp_path):
    from capex_daemon import report
    body = report.sec_aggregate(_snap_with_held_quarter(), report._styles())
    from .test_brief import _flat_text
    text = _flat_text(body)
    assert "Partially reported" in text and "2026Q3" in text
    out = report.build(_snap_with_held_quarter(), str(tmp_path / "r.pdf"))
    assert (tmp_path / "r.pdf").stat().st_size > 0
