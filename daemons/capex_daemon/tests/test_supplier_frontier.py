"""CD-GAP2A A3 — the quarter the suppliers have and the demand panel does not.

NVDA closes in July, MU in August, SMCI in June, so they routinely file a
quarter the hyperscalers will not report until late October. The cross-check
correctly refuses a ratio there — no denominator exists — and the numerator was
being discarded with it. This publishes it as what it is: one supplier's own
quarter, ahead of the panel, on its own history.
"""
import pytest

from capex_daemon import snapshot


class _Leg:
    def __init__(self, quarters):
        self.quarters = quarters
        self.status = "COVERED"
        self.detail = ""
        self.axes = []
        self.concept = "Revenues"
        self.instances = 1
        self.restatements = []
        self.dropped = []
        self.mapping = None
        self.is_mapped = False
        self.partial = []
        self.is_covered = True


class _Hyper:
    def __init__(self, ttm):
        self.ttm = ttm
        self.membership = {q: ["A"] for q in ttm}


def test_quarter_arithmetic_crosses_year_boundaries():
    assert snapshot._shift("2026Q3", -1) == "2026Q2"
    assert snapshot._shift("2026Q3", -4) == "2025Q3"
    assert snapshot._shift("2026Q1", -1) == "2025Q4"
    assert snapshot._shift("2026Q1", -4) == "2025Q1"


def test_growth_refuses_a_zero_or_missing_base():
    assert snapshot._growth(100.0, 150.0) == 0.5
    assert snapshot._growth(0.0, 150.0) is None
    assert snapshot._growth(None, 150.0) is None
    assert snapshot._growth(100.0, None) is None


def test_only_quarters_beyond_the_demand_frontier_are_published():
    legs = {"NVDA": _Leg({"2025Q3": 100.0, "2026Q1": 180.0, "2026Q2": 200.0,
                          "2026Q3": 260.0})}
    fr = snapshot._supplier_frontier(legs, _Hyper({"2026Q1": 1.0, "2026Q2": 2.0}))
    assert fr["demand_frontier"] == "2026Q2"
    assert [r["q"] for r in fr["rows"]] == ["2026Q3"]
    row = fr["rows"][0]
    assert row["qoq"] == pytest.approx(0.30)        # 260 vs 200
    assert row["yoy"] == pytest.approx(1.60)        # 260 vs 100 a year earlier
    assert row["prior_q"] == "2026Q2" and row["year_ago_q"] == "2025Q3"


def test_a_supplier_not_ahead_publishes_nothing():
    legs = {"AMD": _Leg({"2026Q1": 10.0, "2026Q2": 12.0})}
    fr = snapshot._supplier_frontier(legs, _Hyper({"2026Q2": 1.0}))
    assert fr["rows"] == []


def test_a_missing_comparison_quarter_yields_none_not_a_guess():
    """A supplier with no year-ago quarter gets a dash, never an invented base."""
    legs = {"MU": _Leg({"2026Q2": 50.0, "2026Q3": 60.0})}
    fr = snapshot._supplier_frontier(legs, _Hyper({"2026Q2": 1.0}))
    row = fr["rows"][0]
    assert row["qoq"] == pytest.approx(0.2)
    assert row["yoy"] is None


def test_no_hyperscaler_trend_means_no_frontier_rather_than_a_crash():
    assert snapshot._supplier_frontier({"NVDA": _Leg({"2026Q3": 1.0})}, None) == {}


def test_the_frontier_read_is_never_a_ratio_or_a_state():
    """The whole point is that it is NOT comparable to the demand panel."""
    legs = {"NVDA": _Leg({"2025Q3": 100.0, "2026Q2": 200.0, "2026Q3": 260.0})}
    row = snapshot._supplier_frontier(legs, _Hyper({"2026Q2": 2.0}))["rows"][0]
    assert "ratio" not in row and "state" not in row
    assert "not a TTM" in row["basis"] and "not in any aggregate" in row["basis"]
