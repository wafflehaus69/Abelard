"""PS-1 Phase 3 — analytics tests.

The two momentum pins are asserted from LADDER values, not from prices, as the
order specifies. That keeps the scoring formula pinned independently of the
moving-average construction: if a future change to how MAs are computed breaks
a ladder, the ladder test fails and the score test does not, and the failure
says which half moved.
"""

from __future__ import annotations

import math

import pytest

from abelard_common.prices import analytics as A
from abelard_common.prices import schema as S


# ------------------------------------------------- the two pinned ladders ----

def test_fbrx_ladder_scores_15_2():
    """FBRX: 0, 20.2%, 69.2%, 126.6%, 136.9% -> 15.2"""
    ladder = [0.0, 0.202, 0.692, 1.266, 1.369]
    assert round(A.ladder_score(ladder), 1) == 15.2
    assert A.ladder_score(ladder) == pytest.approx(15.208, abs=0.001)


def test_mrna_ladder_scores_16_8():
    """MRNA: 0, 22.8%, 49.7%, 62.3%, 190.4% -> 16.8"""
    ladder = [0.0, 0.228, 0.497, 0.623, 1.904]
    assert round(A.ladder_score(ladder), 1) == 16.8
    assert A.ladder_score(ladder) == pytest.approx(16.812, abs=0.001)


def test_the_two_pins_are_not_accidentally_equal():
    """A formula that returned a constant would pass each pin alone."""
    fbrx = A.ladder_score([0.0, 0.202, 0.692, 1.266, 1.369])
    mrna = A.ladder_score([0.0, 0.228, 0.497, 0.623, 1.904])
    assert abs(fbrx - mrna) > 1.0


def test_a_flat_ladder_scores_zero():
    assert A.ladder_score([0.0, 0.0, 0.0, 0.0, 0.0]) == 0.0


def test_a_falling_ladder_scores_negative():
    assert A.ladder_score([0.0, -0.05, -0.10, -0.18, -0.25]) < 0


def test_score_is_the_slope_times_ten():
    ladder = [0.0, 0.25, 0.5, 0.75, 1.0]        # slope exactly 1.0 on x = same
    assert A.ols_slope(ladder) == pytest.approx(1.0)
    assert A.ladder_score(ladder) == pytest.approx(10.0)


def test_ols_slope_rejects_a_degenerate_x():
    with pytest.raises(ValueError):
        A.ols_slope([1.0, 2.0], x=[0.5, 0.5])
    with pytest.raises(ValueError):
        A.ols_slope([1.0], x=[0.0])


# ------------------------------------------------------ the ladder itself ----

def _rising(n=250, start=100.0, step=0.4):
    return [start + step * i for i in range(n)]


def test_ma_ladder_first_rung_is_always_zero():
    """y[0] is MA200 measured against itself."""
    ladder = A.ma_ladder(_rising())
    assert ladder is not None
    assert ladder[0] == 0.0


def test_ma_ladder_rises_through_the_rungs_on_an_uptrend():
    ladder = A.ma_ladder(_rising())
    assert ladder == sorted(ladder), ladder
    assert ladder[-1] > ladder[-2] > 0


def test_ma_ladder_is_none_when_there_is_no_ma200():
    """199 sessions is not a 200-day average, and a partial one labelled MA200
    is the quiet wrongness this substrate exists against."""
    assert A.ma_ladder(_rising(199)) is None
    assert A.ma_ladder(_rising(200)) is not None


def test_moving_average_refuses_a_partial_window():
    assert A.moving_average([1.0] * 39, 40) is None
    assert A.moving_average([1.0] * 40, 40) == 1.0


def test_momentum_returns_ladder_and_score_together():
    m = A.momentum_ma_ladder(_rising())
    assert m is not None
    assert len(m.ladder) == 5
    assert m.score == pytest.approx(A.ladder_score(m.ladder))


def test_momentum_is_none_for_a_short_name_rather_than_a_guess():
    assert A.momentum_ma_ladder(_rising(50)) is None


def test_the_score_alone_hides_the_shape():
    """Why momentum_ma_ladder returns both. A steady climb and a terminal spike
    can score alike; MRNA's real ladder is the latter."""
    steady = [0.0, 0.40, 0.80, 1.20, 1.60]
    spike = [0.0, 0.02, 0.05, 0.10, 1.92]
    assert abs(A.ladder_score(steady) - A.ladder_score(spike)) < 1.0
    assert steady[3] > 1.0 and spike[3] < 0.2      # utterly different shapes


# ------------------------------------------------------------ 63 skip 5 ----

def test_63_skip_5_ignores_the_most_recent_five_sessions():
    closes = [100.0] * 69
    closes[-1] = 500.0                            # a huge move inside the skip
    closes[-2] = 400.0
    assert A.momentum_return_63_skip_5(closes) == pytest.approx(0.0)


def test_63_skip_5_measures_the_window_it_claims():
    closes = [100.0] * 69
    closes[-6] = 110.0                            # the window's end session
    r = A.momentum_return_63_skip_5(closes)
    assert r == pytest.approx(math.log(1.10))


def test_63_skip_5_needs_the_full_window():
    assert A.momentum_return_63_skip_5([100.0] * 68) is None
    assert A.momentum_return_63_skip_5([100.0] * 69) is not None


# ------------------------------------------------------------------ returns --

def test_log_returns_compose_additively():
    closes = [100.0, 110.0, 121.0]
    rs = A.log_returns(closes)
    assert sum(rs) == pytest.approx(math.log(1.21))


def test_log_returns_skip_non_positive_prices():
    assert A.log_returns([100.0, 0.0, 100.0]) == []


def test_dated_log_returns_are_keyed_by_the_session_they_belong_to():
    out = A.dated_log_returns({"2026-01-02": 100.0, "2026-01-05": 110.0})
    assert list(out) == ["2026-01-05"]


def test_aligned_returns_intersect_and_report_the_window():
    panel = {
        "A": {"2026-01-02": 1.0, "2026-01-05": 1.1, "2026-01-06": 1.2},
        "B": {"2026-01-02": 5.0, "2026-01-05": 5.5},          # stops early
    }
    dates, series = A.aligned_returns(panel)
    assert dates == ["2026-01-05"]
    assert set(series) == {"A", "B"}
    assert len(series["A"]) == len(series["B"]) == 1


def test_aligned_returns_makes_the_truncation_visible():
    """CR-R0 §R1.5: intersecting dates silently dated a 497-name panel to its
    stalest member. Returning the dates alongside the series is what makes that
    visible instead of assumed."""
    panel = {"FRESH": {"2026-08-31": 1.0, "2026-09-01": 1.1},
             "STALE": {"2026-07-22": 1.0, "2026-07-23": 1.1}}
    dates, series = A.aligned_returns(panel)
    assert dates == []            # no overlap at all, and the caller can see it


# ------------------------------------------------------------------ baskets --

def test_leave_one_out_excludes_the_name_from_its_own_benchmark():
    members = {
        "A": {"d1": 100.0, "d2": 200.0},          # +100%
        "B": {"d1": 100.0, "d2": 100.0},          # flat
        "C": {"d1": 100.0, "d2": 100.0},          # flat
    }
    with_a = A.ew_basket_returns(members)
    without_a = A.ew_basket_returns(members, leave_out="A")
    assert with_a["d2"] > 0
    assert without_a["d2"] == pytest.approx(0.0)


def test_a_basket_needs_two_contributors():
    members = {"A": {"d1": 100.0, "d2": 110.0}}
    assert A.ew_basket_returns(members) == {}
    assert A.ew_basket_returns(members, leave_out="A") == {}


def test_basket_is_equal_weight_not_size_weight():
    members = {
        "BIG": {"d1": 1000.0, "d2": 1100.0},      # +10%, large price
        "SMALL": {"d1": 1.0, "d2": 0.9},          # -10%, tiny price
    }
    out = A.ew_basket_returns(members)
    assert out["d2"] == pytest.approx(
        (math.log(1.1) + math.log(0.9)) / 2)


def test_basket_composition_is_reported_not_assumed():
    """E14: an average over a varying membership must disclose its composition."""
    members = {
        "A": {"d1": 1.0, "d2": 1.1, "d3": 1.2},
        "B": {"d1": 1.0, "d2": 1.1},
        "C": {"d1": 1.0, "d2": 1.1, "d3": 1.3},
    }
    comp = A.basket_composition(members)
    assert comp["d2"] == 3 and comp["d3"] == 2


def test_loo_basket_for_each_gives_every_member_its_own():
    members = {n: {"d1": 1.0, "d2": 1.0 + i / 10} for i, n in enumerate("ABC")}
    out = A.loo_basket_for_each(members)
    assert set(out) == {"A", "B", "C"}
    # A's basket is built from B and C only.
    assert out["A"]["d2"] == pytest.approx(
        (math.log(1.1) + math.log(1.2)) / 2)


# ------------------------------------------------------------ I/O boundary --

def test_load_panel_reads_the_adjusted_view_only(tmp_path):
    con = S.connect(tmp_path / "p.db")
    con.execute("INSERT INTO instruments (instrument_id, cik, class_code,"
                " class_source, name, primary_ticker, source, provisional,"
                " first_seen, last_seen) VALUES"
                " ('X.0','X','0','single','X','X','t',0,'2026-01-01','2026-01-01')")
    con.executemany("INSERT INTO adjusted_view VALUES ('X.0',?,?,1)",
                    [("2026-01-02", 10.0), ("2026-01-05", 11.0)])
    con.commit()
    panel = A.load_panel(con, ["X.0"], "2026-01-01", "2026-01-31")
    assert panel == {"X.0": {"2026-01-02": 10.0, "2026-01-05": 11.0}}
    assert A.load_panel(con, ["X.0"], "2026-02-01", "2026-02-28") == {}
    con.close()


def test_load_panel_can_pin_a_factor_version(tmp_path):
    """An as-of audit reproduces a number published under an older factor
    generation; a live dashboard wants whatever is current."""
    con = S.connect(tmp_path / "p.db")
    con.execute("INSERT INTO instruments (instrument_id, cik, class_code,"
                " class_source, name, primary_ticker, source, provisional,"
                " first_seen, last_seen) VALUES"
                " ('Y.0','Y','0','single','Y','Y','t',0,'2026-01-01','2026-01-01')")
    con.execute("INSERT INTO adjusted_view VALUES ('Y.0','2026-01-02',10.0,2)")
    con.commit()
    assert A.load_panel(con, ["Y.0"], "2026-01-01", "2026-01-31", factor_version=2)
    assert A.load_panel(con, ["Y.0"], "2026-01-01", "2026-01-31", factor_version=1) == {}
    con.close()


# ------------------------------------------------------------------- guards --

def test_a_bare_number_is_a_caller_error_not_a_guess():
    """Silently reversing a price series would invert every momentum sign."""
    with pytest.raises(TypeError):
        A.log_returns(42)


def test_mapping_input_is_ordered_by_date_not_insertion():
    out_of_order = {"2026-01-05": 110.0, "2026-01-02": 100.0}
    assert A.log_returns(out_of_order) == pytest.approx([math.log(1.1)])
