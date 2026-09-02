"""PS-1 Phase 3 / 3.1 — analytics tests.

The two momentum pins are asserted from LADDER values, not from prices, as the
order specifies. That keeps the scoring formula pinned independently of the
moving-average construction: if a future change to how MAs are computed breaks a
ladder, the ladder test fails and the score test does not, and the failure says
which half moved.

Phase 3.1 adds the session-awareness half: every window is measured in sessions,
and a hole is counted rather than bridged.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from abelard_common.prices import analytics as A
from abelard_common.prices import calendar as C
from abelard_common.prices import schema as S

FIXTURES = Path(__file__).parent / "fixtures"


def sess(start="2026-01-02", n=400):
    """n consecutive trading sessions from the real NYSE calendar."""
    out, d = [], start
    import datetime as dt
    cur = dt.date.fromisoformat(start)
    while len(out) < n:
        if C.is_session(cur):
            out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out


def series(sessions, values):
    return {d: v for d, v in zip(sessions, values)}


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
    assert A.ladder_score([0.0] * 5) == 0.0


def test_a_falling_ladder_scores_negative():
    assert A.ladder_score([0.0, -0.05, -0.10, -0.18, -0.25]) < 0


def test_score_is_the_slope_times_ten():
    ladder = [0.0, 0.25, 0.5, 0.75, 1.0]
    assert A.ols_slope(ladder) == pytest.approx(1.0)
    assert A.ladder_score(ladder) == pytest.approx(10.0)


def test_ols_slope_rejects_a_degenerate_x():
    with pytest.raises(ValueError):
        A.ols_slope([1.0, 2.0], x=[0.5, 0.5])


# ------------------------------- the prices -> ladder path, on real MRNA ----

def test_mrna_real_series_reproduces_its_ma_rungs_exactly():
    """The end-to-end pin: real adjusted closes -> session-aware ladder.

    Mando's pinned MRNA ladder reproduces EXACTLY on its MA100/MA50/MA30 rungs
    at as_of 2026-08-31 — three independent values matching to a tenth of a
    percent is not coincidence, and it dates the pin.

    The Last rung does NOT reproduce, and that is a finding rather than a
    failure: his 190.4% implies a price of 151.97 against that session's close
    of 140.34. The 2026-09-01 close was 154.27 and 09-02 was 150.81, so his Last
    was a live intraday quote while his moving averages ran to the prior close.
    A stored, versioned system must use the close or the same as_of yields a
    different answer on every recomputation — so the score from prices is 15.04
    here, not 16.8, and 16.8 stays pinned where it belongs: on the ladder.
    """
    fx = json.loads((FIXTURES / "mrna_ladder_20260831.json").read_text())
    closes = fx["closes"]
    sessions = C.sessions_between("2024-01-01", "2026-08-31")
    st = A.ladder_status(closes, sessions, "2026-08-31")
    assert st.ok, st.detail
    assert [round(100 * y, 1) for y in st.ladder[:4]] == [0.0, 22.8, 49.7, 62.3]
    assert round(100 * st.ladder[4], 1) == 168.2      # close, not a live quote
    assert A.ladder_score(st.ladder) == pytest.approx(15.04, abs=0.01)


# ------------------------------------------- 3.1: sessions, not rows ----

def test_a_return_is_not_emitted_across_a_hole():
    s = sess(n=5)
    closes = series(s, [100.0, 101.0, 102.0, 103.0, 104.0])
    del closes[s[2]]                                   # punch a hole
    rets, gaps = A.dated_log_returns(closes, s)
    assert s[2] not in rets and s[3] not in rets       # neither side bridges it
    assert gaps == [(s[1], s[3])]
    assert set(rets) == {s[1], s[4]}


def test_the_gap_is_counted_not_silently_dropped():
    """Bridging is a larger error than omitting: the result looks like a
    single-session return and gets averaged with real ones."""
    s = sess(n=4)
    closes = {s[0]: 100.0, s[3]: 130.0}                # 3 sessions apart
    rets, gaps = A.dated_log_returns(closes, s)
    assert rets == {}
    assert gaps == [(s[0], s[3])]


def test_returns_still_flow_where_sessions_are_consecutive():
    s = sess(n=3)
    rets, gaps = A.dated_log_returns(series(s, [100.0, 110.0, 121.0]), s)
    assert gaps == []
    assert sum(rets.values()) == pytest.approx(math.log(1.21))


def test_a_hole_inside_ma30_refuses_the_ladder_and_names_the_rung():
    s = sess(n=260)
    closes = series(s, [100.0 + i * 0.1 for i in range(260)])
    del closes[s[-10]]                                 # inside MA30, not MA200
    st = A.ladder_status(closes, s, s[-1])
    assert not st.ok
    # The windows nest, so a hole 10 sessions back fails every rung. What the
    # caller needs is the SHORTEST failing window -- it says the hole is recent,
    # not that the history is short.
    assert st.failed == ["MA200", "MA100", "MA50", "MA30"]
    assert "hole inside the last 30 sessions" in st.detail
    assert A.ma_ladder(closes, s, s[-1]) is None


def test_a_hole_outside_every_window_is_harmless():
    s = sess(n=400)
    closes = series(s, [100.0 + i * 0.1 for i in range(400)])
    del closes[s[5]]                                   # ancient history
    st = A.ladder_status(closes, s, s[-1])
    assert st.ok, st.detail


def test_ma200_counts_sessions_not_rows():
    """200 rows spanning 205 sessions is not an MA200."""
    s = sess(n=260)
    closes = series(s, [100.0] * 260)
    for d in s[-40:-35]:                               # 5 holes inside MA200
        del closes[d]
    assert A.moving_average(closes, 200, s, s[-1]) is None
    # ... and with no holes it is fine, on the same number of rows.
    clean = series(s, [100.0] * 260)
    assert A.moving_average(clean, 200, s, s[-1]) == pytest.approx(100.0)


def test_moving_average_refuses_a_short_history():
    s = sess(n=250)
    closes = series(s, [1.0] * 250)
    assert A.moving_average(closes, 200, s, s[199]) == pytest.approx(1.0)
    assert A.moving_average(closes, 200, s, s[198]) is None


def test_ladder_status_distinguishes_too_short_from_holed():
    s = sess(n=260)
    short = series(s[:50], [1.0] * 50)
    st = A.ladder_status(short, s, s[49])
    assert not st.ok and "only 50 sessions held" in st.detail

    holed = series(s, [1.0 + i * 0.01 for i in range(260)])
    del holed[s[-3]]
    st2 = A.ladder_status(holed, s, s[-1])
    assert not st2.ok and "hole inside the last 30 sessions" in st2.detail


def test_63_skip_5_endpoints_must_be_real_held_sessions():
    s = sess(n=100)
    closes = series(s, [100.0] * 100)
    assert A.momentum_return_63_skip_5(closes, s, s[-1]) is not None
    holed = dict(closes)
    del holed[s[-6]]                                   # the window's endpoint
    assert A.momentum_return_63_skip_5(holed, s, s[-1]) is None


def test_63_skip_5_ignores_the_most_recent_five_sessions():
    s = sess(n=100)
    vals = [100.0] * 100
    vals[-1], vals[-2] = 500.0, 400.0                  # huge move inside the skip
    assert A.momentum_return_63_skip_5(series(s, vals), s, s[-1]) == pytest.approx(0.0)


def test_63_skip_5_measures_the_window_it_claims():
    s = sess(n=100)
    vals = [100.0] * 100
    vals[-6] = 110.0
    r = A.momentum_return_63_skip_5(series(s, vals), s, s[-1])
    assert r == pytest.approx(math.log(1.10))


# ------------------------------------------------------------------ baskets --

def test_a_holed_member_is_not_counted_in_the_basket():
    """The propagation this phase removes: a member whose only return spans a
    hole was being averaged against genuine single-session returns."""
    s = sess(n=4)
    members = {
        "A": series(s, [100.0, 101.0, 102.0, 103.0]),
        "B": series(s, [100.0, 101.0, 102.0, 103.0]),
        "C": {s[0]: 100.0, s[3]: 200.0},               # only a 3-session span
    }
    comp = A.basket_composition(members, s)
    assert comp[s[3]] == 2, "C must not be counted on a bridged span"
    out = A.ew_basket_returns(members, s)
    expected = math.log(103.0 / 102.0)
    assert out[s[3]] == pytest.approx(expected)        # C's +100% is absent


def test_leave_one_out_excludes_the_name_from_its_own_benchmark():
    s = sess(n=2)
    members = {"A": series(s, [100.0, 200.0]),
               "B": series(s, [100.0, 100.0]),
               "C": series(s, [100.0, 100.0])}
    assert A.ew_basket_returns(members, s)[s[1]] > 0
    assert A.ew_basket_returns(members, s, leave_out="A")[s[1]] == pytest.approx(0.0)


def test_a_basket_needs_two_contributors():
    s = sess(n=2)
    members = {"A": series(s, [100.0, 110.0])}
    assert A.ew_basket_returns(members, s) == {}


def test_basket_is_equal_weight_not_size_weight():
    s = sess(n=2)
    members = {"BIG": series(s, [1000.0, 1100.0]), "SMALL": series(s, [1.0, 0.9])}
    assert A.ew_basket_returns(members, s)[s[1]] == pytest.approx(
        (math.log(1.1) + math.log(0.9)) / 2)


def test_loo_basket_for_each_gives_every_member_its_own():
    s = sess(n=2)
    members = {n: series(s, [1.0, 1.0 + i / 10]) for i, n in enumerate("ABC")}
    out = A.loo_basket_for_each(members, s)
    assert set(out) == {"A", "B", "C"}
    assert out["A"][s[1]] == pytest.approx((math.log(1.1) + math.log(1.2)) / 2)


# ------------------------------------------------------------------ returns --

def test_aligned_returns_intersect_and_report_the_window():
    s = sess(n=3)
    panel = {"A": series(s, [1.0, 1.1, 1.2]), "B": {s[0]: 5.0, s[1]: 5.5}}
    dates, out = A.aligned_returns(panel, s)
    assert dates == [s[1]]
    assert len(out["A"]) == len(out["B"]) == 1


def test_aligned_returns_makes_the_truncation_visible():
    """CR-R0 §R1.5: intersecting silently dated a 497-name panel to its stalest
    member. Returning the dates makes that visible rather than assumed."""
    s = sess(n=40)
    panel = {"FRESH": series(s[-2:], [1.0, 1.1]), "STALE": series(s[:2], [1.0, 1.1])}
    dates, _ = A.aligned_returns(panel, s)
    assert dates == []


def test_log_returns_compose_additively():
    assert sum(A.log_returns([100.0, 110.0, 121.0])) == pytest.approx(math.log(1.21))


def test_log_returns_skip_non_positive_prices():
    assert A.log_returns([100.0, 0.0, 100.0]) == []


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
    assert A.load_panel(con, ["X.0"], "2026-01-01", "2026-01-31") == {
        "X.0": {"2026-01-02": 10.0, "2026-01-05": 11.0}}
    assert A.load_panel(con, ["X.0"], "2026-02-01", "2026-02-28") == {}
    con.close()


def test_load_panel_can_pin_a_factor_version(tmp_path):
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


def test_a_bare_number_is_a_caller_error_not_a_guess():
    with pytest.raises(TypeError):
        A.log_returns(42)
