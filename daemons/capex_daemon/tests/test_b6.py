"""CD-BRIEF1 B6 — the CONTESTED marker and the frontier credit pair.

Both were GAP2 items promoted because they are visible defects on page one of a
Brief. HUT reads DECELERATING with a latest move of +228.1pp against a 27.0pp
band: both facts are true and the label alone carried only one.
"""
import pytest

from capex_daemon import phases, snapshot, svgcharts


# --- P4: CONTESTED ---------------------------------------------------------

def test_an_out_of_band_rise_inside_a_falling_state_is_contested():
    """HUT, live: DECELERATING with +228.1pp on a 27.0pp band."""
    assert phases.contested(phases.STATE_DECELERATING, phases.DIR_UP)
    assert phases.contested(phases.STATE_CONTRACTING, phases.DIR_UP)


def test_the_mirror_case_belongs_to_softening_not_contested():
    """Corrected after running against the live panel: firing on both mismatches
    made CIFR and DLR carry SOFTENING and CONTESTED at once, which is one fact
    stated twice. SOFTENING already owns the decline-inside-a-rising-state case,
    in the ladder's existing vocabulary. The two flags are mirrors."""
    assert not phases.contested(phases.STATE_ACCELERATING, phases.DIR_DOWN)


def test_agreement_is_never_contested():
    assert not phases.contested(phases.STATE_ACCELERATING, phases.DIR_UP)
    assert not phases.contested(phases.STATE_DECELERATING, phases.DIR_DOWN)


def test_an_in_band_move_is_not_contested():
    """`direction_of` returns FLAT inside the dead-band, and a move inside the
    band is exactly the noise the ladder exists to ignore."""
    assert not phases.contested(phases.STATE_DECELERATING, phases.DIR_FLAT)
    assert not phases.contested(phases.STATE_DECELERATING, None)


def test_contested_never_changes_the_state():
    """A single move against the trend must not flip a state — that is the
    N_CONFIRM rule and CONTESTED is a flag beside it, not a replacement."""
    band = phases.band_for("issuer:builder")
    series = {"2024Q1": 3.0, "2024Q2": 2.0, "2024Q3": 1.0, "2024Q4": 5.0}
    obs = phases.classify(series, "issuer:builder")
    last = obs[-1]
    assert last.state == phases.STATE_DECELERATING          # state held
    assert phases.FLAG_CONTESTED in last.flags              # and argued with
    assert last.delta > band


# --- P4: breadth by direction ----------------------------------------------

def test_breadth_publishes_moves_as_well_as_states():
    d = phases.breadth_by_direction({"A": phases.DIR_UP, "B": phases.DIR_UP,
                                     "C": phases.DIR_DOWN, "D": phases.DIR_FLAT})
    assert d == {"moved_up": 2, "moved_down": 1, "moved_flat": 1, "net_moves": 1}


def test_a_quarter_where_everyone_turns_is_distinguishable_from_a_quiet_one():
    """The strip published only the state census, so a quarter in which most
    names turned but none had yet confirmed looked identical to a still one."""
    turning = phases.breadth_by_direction({"A": phases.DIR_DOWN, "B": phases.DIR_DOWN,
                                           "C": phases.DIR_DOWN})
    quiet = phases.breadth_by_direction({"A": phases.DIR_FLAT, "B": phases.DIR_FLAT,
                                         "C": phases.DIR_FLAT})
    assert turning["net_moves"] == -3 and quiet["net_moves"] == 0


# --- P1: the frontier pair -------------------------------------------------

def _pair(n=10, extra_from=None):
    qs = ["{}Q{}".format(2024 + i // 4, i % 4 + 1) for i in range(n)]
    cap = {q: 100.0 + i for i, q in enumerate(qs)}
    iss = {q: 10.0 + i for i, q in enumerate(qs)}
    capm = {q: ["A", "B"] for q in qs}
    issm = {}
    for i, q in enumerate(qs):
        issm[q] = ["A"] + (["META"] if extra_from is not None and i >= extra_from else [])
    return cap, capm, iss, issm, qs


def test_the_frontier_pair_spans_only_the_trailing_window():
    cap, capm, iss, issm, qs = _pair(20)
    fp = snapshot._frontier_pair(cap, capm, iss, issm)
    assert len(fp["quarters"]) == snapshot.FRONTIER_PAIR_QUARTERS
    assert fp["quarters"] == qs[-8:]


def test_an_issuer_entering_inside_the_window_is_a_composition_event():
    """Over eight quarters membership still changes, so a step caused by an
    arrival must not be readable as spending."""
    cap, capm, iss, issm, qs = _pair(12, extra_from=9)
    fp = snapshot._frontier_pair(cap, capm, iss, issm)
    assert [(e["ticker"], e["q"]) for e in fp["composition_events"]] == [("META", qs[9])]


def test_no_entries_when_membership_is_stable():
    cap, capm, iss, issm, _qs = _pair(12)
    assert snapshot._frontier_pair(cap, capm, iss, issm)["composition_events"] == []


def test_too_short_a_panel_yields_nothing_rather_than_a_one_point_line():
    assert snapshot._frontier_pair({"2026Q1": 1.0}, {}, {"2026Q1": 1.0}, {}) == {}


def test_the_frontier_pair_reaches_the_composite_model():
    from .test_charts import _fake_snapshot
    snap = _fake_snapshot()
    snap["panel"]["frontier_pair"] = {
        "quarters": ["2023Q3"], "capex": [{"q": "2023Q3", "value": 1.0, "members": 9}],
        "issuance": [{"q": "2023Q3", "value": 1.0, "members": 9}],
        "composition_events": [], "basis": "x"}
    m = svgcharts.composite_model(snap)
    assert m["frontier_pair"]["issuance"][0]["members"] == 9


def test_the_frontier_colour_is_distinct_from_the_constant_membership_jaws():
    """They sit on one chart and make DIFFERENT claims; they must not read as
    two shades of the same line."""
    assert svgcharts.SERIES_COLORS["frontier"] != svgcharts.SERIES_COLORS["issuance"]
