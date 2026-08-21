"""CD-PH1 acceptance tests: the ladder, aggregates, breadth, alerts."""
import sqlite3

import pytest

from capex_daemon import config, phases, snapshot, storage, trend


@pytest.fixture()
def con(tmp_path):
    c = sqlite3.connect(str(tmp_path / "p.db"))
    c.executescript(storage.SCHEMA)
    return c


def series(*yoys, start=(2024, 1)):
    """yoys as fractions, laid on consecutive calendar quarters."""
    out, (y, q) = {}, start
    for v in yoys:
        out["{}Q{}".format(y, q)] = v
        q += 1
        if q > 4:
            y, q = y + 1, 1
    return out


CLS = "issuer:hyperscaler"     # 6pp band
BAND = 6.0


# --- bands are ratified constants, not choices --------------------------

def test_bands_are_stamped_and_complete():
    assert config.DEAD_BAND_MEASURED_ON == "2026-08-18"
    for k in ("issuer:hyperscaler", "issuer:builder", "issuer:reit", "issuer:host",
              "bucketsum:hyperscaler", "bucketsum:builder", "bucketsum:reit", "total:panel"):
        assert config.DEAD_BANDS[k] > 0


def test_ratified_values_are_exactly_as_measured():
    assert config.DEAD_BANDS["issuer:hyperscaler"] == 6.0
    assert config.DEAD_BANDS["issuer:builder"] == 27.0
    assert config.DEAD_BANDS["issuer:reit"] == 2.0
    assert config.DEAD_BANDS["issuer:host"] == 5.0
    assert config.DEAD_BANDS["bucketsum:hyperscaler"] == 4.0
    assert config.DEAD_BANDS["bucketsum:builder"] == 27.0
    # REIT sum keeps its own measured 6pp rather than inheriting the 2pp
    # per-issuer value — a two-name sum is genuinely noisier (ratified).
    assert config.DEAD_BANDS["bucketsum:reit"] == 6.0
    assert config.DEAD_BANDS["total:panel"] == 5.0


def test_classifying_against_an_unruled_band_is_refused():
    with pytest.raises(ValueError) as e:
        phases.classify(series(.1, .2, .3), "issuer:does-not-exist")
    assert "unruled constant" in str(e.value)


# --- the ladder ----------------------------------------------------------

def test_two_consecutive_rises_enter_accelerating():
    obs = phases.classify(series(.10, .20, .30), CLS)
    assert obs[-1].state == phases.STATE_ACCELERATING
    assert obs[-1].quarters_in_state == 1


def test_one_rise_does_not_enter_accelerating():
    """N=2. A single move is not a state."""
    obs = phases.classify(series(.10, .11, .30), CLS)
    assert obs[-1].state != phases.STATE_ACCELERATING


def test_moves_inside_the_band_are_plateau():
    """6pp band: 3pp moves are noise and must not change direction."""
    obs = phases.classify(series(.10, .13, .16, .19), CLS)
    assert all(o.direction == phases.DIR_FLAT for o in obs[1:])
    assert obs[-1].state == phases.STATE_PLATEAU


def test_dead_band_suppresses_a_real_wiggle():
    """Down 4pp then up 4pp — both inside the band, so no state churn."""
    obs = phases.classify(series(.50, .46, .50, .46), CLS)
    assert {o.state for o in obs[1:]} == {phases.STATE_PLATEAU}


def test_contracting_is_level_based_and_preempts():
    """TTM YoY < 0 is CONTRACTING regardless of which way it moved to get there."""
    obs = phases.classify(series(.10, .20, -.05), CLS)
    assert obs[-1].state == phases.STATE_CONTRACTING
    # even while RISING, a negative level stays CONTRACTING
    obs2 = phases.classify(series(-.40, -.30, -.20), CLS)
    assert obs2[-1].state == phases.STATE_CONTRACTING
    assert obs2[-1].direction == phases.DIR_UP


def test_softening_is_a_flag_on_the_first_out_of_band_decline():
    obs = phases.classify(series(.10, .30, .50, .30), CLS)
    assert phases.FLAG_SOFTENING in obs[-1].flags
    assert obs[-1].state != phases.FLAG_SOFTENING          # never a state
    assert phases.FLAG_SOFTENING not in phases.REAL_STATES


def test_confirmed_raises_at_three_consecutive():
    obs = phases.classify(series(.10, .20, .30, .40), CLS)
    assert phases.FLAG_CONFIRMED in obs[-1].flags


def test_a_single_counter_move_does_not_flip_a_confirmed_state():
    """Measured on HUT: DECELERATING held while one quarter moved up hard."""
    obs = phases.classify(series(.90, .60, .30, .10, .80), CLS)
    assert obs[-1].state == phases.STATE_DECELERATING
    assert obs[-1].direction == phases.DIR_UP           # direction is published beside it


def test_too_short_a_series_is_insufficient_never_provisional():
    assert phases.classify(series(.1, .2), CLS) == []


# --- transitions ---------------------------------------------------------

def test_transition_key_is_content_derived_not_run_time():
    t = phases.Transition("MSFT", "2026Q2", "PLATEAU", "DECELERATING", .5, -9.0)
    assert t.event_key == "MSFT|2026Q2|PLATEAU->DECELERATING"


def test_recording_transitions_is_idempotent(con):
    t = [phases.Transition("MSFT", "2026Q2", "PLATEAU", "DECELERATING", .5, -9.0)]
    assert len(phases.record_transitions(con, t)) == 1
    assert len(phases.record_transitions(con, t)) == 0


# --- breadth -------------------------------------------------------------

def test_breadth_net_direction():
    b = phases.breadth({"A": phases.STATE_ACCELERATING, "B": phases.STATE_ACCELERATING,
                        "C": phases.STATE_DECELERATING, "D": phases.STATE_CONTRACTING})
    assert b["net_direction"] == 1
    assert b[phases.STATE_CONTRACTING] == 1


# --- aggregates ----------------------------------------------------------

def test_matched_membership_refuses_to_read_an_arrival_as_growth():
    """B joins only in the current window; it must not inflate the YoY."""
    a = {"{}Q{}".format(y, q): 100.0 for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)}
    b = {q: 900.0 for q in ["2026Q1", "2026Q2", "2026Q3", "2026Q4"]}
    bt = trend.bucket_trend("x", {"A": a, "B": b})
    latest = max(bt.yoy, key=trend._cq_sort)
    assert bt.membership[latest] == ["A"]      # B excluded — no prior-year window
    assert abs(bt.yoy[latest]) < 1e-9          # flat, not +800%


def test_composition_events_are_published_beside_the_trend():
    a = {"{}Q{}".format(y, q): 100.0 for y in (2023, 2024, 2025, 2026) for q in (1, 2, 3, 4)}
    b = {"{}Q{}".format(y, q): 50.0 for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)}
    bt = trend.bucket_trend("x", {"A": a, "B": b})
    assert any(e[2] == "B" and e[3] == trend.CHANGE_ENTERED
               for e in bt.composition_events)


def test_a_one_member_bucket_sum_publishes_a_status_not_a_number():
    """DLR's series ends a quarter before EQIX's, which left the REIT 'sum' as
    EQIX alone at +56.7%. A one-name sum is not a sum."""
    assert trend.MIN_BUCKET_MEMBERS == 2
    assert trend.STATE_INSUFFICIENT_MEMBERSHIP != phases.STATE_INSUFFICIENT


def test_aggregates_key_on_calendar_quarter_not_period_end():
    """MSFT closes Jun/Sep/Dec/Mar and ORCL Feb/May/Aug/Nov; keyed on raw end
    dates the member intersection is empty and the sum has one observation."""
    a = {"{}Q{}".format(y, q): 10.0 for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)}
    b = {"{}Q{}".format(y, q): 20.0 for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)}
    bt = trend.bucket_trend("x", {"MSFTLIKE": a, "ORCLLIKE": b})
    assert len(bt.yoy) >= 4
    assert all(len(m) == 2 for m in bt.membership.values())


# --- alerts --------------------------------------------------------------

def _snap(transitions, issuers):
    return {"transitions": transitions, "issuers": issuers, "buckets": {}, "total": {}}


def test_hyperscaler_entering_decelerating_alerts():
    s = _snap([{"series_key": "MSFT", "quarter": "2026Q2", "from_state": "PLATEAU",
                "to_state": phases.STATE_DECELERATING, "yoy": .5, "delta": -9.0,
                "event_key": "k1"}],
              {"MSFT": {"bucket": "hyperscaler"}})
    out = snapshot.alert_lines(s)
    assert len(out) == 1 and "hyperscaler" in out[0]["reason"]


def test_mirror_names_never_alert():
    """SNOW is the calibration ghost: it classifies, it is visible, it is silent."""
    s = _snap([{"series_key": "SNOW", "quarter": "2026Q2", "from_state": "PLATEAU",
                "to_state": phases.STATE_CONTRACTING, "yoy": -.1, "delta": -30.0,
                "event_key": "k2"}],
              {"SNOW": {"bucket": "mirror"}})
    assert snapshot.alert_lines(s) == []


def test_crwv_state_change_alerts_for_the_theses_intersection():
    s = _snap([{"series_key": "CRWV", "quarter": "2026Q2", "from_state": "ACCELERATING",
                "to_state": phases.STATE_PLATEAU, "yoy": .9, "delta": -30.0,
                "event_key": "k3"}],
              {"CRWV": {"bucket": "builder"}})
    out = snapshot.alert_lines(s)
    assert len(out) == 1 and "THESES" in out[0]["reason"]


def test_aggregate_transitions_alert():
    s = _snap([{"series_key": "bucket:builder", "quarter": "2026Q2",
                "from_state": "ACCELERATING", "to_state": phases.STATE_DECELERATING,
                "yoy": .3, "delta": -40.0, "event_key": "k4"}], {})
    assert len(snapshot.alert_lines(s)) == 1


def test_already_seen_transitions_do_not_realert():
    s = _snap([{"series_key": "MSFT", "quarter": "2026Q2", "from_state": "PLATEAU",
                "to_state": phases.STATE_DECELERATING, "yoy": .5, "delta": -9.0,
                "event_key": "k1"}],
              {"MSFT": {"bucket": "hyperscaler"}})
    assert snapshot.alert_lines(s, prior_keys={"k1"}) == []


def test_a_non_hyperscaler_plateau_does_not_alert():
    s = _snap([{"series_key": "HUT", "quarter": "2026Q2", "from_state": "ACCELERATING",
                "to_state": phases.STATE_PLATEAU, "yoy": .3, "delta": -30.0,
                "event_key": "k5"}], {"HUT": {"bucket": "builder"}})
    assert snapshot.alert_lines(s) == []
