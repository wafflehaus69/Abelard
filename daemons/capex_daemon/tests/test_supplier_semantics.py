"""CD-GAP2A A2 — a supplier's phase is its DATACENTER REVENUE, not its capex.

An earlier audit recorded NVDA's capex DECELERATING-CONFIRMED as "the supply
side's biggest name confirmed its rate-bend". That reads a buildout signal off
the wrong series: NVDA's own capex is ~$7.4B of offices and test equipment,
while the buildout is the $277.8B of datacenter revenue it books from the
hyperscalers — PLATEAU at +89.5%, not decelerating.

The layout invited it. The phase board gave supplier capex the same weight and
the same look as a hyperscaler's, and the dcrev phase lived on another page.
Two figures differing by a factor of 37 should not be readable as one claim.
"""
from capex_daemon import phases, snapshot, svgcharts

from .test_charts import _fake_snapshot, qs


def _with_supplier(snap, dc_state=phases.STATE_PLATEAU,
                   capex_state=phases.STATE_DECELERATING):
    q = qs(2019, 1, 20)
    dc_obs = [{"quarter": x, "yoy": 0.895, "delta": -1.0, "direction": "down",
               "state": dc_state, "flags": [], "quarters_in_state": 3,
               "entered": q[0]} for x in q]
    cap_obs = [{"quarter": x, "yoy": 0.605, "delta": -26.2, "direction": "down",
                "state": capex_state, "flags": ["CONFIRMED"],
                "quarters_in_state": 3, "entered": q[0]} for x in q]
    snap["issuers"]["NVDA"] = dict(snap["issuers"]["AA"], ticker="NVDA",
                                   bucket="supplier", state=capex_state,
                                   observations=cap_obs)
    snap["suppliers"]["legs"]["NVDA"]["dc_observations"] = dc_obs
    return snap


def test_the_supplier_row_carries_its_datacenter_phase_not_its_capex_phase():
    snap = _with_supplier(_fake_snapshot())
    rows, _q = svgcharts.issuer_rows_for_grid(snap)
    by_key = {k: (states, sub) for k, states, sub in rows}
    assert "NVDA" in by_key
    states, sub = by_key["NVDA"]
    assert set(states.values()) == {phases.STATE_PLATEAU}      # the dcrev phase
    assert "DATACENTER REVENUE" in sub


def test_the_supplier_capex_row_survives_but_is_labelled_as_not_a_signal():
    """Demoted, not hidden — it is still a real series, just not this one."""
    snap = _with_supplier(_fake_snapshot())
    rows, _q = svgcharts.issuer_rows_for_grid(snap)
    by_key = {k: (states, sub) for k, states, sub in rows}
    key = "NVDA" + svgcharts.SUPPLIER_CAPEX_SUFFIX
    assert key in by_key
    states, sub = by_key[key]
    assert set(states.values()) == {phases.STATE_DECELERATING}
    assert "not a buildout signal" in sub


def test_a_non_supplier_row_is_untouched():
    snap = _with_supplier(_fake_snapshot())
    keys = [k for k, _s, _sub in svgcharts.issuer_rows_for_grid(snap)[0]]
    assert "AA" in keys
    assert "AA" + svgcharts.SUPPLIER_CAPEX_SUFFIX not in keys


# --- the alert analog ------------------------------------------------------

def _snap_with_transition(series_key, to_state):
    obs = [{"quarter": q, "state": to_state} for q in ("2026Q1", "2026Q2")]
    return {"issuers": {}, "buckets": {}, "total": {"observations": obs},
            "transitions": [{"series_key": series_key, "quarter": "2026Q2",
                             "from_state": phases.STATE_PLATEAU,
                             "to_state": to_state, "yoy": 0.4, "delta": -9.0,
                             "event_key": "k1"}]}


def test_supplier_datacenter_revenue_decelerating_alerts():
    snap = _snap_with_transition("dcrev:NVDA", phases.STATE_DECELERATING)
    alerts = snapshot.alert_lines(snap)
    assert [a["reason"] for a in alerts] == [
        "supplier datacenter revenue entering DECELERATING"]


def test_supplier_own_capex_decelerating_does_not_alert():
    """The rule is keyed on dcrev, never on capex. A supplier's capex bending
    is not a claim about the buildout and must not be announced as one."""
    snap = _snap_with_transition("NVDA", phases.STATE_DECELERATING)
    snap["issuers"] = {"NVDA": {"bucket": "supplier", "state": phases.STATE_DECELERATING}}
    assert snapshot.alert_lines(snap) == []


def test_datacenter_revenue_accelerating_does_not_alert():
    """The analog mirrors the hyperscaler rule, which fires on DECELERATING
    only — good news is not an alert."""
    snap = _snap_with_transition("dcrev:NVDA", phases.STATE_ACCELERATING)
    assert snapshot.alert_lines(snap) == []
