"""CD-GAP2A A4 — commitment-stock deltas as events, and the threshold held.

SMCI's forward commitments went $10.10B -> $34.20B between two snapshots — a
server assembler committing to 3.4x the components — and nothing said so. It was
visible only by diffing two PDFs by hand. That is exactly the forward-demand
event the commitments leg exists to catch.

The threshold is deliberately NOT set here. Measured over 308 observation pairs
across 21 issuers the multiples run p50 1.00x, p90 2.00x, p95 3.20x, and the
tail is dominated by near-zero bases: WULF $0.000B -> $0.118B reads 846x. A bare
multiple would fire on that and call it forward demand.
"""
import pytest

from capex_daemon import phases, snapshot


def _snap(points, concept="PurchaseObligation", bucket="supplier", ticker="SMCI"):
    obs = [{"quarter": p["q"], "state": phases.STATE_PLATEAU} for p in points]
    return {"issuers": {ticker: {
        "bucket": bucket, "state": phases.STATE_PLATEAU, "observations": obs,
        "commitments": {"status": "COVERED", "concept": concept,
                        "points_cq": points}}},
        "buckets": {}, "total": {"observations": obs}, "transitions": []}


SMCI = [{"q": "2026Q1", "value": 10.10e9}, {"q": "2026Q2", "value": 34.20e9}]


def test_the_citing_case_is_measured_correctly():
    d = snapshot.commitment_deltas(_snap(SMCI))[0]
    assert d["ticker"] == "SMCI"
    assert d["multiple"] == pytest.approx(3.386, abs=1e-3)
    assert d["delta"] == pytest.approx(24.10e9)
    assert d["quarters_between"] == 1
    assert d["concept"] == "PurchaseObligation"


def test_the_gap_between_observations_travels_with_the_move():
    """A stock is disclosed on the issuer's own schedule. 3x over one quarter
    and 3x over eight are different facts and must not read the same."""
    far = [{"q": "2024Q2", "value": 10.0e9}, {"q": "2026Q2", "value": 30.0e9}]
    assert snapshot.commitment_deltas(_snap(far))[0]["quarters_between"] == 8


def test_nothing_alerts_while_the_threshold_is_unset():
    """E8: an unset constant is None and its consumer surfaces that."""
    assert snapshot.COMMITMENT_JUMP_MULTIPLE is None
    assert snapshot.COMMITMENT_JUMP_MIN_DELTA is None
    assert snapshot.commitment_alert_lines(_snap(SMCI)) == []


def test_both_bounds_are_required_together():
    """A multiple alone fires on a near-zero base; a floor alone fires on every
    large issuer's routine drift. Supplying one is not enough to arm it."""
    snap = _snap(SMCI)
    assert snapshot.commitment_alert_lines(snap, multiple=2.0) == []
    assert snapshot.commitment_alert_lines(snap, min_delta=1e9) == []
    fired = snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9)
    assert len(fired) == 1 and "3.39x" in fired[0]["reason"]


def test_the_proposed_pair_rejects_a_near_zero_base():
    """WULF, live: $0.000B -> $0.118B is 846x and is not forward demand."""
    wulf = [{"q": "2026Q1", "value": 140_000.0}, {"q": "2026Q2", "value": 118_000_000.0}]
    snap = _snap(wulf, ticker="WULF", bucket="builder")
    assert snapshot.commitment_deltas(snap)[0]["multiple"] > 800
    assert snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9) == []


def test_a_zero_base_yields_no_multiple_rather_than_infinity():
    zero = [{"q": "2026Q1", "value": 0.0}, {"q": "2026Q2", "value": 5.0e9}]
    d = snapshot.commitment_deltas(_snap(zero))[0]
    assert d["multiple"] is None
    assert d["delta"] == 5.0e9
    assert snapshot.commitment_alert_lines(_snap(zero), multiple=2.0,
                                           min_delta=1e9) == []


def test_an_issuer_with_one_observation_has_no_delta():
    assert snapshot.commitment_deltas(_snap([{"q": "2026Q2", "value": 1.0e9}])) == []


def test_deltas_are_frontier_gated_like_transitions():
    """A move recorded years ago is history, not news — the same rule that stops
    a reclassification announcing 2013 as though it just happened."""
    old = [{"q": "2013Q1", "value": 1.0e9}, {"q": "2013Q2", "value": 9.0e9}]
    snap = _snap(old)
    assert snapshot.commitment_deltas(snap)                       # published
    assert snapshot.commitment_deltas(snap, frontier="2026Q1") == []


def test_the_event_key_is_content_derived_so_a_rerun_does_not_realert():
    snap = _snap(SMCI)
    k = snapshot.commitment_deltas(snap)[0]["event_key"]
    assert snapshot.commitment_deltas(snap)[0]["event_key"] == k
    assert snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9,
                                           prior_keys=[k]) == []


# --- the second arm: a large base makes the multiple blind ------------------

META = [{"q": "2026Q1", "value": 237.67e9}, {"q": "2026Q2", "value": 349.31e9}]


def test_a_multiple_gate_is_blind_to_the_largest_move_on_the_panel():
    """META added $111.64B in one quarter at 1.47x — the largest absolute move
    measured, and no defensible multiple catches it. Measured live: META went
    $27.95B -> $349.31B over four quarters and a 2.0x gate sees one step of
    four. This is why the absolute arm exists."""
    d = snapshot.commitment_deltas(_snap(META, concept="ContractualObligation",
                                         bucket="hyperscaler", ticker="META"))[0]
    assert d["delta"] == pytest.approx(111.64e9)
    assert d["multiple"] == pytest.approx(1.47, abs=0.01)
    snap = _snap(META, concept="ContractualObligation", bucket="hyperscaler",
                 ticker="META")
    assert snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9) == []


def test_the_absolute_arm_catches_it():
    snap = _snap(META, concept="ContractualObligation", bucket="hyperscaler",
                 ticker="META")
    fired = snapshot.commitment_alert_lines(snap, absolute=20e9)
    assert len(fired) == 1
    assert fired[0]["armed_by"] == "absolute"


def test_the_absolute_arm_does_not_rescue_a_near_zero_base():
    """WULF's 846x move is +$0.118B — below any absolute floor worth setting."""
    wulf = [{"q": "2026Q1", "value": 140_000.0}, {"q": "2026Q2", "value": 118_000_000.0}]
    snap = _snap(wulf, ticker="WULF", bucket="builder")
    assert snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9,
                                           absolute=20e9) == []


def test_an_unarmed_arm_does_not_disable_the_other():
    snap = _snap(SMCI)
    assert snapshot.commitment_alert_lines(snap, multiple=2.0, min_delta=1e9)
    assert snapshot.commitment_alert_lines(snap, absolute=20e9)
    assert snapshot.commitment_alert_lines(snap) == []      # neither armed


def test_a_zero_base_move_can_still_fire_on_size_alone():
    """AVGO went $0.05B -> $128.11B. The multiple is meaningless there; the
    size is not."""
    avgo = [{"q": "2026Q1", "value": 0.0}, {"q": "2026Q2", "value": 128.11e9}]
    snap = _snap(avgo, ticker="AVGO")
    fired = snapshot.commitment_alert_lines(snap, absolute=20e9)
    assert len(fired) == 1 and "from a zero base" in fired[0]["reason"]
