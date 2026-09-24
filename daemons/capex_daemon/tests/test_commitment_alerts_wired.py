"""P2 C4 — the commitment alert reaches the queue, once, and only if it may.

This is the unit that closes a five-time pattern. `commitment_alert_lines` was
built, unit-tested, ratified and rendered on the Brief's since-page — and
`scan.run` enqueued only `alert_lines`, so no commitment move could ever reach
attention. A ratified behaviour is not built until its sink is connected.

Tested against the REAL AlertQueue, not a mock: the claim is that a commitment
move arrives exactly once and that a NOT-A-COMMITMENT move never arrives at all,
and a mock would assert my belief about the queue instead of the fact.
"""
import sqlite3

import pytest

from capex_daemon import alerts, commitments, phases, snapshot, storage


def _q(tmp_path):
    from abelard_common.alert_queue import AlertQueue
    return AlertQueue(tmp_path / "queue.db")


def _snap(ticker, points, concept="PurchaseObligation", bucket="builder"):
    obs = [{"quarter": p["q"], "state": phases.STATE_PLATEAU} for p in points]
    return {"issuers": {ticker: {
        "ticker": ticker, "bucket": bucket, "state": phases.STATE_PLATEAU,
        "observations": obs,
        "commitments": {"status": "COVERED", "concept": concept,
                        "points_cq": points}}},
        "buckets": {}, "total": {"latest_quarter": points[-1]["q"],
                                 "observations": obs},
        "transitions": []}


# A move that must alert: SUPPLY+CAPEX, above the ratified absolute arm.
BUILDOUT = _snap("MARA", [{"q": "2026Q1", "value": 10.0e9},
                          {"q": "2026Q2", "value": 40.0e9}])
# The same move at an issuer whose figure is lease payments it RECEIVES.
NOT_A_COMMITMENT = _snap("CORZ", [{"q": "2026Q1", "value": 10.0e9},
                                  {"q": "2026Q2", "value": 40.0e9}])


def test_a_buildout_move_reaches_the_queue_exactly_once(tmp_path):
    q = _q(tmp_path)
    try:
        rows, _quar = snapshot.commitment_alerts_and_quarantine(BUILDOUT)
        assert len(rows) == 1
        first = alerts.enqueue_commitment_alerts(rows, queue=q)
        second = alerts.enqueue_commitment_alerts(rows, queue=q)   # re-derived
        assert first == (1, 0)
        assert second == (0, 1)
        items = [r for r in q.items() if r.kind == alerts.KIND_COMMITMENT_MOVE]
        assert len(items) == 1
        assert items[0].topic_key == "issuer:MARA"
    finally:
        q.close()


def test_the_class_travels_with_the_alert(tmp_path):
    """A commitment figure that arrives without saying what it contains is how
    lessor receipts reached the buildout read in the first place."""
    q = _q(tmp_path)
    try:
        rows, _ = snapshot.commitment_alerts_and_quarantine(BUILDOUT)
        alerts.enqueue_commitment_alerts(rows, queue=q)
        item = [r for r in q.items() if r.kind == alerts.KIND_COMMITMENT_MOVE][0]
        assert item.payload["basis_class"] == "SUPPLY+CAPEX"
        assert item.payload["ticker"] == "MARA"
        assert item.payload["from_q"] == "2026Q1" and item.payload["to_q"] == "2026Q2"
    finally:
        q.close()


def test_a_not_a_commitment_move_never_reaches_the_queue(tmp_path):
    """The identical move, at CORZ, whose figure is lease payments it RECEIVES."""
    q = _q(tmp_path)
    try:
        rows, quarantined = snapshot.commitment_alerts_and_quarantine(NOT_A_COMMITMENT)
        assert rows == [] and quarantined == []
        assert alerts.enqueue_commitment_alerts(rows, queue=q) == (0, 0)
        assert [r for r in q.items() if r.kind == alerts.KIND_COMMITMENT_MOVE] == []
    finally:
        q.close()


def test_the_dedupe_key_is_the_event_key_unmodified(tmp_path):
    q = _q(tmp_path)
    try:
        rows, _ = snapshot.commitment_alerts_and_quarantine(BUILDOUT)
        alerts.enqueue_commitment_alerts(rows, queue=q)
        item = [r for r in q.items() if r.kind == alerts.KIND_COMMITMENT_MOVE][0]
        assert item.dedupe_key == rows[0]["event_key"]
        assert rows[0]["event_key"].startswith("commit:MARA:2026Q1:2026Q2:")
    finally:
        q.close()


# --- the event store: what makes the second night quiet ---------------------

@pytest.fixture()
def con():
    c = sqlite3.connect(":memory:")
    storage.initialise(c) if hasattr(storage, "initialise") else c.executescript(
        storage.SCHEMA)
    return c


def test_every_delta_is_recorded_not_only_the_alerting_ones(con):
    """A threshold change or a newly ruled class must not announce old moves.
    GOOGL's content-licensing move cannot alert, and is still recorded — so if
    GOOGL were ever reclassified, its history would not arrive as news."""
    snap = _snap("GOOGL", [{"q": "2026Q1", "value": 1.0e9},
                           {"q": "2026Q2", "value": 2.0e9}])
    deltas = snapshot.commitment_deltas(snap)
    assert snapshot.commitment_alert_lines(snap) == []          # cannot alert
    written = commitments.record_commitment_events(con, deltas)
    assert len(written) == 1
    assert con.execute("SELECT basis_class FROM commitment_events").fetchone()[0] \
        == "CONTENT-ENERGY-SOFTWARE"


def test_recording_is_idempotent_on_the_content_key(con):
    deltas = snapshot.commitment_deltas(BUILDOUT)
    assert len(commitments.record_commitment_events(con, deltas)) == 1
    assert commitments.record_commitment_events(con, deltas) == []
    assert con.execute("SELECT COUNT(*) FROM commitment_events").fetchone()[0] == 1


def test_the_first_run_is_silent_and_the_second_is_not(con, tmp_path):
    """R2 in full: the first run of a newly wired alert backfills silently, the
    next real move announces itself."""
    q = _q(tmp_path)
    try:
        prior = {r[0] for r in con.execute("SELECT event_key FROM commitment_events")}
        first_run = not prior
        rows, _ = snapshot.commitment_alerts_and_quarantine(BUILDOUT, prior_keys=prior)
        commitments.record_commitment_events(con, snapshot.commitment_deltas(BUILDOUT))
        if first_run:
            rows = []
        assert alerts.enqueue_commitment_alerts(rows, queue=q) == (0, 0)

        # Night two: a NEW move at the same issuer.
        later = _snap("MARA", [{"q": "2026Q2", "value": 40.0e9},
                               {"q": "2026Q3", "value": 90.0e9}])
        prior = {r[0] for r in con.execute("SELECT event_key FROM commitment_events")}
        assert prior                                    # no longer a first run
        rows, _ = snapshot.commitment_alerts_and_quarantine(later, prior_keys=prior)
        assert len(rows) == 1 and rows[0]["to_q"] == "2026Q3"
        assert alerts.enqueue_commitment_alerts(rows, queue=q) == (1, 0)
    finally:
        q.close()


def test_the_scan_wires_all_three_pieces():
    """The grep this whole unit exists to satisfy: the CALLER, not the
    definition. Recording, gating and enqueuing must all appear on the
    production path."""
    import inspect

    from capex_daemon import scan
    src = inspect.getsource(scan.run)
    assert "record_commitment_events" in src
    assert "commitment_alerts_and_quarantine" in src
    assert "enqueue_commitment_alerts" in src
    assert "first_commit_run" in src
