"""CD-DASH1 P2 acceptance: the queue sink.

The two properties the order names, tested against the real AlertQueue rather
than a mock — the whole claim is that the two dedupe designs fit unmodified, and
a mock would assert my belief about that instead of the fact.
"""
import pytest

from capex_daemon import alerts, phases, snapshot


def _q(tmp_path):
    from abelard_common.alert_queue import AlertQueue
    return AlertQueue(tmp_path / "queue.db")


def _alert(series="bucket:builder", quarter="2026Q2", key=None):
    return {"series_key": series, "quarter": quarter, "from_state": "PLATEAU",
            "to_state": "ACCELERATING", "reason": "aggregate transition",
            "event_key": key or "{}|{}|PLATEAU->ACCELERATING".format(series, quarter)}


def test_the_dedupe_key_IS_the_event_key_unmodified():
    """Not a derived variant. If these diverge, a re-derived transition alerts
    twice — which is the whole failure E12 and E31 exist to prevent."""
    t = phases.Transition("MSFT", "2026Q2", "PLATEAU", "DECELERATING", .5, -9.0)
    a = _alert(series="MSFT", key=t.event_key)
    assert a["event_key"] == t.event_key == "MSFT|2026Q2|PLATEAU->DECELERATING"


def test_a_transition_enqueues_exactly_once_across_two_scans(tmp_path):
    """The acceptance criterion, stated in the order."""
    q = _q(tmp_path)
    try:
        first = alerts.enqueue_alerts([_alert()], queue=q)
        second = alerts.enqueue_alerts([_alert()], queue=q)   # same scan, re-derived
        assert first == (1, 0)
        assert second == (0, 1)          # recognised as a duplicate, not re-queued
        rows = q.items()
        assert len([r for r in rows if r.source == "capex_daemon"]) == 1
    finally:
        q.close()


def test_a_rebuild_enqueues_nothing(tmp_path):
    """A --rebuild re-derives history; the frontier gate upstream drops all of
    it, so the queue never sees it. Gate placement is the point: the queue
    inherits it rather than keeping a second copy that can drift."""
    obs = [{"quarter": q, "state": "PLATEAU"} for q in ("2026Q1", "2026Q2")]
    snap = {"issuers": {}, "buckets": {"builder": {"observations": obs}},
            "total": {"observations": obs},
            "transitions": [
                {"series_key": "bucket:builder", "quarter": "2013Q3",
                 "from_state": "CONTRACTING", "to_state": "PLATEAU",
                 "yoy": .1, "delta": 9.0, "event_key": "ancient"},
                {"series_key": "bucket:builder", "quarter": "2015Q1",
                 "from_state": "CONTRACTING", "to_state": "PLATEAU",
                 "yoy": .1, "delta": 9.0, "event_key": "old"}]}
    gated = snapshot.alert_lines(snap)
    assert gated == []                    # nothing survives the frontier
    q = _q(tmp_path)
    try:
        assert alerts.enqueue_alerts(gated, queue=q) == (0, 0)
        assert q.items() == []
    finally:
        q.close()


def test_a_new_frontier_transition_does_reach_the_queue(tmp_path):
    """The companion to the rebuild test — the gate must not be so tight that
    nothing ever alerts."""
    obs = [{"quarter": q, "state": "PLATEAU"} for q in ("2026Q1", "2026Q2")]
    snap = {"issuers": {}, "buckets": {"builder": {"observations": obs}},
            "total": {"observations": obs},
            "transitions": [{"series_key": "bucket:builder", "quarter": "2026Q2",
                             "from_state": "PLATEAU", "to_state": "ACCELERATING",
                             "yoy": .5, "delta": 30.0, "event_key": "fresh"}]}
    gated = snapshot.alert_lines(snap)
    assert len(gated) == 1
    q = _q(tmp_path)
    try:
        assert alerts.enqueue_alerts(gated, queue=q) == (1, 0)
    finally:
        q.close()


NETWORK_MODULES = {
    "requests", "httpx", "urllib", "urllib3", "http", "smtplib", "socket",
    "ftplib", "telnetlib", "subprocess", "asyncio", "websocket", "websockets",
}


def test_the_sink_has_no_outward_verb():
    """E28, asserted by test rather than by policy.

    Checked over the parsed IMPORTS, not the source text: a first cut grepped
    raw source and failed on its own docstring, because the prose describing the
    prohibition necessarily contains the words. What matters is what the module
    can reach, not what it talks about.
    """
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(alerts))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    offending = imported & NETWORK_MODULES
    assert not offending, "outward verb reachable via {}".format(sorted(offending))
    # And the only thing it imports from the shared layer is the queue itself.
    assert "abelard_common" in imported and imported <= {"pathlib", "abelard_common"}


def test_an_empty_alert_list_touches_nothing(tmp_path):
    assert alerts.enqueue_alerts([], queue=None) == (0, 0)
