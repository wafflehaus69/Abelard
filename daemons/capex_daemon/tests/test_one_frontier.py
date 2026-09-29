"""P2 C3 / ruling R3 — there is ONE frontier, and it is the coverage rule.

Two definitions of "the frontier" lived in this package at once. The aggregate
used coverage: a trailing quarter joins when its reporters cover 95% of the last
accepted quarter's dollars. The alert gate used arrival: the newest quarter any
classified series had reached, minus one.

They disagree exactly when it matters. On 2026-09-12 Oracle alone reached
2026Q3. The coverage rule correctly HELD 2026Q3 — one issuer is not the panel.
The arrival rule moved the alert gate to 2026Q2, so every transition the rest of
the panel had at 2026Q2 fell outside the window and went unannounced. One early
filer silenced the panel.

These tests pin the single definition in both directions: the gate follows the
published frontier, and nothing in the package derives a frontier from issuer
arrival again.
"""
import pathlib
import re

import pytest

from capex_daemon import guards, snapshot, trend


def _snap(total_latest, issuer_quarters, total_obs=("2026Q1", "2026Q2")):
    """A panel where an issuer has run ahead of the published total."""
    return {
        "issuers": {"EARLY": {"observations": [{"quarter": q, "state": "PLATEAU"}
                                               for q in issuer_quarters]}},
        "buckets": {},
        "total": {"latest_quarter": total_latest,
                  "observations": [{"quarter": q, "state": "PLATEAU"}
                                   for q in total_obs]},
        "transitions": [],
    }


def test_the_alert_gate_follows_the_published_frontier():
    """2026Q2 published, one quarter of slack -> 2026Q1."""
    assert snapshot._frontier_quarter(_snap("2026Q2", ["2026Q3"])) == "2026Q1"


def test_one_early_filer_does_not_move_the_gate():
    """The citing case. ORCL alone at 2026Q3 must not drag the window forward
    and silence the rest of the panel at 2026Q2."""
    early = _snap("2026Q2", ["2026Q1", "2026Q2", "2026Q3"])
    quiet = _snap("2026Q2", ["2026Q1", "2026Q2"])
    assert snapshot._frontier_quarter(early) == snapshot._frontier_quarter(quiet)


def test_a_transition_at_the_panel_frontier_still_alerts():
    snap = _snap("2026Q2", ["2026Q3"])
    snap["transitions"] = [{"series_key": "bucket:builder", "quarter": "2026Q2",
                            "from_state": "PLATEAU", "to_state": "ACCELERATING",
                            "yoy": .5, "delta": 30.0, "event_key": "now"}]
    assert [a["event_key"] for a in snapshot.alert_lines(snap)] == ["now"]


def test_history_still_does_not_alert():
    snap = _snap("2026Q2", ["2026Q3"])
    snap["transitions"] = [{"series_key": "bucket:builder", "quarter": "2013Q3",
                            "from_state": "CONTRACTING", "to_state": "PLATEAU",
                            "yoy": .1, "delta": 9.0, "event_key": "old"}]
    assert snapshot.alert_lines(snap) == []


def test_no_published_total_means_no_gate_rather_than_an_invented_one():
    """A panel under its membership floor publishes no total. The honest answer
    is "there is no frontier", not one reconstructed from whoever filed."""
    snap = {"issuers": {"EARLY": {"observations": [{"quarter": "2026Q3",
                                                    "state": "PLATEAU"}]}},
            "buckets": {}, "total": {}, "transitions": []}
    assert snapshot._frontier_quarter(snap) is None


def test_commitment_deltas_are_gated_by_the_same_frontier():
    """One gate for every event type — a commitment move and a phase transition
    answer to the same quarter."""
    snap = _snap("2026Q2", ["2026Q2"])
    snap["issuers"]["MARA"] = {
        "bucket": "builder",
        "commitments": {"status": "COVERED", "concept": "PurchaseObligation",
                        "points_cq": [{"q": "2013Q1", "value": 1.0e9},
                                      {"q": "2013Q2", "value": 9.0e9}]}}
    assert snapshot.commitment_deltas(snap)                      # published
    assert snapshot.commitment_alert_lines(snap) == []           # not alerted


# --- the structural half: no second definition may reappear -----------------

SRC = pathlib.Path(snapshot.__file__).parent


def _code_only(text):
    """Source with comments and string literals removed.

    The removed rule is DESCRIBED in half a dozen docstrings on purpose — that
    is the record of why it went. A structural test that cannot tell a comment
    from a statement would forbid writing the history down.
    """
    import io
    import tokenize
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type in (tokenize.COMMENT, tokenize.STRING):
            continue
        out.append("{}:{}".format(tok.start[0], tok.string))
    return out


def test_the_alert_gate_reads_the_total_and_nothing_else():
    """The structural half of R3. The gate may read the published total; the
    moment it reads `issuers` or `buckets` again it is deriving a frontier from
    arrival order, which is the second definition this unit removed."""
    import inspect
    code = " ".join(s.split(":", 1)[1] for s in
                    _code_only(inspect.getsource(snapshot._frontier_quarter)))
    assert "total" in code
    assert "issuers" not in code, "the alert gate is reading issuer arrival again"
    assert "buckets" not in code, "the alert gate is reading bucket arrival again"


def test_only_one_function_takes_the_alert_lookback():
    """A second gate would need its own slack constant. There is one."""
    text = pathlib.Path(snapshot.__file__).read_text(encoding="utf-8")
    uses = [s for s in _code_only(text) if "ALERT_LOOKBACK_QUARTERS" in s]
    assert len(uses) == 2, uses          # the definition, and the one signature


def test_only_trend_decides_which_quarters_publish():
    """`frontier_split` is the decision; every other module reads its output."""
    deciders = []
    for path in sorted(SRC.glob("*.py")):
        if path.name == "trend.py":
            continue
        code = " ".join(s.split(":", 1)[1] for s in
                        _code_only(path.read_text(encoding="utf-8")))
        if "def frontier_split" in code or "COVERAGE_FLOOR =" in code:
            deciders.append(path.name)
    assert deciders == [], "a second frontier decision lives in " + ", ".join(deciders)


def test_the_coverage_floor_has_exactly_one_definition():
    """Pinned when the frontier was built, restated here because R3 turned it
    from a property of the aggregate into a property of the whole daemon."""
    assert trend.FRONTIER_COVERAGE_FLOOR is trend.COVERAGE_FLOOR
    text = pathlib.Path(trend.__file__).read_text(encoding="utf-8")
    assert text.count("COVERAGE_FLOOR = ") == 2          # the value, and the alias
    assert "FRONTIER_COVERAGE_FLOOR = COVERAGE_FLOOR" in text


def test_every_frontier_gated_path_is_declared_in_the_guards_table():
    gated = [k for k, g in guards.GUARDS.items() if g.get("frontier")]
    assert "total" in gated and "buckets.*" in gated
    assert guards.GUARDS["alerts"]["frontier"] is True
