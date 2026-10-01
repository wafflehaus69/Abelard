"""barrel/recon/actors.py: collapse, classification, and the unresolved state.
Run: python -m pytest barrel/tests/test_actors.py"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import actors  # noqa: E402

KW = dict(fanout_threshold=100, labelled_cex={"EXCH"}, nonpersonal={"FEEACCT"})


def row(w, funder=None, fan_out=None, lat=None):
    return {"w": w, "funder": funder, "fan_out": fan_out, "fund_to_first_buy_s": lat}


def test_resolution_is_the_consensus_module_not_a_copy():
    assert pathlib.Path(actors.resolution.__file__).parts[-3:] == ("consensus", "consensus", "resolution.py")


def test_shared_dedicated_funder_is_one_actor():
    rec = actors.token_record([row("a", "F1", 3), row("b", "F1", 3), row("c", "F2", 5)], **KW)
    assert rec["n_wallets"] == 3 and actors.resolution.actor_count(rec) == 2
    assert rec["collapse_state"] == "collapsed" and rec["u_codes"] == []


@pytest.mark.parametrize("funder,fan", [("EXCH", 2), ("BIG", 100), ("FEEACCT", 1), ("NOFAN", None)])
def test_only_a_dedicated_funder_links(funder, fan):
    rec = actors.token_record([row("a", funder, fan), row("b", funder, fan)], **KW)
    assert actors.resolution.actor_count(rec) == 2 and rec["collapse_state"] == "independent"


def test_one_unfunded_member_makes_the_set_unresolved():
    rec = actors.token_record([row("a", "F1", 3), row("b")], **KW)
    assert actors.resolution.actor_count(rec) is None
    assert rec["collapse_state"] == "unresolved" and rec["u_codes"] == [actors.U_SET_FUNDING]
    assert actors.resolution.fmt(actors.resolution.actor_count(rec)) == "UNRESOLVED"


def test_single_wallet_with_unknown_funding_is_not_one_actor():
    rec = actors.token_record([row("a")], **KW)
    assert rec["collapse_state"] == "unresolved"


def test_kinds():
    c = lambda f, n: actors.classify_funder(f, n, **KW)  # noqa: E731
    assert [c("EXCH", 1), c("FEEACCT", 9), c("x", 99), c("x", 100), c("x", None), c(None, 5)] == \
           ["cex", "nonpersonal", "dedicated", "high_fanout", "unknown", None]


def test_latency_is_none_when_unmeasured_never_zero():
    assert actors.fund_to_first_buy_s([row("a"), row("b")]) is None
    assert actors.fund_to_first_buy_s([row("a", lat=10), row("b", lat=30), row("c")]) == 20


def test_block_id_falls_back_to_launch_day():
    assert actors.block_id(row("cr", "F1", 3), "2025-06-09", **KW) == "F:F1"
    for r in (row("cr", "EXCH", 3), row("cr", "BIG", 5000), row("cr"), None):
        assert actors.block_id(r, "2025-06-09", **KW) == "D:2025-06-09"


def test_parity_with_consensus_m10():
    root = pathlib.Path(__file__).resolve().parents[2]
    sys.path[:0] = [str(root / "consensus"), str(root / "daemons" / "common")]
    m10 = pytest.importorskip("consensus.m10", reason="CONSENSUS dependencies not installed here")
    cases = [{}, {"a": None}, {"a": {"funder": "x", "funder_kind": "dedicated"}, "b": {"funder": "x", "funder_kind": "dedicated"}},
             {"a": {"funder": "x", "funder_kind": "cex"}, "b": {"funder": "x", "funder_kind": "cex"}},
             {"a": {"funder": "x", "funder_kind": "dedicated"}, "b": {"error": "e"}},
             {"a": {"funder": None, "funder_kind": "dedicated"}, "b": {"funder": "y", "funder_kind": "unknown"}}]
    for c in cases:
        assert actors.collapse_actors(c) == m10.collapse_actors(c)
