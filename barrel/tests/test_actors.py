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
           ["cex", "nonpersonal", "dedicated", "hub", "unknown", None]


def test_latency_is_none_when_unmeasured_never_zero():
    assert actors.fund_to_first_buy_s([row("a"), row("b")]) is None
    assert actors.fund_to_first_buy_s([row("a", lat=10), row("b", lat=30), row("c")]) == 20


def test_block_is_keyed_by_address_alone():
    assert actors.block_id(row("cr", "F1", 3), "cr", **KW) == "F1"
    for r in (row("cr", "EXCH", 3), row("cr", "BIG", 5000), row("cr"), None):
        assert actors.block_id(r, "cr", **KW) == "cr"
    # R2: a wallet that creates one token and funds the creator of another is ONE block
    assert actors.block_id(row("X", "EXCH", 3), "X", **KW) == actors.block_id(row("W", "X", 3), "W", **KW) == "X"


def test_label_beats_fan_out():
    assert actors.classify_funder("EXCH", 1, **KW) == "cex" and actors.classify_funder("EXCH", 10**6, **KW) == "cex"


def test_a_factory_links_where_a_hub_does_not():
    rows = [row("a", "BIG", 5000), row("b", "BIG", 5000)]
    assert actors.resolution.actor_count(actors.token_record(rows, **KW)) == 2
    rec = actors.token_record(rows, factories={"BIG"}, **KW)
    assert actors.resolution.actor_count(rec) == 1 and rec["collapse_state"] == "collapsed"
    assert actors.block_id(row("cr", "BIG", 5000), "cr", factories={"BIG"}, **KW) == "BIG"


def test_grouped_rows_give_the_same_record_as_member_rows():
    members = [row("a", "F1", 3), row("b", "F1", 3), row("c", "BIG", 5000), row("d", "BIG", 5000), row("e", "EXCH", 2)]
    groups = [{"funder": "F1", "fan_out": 3, "n_members": 2}, {"funder": "BIG", "fan_out": 5000, "n_members": 2},
              {"funder": "EXCH", "fan_out": 2, "n_members": 1}]
    a, b = actors.token_record(members, **KW), actors.token_record_grouped(groups, **KW)
    assert (a["n_wallets"], a[actors.ACTORS_KEY], a["collapse_state"]) == (b["n_wallets"], b[actors.ACTORS_KEY], b["collapse_state"]) == (5, 4, "collapsed")
    unk = actors.token_record_grouped(groups + [{"funder": None, "fan_out": None, "n_members": 3}], **KW)
    assert actors.resolution.actor_count(unk) is None and unk["collapse_state"] == "unresolved" and unk["n_wallets"] == 8


def test_a_linking_funder_split_across_groups_is_one_actor():
    # found on 2025-06-09: the same funder behind a creator-funded wallet and 16 bundle-only wallets
    groups = [{"funder": "HUB", "fan_out": 11240, "n_members": 1, "b_only_n": None},
              {"funder": "F1", "fan_out": 19, "n_members": 1, "b_only_n": None},
              {"funder": "F1", "fan_out": 19, "n_members": 16, "b_only_n": 17}]
    rec = actors.token_record_grouped(groups, **KW)
    assert (rec["n_wallets"], actors.resolution.actor_count(rec)) == (18, 2)


def test_bundle_cut_is_local_and_the_same_for_member_and_group_rows():
    members = [{"w": "cr", "is_creator": True, "is_funded": False, "bundle_n": None},
               {"w": "f1", "is_creator": False, "is_funded": True, "bundle_n": 2},
               {"w": "b1", "is_creator": False, "is_funded": False, "bundle_n": 5},
               {"w": "b2", "is_creator": False, "is_funded": False, "bundle_n": 4}]
    assert [m["w"] for m in actors.aligned(members)] == ["cr", "f1", "b1"]
    groups = [{"b_only_n": None, "n_members": 2}, {"b_only_n": 5, "n_members": 1}, {"b_only_n": 4, "n_members": 1}]
    assert sum(g["n_members"] for g in actors.aligned(groups)) == 3
    assert sum(g["n_members"] for g in actors.aligned(groups, bundle_min=4)) == 4


def test_in_aligned_set_refuses_a_row_it_does_not_recognise():
    with pytest.raises(KeyError):
        actors.in_aligned_set({"w": "a", "funder": "F1"})
    assert actors.in_aligned_set({"is_creator": False, "is_funded": False, "is_bundle": True})   # pre-MR-14 member row


def test_latency_from_group_rows_is_the_median_across_members():
    groups = [{"lat_s": [10, 30]}, {"lat_s": [50]}, {"lat_s": None}]
    assert actors.fund_to_first_buy_s(groups) == 30
    assert actors.fund_to_first_buy_s([{"lat_s": None}, {"lat_s": []}]) is None


def test_r1_a_creator_and_the_wallets_it_funded_are_one_actor():
    # creator came from an exchange; it funded two wallets and its own fan-out is small, so it links
    members = [row("cr", "EXCH", 2), row("a", "cr", 3), row("b", "cr", 3)]
    assert actors.resolution.actor_count(actors.token_record(members, **KW)) == 1
    groups = [{"funder": "EXCH", "fan_out": 2, "n_members": 1, "b_only_n": None, "creator": "cr", "n_creator": 1},
              {"funder": "cr", "fan_out": 3, "n_members": 2, "b_only_n": None, "creator": "cr", "n_creator": 0}]
    mfs = [{"member": "cr", "funder": "EXCH", "fan_out": 2, "b_only_n": None}]
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, mfs, **KW)) == 1
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, **KW)) == 1      # creator rebuilt from the groups


def test_r1_chain_and_non_creator_member_funder():
    # F1 links cr and a; a links b; so cr, a, b are one actor. c stands alone behind a hub.
    members = [row("cr", "F1", 3), row("a", "F1", 3), row("b", "a", 2), row("c", "BIG", 5000)]
    assert actors.resolution.actor_count(actors.token_record(members, **KW)) == 2
    groups = [{"funder": "F1", "fan_out": 3, "n_members": 2, "b_only_n": None, "creator": "cr", "n_creator": 1},
              {"funder": "a", "fan_out": 2, "n_members": 1, "b_only_n": None, "creator": "cr", "n_creator": 0},
              {"funder": "BIG", "fan_out": 5000, "n_members": 1, "b_only_n": None, "creator": "cr", "n_creator": 0}]
    mfs = [{"member": "a", "funder": "F1", "fan_out": 3, "b_only_n": None}]
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, mfs, **KW)) == 2


def test_r1_does_not_merge_through_a_funder_that_does_not_link():
    # the creator funds 5,000 wallets: a hub by fan-out, so nothing is merged (the factory class would)
    members = [row("cr", "EXCH", 2), row("a", "cr", 5000), row("b", "cr", 5000)]
    assert actors.resolution.actor_count(actors.token_record(members, **KW)) == 3


def test_fan_rate_table_replaces_the_row_value_and_absence_is_a_measured_zero():
    members = [row("a", "F1", 5000), row("b", "F1", 5000)]
    assert actors.resolution.actor_count(actors.token_record(members, **KW)) == 2
    assert actors.resolution.actor_count(actors.token_record(members, rates={"F1": 3.0}, **KW)) == 1
    assert actors.resolution.actor_count(actors.token_record(members, rates={}, **KW)) == 1
    assert actors.resolution.actor_count(actors.token_record(members, rates={"F1": 900.0}, **KW)) == 2


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
