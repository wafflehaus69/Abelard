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


def test_fan_rate_table_replaces_the_row_value_and_only_an_asked_funder_is_a_measured_zero():
    members = [row("a", "F1", 5000), row("b", "F1", 5000)]
    assert actors.resolution.actor_count(actors.token_record(members, **KW)) == 2
    assert actors.resolution.actor_count(actors.token_record(members, rates={"F1": 3.0}, **KW)) == 1
    assert actors.resolution.actor_count(actors.token_record(members, rates={"F1": 900.0}, **KW)) == 2
    # a funder the table does not cover was never measured: 'unknown', which never links
    assert actors.resolution.actor_count(actors.token_record(members, rates={}, **KW)) == 2
    # asked about and absent from the query's rows: sent nothing in the window, a measured zero, links
    assert actors.resolution.actor_count(actors.token_record(members, rates=actors.fan_rates([], ["F1"]), **KW)) == 1


def test_fan_rates_is_recipients_per_day_and_refuses_rows_for_a_funder_not_asked():
    rates = actors.fan_rates([{"funder": "F1", "recipients": 62, "window_days": 31}], ["F1", "F2"])
    assert rates == {"F1": 2.0, "F2": 0.0}
    with pytest.raises(ValueError):
        actors.fan_rates([{"funder": "ZZ", "recipients": 1, "window_days": 31}], ["F1"])


def test_rows_shaped_as_the_query_returns_them_go_straight_through_split_grouped():
    # on a member_funder row the query's funder / b_only_n are NULL; the member's own are in member_own_*
    rows = [{"row_kind": "group", "mint": "T", "funder": "EXCH", "b_only_n": None, "member": None, "n_members": 1, "creator": "cr", "n_creator": 1},
            {"row_kind": "group", "mint": "T", "funder": "cr", "b_only_n": None, "member": None, "n_members": 2, "creator": "cr", "n_creator": 0},
            {"row_kind": "member_funder", "mint": "T", "funder": None, "b_only_n": None, "member": "cr",
             "member_own_funder": "EXCH", "member_b_only_n": None, "n_members": 1}]
    groups, mfs = actors.split_grouped(rows)
    assert mfs[0]["funder"] == "EXCH" and len(groups) == 2
    rec = actors.token_record_grouped(groups, mfs, rates={"EXCH": 5.0, "cr": 2.0}, **KW)
    assert actors.resolution.actor_count(rec) == 1          # cr and the two wallets it funded
    # a member-funder that is only a bundle candidate below the cut is not in the set: nothing is merged through it
    rows[2]["member_b_only_n"] = 3
    groups, mfs = actors.split_grouped(rows)
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, mfs, rates={"EXCH": 5.0, "cr": 2.0}, **KW)) == 2
    assert actors.resolution.actor_count(actors.token_record_grouped(groups, mfs, bundle_min=3, rates={"EXCH": 5.0, "cr": 2.0}, **KW)) == 1


def test_assign_blocks_puts_one_wallet_in_one_block_across_the_whole_set():
    tokens = [{"mint": "t1", "creator": "X", "funder": "G", "fan_out": 3},        # G funds X
              {"mint": "t2", "creator": "W", "funder": "X", "fan_out": 3},        # X funds W: the chain G -> X -> W
              {"mint": "t3", "creator": "X", "funder": None},                     # same creator, funder not found this time
              {"mint": "t4", "creator": "Y", "funder": "EXCH", "fan_out": 2},     # an exchange never links
              {"mint": "t5", "creator": "Z", "funder": "EXCH", "fan_out": 2}]
    b = actors.assign_blocks(tokens, **KW)
    assert b["t1"] == b["t2"] == b["t3"] == "G"
    assert b["t4"] == "Y" and b["t5"] == "Z" and len(set(b.values())) == 3


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
