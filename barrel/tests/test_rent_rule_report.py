"""The count MR-17 ruling 3 waits for: what the transaction rule touched, read as a difference
between two results for the same day. Synthetic rows only.
Run: python -m pytest barrel/tests/test_rent_rule_report.py"""
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import rent_rule_report as rr  # noqa: E402

NET = "members whose funder differs (net)"
GROUPS = "groups touched (a named funder lost members)"


def g(mint, funder, bo, n, creator="CR", n_creator=0, **extra):
    return {"row_kind": "group", "mint": mint, "funder": funder, "b_only_n": bo, "n_members": n, "creator": creator, "n_creator": n_creator, **extra}


def test_a_bundle_funded_inside_the_buy_loses_its_funder_and_is_counted_on_the_bundle_side():
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "X", 6, 6)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", None, 6, 6)]
    r = rr.moves(before, after)
    assert r["bundle_only"]["tokens touched"] == 1 and r["bundle_only"][GROUPS] == 1
    assert r["bundle_only"][NET] == 6
    assert r["bundle_only"]["  left another named funder"] == 6
    assert r["bundle_only"]["members with no funder after"] == 6
    assert r["creator_side"]["tokens touched"] == 0 == r["creator_side"][GROUPS]      # a zero is printed, not left out


def test_the_ruling_asks_for_groups_and_two_groups_in_one_token_are_two():
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "X", 6, 6), g("T", "Y", 5, 5)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", None, 6, 6), g("T", None, 5, 5)]
    r = rr.moves(before, after)["bundle_only"]
    assert (r["tokens touched"], r[GROUPS], r[NET], r["groups before (token, named funder)"]) == (1, 2, 11, 2)


def test_rent_paid_by_the_creator_is_counted_as_leaving_the_creator_not_as_something_else():
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "CR", None, 3)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", None, None, 2), g("T", "OLD", None, 1)]
    r = rr.moves(before, after)["creator_side"]
    assert r[NET] == 3 and r["  left the creator as funder"] == 3
    assert r["  left another named funder"] == 0


def test_the_count_is_net_inside_a_token_so_opposite_moves_cancel():
    """Group rows do not name members. Two members swapping senders leave every count as it was: the figure is the
    fewest members that can have changed, and the documents must not call it an upper bound."""
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "P", None, 1), g("T", "Q", None, 1)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", "Q", None, 1), g("T", "P", None, 1)]
    assert rr.moves(before, after)["creator_side"][NET] == 0


def test_a_group_below_the_bundle_cut_is_outside_the_set_and_is_not_counted():
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "X", 4, 4)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", None, 4, 4)]
    r = rr.moves(before, after)
    assert r["bundle_only"][NET] == 0 and "members before" not in r["bundle_only"]
    assert rr.moves(before, after, bundle_min=4)["bundle_only"][NET] == 4


def test_a_side_whose_size_differs_is_reported_as_not_compared_with_its_members():
    before = [g("T", "EX", None, 1, n_creator=1), g("T", "X", 6, 6)]
    after = [g("T", "EX", None, 1, n_creator=1), g("T", "X", 5, 5)]
    r = rr.moves(before, after)["bundle_only"]
    assert r["tokens not compared: side has a different size"] == 1 and r[NET] == 0
    assert r["members before, in tokens not compared"] == 6
    assert r["groups before, in tokens not compared"] == 1 == r["groups before (token, named funder)"]


def test_member_rows_and_group_rows_give_the_same_count():
    """--bound compares member rows from before MR-14 (is_bundle) with group rows."""
    members = ([{"mint": "T", "w": "CR", "is_creator": True, "is_funded": False, "is_bundle": False, "funder": "EX"}]
               + [{"mint": "T", "w": f"f{i}", "is_creator": False, "is_funded": True, "is_bundle": False, "funder": None} for i in range(3)]
               + [{"mint": "T", "w": f"b{i}", "is_creator": False, "is_funded": False, "is_bundle": True, "funder": "X"} for i in range(5)])
    after = [g("T", "EX", None, 1, n_creator=1), g("T", "CR", None, 2), g("T", None, None, 1), g("T", "X", 5, 5)]
    r = rr.moves(members, after)
    assert r["creator_side"][NET] == 2
    assert r["creator_side"]["  had no funder before and have one after"] == 2
    assert r["creator_side"]["  now behind the creator as funder"] == 2
    assert r["bundle_only"]["members before"] == 5 and r["bundle_only"]["tokens touched"] == 0


def test_which_text_produced_a_result_is_read_from_its_columns():
    assert rr.text_of([{"mint": "T", "funder": None}]) == "before"
    assert rr.text_of([g("T", "X", None, 1, n_slot0=3)]) == "slot"
    assert rr.text_of([g("T", "X", None, 1, n_slot0=3, link=None)]) == "rule"


@pytest.fixture
def held(tmp_path, monkeypatch):
    (tmp_path / "private" / "out").mkdir(parents=True); (tmp_path / "data").mkdir(); (tmp_path / "recon" / "out").mkdir(parents=True)
    monkeypatch.setattr(rr, "ROOT", tmp_path); monkeypatch.setattr(rr, "PRIV", tmp_path / "private" / "out"); monkeypatch.setattr(rr, "DATA", tmp_path / "data")

    def put(where, name, rows):
        (tmp_path / where / name).write_text(json.dumps(rows), encoding="utf-8")
    return tmp_path, put


def test_two_results_from_the_same_text_are_refused_not_reported_as_a_bound(held):
    """Found in review: `--bound day` paired two results that both predate the slot rule and printed 'nothing touched'
    under a heading that called it a bound."""
    tmp, put = held
    put("private/out", "rows_heavy_b1a_members_day_20261001T014415.json", [{"mint": "T", "w": "CR", "is_creator": True, "is_funded": False, "is_bundle": False, "funder": "EX"}])
    put("private/out", "rows_heavy_b1a_grouped_day_20261002T012425.json", [{"mint": "T", "funder": "EX", "b_only_n": None, "n_members": 1, "creator": "CR", "n_creator": 1}])
    with pytest.raises(SystemExit, match="no difference can be taken"):
        rr.main(["--bound", "day"])
    assert not list((tmp / "recon" / "out").glob("*.json"))


def test_the_default_mode_needs_slot_rule_rows_against_rows_with_the_rule(held):
    tmp, put = held
    slot = [g("T", "EX", None, 1, n_creator=1, n_slot0=6), g("T", "X", 6, 6, n_slot0=6)]
    rule = [g("T", "EX", None, 1, n_creator=1, n_slot0=6, link=None), g("T", None, 6, 6, n_slot0=6, link=None)]
    put("data", "heavy_b1a_grouped_pbday_20261002T053913.json", slot)
    with pytest.raises(SystemExit, match="has not been made"):
        rr.main(["pbday"])
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", rule)
    rr.main(["pbday"])
    out = json.loads((tmp / "recon" / "out" / "rent_rule_report_pbday.json").read_text(encoding="utf-8"))
    assert out["before"].endswith("20261002T053913.json") and out["after"].endswith("20261101T120000.json")
    assert out["result"]["bundle_only"][GROUPS] == 1 and out["result"]["bundle_only"][NET] == 6
    put("data", "heavy_b1a_grouped_pbday_20261101T130000.json", rule)      # A1 run twice: still the slot-rule rows against the newest
    rr.main(["pbday"])
    (tmp / "data" / "heavy_b1a_grouped_pbday_20261002T053913.json").unlink()   # the earlier export is gone: A1 is never compared with itself
    with pytest.raises(SystemExit, match="no difference can be taken"):
        rr.main(["pbday"])
