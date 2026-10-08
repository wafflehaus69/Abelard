"""recon/compare_b1a_forms.py at step A1, on synthetic rows written to a temporary tree.

Two things found in review of the MR-17 work, both about what the script does with real files at A1 and neither
about the readers it calls:
  * member rows from before MR-16 carry no link column; with delivered wallets ruled to be the creator's actor,
    the grouped side alone resolved them and every such token printed DISAGREE;
  * member rows since MR-15 carry no fan measure, and the script stopped before making any check.
Run: python -m pytest barrel/tests/test_compare_b1a_forms.py"""
import collections
import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "recon"))
import actors  # noqa: E402
import compare_b1a_forms as cmp  # noqa: E402

NUM = ("b15", "b60", "b240", "net_after")


def member(w, *, creator=False, funded=False, bundle_n=None, funder=None, link=None, lat=None):
    return {"mint": "T", "w": w, "is_creator": creator, "is_funded": funded, "bundle_n": bundle_n, "funder": funder, "link": link,
            "b15": 1.0, "b60": 2.0, "b240": 3.0, "net_after": -1.0, "supply15": 9.0, "supply60": 9.0, "supply240": 9.0,
            "fund_to_first_buy_s": lat}


def old_rows(members, fan):
    """The rows of 2026-10-01: no link, no bundle_n, the cut applied in SQL, a fan count on the row."""
    out = []
    for r in members:
        o = {k: v for k, v in r.items() if k not in ("link", "bundle_n")}
        o["is_bundle"] = (r["bundle_n"] or 0) >= actors.BUNDLE_MIN
        o["fan_out"] = fan.get(r["funder"])
        out.append(o)
    return out


def grouped(members):
    """What the build query returns for these members (see gen_heavy_b1.members_grouped)."""
    groups = collections.defaultdict(list)
    for r in members:
        bo = None if (r["is_creator"] or r["is_funded"]) else r["bundle_n"]
        groups[(r["funder"], bo, r["link"])].append(r)
    by_w = {r["w"]: r for r in members}
    out = [{"row_kind": "group", "mint": "T", "funder": f, "b_only_n": bo, "link": lk, "member": None, "creator": "cr", "n_slot0": 1,
            "n_members": len(v), "n_creator": sum(x["is_creator"] for x in v), "n_funded": sum(x["is_funded"] for x in v),
            **{k: sum(x[k] for x in v) for k in NUM}, "supply15": 9.0, "supply60": 9.0, "supply240": 9.0,
            "lat_s": [x["fund_to_first_buy_s"] for x in v]} for (f, bo, lk), v in groups.items()]
    for x in sorted({r["funder"] for r in members if r["funder"] in by_w}):
        m = by_w[x]
        out.append({"row_kind": "member_funder", "mint": "T", "funder": None, "b_only_n": None, "member": x, "n_members": 1,
                    "member_own_funder": m["funder"], "member_link": m["link"],
                    "member_b_only_n": None if (m["is_creator"] or m["is_funded"]) else m["bundle_n"]})
    return out


MEMBERS = [member("cr", creator=True, funder="EXCH", lat=40),
           member("a", funded=True, funder=None, link=actors.LINK_DELIVERY),          # delivered, no SOL funder
           member("b", funded=True, funder="HUB", link=actors.LINK_SOL, lat=7)]
FAN = {"EXCH": 5000, "HUB": 5000}


@pytest.fixture
def tree(tmp_path, monkeypatch):
    (tmp_path / "private" / "out").mkdir(parents=True); (tmp_path / "data").mkdir(); (tmp_path / "recon" / "out").mkdir(parents=True)
    monkeypatch.setattr(cmp, "ROOT", tmp_path); monkeypatch.setattr(cmp, "PRIV", tmp_path / "private" / "out"); monkeypatch.setattr(cmp, "DATA", tmp_path / "data")

    def put(where, name, rows):
        (tmp_path / where / name).write_text(json.dumps(rows), encoding="utf-8")

    def result():
        return json.loads((tmp_path / "recon" / "out" / "b1a_forms_agreement_pbday.json").read_text(encoding="utf-8"))
    return put, result


def test_against_rows_with_no_link_column_both_forms_are_read_as_before_the_ruling(tree):
    put, result = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    cmp.main("pbday")
    out = result()
    assert out["reading"].startswith("before MR-17")
    assert out["result"]["actors after collapse: agree"] == 1 and "actors after collapse" not in out["tokens_disagreeing"]
    # the ruling's effect is still reported, from the build form alone
    assert out["result"]["ruling 2: sets unresolved, reading before the ruling"] == 1
    assert out["result"]["ruling 2: sets unresolved, delivered wallets as the creator's actor (ruled)"] == 0


def test_member_rows_with_no_fan_measure_borrow_it_and_the_comparison_runs_to_the_end(tree):
    put, result = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("private/out", "rows_heavy_b1a_members_pbday_20261101T121500.json", MEMBERS)          # A1's member form: link, no fan measure
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    cmp.main("pbday")
    out = result()
    assert out["member_file"].endswith("20261101T121500.json")
    assert out["reading"].startswith("delivered wallets are the creator's actor")
    r = out["result"]
    assert r["actors after collapse: agree"] == 1 and r["latency median: agree"] == 1 and r["set size: agree"] == 1
    assert r["delivered wallets (wallet, token)"] == 1
    assert r["delivered wallets that are the creator of another token that day"] == 0
    assert not out["tokens_disagreeing"]


def test_with_no_fan_measure_anywhere_it_still_refuses(tree):
    put, _ = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261101T121500.json", MEMBERS)
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    with pytest.raises(SystemExit, match="no member rows for this day carry a fan measure"):
        cmp.main("pbday")


def test_a_rows_file_left_by_a_cancelled_run_is_not_taken_as_the_newest_result(tree, tmp_path):
    put, result = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    (tmp_path / "private" / "out" / "rows_heavy_b1a_members_pbday_20261101T121500.json").write_text("null", encoding="utf-8")
    cmp.main("pbday")
    assert result()["member_file"].endswith("20261001T174118.json")


def test_each_pair_of_inputs_keeps_its_own_result_file(tree, tmp_path):
    """A1 runs the comparison twice; the second run must not replace the first."""
    put, _ = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    cmp.main("pbday")
    put("private/out", "rows_heavy_b1a_members_pbday_20261101T121500.json", MEMBERS)
    cmp.main("pbday")
    kept = sorted(f.name for f in (tmp_path / "recon" / "out").glob("b1a_forms_agreement_pbday_*.json"))
    assert kept == ["b1a_forms_agreement_pbday_20261001T174118_20261101T120000.json", "b1a_forms_agreement_pbday_20261101T121500_20261101T120000.json"]


def test_delivered_wallets_that_created_another_token_and_creator_paid_wallets_above_the_cut_are_counted(tree):
    """The two counts A1 takes from the member form alone, on rows where neither is zero."""
    put, result = tree
    t = MEMBERS + [member("p", funded=True, bundle_n=6, funder=None, link=actors.LINK_SOL),
                   member("q", funded=True, bundle_n=5, funder="HUB", link=actors.LINK_SOL, lat=3),
                   member("z", bundle_n=4, funder="HUB", lat=2)]                        # below the cut: outside the set
    u = [member("a", creator=True, funder="EXCH", lat=5)]                                # the wallet delivered in T created U

    def retag(rows):
        return [dict(r, mint="U", **({"creator": "a"} if "creator" in r else {})) for r in rows]
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("private/out", "rows_heavy_b1a_members_pbday_20261101T121500.json", t + retag(u))
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(t) + retag(grouped(u)))
    cmp.main("pbday")
    out = result()
    r = out["result"]
    assert r["tokens"] == 2 and not out["tokens_disagreeing"]
    assert r["delivered wallets (wallet, token)"] == 1
    assert r["delivered wallets that are the creator of another token that day"] == 1
    assert r["creator-paid members at or above the bundle cut"] == 2
    assert r["creator-paid members at or above the bundle cut with no funder"] == 1


def test_the_reading_line_says_why_when_the_switch_is_off(tree, monkeypatch):
    put, result = tree
    put("private/out", "rows_heavy_b1a_members_pbday_20261001T174118.json", old_rows(MEMBERS, FAN))
    put("private/out", "rows_heavy_b1a_members_pbday_20261101T121500.json", MEMBERS)
    put("data", "heavy_b1a_grouped_pbday_20261101T120000.json", grouped(MEMBERS))
    monkeypatch.setattr(actors, "DELIVERY_AS_CREATOR", False)
    cmp.main("pbday")
    assert result()["reading"] == "before MR-17: actors.DELIVERY_AS_CREATOR is off"
