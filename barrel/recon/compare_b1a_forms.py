"""MR-14 order 1: do the member form and the grouped (build) form of the aligned-set query agree?

Compares, per token, for one graduation day:
  set size        members in the aligned set
  holdings        set balance at each entry lag, and net flow after entry
  funders         which funder, how many members behind it, and its fan-out
The member rows were produced on 2026-10-01 by the earlier query, which applied the bundle cut
in SQL; the grouped rows by the build query, which returns same-funder counts and leaves the
cut to actors.aligned(). Agreement therefore also tests that the local cut reproduces the old one.

Reads barrel/private/out (rows name wallets); prints and writes counts only.
  python barrel/recon/compare_b1a_forms.py day
"""
import collections
import glob
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import actors

ROOT = pathlib.Path(__file__).resolve().parents[1]
PRIV = ROOT / "private" / "out"
DATA = ROOT / "data"


def latest(pattern: str) -> str:
    """Newest matching file, by the timestamp in its name, in private/out or data/ (both gitignored)."""
    found = glob.glob(str(PRIV / ("rows_" + pattern))) + glob.glob(str(DATA / pattern))
    return sorted(found, key=lambda f: f[-21:])[-1]


def close(a, b, rel=1e-9) -> bool:
    """Both missing is NOT agreement here: callers pass sums (never None) or values that must exist."""
    if a is None or b is None:
        return False
    return abs(a - b) <= rel * max(1.0, abs(a), abs(b))


def main(tag: str) -> None:
    mfile, gfile = latest(f"heavy_b1a_members_{tag}_*.json"), latest(f"heavy_b1a_grouped_{tag}_*.json")
    members = json.load(open(mfile))
    groups, mfs = actors.split_grouped(json.load(open(gfile)))
    mfs = [{"mint": r["mint"], "member": r["member"], "funder": r["member_own_funder"], "b_only_n": r["member_b_only_n"]} for r in mfs]
    # one fan measure for BOTH forms, so the collapse comparison tests the logic and not the window:
    # the counts carried by the earlier member rows, used as a rates table
    fanmap = {r["funder"]: r["fan_out"] for r in members if r.get("funder") and r.get("fan_out") is not None}
    bm, bg = collections.defaultdict(list), collections.defaultdict(list)
    for r in actors.aligned(members):
        bm[r["mint"]].append(r)
    for r in actors.aligned(groups):
        bg[r["mint"]].append(r)
    dropped = len(groups) - sum(len(v) for v in bg.values())
    res = collections.Counter()
    mism = collections.defaultdict(list)
    for mint in sorted(set(bm) | set(bg)):
        m, g = bm.get(mint, []), bg.get(mint, [])
        res["tokens"] += 1
        if not m or not g:
            res["token missing in one form"] += 1
            mism["missing"].append(mint)
            continue
        checks = {
            "set size": len(m) == sum(x["n_members"] for x in g),
            "exactly one creator on each side": sum(bool(x["is_creator"]) for x in m) == 1 == sum(x["n_creator"] for x in g),
            "creator-funded members": sum(bool(x["is_funded"]) for x in m) == sum(x["n_funded"] for x in g),
            "holdings g15": close(sum(x["b15"] or 0 for x in m), sum(x["b15"] or 0 for x in g)),
            "holdings g60": close(sum(x["b60"] or 0 for x in m), sum(x["b60"] or 0 for x in g)),
            "holdings g240": close(sum(x["b240"] or 0 for x in m), sum(x["b240"] or 0 for x in g)),
            "net flow after entry": close(sum(x["net_after"] or 0 for x in m), sum(x["net_after"] or 0 for x in g)),
            "supply at g15 / g60 / g240": all(close(m[0][k], g[0][k]) for k in ("supply15", "supply60", "supply240")),
            "latency median": (actors.fund_to_first_buy_s(m) == actors.fund_to_first_buy_s(g)) if "lat_s" in g[0] else None,
        }
        fm = collections.Counter(x["funder"] for x in m)
        fg = collections.Counter()
        for x in g:
            fg[x["funder"]] += x["n_members"]
        checks["funders and members per funder"] = fm == fg
        if "fan_out" in g[0]:
            checks["fan-out per funder"] = ({x["funder"]: x["fan_out"] for x in m if x["funder"]}
                                            == {x["funder"]: x["fan_out"] for x in g if x["funder"]})
        kw = dict(fanout_threshold=actors.FANOUT_PROVISIONAL, rates=fanmap)
        tm = [x for x in mfs if x["mint"] == mint] if "row_kind" in g[0] else None
        a, b = actors.token_record(m, **kw), actors.token_record_grouped(g, tm, **kw)
        checks["actors after collapse"] = (a[actors.ACTORS_KEY], a["collapse_state"]) == (b[actors.ACTORS_KEY], b["collapse_state"])
        for k, ok in checks.items():
            if ok is None:
                res[f"{k}: not comparable (grouped rows predate the column)"] += 1
                continue
            res[f"{k}: agree"] += ok
            if not ok:
                mism[k].append(mint)
    out = {"day": tag, "member_file": pathlib.Path(mfile).name, "grouped_file": pathlib.Path(gfile).name,
           "member_rows": len(members), "group_rows": len(groups), "member_funder_rows": len(mfs),
           "group_rows_outside_the_set_after_the_local_cut": dropped, "result": dict(res),
           "tokens_disagreeing": {k: len(v) for k, v in mism.items()}}
    (ROOT / "recon" / "out" / f"b1a_forms_agreement_{tag}.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(json.dumps(out, indent=1))
    for k, v in mism.items():
        print(f"  DISAGREE {k}: {len(v)} tokens, e.g. {[x[:8] for x in v[:4]]}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "day")
