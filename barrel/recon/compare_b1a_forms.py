"""MR-14 order 1: do the member form and the grouped (build) form of the aligned-set query agree?

Compares, per token, for one graduation day:
  set size        members in the aligned set
  holdings        set balance at each entry lag, and net flow after entry
  funders         which funder, how many members behind it, and its fan-out
The member rows were produced on 2026-10-01 by the earlier query, which applied the bundle cut
in SQL; the grouped rows by the build query, which returns same-funder counts and leaves the
cut to actors.aligned(). Agreement therefore also tests that the local cut reproduces the old one.

Reads barrel/private/out (rows name wallets); writes counts only. On a disagreement it prints the first eight
characters of up to four token addresses to the terminal, never a wallet.
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
    # a run cancelled at its cap under --private-rows used to leave a rows file holding only `null`: that is not a result
    found = [f for f in found if pathlib.Path(f).stat().st_size > 4]
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
    # one fan measure for BOTH forms, so the collapse comparison tests the logic and not the window:
    # the counts carried by the earlier member rows, used as a rates table
    fanmap = {r["funder"]: r["fan_out"] for r in members if r.get("funder") and r.get("fan_out") is not None}
    if not fanmap:      # member rows since MR-15 carry no fan measure: borrow the counts of the newest earlier member rows that do
        for f in sorted(glob.glob(str(PRIV / f"rows_heavy_b1a_members_{tag}_*.json")), key=lambda f: f[-21:], reverse=True):
            fanmap = {r["funder"]: r["fan_out"] for r in (json.load(open(f)) or []) if r.get("funder") and r.get("fan_out") is not None}
            if fanmap:
                break
    if not fanmap:
        raise SystemExit("no member rows for this day carry a fan measure: collapse cannot be compared")
    # MR-17 ruling 2 reads the link column. Rows from before MR-16 carry none, so against them BOTH forms are read
    # as before the ruling: otherwise the member side alone is unresolved wherever a wallet was delivered, and the
    # two forms disagree for a reason that is not a defect in either reader.
    both_link = "link" in members[0] and any("link" in r for r in groups)
    ruled_reading = actors.DELIVERY_AS_CREATOR and both_link
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
        kw = dict(fanout_threshold=actors.FANOUT_PROVISIONAL, rates=fanmap, delivery_as_creator=ruled_reading)
        tm = [x for x in mfs if x["mint"] == mint] if "row_kind" in g[0] else None
        named = {x["funder"] for x in g if x["funder"]} | {x["funder"] for x in (tm or []) if x["funder"]}
        if named - set(fanmap):      # a funder only the regenerated rows name has no carried measure: not comparable
            checks["actors after collapse"] = None
        else:
            a, b = actors.token_record(m, **kw), actors.token_record_grouped(g, tm, **kw)
            checks["actors after collapse"] = (a[actors.ACTORS_KEY], a["collapse_state"]) == (b[actors.ACTORS_KEY], b["collapse_state"])
        if "link" in g[0]:      # MR-17 ruling 2 on the build form: the ruled reading against the one before it
            base = dict(fanout_threshold=actors.FANOUT_PROVISIONAL, rates=fanmap)
            ruled, before = (actors.token_record_grouped(g, tm, delivery_as_creator=s, **base) for s in (True, False))
            res["ruling 2: sets with a link column"] += 1
            res["ruling 2: sets unresolved, delivered wallets as the creator's actor (ruled)"] += ruled["collapse_state"] == "unresolved"
            res["ruling 2: sets unresolved, reading before the ruling"] += before["collapse_state"] == "unresolved"
            if not (named - set(fanmap)):      # an actor COUNT needs every named funder's fan measure
                res["ruling 2: sets whose actor count differs between the two readings (funders all measured)"] += ruled[actors.ACTORS_KEY] != before[actors.ACTORS_KEY]
        for k, ok in checks.items():
            if ok is None:
                res[f"{k}: not comparable"] += 1
                continue
            res[f"{k}: agree"] += ok
            if not ok:
                mism[k].append(mint)
    if "link" in members[0]:      # counts the member form alone can give (the build form does not name delivered wallets)
        in_set = actors.aligned(members)
        creator_of = collections.defaultdict(set)
        for r in members:
            if r["is_creator"]:
                creator_of[r["w"]].add(r["mint"])
        deliv = {(r["w"], r["mint"]) for r in in_set if actors.delivered_by_creator(r)}
        res["delivered wallets (wallet, token)"] = len(deliv)
        # blocks do not see delivery (RULINGS_2026-10-07.md): a delivered wallet that is the creator of another token
        res["delivered wallets that are the creator of another token that day"] = len({w for w, t in deliv if creator_of.get(w, set()) - {t}})
        # wallets at or above the bundle cut that the creator also paid sit on the creator's side of rent_rule_report
        paid_bundle = [r for r in in_set if r["is_funded"] and (r.get("bundle_n") or 0) >= actors.BUNDLE_MIN]
        res["creator-paid members at or above the bundle cut"] = len(paid_bundle)
        res["creator-paid members at or above the bundle cut with no funder"] = sum(r["funder"] is None for r in paid_bundle)
    out = {"day": tag, "reading": ("delivered wallets are the creator's actor (MR-17)" if ruled_reading
                                   else "before MR-17: at least one form has no link column" if not both_link
                                   else "before MR-17: actors.DELIVERY_AS_CREATOR is off"),
           "member_file": pathlib.Path(mfile).name, "grouped_file": pathlib.Path(gfile).name,
           "member_rows": len(members), "group_rows": len(groups), "member_funder_rows": len(mfs),
           "group_rows_outside_the_set_after_the_local_cut": dropped, "result": dict(res),
           "tokens_disagreeing": {k: len(v) for k, v in mism.items()}}
    (ROOT / "recon" / "out" / f"b1a_forms_agreement_{tag}.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    # step A1 runs this twice (the member rows on disk, then A1's own): one file per pair of inputs, so the second does not replace the first
    pair = "_".join(pathlib.Path(f).stem[-15:] for f in (mfile, gfile))
    (ROOT / "recon" / "out" / f"b1a_forms_agreement_{tag}_{pair}.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(json.dumps(out, indent=1))
    for k, v in mism.items():
        print(f"  DISAGREE {k}: {len(v)} tokens, e.g. {[x[:8] for x in v[:4]]}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "day")
