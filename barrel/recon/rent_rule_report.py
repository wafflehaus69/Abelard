"""MR-17 ruling 3: how many groups does the transaction rule touch?

The rent rule (MR-16) excludes from the funder choice every SOL transfer inside a member's own
first-acquisition transaction, whoever sent it and however large. An excluded transfer is not in
the rows the query returns, so what the rule touched is read as a difference between two results
for the same graduation day: rows produced without the rule and rows produced with it.

Two sides of the set are counted apart, because they answer different questions:

  bundle_only    creation-slot traders in a same-funder group at or above the bundle cut, not paid
                 by the creator. Their first action is in the creation slot, so nothing else that
                 changed between the two texts can move their funder: every move here is the
                 transaction rule taking away funding that came inside the buy. This is the
                 population of the third review's finding 3.
  creator_side   the creator and the wallets it paid. A member that leaves the creator as funder
                 is the rent the rule was written for. Other moves mix the rule with the longer
                 funder scan (a late first acquisition now finds a later sender).

Modes:
  (default)  slot-rule rows of 2026-10-02 against the newest rows of the same query (step A1).
             This is the count the ruling asks for, in groups and in members.
  --bound    before A1 exists: the member rows of 2026-10-01 (funding strictly before the first
             action) against the slot-rule rows of 2026-10-02. For a member whose first action
             is before the end of the funder scan both results share (2026-09-03 on pbday), a
             transfer the transaction rule can exclude is one the slot rule added, so what
             differs here indicates what the rule can touch. A member that first acquires later
             is outside it (8 on pbday), and so are the blind spots listed in
             docs/RULINGS_2026-10-07.md.

What is counted is the NET change in members per funder inside one token and side. Group rows do
not name members, so opposite moves inside one token cancel: the figure is the FEWEST members
whose funder can differ. It is exact where a side has one member, or one funder in both results.

The two files must come from the texts the mode names. That is read from the columns only each
text returns, never from the order of the files.

Reads gitignored rows (they name wallets); prints and writes counts only.
  python barrel/recon/rent_rule_report.py [--bound] [tag]        tag defaults to pbday
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
SIDES = ("bundle_only", "creator_side")


def side(row: dict, bundle_min: int) -> str | None:
    """Which side of the set a member or group row is on; None when the local cut leaves it out."""
    if not actors.in_aligned_set(row, bundle_min):
        return None
    if "b_only_n" in row:
        return "creator_side" if row["b_only_n"] is None else "bundle_only"
    return "creator_side" if (row["is_creator"] or row["is_funded"]) else "bundle_only"


def tally(rows: list[dict], bundle_min: int) -> dict:
    """(mint, side) -> funder -> members. Member rows count one each, group rows their n_members."""
    out: dict = collections.defaultdict(collections.Counter)
    for r in rows:
        s = side(r, bundle_min)
        if s:
            out[(r["mint"], s)][r.get("funder")] += r.get("n_members", 1)
    return out


def creators_of(*row_sets: list[dict]) -> dict:
    out = {}
    for rows in row_sets:
        for r in rows:
            if r.get("creator"):
                out[r["mint"]] = r["creator"]
            elif r.get("is_creator") and r.get("w"):
                out[r["mint"]] = r["w"]
    return out


def moves(before: list[dict], after: list[dict], bundle_min: int = actors.BUNDLE_MIN) -> dict:
    """Net change in members per funder between two results for the same tokens, by side: the
    FEWEST members whose funder can differ (opposite moves inside one token cancel). A group is a
    (token, named funder) pair. A token whose side has a different size in the two results is not
    compared (membership is not this rule's business) and is counted as such with its members,
    never silently dropped."""
    b, a = tally(before, bundle_min), tally(after, bundle_min)
    cr = creators_of(before, after)
    # the three counts the ruling waits for are always printed: a zero is a measured zero, not a missing line
    zero = ("tokens touched", "groups touched (a named funder lost members)", "members whose funder differs (net)")
    res = {s: collections.Counter({k: 0 for k in zero}) for s in SIDES}
    for key in set(b) | set(a):
        mint, s = key
        o, n, c = b.get(key, collections.Counter()), a.get(key, collections.Counter()), res[s]
        c["tokens with such members"] += 1
        c["groups before (token, named funder)"] += sum(1 for f_ in o if f_ is not None)
        c["members before"] += sum(o.values())
        c["members after"] += sum(n.values())
        if sum(o.values()) != sum(n.values()):
            c["tokens not compared: side has a different size"] += 1
            c["members before, in tokens not compared"] += sum(o.values())
            c["groups before, in tokens not compared"] += sum(1 for f_ in o if f_ is not None)
            continue
        left = {f: o[f] - n[f] for f in o if o[f] > n[f]}          # funder -> members that no longer have it
        c["members with no funder before"] += o[None]
        c["members with no funder after"] += n[None]
        if not left:
            continue
        creator = cr.get(mint)
        c["tokens touched"] += 1
        c["groups touched (a named funder lost members)"] += sum(1 for f_ in left if f_ is not None)
        c["members whose funder differs (net)"] += sum(left.values())
        c["  left the creator as funder"] += left.get(creator, 0) if creator else 0
        c["  left another named funder"] += sum(v for f, v in left.items() if f is not None and f != creator)
        c["  had no funder before and have one after"] += left.get(None, 0)
        c["  now behind the creator as funder"] += max(0, n[creator] - o[creator]) if creator else 0
    return {s: dict(v) for s, v in res.items()}


def text_of(rows: list[dict]) -> str:
    """Which funder rule produced a result, read from columns only that text returns: ``link`` came
    with the transaction rule (MR-16), ``n_slot0`` with the slot rule (MR-15); rows with neither
    counted funding strictly before the second of the first action."""
    r = rows[0]
    return "rule" if "link" in r else "slot" if "n_slot0" in r else "before"


NAMES = {"before": "rows from before the slot rule", "slot": "slot-rule rows", "rule": "rows with the transaction rule"}


def _named(pattern: str) -> list[str]:
    found = glob.glob(str(PRIV / ("rows_" + pattern))) + glob.glob(str(DATA / pattern))
    # a run cancelled at its cap under --private-rows used to leave a rows file holding only `null`: that is not a result
    found = [f for f in found if pathlib.Path(f).stat().st_size > 4]
    return sorted(found, key=lambda f: f[-20:])


def main(argv: list[str]) -> None:
    bound = "--bound" in argv
    tag = next((x for x in argv if not x.startswith("--")), "pbday")
    grouped = _named(f"heavy_b1a_grouped_{tag}_*.json")
    if not grouped:
        raise SystemExit(f"no grouped rows for {tag}")
    if bound:
        mem = _named(f"heavy_b1a_members_{tag}_*.json")
        if not mem:
            raise SystemExit(f"no member rows for {tag}")
        bfile, afile = mem[0], grouped[0]
    else:
        if len(grouped) < 2:
            raise SystemExit(f"only one grouped result for {tag} exists ({pathlib.Path(grouped[0]).name}): the run with the "
                             "transaction rule (step A1) has not been made. --bound compares rows already held, where they "
                             "come from the two texts it needs (pbday only).")
        bfile, afile = grouped[0], grouped[-1]
    before = json.loads(pathlib.Path(bfile).read_text(encoding="utf-8"))
    after = json.loads(pathlib.Path(afile).read_text(encoding="utf-8"))
    want, got = (("before", "slot") if bound else ("slot", "rule")), (text_of(before), text_of(after))
    if got != want:
        raise SystemExit(f"{tag}: found {NAMES[got[0]]} ({pathlib.Path(bfile).name}) and {NAMES[got[1]]} ({pathlib.Path(afile).name}); "
                         f"this mode needs {NAMES[want[0]]} against {NAMES[want[1]]}, so no difference can be taken")
    before = actors.split_grouped(before)[0] if "row_kind" in before[0] else before
    after = actors.split_grouped(after)[0] if "row_kind" in after[0] else after
    out = {"mode": "before A1: rows from before the slot rule against slot-rule rows" if bound
           else "the transaction rule: slot-rule rows against rows with the rule",
           "unit": "net change in members per funder inside a token and side: the fewest members whose funder can differ",
           "tag": tag, "before": pathlib.Path(bfile).name, "after": pathlib.Path(afile).name,
           "bundle_min": actors.BUNDLE_MIN, "tokens": len({r["mint"] for r in after}),
           "result": moves(before, after)}
    name = f"rent_rule_{'bound' if bound else 'report'}_{tag}.json"
    (ROOT / "recon" / "out" / name).write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main(sys.argv[1:])
