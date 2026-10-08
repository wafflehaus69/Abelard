"""Between the two fee-share queries, and before the organic query: turn a fee-share export into the
lists the next runs are given (MR-18). Local: no network, no credits.

  python barrel/recon/feeshare_lists.py <tag>      tag = a chunk (2026-08) or a proving tag (day, week, pbday)

Reads the newest rows of the first fee-share query for that tag (recon/gen_feeshare.fs: one row per
sharing-config event of a graduating token), decodes them (recon/decode_feeshare.py) and writes

  barrel/private/out/fs_pairs_<tag>.json   [{"mint", "w"}]           for the member query   (--pairs-from)
  barrel/private/out/fs_codes_<tag>.json   [{"mint", "w", "code"}]   for the organic query  (--pairs-from)
  barrel/recon/out/feeshare_lists_<tag>.json                         counts only; this one is tracked

Both lists name wallets and stay in the gitignored tree; what is printed and tracked is counts.

Pairs: every address in force at graduation + 15, 60 or 240 minutes (decode_feeshare.pairs).
Codes, one letter per (token, wallet), nothing decided here about which of them excludes a taker:
  E   a recipient in force at graduation + 240 minutes
  P   a recipient of an earlier config of that token that is no longer one at + 240 minutes
A wallet that becomes a recipient LATER than + 240 minutes has no code and cannot have one: the
first query stops there (the as-of rule needs nothing later). Reading recipients through day 7
would mean scanning seven more days per chunk in that query. Builder's reading, flagged in
docs/PLUS_QUERIES.md.

Owner wallets are removed from both lists here, and counted. The first query has already withheld
any payload that holds one, and the runner removes them a third time when it substitutes the list.
"""
import collections
import glob
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_feeshare as dfs
import owner_wallets

ROOT = pathlib.Path(__file__).resolve().parents[1]
PRIV = ROOT / "private" / "out"
DATA = ROOT / "data"
ENTRY_LAG = max(dfs.LAGS)       # the last entry lag: what "in force at entry" means for a code


def source(tag: str) -> str:
    """Newest rows of the first fee-share query for a chunk (2026-08_fs) or a proving tag (feeshare_fs_pbday).
    A file holding nothing (a run cancelled under --private-rows used to leave one) is not a result."""
    stem = f"{tag}_fs" if re.fullmatch(r"\d{4}-\d{2}", tag) else f"feeshare_fs_{tag}"
    found = glob.glob(str(DATA / f"{stem}_*.json")) + glob.glob(str(PRIV / f"rows_{stem}_*.json"))
    found = [f for f in found if pathlib.Path(f).stat().st_size > 4]
    if not found:
        raise SystemExit(f"no rows of {stem} in barrel/data or barrel/private/out: the first fee-share query has not been run for {tag}")
    return sorted(found, key=lambda f: f[-20:])[-1]


def codes(hist: dict, owner=()) -> tuple[list[dict], int]:
    """One code per (token, wallet): E in force at the last entry lag, P a recipient of an earlier usable
    event that is not in force then. Built from `in_force`, the wider reading, as the pairs are. A token
    whose state at entry cannot be read gets no E (there is no list to name); its earlier recipients are
    still P."""
    own, out, removed = set(owner), {}, set()
    for mint, tok in hist.items():
        t_entry = tok["grad_time"] + 60 * ENTRY_LAG
        now = {s["w"] for s in dfs.state_at(tok["events"], t_entry)["in_force"] or []}
        before = {s["w"] for e in tok["events"] if e["t"] <= t_entry and not e["code"] for s in e["shareholders"]}
        for w in now | before:
            if w in own:
                removed.add((mint, w))
            else:
                out[(mint, w)] = "E" if w in now else "P"
    return [{"mint": m, "w": w, "code": c} for (m, w), c in sorted(out.items())], len(removed)


def summary(rows: list[dict], owner=()) -> tuple[dict, list[dict], list[dict]]:
    hist = dfs.history(rows)
    states = {mint: {g: dfs.state_at(tok["events"], tok["grad_time"] + 60 * g) for g in dfs.LAGS} for mint, tok in hist.items()}
    pairs, n_own_p = dfs.pairs(states, owner)
    cds, n_own_c = codes(hist, owner)
    c = dfs.counts(hist)
    by_lag = {str(g): dict(collections.Counter(st[g]["state"] for st in states.values())) for g in dfs.LAGS}
    recipients = collections.Counter(st[ENTRY_LAG]["c07b"] for st in states.values() if st[ENTRY_LAG]["c07b"] is not None)
    out = {"tokens": len(hist), "rows": dict(c["rows"]), "events_by_kind": dict(c["kinds"]), "events_by_code": dict(c["codes"]),
           "state_by_lag": by_lag, "tokens_by_recipient_count_at_entry": {str(k): v for k, v in sorted(recipients.items())},
           "pairs": len(pairs), "tokens_with_a_pair": len({p["mint"] for p in pairs}), "owner_pairs_removed": n_own_p,
           "codes": dict(collections.Counter(x["code"] for x in cds)), "owner_codes_removed": n_own_c,
           # what the member query's text will weigh: about 98 characters a pair (measured on a substituted text)
           "member_query_text_mb": round(len(pairs) * 98 / 1e6, 2)}
    return out, pairs, cds


def main(tag: str) -> None:
    src = source(tag)
    rows = json.loads(pathlib.Path(src).read_text(encoding="utf-8"))
    out, pairs, cds = summary(rows, set(owner_wallets.owner_wallets()))
    PRIV.mkdir(parents=True, exist_ok=True)
    (PRIV / f"fs_pairs_{tag}.json").write_text(json.dumps(pairs), encoding="utf-8")
    (PRIV / f"fs_codes_{tag}.json").write_text(json.dumps(cds), encoding="utf-8")
    out = {"tag": tag, "source": pathlib.Path(src).name, **out}
    (ROOT / "recon" / "out" / f"feeshare_lists_{tag}.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(json.dumps(out, indent=1))
    print(f"lists written to barrel/private/out/fs_pairs_{tag}.json and fs_codes_{tag}.json (addresses not shown)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "pbday")
