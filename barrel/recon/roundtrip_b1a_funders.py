"""MR-15 B2: where the regenerated aligned-set query gives a member a different funder than the
earlier member rows did, which one does the chain support?

The two forms differ by definition: the earlier query took the last sender STRICTLY BEFORE THE
SECOND of the member's first acquisition; the ratified one takes the last sender up to and
including the SLOT of that acquisition (ties by slot, then address). For a sample of members
whose funder changed, this reads the member's own transactions over RPC (free), finds its first
acquisition of the token and every SOL transfer of at least 0.001 SOL it received up to that
slot, and says which funder the ratified rule should give.

Reads wallet-bearing rows from barrel/private/out and barrel/data; prints and writes counts and
slot numbers only.
  python barrel/recon/roundtrip_b1a_funders.py [n_per_kind]
"""
import collections
import datetime as dt
import glob
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import actors
import decode_pumpswap_events as d

ROOT = pathlib.Path(__file__).resolve().parents[1]
MAX_PAGES = 6


def ts(s: str) -> int:
    return int(dt.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp())


def history(w: str, not_after: int):
    """Signatures of w up to `not_after` (unix), oldest first. None if the history is too long to reach."""
    sigs, before = [], None
    for _ in range(MAX_PAGES):
        opts = {"limit": 1000}
        if before:
            opts["before"] = before
        page = d.rpc("getSignaturesForAddress", [w, opts]) or []
        sigs += [s for s in page if not s.get("err")]
        if len(page) < 1000:
            return sorted([s for s in sigs if (s.get("blockTime") or 0) <= not_after + 2], key=lambda s: s["slot"])
        before = page[-1]["signature"]
    return None


def inspect(w: str, mint: str, first_in: int):
    sigs = history(w, first_in)
    if sigs is None:
        return {"status": "history too long"}
    inflows, acq_slot = [], None
    for s in sigs:
        if (s.get("blockTime") or 0) < first_in - 5 * 86400:
            continue
        tx = d.rpc("getTransaction", [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}])
        if not tx:
            continue
        meta = tx.get("meta") or {}
        ixs = list(tx["transaction"]["message"]["instructions"])
        for inner in meta.get("innerInstructions") or []:
            ixs += inner["instructions"]
        for ix in ixs:
            p = ix.get("parsed")
            if isinstance(p, dict) and ix.get("program") == "system" and p.get("type") in ("transfer", "transferWithSeed", "createAccount", "createAccountWithSeed"):
                info = p["info"]
                dst = info.get("destination") or info.get("newAccount")
                if dst == w and info.get("source") != w and (info.get("lamports") or 0) >= 1_000_000:
                    inflows.append((tx["slot"], info["source"]))
        if acq_slot is None:      # first slot in which w's balance of the token rises
            pre = {(b["accountIndex"]): float(b["uiTokenAmount"]["amount"]) for b in meta.get("preTokenBalances") or [] if b.get("mint") == mint and b.get("owner") == w}
            for b in meta.get("postTokenBalances") or []:
                if b.get("mint") == mint and b.get("owner") == w and float(b["uiTokenAmount"]["amount"]) > pre.get(b["accountIndex"], 0.0):
                    acq_slot = tx["slot"]
    if acq_slot is None:
        return {"status": "acquisition not found in the transactions read"}
    upto = [x for x in inflows if x[0] <= acq_slot]
    strictly_before = [x for x in inflows if x[0] < acq_slot]
    chain = max(upto, key=lambda x: (x[0], x[1]))[1] if upto else None
    return {"status": "ok", "acq_slot": acq_slot, "chain_funder": chain,
            "inflows_up_to_acq_slot": len(upto), "inflows_in_acq_slot": len(upto) - len(strictly_before)}


def main(n: int) -> None:
    m = json.load(open(sorted(glob.glob(str(ROOT / "private" / "out" / "rows_heavy_b1a_members_pbday_*.json")))[-1]))
    g, _ = actors.split_grouped(json.load(open(sorted(glob.glob(str(ROOT / "data" / "heavy_b1a_grouped_pbday_*.json")))[-1])))
    g = actors.aligned(g)
    bm, bg = collections.defaultdict(list), collections.defaultdict(collections.Counter)
    for x in m:
        bm[x["mint"]].append(x)
    for x in g:
        bg[x["mint"]][x["funder"]] += x["n_members"]
    picks = {"none -> funder": [], "funder -> other funder": []}
    for mint, rows in sorted(bm.items()):
        fm = collections.Counter(x["funder"] for x in rows)
        lost, gained = fm - bg[mint], bg[mint] - fm
        if not lost:
            continue
        for x in rows:
            if x["funder"] in lost and x.get("first_in") and len(rows) <= 40:
                kind = "none -> funder" if x["funder"] is None else "funder -> other funder"
                if len(picks[kind]) < n and sum(lost.values()) == 1:      # one changed member: unambiguous
                    picks[kind].append((mint, x, next(iter(gained))))
    out = collections.Counter()
    detail = []
    for kind, lst in picks.items():
        for mint, x, new_funder in lst:
            r = inspect(x["w"], mint, ts(x["first_in"]))
            verdict = "not checked: " + r["status"]
            if r["status"] == "ok":
                verdict = ("chain agrees with the regenerated query" if r["chain_funder"] == new_funder
                           else "chain agrees with the earlier rows" if r["chain_funder"] == x["funder"] else "chain agrees with neither")
            out[f"{kind}: {verdict}"] += 1
            detail.append({"kind": kind, "verdict": verdict, **{k: v for k, v in r.items() if k not in ("chain_funder",)},
                           "new_funder_is_creator": new_funder == next(y["w"] for y in bm[mint] if y["is_creator"])})
    res = {"sampled": {k: len(v) for k, v in picks.items()}, "result": dict(out), "detail": detail}
    (ROOT / "recon" / "out" / "b1a_funder_roundtrip_pbday.json").write_text(json.dumps(res, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "detail"}, indent=1))
    for x in detail:
        print("  ", x)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 6)
