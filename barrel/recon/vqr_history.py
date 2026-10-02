"""MR-14 order 2, follow-up to the halt: for the sampled post-BOOST pools, what did the swap
EVENTS carry as virtual_quote_reserves early in the pool's life and late in it?

The pool accounts say 11 of 31 post-BOOST pools hold 0 today and 20 hold about 17.58 SOL. That
is today's state. This reads each pool's own swap transactions over RPC (free), decodes the
events with the pinned full layout, and reports the field at the oldest and newest swaps found,
so "zero from birth" can be told from "zeroed later".

Bounded: at most MAX_PAGES pages of signatures per pool. A pool whose history is longer than
that is reported as "history not reached", not guessed.
  python barrel/recon/vqr_history.py
"""
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as d

OUT = pathlib.Path(__file__).resolve().parents[1] / "recon" / "out"
MAX_PAGES = 12          # 12,000 signatures per pool
N_EACH = 4              # swaps decoded at each end


def swaps_in(sig: str, pool: str, layouts) -> list[dict]:
    tx = d.rpc("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1}])
    if not tx or (tx.get("meta") or {}).get("err"):
        return []
    out = []
    for s in d.decode_swaps(tx, layouts):
        dec = s["decoded"]
        if dec.get("pool") == pool:
            out.append({"t": tx.get("blockTime"), "slot": tx.get("slot"), "event": s["event"],
                        "vqr": dec.get("virtual_quote_reserves"), "vqr_absent": "virtual_quote_reserves" in s["absent"],
                        "q": dec.get("pool_quote_token_reserves"), "body_len": s["body_len"]})
    return out


def history(pool: str, layouts) -> dict:
    sigs, before, pages = [], None, 0
    while pages < MAX_PAGES:
        opts = {"limit": 1000}
        if before:
            opts["before"] = before
        page = d.rpc("getSignaturesForAddress", [pool, opts]) or []
        pages += 1
        sigs += [s for s in page if not s.get("err")]
        if len(page) < 1000:
            break
        before = page[-1]["signature"]
    reached_birth = pages < MAX_PAGES or len(page) < 1000
    newest, oldest = [], []
    for s in sigs:                                   # newest first
        newest += swaps_in(s["signature"], pool, layouts)
        if len(newest) >= N_EACH:
            break
    if reached_birth:
        for s in reversed(sigs):                     # oldest first
            oldest += swaps_in(s["signature"], pool, layouts)
            if len(oldest) >= N_EACH:
                break
    return {"signatures": len(sigs), "reached_birth": reached_birth, "oldest": oldest[:N_EACH], "newest": newest[:N_EACH]}


def main() -> None:
    chk = json.load(open(OUT / "vqr_check.json"))["pools"]
    layouts = d.load_event_layouts()
    res = []
    for p in chk:
        if p["era"] != "post_boost":
            continue
        h = history(p["pool"], layouts)
        h.update(pool=p["pool"], grad_day=p["grad_day"], account_vqr_now=p.get("virtual_quote_reserves"))
        res.append(h)
        o = sorted({(x["vqr"], x["vqr_absent"]) for x in h["oldest"]}, key=str)
        n = sorted({(x["vqr"], x["vqr_absent"]) for x in h["newest"]}, key=str)
        print(f"{p['grad_day']} {p['pool'][:8]}  now={p.get('virtual_quote_reserves')}  sigs={h['signatures']:6d} birth_reached={h['reached_birth']}  "
              f"oldest swaps (vqr, absent)={o}  newest={n}", flush=True)
        time.sleep(0.1)
    (OUT / "vqr_history.json").write_text(json.dumps(res, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
