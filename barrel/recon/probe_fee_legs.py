"""BARREL M0 — A5 fee-leg resolution, measured from executed swaps.

Amendment v1.1 §A5 requires the creator/fee-share leg "resolved against the live
fee program, not the README". R6 recorded the conflict this settles: pump.fun's
own program README says the creator fee "is set to 0 and is not used", while
2026-01-10 reporting describes a creator fee-sharing overhaul across up to ten
wallets. Documentation on both sides; no measurement on either.

So measure it. [E8] — no spec constant ships without an observed distribution
behind it, and a fee schedule is a spec constant that goes straight into §5.

Method, deliberately assumption-light: take recent executed PumpSwap swaps and
read the lamport and WSOL deltas the transaction actually produced. The largest
delta is the trade; every other account that GAINED quote-side value in the same
transaction is a fee leg, whatever the docs call it. Fee legs are then expressed
in basis points of the trade and grouped by recipient across the sample.

This resolves the CURRENT era only. A5 wants a per-era schedule
(pre-2026-01-10 / 2026-01-10-to-BOOST / post-BOOST), and historical eras need the
archival source that A1 is blocked on. What this can settle today, it settles.

    python barrel/recon/probe_fee_legs.py [n_transactions]

Keyless, read-only. Requests transaction data only — no token metadata is
fetched, per M0_TASKING §1 as restated in v1.1 §A7: the quarantine is a property
of the fetch layer.
"""

from __future__ import annotations

import collections
import datetime as dt
import json
import sys
import time
import urllib.request

RPC = "https://api.mainnet-beta.solana.com"
PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
WSOL = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000

# Swap-shaped PumpSwap instructions. Anything else (GetFees, pool admin) is not a trade.
SWAP_IX = {"Buy", "Sell", "BuyExactQuoteIn", "SellExactQuoteIn", "BuyExactIn", "SellExactOut"}

# A leg below this share of the trade is dust — rent, ATA creation, rounding —
# not a fee schedule. Reported separately rather than silently dropped ([E1]).
DUST_BPS = 1.0

# Single-hop filter. The first version of this probe took every PumpSwap
# transaction and called the largest quote-side gain the principal. On a
# multi-hop route (Jupiter, or an arbitrage touching two AMMs) the largest gain
# is an intermediate hop, so the OTHER hops are read as fee legs — which is how
# that run produced a median "fee load" of 95 bps and a maximum of 7,429 bps.
# The fee schedule is only observable on transactions where PumpSwap is the one
# venue that moved value, so everything else is excluded rather than modelled.
INFRASTRUCTURE_PROGRAMS = {
    "ComputeBudget111111111111111111111111111111",
    "11111111111111111111111111111111",                  # System
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",       # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",       # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",      # Associated Token Account
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",       # Memo
    PUMPSWAP,
    "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ",       # pump fee program
}


def invoked_programs(tx: dict) -> set[str]:
    ixs = list(tx["transaction"]["message"].get("instructions", []))
    for inner in (tx["meta"].get("innerInstructions") or []):
        ixs += inner.get("instructions", [])
    return {ix.get("programId", "") for ix in ixs}


def is_single_hop(tx: dict) -> bool:
    """True only if PumpSwap is the sole value-moving venue in this transaction."""
    foreign = invoked_programs(tx) - INFRASTRUCTURE_PROGRAMS
    if foreign:
        return False
    swaps = sum(
        1 for line in (tx["meta"].get("logMessages") or [])
        if "Instruction:" in line and line.split("Instruction:")[-1].strip() in SWAP_IX
    )
    return swaps == 1


class RpcError(RuntimeError):
    pass


def rpc(method: str, params: list, tries: int = 5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                RPC, data=body, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.load(resp)
            if "error" in payload:
                last = payload["error"]
                time.sleep(0.7 * (attempt + 1))
                continue
            return payload["result"]
        except Exception as exc:  # noqa: BLE001 — reported, never swallowed
            last = exc
            time.sleep(0.7 * (attempt + 1))
    raise RpcError(f"{method} failed after {tries} attempts: {last!r}")


def account_keys(tx: dict) -> list[str]:
    """Full key list in the order preBalances/postBalances are indexed.

    Static keys first, then loaded writable, then loaded readonly. Getting this
    order wrong silently misattributes every fee leg to the wrong account, so it
    is done explicitly rather than by trusting accountKeys alone.
    """
    keys = [k["pubkey"] if isinstance(k, dict) else k
            for k in tx["transaction"]["message"].get("accountKeys", [])]
    loaded = (tx.get("meta") or {}).get("loadedAddresses") or {}
    return keys + list(loaded.get("writable", [])) + list(loaded.get("readonly", []))


def quote_deltas(tx: dict) -> dict[str, float]:
    """Quote-side (SOL/WSOL) value change per OWNER, in SOL.

    Keyed by owner, not by account key, and that distinction is the whole
    correctness of this function. A trader who wraps SOL moves value from their
    wallet into a temporary WSOL account they own, and closing it moves the
    lamports back. Keyed by account, those two legs of one wrap look like a
    stranger receiving a large payment — which is how the previous run produced
    a "fee recipient" collecting 1,132 bps across three swaps. Collapsing an
    owner's own accounts together makes the wrap net to zero, which is what it is.
    """
    meta = tx["meta"]
    keys = account_keys(tx)
    deltas: dict[str, float] = collections.defaultdict(float)

    # index -> owner, for every token account the transaction touched.
    owner_of: dict[int, str] = {}
    for entries in (meta.get("preTokenBalances"), meta.get("postTokenBalances")):
        for e in entries or []:
            if e.get("owner"):
                owner_of[e["accountIndex"]] = e["owner"]

    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
    for idx, key in enumerate(keys):
        if idx < len(pre) and idx < len(post):
            deltas[owner_of.get(idx, key)] += (post[idx] - pre[idx]) / LAMPORTS

    def wsol_by_index(entries):
        return {
            e["accountIndex"]: float(e["uiTokenAmount"].get("uiAmount") or 0.0)
            for e in entries or []
            if e.get("mint") == WSOL
        }

    pre_w = wsol_by_index(meta.get("preTokenBalances"))
    post_w = wsol_by_index(meta.get("postTokenBalances"))
    for idx in set(pre_w) | set(post_w):
        if idx < len(keys):
            deltas[owner_of.get(idx, keys[idx])] += post_w.get(idx, 0.0) - pre_w.get(idx, 0.0)

    # The fee payer also paid the network fee; back it out so it is not read as a leg.
    if keys and meta.get("fee"):
        deltas[owner_of.get(0, keys[0])] += meta["fee"] / LAMPORTS

    return deltas


def swap_kind(tx: dict) -> str | None:
    for line in (tx["meta"].get("logMessages") or []):
        if "Instruction:" in line:
            name = line.split("Instruction:")[-1].strip()
            if name in SWAP_IX:
                return name
    return None


def analyse(tx: dict) -> dict | None:
    kind = swap_kind(tx)
    if kind is None:
        return None
    deltas = quote_deltas(tx)
    gains = {k: v for k, v in deltas.items() if v > 0}
    if not gains:
        return None

    # Largest quote-side gain is the counterparty of the trade (pool on a buy,
    # trader on a sell). Everything else that gained is a fee leg.
    principal_acct = max(gains, key=gains.get)
    principal = gains[principal_acct]
    if principal <= 0:
        return None

    legs = []
    for acct, amount in gains.items():
        if acct == principal_acct:
            continue
        legs.append({"recipient": acct, "sol": amount, "bps": 10_000 * amount / principal})
    legs.sort(key=lambda leg: -leg["bps"])
    return {"kind": kind, "principal_sol": principal, "legs": legs}


def main() -> None:
    want = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    print(f"BARREL M0 — A5 fee-leg probe — {dt.datetime.now(dt.UTC).isoformat()}")
    print(f"venue: PumpSwap {PUMPSWAP}\ntarget swaps: {want}\n")

    sigs = rpc("getSignaturesForAddress", [PUMPSWAP, {"limit": min(1000, want * 25)}])
    analysed, skipped_no_swap, skipped_multihop, skipped_rpc = [], 0, 0, 0
    for s in sigs:
        if len(analysed) >= want:
            break
        if s.get("err"):
            continue
        try:
            tx = rpc(
                "getTransaction",
                [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}],
                tries=3,
            )
        except RpcError:
            skipped_rpc += 1
            continue
        if swap_kind(tx) is None:
            skipped_no_swap += 1
            continue
        if not is_single_hop(tx):
            skipped_multihop += 1
            continue
        result = analyse(tx)
        if result is None:
            skipped_no_swap += 1
            continue
        analysed.append(result)
        time.sleep(0.15)

    print(f"single-hop swaps analysed : {len(analysed)}")
    print(f"skipped (not a swap)      : {skipped_no_swap}")
    print(f"skipped (multi-hop/routed): {skipped_multihop}")
    print(f"skipped (rpc)             : {skipped_rpc}\n")
    if skipped_multihop:
        share = skipped_multihop / max(1, skipped_multihop + len(analysed))
        print(f"  {share:.0%} of PumpSwap swaps seen were routed/multi-hop. That is a")
        print("  finding for §5 in its own right: Mando's own fills would route too.\n")
    if not analysed:
        print("NO SWAPS ANALYSED — reporting empty rather than inferring a schedule.")
        return

    by_recipient: dict[str, list[float]] = collections.defaultdict(list)
    leg_counts, totals = collections.Counter(), []
    for r in analysed:
        real = [leg for leg in r["legs"] if leg["bps"] >= DUST_BPS]
        leg_counts[len(real)] += 1
        totals.append(sum(leg["bps"] for leg in real))
        for leg in real:
            by_recipient[leg["recipient"]].append(leg["bps"])

    def pct(xs, p):
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else float("nan")

    print(f"== total fee load per swap, bps of principal (legs >= {DUST_BPS} bps) ==")
    print(f"  n={len(totals)}  p10={pct(totals,0.10):.1f}  median={pct(totals,0.50):.1f}  "
          f"p90={pct(totals,0.90):.1f}  min={min(totals):.1f}  max={max(totals):.1f}")
    print(f"  distinct fee legs per swap: {dict(sorted(leg_counts.items()))}\n")

    # The 0-vs-3 split above is the interesting shape. If a swap's fees are taken
    # on the OUTPUT token, a quote-side probe sees them only in one direction —
    # so split by direction rather than reporting a bimodal median as if it were
    # one population ([E14]: composition travels with the aggregate).
    print("== by swap direction (quote-side legs only) ==")
    print(f"{'direction':20} {'n':>4} {'0 legs':>7} {'>=1 leg':>8} {'median bps':>11}")
    by_kind: dict[str, list[dict]] = collections.defaultdict(list)
    for r in analysed:
        by_kind[r["kind"]].append(r)
    for kind, rows in sorted(by_kind.items()):
        real = [[leg for leg in r["legs"] if leg["bps"] >= DUST_BPS] for r in rows]
        zero = sum(1 for legs in real if not legs)
        tot = [sum(leg["bps"] for leg in legs) for legs in real if legs]
        med = f"{pct(tot,0.5):.1f}" if tot else "n/a"
        print(f"{kind:20} {len(rows):>4} {zero:>7} {len(rows)-zero:>8} {med:>11}")
    print("  A direction showing 0 legs is not fee-free — it pays on the base-token")
    print("  side, which this quote-side probe cannot see. Stated, not inferred away.\n")

    print("== leg structure on swaps where legs are visible ==")
    shapes = collections.Counter()
    for r in analysed:
        legs = [leg for leg in r["legs"] if leg["bps"] >= DUST_BPS]
        if legs:
            shapes[" + ".join(f"{leg['bps']:.1f}" for leg in legs)] += 1
    for shape, count in shapes.most_common(10):
        print(f"  {count:>3} x  {shape} bps")
    print()

    print("== recurring fee recipients ==")
    print(f"{'recipient':46} {'swaps':>6} {'median bps':>11} {'min':>7} {'max':>7}")
    for acct, bps in sorted(by_recipient.items(), key=lambda kv: -len(kv[1])):
        if len(bps) < 2:
            continue
        print(f"{acct:46} {len(bps):>6} {pct(bps,0.5):>11.1f} {min(bps):>7.1f} {max(bps):>7.1f}")

    singles = sum(1 for b in by_recipient.values() if len(b) < 2)
    print(f"\n  recipients seen once only: {singles} "
          "(expected — per-token creator/fee-share destinations differ by token)")
    print("\nNOTE: current era only. The per-era schedule A5 requires needs the archival")
    print("      source blocked at A1. Principal is the largest quote-side gain in the")
    print("      transaction; legs are every other account that gained quote-side value.")


if __name__ == "__main__":
    main()
