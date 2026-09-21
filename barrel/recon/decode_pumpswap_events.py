"""BARREL M0 — A5 fee legs, read from PumpSwap's own swap events.

Supersedes the balance-delta method of probe_fee_legs.py for identifying fee
legs. That method had to *infer* which account was a fee recipient, and needed
two fixes (a single-hop filter, and per-owner aggregation) before its numbers
were even plausible — and still could not name its largest leg. PumpSwap emits
an Anchor `BuyEvent` / `SellEvent` on every swap that carries each fee leg BY
NAME with its configured basis points. Reading that is a lookup, not an
inference, and it works on routed swaps too, so no filter is needed.

    python barrel/recon/decode_pumpswap_events.py [n_swaps]

Layout comes from the pinned IDL in recon/idl/pump_amm.json (see PROVENANCE.json).

VERSIONING — the one trap in this method. Anchor events gain fields over time;
the current IDL is the newest layout, and older swaps emit SHORTER events. The
decoder reads fields in order until the bytes run out and records the rest as
ABSENT. Absent means "this leg did not exist in the event version that swap
emitted" — it is never defaulted to zero ([E1]). That distinction is exactly what
A5's per-era fee schedule needs: event length is itself a dated version marker.

Keyless, read-only. Transaction data only; no token metadata is fetched (§1 / A7).
"""

from __future__ import annotations

import base64
import collections
import datetime as dt
import hashlib
import json
import pathlib
import struct
import sys
import time
import urllib.request

RPC = "https://api.mainnet-beta.solana.com"
PUMPSWAP = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
IDL_PATH = pathlib.Path(__file__).with_name("idl") / "pump_amm.json"

# Anchor emit_cpi! prefixes the self-CPI instruction data with this tag.
EVENT_IX_TAG = hashlib.sha256(b"anchor:event").digest()[:8]

FEE_LEGS = ("lp_fee", "protocol_fee", "coin_creator_fee", "cashback", "buyback_fee", "holder_rewards")
# Each leg's configured rate lives in a differently-named field; map it explicitly
# rather than guessing by string suffix.
LEG_BPS_FIELD = {
    "lp_fee": "lp_fee_basis_points",
    "protocol_fee": "protocol_fee_basis_points",
    "coin_creator_fee": "coin_creator_fee_basis_points",
    "cashback": "cashback_fee_basis_points",
    "buyback_fee": "buyback_fee_basis_points",
    "holder_rewards": "holder_rewards_bps",
}

# Legs the TRADER pays on top of the pool amount. Established by conservation on
# live swaps (2026-09-21, n=150 + 60 buys): gross - net == lp + protocol + creator
# on every sell and every buy once the buy variants are read correctly.
# buyback_fee, holder_rewards and cashback are CARVE-OUTS — redistributions of the
# fees above (buyback_fee_basis_points=5000 means 50% of the protocol fee, not
# 5000 bps of the trade). Summing them in would double-count trader cost.
ADDITIVE_LEGS = ("lp_fee", "protocol_fee", "coin_creator_fee")

# Which event field is the trader's gross side and which is the pool's net side.
# The SAME field names mean opposite things in the two buy variants:
#   buy                : user_quote_amount_in = what the user paid (gross);
#                        quote_amount_in      = what reached the pool (net)
#   buy_exact_quote_in : quote_amount_in      = what the user paid (gross);
#                        user_quote_amount_in = what reached the pool (net)
# An ix_name not listed here is REFUSED, not guessed ([E1]); a new variant means
# re-running the conservation check before its numbers are used.
BUY_SIDES = {
    "buy": ("user_quote_amount_in", "quote_amount_in"),
    "buy_exact_quote_in": ("quote_amount_in", "user_quote_amount_in"),
}
SELL_SIDES = ("quote_amount_out", "user_quote_amount_out")   # pool gross, user net


def trader_sides(e: dict) -> tuple[int, int] | None:
    """(gross, net) quote amounts for this swap, or None if the variant is unknown."""
    d = e["decoded"]
    if e["event"] == "SellEvent":
        g, n = SELL_SIDES
    else:
        sides = BUY_SIDES.get(d.get("ix_name"))
        if sides is None:
            return None
        g, n = sides
    if g not in d or n not in d:
        return None
    return d[g], d[n]


class RpcError(RuntimeError):
    pass


class DecodeError(RuntimeError):
    pass


# --------------------------------------------------------------------------- rpc

def rpc(method: str, params: list, tries: int = 5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(RPC, data=body, headers={"Content-Type": "application/json"})
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


# ------------------------------------------------------------------------ base58

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58)}


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        if ch not in _B58_IDX:
            raise DecodeError(f"invalid base58 character {ch!r}")
        n = n * 58 + _B58_IDX[ch]
    body = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * (len(s) - len(s.lstrip("1"))) + body


def b58encode(b: bytes) -> str:
    n = int.from_bytes(b, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    return "1" * (len(b) - len(b.lstrip(b"\x00"))) + out


# ------------------------------------------------------------------------- borsh

def load_event_layouts() -> dict[bytes, tuple[str, list[dict]]]:
    idl = json.loads(IDL_PATH.read_text(encoding="utf-8"))
    types = {t["name"]: t for t in idl.get("types", [])}
    layouts = {}
    for ev in idl.get("events", []):
        if ev["name"] in ("BuyEvent", "SellEvent"):
            layouts[bytes(ev["discriminator"])] = (ev["name"], types[ev["name"]]["type"]["fields"])
    if len(layouts) != 2:
        raise DecodeError(f"expected BuyEvent+SellEvent in IDL, found {sorted(n for n, _ in layouts.values())}")
    return layouts


_FIXED = {"u8": 1, "bool": 1, "u16": 2, "u32": 4, "u64": 8, "i64": 8, "u128": 16, "i128": 16, "pubkey": 32}


def decode_fields(buf: bytes, fields: list[dict]) -> tuple[dict, list[str], int]:
    """Decode fields in order; stop cleanly at end of buffer.

    Returns (decoded, absent_field_names, bytes_consumed). A field that does not
    fit in the remaining bytes, and every field after it, is ABSENT — never zero.
    """
    out, pos = {}, 0
    for i, f in enumerate(fields):
        name, typ = f["name"], f["type"]
        if typ == "string":
            if pos + 4 > len(buf):
                return out, [x["name"] for x in fields[i:]], pos
            (n,) = struct.unpack_from("<I", buf, pos)
            if pos + 4 + n > len(buf):
                return out, [x["name"] for x in fields[i:]], pos
            out[name] = buf[pos + 4: pos + 4 + n].decode("utf-8", errors="replace")
            pos += 4 + n
            continue
        if typ not in _FIXED:
            raise DecodeError(f"unsupported IDL type {typ!r} for field {name}")
        size = _FIXED[typ]
        if pos + size > len(buf):
            return out, [x["name"] for x in fields[i:]], pos
        raw = buf[pos: pos + size]
        if typ == "pubkey":
            out[name] = b58encode(raw)
        elif typ == "bool":
            out[name] = raw[0] != 0
        else:
            out[name] = int.from_bytes(raw, "little", signed=typ.startswith("i"))
        pos += size
    return out, [], pos


# ------------------------------------------------------------------- extraction

def event_payloads(tx: dict) -> list[bytes]:
    """Every AUTHENTIC PumpSwap event payload in a transaction.

    Two emission paths exist, and they are NOT equally trustworthy:

    * emit_cpi! — a self-CPI inner instruction whose programId is PumpSwap and
      whose data starts with EVENT_IX_TAG. Only PumpSwap can invoke itself with
      its event-authority signer, so this path cannot be forged by a third party.
    * emit!    — a 'Program data: <base64>' log line. ANY program in the
      transaction can print one. On a routed swap a hostile program in the route
      could print a well-formed fake BuyEvent, and a decoder that reads every
      such line would count it. The data is adversarial by assumption (§0).

    So log-path events are accepted ONLY when printed inside a PumpSwap frame:
    the log stack is tracked through 'Program <id> invoke [n]' / 'success' /
    'failed' lines and a 'Program data:' line counts only if PumpSwap is the
    innermost executing program at that point. The first version of this
    function accepted log lines from any program; no forged event was observed
    in samples, but that is an absence of evidence, not a guarantee.
    """
    found: list[bytes] = []
    for inner in (tx["meta"].get("innerInstructions") or []):
        for ix in inner.get("instructions", []):
            if ix.get("programId") != PUMPSWAP or "data" not in ix:
                continue
            raw = b58decode(ix["data"])
            if raw[:8] == EVENT_IX_TAG:
                found.append(raw[8:])

    stack: list[str] = []
    for line in (tx["meta"].get("logMessages") or []):
        if line.startswith("Program ") and " invoke [" in line:
            stack.append(line.split()[1])
        elif line.startswith("Program ") and (line.endswith(" success") or " failed" in line):
            if stack and line.split()[1] == stack[-1]:
                stack.pop()
        elif line.startswith("Program data: "):
            if not stack or stack[-1] != PUMPSWAP:
                continue                       # printed by some other program: refuse
            try:
                found.append(base64.b64decode(line[len("Program data: "):]))
            except Exception:  # noqa: BLE001 — a non-base64 log line is not an event
                pass
    return list(dict.fromkeys(found))


def decode_swaps(tx: dict, layouts) -> list[dict]:
    swaps = []
    for payload in event_payloads(tx):
        layout = layouts.get(payload[:8])
        if layout is None:
            continue
        name, fields = layout
        body = payload[8:]
        decoded, absent, used = decode_fields(body, fields)
        swaps.append({
            "event": name,
            "decoded": decoded,
            "absent": absent,
            "body_len": len(body),
            "trailing_bytes": len(body) - used,
        })
    return swaps


# ------------------------------------------------------------------------ report

def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def main() -> None:
    want = int(sys.argv[1]) if len(sys.argv) > 1 else 80
    layouts = load_event_layouts()
    print(f"BARREL M0 — PumpSwap event decode — {dt.datetime.now(dt.UTC).isoformat(timespec='seconds')}")
    print(f"IDL: {IDL_PATH.name}  target swaps: {want}\n")

    sigs = rpc("getSignaturesForAddress", [PUMPSWAP, {"limit": min(1000, want * 3)}])
    events, tx_seen, tx_no_event, rpc_fail = [], 0, 0, 0
    for s in sigs:
        if len(events) >= want:
            break
        if s.get("err"):
            continue
        try:
            tx = rpc("getTransaction",
                     [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                     tries=3)
        except RpcError:
            rpc_fail += 1
            continue
        tx_seen += 1
        got = decode_swaps(tx, layouts)
        if not got:
            tx_no_event += 1
        events.extend(got)
        time.sleep(0.12)

    print(f"transactions fetched      : {tx_seen}")
    print(f"  with no Buy/Sell event  : {tx_no_event}  (pool admin, GetFees, failed routes)")
    print(f"  rpc failures            : {rpc_fail}")
    print(f"swap events decoded       : {len(events)}  "
          f"({sum(e['event']=='BuyEvent' for e in events)} buy / {sum(e['event']=='SellEvent' for e in events)} sell)\n")
    if not events:
        print("NO EVENTS DECODED — reporting empty, not inferring a schedule.")
        return

    # Version fingerprint: body length + which fields were absent.
    print("== event versions seen (body length / absent tail) ==")
    versions = collections.Counter((e["event"], e["body_len"], tuple(e["absent"])) for e in events)
    for (name, blen, absent), n in versions.most_common():
        tail = f"absent: {', '.join(absent)}" if absent else "complete vs current IDL"
        print(f"  {n:4} x {name:9} {blen:4} bytes  {tail}")
    trailing = [e for e in events if e["trailing_bytes"]]
    if trailing:
        print(f"  WARNING: {len(trailing)} events carry bytes BEYOND the pinned IDL — the "
              "on-chain layout is newer than the IDL. Re-fetch before trusting field alignment.")
    print()

    print("== fee legs: configured rate (bps field in the event) ==")
    print(f"{'leg':18} {'present':>8} {'nonzero':>8}   configured bps (value: count)")
    for leg in FEE_LEGS:
        bps_field = LEG_BPS_FIELD[leg]
        present = [e for e in events if bps_field in e["decoded"]]
        vals = collections.Counter(e["decoded"][bps_field] for e in present)
        nonzero = sum(n for v, n in vals.items() if v)
        shown = ", ".join(f"{v}: {n}" for v, n in vals.most_common(8))
        print(f"{leg:18} {len(present):>8} {nonzero:>8}   {shown or '-'}")
    print()

    print("== conservation: gross - net == lp + protocol + creator ==")
    ok, bad, unknown = 0, [], collections.Counter()
    for e in events:
        sides = trader_sides(e)
        if sides is None:
            unknown[e["decoded"].get("ix_name", e["event"])] += 1
            continue
        gross, net = sides
        fees = sum(e["decoded"].get(leg, 0) for leg in ADDITIVE_LEGS)
        if abs((gross - net) - fees) <= 1:
            ok += 1
            e["_gross"] = gross
        else:
            bad.append(e)
    print(f"  holds: {ok}   fails: {len(bad)}   unknown variant (refused): {dict(unknown)}")
    if bad:
        print("  WARNING: conservation failed — additive-leg model is wrong for these; "
              "their costs are EXCLUDED below, not estimated.")
    print()

    print("== trader cost, bps of the trader's gross side (conservation-verified swaps only) ==")
    print(f"{'leg':18} {'n':>5} {'p10':>7} {'median':>7} {'p90':>7} {'max':>7}")
    verified = [e for e in events if "_gross" in e and e["_gross"] > 0]
    for leg in ADDITIVE_LEGS:
        xs = [10_000 * e["decoded"][leg] / e["_gross"] for e in verified]
        print(f"{leg:18} {len(xs):>5} {pct(xs,.1):>7.1f} {pct(xs,.5):>7.1f} {pct(xs,.9):>7.1f} {max(xs):>7.1f}")
    tot = [10_000 * sum(e["decoded"][leg] for leg in ADDITIVE_LEGS) / e["_gross"] for e in verified]
    print(f"{'TOTAL per leg':18} {len(tot):>5} {pct(tot,.1):>7.1f} {pct(tot,.5):>7.1f} {pct(tot,.9):>7.1f} {max(tot):>7.1f}")
    with_cc = [t for t, e in zip(tot, verified) if e["decoded"].get("coin_creator_fee")]
    no_cc = [t for t, e in zip(tot, verified) if not e["decoded"].get("coin_creator_fee")]
    if with_cc:
        print(f"  split: creator fee charged  n={len(with_cc):>3}  median {pct(with_cc,.5):.1f} bps  max {max(with_cc):.1f}")
    if no_cc:
        print(f"         no creator fee       n={len(no_cc):>3}  median {pct(no_cc,.5):.1f} bps  max {max(no_cc):.1f}")
    print("  Round trip = entry leg + exit leg, so roughly double these, before slippage.")
    print()

    zero_fee = [e for e in events if all(not e["decoded"].get(leg) for leg in ADDITIVE_LEGS)]
    if zero_fee:
        print(f"zero-fee swaps: {len(zero_fee)} ({', '.join(sorted({e['decoded'].get('ix_name', e['event']) for e in zero_fee}))}) "
              "— UNEXPLAINED. Not the BOOST buy-and-burn: per the IDL that is its own "
              "instruction emitting BoostBuyAndBurnEvent, not a BuyEvent.")
        print()

    cc = collections.Counter(e["decoded"].get("coin_creator_fee_basis_points") for e in events)
    creators = {e["decoded"].get("coin_creator") for e in events}
    print(f"distinct coin_creator pubkeys in sample: {len(creators)}")
    print(f"coin_creator_fee_basis_points spread   : {dict(cc.most_common())}")
    print("\nNOTE: current era only. Per-era history needs the same decode run over")
    print("      BigQuery `Instructions` (program_id-clustered), blocked on a billing project.")


if __name__ == "__main__":
    main()
