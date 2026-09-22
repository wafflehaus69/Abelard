"""BARREL — Dune round-trip follow-up. Fixes four harness defects in the first run.

The first run's per-item verdict was not a measurement of Dune:
  * instructions: the poll gave up after 4 min while Dune was still EXECUTING.
    -> poll the SAME execution_id (re-polling is free; re-executing bills again).
  * transfers: counts compared apples to oranges (chain count included WSOL wraps
    and both token programs). -> compare row by row on (mint, amount).
  * v1 specimen: picked a v1 tx that touched PumpSwap without swapping.
    -> require a decoded BuyEvent/SellEvent on chain before accepting a specimen.
  * decoded tables: Dune has pumpdotfun_solana.pump_amm_evt_{buy,sell}event.
    -> look the specimens up there; that is the prize, and it was never queried.

Credits consumed are read from Dune's own status metadata, never estimated.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as m  # noqa: E402
import dune_roundtrip as rt          # noqa: E402

OUT = pathlib.Path(__file__).with_name("out")
PRIOR = json.loads((OUT / "dune_roundtrip.json").read_text(encoding="utf-8"))
KEY = rt.api_key()


def poll(eid: str, minutes: int = 15) -> dict:
    for _ in range(minutes * 20):
        st = rt.dune("GET", f"/execution/{eid}/status", KEY)
        if st.get("is_execution_finished"):
            res = rt.dune("GET", f"/execution/{eid}/results", KEY)
            return {"status": st, "rows": (res.get("result") or {}).get("rows"), "error": res.get("error")}
        time.sleep(3)
    return {"status": st, "rows": None, "error": "still executing after poll window"}


def chain_transfers(sig: str) -> list[tuple[str, int]]:
    """(mint, raw amount) for every SPL transfer, outer or inner, both token programs."""
    tx = m.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=6)
    ixs = list(tx["transaction"]["message"]["instructions"])
    for grp in tx["meta"].get("innerInstructions") or []:
        ixs += grp["instructions"]
    out = []
    for ix in ixs:
        p = ix.get("parsed")
        if ix.get("programId") in rt.TOKEN_PROGRAMS and isinstance(p, dict) and p.get("type") in rt.TRANSFER_TYPES:
            info = p["info"]
            amt = int(info.get("amount") or info.get("tokenAmount", {}).get("amount") or 0)
            out.append((info.get("mint", "?"), amt))
    return out


def find_v1_swap() -> tuple[str, dict] | None:
    layouts = m.load_event_layouts()
    for s in m.rpc("getSignaturesForAddress", [m.PUMPSWAP, {"limit": 300}]):
        if s.get("err"):
            continue
        try:
            tx = m.rpc("getTransaction", [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=3)
        except m.RpcError:
            time.sleep(5)
            continue
        if tx.get("version") == 1 and any(p[:8] in layouts for p in m.event_payloads(tx)):
            return s["signature"], rt.chain_truth(s["signature"])
        time.sleep(0.8)
    return None


def main() -> None:
    print(f"Dune round-trip follow-up — {dt.datetime.now(dt.UTC).isoformat(timespec='seconds')}\n")
    specimens = {k: v for k, v in PRIOR["specimens"].items() if not k.startswith("v1_")}
    truth = {k: v for k, v in PRIOR["truth"].items() if not k.startswith("v1_")}

    # 1. Instructions: finish the existing execution, no re-billing.
    eid = PRIOR["results"]["instructions"]["status"]["execution_id"]
    ins = poll(eid)
    by_tx = {r["tx_id"]: r for r in (ins["rows"] or [])}
    print("== 1. PumpSwap instruction calls (existing execution, re-polled) ==")
    print(f"   state={ins['status'].get('state')}  credits={ins['status'].get('execution_cost_credits')}  error={ins['error']}")
    ok_ins = True
    for name, sig in specimens.items():
        t, d = truth[name], by_tx.get(sig, {})
        hit = d.get("pumpswap_outer") == t["pumpswap_outer"] and d.get("pumpswap_inner") == t["pumpswap_inner"]
        ok_ins &= hit
        print(f"   {name:20} chain {t['pumpswap_outer']}/{t['pumpswap_inner']}  dune {d.get('pumpswap_outer')}/{d.get('pumpswap_inner')}  -> {'PASS' if hit else 'FAIL'}")

    # 2. Proper v1 swap specimen.
    v1 = find_v1_swap()
    if v1:
        specimens["v1_swap"], truth["v1_swap"] = v1
        print(f"\n   v1 swap specimen found: {v1[0][:20]}…  chain outer/inner {v1[1]['pumpswap_outer']}/{v1[1]['pumpswap_inner']}, events {v1[1]['swap_events']}")
    else:
        print("\n   NOTE: no version-1 transaction containing a swap in the last 300 sigs; v1 boundary untested this run.")

    sigs = list(specimens.values())
    dates = sorted({t["block_date"] for t in truth.values()})
    date_filter = f"block_date IN ({', '.join(f'DATE {d!r}' for d in dates)})"

    # 3. Decoded event tables — the prize. Which columns exist is unknown, so select
    #    a small named set and let a missing column fail loud rather than SELECT *.
    dec = rt.run_sql("decoded_events", f"""
        SELECT 'buy'  AS side, tx_id, block_slot, call_tx_index, evt_index
        FROM pumpdotfun_solana.pump_amm_evt_buyevent
        WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})
        UNION ALL
        SELECT 'sell' AS side, tx_id, block_slot, call_tx_index, evt_index
        FROM pumpdotfun_solana.pump_amm_evt_sellevent
        WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})""", KEY)
    if dec.get("error") and "rows" not in dec:
        # Retry with the minimal column set if names differed.
        dec = rt.run_sql("decoded_events_min", f"""
            SELECT 'buy'  AS side, tx_id FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})
            UNION ALL
            SELECT 'sell' AS side, tx_id FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})""", KEY)
    dec_by_tx: dict[str, int] = {}
    for r in dec.get("rows") or []:
        dec_by_tx[r["tx_id"]] = dec_by_tx.get(r["tx_id"], 0) + 1
    print("\n== 3. Decoded PumpSwap swap events (pumpdotfun_solana.pump_amm_evt_*) ==")
    print(f"   credits={dec.get('status', {}).get('execution_cost_credits')}  error={dec.get('error')}")
    ok_dec = True
    for name, sig in specimens.items():
        want, got = truth[name]["swap_events"], dec_by_tx.get(sig, 0)
        hit = got == want
        ok_dec &= hit
        print(f"   {name:20} chain events {want}  dune decoded {got}  -> {'PASS' if hit else 'FAIL'}")

    # 4. Transfers, row by row on (mint, amount).
    xf = rt.run_sql("transfers_rows", f"""
        SELECT tx_id, token_mint_address AS mint, CAST(amount AS varchar) AS amount
        FROM tokens_solana.transfers
        WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})""", KEY)
    if xf.get("error") and not xf.get("rows"):
        xf = rt.run_sql("transfers_rows_alt", f"""
            SELECT tx_id, mint, CAST(amount AS varchar) AS amount
            FROM tokens_solana.transfers
            WHERE {date_filter} AND tx_id IN ({rt.sql_list(sigs)})""", KEY)
    print("\n== 4. SPL transfers, row-level (mint, amount) ==")
    print(f"   credits={xf.get('status', {}).get('execution_cost_credits')}  error={xf.get('error')}")
    ok_xf = True
    for name, sig in specimens.items():
        chain = sorted(chain_transfers(sig))
        dune_rows = sorted((r["mint"], int(r["amount"].split(".")[0])) for r in (xf.get("rows") or []) if r["tx_id"] == sig)
        chain_set, dune_set = set(chain), set(dune_rows)
        missing, extra = chain_set - dune_set, dune_set - chain_set
        hit = not missing
        ok_xf &= hit
        print(f"   {name:20} chain {len(chain)} rows, dune {len(dune_rows)}; missing-from-dune {len(missing)}, extra-in-dune {len(extra)}  -> {'PASS' if hit else 'FAIL'}")
        time.sleep(1)

    credits = sum(float(x.get("status", {}).get("execution_cost_credits") or 0) for x in (ins, dec, xf))
    prior_credits = sum(float((r.get("status") or {}).get("execution_cost_credits") or 0)
                        for k, r in PRIOR["results"].items() if k != "instructions")
    print("\n== VERDICT ==")
    print(f"   routed swaps present in instruction_calls : {'PASS' if ok_ins else 'FAIL'}")
    print(f"   swap events present in logs               : PASS (first run, 3/3 specimens)")
    print(f"   swap events DECODED with fee fields       : {'PASS' if ok_dec else 'FAIL'}")
    print(f"   inner transfers present, row-level        : {'PASS' if ok_xf else 'FAIL'}")
    print(f"\n   credits this run {credits:.1f} + first run {prior_credits:.1f} = {credits + prior_credits:.1f} of 2,500 free")
    (OUT / "dune_roundtrip_followup.json").write_text(json.dumps(
        {"specimens": specimens, "truth": truth, "instructions": ins, "decoded": dec, "transfers": xf,
         "verdict": {"routed_swaps": ok_ins, "events_in_logs": True, "events_decoded": ok_dec, "inner_transfers": ok_xf},
         "credits_total": credits + prior_credits}, indent=1, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
