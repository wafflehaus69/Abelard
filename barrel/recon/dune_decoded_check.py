"""Decoded-event round-trip and one-day cost unit, submitted together, long-polled.

  A. Specimen lookup in pumpdotfun_solana.pump_amm_evt_{buy,sell}event and a
     field-for-field comparison of Dune's decoded fee legs against this repo's
     own IDL decode of the chain's copy. Equality here is the strongest evidence
     the source can give: two independent decoders agreeing on the same bytes.
  B. One full day (2026-09-01) of buy+sell events, aggregated. Its credit cost
     is the unit order 3 extrapolates from. Aggregation only; no rows exported.
"""
import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt, decode_pumpswap_events as m
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
FOLLOW = json.loads((OUT / "dune_roundtrip_followup.json").read_text(encoding="utf-8"))
spec, truth = FOLLOW["specimens"], FOLLOW["truth"]
sigs = list(spec.values()); dates = sorted({t["block_date"] for t in truth.values()})
df = f"evt_block_date IN ({', '.join(f'DATE {d!r}' for d in dates)})"
COLS = "evt_tx_id, evt_is_inner, ix_name, lp_fee, protocol_fee, coin_creator_fee, coin_creator_fee_basis_points"
Q = {
 "specimens": f"""
   SELECT 'buy' AS side, {COLS}, quote_amount_in AS quote, user_quote_amount_in AS user_quote
   FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE {df} AND evt_tx_id IN ({rt.sql_list(sigs)})
   UNION ALL
   SELECT 'sell', {COLS}, quote_amount_out, user_quote_amount_out
   FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {df} AND evt_tx_id IN ({rt.sql_list(sigs)})""",
 "one_day": """
   SELECT side, COUNT(*) AS events, COUNT(DISTINCT evt_tx_id) AS txs,
          SUM(CAST(lp_fee AS double))/1e9 AS lp_sol, SUM(CAST(protocol_fee AS double))/1e9 AS protocol_sol,
          SUM(CAST(coin_creator_fee AS double))/1e9 AS creator_sol,
          COUNT_IF(coin_creator_fee > 0) AS events_with_creator_fee
   FROM (SELECT 'buy' AS side, evt_tx_id, lp_fee, protocol_fee, coin_creator_fee FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE evt_block_date = DATE '2026-09-01'
         UNION ALL
         SELECT 'sell', evt_tx_id, lp_fee, protocol_fee, coin_creator_fee FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date = DATE '2026-09-01')
   GROUP BY side""",
}
ids = {n: rt.dune("POST", "/sql/execute", KEY, {"sql": q, "performance": "medium"}).get("execution_id") for n, q in Q.items()}
(OUT / "dune_decoded_check_ids.json").write_text(json.dumps(ids, indent=1), encoding="utf-8")
res, pending, t0 = {}, dict(ids), time.time()
while pending and time.time() - t0 < 900:
    for n, e in list(pending.items()):
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"):
            r = rt.dune("GET", f"/execution/{e}/results", KEY)
            res[n] = {"state": st.get("state"), "credits": st.get("execution_cost_credits"),
                      "rows": (r.get("result") or {}).get("rows"), "error": r.get("error")}
            del pending[n]
    if pending: time.sleep(5)
for n in pending: res[n] = {"state": "NOT RUN (15 min)", "credits": None, "rows": None, "error": None}

# A. compare against our own chain decode
r = res["specimens"]; print(f"== A. decoded specimens ==  state={r['state']} credits={r['credits']} error={r['error']}")
dune_by_tx = {}
for row in r["rows"] or []: dune_by_tx.setdefault(row["evt_tx_id"], []).append(row)
layouts = m.load_event_layouts(); ok = True
for name, sig in spec.items():
    tx = m.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=6)
    mine = [e["decoded"] for e in m.decode_swaps(tx, layouts)]
    theirs = dune_by_tx.get(sig, [])
    match = len(mine) == len(theirs) and all(
        any(int(d["lp_fee"]) == c["lp_fee"] and int(d["protocol_fee"]) == c["protocol_fee"] and int(d["coin_creator_fee"]) == c["coin_creator_fee"]
            for d in theirs) for c in mine)
    ok &= match
    fees = "; ".join(f"{d['side']} lp={d['lp_fee']} prot={d['protocol_fee']} cre={d['coin_creator_fee']} inner={d['evt_is_inner']}" for d in theirs) or "(none)"
    print(f"   {name:20} chain events {len(mine)}  dune {len(theirs)}  fees equal {'PASS' if match else 'FAIL'}   [{fees}]")
    time.sleep(1.2)
# B. cost unit
r = res["one_day"]; print(f"\n== B. one full day 2026-09-01, aggregated ==  state={r['state']} credits={r['credits']} error={r['error']}")
for row in r["rows"] or []: print("  ", row)
total = sum(float(x["credits"] or 0) for x in res.values())
print(f"\ncredits this pass: {total:.2f}   decoded-events verdict: {'PASS' if ok else 'FAIL'}")
(OUT / "dune_decoded_check.json").write_text(json.dumps({"ids": ids, "results": res, "pass": ok, "credits": total}, indent=1, default=str), encoding="utf-8")
