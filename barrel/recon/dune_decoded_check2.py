"""Decoded-event round-trip, second attempt (ix_name is buy-only; dropped), plus a
quantification of impossible fee rows on 2026-09-01 (sums exceeded SOL supply)."""
import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt, decode_pumpswap_events as m
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
FOLLOW = json.loads((OUT / "dune_roundtrip_followup.json").read_text(encoding="utf-8"))
spec, truth = FOLLOW["specimens"], FOLLOW["truth"]
sigs = list(spec.values()); dates = sorted({t["block_date"] for t in truth.values()})
df = f"evt_block_date IN ({', '.join(f'DATE {d!r}' for d in dates)})"
COLS = "evt_tx_id, evt_is_inner, lp_fee, protocol_fee, coin_creator_fee, coin_creator_fee_basis_points"
Q = {
 "specimens": f"""
   SELECT 'buy' AS side, {COLS}, quote_amount_in AS quote FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE {df} AND evt_tx_id IN ({rt.sql_list(sigs)})
   UNION ALL
   SELECT 'sell', {COLS}, quote_amount_out FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {df} AND evt_tx_id IN ({rt.sql_list(sigs)})""",
 "sanity_0901": """
   WITH e AS (
     SELECT 'buy' AS side, lp_fee, protocol_fee, coin_creator_fee, quote_amount_in AS quote FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE evt_block_date = DATE '2026-09-01'
     UNION ALL
     SELECT 'sell', lp_fee, protocol_fee, coin_creator_fee, quote_amount_out FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date = DATE '2026-09-01')
   SELECT side, COUNT(*) AS events,
          COUNT_IF(lp_fee > quote OR protocol_fee > quote OR coin_creator_fee > quote) AS impossible_rows,
          SUM(CASE WHEN lp_fee <= quote AND protocol_fee <= quote AND coin_creator_fee <= quote THEN CAST(lp_fee AS double) ELSE 0 END)/1e9 AS lp_sol_clean,
          SUM(CASE WHEN lp_fee <= quote AND protocol_fee <= quote AND coin_creator_fee <= quote THEN CAST(protocol_fee AS double) ELSE 0 END)/1e9 AS protocol_sol_clean,
          SUM(CASE WHEN lp_fee <= quote AND protocol_fee <= quote AND coin_creator_fee <= quote THEN CAST(coin_creator_fee AS double) ELSE 0 END)/1e9 AS creator_sol_clean,
          SUM(CASE WHEN lp_fee <= quote AND protocol_fee <= quote AND coin_creator_fee <= quote THEN CAST(quote AS double) ELSE 0 END)/1e9 AS quote_sol_clean
   FROM e GROUP BY side""",
}
ids = {n: rt.dune("POST", "/sql/execute", KEY, {"sql": q, "performance": "medium"}).get("execution_id") for n, q in Q.items()}
(OUT / "dune_decoded_check2_ids.json").write_text(json.dumps(ids, indent=1), encoding="utf-8")
res, pending, t0 = {}, dict(ids), time.time()
while pending and time.time() - t0 < 900:
    for n, e in list(pending.items()):
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"):
            r = rt.dune("GET", f"/execution/{e}/results", KEY)
            res[n] = {"state": st.get("state"), "credits": st.get("execution_cost_credits"), "rows": (r.get("result") or {}).get("rows"), "error": r.get("error")}
            del pending[n]
    if pending: time.sleep(5)
for n in pending: res[n] = {"state": "NOT RUN (15 min)", "credits": None, "rows": None, "error": None}

r = res["specimens"]; print(f"== A. decoded specimens ==  state={r['state']} credits={r['credits']} error={r['error']}")
by_tx = {}
for row in r["rows"] or []: by_tx.setdefault(row["evt_tx_id"], []).append(row)
layouts = m.load_event_layouts(); ok = True
for name, sig in spec.items():
    tx = m.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=6)
    mine = [e["decoded"] for e in m.decode_swaps(tx, layouts)]; theirs = by_tx.get(sig, [])
    match = len(mine) == len(theirs) and all(any(int(d["lp_fee"]) == c["lp_fee"] and int(d["protocol_fee"]) == c["protocol_fee"] and int(d["coin_creator_fee"]) == c["coin_creator_fee"] for d in theirs) for c in mine)
    ok &= match
    print(f"   {name:20} chain events {len(mine)}  dune {len(theirs)}  fees equal field-for-field: {'PASS' if match else 'FAIL'}")
    for d in theirs: print(f"      dune {d['side']:4} inner={d['evt_is_inner']} lp={d['lp_fee']} prot={d['protocol_fee']} cre={d['coin_creator_fee']} ({d['coin_creator_fee_basis_points']} bps) quote={d['quote']}")
    for c in mine:  print(f"      mine      lp={c['lp_fee']} prot={c['protocol_fee']} cre={c['coin_creator_fee']} ({c['coin_creator_fee_basis_points']} bps)")
    time.sleep(1.2)
r = res["sanity_0901"]; print(f"\n== B. 2026-09-01 impossible-row gate ==  state={r['state']} credits={r['credits']} error={r['error']}")
for row in r["rows"] or []:
    q = row["quote_sol_clean"] or 0
    print(f"   {row['side']:4} events {row['events']:,}  impossible {row['impossible_rows']:,}  clean quote {q:,.0f} SOL  lp {row['lp_sol_clean']:,.0f} ({10000*row['lp_sol_clean']/q if q else 0:.1f} bps)  prot {row['protocol_sol_clean']:,.0f} ({10000*row['protocol_sol_clean']/q if q else 0:.1f} bps)  creator {row['creator_sol_clean']:,.0f} ({10000*row['creator_sol_clean']/q if q else 0:.1f} bps)")
total = sum(float(x["credits"] or 0) for x in res.values())
print(f"\ncredits this pass: {total:.2f}   decoded verdict: {'PASS' if ok else 'FAIL'}")
(OUT / "dune_decoded_check2.json").write_text(json.dumps({"ids": ids, "results": res, "pass": ok, "credits": total}, indent=1, default=str), encoding="utf-8")
