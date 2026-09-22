"""Quote-mint mix on 2026-09-01: what share of PumpSwap swaps are SOL-quoted.
Decides the coverage of a SOL-denominated cost model. Aggregate only."""
import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
WSOL = "So11111111111111111111111111111111111111112"
SQL = f"""
WITH e AS (
  SELECT pool, quote_amount_in AS quote, lp_fee FROM pumpdotfun_solana.pump_amm_evt_buyevent WHERE evt_block_date = DATE '2026-09-01'
  UNION ALL
  SELECT pool, quote_amount_out, lp_fee FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE evt_block_date = DATE '2026-09-01'),
p AS (SELECT pool, quote_mint FROM pumpdotfun_solana.pump_amm_evt_createpoolevent)
SELECT CASE WHEN p.quote_mint = '{WSOL}' THEN 'SOL' WHEN p.quote_mint IS NULL THEN 'pool not in createpoolevent' ELSE 'other' END AS quote_kind,
       COUNT(*) AS events, COUNT(DISTINCT e.pool) AS pools,
       SUM(CAST(e.quote AS double)) / 1e9 AS quote_sum_over_1e9
FROM e LEFT JOIN p ON e.pool = p.pool
GROUP BY 1 ORDER BY events DESC"""
e = rt.dune("POST", "/sql/execute", KEY, {"sql": SQL, "performance": "medium"}).get("execution_id")
print("execution_id:", e)
t0 = time.time(); r = None
while time.time() - t0 < 900:
    st = rt.dune("GET", f"/execution/{e}/status", KEY)
    if st.get("is_execution_finished"):
        res = rt.dune("GET", f"/execution/{e}/results", KEY)
        r = {"state": st.get("state"), "credits": st.get("execution_cost_credits"), "rows": (res.get("result") or {}).get("rows"), "error": res.get("error")}
        break
    time.sleep(5)
r = r or {"state": "NOT RUN (15 min)", "credits": None, "rows": None, "error": None}
print(f"state={r['state']} credits={r['credits']} error={r['error']}")
for row in r["rows"] or []: print("  ", row)
(OUT / "dune_quote_mix.json").write_text(json.dumps({"id": e, "result": r}, indent=1, default=str), encoding="utf-8")
