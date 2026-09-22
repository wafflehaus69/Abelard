"""One-row column probes for the tables strata R and N read. Wrong time-column guesses
fail at name resolution for 0 credits; the first success prints the column list."""
import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
TABLES = {
  "raydium_v4_init2":   "raydium_amm_solana.raydium_amm_call_initialize2",
  "launchlab_poolcreate": "raydium_solana.raydium_launchpad_evt_poolcreateevent",
  "launchlab_migrate":  "raydium_solana.raydium_launchpad_call_migrate_to_cpswap",
  "meteora_dlmm_trades": "meteora_version_dlmm.base_trades",
  "raydium_v4_trades":  "raydium_v4.trades",
  "pump_complete":      "pumpdotfun_solana.pump_evt_completeevent",
}
FILTERS = ["call_block_date = DATE '2025-02-15'", "evt_block_date = DATE '2025-02-15'", "block_date = DATE '2025-02-15'", "block_time >= TIMESTAMP '2025-02-15 00:00:00' AND block_time < TIMESTAMP '2025-02-15 00:10:00'"]
def run(sql):
    e = rt.dune("POST", "/sql/execute", KEY, {"sql": sql, "performance": "medium"}).get("execution_id")
    if not e: return {"state": "REFUSED"}
    t0 = time.time()
    while time.time() - t0 < 600:
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"):
            r = rt.dune("GET", f"/execution/{e}/results", KEY)
            return {"state": st.get("state"), "credits": st.get("execution_cost_credits"), "rows": (r.get("result") or {}).get("rows"), "error": r.get("error"), "id": e}
        time.sleep(4)
    return {"state": "NOT RUN"}
out, total = {}, 0.0
for name, t in TABLES.items():
    for f in FILTERS:
        r = run(f"SELECT * FROM {t} WHERE {f} LIMIT 1"); total += float(r.get("credits") or 0)
        if r.get("rows"):
            cols = list(r["rows"][0].keys()); out[name] = {"table": t, "filter": f, "columns": cols, "credits": r.get("credits")}
            print(f"{name:22} {t}\n   filter: {f.split(' ')[0]}   columns: {', '.join(cols)}"); break
        if r.get("state") == "QUERY_STATE_COMPLETED" and not r.get("rows"):
            out[name] = {"table": t, "filter": f, "columns": None, "note": "query ok, no rows on sample day"}; print(f"{name:22} {t}: no rows on 2025-02-15 with {f.split(' ')[0]}"); break
    else:
        out[name] = {"table": t, "error": "no filter resolved"}; print(f"{name:22} {t}: NO FILTER RESOLVED")
print(f"\ncredits: {total:.3f}")
(OUT / "dune_probe_venue_columns.json").write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
