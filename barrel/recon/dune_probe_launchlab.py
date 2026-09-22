import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent)); import dune_roundtrip as rt
KEY = rt.api_key()
for t, f in (("raydium_solana.raydium_launchpad_evt_poolcreateevent", "evt_block_date = DATE '2025-06-01'"),
             ("raydium_solana.raydium_launchpad_call_migrate_to_cpswap", "call_block_date = DATE '2025-06-01'")):
    e = rt.dune("POST", "/sql/execute", KEY, {"sql": f"SELECT * FROM {t} WHERE {f} LIMIT 1", "performance": "medium"}).get("execution_id")
    t0 = time.time()
    while time.time() - t0 < 600:
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"): break
        time.sleep(4)
    rows = (rt.dune("GET", f"/execution/{e}/results", KEY).get("result") or {}).get("rows")
    print(f"{t}  credits={st.get('execution_cost_credits')}\n   " + (", ".join(rows[0].keys()) if rows else "(no rows)"))
