import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent)); import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
T = {"spl_init2": "spl_token_solana.spl_token_call_initializemint2", "spl_setauth": "spl_token_solana.spl_token_call_setauthority",
     "t22_init2": "spl_token_2022_solana.spl_token_2022_call_initializemint2", "t22_setauth": "spl_token_2022_solana.spl_token_2022_call_setauthority",
     "t22_transferfee": "spl_token_2022_solana.spl_token_2022_call_transferfeeextension"}
res = {}
for n, t in T.items():
    e = rt.dune("POST", "/sql/execute", KEY, {"sql": f"SELECT * FROM {t} WHERE call_block_date = DATE '2026-09-01' LIMIT 1", "performance": "medium"}).get("execution_id")
    t0 = time.time()
    while time.time() - t0 < 600:
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"): break
        time.sleep(4)
    r = rt.dune("GET", f"/execution/{e}/results", KEY); rows = (r.get("result") or {}).get("rows")
    cols = [c for c in (rows[0].keys() if rows else []) if not c.startswith("call_") or c in ("call_block_slot","call_tx_id","call_tx_signer","call_is_inner")]
    res[n] = {"table": t, "columns": list(rows[0].keys()) if rows else None, "credits": st.get("execution_cost_credits"), "error": r.get("error")}
    print(f"{n:16} credits={st.get('execution_cost_credits')}  " + (", ".join(cols) if rows else f"NO ROWS / {str(r.get('error'))[:80]}"))
(OUT / "dune_probe_spl_columns.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
