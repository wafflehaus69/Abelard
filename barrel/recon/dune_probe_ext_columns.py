import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent)); import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
T = {"hook": "spl_token_2022_solana.spl_token_2022_call_transferhookextension", "permdel": "spl_token_2022_solana.spl_token_2022_call_initializepermanentdelegate",
     "nontransfer": "spl_token_2022_solana.spl_token_2022_call_initializenontransferablemint", "confidential": "spl_token_2022_solana.spl_token_2022_call_confidentialtransferextension"}
res = {}
for n, t in T.items():
    e = rt.dune("POST", "/sql/execute", KEY, {"sql": f"SELECT * FROM {t} WHERE call_block_date BETWEEN DATE '2026-08-25' AND DATE '2026-09-01' LIMIT 1", "performance": "medium"}).get("execution_id")
    t0 = time.time()
    while time.time() - t0 < 600:
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"): break
        time.sleep(4)
    rows = (rt.dune("GET", f"/execution/{e}/results", KEY).get("result") or {}).get("rows")
    cols = [c for c in (rows[0].keys() if rows else []) if not c.startswith("call_") or c == "call_block_slot"]
    res[n] = {"table": t, "columns": list(rows[0].keys()) if rows else None, "credits": st.get("execution_cost_credits")}
    print(f"{n:12} credits={st.get('execution_cost_credits')}  " + (", ".join(cols) if rows else "NO ROWS in window"))
(OUT / "dune_probe_ext_columns.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
