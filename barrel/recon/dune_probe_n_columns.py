import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent)); import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
T = {"cp_init": "raydium_cp_solana.raydium_cp_swap_call_initialize", "clmm_created": "raydium_clmm_solana.amm_v3_evt_poolcreatedevent"}
F = ["call_block_date = DATE '2026-09-01'", "evt_block_date = DATE '2026-09-01'"]
res = {}
for n, t in T.items():
    for f in F:
        e = rt.dune("POST", "/sql/execute", KEY, {"sql": f"SELECT * FROM {t} WHERE {f} LIMIT 1", "performance": "medium"}).get("execution_id")
        t0 = time.time()
        while time.time() - t0 < 600:
            st = rt.dune("GET", f"/execution/{e}/status", KEY)
            if st.get("is_execution_finished"): break
            time.sleep(4)
        rows = (rt.dune("GET", f"/execution/{e}/results", KEY).get("result") or {}).get("rows")
        if rows:
            res[n] = {"table": t, "filter": f, "columns": list(rows[0].keys()), "credits": st.get("execution_cost_credits")}
            print(f"{n:13} {t}  credits={st.get('execution_cost_credits')}\n   " + ", ".join(rows[0].keys())); break
    else:
        res[n] = {"table": t, "error": "no filter resolved / no rows"}; print(f"{n}: unresolved")
(OUT / "dune_probe_n_columns.json").write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
