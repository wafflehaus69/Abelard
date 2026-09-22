"""Learn a decoded table's columns by a one-row query, since the catalog has none.
Wrong column-name guesses fail at name resolution and cost 0 credits (observed).
buyevent only: no quarantined fields on it. Long poll; state printed."""
import json, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
KEY = rt.api_key(); OUT = pathlib.Path(__file__).with_name("out")
T = "pumpdotfun_solana.pump_amm_evt_buyevent"
CANDIDATES = [
    ("evt_block_date", f"SELECT * FROM {T} WHERE evt_block_date = DATE '2026-09-01' LIMIT 1"),
    ("evt_block_time", f"SELECT * FROM {T} WHERE evt_block_time >= TIMESTAMP '2026-09-01 00:00:00' AND evt_block_time < TIMESTAMP '2026-09-01 00:05:00' LIMIT 1"),
    ("block_time",     f"SELECT * FROM {T} WHERE block_time >= TIMESTAMP '2026-09-01 00:00:00' AND block_time < TIMESTAMP '2026-09-01 00:05:00' LIMIT 1"),
    ("bare_limit1",    f"SELECT * FROM {T} LIMIT 1"),
]
def run(sql):
    e = rt.dune("POST", "/sql/execute", KEY, {"sql": sql, "performance": "medium"}).get("execution_id")
    if not e: return {"state": "REFUSED"}
    t0 = time.time()
    while time.time() - t0 < 900:
        st = rt.dune("GET", f"/execution/{e}/status", KEY)
        if st.get("is_execution_finished"):
            res = rt.dune("GET", f"/execution/{e}/results", KEY)
            return {"state": st.get("state"), "credits": st.get("execution_cost_credits"),
                    "rows": (res.get("result") or {}).get("rows"), "error": res.get("error"),
                    "meta": (res.get("result") or {}).get("metadata")}
        time.sleep(4)
    return {"state": "NOT RUN (15 min)"}
out = {}
for name, sql in CANDIDATES:
    r = run(sql); out[name] = r
    err = (r.get("error") or {}).get("message", "") if isinstance(r.get("error"), dict) else r.get("error")
    print(f"{name:15} state={r.get('state')} credits={r.get('credits')} rows={len(r.get('rows') or [])} err={str(err)[:90]}")
    if r.get("rows"):
        cols = list(r["rows"][0].keys())
        print("   COLUMNS:", ", ".join(cols))
        break
(OUT / "dune_probe_columns.json").write_text(json.dumps(out, indent=1, default=str), encoding="utf-8")
