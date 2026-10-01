"""Burn-down item 3a: materialize one week of pt_features (tier A) and take the measurements.

    python barrel/recon/burn_3a.py <step>     steps: create | measure | refresh | cleanup | status

Each step saves its result to recon/out/burn_3a.json before the next begins, so a
failure loses nothing and no step is ever repeated by accident (re-running a step
that already has a result refuses).

Controls (TRIAL_BURNDOWN_ORDERS + RULINGS_2026-09-30):
  * weekly cron with expires_at BEFORE its first firing, so the view never refreshes
    on a schedule; the one refresh measured here is manual;
  * in-flight cancel if the build passes BUILD_CAP credits;
  * the Read/Write key is used only for create / delete; reads use the read-only key;
  * export is measured on EXPORT_ROWS rows and extrapolated (1 credit / 1,000 datapoints,
    linear), because fetching the whole week would cost ~130 credits by itself;
  * the saved query holds public column names only and no owner wallets.
"""
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt   # noqa: E402
import dune_usage             # noqa: E402
import keys                   # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE = ROOT / "recon" / "out" / "burn_3a.json"
SQL = (ROOT / "recon" / "sql" / "pt_features_a_week.sql")
VIEW = "result_pt_features_w"
CRON = "0 0 * * 0"                       # Sundays 00:00 UTC; first firing would be 2026-10-04
EXPIRES = "2026-10-03T12:00:00Z"         # before that firing: no scheduled refresh ever runs
BUILD_CAP = 110.0
EXPORT_ROWS = 100


def load() -> dict:
    return json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}


def save(s: dict) -> None:
    STATE.write_text(json.dumps(s, indent=1, default=str), encoding="utf-8")


def used() -> float:
    u = dune_usage.usage()
    if "credits_used" not in u or u["credits_used"] is None:
        raise SystemExit(f"usage unavailable: {u}")
    return float(u["credits_used"])


def poll(eid: str, key: str, cap: float | None) -> dict:
    t0 = time.time()
    st = {}
    while time.time() - t0 < 1500:
        st = rt.dune("GET", f"/execution/{eid}/status", key)
        spent = float(st.get("execution_cost_credits") or 0)
        if cap is not None and not st.get("is_execution_finished") and spent > cap:
            rt.dune("POST", f"/execution/{eid}/cancel", key, {})
            st["_cancelled_at_credits"] = spent
            time.sleep(4)
            st.update(rt.dune("GET", f"/execution/{eid}/status", key))
            return st
        if st.get("is_execution_finished"):
            return st
        time.sleep(4)
    st["_poll_timeout"] = True
    return st


def step_create(s: dict) -> None:
    if "create" in s:
        raise SystemExit("create already recorded; refusing to create twice")
    rw = keys.dune_rw_key()
    before = used()
    q = rt.dune("POST", "/query", rw, {"name": "pt_features_w", "query_sql": SQL.read_text(encoding="utf-8"), "is_private": True})
    if "query_id" not in q:
        s["create_error"] = {"stage": "query", "response": q}
        save(s)
        raise SystemExit(f"saved-query creation failed: {q}")
    s["query_id"] = q["query_id"]
    save(s)
    mv = rt.dune("POST", "/materialized-views", rw, {
        "query_id": q["query_id"], "name": VIEW, "cron_expression": CRON,
        "expires_at": EXPIRES, "is_private": True, "performance": "medium"})
    s["mv_create_response"] = mv
    save(s)
    eid = mv.get("execution_id")
    if not eid:
        raise SystemExit(f"materialized-view creation failed: {mv}")
    st = poll(eid, keys.dune_key(), BUILD_CAP)
    s["create"] = {"name": mv.get("name"), "execution_id": eid, "state": st.get("state"),
                   "credits": st.get("execution_cost_credits"), "cancelled_at": st.get("_cancelled_at_credits"),
                   "usage_before": before, "usage_after": used(), "status": st}
    save(s)
    print(json.dumps({k: s["create"][k] for k in ("name", "state", "credits", "cancelled_at", "usage_before", "usage_after")}, indent=1))


def step_measure(s: dict) -> None:
    if "measure" in s:
        raise SystemExit("measure already recorded")
    name = s["create"]["name"]
    ro = keys.dune_key()
    out = {}
    out["view"] = rt.dune("GET", f"/materialized-views/{name}", ro)
    out["usage_bytes"] = dune_usage.usage().get("bytes_used")
    # (iii) one full-table read: every column, every row, executed but not fetched
    e = rt.dune("POST", "/sql/execute", ro, {"sql": f"SELECT * FROM {name}", "performance": "medium"})
    st = poll(e["execution_id"], ro, 30.0)
    out["read"] = {"execution_id": e["execution_id"], "state": st.get("state"), "credits": st.get("execution_cost_credits"),
                   "rows": (st.get("result_metadata") or {}).get("total_row_count"),
                   "datapoints": (st.get("result_metadata") or {}).get("datapoint_count"),
                   "result_bytes": (st.get("result_metadata") or {}).get("total_result_set_bytes")}
    # (iv) export: fetch EXPORT_ROWS rows, measure the usage delta
    u0 = used()
    res = rt.dune("GET", f"/execution/{e['execution_id']}/results?limit={EXPORT_ROWS}", ro)
    rows = (res.get("result") or {}).get("rows") or []
    time.sleep(25)
    u1 = used()
    out["export"] = {"rows_fetched": len(rows), "columns": len(rows[0]) if rows else 0,
                     "datapoints_fetched": len(rows) * (len(rows[0]) if rows else 0),
                     "usage_before": u0, "usage_after": u1, "credits_delta": round(u1 - u0, 4)}
    # row count and null audit, aggregate only
    c = rt.dune("POST", "/sql/execute", ro, {"sql": f"SELECT count(*) AS n, count(px_g240) AS has_px_g240, count(mdd_24h_g15) AS has_mdd24, count(create_time) AS has_create FROM {name}", "performance": "medium"})
    stc = poll(c["execution_id"], ro, 10.0)
    rc = rt.dune("GET", f"/execution/{c['execution_id']}/results", ro)
    out["counts"] = {"credits": stc.get("execution_cost_credits"), "rows": (rc.get("result") or {}).get("rows")}
    s["measure"] = out
    save(s)
    print(json.dumps({"table_size_bytes": out["view"].get("table_size_bytes"), "usage_bytes": out["usage_bytes"],
                      "read": out["read"], "export": out["export"], "counts": out["counts"]}, indent=1))


def step_refresh(s: dict) -> None:
    if "refresh" in s:
        raise SystemExit("refresh already recorded")
    name = s["create"]["name"]
    ro = keys.dune_key()
    before = used()
    r = rt.dune("POST", f"/materialized-views/{name}/refresh", ro, {"performance": "medium"})
    eid = r.get("execution_id")
    if not eid:
        s["refresh_error"] = r
        save(s)
        raise SystemExit(f"refresh failed: {r}")
    st = poll(eid, ro, BUILD_CAP)
    s["refresh"] = {"execution_id": eid, "state": st.get("state"), "credits": st.get("execution_cost_credits"),
                    "cancelled_at": st.get("_cancelled_at_credits"), "usage_before": before, "usage_after": used()}
    save(s)
    print(json.dumps(s["refresh"], indent=1))


def step_cleanup(s: dict) -> None:
    rw = keys.dune_rw_key()
    name = s["create"]["name"]
    out = {"delete_view": rt.dune("DELETE", f"/materialized-views/{name}", rw),
           "archive_query": rt.dune("POST", f"/query/{s['query_id']}/archive", rw, {})}
    time.sleep(5)
    out["view_after"] = rt.dune("GET", f"/materialized-views/{name}", keys.dune_key())
    out["usage_bytes_after"] = dune_usage.usage().get("bytes_used")
    s["cleanup"] = out
    save(s)
    print(json.dumps(out, indent=1)[:1200])


def main() -> None:
    step = sys.argv[1] if len(sys.argv) > 1 else "status"
    s = load()
    if step == "status":
        print(json.dumps({k: (v if not isinstance(v, dict) else {kk: v[kk] for kk in list(v)[:6]}) for k, v in s.items()}, indent=1, default=str)[:2500])
        return
    {"create": step_create, "measure": step_measure, "refresh": step_refresh, "cleanup": step_cleanup}[step](s)


if __name__ == "__main__":
    main()
