"""Burn-down item 3b: materialize ONE DAY of per-swap rows for stratum P and measure it.

    python barrel/recon/burn_3b.py <step>     steps: validate | create | measure | cleanup

The frozen 20 + 3 per-swap schema (DERIVED_TABLE_SCHEMA.md), one day (2025-06-10, inside the
calibration slice, not DEGRADED). Per RULINGS_2026-09-30 this is "a fact about Plus, not a
design option": Analyst's 1 GB cannot hold per-swap slices. Budget <= 100 credits.

Per-swap rows carry trader wallets. They are NEVER written to the repo: the export sample is
fetched to measure its cost, checked against the owner-wallet list, counted, and discarded.
"""
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import burn_3a as a           # noqa: E402  poll(), used()
import dune_roundtrip as rt   # noqa: E402
import dune_usage             # noqa: E402
import keys                   # noqa: E402
import owner_wallets          # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE = ROOT / "recon" / "out" / "burn_3b.json"
SQLF = ROOT / "recon" / "sql" / "trades_day_p.sql"
VIEW = "result_tr_day"
DAY = "2025-06-10"
WSOL = "So11111111111111111111111111111111111111112"
BUILD_CAP = 60.0
EXPORT_ROWS = 5000

ROWS_SQL = f"""-- Per-swap rows, stratum P, one day ({DAY}). Frozen 20 + 3 schema.
WITH pools AS (
  SELECT pool, arbitrary(base_mint) AS mint
  FROM pumpdotfun_solana.pump_amm_evt_createpoolevent
  WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '{DAY}' AND quote_mint = '{WSOL}'
    AND base_mint IN (SELECT mint FROM pumpdotfun_solana.pump_evt_completeevent
                      WHERE evt_block_date BETWEEN DATE '2025-03-20' AND DATE '{DAY}')
  GROUP BY 1),
s AS (
  SELECT b.evt_tx_id AS signature, b.evt_block_slot AS slot, b.evt_block_time AS block_timestamp, b.evt_tx_index AS tx_index,
         b.pool, p.mint, 'buy' AS side, b.user AS wallet,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.quote_amount_in ELSE b.user_quote_amount_in END AS gross_quote,
         CASE WHEN b.ix_name = 'buy_exact_quote_in' THEN b.user_quote_amount_in ELSE b.quote_amount_in END AS net_quote,
         b.lp_fee, b.protocol_fee, b.coin_creator_fee, b.base_amount_out AS base_amount
  FROM pumpdotfun_solana.pump_amm_evt_buyevent b JOIN pools p ON p.pool = b.pool
  WHERE b.evt_block_date = DATE '{DAY}'
  UNION ALL
  SELECT e.evt_tx_id, e.evt_block_slot, e.evt_block_time, e.evt_tx_index, e.pool, p.mint, 'sell', e.user,
         e.quote_amount_out, e.user_quote_amount_out, e.lp_fee, e.protocol_fee, e.coin_creator_fee, e.base_amount_in
  FROM pumpdotfun_solana.pump_amm_evt_sellevent e JOIN pools p ON p.pool = e.pool
  WHERE e.evt_block_date = DATE '{DAY}')
SELECT signature, slot, block_timestamp, tx_index,
       'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA' AS program, pool, mint, side, wallet,
       gross_quote, net_quote, lp_fee, protocol_fee, coin_creator_fee,
       CASE WHEN block_timestamp < TIMESTAMP '2026-07-21 00:00:00' THEN 'pre_boost' ELSE 'post_boost' END AS era,
       'P' AS stratum, '{WSOL}' AS quote_mint,
       CAST(gross_quote AS double) - CAST(net_quote AS double) AS trader_cost,
       CAST(gross_quote AS double) - CAST(net_quote AS double)
         - CAST(lp_fee AS double) - CAST(protocol_fee AS double) - COALESCE(CAST(coin_creator_fee AS double), 0) AS residual,
       abs(CAST(gross_quote AS double) - CAST(net_quote AS double)
         - CAST(lp_fee AS double) - CAST(protocol_fee AS double) - COALESCE(CAST(coin_creator_fee AS double), 0)) <= 1 AS cost_identity_ok,
       base_amount,
       CAST(block_timestamp AS date) BETWEEN DATE '2025-08-05' AND DATE '2025-08-11' AS degraded,
       CAST(NULL AS boolean) AS birth_is_proxy
FROM s"""


def load():
    return json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}


def save(s):
    STATE.write_text(json.dumps(s, indent=1, default=str), encoding="utf-8")


def step_validate(s):
    ro = keys.dune_key()
    SQLF.write_text(ROWS_SQL, encoding="utf-8")
    q = f"SELECT count(*) AS n, count(DISTINCT mint) AS mints, count(DISTINCT wallet) AS wallets, count_if(cost_identity_ok) AS identity_ok FROM (\n{ROWS_SQL}\n)"
    e = rt.dune("POST", "/sql/execute", ro, {"sql": q, "performance": "medium"})
    if "execution_id" not in e:
        raise SystemExit(f"refused: {e}")
    st = a.poll(e["execution_id"], ro, 15.0)
    r = rt.dune("GET", f"/execution/{e['execution_id']}/results", ro)
    s["validate"] = {"state": st.get("state"), "credits": st.get("execution_cost_credits"),
                     "rows": (r.get("result") or {}).get("rows"), "error": r.get("error")}
    save(s)
    print(json.dumps(s["validate"], indent=1))


def step_create(s):
    if "create" in s:
        raise SystemExit("create already recorded")
    rw = keys.dune_rw_key()
    before = a.used()
    q = rt.dune("POST", "/query", rw, {"name": "tr_day", "query_sql": ROWS_SQL, "is_private": True})
    if "query_id" not in q:
        raise SystemExit(f"saved-query creation failed: {q}")
    s["query_id"] = q["query_id"]
    save(s)
    mv = rt.dune("POST", "/materialized-views", rw, {"query_id": q["query_id"], "name": VIEW, "cron_expression": a.CRON,
                                                    "expires_at": a.EXPIRES, "is_private": True, "performance": "medium"})
    s["mv_create_response"] = mv
    save(s)
    eid = mv.get("execution_id")
    if not eid:
        raise SystemExit(f"materialized-view creation failed: {mv}")
    st = a.poll(eid, keys.dune_key(), BUILD_CAP)
    s["create"] = {"name": mv.get("name"), "execution_id": eid, "state": st.get("state"), "credits": st.get("execution_cost_credits"),
                   "cancelled_at": st.get("_cancelled_at_credits"), "usage_before": before, "usage_after": a.used()}
    save(s)
    print(json.dumps(s["create"], indent=1))


def step_measure(s):
    if "measure" in s:
        raise SystemExit("measure already recorded")
    name = s["create"]["name"]
    ro = keys.dune_key()
    out = {"view": rt.dune("GET", f"/materialized-views/{name}", ro), "usage_bytes": dune_usage.usage().get("bytes_used")}
    # full-table read: an aggregate touching every column (SELECT * of ~20M rows would exceed result limits)
    read_sql = (f"SELECT count(*) AS n, count(DISTINCT signature) AS sigs, max(slot) AS s, max(tx_index) AS ti, count(DISTINCT pool) AS pools, "
                f"count(DISTINCT mint) AS mints, count(DISTINCT wallet) AS wallets, count_if(side = 'buy') AS buys, "
                f"sum(CAST(gross_quote AS double)) AS g, sum(CAST(net_quote AS double)) AS nq, sum(CAST(lp_fee AS double)) AS lp, "
                f"sum(CAST(protocol_fee AS double)) AS pf, sum(CAST(coin_creator_fee AS double)) AS cf, sum(trader_cost) AS tc, "
                f"sum(residual) AS rs, count_if(cost_identity_ok) AS ok, sum(CAST(base_amount AS double)) AS ba, "
                f"count_if(degraded) AS dg, count(birth_is_proxy) AS bp, max(block_timestamp) AS t, max(era) AS era, "
                f"max(stratum) AS st, max(quote_mint) AS qm, max(program) AS pg FROM {name}")
    e = rt.dune("POST", "/sql/execute", ro, {"sql": read_sql, "performance": "medium"})
    st = a.poll(e["execution_id"], ro, 20.0)
    r = rt.dune("GET", f"/execution/{e['execution_id']}/results", ro)
    out["read"] = {"state": st.get("state"), "credits": st.get("execution_cost_credits"), "rows": (r.get("result") or {}).get("rows")}
    # export sample: fetched, checked, counted, DISCARDED (per-swap rows never reach the repo)
    x = rt.dune("POST", "/sql/execute", ro, {"sql": f"SELECT * FROM {name} LIMIT {EXPORT_ROWS}", "performance": "medium"})
    sx = a.poll(x["execution_id"], ro, 10.0)
    time.sleep(20)
    u0 = a.used()
    res = rt.dune("GET", f"/execution/{x['execution_id']}/results?limit={EXPORT_ROWS}", ro)
    rows = (res.get("result") or {}).get("rows") or []
    owner_hits = 0
    try:
        owner_wallets.assert_no_owner_rows(rows, "wallet")
    except AssertionError:
        owner_hits = 1
    nbytes = len(json.dumps(rows, separators=(",", ":")))
    n, ncols = len(rows), (len(rows[0]) if rows else 0)
    del rows
    time.sleep(90)
    u1 = a.used()
    out["export"] = {"select_credits": sx.get("execution_cost_credits"), "rows_fetched": n, "columns": ncols,
                     "datapoints_fetched": n * ncols, "json_bytes": nbytes, "usage_before": u0, "usage_after": u1,
                     "credits_delta": round(u1 - u0, 4), "owner_wallet_in_sample": bool(owner_hits), "rows_saved_to_repo": 0}
    s["measure"] = out
    save(s)
    print(json.dumps({"table_size_bytes": out["view"].get("table_size_bytes"), "read": out["read"], "export": out["export"]}, indent=1))


def step_cleanup(s):
    rw = keys.dune_rw_key()
    name = s["create"]["name"]
    out = {"delete_view": rt.dune("DELETE", f"/materialized-views/{name}", rw),
           "archive_query": rt.dune("POST", f"/query/{s['query_id']}/archive", rw, {})}
    time.sleep(5)
    out["view_after"] = rt.dune("GET", f"/materialized-views/{name}", keys.dune_key())
    out["usage_bytes_after"] = dune_usage.usage().get("bytes_used")
    s["cleanup"] = out
    save(s)
    print(json.dumps(out, indent=1)[:800])


if __name__ == "__main__":
    step = sys.argv[1]
    st_ = load()
    {"validate": step_validate, "create": step_create, "measure": step_measure, "cleanup": step_cleanup}[step](st_)
