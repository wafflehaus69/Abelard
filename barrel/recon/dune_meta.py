"""BARREL — Dune metadata pass: decoded-table columns and curated-table freshness.

Replaces the failed step 1/2 of dune_roundtrip_final.py. That run polled for
4 minutes; free-tier queue time exceeded it; the harness then reported empty
results as if Dune had returned nothing. Two rules from that:

  * poll on `is_execution_finished`, for up to 15 minutes, and PRINT the state;
  * a result that never finished is reported as NOT RUN, never as empty/FAIL.

Execution IDs are saved to out/dune_meta.json so a later re-poll costs nothing.
Both queries are submitted first, then polled together.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt  # noqa: E402

OUT = pathlib.Path(__file__).with_name("out")
KEY = rt.api_key()

QUERIES = {
    "cols_buy":      "SHOW COLUMNS FROM pumpdotfun_solana.pump_amm_evt_buyevent",
    "cols_migrate":  "SHOW COLUMNS FROM pumpdotfun_solana.pump_evt_completepumpammmigrationevent",
    "cols_create":   "SHOW COLUMNS FROM pumpdotfun_solana.pump_evt_createevent",
    "cols_transfers": "SHOW COLUMNS FROM tokens_solana.transfers",
    "freshness": """
        SELECT 'tokens_solana.transfers' AS t, CAST(MAX(block_date) AS varchar) AS newest
        FROM tokens_solana.transfers WHERE block_date >= DATE '2026-09-18'
        UNION ALL
        SELECT 'solana.instruction_calls', CAST(MAX(block_date) AS varchar)
        FROM solana.instruction_calls WHERE block_date >= DATE '2026-09-18'""",
}


def submit(sql: str) -> dict:
    return rt.dune("POST", "/sql/execute", KEY, {"sql": sql, "performance": "medium"})


def main() -> None:
    print(f"Dune metadata pass — {dt.datetime.now(dt.UTC).isoformat(timespec='seconds')}")
    subs = {name: submit(sql) for name, sql in QUERIES.items()}
    ids = {n: s.get("execution_id") for n, s in subs.items()}
    for n, s in subs.items():
        if not ids[n]:
            print(f"   {n}: submission refused: {s}")
    (OUT / "dune_meta_ids.json").write_text(json.dumps(ids, indent=1), encoding="utf-8")

    pending = {n: e for n, e in ids.items() if e}
    results: dict[str, dict] = {}
    deadline = time.time() + 15 * 60
    while pending and time.time() < deadline:
        for n, e in list(pending.items()):
            st = rt.dune("GET", f"/execution/{e}/status", KEY)
            if st.get("is_execution_finished"):
                res = rt.dune("GET", f"/execution/{e}/results", KEY)
                results[n] = {"state": st.get("state"), "credits": st.get("execution_cost_credits"),
                              "rows": (res.get("result") or {}).get("rows"), "error": res.get("error")}
                del pending[n]
        if pending:
            time.sleep(5)
    for n in pending:
        results[n] = {"state": "NOT RUN (still queued/executing after 15 min)", "credits": None, "rows": None, "error": None}

    print()
    for n, r in results.items():
        print(f"== {n} ==  state={r['state']}  credits={r['credits']}  error={r['error']}")
        rows = r["rows"] or []
        if n.startswith("cols_"):
            print("   " + ", ".join(f"{row.get('Column')}:{row.get('Type')}" for row in rows) if rows else "   (no rows)")
        else:
            for row in rows:
                print(f"   {row}")
    total = sum(float(r["credits"] or 0) for r in results.values())
    print(f"\ncredits this pass: {total:.2f}")
    (OUT / "dune_meta.json").write_text(json.dumps({"ids": ids, "results": results, "credits": total},
                                                   indent=1, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
