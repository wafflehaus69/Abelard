"""BARREL — Dune metadata pass 2: decoded columns via information_schema, and
the 09-21 transfer specimens re-run now that the curated table has reached 09-21.

SHOW COLUMNS returned nothing for the decoded tables (and worked for the curated
one). information_schema.tables listed the decoded tables in run 1, so its
columns view is the next candidate. Long poll, state printed, NOT RUN never
reported as empty.

Quarantine: pump_evt_createevent almost certainly carries name/symbol/uri.
Column NAMES are read here; those columns are never selected (§1, v1.1 A7).
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt            # noqa: E402
import dune_roundtrip_followup as fu   # noqa: E402

OUT = pathlib.Path(__file__).with_name("out")
KEY = rt.api_key()
FOLLOW = json.loads((OUT / "dune_roundtrip_followup.json").read_text(encoding="utf-8"))
TABLES = ["pump_amm_evt_buyevent", "pump_amm_evt_sellevent",
          "pump_evt_completepumpammmigrationevent", "pump_evt_createevent", "pump_evt_tradeevent"]
SPEC_0921 = {n: s for n, s in FOLLOW["specimens"].items() if n.endswith("2026-09-21")}

QUERIES = {
    "cols": f"""
        SELECT table_name, column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'pumpdotfun_solana' AND table_name IN ({rt.sql_list(TABLES)})
        ORDER BY table_name, ordinal_position""",
    "transfers_0921": f"""
        SELECT tx_id, token_mint_address AS mint, CAST(amount AS varchar) AS amount
        FROM tokens_solana.transfers
        WHERE block_date = DATE '2026-09-21' AND tx_id IN ({rt.sql_list(SPEC_0921.values())})""",
}


def main() -> None:
    print(f"Dune metadata pass 2 — {dt.datetime.now(dt.UTC).isoformat(timespec='seconds')}")
    ids = {n: rt.dune("POST", "/sql/execute", KEY, {"sql": q, "performance": "medium"}).get("execution_id")
           for n, q in QUERIES.items()}
    (OUT / "dune_meta2_ids.json").write_text(json.dumps(ids, indent=1), encoding="utf-8")
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
        results[n] = {"state": "NOT RUN (unfinished after 15 min)", "credits": None, "rows": None, "error": None}

    r = results["cols"]
    print(f"\n== decoded columns ==  state={r['state']}  credits={r['credits']}  error={r['error']}")
    by_table: dict[str, list[str]] = {}
    for row in r["rows"] or []:
        by_table.setdefault(row["table_name"], []).append(f"{row['column_name']}:{row['data_type']}")
    for t in TABLES:
        cs = by_table.get(t)
        print(f"   {t}: " + (", ".join(cs) if cs else "(no rows)"))

    r = results["transfers_0921"]
    print(f"\n== transfers, 09-21 specimens, re-run ==  state={r['state']}  credits={r['credits']}  error={r['error']}")
    ok = True
    for name, sig in SPEC_0921.items():
        chain = set(fu.chain_transfers(sig))
        dune_rows = {(x["mint"], int(x["amount"].split(".")[0])) for x in (r["rows"] or []) if x["tx_id"] == sig}
        missing, extra = chain - dune_rows, dune_rows - chain
        ok &= not missing
        print(f"   {name:20} chain {len(chain)}  dune {len(dune_rows)}  missing {len(missing)}  extra {len(extra)}  -> {'PASS' if not missing else 'FAIL'}")
        time.sleep(1)
    total = sum(float(x["credits"] or 0) for x in results.values())
    print(f"\ncredits this pass: {total:.2f}")
    (OUT / "dune_meta2.json").write_text(json.dumps({"ids": ids, "columns": by_table, "results": results,
                                                    "transfers_0921_pass": ok, "credits": total}, indent=1, default=str),
                                         encoding="utf-8")


if __name__ == "__main__":
    main()
