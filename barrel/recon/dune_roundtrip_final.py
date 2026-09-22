"""BARREL — Dune round-trip, final pass. Closes the two items the follow-up left open.

  * decoded events: the follow-up's query failed on a column name (block_date
    does not exist on the decoded tables). Read the real columns first, then
    look the specimens up. Also read the columns of the graduation and create
    event tables, which Gate 0 will need regardless.
  * transfers: the 3-week-old specimen passed row-for-row; today's specimens
    were missing. Hypothesis: the curated table lags the raw one by about a
    day. Test it by comparing each table's newest partition, and re-run the
    row comparison only on days the curated table has reached.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt                 # noqa: E402
import dune_roundtrip_followup as fu        # noqa: E402  (imports run its module-level PRIOR/KEY only)

OUT = pathlib.Path(__file__).with_name("out")
FOLLOW = json.loads((OUT / "dune_roundtrip_followup.json").read_text(encoding="utf-8"))
KEY = rt.api_key()
specimens, truth = FOLLOW["specimens"], FOLLOW["truth"]
sigs = list(specimens.values())

TABLES = ["pump_amm_evt_buyevent", "pump_amm_evt_sellevent",
          "pump_evt_completepumpammmigrationevent", "pump_evt_createevent", "pump_evt_tradeevent"]

# 1. Columns of the decoded tables (metadata; cheap).
cols = rt.run_sql("columns", f"""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'pumpdotfun_solana' AND table_name IN ({rt.sql_list(TABLES)})
    ORDER BY table_name, ordinal_position""", KEY)
by_table: dict[str, list[tuple[str, str]]] = {}
for r in cols.get("rows") or []:
    by_table.setdefault(r["table_name"], []).append((r["column_name"], r["data_type"]))
print("== 1. decoded table columns ==")
for t, cs in by_table.items():
    print(f"   {t} ({len(cs)} cols): " + ", ".join(c for c, _ in cs))
buy_cols = {c for c, _ in by_table.get("pump_amm_evt_buyevent", [])}
time_col = next((c for c in ("block_date", "block_time", "block_slot") if c in buy_cols), None)
print(f"   time column on buyevent: {time_col}")

# 2. Freshness: newest partition of the curated transfers table vs the raw instruction table.
fresh = rt.run_sql("freshness", """
    SELECT 'tokens_solana.transfers' AS t, MAX(block_date) AS newest FROM tokens_solana.transfers WHERE block_date >= DATE '2026-09-18'
    UNION ALL
    SELECT 'solana.instruction_calls', MAX(block_date) FROM solana.instruction_calls WHERE block_date >= DATE '2026-09-18'""", KEY)
print("\n== 2. freshness (newest partition present) ==")
newest = {}
for r in fresh.get("rows") or []:
    newest[r["t"]] = r["newest"]
    print(f"   {r['t']:28} {r['newest']}")
print(f"   error={fresh.get('error')}")

# 3. Decoded swap events for the specimens, using the real time column.
dates = sorted({t["block_date"] for t in truth.values()})
if time_col == "block_date":
    tf = f"block_date IN ({', '.join(f'DATE {d!r}' for d in dates)})"
elif time_col == "block_time":
    lo, hi = min(dates), (dt.date.fromisoformat(max(dates)) + dt.timedelta(days=1)).isoformat()
    tf = f"block_time >= TIMESTAMP '{lo} 00:00:00' AND block_time < TIMESTAMP '{hi} 00:00:00'"
else:
    tf = None
dec = {"rows": None, "error": "no usable time column; not run"}
if tf:
    dec = rt.run_sql("decoded_events", f"""
        SELECT 'buy'  AS side, tx_id FROM pumpdotfun_solana.pump_amm_evt_buyevent  WHERE {tf} AND tx_id IN ({rt.sql_list(sigs)})
        UNION ALL
        SELECT 'sell' AS side, tx_id FROM pumpdotfun_solana.pump_amm_evt_sellevent WHERE {tf} AND tx_id IN ({rt.sql_list(sigs)})""", KEY)
dec_by_tx: dict[str, int] = {}
for r in dec.get("rows") or []:
    dec_by_tx[r["tx_id"]] = dec_by_tx.get(r["tx_id"], 0) + 1
print("\n== 3. decoded swap events per specimen ==")
print(f"   credits={(dec.get('status') or {}).get('execution_cost_credits')}  error={dec.get('error')}")
ok_dec = True
for name, sig in specimens.items():
    want, got = truth[name]["swap_events"], dec_by_tx.get(sig, 0)
    ok_dec &= got == want
    print(f"   {name:20} chain {want}  dune decoded {got}  -> {'PASS' if got == want else 'FAIL'}")

# 4. Transfers, row-level, only on specimens whose day the curated table has reached.
reached = newest.get("tokens_solana.transfers")
eligible = {n: s for n, s in specimens.items() if reached and truth[n]["block_date"] <= str(reached)}
print(f"\n== 4. transfers row-level, specimens on days <= {reached} ==")
ok_xf = True
if eligible:
    xf = rt.run_sql("transfers_rows", f"""
        SELECT tx_id, token_mint_address AS mint, CAST(amount AS varchar) AS amount
        FROM tokens_solana.transfers
        WHERE block_date IN ({', '.join(f'DATE {truth[n]["block_date"]!r}' for n in eligible)})
          AND tx_id IN ({rt.sql_list(eligible.values())})""", KEY)
    print(f"   credits={(xf.get('status') or {}).get('execution_cost_credits')}  error={xf.get('error')}")
    for name, sig in eligible.items():
        chain = set(fu.chain_transfers(sig))
        dune_rows = {(r["mint"], int(r["amount"].split(".")[0])) for r in (xf.get("rows") or []) if r["tx_id"] == sig}
        missing, extra = chain - dune_rows, dune_rows - chain
        ok_xf &= not missing
        print(f"   {name:20} chain {len(chain)}  dune {len(dune_rows)}  missing {len(missing)}  extra {len(extra)}  -> {'PASS' if not missing else 'FAIL'}")
        if extra:
            print(f"      extra-in-dune sample: {sorted(extra)[:3]}")
        time.sleep(1)
    excluded = [n for n in specimens if n not in eligible]
    if excluded:
        print(f"   not testable yet (newer than the curated table's newest day): {excluded}")
else:
    print("   no specimen is on a day the curated table has reached; nothing run")

credits = FOLLOW["credits_total"] + sum(float((x.get("status") or {}).get("execution_cost_credits") or 0)
                                         for x in (cols, fresh, dec) + ((xf,) if eligible else ()))
print("\n== VERDICT ==")
print(f"   routed swaps in instruction_calls     : PASS (follow-up, 3/3 incl. 0/6 routed)")
print(f"   swap events in logs                   : PASS (first run, 3/3)")
print(f"   swap events decoded (pumpdotfun_solana): {'PASS' if ok_dec else 'FAIL'}")
print(f"   inner transfers row-level             : {'PASS' if ok_xf else 'FAIL'} on {len(eligible)} eligible specimen(s)")
print(f"   curated-table lag                     : transfers newest={reached}, instruction_calls newest={newest.get('solana.instruction_calls')}")
print(f"   total credits, all three runs         : {credits:.1f} of 2,500 free")
(OUT / "dune_roundtrip_final.json").write_text(json.dumps(
    {"columns": by_table, "freshness": newest, "decoded": dec, "specimens": specimens,
     "verdict": {"routed": True, "events_logs": True, "events_decoded": ok_dec, "transfers": ok_xf},
     "credits_total": credits}, indent=1, default=str), encoding="utf-8")
