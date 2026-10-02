"""BARREL M0 — Dune fitness test by known-transaction round-trip ([E34]).

Orders (Architect, 2026-09-21): before a dollar is spent, run on Dune's FREE tier
the exact round-trip that disqualified BigQuery, and report pass/fail per item
plus the credits consumed. Items:

  1. Routed swaps  — PumpSwap calls made INSIDE an aggregator are present, not
                     just top-level PumpSwap instructions.
  2. Swap events   — PumpSwap's `Program data:` events are present in the logs
                     (and, if Dune decodes them, in a decoded table).
  3. Inner transfers — the token movements inside a swap are present, which
                     holder reconstruction (S6/S7) depends on.

Ground truth is the chain's own copy, fetched live over public RPC — never
assumed, never copied from a previous run. A Dune count that differs from the
chain's is a FAIL for that specimen and item, whatever the direction.

    python barrel/recon/dune_roundtrip.py

Requires DUNE_API_KEY (Read scope) in barrel/.env or the environment. The key is
never printed or logged. Every query is filtered by block_date to the
specimens' days only, to keep credit use minimal. Nothing is written to Dune.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import decode_pumpswap_events as m  # noqa: E402 — rpc, base58, forgery-safe event extraction

DUNE = "https://api.dune.com/api/v1"
PUMPSWAP = m.PUMPSWAP
TOKEN_PROGRAMS = {"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"}
TRANSFER_TYPES = {"transfer", "transferChecked"}

# Specimens chosen to cross each boundary a source might flatten (E34 corollary).
SPECIMENS = {
    "direct_2026-09-21": "SXW7mNsyArE8TK4GNuGVD4xfhV7YGi2MY5o1YkE9p5bW8wpivpxad7jdg1suyqjw8GbKydsiiM8gwmtfdxLewJw",
    "routed_2026-09-21": "BsVZee9YMzyBXvLDPSxbAY3vT86L9aN5p48Eywiw7SjPUqZyoiLdYTtfTg3q39qNuin3JqvyZodRudmmxFMDwyb",
    "direct_2026-09-01": "29crGCun9VjKjtweu6NQEXBQM4Xkog9if9orazySrXqm8AcgB8MozzgJTUnQo22KRNz6Mx7NKhugNNkS5U9dfqnb",
}


# ------------------------------------------------------------------ credentials

def api_key() -> str:
    import keys
    return keys.dune_key()


# ------------------------------------------------------------------ chain truth

def chain_truth(sig: str) -> dict:
    tx = m.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=6)
    outer = tx["transaction"]["message"]["instructions"]
    inner = [i for grp in (tx["meta"].get("innerInstructions") or []) for i in grp["instructions"]]
    def is_transfer(ix):
        p = ix.get("parsed")
        return ix.get("programId") in TOKEN_PROGRAMS and isinstance(p, dict) and p.get("type") in TRANSFER_TYPES
    layouts = m.load_event_layouts()
    swap_events = [p for p in m.event_payloads(tx) if p[:8] in layouts]
    return {
        "block_date": dt.datetime.fromtimestamp(tx["blockTime"], dt.UTC).date().isoformat(),
        "slot": tx["slot"],
        "version": tx.get("version"),
        "pumpswap_outer": sum(i.get("programId") == PUMPSWAP for i in outer),
        "pumpswap_inner": sum(i.get("programId") == PUMPSWAP for i in inner),
        "swap_events": len(swap_events),
        "token_transfers_outer": sum(is_transfer(i) for i in outer),
        "token_transfers_inner": sum(is_transfer(i) for i in inner),
    }


def find_v1_specimen() -> tuple[str, str] | None:
    """Newest transaction version is a boundary (it broke our own probes). Find one live."""
    # The public RPC rate-limits bursts; a 429 here is a pacing problem, not a
    # missing specimen, so back off and keep looking rather than abort the test.
    for s in m.rpc("getSignaturesForAddress", [PUMPSWAP, {"limit": 120}]):
        if s.get("err"):
            continue
        try:
            tx = m.rpc("getTransaction", [s["signature"], {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}], tries=3)
        except m.RpcError:
            time.sleep(5)
            continue
        if tx.get("version") == 1:
            return "v1_live", s["signature"]
        time.sleep(1.0)
    return None


# ------------------------------------------------------------------------ dune

def dune(method: str, path: str, key: str, body: dict | None = None) -> dict:
    # Chokepoint (MR-14 ruling 5, review 2026-10-01): every query text that goes to Dune passes here,
    # ad hoc, saved query or view definition alike. Checked before any network call.
    for _k in ("sql", "query_sql"):
        if body and isinstance(body.get(_k), str):
            import re as _re
            import verdict_constants as _vc
            _vc.refuse(body[_k], what=f"{method} {path}")
            _t = _vc.strip(body[_k]).strip()
            if _re.match(r"(?is)^SELECT\s+\*\s+FROM\b", _t) and not _re.search(r"(?i)\bLIMIT\s+0\s*$", _t):
                raise SystemExit("REFUSED: a bare SELECT * fetches whatever the table carries, quarantined metadata "
                                 "and wallets included. Column probes use LIMIT 0 and read metadata.column_names.")
    req = urllib.request.Request(
        f"{DUNE}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-Dune-Api-Key": key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        # The response body is reported (it explains plan/credit refusals); the key is not.
        return {"_http_error": e.code, "_body": e.read().decode("utf-8", "replace")[:600]}


def run_sql(label: str, sql: str, key: str) -> dict:
    start = dune("POST", "/sql/execute", key, {"sql": sql, "performance": "medium"})
    if "execution_id" not in start:
        return {"label": label, "error": start}
    eid = start["execution_id"]
    for _ in range(120):
        st = dune("GET", f"/execution/{eid}/status", key)
        if st.get("state") in ("QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXPIRED"):
            break
        time.sleep(2)
    res = dune("GET", f"/execution/{eid}/results", key)
    meta = {k: v for k, v in st.items() if k not in ("result",)}   # credits/cost fields, whatever Dune names them
    return {"label": label, "status": meta, "rows": (res.get("result") or {}).get("rows"), "error": res.get("error")}


def sql_list(xs) -> str:
    return ", ".join(f"'{x}'" for x in xs)


def main() -> None:
    key = api_key()
    specimens = dict(SPECIMENS)
    v1 = find_v1_specimen()
    if v1:
        specimens[v1[0]] = v1[1]
    else:
        print("NOTE: no version-1 transaction found in the last 200 PumpSwap sigs; that boundary is UNTESTED.")

    truth = {}
    for name, sig in specimens.items():
        for attempt in range(4):
            try:
                truth[name] = chain_truth(sig)
                break
            except m.RpcError:
                time.sleep(8 * (attempt + 1))
        else:
            raise SystemExit(f"chain truth for {name} could not be fetched (RPC rate limit); nothing was run on Dune.")
        time.sleep(1.5)
    sigs = list(specimens.values())
    dates = sorted({t["block_date"] for t in truth.values()})
    date_filter = f"block_date IN ({', '.join(f'DATE {d!r}' for d in dates)})"

    queries = {
        "instructions": f"""
            SELECT tx_id,
                   COUNT_IF(executing_account = '{PUMPSWAP}' AND NOT is_inner) AS pumpswap_outer,
                   COUNT_IF(executing_account = '{PUMPSWAP}' AND is_inner)     AS pumpswap_inner
            FROM solana.instruction_calls
            WHERE {date_filter} AND tx_id IN ({sql_list(sigs)})
            GROUP BY tx_id""",
        "logs": f"""
            SELECT id AS tx_id,
                   cardinality(log_messages) AS n_logs,
                   cardinality(filter(log_messages, x -> x LIKE 'Program data:%')) AS n_program_data
            FROM solana.transactions
            WHERE {date_filter} AND id IN ({sql_list(sigs)})""",
        "transfers": f"""
            SELECT tx_id, COUNT(*) AS n_transfers
            FROM tokens_solana.transfers
            WHERE {date_filter} AND tx_id IN ({sql_list(sigs)})
            GROUP BY tx_id""",
        "decoded_tables": """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema LIKE '%pump%' AND (table_name LIKE '%event%' OR table_name LIKE '%evt%')""",
    }
    results = {name: run_sql(name, sql, key) for name, sql in queries.items()}

    def by_tx(name):
        return {r["tx_id"]: r for r in (results[name].get("rows") or [])}

    ins, logs, xfer = by_tx("instructions"), by_tx("logs"), by_tx("transfers")
    print(f"\nBARREL — Dune round-trip ({dt.datetime.now(dt.UTC).isoformat(timespec='seconds')})\n")
    verdict = {"routed_swaps": True, "events_in_logs": True, "inner_transfers": True}
    for name, sig in specimens.items():
        t = truth[name]
        d_ins, d_log, d_x = ins.get(sig, {}), logs.get(sig), xfer.get(sig, {})
        ok_ins = (d_ins.get("pumpswap_outer") == t["pumpswap_outer"] and d_ins.get("pumpswap_inner") == t["pumpswap_inner"])
        # Chain 'Program data' lines include non-PumpSwap programs' events, so compare >= PumpSwap events.
        ok_log = d_log is not None and (d_log.get("n_program_data") or 0) >= t["swap_events"] > 0
        want_x = t["token_transfers_outer"] + t["token_transfers_inner"]
        ok_x = d_x.get("n_transfers") == want_x
        verdict["routed_swaps"] &= ok_ins
        verdict["events_in_logs"] &= ok_log
        verdict["inner_transfers"] &= ok_x
        print(f"{name} (v{t['version']}, {t['block_date']})")
        print(f"  PumpSwap ix  chain outer/inner {t['pumpswap_outer']}/{t['pumpswap_inner']}  "
              f"dune {d_ins.get('pumpswap_outer')}/{d_ins.get('pumpswap_inner')}  -> {'PASS' if ok_ins else 'FAIL'}")
        print(f"  swap events  chain {t['swap_events']}  dune 'Program data' lines "
              f"{None if d_log is None else d_log.get('n_program_data')}  -> {'PASS' if ok_log else 'FAIL'}")
        print(f"  transfers    chain {want_x}  dune {d_x.get('n_transfers')}  -> {'PASS' if ok_x else 'FAIL'}")

    print("\nVERDICT per item:", {k: ("PASS" if v else "FAIL") for k, v in verdict.items()})
    print("\nDecoded PumpSwap event tables:", results["decoded_tables"].get("rows"))
    print("\nPer-query status and cost metadata (credits as Dune reports them):")
    for name, r in results.items():
        print(f"  {name}: error={r.get('error')}  status={json.dumps(r.get('status'))[:400]}")

    out = pathlib.Path(__file__).with_name("out") / "dune_roundtrip.json"
    out.write_text(json.dumps({"specimens": specimens, "truth": truth, "results": results,
                               "verdict": verdict}, indent=1, default=str), encoding="utf-8")
    print(f"\nwritten: {out}")


if __name__ == "__main__":
    main()
