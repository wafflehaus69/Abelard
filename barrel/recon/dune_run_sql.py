"""Run a repo .sql file on Dune with the credit meter, long poll, ledger append.
python barrel/recon/dune_run_sql.py recon/sql/x.sql --expect 5
Refuses if --expect exceeds what is spendable above the reserve (usage API; the ledger is the fallback).
--refetch <execution id> reads the rows of an execution this runner already ran for the same file: nothing is
executed, only the export is billed."""
import argparse, json, pathlib, re, sys, time, datetime as dt
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
import dune_usage
import owner_wallets
import verdict_constants
ROOT = pathlib.Path(__file__).resolve().parents[1]; OUT = ROOT / "recon" / "out"; LEDGER = ROOT / "docs" / "credits.md"
ALLOWANCE = 2500.0; RESERVE = 0.15
def consumed():
    m = re.search(r"Total consumed: ([\d.]+)", LEDGER.read_text(encoding="utf-8")); return float(m.group(1)) if m else 0.0
def cancel(eid, key):
    """Cancel and read the state back. rt.dune returns an HTTP error as a dict, so a cancel that was refused looks
    like one that worked; a query left running has no cap."""
    st = {}
    for i in range(5):
        r = rt.dune("POST", f"/execution/{eid}/cancel", key, {})
        time.sleep(3); st = rt.dune("GET", f"/execution/{eid}/status", key)
        if st.get("is_execution_finished") or "CANCEL" in str(st.get("state", "")).upper(): return st
        print(f"cancel of {eid} not confirmed (attempt {i + 1}): response {str(r)[:120]}, state {st.get('state')}", flush=True)
    print(f"CANCEL NOT CONFIRMED: execution {eid} may still be running. Cancel it by hand.", flush=True); return st
def main():
    ap = argparse.ArgumentParser(); ap.add_argument("sql"); ap.add_argument("--expect", type=float, required=True); ap.add_argument("--label", default=""); ap.add_argument("--confirm", action="store_true"); ap.add_argument("--proven", action="store_true"); ap.add_argument("--max-seconds", type=float, default=420.0, dest="max_seconds"); ap.add_argument("--quiet", action="store_true"); ap.add_argument("--export", action="store_true"); ap.add_argument("--funders-from", dest="funders_from", default=None); ap.add_argument("--no-rows", action="store_true", dest="no_rows"); ap.add_argument("--private-rows", action="store_true", dest="private_rows"); ap.add_argument("--refetch", default=None)
    a = ap.parse_args(); sql = pathlib.Path(a.sql).read_text(encoding="utf-8")   # path as given, relative to cwd
    # MR-14 ruling 5: a query carrying a registered verdict constant is never submitted. No override.
    verdict_constants.refuse(sql, what=a.sql)     # also enforced in dune_roundtrip.dune, the one request function
    # A query that says its rows name wallets is never written to the tracked tree, whatever flags were given.
    if "--private-rows" in sql and not (a.private_rows or a.export or a.no_rows):
        a.private_rows = True; print("rows name wallets (query header): forcing --private-rows")
    # Fan-out query (MR-15 B3): its senders are the funders in the chunk's exported aligned-set rows. The
    # addresses go into the text sent to Dune and nowhere else; the file in the repo keeps the placeholder.
    if "__FUNDERS__" in sql:
        if not a.funders_from: raise SystemExit("REFUSED: this query needs --funders-from <exported aligned-set rows>")
        own = set(owner_wallets.owner_wallets())
        frows = json.loads(pathlib.Path(a.funders_from).read_text(encoding="utf-8"))
        fset = sorted({v for r in frows for v in (r.get("funder"), r.get("member_own_funder")) if v and v not in own})
        if not fset: raise SystemExit("REFUSED: no funder in the rows given")
        if not all(re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", f) for f in fset): raise SystemExit("REFUSED: a funder value is not a base58 address")
        # the list must be the chunk's own: a fan-out file for 2026-08 takes rows whose file name carries 2026-08
        cm = re.match(r"(\d{4}-\d{2})_fan", pathlib.Path(a.sql).stem)
        if cm and cm.group(1) not in pathlib.Path(a.funders_from).name:
            raise SystemExit(f"REFUSED: {a.sql} is the fan-out query of chunk {cm.group(1)}; --funders-from does not name that chunk")
        sql = sql.replace("__FUNDERS__", ", ".join(f"'{f}'" for f in fset))
        import hashlib as _h
        # what was asked is recorded (count and hash, never the addresses): a funder absent from the result
        # is a measured zero only if it was in this list
        asked = {"funders_from": pathlib.Path(a.funders_from).name, "n_funders": len(fset),
                 "funders_sha256": _h.sha256("\n".join(fset).encode()).hexdigest()}
        print(f"funders substituted: {len(fset)} (query text {len(sql) / 1e6:.2f} MB)")
    # Owner-wallet exclusion (MR-3.4): SQL files write __NOT_OWNER(col)__; the addresses are read
    # from barrel/private/ at run time and reach only the query text sent to Dune, never the repo.
    for m_ in set(re.findall(r"__NOT_OWNER\(([A-Za-z0-9_.]+)\)__", sql)):
        sql = sql.replace(f"__NOT_OWNER({m_})__", owner_wallets.sql_not_owner(m_))
    # Authoritative balance from POST /v1/usage (no credits consumed); ledger is the fallback.
    u = dune_usage.usage()
    if u.get("credits_included") is not None:
        allowance, used, src = float(u["credits_included"]), float(u["credits_used"]), "api"
    else:
        allowance, used, src = ALLOWANCE, consumed(), "ledger"
    # Reserve: 15% of the period's ALLOWANCE, untouchable (launch runbook section 0). The 14-day trial keeps
    # its own ruled reserve of 180 (15% of what remained when the burn-down orders were issued).
    reserve = 180.0 if allowance <= 2500.0 else RESERVE * allowance
    usable = allowance - used - reserve
    if a.expect > usable:
        print(f"REFUSED: expect {a.expect} > usable {usable:.1f} ({src}: allowance {allowance:.0f}, used {used:.2f}, reserve {reserve:.0f})"); sys.exit(2)
    print(f"meter[{src}]: used {used:.2f} / {allowance:.0f}, usable {usable:.1f}, this run expects {a.expect}")
    # HARD CAP (added 2026-09-23 after a runaway: expect 8, billed 840). A query is cancelled the
    # moment Dune's in-flight execution_cost_credits exceeds max(3 x expect, 5). --expect above 25
    # needs --confirm, which is only given after the same SQL has run at one-partition scope.
    # CAP RULE (RULINGS_2026-10-01, order 1). Incident: on 2026-09-30 `a3_seta_day.sql` ran with
    # --expect 25, cap 3x = 75, and billed 85.1 for an empty result; the query finished without the
    # in-flight counter ever being seen above the cap. So:
    #   * a pattern NOT previously run at one-partition scope gets --expect as its HARD cap;
    #   * only --proven (the same pattern already measured at one-partition scope) gets 3x;
    #   * polling every 2 s; a wall-clock ceiling (--max-seconds) backs up the credit counter,
    #     because Dune's in-flight execution_cost_credits can lag the true cost.
    # Every in-flight sample is recorded, so how the counter behaves is evidence, not a guess.
    cap = max(3.0 * a.expect, 5.0) if a.proven else a.expect
    if a.expect > 25 and not a.confirm:
        print(f"REFUSED: --expect {a.expect} > 25 without --confirm (run the pattern at one-partition scope first)"); sys.exit(2)
    key = rt.api_key()
    if a.refetch:
        # --refetch <execution id>: read the rows of an execution that already ran and was already billed (a --no-rows
        # cost run, or a fetch that came back incomplete). Nothing is executed and nothing is cancelled. The id must be
        # one this runner submitted for this same file, so rows of one query are never written under another's name.
        stem = pathlib.Path(a.sql).stem
        prior = [r for r in (json.loads(f.read_text(encoding="utf-8")) for f in OUT.glob("run_*.json"))
                 if r.get("execution_id") == a.refetch and pathlib.Path(str(r.get("file"))).stem == stem]
        if not prior: raise SystemExit(f"REFUSED: no run record of {stem} carries execution {a.refetch}")
        eid = a.refetch; st = rt.dune("GET", f"/execution/{eid}/status", key)
        if st.get("state") != "QUERY_STATE_COMPLETED": raise SystemExit(f"REFUSED: execution {eid} is {st.get('state')}, not completed; nothing to fetch")
        a.no_rows = False; print(f"refetch: execution {eid}, billed {float(st.get('execution_cost_credits') or 0):.3f} when it ran; nothing is executed", flush=True)
    else:
        e = rt.dune("POST", "/sql/execute", key, {"sql": sql, "performance": "medium"})
        eid = e.get("execution_id")
        if not eid: print("submission refused:", e); sys.exit(1)
        # Printed at once: if anything below fails, the result can still be fetched or the query cancelled by id.
        print(f"submitted: execution {eid}", flush=True)
    t0 = time.time(); st = st if a.refetch else {}; killed = None; samples = []
    # The loop ends only on a finished execution, a cancellation or lost contact. (Until 2026-10-06 it also ended
    # after 900 s without cancelling, so --max-seconds above 900 would have left a query running with no cap.)
    while not a.refetch:
        try:
            st = rt.dune("GET", f"/execution/{eid}/status", key)
        except Exception as ex:   # the request function already retried for about a minute; keep watching
            lost = locals().get("lost", 0) + 1
            print(f"status poll failed ({type(ex).__name__}), attempt {lost}; execution {eid} is still being watched", flush=True)
            if lost >= 8:
                # an unwatched query has no cap: try to cancel; if it had finished, its rows are still there for --refetch
                try: cancel(eid, key)
                except Exception: print(f"LOST CONTACT and the cancel did not go through: execution {eid} may still be running", flush=True)
                st = {"state": "LOST CONTACT", "is_execution_finished": False}; break
            time.sleep(10); continue
        lost = 0
        spent = float(st.get("execution_cost_credits") or 0)
        el = time.time() - t0
        if not samples or samples[-1][1] != spent:
            samples.append((round(el, 1), spent))
        if not st.get("is_execution_finished") and (spent > cap or el > a.max_seconds):
            killed = f"credits {spent:.1f} > cap {cap:.1f}" if spent > cap else f"elapsed {el:.0f}s > {a.max_seconds}s"
            print(f"WATCHDOG: cancelling {eid}: {killed}", flush=True)
            st = cancel(eid, key); break
        if st.get("is_execution_finished"): break
        time.sleep(2)
    try:
        res = rt.dune("GET", f"/execution/{eid}/results", key) if st.get("is_execution_finished") and not a.no_rows else {}  # --no-rows: cost measurement only; export is billed per MB
    except Exception as ex:
        res = {"_http_error": f"fetch failed: {type(ex).__name__}"}
    cost = 0.0 if a.refetch else float(st.get("execution_cost_credits") or 0)   # a refetch was billed, and ledgered, when it ran
    rows = (res.get("result") or {}).get("rows")
    # A result larger than one page comes back with next_offset; fetch every page (export is billed per MB).
    off = res.get("next_offset")
    fetch_error = res.get("_http_error")
    expected_rows = ((res.get("result") or {}).get("metadata") or {}).get("total_row_count")
    while rows is not None and off and not fetch_error:
        try:
            pg = rt.dune("GET", f"/execution/{eid}/results?limit=1000&offset={off}", key)
        except Exception as ex:
            pg = {"_http_error": f"fetch failed: {type(ex).__name__}"}
        fetch_error = pg.get("_http_error")
        rows += (pg.get("result") or {}).get("rows") or []
        off = pg.get("next_offset")
    # A failed page must not look like the end of the result: nothing is exported from an incomplete fetch.
    # The execution id is kept, so the rows can be fetched again without re-running the query.
    incomplete = bool(fetch_error) or (rows is not None and expected_rows is not None and len(rows) != expected_rows)
    if incomplete:
        print(f"INCOMPLETE FETCH: http error {fetch_error}, rows {len(rows or [])} of {expected_rows}; nothing written but the run record")
        rows = None
    # Owner wallets never reach the repo: drop any fetched row naming one, and say so.
    dropped = 0
    if rows:
        ow = set(owner_wallets.owner_wallets())
        keep = [r for r in rows if not any(isinstance(v, str) and v in ow for v in r.values())]
        dropped, rows = len(rows) - len(keep), keep
    rec = {"file": a.sql, "label": a.label, "execution_id": eid, "state": (f"CANCELLED BY WATCHDOG ({killed})" if killed else st.get("state", "NOT RUN (15 min)")), "credits": cost,
           "cap": cap, "proven": a.proven, "inflight_samples": samples, "rows_dropped_owner": dropped,
           "submitted_at": st.get("submitted_at"), "rows": rows, "error": res.get("error") or (f"INCOMPLETE FETCH ({fetch_error})" if incomplete else None)}
    if "asked" in dir(): rec["asked"] = asked
    if a.refetch: rec["refetch_of"] = eid; rec["credits_when_run"] = float(st.get("execution_cost_credits") or 0)
    name = pathlib.Path(a.sql).stem + "_" + dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")
    # --private-rows: the rows name wallets. They go to barrel/private/out (gitignored); the
    # committed run file keeps the cost, the samples and the row count only.
    if a.private_rows:
        pdir = ROOT / "private" / "out"; pdir.mkdir(parents=True, exist_ok=True)
        (pdir / f"rows_{name}.json").write_text(json.dumps(rows, default=str), encoding="utf-8")
        rec["rows"] = None; rec["rows_private"] = True; rec["n_rows"] = len(rows or [])
    # --export (MR-14 ruling 6): build rows go to gitignored barrel/data/; what is committed is the manifest
    # entry: SHA-256 of the file, row count, and the null count of every column.
    if a.export and rows is not None:
        import hashlib
        ddir = ROOT / "data"; ddir.mkdir(parents=True, exist_ok=True)
        blob = json.dumps(rows, default=str).encode("utf-8")
        (ddir / f"{name}.json").write_bytes(blob)
        cols = sorted({k for r in rows for k in r})
        entry = {"file": f"data/{name}.json", "sql": a.sql, "execution_id": eid, "sha256": hashlib.sha256(blob).hexdigest(),
                 "bytes": len(blob), "n_rows": len(rows), "null_counts": {c: sum(r.get(c) is None for r in rows) for c in cols}}
        if "asked" in dir(): entry["asked"] = asked
        mpath = ROOT / "recon" / "export_manifest.json"
        man = json.loads(mpath.read_text(encoding="utf-8")) if mpath.exists() else []
        man.append(entry); mpath.write_text(json.dumps(man, indent=1), encoding="utf-8")
        rec["rows"] = None; rec["exported"] = entry["file"]; rec["n_rows"] = len(rows)
        print(f"exported {len(rows)} rows to barrel/{entry['file']}; manifest entry added")
    (OUT / f"run_{name}.json").write_text(json.dumps(rec, indent=1, default=str), encoding="utf-8")
    # append to ledger
    txt = LEDGER.read_text(encoding="utf-8"); n = txt.count("\n| ") ; total = used + cost
    row = f"| {n} | {a.sql}{(' ('+a.label+')') if a.label else ''}{' (refetch, rows only)' if a.refetch else ''} | `{eid[:12]}…` | {cost:.3f} | {total:.2f} |"
    txt = re.sub(r"(\|[^\n]*\|\n)(\n\*\*Total consumed)", lambda m_: m_.group(1) + row + "\n" + m_.group(2), txt, count=1)
    api_used = float((dune_usage.usage() or {}).get("credits_used") or total)
    txt = re.sub(r"\*\*Total consumed: [\d.]+ credits\. Remaining: -?\d+\. Usable after 15% reserve: -?\d+\.\*\*",
                 f"**Total consumed: {total:.2f} credits. Remaining: {allowance-total:.0f}. Usable after 15% reserve: {allowance-total-reserve:.0f}.**", txt)
    txt = re.sub(r"\n_API says used: [^\n]*_", "", txt)
    txt += f"\n_API says used: {api_used:.3f} of {allowance:.0f}; spendable above the reserve of {reserve:.0f}: {allowance-api_used-reserve:.1f} (authoritative; ledger undercounts pre-ledger probes)_"
    LEDGER.write_text(txt, encoding="utf-8")
    print(f"state={rec['state']} credits={cost:.3f} error={rec['error']}")
    if incomplete: sys.exit(4)
    print(f"cap={cap:.1f} proven={a.proven} in-flight samples={samples[-6:]} rows={len(rows or [])} dropped_owner={dropped}")
    if a.private_rows: print(f"rows written to barrel/private/out/rows_{name}.json")
    if not a.quiet and not a.private_rows and not a.export:
        for r in rec["rows"] or []: print("  ", r)
if __name__ == "__main__": main()
