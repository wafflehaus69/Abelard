"""Run a repo .sql file on Dune with the credit meter, long poll, ledger append.
python barrel/recon/dune_run_sql.py recon/sql/x.sql --expect 5
Refuses if --expect exceeds (remaining - 15% reserve) per the ledger. Remaining
is ledger-derived until a usage endpoint is confirmed (readiness 1.1)."""
import argparse, json, pathlib, re, sys, time, datetime as dt
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
import dune_usage
ROOT = pathlib.Path(__file__).resolve().parents[1]; OUT = ROOT / "recon" / "out"; LEDGER = ROOT / "docs" / "credits.md"
ALLOWANCE = 2500.0; RESERVE = 0.15
def consumed():
    m = re.search(r"Total consumed: ([\d.]+)", LEDGER.read_text(encoding="utf-8")); return float(m.group(1)) if m else 0.0
def main():
    ap = argparse.ArgumentParser(); ap.add_argument("sql"); ap.add_argument("--expect", type=float, required=True); ap.add_argument("--label", default="")
    a = ap.parse_args(); sql = pathlib.Path(a.sql).read_text(encoding="utf-8")   # path as given, relative to cwd
    # Authoritative balance from POST /v1/usage (no credits consumed); ledger is the fallback.
    u = dune_usage.usage()
    if u.get("credits_included") is not None:
        allowance, used, src = float(u["credits_included"]), float(u["credits_used"]), "api"
    else:
        allowance, used, src = ALLOWANCE, consumed(), "ledger"
    usable = (allowance - used) * (1 - RESERVE)
    if a.expect > usable:
        print(f"REFUSED: expect {a.expect} > usable {usable:.1f} ({src}: allowance {allowance:.0f}, used {used:.2f}, reserve {RESERVE:.0%})"); sys.exit(2)
    print(f"meter[{src}]: used {used:.2f} / {allowance:.0f}, usable {usable:.1f}, this run expects {a.expect}")
    key = rt.api_key(); e = rt.dune("POST", "/sql/execute", key, {"sql": sql, "performance": "medium"})
    eid = e.get("execution_id")
    if not eid: print("submission refused:", e); sys.exit(1)
    t0 = time.time(); st = {}
    while time.time() - t0 < 900:
        st = rt.dune("GET", f"/execution/{eid}/status", key)
        if st.get("is_execution_finished"): break
        time.sleep(5)
    res = rt.dune("GET", f"/execution/{eid}/results", key) if st.get("is_execution_finished") else {}
    cost = float(st.get("execution_cost_credits") or 0)
    rec = {"file": a.sql, "label": a.label, "execution_id": eid, "state": st.get("state", "NOT RUN (15 min)"), "credits": cost,
           "submitted_at": st.get("submitted_at"), "rows": (res.get("result") or {}).get("rows"), "error": res.get("error")}
    name = pathlib.Path(a.sql).stem + "_" + dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")
    (OUT / f"run_{name}.json").write_text(json.dumps(rec, indent=1, default=str), encoding="utf-8")
    # append to ledger
    txt = LEDGER.read_text(encoding="utf-8"); n = txt.count("\n| ") ; total = used + cost
    row = f"| {n} | {a.sql}{(' ('+a.label+')') if a.label else ''} | `{eid[:12]}…` | {cost:.3f} | {total:.2f} |"
    txt = re.sub(r"(\|[^\n]*\|\n)(\n\*\*Total consumed)", lambda m_: m_.group(1) + row + "\n" + m_.group(2), txt, count=1)
    api_used = float((dune_usage.usage() or {}).get("credits_used") or total)
    txt = re.sub(r"\*\*Total consumed: [\d.]+ credits\. Remaining: \d+\. Usable after 15% reserve: \d+\.\*\*",
                 f"**Total consumed: {total:.2f} credits. Remaining: {ALLOWANCE-total:.0f}. Usable after 15% reserve: {(ALLOWANCE-total)*0.85:.0f}.**", txt)
    txt = re.sub(r"\n_API says used: [^\n]*_", "", txt)
    txt += f"\n_API says used: {api_used:.3f} of {ALLOWANCE:.0f} (authoritative; ledger undercounts pre-ledger probes)_"
    LEDGER.write_text(txt, encoding="utf-8")
    print(f"state={rec['state']} credits={cost:.3f} error={rec['error']}")
    for r in rec["rows"] or []: print("  ", r)
if __name__ == "__main__": main()
