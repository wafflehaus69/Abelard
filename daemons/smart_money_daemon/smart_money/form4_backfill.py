"""SM-F4 Step 2: bounded historical Form 4 backfill, issuer-driven.

METHOD DEVIATION (flagged for Mando): the order proposed a quarterly form.idx
walk. Our scope is issuer-driven, so this uses EDGAR's per-issuer submissions
API (data.sec.gov/submissions/CIK.json + older pages), which lists an issuer's
Form 4 filings directly. That is far cheaper than downloading ~12 large
quarterly indexes to filter for a handful of issuers, and it matches how SM-A1
Phase 1 enumerates by browse. Same corpus, same Step-1 persistence, same
YYYYMMDD-date lesson (dates are ISO in the submissions API). Swap to form.idx is
easy if Mando prefers.

Resume-safe: every processed accession is recorded in form4_backfill_seen, so a
second run does zero fetches (two-run idempotence). persist itself is also
idempotent by (accession, tx_index). UA header + 10 req/s cap. Parse failures
are counted, never guessed. Per-issuer watermark stored for visibility.
"""
import argparse
import datetime as dt
import json
import sys
import time

import requests

from . import db as dbmod
from . import form4
from . import watermarks
from .efd_ingest import load_env
from .overlay import load_overlay

SUB_URL = "https://data.sec.gov/submissions/{name}"
PACE = 0.11  # <10 req/s
FORM_TYPES = {"4", "4/A"}


def _ua(contact):
    return {"User-Agent": form4.UA_TMPL.format(contact)}


def issuer_ciks(contact, overlay, reg, extra_ciks=None):
    """Overlay tickers -> CIK, plus registry insider CIKs, plus extra (e.g. the
    SM-A1 trump_network issuer set). Returns {cik10: label}."""
    tickers = sorted(overlay.conviction | overlay.watchlist)
    tk_cik = form4.ticker_to_cik(contact, tickers)
    out = {c: t for t, c in tk_cik.items()}
    for e in reg["entries"]:
        if e.get("role") == "insider" and e.get("cik"):
            out[str(e["cik"]).zfill(10)] = e.get("name", "registry")
    for c in (extra_ciks or []):
        out[str(c).zfill(10)] = out.get(str(c).zfill(10), "trump_network")
    return out


def _iter_filings(contact, cik10):
    """Yield (accession, form, filed_date) across recent + older submission
    pages for an issuer CIK."""
    time.sleep(PACE)
    r = requests.get(SUB_URL.format(name="CIK{}.json".format(cik10)),
                     headers=_ua(contact), timeout=30)
    r.raise_for_status()
    d = r.json()
    pages = [d["filings"]["recent"]]
    for f in d["filings"].get("files", []):
        time.sleep(PACE)
        rr = requests.get(SUB_URL.format(name=f["name"]), headers=_ua(contact),
                          timeout=30)
        if rr.status_code == 200:
            pages.append(rr.json())
    for block in pages:
        for acc, form, fdate in zip(block["accessionNumber"], block["form"],
                                    block["filingDate"]):
            yield acc, form, fdate


def _fetch_and_persist(con, contact, cik10, accession, filed_date):
    acc_nodash = accession.replace("-", "")
    idx = form4.ARCH.format(cik=cik10.lstrip("0"), acc_nodash=acc_nodash,
                            doc="index.json")
    time.sleep(PACE)
    d = requests.get(idx, headers=_ua(contact), timeout=30).json()
    doc = None
    for it in d["directory"]["item"]:
        if it["name"].lower().endswith(".xml"):
            doc = it["name"]
    if not doc:
        return None
    url = form4.ARCH.format(cik=cik10.lstrip("0"), acc_nodash=acc_nodash, doc=doc)
    time.sleep(PACE)
    parsed = form4.parse_ownership(
        requests.get(url, headers=_ua(contact), timeout=30).text)
    ticker = parsed.get("symbol")
    n, _ = form4.persist_transactions(con, accession, parsed, ticker, filed_date)
    return n


def backfill(con, contact, ciks, months, now_iso):
    since = (dt.date.fromisoformat(now_iso)
             - dt.timedelta(days=int(months * 30.44))).isoformat()
    seen = {r[0] for r in con.execute("SELECT accession FROM form4_backfill_seen")}
    report = {}
    for cik10, label in ciks.items():
        stats = {"filings": 0, "persisted_rows": 0, "parse_fail": 0,
                 "skipped_seen": 0, "newest": None}
        try:
            filings = list(_iter_filings(contact, cik10))
        except requests.RequestException as exc:
            report[cik10] = {"label": label, "error": str(exc)[:120]}
            continue
        for acc, form, fdate in filings:
            if form not in FORM_TYPES or fdate < since:
                continue
            if acc in seen:
                stats["skipped_seen"] += 1
                continue
            try:
                n = _fetch_and_persist(con, contact, cik10, acc, fdate)
                if n is None:
                    stats["parse_fail"] += 1
                else:
                    stats["persisted_rows"] += n
                    stats["filings"] += 1
                    stats["newest"] = max(stats["newest"] or "", fdate)
            except Exception as exc:  # noqa: BLE001 - count, never guess
                stats["parse_fail"] += 1
                report.setdefault("_errors", []).append(
                    "{} {} {}".format(cik10, acc, str(exc)[:80]))
            con.execute("INSERT OR IGNORE INTO form4_backfill_seen VALUES (?,?)",
                        (acc, int(time.time())))
            seen.add(acc)
            con.commit()
        if stats["newest"]:
            watermarks.advance(con, "form4_backfill:{}".format(cik10),
                               stats["newest"])
        report[cik10] = {"label": label, **stats}
    return {"since": since, "months": months, "issuers": report}


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-F4 Step 2 Form 4 backfill")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--months", type=float, default=36)
    ap.add_argument("--now", default=dt.date.today().isoformat())
    ap.add_argument("--only", help="comma-separated tickers to limit scope (smoke)")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    overlay = load_overlay()
    reg = {"entries": []}
    import pathlib
    regp = pathlib.Path("analysis/registry.json")
    if regp.exists():
        reg = json.loads(regp.read_text())
    # SM-A1 Phase 1 hands its discovered issuer set here; auto-include if present.
    tn_tickers = []
    tnp = pathlib.Path("scans/trump_network_issuers.json")
    if tnp.exists():
        tn_tickers = json.loads(tnp.read_text()).get("tickers", [])
    ciks = issuer_ciks(contact, overlay, reg)
    if tn_tickers:
        tk_cik = form4.ticker_to_cik(contact, tn_tickers)
        for t, c in tk_cik.items():
            ciks[c] = ciks.get(c, "trump_network:" + t)
        print("[backfill] trump_network added {} tickers -> {} CIKs".format(
            len(tn_tickers), len(tk_cik)))
    if args.only:
        want = {t.strip().upper() for t in args.only.split(",")}
        ciks = {c: l for c, l in ciks.items() if l.upper() in want}
    print("[backfill] {} issuer CIKs, {} months, since scope".format(
        len(ciks), args.months))
    rep = backfill(con, contact, ciks, args.months, args.now)
    tot_rows = sum(v.get("persisted_rows", 0) for v in rep["issuers"].values()
                   if isinstance(v, dict))
    tot_fil = sum(v.get("filings", 0) for v in rep["issuers"].values()
                  if isinstance(v, dict))
    print("[backfill] since={} filings={} persisted_rows={}".format(
        rep["since"], tot_fil, tot_rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
