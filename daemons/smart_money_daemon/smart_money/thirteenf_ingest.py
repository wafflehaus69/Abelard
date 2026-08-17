"""SM-A1 Phase 2: confirmed-filer 13F multi-quarter ingest.

Pulls the last N 13F-HR filings for the Mando-confirmed CIK set, parses each
information table into per-holding rows, maps CUSIP -> ticker via OpenFIGI
(free, keyless, cached), and lands durable rows into thirteenf_holdings.
Idempotent by (cik, accession) via thirteenf_filings_seen. Reports retrieved
vs requested and the unmapped-CUSIP count + list — never silently dropped.
"""
import argparse
import json
import sys
import time

import requests

from . import db as dbmod
from . import filing_scale
from . import thirteenf
from .efd_ingest import load_env

# Mando-confirmed set. No substitutions, no additions without an order.
# SM-A1 Phase 2 seeded the first six; ORDER SM-P2 added ten more after the CIK
# resolution report (scans/SM_P2_FILER_RESOLUTION.md) and Mando's rulings:
#   * Appaloosa -> 0001656456, the LIVE entity (0001006438 stops filing in 2016).
#   * Greenlight Capital DROPPED — its only CIK stopped filing 2024-02-14, so an
#     8-quarter window would be near-empty. Dropped on ruling, not an oversight.
#   * Scion kept at 0001649339 despite its newest filing being ~2 quarters behind
#     the rest (0001182422 is the dead 2005-2009 Scion Capital era).
CONFIRMED = {
    "0001536411": "Duquesne Family Office",
    "0001562087": "Thiel Macro",
    "0001846021": "Founders Fund VII",
    "0002106825": "Founders Fund Growth II",
    "0002059583": "Affinity Partners (Kushner)",
    "0002045724": "Situational Awareness LP",
    # --- ORDER SM-P2 expansion ---
    "0001135730": "Coatue Management",
    "0001387322": "Whale Rock Capital Management",
    "0001569049": "Light Street Capital Management",
    "0001061165": "Lone Pine Capital",
    "0001656456": "Appaloosa LP",
    "0001263508": "Baker Bros Advisors",
    "0001336528": "Pershing Square Capital Management",
    "0001029160": "Soros Fund Management",
    "0001040273": "Third Point LLC",
    "0001649339": "Scion Asset Management",
    # --- Mag7 operating companies that actually file 13F (verified: Apple, Microsoft,
    # Meta and Tesla do NOT; their strategic stakes surface only via SC 13D/G). These
    # are corporate strategic books, not funds — a different KIND of filer, tagged
    # corporate_strategic so they never read as manager skill.
    "0001652044": "Alphabet Inc",
    "0001018724": "Amazon com Inc",
    "0001045810": "NVIDIA Corp",
}
OPENFIGI = "https://api.openfigi.com/v3/mapping"
FIGI_BATCH = 10       # jobs per request (keyless limit)
FIGI_PACE = 2.6       # ~25 requests/min keyless


def list_13f_filings(cik, contact, limit=8):
    """Newest-first list of up to `limit` 13F-HR filings for a CIK."""
    cik10 = str(int(cik)).zfill(10)
    time.sleep(thirteenf.PACE)
    r = requests.get(thirteenf.SUBMISSIONS.format(cik10=cik10),
                     headers=thirteenf._ua(contact), timeout=30)
    if r.status_code != 200:
        raise RuntimeError("submissions HTTP {} for {}".format(r.status_code, cik))
    d = r.json()["filings"]["recent"]
    rdates = d.get("reportDate", d["filingDate"])
    out = []
    for form, acc, fdate, rdate in zip(
            d["form"], d["accessionNumber"], d["filingDate"], rdates):
        if form == "13F-HR":
            out.append({"accession": acc, "period": rdate, "filed": fdate})
        if len(out) >= limit:
            break
    return out


def _holding_rows(holdings):
    """parse_holdings aggregates per cusip into long/call/put buckets; emit one
    durable row per non-zero bucket.

    shares_type belongs only to the long bucket — option buckets carry shares=0 by
    construction, so typing them SH would assert something the filing never said.
    title_of_class travels with every bucket; it is the filer's own class label."""
    for cusip, h in holdings.items():
        title = h.get("title_of_class")
        if h["value"] or h["shares"]:
            yield (cusip, h["issuer"], "long", h["value"], h["shares"],
                   h.get("shares_type"), title)
        if h["call_val"]:
            yield cusip, h["issuer"], "call", h["call_val"], 0, None, title
        if h["put_val"]:
            yield cusip, h["issuer"], "put", h["put_val"], 0, None, title


def map_cusips(con, cusips, contact):
    """CUSIP -> ticker via OpenFIGI, cached in cusip_ticker. Returns
    {cusip: ticker_or_None}. Method + failure surfaced by the caller."""
    have = {r[0]: r[1] for r in con.execute("SELECT cusip, ticker FROM cusip_ticker")}
    todo = sorted({c for c in cusips if c and c not in have})
    for i in range(0, len(todo), FIGI_BATCH):
        batch = todo[i:i + FIGI_BATCH]
        jobs = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]
        time.sleep(FIGI_PACE)
        try:
            r = requests.post(OPENFIGI, json=jobs,
                              headers={"Content-Type": "application/json",
                                       "User-Agent": thirteenf.UA_TMPL.format(contact)},
                              timeout=30)
        except requests.RequestException:
            r = None
        results = r.json() if (r is not None and r.status_code == 200) else \
            [{} for _ in batch]
        for cusip, res in zip(batch, results):
            tk = None
            data = res.get("data") if isinstance(res, dict) else None
            if data:
                tk = data[0].get("ticker")
            con.execute(
                "INSERT OR REPLACE INTO cusip_ticker VALUES (?,?,?,?,?)",
                (cusip, tk, (data[0].get("name") if data else None),
                 "openfigi", int(time.time())))
            have[cusip] = tk
        con.commit()
    return {c: have.get(c) for c in cusips}


def ingest_filer(con, cik, contact, quarters, report):
    seen = {r[0] for r in con.execute(
        "SELECT accession FROM thirteenf_filings_seen WHERE cik=?",
        (str(int(cik)),))}
    filings = list_13f_filings(cik, contact, limit=quarters)
    stat = {"requested": quarters, "retrieved": len(filings), "new": 0,
            "holding_rows": 0, "cusips": 0}
    all_cusips = set()
    pending = []
    for f in filings:
        if f["accession"] in seen:
            continue
        holdings = thirteenf.fetch_info_table(cik, f["accession"], contact)
        rows = list(_holding_rows(holdings))
        for cusip, issuer, pc, val, sh, stype, title in rows:
            all_cusips.add(cusip)
            pending.append((cik, f, cusip, issuer, pc, val, sh, stype, title))
        f["cover"] = thirteenf.fetch_cover(cik, f["accession"], contact)
        stat["new"] += 1
    cmap = map_cusips(con, all_cusips, contact) if all_cusips else {}
    for cik_, f, cusip, issuer, pc, val, sh, stype, title in pending:
        con.execute(
            "INSERT OR REPLACE INTO thirteenf_holdings("
            "cik, accession, period, filed_date, cusip, ticker, issuer, put_call,"
            "value, shares, ingested_at_unix, shares_type, title_of_class)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(int(cik_)), f["accession"], f["period"], f["filed"], cusip,
             cmap.get(cusip), issuer, pc, val, sh, int(time.time()), stype, title))
        stat["holding_rows"] += 1
    # Resolve the VALUE unit per filing and stamp it, so no reader re-derives it.
    # Runs after the rows land because the anchor reads them back.
    for f in filings:
        if f["accession"] in seen:
            continue
        r = filing_scale.resolve(con, cik, f["accession"], f["period"])
        r["filed_date"] = f["filed"]
        cover = f.get("cover") or {}
        if r["value_scale"]:
            filing_scale.apply_to_filing(con, cik, f["accession"], r["value_scale"])
        filing_scale.record_meta(con, r, cover.get("entry_total"),
                                 cover.get("value_total"))
        stat.setdefault("scale", {})[f["accession"]] = r["scale_basis"]
        # The cover page declares its own row count; a mismatch means we parsed a
        # partial table, which would also poison the price anchor above.
        if cover.get("entry_total") and cover["entry_total"] != r["parsed_rows"]:
            report.setdefault("control_total_mismatch", []).append({
                "cik": str(int(cik)), "accession": f["accession"],
                "declared_rows": cover["entry_total"],
                "parsed_rows": r["parsed_rows"]})
    for f in filings:
        if f["accession"] not in seen:
            con.execute("INSERT OR IGNORE INTO thirteenf_filings_seen VALUES (?,?,?)",
                        (str(int(cik)), f["accession"], int(time.time())))
    con.commit()
    stat["cusips"] = len(all_cusips)
    report["filers"][str(int(cik))] = {"name": CONFIRMED.get(cik.zfill(10), "?"), **stat}


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-A1 Phase 2 confirmed 13F ingest")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--quarters", type=int, default=8)
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    report = {"filers": {}}
    for cik10 in CONFIRMED:
        try:
            ingest_filer(con, cik10, contact, args.quarters, report)
        except Exception as exc:  # noqa: BLE001 - fail loud, per filer
            # Key on the SAME form the success path uses, else the lookup below misses
            # and a real failure prints as a bare "None" — a silent failure, which is
            # exactly what the fail-loud doctrine forbids.
            report["filers"][str(int(cik10))] = {
                "name": CONFIRMED[cik10], "error": str(exc)[:150]}
        stat = report["filers"].get(str(int(cik10)))
        print("[13f] {} {}".format(cik10, stat if stat is not None else
                                   "NO REPORT ENTRY - investigate"), flush=True)
    # unmapped CUSIP report
    unmapped = con.execute(
        "SELECT cusip, name FROM cusip_ticker WHERE ticker IS NULL").fetchall()
    total = con.execute("SELECT COUNT(*) FROM cusip_ticker").fetchone()[0]
    print("[13f] CUSIP map via OpenFIGI: {}/{} unmapped ({:.1f}%)".format(
        len(unmapped), total, 100.0 * len(unmapped) / total if total else 0))
    holdings = con.execute("SELECT COUNT(*) FROM thirteenf_holdings").fetchone()[0]
    print("[13f] thirteenf_holdings rows: {}".format(holdings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
