"""SM-U1: universal Form 4 ingest (discovery mode). EDGAR daily-index (form.idx)
walk over ALL issuers — the correct regime for universal scope (the per-issuer
submissions API stays for the watchlist regime). Persists ALL transaction codes
(P/S/A/M/F/G); code filtering is a query-time concern, never an ingest one.
Rows tagged ingest_regime='universal'. Per-day watermark + accession
idempotence make it resume-safe across interruption.

PH1 is a COST PROBE (measure-and-stop gate); PH2 is the rolling backfill.
"""
import argparse
import datetime as dt
import sys
import time

from . import db as dbmod
from . import form4
from .efd_ingest import load_env


def _trading_days_back(anchor_iso, n):
    """n calendar days back from anchor, newest first (weekends yield empty
    indexes and are skipped by daily_form4 returning None)."""
    a = dt.date.fromisoformat(anchor_iso)
    return [(a - dt.timedelta(days=i)).isoformat() for i in range(n)]


def cost_probe(contact, anchor_iso, days=30, sample=40):
    """Walk `days` of daily indexes; count Form 4s; measure fetch+parse
    throughput on a `sample` of filings; extrapolate 12-month wall-clock and DB
    growth. Returns a report dict."""
    counts = {}
    sample_paths = []
    idx_fetches = 0
    for d in _trading_days_back(anchor_iso, days):
        rows = form4.daily_form4(contact, dt.date.fromisoformat(d))
        idx_fetches += 1
        if rows is None:
            continue
        counts[d] = len(rows)
        for r in rows:
            if len(sample_paths) < sample:
                sample_paths.append(r["path"])
    # throughput: time fetch+parse of the sample
    t0 = time.monotonic()
    parsed_ok = 0
    txns = 0
    for p in sample_paths:
        try:
            pr = form4.fetch_form4_xml(contact, p)
            if pr:
                parsed_ok += 1
                txns += len(pr.get("txns", []))
        except Exception:  # noqa: BLE001
            pass
    elapsed = time.monotonic() - t0
    n_sample = len(sample_paths)
    per_filing_s = elapsed / n_sample if n_sample else None
    avg_txns = txns / parsed_ok if parsed_ok else 0
    trading_days = [d for d in counts]
    mean_per_trading_day = (sum(counts.values()) / len(trading_days)) if trading_days else 0
    yearly = mean_per_trading_day * 252
    twelve_mo_filings = yearly  # 12mo depth
    wall_h = (twelve_mo_filings * per_filing_s / 3600) if per_filing_s else None
    db_rows = twelve_mo_filings * avg_txns
    db_mb = db_rows * 250 / 1e6  # ~250 bytes/row rough
    return {
        "anchor": anchor_iso, "days_walked": days,
        "trading_days_with_index": len(trading_days),
        "form4_total_in_window": sum(counts.values()),
        "mean_per_trading_day": round(mean_per_trading_day, 1),
        "max_day": max(counts.values()) if counts else 0,
        "sample_filings": n_sample, "sample_parse_ok": parsed_ok,
        "per_filing_seconds": round(per_filing_s, 3) if per_filing_s else None,
        "avg_txns_per_filing": round(avg_txns, 2),
        "est_12mo_filings": int(twelve_mo_filings),
        "est_12mo_wall_hours": round(wall_h, 1) if wall_h else None,
        "est_12mo_db_rows": int(db_rows), "est_12mo_db_mb": round(db_mb, 1),
        "exceeds_12h": bool(wall_h and wall_h > 12),
    }


def backfill_day(con, contact, day):
    """Persist every Form 4 in a day's index (all codes). Idempotent by
    accession. Returns (form4_count, persisted, parse_fail)."""
    rows = form4.daily_form4(contact, dt.date.fromisoformat(day))
    if rows is None:
        return None
    seen = {r[0] for r in con.execute("SELECT accession FROM form4_backfill_seen")}
    persisted = 0
    parse_fail = 0
    for r in rows:
        accession = r["path"].rsplit("/", 1)[-1].replace(".txt", "")
        if accession in seen:
            continue
        try:
            parsed = form4.fetch_form4_xml(contact, r["path"])
            if parsed:
                ticker = parsed.get("symbol")
                n, _ = form4.persist_transactions(con, accession, parsed, ticker,
                                                  day, regime="universal")
                persisted += n
            else:
                parse_fail += 1
        except Exception:  # noqa: BLE001 - count, never guess
            parse_fail += 1
        con.execute("INSERT OR IGNORE INTO form4_backfill_seen VALUES (?,?)",
                    (accession, int(time.time())))
        con.commit()
    return len(rows), persisted, parse_fail


def backfill(con, contact, months, anchor_iso):
    start = dt.date.fromisoformat(anchor_iso) - dt.timedelta(days=int(months * 30.44))
    done = {r[0] for r in con.execute("SELECT day FROM form4_universal_days")}
    day = dt.date.fromisoformat(anchor_iso)
    totals = {"days": 0, "form4": 0, "persisted": 0, "parse_fail": 0, "skipped": 0}
    while day >= start:
        diso = day.isoformat()
        if diso in done:
            totals["skipped"] += 1
        else:
            res = backfill_day(con, contact, diso)
            if res is not None:
                fc, pers, pf = res
                con.execute("INSERT OR REPLACE INTO form4_universal_days VALUES (?,?,?,?,?)",
                            (diso, fc, pers, pf, int(time.time())))
                con.commit()
                totals["days"] += 1
                totals["form4"] += fc
                totals["persisted"] += pers
                totals["parse_fail"] += pf
                if totals["days"] % 10 == 0:
                    print("[universal] {} days, {} filings, {} rows, {} parse-fail".format(
                        totals["days"], totals["form4"], totals["persisted"],
                        totals["parse_fail"]), flush=True)
        day -= dt.timedelta(days=1)
    return totals


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-U1 universal Form 4 ingest")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--mode", choices=["probe", "backfill"], default="probe")
    ap.add_argument("--anchor", default=dt.date.today().isoformat())
    ap.add_argument("--probe-days", type=int, default=30)
    ap.add_argument("--months", type=float, default=12)
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    if args.mode == "probe":
        rep = cost_probe(contact, args.anchor, args.probe_days)
        print("[probe] " + " ".join("{}={}".format(k, v) for k, v in rep.items()))
        if rep["exceeds_12h"]:
            print("[probe] GATE: est 12mo wall-clock {}h EXCEEDS ~12h — STOP, "
                  "report to Mando.".format(rep["est_12mo_wall_hours"]))
        return 0
    tot = backfill(con, contact, args.months, args.anchor)
    print("[universal] DONE {}".format(tot))
    return 0


if __name__ == "__main__":
    sys.exit(main())
