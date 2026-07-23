"""Delta-scan positioning-events emitter (SM-4 STEP 3). One command, three legs,
one envelope. Emits positioning events, never a leaderboard. Scripts-only, no
LLM. Fail-loud with DEGRADED source status; zero events on a quiet day is
SUCCESS, never an error, never a fabricated event.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import time

from . import db as dbmod
from . import form4, thirteenf, watermarks
from .events import load_registry, make_event
from .overlay import load_overlay
from .efd_ingest import load_env
from . import house_ingest
from .amendments import apply_supersedes
from .efd_session import bootstrap, post_data, EfdSessionError

REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "..", "analysis",
                             "registry.json")
UA_TMPL = "Abelard-SmartMoney mdiba personal research {}"


def _src(name, status, note="", items=0):
    return {"source": name, "status": status, "note": note, "items": items}


def leg_congress(con, scan_id, scan_start, overlay, reg, ua, raw_dir):
    """House current-year delta + Senate search-delta (DEGRADED if WAF)."""
    sources = []
    events = []
    counts = {"house_new_filings": 0, "senate_new_filings": 0}

    # House: refresh current-year index, ingest new DocIDs (dedup resume-safe).
    year = dt.date.today().year
    try:
        entries = house_ingest.fetch_year_zip(year, raw_dir, ua)
        new_house = 0
        if entries:
            for filing in entries:
                outcome = house_ingest.ingest_filing(con, filing, year, raw_dir, ua)
                if outcome == "electronic":
                    new_house += 1
        counts["house_new_filings"] = new_house
        sources.append(_src("house_clerk", "OK", items=new_house))
    except Exception as exc:  # noqa: BLE001 - fail-loud into source status
        sources.append(_src("house_clerk", "DEGRADED", str(exc)[:120]))

    # Senate: attempt the certified search delta. WAF blocks the endpoint for
    # scripts (recon/EFD_WAF_FINDING.md); on 503 report DEGRADED, never fake.
    try:
        s = bootstrap(ua, probe=False)
        post_data(s, {"draw": "1", "start": "0", "length": "1",
                      "report_types": "[11]", "filer_types": "[]",
                      "submitted_start_date": "", "submitted_end_date": "",
                      "candidate_state": "", "senator_state": "", "office_id": "",
                      "first_name": "", "last_name": ""})
        sources.append(_src("senate_efd", "OK", "search reachable", 0))
    except EfdSessionError as exc:
        sources.append(_src("senate_efd", "DEGRADED",
                            "search endpoint blocked ({}); browser index adapter "
                            "is a deploy concern".format(str(exc)[:40])))

    # F5 amendment supersede — active in the scan path, first live use.
    apply_supersedes(con)

    # New congressional trades = filings ingested this scan, not superseded.
    rows = con.execute(
        "SELECT ct.person_id, p.name, p.cik_or_chamber, ct.ticker, ct.side, "
        "ct.amt_low, ct.amt_high, ct.tx_date, ct.disclosure_date, ct.lag_days, "
        "ct.filing_id, ct.chamber FROM congress_trades ct "
        "JOIN persons p USING(person_id) "
        "JOIN ingested_filings f ON f.filing_id = ct.filing_id "
        "WHERE ct.superseded = 0 AND f.ingested_at_unix >= ?",
        (scan_start,),
    ).fetchall()

    newest_disc = None
    for (pid, name, chamber, ticker, side, lo, hi, tx, disc, lag, fid,
         cham) in rows:
        rentry = reg["by_name"].get(name)
        ev = make_event(
            scan_id, "congress", name,
            rentry["role"] if rentry else None,
            rentry["status"] if rentry else None,
            ticker, side, "stock", (lo, hi), tx, disc, lag, None,
            "efd" if cham == "senate" else "house_clerk", fid, overlay, con,
        )
        events.append(ev)
        if disc and (newest_disc is None or disc > newest_disc):
            newest_disc = disc

    # Watermark advances ONLY on ok-with-items, to newest ingested disclosure.
    if rows and newest_disc:
        watermarks.advance(con, "house_clerk", newest_disc)
    return events, sources, counts


def leg_form4(con, scan_id, overlay, reg, contact):
    sources = []
    events = []
    counts = {"open_market": 0, "counted_only": 0, "filings_matched": 0}
    ua = UA_TMPL.format(contact)
    try:
        overlay_tickers = overlay.conviction | overlay.watchlist
        insider_ciks = {e.get("cik") for e in reg["entries"]
                        if e.get("role") == "insider" and e.get("cik")}
        tk_cik = form4.ticker_to_cik(contact, overlay_tickers)
        want_ciks = {c.lstrip("0") for c in tk_cik.values()} | \
                    {c.lstrip("0") for c in insider_ciks}
        cik_to_ticker = {v.lstrip("0"): k for k, v in tk_cik.items()}

        for d in (dt.date.today() - dt.timedelta(days=1), dt.date.today()):
            rows = form4.daily_form4(contact, d)
            if rows is None:
                continue
            for row in rows:
                if row["cik"].lstrip("0") not in want_ciks:
                    continue
                counts["filings_matched"] += 1
                parsed = form4.fetch_form4_xml(contact, row["path"])
                if not parsed:
                    continue
                ticker = parsed["symbol"] or cik_to_ticker.get(row["cik"].lstrip("0"))
                # SM-F4 Step 1: persist the full parsed filing to the corpus. No
                # scan discards parsed data from here on.
                accession = row["path"].rsplit("/", 1)[-1].replace(".txt", "")
                form4.persist_transactions(con, accession, parsed, ticker,
                                           row["date"])
                con.commit()
                for t in parsed["txns"]:
                    code = t["code"]
                    if code not in form4.OPEN_MARKET:
                        counts["counted_only"] += 1
                        continue
                    counts["open_market"] += 1
                    side = "purchase" if code == "P" else "sale"
                    shares = float(t["shares"] or 0)
                    price = float(t["price"] or 0)
                    ev = make_event(
                        scan_id, "form4", parsed["owner"], "insider", None,
                        ticker, side, "stock", None, t["date"], d.isoformat(),
                        None, parsed["plan_flag"], "edgar_form4",
                        row["path"], overlay, con,
                        shares=shares, value=round(shares * price, 2),
                    )
                    events.append(ev)
        sources.append(_src("edgar_form4", "OK", items=counts["open_market"]))
    except Exception as exc:  # noqa: BLE001
        sources.append(_src("edgar_form4", "DEGRADED", str(exc)[:120]))
    return events, sources, counts


def leg_13f(con, scan_id, overlay, reg, contact):
    sources = []
    events = []
    counts = {"managers_checked": 0, "new_filings": 0, "diff_lines": 0}
    ciks = [(e["name"], e.get("cik")) for e in reg["entries"]
            if e.get("role") == "manager_13f" and e.get("cik")]
    for name, cik in ciks:
        counts["managers_checked"] += 1
        try:
            latest = thirteenf.latest_13f(cik, contact)
            base = thirteenf.get_baseline(con, cik)
            if not latest:
                sources.append(_src("13f:{}".format(cik), "OK", "no 13F-HR"))
                continue
            if base and latest["accession"] == base["accession"]:
                sources.append(_src("13f:{}".format(cik), "OK",
                                    "no new filing since {}".format(base["period"])))
                continue
            new_holdings = thirteenf.fetch_info_table(cik, latest["accession"], contact)
            counts["new_filings"] += 1
            if base:
                for cusip, issuer, kind, detail in thirteenf.diff(
                        base["holdings"], new_holdings):
                    counts["diff_lines"] += 1
                    ev = make_event(
                        scan_id, "13f", None, "manager_13f", "active",
                        None, kind, "stock", None, latest["period"],
                        latest["filed"], None, None, "edgar_13f",
                        "{}/{}".format(cik, latest["accession"]), overlay, con,
                        entity=name, value=detail.get("to") or detail.get("value"),
                    )
                    ev["issuer"] = issuer
                    ev["diff_detail"] = detail
                    events.append(ev)
            thirteenf.store_baseline(con, cik, latest, new_holdings)
            sources.append(_src("13f:{}".format(cik), "OK",
                                "new filing {}".format(latest["period"]),
                                counts["diff_lines"]))
        except Exception as exc:  # noqa: BLE001
            sources.append(_src("13f:{}".format(cik), "DEGRADED", str(exc)[:120]))
    return events, sources, counts


def run_scan(con, contact, raw_dir):
    scan_start = int(time.time())
    scan_id = "scan_{}".format(scan_start)
    overlay = load_overlay()
    reg = load_registry(REGISTRY_PATH)
    ua = UA_TMPL.format(contact)

    wm_before = {r[0]: r[1] for r in con.execute(
        "SELECT source, watermark_ts FROM watermarks")}

    ev_a, src_a, cnt_a = leg_congress(con, scan_id, scan_start, overlay, reg, ua, raw_dir)
    ev_b, src_b, cnt_b = leg_form4(con, scan_id, overlay, reg, contact)
    ev_c, src_c, cnt_c = leg_13f(con, scan_id, overlay, reg, contact)

    sources = src_a + src_b + src_c
    all_events = ev_a + ev_b + ev_c

    # Event-level dedup across scans by event_id (scan_events ledger). A Form 4
    # or 13F seen in a prior scan is NOT re-emitted; makes the whole scan
    # idempotent, so a quiet re-run yields 0 new events. congress dedup is also
    # covered here on top of its ingest-time guard.
    events = []
    for ev in all_events:
        if con.execute("SELECT 1 FROM scan_events WHERE event_id=?",
                       (ev["event_id"],)).fetchone():
            continue
        events.append(ev)
        con.execute(
            "INSERT OR IGNORE INTO scan_events VALUES (?,?,?,?,?,?,?,?)",
            (ev["event_id"], scan_id, ev["leg"], ev["ticker"], ev["side"],
             ev["tx_date"], ev["disclosure_date"], int(time.time())))
    con.commit()

    # Per-source watermark advance for the fixed-window legs, only on
    # ok-with-NEW-items, to the newest emitted disclosure date.
    for src_name, leg_name in (("edgar_form4", "form4"), ("edgar_13f", "13f")):
        discs = [e["disclosure_date"] for e in events
                 if e["leg"] == leg_name and e["disclosure_date"]]
        src_ok = any(s["source"] == src_name and s["status"] == "OK"
                     for s in sources) or leg_name == "form4"
        if discs and src_ok:
            watermarks.advance(con, src_name, max(discs))

    wm_after = {r[0]: r[1] for r in con.execute(
        "SELECT source, watermark_ts FROM watermarks")}

    envelope = {
        "scan_id": scan_id,
        "started": dt.datetime.fromtimestamp(
            scan_start, dt.timezone.utc).isoformat(),
        "finished": dt.datetime.fromtimestamp(
            int(time.time()), dt.timezone.utc).isoformat(),
        "watermarks": {"before": wm_before, "after": wm_after},
        "sources": sources,
        "counts": {"congress": cnt_a, "form4": cnt_b, "thirteenf": cnt_c,
                   "events_total": len(events)},
        "events": events,
        "cost": 0.0,
    }
    return envelope, events


def _notable(ev):
    """An event Abelard should judge: any overlay, cluster, or sentinel hit.
    Quiet events stay in the envelope and scan_events but do not flood the
    decision queue."""
    f = ev.get("flags") or {}
    return bool(f.get("conviction_overlay") or f.get("watchlist_overlay")
                or f.get("cluster") or f.get("sentinel"))


def _enqueue(envelope, events):
    """Enqueue NOTABLE events to abelard_queue. Soft-detect: absent sink =
    envelope-noted, not fatal (Orban lacks it). Idempotent by event_id via the
    dedupe_key UNIQUE constraint, so re-scans never double-enqueue. The daemon
    only enqueues; Abelard's consumer interprets and decides push or suppress."""
    import sqlite3
    qpath = os.environ.get("ABELARD_QUEUE_DB_PATH") or \
        dbmod._load_env_var("ABELARD_QUEUE_DB_PATH")
    if not qpath or not os.path.exists(os.path.expanduser(qpath)):
        return {"queue": "absent", "enqueued": 0,
                "note": "no abelard_queue sink on this host"}
    qpath = os.path.expanduser(qpath)
    notable = [e for e in events if _notable(e)]
    enq = 0
    qcon = sqlite3.connect(qpath, timeout=30)
    try:
        for e in notable:
            cur = qcon.execute(
                "INSERT OR IGNORE INTO queue_items"
                "(created_at_unix, source, kind, topic_key, dedupe_key, payload_json)"
                " VALUES (?,?,?,?,?,?)",
                (int(time.time()), "smart_money_daemon", "positioning_event",
                 e.get("ticker") or "unknown", e["event_id"], json.dumps(e)))
            enq += cur.rowcount
        qcon.commit()
    finally:
        qcon.close()
    return {"queue": "present", "path": qpath, "enqueued": enq,
            "notable_total": len(notable),
            "note": "notable events enqueued idempotently by event_id"}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Smart money delta-scan")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--raw", default="data/raw")
    args = ap.parse_args(argv)

    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT in .env", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    raw_dir = pathlib.Path(args.raw) / "house"
    raw_dir.mkdir(parents=True, exist_ok=True)

    envelope, events = run_scan(con, contact, raw_dir)
    envelope["queue"] = _enqueue(envelope, events)

    scans_dir = pathlib.Path(dbmod.SCANS_DIR)
    scans_dir.mkdir(parents=True, exist_ok=True)
    out = scans_dir / "{}.json".format(envelope["scan_id"])
    out.write_text(json.dumps(envelope, indent=2))
    print("[scan] {} events={} -> {}".format(
        envelope["scan_id"], len(events), out))
    for s in envelope["sources"]:
        print("  [{}] {} {}".format(s["status"], s["source"], s["note"]))

    # Exit spine: all sources failed => 1; anything else => 0.
    statuses = [s["status"] for s in envelope["sources"]]
    if statuses and all(st == "DEGRADED" for st in statuses):
        print("[scan] ALL sources degraded", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
