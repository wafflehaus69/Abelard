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
from . import form4, form4_universal, thirteenf, watermarks
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

    # Senate: search-driven PTR delta. UN-DEGRADED 2026-07-30 — the eFD data endpoint is
    # reachable again via post_data + X-CSRFToken (recon/EFD_WAF_FINDING.md superseded), so
    # this now ENUMERATES recent PTRs and ingests new uuids (resume-safe via
    # ingested_filings), instead of just probing. A 503 / any failure reverts to DEGRADED,
    # never fakes.
    try:
        import pathlib as _pl
        from . import efd_ingest
        efd_raw = _pl.Path(efd_ingest.RAW_DIR_DEFAULT)
        efd_raw.mkdir(parents=True, exist_ok=True)
        s = bootstrap(ua, probe=False)
        since = (dt.date.today() - dt.timedelta(days=45)).strftime("%m/%d/%Y")
        new_sen, start = 0, 0
        while True:
            body = post_data(s, {
                "draw": "1", "start": str(start), "length": "100",
                "report_types": "[11]", "filer_types": "[]", "first_name": "",
                "last_name": "", "submitted_start_date": "{} 00:00:00".format(since),
                "submitted_end_date": "", "candidate_state": "", "senator_state": "",
                "office_id": "", "order[0][column]": "4", "order[0][dir]": "desc"})
            data = body.get("data") or []
            for row in data:
                filing = efd_ingest.parse_search_row(row)
                if efd_ingest.ingest_filing(con, s, filing, efd_raw) == "electronic":
                    new_sen += 1
            start += 100
            if start >= body.get("recordsTotal", 0) or not data:
                break
        counts["senate_new_filings"] = new_sen
        sources.append(_src("senate_efd", "OK", "search delta", new_sen))
    except EfdSessionError as exc:
        sources.append(_src("senate_efd", "DEGRADED",
                            "search endpoint {} - see EFD_WAF_FINDING".format(str(exc)[:50])))
    except Exception as exc:  # noqa: BLE001 - any other failure -> DEGRADED, never fake
        sources.append(_src("senate_efd", "DEGRADED", "senate delta {}".format(str(exc)[:60])))

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

    # SM-C2 P3: refresh party/state for FD filer identities. Keyless public roster,
    # idempotent, resolves DETERMINISTICALLY only — a failure here must never take the
    # congress leg down, it just leaves party as it was (or unknown).
    try:
        from . import roster
        rt = roster.sync(con)
        counts["roster_resolved"] = rt["unique"] + rt["byname"]
        counts["roster_unmatched"] = rt["unmatched"]
    except Exception as exc:  # noqa: BLE001 - degraded, surfaced in counts
        counts["roster_error"] = str(exc)[:80]
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
                # SM-O1 P1: Table II persisted forward from the same filing.
                form4.persist_derivatives(con, accession, parsed, ticker, row["date"])
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


def leg_universal_ingest(con, contact):
    """SM-U1 universal Form 4 corpus ingest. Discovery-mode: ingests ALL Form 4s
    (every transaction code) tagged ingest_regime='universal', compounding the
    unbiased corpus that makes the g2 counter honest and feeds SM-R1 sell-
    baselines / ownership pressure. INGEST-ONLY BY DESIGN: returns ZERO events
    and enqueues nothing. One blind PH4 validation pass (U1_DISCOVERY) showed no
    independent cluster edge once calendar-clustering is accounted for, so
    alerting on clusters would manufacture signal; the joins thesis waits on
    accrued corpus, not on alerts. Bounded and resume-safe via the per-day
    watermark. Returns (source_status, counts) — NO event list."""
    counts = {"days": 0, "filings": 0, "rows": 0, "parse_fail": 0}
    try:
        tot = form4_universal.ingest_recent(con, contact)
        counts = {"days": tot["days"], "filings": tot["form4"],
                  "rows": tot["persisted"], "parse_fail": tot["parse_fail"]}
        note = "days={days} filings={filings} rows={rows} parsefail={parse_fail}".format(
            **counts)
        return _src("edgar_form4_universal", "OK", note, counts["rows"]), counts
    except Exception as exc:  # noqa: BLE001 - fail-loud into source status
        return _src("edgar_form4_universal", "DEGRADED", str(exc)[:120]), counts


def leg_enrich(con, contact):
    """SM-O1/SM-R1 nightly enrichment leg. Refreshes the EOD price series for every
    ticker traded (P/S) in the last 90 days (~1k names, so the actively-traded corpus
    stays current), and computes market_cap SMID bands for the narrower scoped set
    (overlay + network issuers + >=2-buyer 90d names). Ingest-only, NO events. Prices
    use the crumb-free v8 chart endpoint; bands use SEC companyconcept. First run
    fetches full series; later runs are span-cached (each night = one delta call each)."""
    from . import queries as qmod, marketcap, prices
    _NON = {"NONE", "N/A", "N.A.", "NA", ""}
    tickers = set(qmod._scoped_tickers())
    # Plus the active-convergence universe names (>=2 distinct open-market buyers
    # in the trailing 90d) so the scope=all trades feed is covered where multi-
    # buyer signal actually lives. Bounded (a few hundred); single-buyer noise
    # names are left un-priced by design.
    start90 = (dt.date.today() - dt.timedelta(days=90)).isoformat()
    for (tk,) in con.execute(
        "SELECT ticker FROM form4_transactions WHERE code='P' AND plan_flag=0 "
        "AND ticker IS NOT NULL AND substr(tx_date,1,10)>=? GROUP BY ticker "
        "HAVING COUNT(DISTINCT reporting_cik)>=2", (start90,)):
        t = (tk or "").upper().strip()
        if t and t not in _NON:
            tickers.add(t)
    tickers = sorted(tickers)
    # Every ticker traded (P/S) in the last 90 days gets its LATEST close refreshed
    # nightly (~1k names, bounded cost), so the actively-traded slice of the backfilled
    # corpus stays current instead of going stale (Mando ruling 2026-07-30: 90d window).
    # Bands stay on the narrow set only — EDGAR companyconcept is costly and
    # shares-outstanding is stable, so widening bands too would add a heavy sweep for
    # no signal. Dormant names keep their backfilled close (re-run price_backfill to top up).
    price_tickers = set(tickers)
    for (tk,) in con.execute(
        "SELECT DISTINCT UPPER(ticker) FROM form4_transactions WHERE code IN ('P','S') "
        "AND ticker IS NOT NULL AND substr(tx_date,1,10)>=?", (start90,)):
        t = (tk or "").upper().strip()
        if t and t not in _NON and "/" not in t and " " not in t:  # invalid Yahoo symbols
            price_tickers.add(t)
    price_tickers = sorted(price_tickers)
    counts = {"tickers": len(tickers), "price_tickers": len(price_tickers),
              "bands": 0, "price_ok": 0, "price_fail": 0}
    if not price_tickers:
        return _src("enrich_scoped", "OK", "no tickers", 0), counts
    try:
        # Compute only MISSING bands (shares outstanding is stable) so later
        # nightly runs do not re-hit EDGAR for the whole set.
        need = [t for t in tickers if t not in marketcap.bands_for(con, tickers)]
        if need:
            marketcap.compute(con, need, contact)
        counts["bands"] = len(marketcap.bands_for(con, tickers))
    except Exception as exc:  # noqa: BLE001 - fail-loud into source status
        return _src("enrich_scoped", "DEGRADED", "marketcap " + str(exc)[:100]), counts
    end = dt.date.today().isoformat()
    start = (dt.date.today() - dt.timedelta(days=400)).isoformat()
    for tk in price_tickers:
        try:
            prices.eod(con, tk, start, end)
            counts["price_ok"] += 1
        except Exception:  # noqa: BLE001 - count, never guess
            counts["price_fail"] += 1
    return _src("enrich_scoped", "OK",
                "tickers={} price_tickers={} bands={} price_ok={} price_fail={}".format(
                    counts["tickers"], counts["price_tickers"], counts["bands"],
                    counts["price_ok"], counts["price_fail"]), counts["price_ok"]), counts


def leg_congress_annual(con, contact, raw_dir, lookback_days=120):
    """Annual FD HOLDINGS refresh, both chambers. Ingest-only, NO events.

    `leg_congress` covers PTR *trades*; this covers the annual *holdings snapshot* that
    /congress, the member books and the Phase F fusion anchor are built on. Without it the
    holdings corpus silently ages a full year, because annual FDs land once a cycle
    (~May) and nothing else refetches them.

    CHEAP IN STEADY STATE by construction: both ingests check congress_fd_seen BEFORE
    fetching a document, so a night with no new filings costs one House index read plus a
    short Senate search — no PDF fetches, no parsing. Only genuinely new DocIDs do work.

    Degrades per chamber: eFD is WAF-fronted and the House zip can 404 early in a cycle,
    so each side is caught independently and reported rather than taking the scan down.
    """
    import datetime as _dt

    sources = []
    counts = {"house_new": 0, "house_rows": 0, "senate_new": 0, "senate_rows": 0}
    ua = UA_TMPL.format(contact)
    year = _dt.date.today().year
    # Current year AND prior year: amendments to the prior cycle keep arriving, and early
    # in a calendar year the current zip may not exist yet.
    try:
        from . import house_fd_ingest as hfd
        unparsed = pathlib.Path(hfd.UNPARSED_DIR)
        hraw = pathlib.Path(hfd.RAW_DIR_DEFAULT)
        hraw.mkdir(parents=True, exist_ok=True)
        for y in (year, year - 1):
            # max_age_days=1 so the CURRENT cycle's zip is re-read daily; without it the
            # disk cache would hide every filing added after the first download.
            idx = hfd.fetch_year_index(y, hraw, ua, max_age_days=1)
            if not idx:
                continue
            for filing in idx:
                res = hfd.ingest_filing(con, y, filing, hraw, ua, unparsed)
                if res["status"] != "seen":
                    counts["house_new"] += 1
                    counts["house_rows"] += res["rows"]
        sources.append(_src("congress_annual:house", "OK", "new={} rows={}".format(
            counts["house_new"], counts["house_rows"]), counts["house_rows"]))
    except Exception as exc:  # noqa: BLE001 - per chamber, never abort the scan
        sources.append(_src("congress_annual:house", "DEGRADED", str(exc)[:120]))
    try:
        from . import senate_fd_ingest as sfd
        sess = bootstrap(UA_TMPL.format(contact), probe=False)
        since = (_dt.date.today()
                 - _dt.timedelta(days=lookback_days)).strftime("%m/%d/%Y")
        for row in sfd.search_annual(sess, since):
            res = sfd.ingest_report(con, sess, row)
            if res["status"] != "seen":
                counts["senate_new"] += 1
                counts["senate_rows"] += res["rows"]
        sources.append(_src("congress_annual:senate", "OK", "new={} rows={}".format(
            counts["senate_new"], counts["senate_rows"]), counts["senate_rows"]))
    except Exception as exc:  # noqa: BLE001 - eFD is WAF-fronted; degrade, do not abort
        sources.append(_src("congress_annual:senate", "DEGRADED", str(exc)[:120]))
    return sources, counts


def leg_13f_holdings(con, contact, quarters=8):
    """SM-P1: durable per-holding 13F ingest into thirteenf_holdings, so the reported
    portfolios view auto-refreshes when new 13Fs land (Leg C only refreshes the JSON
    thirteenf_baseline used for diffing, NOT this table). Idempotent via
    thirteenf_filings_seen — steady-state nightly is no new filings = ~no work; only a
    fresh quarterly filing does EDGAR + OpenFIGI work. Ingest-only, NO events."""
    from . import thirteenf_ingest
    report = {"filers": {}}
    for cik10 in thirteenf_ingest.CONFIRMED:
        try:
            thirteenf_ingest.ingest_filer(con, cik10, contact, quarters, report)
        except Exception as exc:  # noqa: BLE001 - per filer, fail into source status
            report["filers"][cik10] = {"error": str(exc)[:120]}
    vals = [f for f in report["filers"].values() if isinstance(f, dict)]
    new = sum(f.get("new", 0) for f in vals)
    rows = sum(f.get("holding_rows", 0) for f in vals)
    errs = [c for c, f in report["filers"].items() if f.get("error")]
    status = "DEGRADED" if errs and not rows else "OK"
    return _src("13f_holdings", status, "filers={} new_filings={} rows={} errors={}".format(
        len(report["filers"]), new, rows, len(errs)), rows), report


def run_scan(con, contact, raw_dir, skip_universal=False):
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
    # Fourth leg: universal corpus ingest. Ingest-only — contributes a source
    # status and counts but NO events, so it can never reach the decision queue.
    if skip_universal:
        src_u, cnt_u = _src("edgar_form4_universal", "SKIPPED", "disabled"), {}
        src_e, cnt_e = _src("enrich_scoped", "SKIPPED", "disabled"), {}
        src_h = _src("13f_holdings", "SKIPPED", "disabled")
        src_ca, cnt_ca = [_src("congress_annual", "SKIPPED", "disabled")], {}
    else:
        src_u, cnt_u = leg_universal_ingest(con, contact)
        src_e, cnt_e = leg_enrich(con, contact)  # bounded scoped bands+prices
        src_h, _ = leg_13f_holdings(con, contact)  # SM-P1 durable 13F holdings ingest
        # SM-C3: annual FD HOLDINGS refresh (both chambers). Ingest-only, no events —
        # without it the holdings corpus behind /congress and the Phase F anchor ages a
        # full year, since annual FDs land once a cycle and nothing else refetches them.
        src_ca, cnt_ca = leg_congress_annual(con, contact, raw_dir)

    sources = src_a + src_b + src_c + src_ca + [src_u, src_e, src_h]
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
                   "universal_ingest": cnt_u, "enrich": cnt_e,
                   "congress_annual": cnt_ca,
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
    ap.add_argument("--skip-universal", action="store_true",
                    help="skip the universal corpus ingest leg (dev/ad-hoc runs "
                         "where the ~1200-filing walk is not wanted)")
    args = ap.parse_args(argv)

    env = load_env()
    contact = env.get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT in .env", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    raw_dir = pathlib.Path(args.raw) / "house"
    raw_dir.mkdir(parents=True, exist_ok=True)

    envelope, events = run_scan(con, contact, raw_dir,
                                skip_universal=args.skip_universal)
    envelope["queue"] = _enqueue(envelope, events)

    scans_dir = pathlib.Path(dbmod.SCANS_DIR)
    scans_dir.mkdir(parents=True, exist_ok=True)
    out = scans_dir / "{}.json".format(envelope["scan_id"])
    out.write_text(json.dumps(envelope, indent=2))
    print("[scan] {} events={} -> {}".format(
        envelope["scan_id"], len(events), out))
    for s in envelope["sources"]:
        print("  [{}] {} {}".format(s["status"], s["source"], s["note"]))

    # Exit spine: all SIGNAL sources failed => 1; anything else => 0. The
    # universal ingest is a collection leg, not a signal source — its status is
    # logged but excluded here so an OK ingest can never mask all signal legs
    # being down, nor a degraded ingest trip a false alarm.
    statuses = [s["status"] for s in envelope["sources"]
                if s["source"] not in ("edgar_form4_universal", "enrich_scoped")]
    if statuses and all(st == "DEGRADED" for st in statuses):
        print("[scan] ALL signal sources degraded", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
