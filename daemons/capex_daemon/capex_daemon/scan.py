"""CD-OPS1 — the nightly scan. Freshness-driven, idempotent, zero LLM calls.

Most nights nothing has been filed and this is a no-op that says so and exits 0.
That is the design point, not a degenerate case: a scan that rebuilds everything
every night would burn EDGAR requests to produce identical artifacts, and would
make a genuine change indistinguishable from routine churn in the logs.

The cycle:

  1. Resolve identity for every universe member (CIK-keyed, renames detected
     across scans — E10/R3).
  2. Ask each issuer's submissions index for its newest periodic filing and
     compare against the stored watermark. Only issuers with something NEW are
     "affected".
  3. Refresh facts, panel, reconciliation and divergence for affected issuers
     only.
  4. Regenerate chart artifacts ONLY if the panel actually changed.
  5. Advance watermarks only on success-with-items; a zero-item or failed run
     leaves them exactly where they were (E12).

Watermarks are per-issuer, keyed `scan:<cik10>`, and hold the newest *filing
date* ingested for that issuer — a content-derived value, never `now()`. That is
what makes the run idempotent: a second run the same night finds the same
newest filing, sees no advance, and does nothing.
"""
import json
import os
import time

from . import (alerts as alertmod, brief, config, divergence, edgar, facts_api,
               freshness, identity, phases, snapshot, storage, suppliers, tagmap,
               universe)

WATERMARK_PREFIX = "scan:"

OUTCOME_NOOP = "no-op"
OUTCOME_UPDATED = "updated"
OUTCOME_ERROR = "error"


LAST_SCAN_KEY = "last_scan_unix"


def record_scan_completed(con, now_unix):
    """Stamp that a scan RAN, separately from whether it changed anything.

    These are different facts and the dashboard needs the first one. Most nights
    are no-ops by design, so the snapshot legitimately keeps an old
    generated_unix while the daemon is perfectly healthy — measured live, two
    consecutive clean no-ops left the snapshot 54h old and a staleness banner
    claiming the nightly had not completed, when it had, twice.
    """
    con.execute("INSERT OR REPLACE INTO meta_kv(key, value) VALUES (?,?)",
                (LAST_SCAN_KEY, str(int(now_unix))))
    con.commit()


def read_last_scan(con):
    row = con.execute("SELECT value FROM meta_kv WHERE key=?", (LAST_SCAN_KEY,)).fetchone()
    try:
        return int(row[0]) if row else None
    except (TypeError, ValueError):
        return None


def _wm_key(cik):
    return "{}{}".format(WATERMARK_PREFIX, cik)


def read_watermark(con, cik):
    row = con.execute("SELECT value FROM watermarks WHERE key=?", (_wm_key(cik),)).fetchone()
    return row[0] if row else None


def write_watermark(con, cik, value, now_unix=None):
    """Advance a watermark. Refuses to move backwards or to a null (E12)."""
    if not value:
        return False
    current = read_watermark(con, cik)
    if current and value <= current:
        return False
    con.execute(
        "INSERT OR REPLACE INTO watermarks(key, value, updated_unix) VALUES (?,?,?)",
        (_wm_key(cik), value, int(now_unix if now_unix is not None else time.time())))
    con.commit()
    return True


class IssuerScan:
    __slots__ = ("cik", "ticker", "status", "newest_filing", "watermark", "detail",
                 "submissions_doc")

    def __init__(self, cik, ticker, status, newest_filing, watermark, detail,
                 submissions_doc=None):
        self.cik = cik
        self.ticker = ticker
        self.status = status
        self.newest_filing = newest_filing
        self.watermark = watermark
        self.detail = detail
        # Kept so the supplier harvest can reuse the document this check already
        # paid for, instead of asking EDGAR for it a second time.
        self.submissions_doc = submissions_doc

    @property
    def is_affected(self):
        return self.status == "new-filing"

    def __repr__(self):
        return "IssuerScan({} {} newest={} wm={})".format(
            self.ticker, self.status, self.newest_filing, self.watermark)


def _cq_of(period_end):
    """'2026-06-30' -> '2026Q2'. Calendar quarter of a period end."""
    try:
        y, m, _d = str(period_end).split("-")
        return "{}Q{}".format(int(y), (int(m) - 1) // 3 + 1)
    except (AttributeError, ValueError):
        return None


def _panel_is_behind(covered, cik, filing):
    """Does the published panel lack the newest filed period?

    `covered` maps cik -> the newest calendar quarter the panel holds for that
    issuer. Absent or empty, this answers False and the watermark keeps its old
    meaning — a fresh database must not look like 35 ingest gaps.
    """
    if not covered or not filing or not filing.report_date:
        return False
    want = _cq_of(filing.report_date)
    have = covered.get(cik)
    return bool(want and have and have < want)


def _covered_quarters(con, roster):
    """cik -> newest calendar quarter the published panel holds for that issuer.

    Read from the snapshot rather than recomputed: the snapshot is what a reader
    sees, so it is the right authority on whether a period actually arrived. A
    database with no snapshot yet returns {} and every watermark keeps its
    original meaning.
    """
    try:
        snap = snapshot.load(con)
    except Exception:
        return {}
    if not snap:
        return {}
    by_ticker = {}
    for tick, iss in (snap.get("issuers") or {}).items():
        qs = iss.get("quarters") or []
        if qs:
            by_ticker[tick] = max(q["q"] for q in qs)
    return {cik: by_ticker[e.ticker_display]
            for cik, e in roster.items() if e.ticker_display in by_ticker}


def check_issuer(con, entity, http=None, submissions_doc=None, covered=None):
    """Is there a periodic filing newer than this issuer's watermark?

    `submissions_doc` may be injected for tests; otherwise it is fetched.
    `covered` maps cik -> newest calendar quarter already in the panel, which is
    what distinguishes "handled" from "attempted" when the watermark disagrees.
    """
    try:
        doc = submissions_doc if submissions_doc is not None else edgar.fetch_submissions(
            entity.cik, http)
    except Exception as exc:
        return IssuerScan(entity.cik, entity.ticker_display, OUTCOME_ERROR, None,
                          read_watermark(con, entity.cik),
                          "submissions fetch failed: {}".format(exc))

    # NOT named `snapshot` — that is the view-model module imported above, and
    # shadowing it here is a trap for whoever next needs it in this function.
    ident = identity.from_snapshot_doc(doc) if hasattr(identity, "from_snapshot_doc") \
        else identity.from_submissions(doc)
    events = identity.record(con, ident, entity.bucket)

    filing = freshness.latest_periodic_filing(doc)
    wm = read_watermark(con, entity.cik)
    if filing is None:
        return IssuerScan(entity.cik, entity.ticker_display, "no-periodic", None, wm,
                          "no 10-K or 10-Q in the submissions index",
                          submissions_doc=doc)
    if wm and filing.filing_date <= wm and not _panel_is_behind(covered, entity.cik, filing):
        return IssuerScan(entity.cik, entity.ticker_display, "current",
                          filing.filing_date, wm, "nothing newer than watermark",
                          submissions_doc=doc)
    if wm and filing.filing_date <= wm:
        # Self-healing: the watermark says this filing was handled, the panel
        # says its period never arrived. Trust the panel — the watermark is a
        # record of an attempt and the panel is a record of the result.
        return IssuerScan(entity.cik, entity.ticker_display, "new-filing",
                          filing.filing_date, wm,
                          "INGEST-GAP retry: {} period {} filed {} is on the index "
                          "but absent from the panel".format(
                              filing.form, filing.report_date, filing.filing_date),
                          submissions_doc=doc)
    detail = "new filing {} {} (period {})".format(
        filing.form, filing.filing_date, filing.report_date)
    if events:
        detail += "; identity events: {}".format(", ".join(e.field for e in events))
    return IssuerScan(entity.cik, entity.ticker_display, "new-filing",
                      filing.filing_date, wm, detail, submissions_doc=doc)


def _fill_gaps(roster, indexed, subs_by_cik, http=None):
    """Run the filing-instance fallback across the panel. Returns {cik: Fill}.

    Operates on the index the snapshot is built from, in place. An issuer whose
    capex concept is REFUSED is skipped: its gap is a resolution question
    (RIOT: CAPEX-UNRESOLVED, already published as coverage), not a late API, and
    treating it as an ingest gap would re-fetch its filing every night forever
    for a hole no fetch can fill.
    """
    out = {}
    for cik, ix in (indexed or {}).items():
        sub = (subs_by_cik or {}).get(cik)
        if sub is None or cik not in roster:
            continue
        res = tagmap.resolve(ix, tagmap.CAPEX)
        if res.is_unresolved or not res.current_concept:
            continue
        try:
            out[cik] = freshness.fill_from_filing(
                cik, sub, ix, concept=res.current_concept, http=http)
        except Exception as exc:            # a fill must never fail a scan
            out[cik] = freshness.Fill(freshness.FILL_FAILED,
                                      detail="fill raised: {}".format(exc))
    return out


def refresh_issuer(con, entity, http=None, facts_doc=None, submissions_doc=None,
                   indexed=None):
    """Re-derive one issuer's view. Returns (IssuerView, indexed facts, Fill).

    **The fallback runs here, and until 2026-09-02 it ran nowhere.** `freshness`
    was written for the case where a periodic filing exists and companyfacts has
    not published its period yet — documented, tested, and called by nothing
    outside its own module. Measured: DLR and AMT both filed 2026Q2 in late July
    and companyfacts still lacked the period on 2026-09-02, so both were absent
    from the panel and rendered as "behind on filing".

    `submissions_doc` is optional only so existing callers keep working; without
    it there is no filing to fall back to and the API is trusted as before.
    """
    if indexed is None:
        doc = (facts_doc if facts_doc is not None
               else edgar.fetch_companyfacts(entity.cik, http))
        indexed = facts_api.index_facts(doc)
    fill = None
    if submissions_doc is not None:
        res = tagmap.resolve(indexed, tagmap.CAPEX)
        # An issuer whose capex concept is REFUSED has no period that could
        # arrive — its gap is a resolution question (RIOT: CAPEX-UNRESOLVED,
        # already published as coverage), not a late API. Calling that an ingest
        # gap would hold its watermark open and re-fetch its filing every night
        # forever, for a hole no fetch can fill.
        if not res.is_unresolved and res.current_concept:
            fill = freshness.fill_from_filing(entity.cik, submissions_doc, indexed,
                                              concept=res.current_concept, http=http)
    return divergence.build_issuer_view(entity, indexed), indexed, fill


def run(con=None, roster=None, http=None, render=True, outdir=None, now_unix=None,
        submissions_by_cik=None, facts_by_cik=None, rebuild=False,
        queue_path=None, fallback=True):
    """One scan cycle. Returns a summary dict; never raises on a single issuer.

    Injection points (`submissions_by_cik`, `facts_by_cik`) exist so the whole
    cycle is testable without network — idempotency is a property worth testing
    deterministically rather than against a live index.

    `rebuild=True` forces the full path even when nothing was filed. The snapshot
    is derived from code as much as from data, so a code change can leave a
    correct database behind a stale published view with no filing due for weeks
    to dislodge it. Watermarks are untouched by this: it recomputes, it does not
    re-ingest.
    """
    con = con if con is not None else storage.connect()
    roster = roster if roster is not None else universe.load()
    started = int(now_unix if now_unix is not None else time.time())

    # What the PUBLISHED panel already holds, per issuer. The watermark records
    # that a filing was attempted; this records that its period arrived. When
    # the two disagree the panel wins — see `_panel_is_behind`.
    covered = _covered_quarters(con, roster)

    checks, errors = [], []
    for cik, entity in sorted(roster.items()):
        sub = (submissions_by_cik or {}).get(cik)
        c = check_issuer(con, entity, http=http, submissions_doc=sub, covered=covered)
        checks.append(c)
        if c.status == OUTCOME_ERROR:
            errors.append((c.ticker, c.detail))

    # The supplier harvest is NOT gated on the capex freshness check. Its
    # idempotency comes from its own per-instance cache, not from watermarks,
    # and gating it here stranded four of five suppliers permanently: the first
    # live run advanced every watermark while the harvest was failing, after
    # which no issuer was ever "affected" again and the harvest was never
    # reached. It costs nothing on a quiet night — the submissions documents are
    # reused from the checks above, and instances already cached are not fetched.
    subs_seen = {c.cik: c.submissions_doc for c in checks if c.submissions_doc}
    subs_seen.update(submissions_by_cik or {})
    supplier_legs, harvested = _harvest_suppliers(roster, con, http, subs_seen, errors)

    record_scan_completed(con, started)

    affected = [c for c in checks if c.is_affected]
    # A run is a no-op only when NOTHING changed. Newly harvested supplier
    # instances are a change, and the published snapshot is what readers see, so
    # a harvest that lands on an otherwise quiet night still refreshes it. In
    # steady state this cannot fire — a supplier that files is itself affected —
    # so the cost of rebuilding is paid only when there is genuinely new data.
    if not affected and not harvested and not rebuild:
        return {
            "outcome": OUTCOME_NOOP,
            "checked": len(checks),
            "affected": 0,
            "errors": errors,
            "artifacts_written": False,
            "supplier_instances_harvested": 0,
            "summary": "{} issuers checked, none with a new filing since watermark".format(
                len(checks)),
            "started_unix": started,
        }

    # The snapshot is the single published view-model — dashboard, PDF and
    # alerts all read it and none recomputes (P4). It is built BEFORE the
    # refresh loop because the fallback fill has to land in THIS index.
    #
    # The first cut of the ingest fix filled a private copy inside
    # refresh_issuer, whose only consumer is `views`. Measured on Basilic: the
    # run reported "updated: 2 of 35 — AMT, DLR" and published a snapshot still
    # sitting on their previous quarter, because the index the snapshot is
    # built from was fetched separately and never saw the fill. Filling the
    # right object is the whole fix.
    indexed = _indexed_all(roster, http, facts_by_cik, errors=errors)
    fills = _fill_gaps(roster, indexed, subs_seen if fallback else {}, http=http)

    views, refreshed, ingest_gaps = [], [], []
    for c in affected:
        entity = roster[c.cik]
        fill = fills.get(c.cik)
        ix = indexed.get(c.cik)
        if ix is None:
            # `_indexed_all` already recorded why it could not be fetched. An
            # issuer with no index was not refreshed, so its watermark must not
            # move — and it must NOT quietly fall through to a second network
            # fetch here, which would turn an injected failure into a live call.
            continue
        try:
            # Reuse the filled index rather than re-fetching companyfacts for
            # an issuer `_indexed_all` has already fetched.
            view, _ix, _f = refresh_issuer(con, entity, http=http, indexed=ix)
        except Exception as exc:
            errors.append((c.ticker, "refresh failed: {}".format(exc)))
            continue
        views.append(view)
        # A refresh that did not reach the filed period has NOT ingested it.
        # Advancing the watermark on that is what turned a temporary API lag
        # into a permanent hole: the gate closes, the issuer is never
        # "affected" again, and no later scan retries. DLR sat like that from
        # 2026-08-22 with a 2026Q2 filed on 07-31 and would have stayed dark
        # until its Q3 filing in late October.
        if fill is not None and fill.status in (freshness.FILL_EMPTY,
                                                freshness.FILL_FAILED):
            # A finding, NOT a run failure. `cmd_scan` exits non-zero on
            # `errors`, so filing an ingest gap there would make the nightly
            # alert every night for a condition that is already being retried
            # and reported. It gets its own key and its own line.
            ingest_gaps.append({
                "ticker": c.ticker, "status": fill.status, "period": fill.period,
                "form": fill.filing.form if fill.filing else None,
                "accession": fill.filing.accession if fill.filing else None,
                "detail": fill.detail})
            continue                      # watermark stays put; retry tomorrow
        refreshed.append(c)

    # Watermarks advance ONLY for issuers whose refresh actually INGESTED the
    # filed period — sighting a filing is not the same fact as holding it.
    advanced = []
    for c in refreshed:
        if write_watermark(con, c.cik, c.newest_filing, now_unix=started):
            advanced.append(c.ticker)

    # CD-3 supplier leg. Parser-only, so it is harvested per FILING rather than
    # per fact: only instances absent from the cache are fetched, which keeps a
    # quiet night to one submissions request per supplier.
    snap = snapshot.build(roster, indexed, now_unix=started,
                          supplier_legs=supplier_legs)
    prior = {r[0] for r in con.execute("SELECT event_key FROM phase_events")}
    first_run = not prior
    alerts = snapshot.alert_lines(snap, prior_keys=prior)
    # Record EVERY transition the snapshot knows about — issuer, bucket AND
    # total-panel. Recording only issuer transitions would leave aggregate ones
    # permanently absent from phase_events, so they would re-alert on every scan
    # forever.
    all_trans = [phases.Transition(x["series_key"], x["quarter"], x["from_state"],
                                   x["to_state"], x["yoy"], x["delta"])
                 for x in snap["transitions"]]
    phases.record_transitions(con, all_trans, now_unix=started)
    snapshot.save(con, snap)
    if first_run:
        # A first run rediscovers the entire history at once. That is a backfill,
        # not news — it is recorded so later runs are quiet, and reported as a
        # count rather than blasted into the alert bar.
        alerts = []

    # Durability, not dispatch (E28). Whatever survived the frontier gate is
    # handed to the shared queue; Abelard decides what becomes a push. An
    # enqueue failure is an error on the run, never a silent drop — an alert
    # that vanished between derivation and the queue is the one failure this
    # whole path exists to prevent.
    enqueued = duplicates = 0
    try:
        enqueued, duplicates = alertmod.enqueue_alerts(alerts, queue_path=queue_path)
    except Exception as exc:
        errors.append(("alert-queue", "enqueue failed: {}".format(exc)))

    # The nightly artifact is the PDF phase page, drawn from the same model the
    # dashboard renders (brief.py). The matplotlib PNG pipeline it replaced was
    # retired 2026-08-21: nothing consumed its four PNGs or cd2_thesis_layer.pdf,
    # and it hard-imported an UNDECLARED matplotlib at module scope, so a clean
    # Basilic venv could not import `scan` at all.
    # Reaching here means SOMETHING changed — a filing, a supplier instance, or
    # an explicit rebuild — because a true no-op returned above. Gating the
    # artifact on `views` instead meant --rebuild republished the snapshot but
    # left the PDF on disk stale, which is the failure mode --rebuild exists to
    # prevent.
    artifacts = False
    if render:
        outdir = outdir or config.artifact_path("", sub="charts")
        os.makedirs(outdir, exist_ok=True)
        try:
            brief.phase_page(snap, os.path.join(outdir, "capex_phase_page.pdf"))
            artifacts = True
        except Exception as exc:
            errors.append(("brief", "phase page render failed: {}".format(exc)))

    return {
        "outcome": OUTCOME_UPDATED,
        "checked": len(checks),
        "affected": len(affected),
        "refreshed": [c.ticker for c in refreshed],
        "watermarks_advanced": advanced,
        # Findings, not failures: a gap is already being retried and reported,
        # and putting it in `errors` would exit the nightly non-zero forever.
        "ingest_gaps": ingest_gaps,
        "errors": errors,
        "artifacts_written": artifacts,
        "alerts": alerts,
        "alerts_enqueued": enqueued,
        "alerts_duplicate": duplicates,
        "transitions_recorded": len(all_trans),
        "first_run_backfill": first_run,
        "phase_states": {k: v["state"] for k, v in snap["issuers"].items()},
        "supplier_instances_harvested": harvested,
        "summary": ("{} of {} issuers had new filings: {}".format(
            len(affected), len(checks), ", ".join(c.ticker for c in affected))
            if affected else
            "no new filings; rebuilt the snapshot{}".format(
                " for {} newly harvested supplier instance(s)".format(harvested)
                if harvested else " on request")),
        "started_unix": started,
    }


def _harvest_suppliers(roster, con, http, submissions_by_cik, errors):
    """Refresh and rebuild every supplier leg. Returns ({ticker: leg}, added)."""
    legs, added = {}, 0
    for cik, entity in sorted(roster.items()):
        if entity.bucket != suppliers.SUPPLIER_BUCKET:
            continue
        try:
            n, failures = suppliers.harvest(
                entity, con, http=http,
                submissions_doc=(submissions_by_cik or {}).get(cik))
            added += n
            for key, detail in failures:
                errors.append((entity.ticker_display,
                               "supplier instance {} unusable: {}".format(key, detail)))
        except Exception as exc:
            errors.append((entity.ticker_display, "supplier harvest failed: {}".format(exc)))
        try:
            legs[entity.ticker_display] = suppliers.leg_from_db(entity, con)
        except Exception as exc:
            errors.append((entity.ticker_display, "supplier leg failed: {}".format(exc)))
    return legs, added


def _indexed_all(roster, http, facts_by_cik, errors=None):
    """Indexed facts for every roster member — the snapshot needs the whole panel.

    A fetch that fails must be LOUD. An issuer missing from `indexed` silently
    drops out of the panel, which moves matched membership and every bucket sum
    it belongs to — a membership change caused by a 503, indistinguishable on
    the dashboard from a company that stopped filing. So failures land in the
    scan's `errors` and the nightly line reports them.
    """
    out = {}
    for cik, entity in sorted(roster.items()):
        try:
            doc = (facts_by_cik or {}).get(cik) or edgar.fetch_companyfacts(cik, http)
            out[cik] = facts_api.index_facts(doc)
        except Exception as exc:
            if errors is not None:
                errors.append((entity.ticker_display,
                               "companyfacts unavailable, dropped from the panel "
                               "for this scan: {}".format(exc)))
    return out


def _view_of(roster, indexed, cik):
    e = roster.get(cik)
    ix = indexed.get(cik)
    if not e or ix is None:
        return None
    try:
        return divergence.build_issuer_view(e, ix)
    except Exception:
        return None


def _all_views(con, roster, http, facts_by_cik):
    """Full-panel views for artifact regeneration.

    Charts are a whole-panel product — a bucket subtotal and its concentration
    share cannot be computed from the changed issuers alone (E14).
    """
    out = []
    for cik, entity in sorted(roster.items()):
        try:
            doc = (facts_by_cik or {}).get(cik) or edgar.fetch_companyfacts(cik, http)
            out.append(divergence.build_issuer_view(entity, facts_api.index_facts(doc)))
        except Exception:
            continue
    return out


def format_summary(result):
    """One line for the nightly log. Loud on error, quiet on a no-op."""
    base = "[capex-scan] {}: {}".format(result["outcome"], result["summary"])
    if result.get("first_run_backfill"):
        base += " | first run: {} transitions backfilled, none alerted".format(
            result.get("transitions_recorded", 0))
    elif result.get("alerts"):
        base += " | {} enqueued".format(result.get("alerts_enqueued", 0))
        base += " | TRANSITIONS: {}".format("; ".join(
            "{} {} {}->{} ({})".format(a["series_key"], a["quarter"], a["from_state"],
                                       a["to_state"], a["reason"])
            for a in result["alerts"][:6]))
    for g in result.get("ingest_gaps") or []:
        base += (" | INGEST-GAP {}: {} period {} is on the submissions index and "
                 "not in the panel ({})".format(g["ticker"], g.get("form") or "?",
                                                g.get("period"), g["status"]))
    if result.get("errors"):
        base += " | ERRORS: {}".format("; ".join("{} {}".format(t, d) for t, d in result["errors"]))
    return base


def to_json(result):
    return json.dumps(result, indent=2, sort_keys=True)
