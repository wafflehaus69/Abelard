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
import time

from . import (charts, config, divergence, edgar, facts_api, freshness,
               identity, storage, universe)

WATERMARK_PREFIX = "scan:"

OUTCOME_NOOP = "no-op"
OUTCOME_UPDATED = "updated"
OUTCOME_ERROR = "error"


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
    __slots__ = ("cik", "ticker", "status", "newest_filing", "watermark", "detail")

    def __init__(self, cik, ticker, status, newest_filing, watermark, detail):
        self.cik = cik
        self.ticker = ticker
        self.status = status
        self.newest_filing = newest_filing
        self.watermark = watermark
        self.detail = detail

    @property
    def is_affected(self):
        return self.status == "new-filing"

    def __repr__(self):
        return "IssuerScan({} {} newest={} wm={})".format(
            self.ticker, self.status, self.newest_filing, self.watermark)


def check_issuer(con, entity, http=None, submissions_doc=None):
    """Is there a periodic filing newer than this issuer's watermark?

    `submissions_doc` may be injected for tests; otherwise it is fetched.
    """
    try:
        doc = submissions_doc if submissions_doc is not None else edgar.fetch_submissions(
            entity.cik, http)
    except Exception as exc:
        return IssuerScan(entity.cik, entity.ticker_display, OUTCOME_ERROR, None,
                          read_watermark(con, entity.cik),
                          "submissions fetch failed: {}".format(exc))

    snapshot = identity.from_snapshot_doc(doc) if hasattr(identity, "from_snapshot_doc") \
        else identity.from_submissions(doc)
    events = identity.record(con, snapshot, entity.bucket)

    filing = freshness.latest_periodic_filing(doc)
    wm = read_watermark(con, entity.cik)
    if filing is None:
        return IssuerScan(entity.cik, entity.ticker_display, "no-periodic", None, wm,
                          "no 10-K or 10-Q in the submissions index")
    if wm and filing.filing_date <= wm:
        return IssuerScan(entity.cik, entity.ticker_display, "current",
                          filing.filing_date, wm, "nothing newer than watermark")
    detail = "new filing {} {} (period {})".format(
        filing.form, filing.filing_date, filing.report_date)
    if events:
        detail += "; identity events: {}".format(", ".join(e.field for e in events))
    return IssuerScan(entity.cik, entity.ticker_display, "new-filing",
                      filing.filing_date, wm, detail)


def refresh_issuer(con, entity, http=None, facts_doc=None):
    """Re-derive one issuer's view. Returns (IssuerView, indexed facts)."""
    doc = facts_doc if facts_doc is not None else edgar.fetch_companyfacts(entity.cik, http)
    indexed = facts_api.index_facts(doc)
    return divergence.build_issuer_view(entity, indexed), indexed


def run(con=None, roster=None, http=None, render=True, outdir=None, now_unix=None,
        submissions_by_cik=None, facts_by_cik=None):
    """One scan cycle. Returns a summary dict; never raises on a single issuer.

    Injection points (`submissions_by_cik`, `facts_by_cik`) exist so the whole
    cycle is testable without network — idempotency is a property worth testing
    deterministically rather than against a live index.
    """
    con = con if con is not None else storage.connect()
    roster = roster if roster is not None else universe.load()
    started = int(now_unix if now_unix is not None else time.time())

    checks, errors = [], []
    for cik, entity in sorted(roster.items()):
        sub = (submissions_by_cik or {}).get(cik)
        c = check_issuer(con, entity, http=http, submissions_doc=sub)
        checks.append(c)
        if c.status == OUTCOME_ERROR:
            errors.append((c.ticker, c.detail))

    affected = [c for c in checks if c.is_affected]
    if not affected:
        return {
            "outcome": OUTCOME_NOOP,
            "checked": len(checks),
            "affected": 0,
            "errors": errors,
            "artifacts_written": False,
            "summary": "{} issuers checked, none with a new filing since watermark".format(
                len(checks)),
            "started_unix": started,
        }

    views, refreshed = [], []
    for c in affected:
        entity = roster[c.cik]
        try:
            view, _ = refresh_issuer(con, entity, http=http,
                                     facts_doc=(facts_by_cik or {}).get(c.cik))
        except Exception as exc:
            errors.append((c.ticker, "refresh failed: {}".format(exc)))
            continue
        views.append(view)
        refreshed.append(c)

    # Watermarks advance ONLY for issuers whose refresh actually succeeded.
    advanced = []
    for c in refreshed:
        if write_watermark(con, c.cik, c.newest_filing, now_unix=started):
            advanced.append(c.ticker)

    artifacts = False
    if render and views:
        all_views = _all_views(con, roster, http, facts_by_cik)
        comp = divergence.composition(all_views)
        charts.render_all(all_views, comp, outdir=outdir)
        artifacts = True

    return {
        "outcome": OUTCOME_UPDATED,
        "checked": len(checks),
        "affected": len(affected),
        "refreshed": [c.ticker for c in refreshed],
        "watermarks_advanced": advanced,
        "errors": errors,
        "artifacts_written": artifacts,
        "summary": "{} of {} issuers had new filings: {}".format(
            len(affected), len(checks), ", ".join(c.ticker for c in affected)),
        "started_unix": started,
    }


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
    if result.get("errors"):
        base += " | ERRORS: {}".format("; ".join("{} {}".format(t, d) for t, d in result["errors"]))
    return base


def to_json(result):
    return json.dumps(result, indent=2, sort_keys=True)
