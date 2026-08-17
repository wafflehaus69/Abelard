"""Per-issuer staleness detection and the filing-instance fallback (E6, E5).

Leg A is not merely incomplete, it is *silently late*. Meta filed its Q2-2026
10-Q on 2026-07-30; companyfacts held zero facts for that period on 2026-08-07
and served them by 2026-08-13. No error, no null, no signal — the series simply
looked flat for a fortnight. WULF's filing from two days earlier was present
throughout, so the lag is per-issuer and unpredictable.

That window is exactly where a deceleration sensor earns its keep, so the
fallback is justified on **latency** here, independently of the correctness
argument that dimensioned and custom-namespace facts are permanently absent.

The fallback path carries a hazard the API path does not: filing-level inline
XBRL has a ``scale`` attribute (observed ``scale="9"``). Every fact crossing this
path must land with an explicit basis, and a fact whose basis is
``undetermined`` is surfaced, never silently trusted (E5).
"""
from . import edgar, ixbrl
from .facts_api import SCALE_BASIS_API

PERIODIC_FORMS = ("10-Q", "10-K", "10-K/A", "10-Q/A")

PROVENANCE_API = "companyfacts"
PROVENANCE_INSTANCE = "filing-instance"

STATUS_CURRENT = "CURRENT"
STATUS_STALE = "STALE"
STATUS_NO_FILINGS = "NO-PERIODIC-FILINGS"


class Filing:
    __slots__ = ("form", "filing_date", "report_date", "accession", "primary_document")

    def __init__(self, form, filing_date, report_date, accession, primary_document):
        self.form = form
        self.filing_date = filing_date
        self.report_date = report_date
        self.accession = accession
        self.primary_document = primary_document

    @property
    def instance_document(self):
        return edgar.instance_document_name(self.primary_document)

    def __repr__(self):
        return "Filing({} {} period={} {})".format(
            self.form, self.filing_date, self.report_date, self.accession)


def latest_periodic_filing(submissions_doc, forms=PERIODIC_FORMS):
    """Newest 10-K/10-Q from a submissions document, by filing date."""
    recent = ((submissions_doc or {}).get("filings") or {}).get("recent") or {}
    cols = ("form", "filingDate", "reportDate", "accessionNumber", "primaryDocument")
    series = [recent.get(c) or [] for c in cols]
    if not series[0]:
        return None
    best = None
    for row in zip(*series):
        form, filed, report, accession, doc = row
        if form not in forms:
            continue
        if best is None or (filed or "") > (best.filing_date or ""):
            best = Filing(form, filed, report, accession, doc)
    return best


def api_latest_period(indexed, concept):
    """Newest period_end Leg A holds for a concept, or None."""
    rows = indexed.get(concept) or []
    ends = [f.period_end for f in rows if f.period_end]
    return max(ends) if ends else None


class Staleness:
    __slots__ = ("cik", "status", "filing", "api_latest", "detail")

    def __init__(self, cik, status, filing, api_latest, detail):
        self.cik = cik
        self.status = status
        self.filing = filing
        self.api_latest = api_latest
        self.detail = detail

    @property
    def needs_fallback(self):
        return self.status == STATUS_STALE

    def __repr__(self):
        return "Staleness({} {} api={} filed={})".format(
            self.cik, self.status, self.api_latest,
            self.filing.report_date if self.filing else None)


def assess(cik, submissions_doc, indexed, concept):
    """Compare the newest filed period against what Leg A actually carries.

    STALE means a periodic filing exists whose period the API has not published.
    That is the fallback trigger — and it is a *positive* finding about the API,
    never an absence of data at the issuer.
    """
    filing = latest_periodic_filing(submissions_doc)
    if filing is None:
        return Staleness(cik, STATUS_NO_FILINGS, None, None,
                         "no 10-K or 10-Q in the submissions index")
    api_latest = api_latest_period(indexed, concept)
    if api_latest is not None and filing.report_date and api_latest >= filing.report_date:
        return Staleness(cik, STATUS_CURRENT, filing, api_latest,
                         "companyfacts covers the newest filed period {}".format(api_latest))
    return Staleness(
        cik, STATUS_STALE, filing, api_latest,
        "filing {} covers period {} (filed {}); companyfacts latest is {}".format(
            filing.accession, filing.report_date, filing.filing_date, api_latest or "none"))


def gate_scale_basis(facts):
    """G1 on the fallback path: every fact must carry an explicit basis (E5).

    Returns (ok_facts, undetermined_facts). Undetermined facts are surfaced to
    the caller for reporting; they are never silently promoted into a series.
    """
    ok, undetermined = [], []
    for f in facts:
        if f.scale_basis == ixbrl.SCALE_BASIS_UNDETERMINED:
            undetermined.append(f)
        else:
            ok.append(f)
    return ok, undetermined


def fetch_fallback_facts(cik, filing, http=None, prefer_instance=True):
    """Pull facts for a stale period straight from the filing.

    Prefers the extracted instance (absolute values, no scale attribute); falls
    back to the inline document, which is where the ``scale`` hazard lives and
    where the nested-fact collapse matters.
    """
    if prefer_instance and filing.instance_document:
        try:
            raw = edgar.fetch_document(cik, filing.accession, filing.instance_document, http)
            return ixbrl.parse_instance(raw.encode("utf-8")), PROVENANCE_INSTANCE, "instance"
        except Exception:
            pass
    raw = edgar.fetch_document(cik, filing.accession, filing.primary_document, http)
    return ixbrl.parse_ixbrl(raw.encode("utf-8")), PROVENANCE_INSTANCE, "inline"


def provenance_for(source):
    return PROVENANCE_API if source == SCALE_BASIS_API else PROVENANCE_INSTANCE
