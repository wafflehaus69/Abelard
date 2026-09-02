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


# --- the crossing: filing facts into the API's own shape --------------------

def to_api_facts(facts, filing):
    """`ixbrl.Fact` -> `facts_api.Fact`, so a filled period is indistinguishable
    downstream from one companyfacts served.

    **Undimensioned only, and that is not a detail.** The API's undimensioned
    series is a total; an instance carries the total *and* every segment
    breakdown under the same concept. Merging those in would add a company's
    parts to its whole and silently double-count the quarter this function
    exists to recover.
    """
    from .facts_api import ApiFact, _days
    out = []
    for f in facts:
        if f.dim_key:
            continue
        if f.period_start is None or f.period_end is None:
            continue
        out.append(ApiFact(
            concept=f.concept, taxonomy=f.taxonomy, unit=f.unit, value=f.value,
            period_start=f.period_start, period_end=f.period_end,
            duration_days=_days(f.period_start, f.period_end),
            form=filing.form, filed=filing.filing_date, frame=None))
    return out


class Fill:
    """What a fallback attempt did, so a caller can report it rather than guess."""

    __slots__ = ("status", "filing", "period", "added", "undetermined",
                 "concepts", "detail")

    def __init__(self, status, filing=None, period=None, added=0,
                 undetermined=0, concepts=(), detail=""):
        self.status = status
        self.filing = filing
        self.period = period
        self.added = added
        self.undetermined = undetermined
        self.concepts = tuple(concepts)
        self.detail = detail

    @property
    def filled(self):
        return self.status == FILL_FILLED

    def __repr__(self):
        return "Fill({} period={} added={})".format(
            self.status, self.period, self.added)


FILL_NOT_NEEDED = "NOT-NEEDED"
FILL_FILLED = "FILLED"
FILL_EMPTY = "EMPTY"
FILL_FAILED = "FAILED"


def fill_from_filing(cik, submissions_doc, indexed, concept=None, http=None,
                     candidates=None):
    """Merge the newest filed period into `indexed` when the API lacks it (E6).

    Mutates `indexed` in place and returns a `Fill` describing what happened.

    This is the wiring the module was written for and never had. `assess` and
    `fetch_fallback_facts` existed, were tested, and were called by nothing
    outside this file — so the companyfacts lag they were built to cover ran
    unmitigated in production. Measured 2026-09-02: DLR and AMT had a filed
    2026Q2 that companyfacts still did not carry 33 and 36 days after filing,
    and both were invisible to the panel.

    Only concepts the daemon actually reads are filled. A blanket merge would
    pull ~1800 facts per filing into an index sized for the ones that matter.
    """
    from . import tagmap
    if candidates is None:
        candidates = {c for cs in tagmap.CANDIDATES.values() for c in cs}
    st = assess(cik, submissions_doc, indexed, concept)
    if st.status != STATUS_STALE or st.filing is None:
        return Fill(FILL_NOT_NEEDED, st.filing, None, detail=st.detail)
    try:
        facts, _prov, mode = fetch_fallback_facts(cik, st.filing, http=http)
    except Exception as exc:
        return Fill(FILL_FAILED, st.filing, st.filing.report_date,
                    detail="filing fetch/parse failed: {}".format(exc))
    wanted = [f for f in facts if f.concept in candidates]
    ok, undetermined = gate_scale_basis(wanted)      # E5: no silent trust
    api_facts = to_api_facts(ok, st.filing)
    if not api_facts:
        return Fill(FILL_EMPTY, st.filing, st.filing.report_date,
                    undetermined=len(undetermined),
                    detail="{}: no undimensioned facts on a tracked concept "
                           "({} undetermined-basis dropped)".format(
                               mode, len(undetermined)))
    touched, added = set(), 0
    for f in api_facts:
        rows = indexed.setdefault(f.concept, [])
        # Holes only. A period companyfacts already carries is authoritative —
        # this path exists because the API is LATE, not because it is wrong, and
        # a filled fact must never displace a served one.
        if any(r.period_start == f.period_start and r.period_end == f.period_end
               for r in rows):
            continue
        rows.append(f)
        touched.add(f.concept)
        added += 1
    if not added:
        return Fill(FILL_EMPTY, st.filing, st.filing.report_date,
                    undetermined=len(undetermined),
                    detail="{}: every parsed period was already in the index"
                           .format(mode))
    return Fill(FILL_FILLED, st.filing, st.filing.report_date, added=added,
                undetermined=len(undetermined), concepts=sorted(touched),
                detail="filled period {} from the {} of {}: {} facts across {} "
                       "concepts".format(st.filing.report_date, mode,
                                         st.filing.accession, added, len(touched)))
