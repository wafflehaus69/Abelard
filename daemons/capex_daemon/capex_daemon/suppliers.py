"""CD-3 — the supplier leg. The same dollar, counted at the other end.

A hyperscaler's capex and NVIDIA's datacenter revenue are largely the **same
money seen from opposite sides of the invoice**. That makes the supplier leg a
cross-check of unusual quality: it is an independent observation of the buildout,
filed by a different company, on a different fiscal calendar, under a different
set of incentives. If capex is real, it has to show up here.

It also makes blending the two a category error, and the ruling is explicit
(CD-R2 §2.3): suppliers are **a separate bucket with cross-check semantics,
never summed into the spending aggregate.** Adding NVDA's revenue to Microsoft's
capex double-counts one dollar. `trend.AGGREGATED_BUCKETS` does not contain
"supplier", so the exclusion is structural rather than remembered — and
`test_suppliers.py` pins it.

**This leg is parser-only, and that is the whole point.** Segment revenue is
dimension-qualified, so the companyfacts aggregation API drops it (E6): NVDA's
API record contains `Revenues` (total) and `NumberOfReportableSegments` (a
count), and nothing else. The $75.2B Data Center line exists only inside the
filing. `ixbrl.py` was built for exactly this and this is its second use.

**Coverage is two of five, measured, and the other three are refused by name:**

    NVDA   ProductOrServiceAxis          17 quarters, 2022Q2-2026Q2, TTM $229.9B
    AMD    StatementBusinessSegmentsAxis 17 quarters, 2022Q1-2026Q2, TTM $22.2B
    AVGO   no datacenter member — InfrastructureSoftware / SemiconductorSolutions
    MU     no datacenter member — CDBU/CMBU/MCBU/AEBU business-unit codes
    SMCI   no segment axis at all, in any of 14 instances

MU is the interesting refusal. Its "Cloud Memory" and "Core Data Center"
business units plainly bear on the buildout, but deciding that CMBU+CDBU *is*
datacenter revenue is a semantic judgement no ruling has made. Resolving it here
would be inventing a mapping and publishing it as a measurement, so the leg
refuses and names what it saw (E1, E8).
"""
from . import ixbrl, normalize, tagmap

SUPPLIER_BUCKET = "supplier"

# Matched on the MEMBER local-name, on ANY axis, CASE-INSENSITIVELY. Three
# separate things vary and all three were measured, not assumed:
#
#   axis      NVDA files on ProductOrServiceAxis, AMD on StatementBusinessSegmentsAxis
#   case      AMD's 10-Q says `DataCenterMember`, AMD's 10-K says `Datacenter-
#             Member` — same issuer, same disclosure, different capitalisation
#             by form type. Exact matching silently lost every Q4, because Q4 is
#             only derivable from the 10-K, which left AMD with no TTM at all.
#   concept   `Revenues` for NVDA, `RevenueFromContractWithCustomer…` for AMD
#
# Keying on any one of them alone drops a name or a quarter.
DC_MEMBER = "DataCenterMember"
_DC_MEMBER_NORM = "datacentermember"

STATUS_COVERED = "COVERED"
STATUS_NO_DC_MEMBER = "UNCOVERED-NO-DC-MEMBER"
STATUS_NO_INSTANCES = "UNCOVERED-NO-INSTANCES"
STATUS_NOT_A_SUPPLIER = "NOT-A-SUPPLIER"

REVENUE_CONCEPTS = tagmap.CANDIDATES[tagmap.REVENUE]


def _local(name):
    """`us-gaap:DataCenterMember` / `{ns}DataCenterMember` -> `DataCenterMember`."""
    return str(name).split("}")[-1].split(":")[-1]


def _is_dc(member):
    """Case-insensitive member match — see DC_MEMBER for why case varies."""
    return _local(member).lower() == _DC_MEMBER_NORM


# Axes that QUALIFY a fact without subdividing it. `ConsolidationItemsAxis=
# OperatingSegmentsMember` says "this figure is a reportable operating segment",
# which is how segment revenue is normally tagged — AMD files its datacenter
# line that way and NVDA does not. Treating any extra axis as a breakdown
# refused AMD outright on its first run here; treating none as a breakdown would
# have summed geographic splits of the same line. So the distinction is named.
QUALIFIER_AXES = frozenset({"ConsolidationItemsAxis"})


def dc_facts(batches):
    """Every revenue fact that IS the datacenter line, newest filing winning.

    `batches` is [(instance_key, [facts])] where `instance_key` sorts
    chronologically — the filing's report date is what the callers use.

    Returns ([(fact, concept)], axes, restatements) ready for
    `normalize.discrete_quarters`.

    A fact qualifies when its only subdividing dimension is the datacenter
    member itself. `DataCenter x Americas` is a breakdown of the line, not the
    line, and taking it would undercount.

    **Newest filing wins, and this is not a formality.** AMD's 10-Q for the
    quarter ending 2024-03-30 tagged its segment revenue with the members
    ROTATED: $2,337M — the figure AMD reports as Data Center — carries
    `ClientMember`, and `DataCenterMember` carries $922M, which is Gaming. The
    filing a year later restates the same quarter correctly at $2,337M. Keyed
    on anything but recency this leg publishes a 2.5x undercount for that
    quarter and calls it Data Center revenue. Restatements are counted and
    returned so a silent correction is still a visible one (E12).
    """
    picked, axes, restatements = {}, set(), []
    for key, facts in batches:
        for f in facts:
            if f.concept not in REVENUE_CONCEPTS or f.value is None:
                continue
            dims = {_local(a): _local(m) for a, m in (f.dims or {}).items()}
            hit = [a for a, m in dims.items() if _is_dc(m)]
            if not hit:
                continue
            if [a for a in dims if a not in QUALIFIER_AXES] != hit:
                continue
            axes.add(hit[0])
            pk = (f.period_start, f.period_end)
            prior = picked.get(pk)
            # Newer instance always wins; within one instance prefer the
            # least-qualified statement of the same fact.
            if prior is None or (key, -len(dims)) > (prior[2], -prior[3]):
                if prior is not None and prior[0].value != f.value:
                    restatements.append({
                        "period_start": f.period_start, "period_end": f.period_end,
                        "was": prior[0].value, "now": f.value,
                        "superseded_by": key, "from_instance": prior[2]})
                picked[pk] = (f, f.concept, key, len(dims))
    return ([(f, c) for f, c, _k, _n in picked.values()], sorted(axes),
            restatements)


def observed_members(facts):
    """Every member seen on a revenue fact — what a refusal gets to cite."""
    seen = set()
    for f in facts:
        if f.concept not in REVENUE_CONCEPTS:
            continue
        for a, m in (f.dims or {}).items():
            seen.add((_local(a), _local(m)))
    return sorted(seen)


class SupplierLeg:
    """One supplier's datacenter revenue, or a named reason there is none."""

    __slots__ = ("cik", "ticker", "status", "detail", "axes", "concept",
                 "quarters", "dropped", "instances", "restatements")

    def __init__(self, cik, ticker, status, detail="", axes=(), concept=None,
                 quarters=None, dropped=(), instances=0, restatements=()):
        self.cik = cik
        self.ticker = ticker
        self.status = status
        self.detail = detail
        self.axes = list(axes)
        self.concept = concept
        self.quarters = quarters or {}
        self.dropped = list(dropped)
        self.instances = instances
        self.restatements = list(restatements)

    @property
    def is_covered(self):
        return self.status == STATUS_COVERED and bool(self.quarters)

    def __repr__(self):
        return "SupplierLeg({} {} n={})".format(self.ticker, self.status,
                                                len(self.quarters))


def build_leg(entity, batches):
    """Assemble one supplier's datacenter revenue series.

    `batches` is [(instance_key, [facts])], one entry per filing instance, where
    `instance_key` sorts chronologically — the report date is what callers pass.
    """
    if entity.bucket != SUPPLIER_BUCKET:
        return SupplierLeg(entity.cik, entity.ticker_display, STATUS_NOT_A_SUPPLIER,
                           "bucket is {}, not {}".format(entity.bucket, SUPPLIER_BUCKET))
    batches = sorted((k, f) for k, f in batches if f)
    if not batches:
        return SupplierLeg(entity.cik, entity.ticker_display, STATUS_NO_INSTANCES,
                           "no filing instances parsed")

    pairs, axes, restatements = dc_facts(batches)
    if not pairs:
        members = observed_members(f for _k, batch in batches for f in batch)
        seg = [m for a, m in members if "Segment" in a] or [m for _a, m in members]
        return SupplierLeg(
            entity.cik, entity.ticker_display, STATUS_NO_DC_MEMBER,
            "no `{}` on any axis across {} instances; members observed: {}".format(
                DC_MEMBER, len(batches), ", ".join(seg[:6]) or "none"),
            instances=len(batches))

    # `dc_facts` has already resolved each (start, end) to its newest filing,
    # so the cohort differencing below sees each period exactly once.
    dropped = []
    rows = normalize.discrete_quarters(pairs, source_leg="ixbrl", dropped=dropped)
    quarters = {r.calendar_quarter: r.value for r in rows}
    concept = pairs[-1][1]
    detail = "resolved `{}` on {}".format(concept, ", ".join(axes))
    if restatements:
        detail += "; {} period(s) superseded by a later filing".format(len(restatements))
    return SupplierLeg(entity.cik, entity.ticker_display, STATUS_COVERED, detail,
                       axes=axes, concept=concept, quarters=quarters,
                       dropped=dropped, instances=len(batches),
                       restatements=restatements)


# ---------------- fetching and caching ----------------

# ~3.5 years: enough for a TTM plus its year-ago comparison plus the restatement
# lag that AMD's rotated-member correction needed to be visible at all.
INSTANCE_LIMIT = 14

PERIODIC_FORMS = ("10-K", "10-Q")


def instance_index(cik, http, limit=INSTANCE_LIMIT, submissions_doc=None):
    """[(report_date, accession, instance_document)] newest first."""
    from . import edgar
    doc = submissions_doc if submissions_doc is not None else edgar.fetch_submissions(cik, http)
    r = (doc.get("filings") or {}).get("recent") or {}
    out = []
    for i in range(len(r.get("form", []))):
        if r["form"][i] not in PERIODIC_FORMS:
            continue
        out.append((r["reportDate"][i], r["accessionNumber"][i],
                    edgar.instance_document_name(r["primaryDocument"][i])))
    return out[:limit]


class _Row:
    """A cached datacenter fact, shaped like the parsed fact it came from."""
    __slots__ = ("concept", "value", "period_start", "period_end", "dims", "scale_basis")

    def __init__(self, concept, value, period_start, period_end, dims):
        self.concept = concept
        self.value = value
        self.period_start = period_start
        self.period_end = period_end
        self.dims = dims
        self.scale_basis = "ixbrl"

    @property
    def dim_key(self):
        return ";".join("{}={}".format(a, m) for a, m in sorted(self.dims.items()))


def harvest(entity, con, http=None, limit=INSTANCE_LIMIT, submissions_doc=None,
            fetch=None):
    """Fetch and store any datacenter facts this issuer has filed since last time.

    Only instances absent from the cache are fetched, so a nightly run costs one
    submissions request per supplier on a quiet night instead of re-parsing
    fourteen documents. Returns (instances_added, failures).
    """
    from . import edgar
    if entity.bucket != SUPPLIER_BUCKET:
        return 0, []
    have = {r[0] for r in con.execute(
        "SELECT DISTINCT instance_key FROM supplier_dc_facts WHERE cik=?", (entity.cik,))}
    seen = {r[0] for r in con.execute(
        "SELECT instance_key FROM supplier_instances WHERE cik=?", (entity.cik,))}
    added, failures = 0, []
    for report_date, accession, docname in instance_index(
            entity.cik, http, limit=limit, submissions_doc=submissions_doc):
        if report_date in have or report_date in seen:
            continue
        try:
            blob = (fetch or edgar.fetch_document)(entity.cik, accession, docname, http=http)
            facts = ixbrl.parse_instance(blob)
        except Exception as exc:
            failures.append((report_date, str(exc)))
            continue
        pairs, _axes, _rest = dc_facts([(report_date, facts)])
        for f, concept in pairs:
            con.execute(
                "INSERT OR REPLACE INTO supplier_dc_facts(cik, instance_key, "
                "period_start, period_end, concept, dim_key, value) VALUES (?,?,?,?,?,?,?)",
                (entity.cik, report_date, f.period_start, f.period_end, concept,
                 f.dim_key, f.value))
        # Recorded even when it yielded nothing, so an issuer with no datacenter
        # member is not re-fetched every night forever.
        con.execute("INSERT OR REPLACE INTO supplier_instances(cik, instance_key, "
                    "dc_facts, accession) VALUES (?,?,?,?)",
                    (entity.cik, report_date, len(pairs), accession))
        added += 1
    con.commit()
    return added, failures


def leg_from_db(entity, con):
    """Rebuild a supplier's leg from cached facts — no network, no re-parsing."""
    rows = con.execute(
        "SELECT instance_key, period_start, period_end, concept, dim_key, value "
        "FROM supplier_dc_facts WHERE cik=? ORDER BY instance_key", (entity.cik,)).fetchall()
    n_inst = con.execute("SELECT COUNT(*) FROM supplier_instances WHERE cik=?",
                         (entity.cik,)).fetchone()[0]
    if not rows:
        if not n_inst:
            return SupplierLeg(entity.cik, entity.ticker_display, STATUS_NO_INSTANCES,
                               "no filing instances cached")
        return SupplierLeg(entity.cik, entity.ticker_display, STATUS_NO_DC_MEMBER,
                           "no `{}` on any axis across {} cached instances".format(
                               DC_MEMBER, n_inst), instances=n_inst)
    batches = {}
    for key, ps, pe, concept, dim_key, value in rows:
        dims = dict(p.split("=", 1) for p in dim_key.split(";") if "=" in p)
        batches.setdefault(key, []).append(_Row(concept, value, ps, pe, dims))
    leg = build_leg(entity, sorted(batches.items()))
    leg.instances = n_inst or leg.instances
    return leg


def parse_instances(blobs):
    """Parse instance documents into [(instance_key, facts)], newest-sortable.

    `blobs` is [(instance_key, source)]; the key must sort chronologically,
    because `dc_facts` resolves restatements by taking the newest.

    A malformed instance is one filing lost, not the whole leg — but it is
    counted so a leg quietly built on half its filings is visible.
    """
    out, failed = [], []
    for key, blob in blobs:
        try:
            out.append((key, ixbrl.parse_instance(blob)))
        except Exception as exc:
            failed.append((key, str(exc)))
    return out, failed


# ---------------- the cross-check ----------------

STATUS_NO_OVERLAP = "NO-WINDOW-OVERLAP"


def crosscheck(dc_ttm, capex_ttm):
    """Supplier datacenter revenue against buyer capex, as a RATIO.

    Never a sum, never a difference dressed as a total — those are the same
    dollar and adding them double-counts it. The ratio is the honest reading:
    what share of the panel's reported capital spending shows up as one
    supplier's datacenter revenue. It is not expected to be 1.0 and is not a
    reconciliation; it is a corroboration, and a sharp move in it is the signal.
    """
    if not dc_ttm or not capex_ttm:
        return {"status": STATUS_NO_OVERLAP, "ratio": None, "quarters": []}
    shared = sorted(set(dc_ttm) & set(capex_ttm), key=_cq_sort)
    if not shared:
        return {"status": STATUS_NO_OVERLAP, "ratio": None, "quarters": []}
    series = [{"q": q, "ratio": dc_ttm[q] / capex_ttm[q], "dc": dc_ttm[q],
               "capex": capex_ttm[q]} for q in shared if capex_ttm[q]]
    return {"status": STATUS_COVERED,
            "ratio": series[-1]["ratio"] if series else None,
            "latest_quarter": series[-1]["q"] if series else None,
            "quarters": series}


def _cq_sort(q):
    y, n = q.split("Q")
    return (int(y), int(n))
