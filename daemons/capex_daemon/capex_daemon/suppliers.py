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

**Coverage is three of five. Two are measured, one is RULED, two refuse by name:**

    NVDA   measured  ProductOrServiceAxis           17 quarters, 2022Q2-2026Q2, TTM $229.9B
    AMD    measured  StatementBusinessSegmentsAxis  17 quarters, 2022Q1-2026Q2, TTM $22.2B
    MU     MAPPED    CMBU + CDBU, ruled 2026-08-21   7 quarters, 2024Q4-2026Q2, TTM $52.5B
    AVGO   refused   no datacenter member — InfrastructureSoftware / SemiconductorSolutions
    SMCI   refused   no segment axis at all, in any of 14 instances

Micron was the interesting refusal and is now the interesting admission. Its
Cloud Memory and Core Data Center units plainly bear on the buildout, but
deciding that CMBU+CDBU *is* datacenter revenue is a semantic judgement — so it
was made by RULING rather than by this module, and it is carried as one. The leg
publishes `MAPPED-BUSINESS-UNITS`, never `COVERED`; it names the units summed,
the units excluded, and the date and author of the ruling; and `/suppliers`
prints all of that beside the number. A mapped figure must never be able to pass
as a measured one (E1, E8).

Micron also only exists as a series because of [E26]. CMBU/CDBU appear no earlier
than the 2025-08-28 instance — before that Micron reported CNBU/MBU/EBU/SBU — and
the prior periods are recoverable solely because the newer filings restate them.
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
STATUS_MAPPED = "MAPPED-BUSINESS-UNITS"
STATUS_NO_DC_MEMBER = "UNCOVERED-NO-DC-MEMBER"
STATUS_NO_INSTANCES = "UNCOVERED-NO-INSTANCES"
STATUS_NOT_A_SUPPLIER = "NOT-A-SUPPLIER"

# --- ruled mappings: a semantic judgement, carried as one ---------------------
#
# Some issuers report no datacenter member but do report business units that
# plainly ARE datacenter demand. Calling those units "datacenter revenue" is a
# judgement, not a parse, so it is made by ruling and then carried visibly: the
# leg publishes STATUS_MAPPED rather than COVERED, names the units it summed and
# the units it excluded, and stamps the date it was ruled. `/suppliers` prints
# all of it. A mapped figure must never be able to pass as a measured one.
MAPPED_UNITS = {
    "0000723125": {                                   # Micron
        "ticker": "MU",
        "members": ("CMBUMember", "CDBUMember"),
        "labels": {"CMBUMember": "Cloud Memory Business Unit",
                   "CDBUMember": "Core Data Center Business Unit"},
        "excluded": ("MCBUMember", "AEBUMember", "AllOtherSegmentsMember"),
        "excluded_labels": {"MCBUMember": "Mobile and Client",
                            "AEBUMember": "Automotive and Embedded",
                            "AllOtherSegmentsMember": "All other"},
        "ruled": "2026-08-21",
        "ruled_by": "Mando",
        "rationale": ("Memory sold into cloud and datacenter is the demand being "
                      "cross-checked, and HBM — the AI-relevant product — sits in "
                      "exactly those two units. Mobile/client and automotive/"
                      "embedded are excluded because they are not buildout demand."),
    },
}


def mapping_for(entity):
    """The ruled unit mapping for an issuer, or None."""
    return MAPPED_UNITS.get(str(getattr(entity, "cik", "")).zfill(10))

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


class _Synth:
    """A summed mapped-unit period, shaped like the facts it was built from."""
    __slots__ = ("concept", "value", "period_start", "period_end", "dims", "scale_basis")

    def __init__(self, concept, value, period_start, period_end, dims):
        self.concept, self.value = concept, value
        self.period_start, self.period_end = period_start, period_end
        self.dims, self.scale_basis = dims, "ixbrl-mapped"

    @property
    def dim_key(self):
        return ";".join("{}={}".format(a, m) for a, m in sorted(self.dims.items()))


def mapped_facts(batches, spec):
    """Sum a ruled set of business units into one series, newest filing winning.

    Every mapped member must be present for a period or the period is skipped —
    a partial sum would understate the line and look like a decline. Skipped
    periods are returned so the gap is visible rather than inferred.
    """
    want = set(spec["members"])
    picked = {}
    for key, facts in batches:
        for f in facts:
            if f.concept not in REVENUE_CONCEPTS or f.value is None:
                continue
            dims = {_local(a): _local(m) for a, m in (f.dims or {}).items()}
            sub = [a for a in dims if a not in QUALIFIER_AXES]
            if len(sub) != 1 or dims[sub[0]] not in want:
                continue
            pk = (dims[sub[0]], f.period_start, f.period_end)
            prior = picked.get(pk)
            if prior is None or (key, -len(dims)) > (prior[3], -prior[4]):
                picked[pk] = (f, f.concept, sub[0], key, len(dims))

    by_period, axes = {}, set()
    for (member, ps, pe), (f, concept, axis, _k, _n) in picked.items():
        by_period.setdefault((ps, pe), {})[member] = (f.value, concept)
        axes.add(axis)

    out, partial = [], []
    for (ps, pe), got in by_period.items():
        missing = want - set(got)
        if missing:
            partial.append({"period_start": ps, "period_end": pe,
                            "missing": sorted(missing)})
            continue
        concept = next(iter(got.values()))[1]
        out.append((_Synth(concept, sum(v for v, _c in got.values()), ps, pe,
                           {sorted(axes)[0]: "+".join(sorted(want))}), concept))
    return out, sorted(axes), partial


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
                 "quarters", "dropped", "instances", "restatements", "mapping",
                 "partial")

    def __init__(self, cik, ticker, status, detail="", axes=(), concept=None,
                 quarters=None, dropped=(), instances=0, restatements=(),
                 mapping=None, partial=()):
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
        self.mapping = mapping
        self.partial = list(partial)

    @property
    def is_covered(self):
        """Usable for the cross-check — measured OR ruled-mapped.

        Both contribute; only one of them is a measurement, which is why
        `status` travels with every published figure rather than being
        collapsed into a boolean here.
        """
        return self.status in (STATUS_COVERED, STATUS_MAPPED) and bool(self.quarters)

    @property
    def is_mapped(self):
        return self.status == STATUS_MAPPED

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
    status, mapping, partial = STATUS_COVERED, None, []

    if not pairs:
        # No datacenter member. A ruled unit mapping may still cover this
        # issuer — and if it does, the result is labelled as MAPPED, never as
        # measured, for as long as it is published.
        spec = mapping_for(entity)
        if spec:
            pairs, axes, partial = mapped_facts(batches, spec)
            status, mapping = STATUS_MAPPED, spec
        if not pairs:
            members = observed_members(f for _k, batch in batches for f in batch)
            seg = [m for a, m in members if "Segment" in a] or [m for _a, m in members]
            return SupplierLeg(
                entity.cik, entity.ticker_display, STATUS_NO_DC_MEMBER,
                "no `{}` on any axis across {} instances; members observed: {}".format(
                    DC_MEMBER, len(batches), ", ".join(seg[:6]) or "none"),
                instances=len(batches))

    # Each (start, end) is already resolved to its newest filing, so the cohort
    # differencing below sees each period exactly once.
    dropped = []
    rows = normalize.discrete_quarters(pairs, source_leg="ixbrl", dropped=dropped)
    quarters = {r.calendar_quarter: r.value for r in rows}
    concept = pairs[-1][1]
    if mapping:
        detail = "RULED MAPPING {} ({}): {} summed; {} excluded".format(
            mapping["ruled"], mapping.get("ruled_by", "ruling"),
            " + ".join(mapping["labels"].get(m, m) for m in mapping["members"]),
            ", ".join(mapping["excluded_labels"].get(m, m)
                      for m in mapping["excluded"]))
        if partial:
            detail += "; {} period(s) skipped for a missing unit".format(len(partial))
    else:
        detail = "resolved `{}` on {}".format(concept, ", ".join(axes))
    if restatements:
        detail += "; {} period(s) superseded by a later filing".format(len(restatements))
    return SupplierLeg(entity.cik, entity.ticker_display, status, detail,
                       axes=axes, concept=concept, quarters=quarters,
                       dropped=dropped, instances=len(batches),
                       restatements=restatements, mapping=mapping, partial=partial)


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
            # A test may inject already-parsed facts; anything else is a document.
            facts = blob if isinstance(blob, list) else ixbrl.parse_instance(blob)
        except Exception as exc:
            failures.append((report_date, str(exc)))
            continue
        # Cache the OBSERVATIONS, never the conclusion. For a mapped issuer the
        # individual unit facts are stored and the ruled sum is applied at read
        # time, so re-ruling the mapping does not require re-fetching a filing —
        # and a summed synthetic row, which neither matcher recognises on
        # reload, never reaches the cache.
        pairs, _axes, _rest = dc_facts([(report_date, facts)])
        spec = mapping_for(entity)
        if spec:
            want = set(spec["members"])
            for f in facts:
                if f.concept not in REVENUE_CONCEPTS or f.value is None:
                    continue
                dims = {_local(a): _local(m) for a, m in (f.dims or {}).items()}
                sub = [a for a in dims if a not in QUALIFIER_AXES]
                if len(sub) == 1 and dims[sub[0]] in want:
                    pairs.append((f, f.concept))
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
