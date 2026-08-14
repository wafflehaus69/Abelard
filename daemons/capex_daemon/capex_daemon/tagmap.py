"""Per-issuer, per-era concept resolution for the three series maps (E7).

Concept names are neither uniform across issuers nor stable within one. A fixed
global preference order silently returned Amazon's capex from a tag abandoned in
2017 — $7.42B against a true $173.03B, 23x low, no error and no null. That is
**plausible-stale-resolution**, and this module exists to prevent it.

Rules:

  * Resolve by **recency**, per issuer. Among candidates present, the one with
    the most recent observation owns the current era; older tags own the periods
    before the newer one began.
  * Record the resolved concept on **every** series row. A series without its
    resolved tag is not interpretable and must not be published.
  * **Concurrency is not an era.** Where two candidates report the SAME period,
    that is co-reporting of different instruments, not a migration — and picking
    one would undercount. It is flagged, never silently resolved.
"""
from . import facts_api

CAPEX = "capex"
DEBT = "debt"
ANCHOR = "anchor"

# Candidate sets seeded from CD-R1's live survey. Order is irrelevant by design —
# resolution is by recency, never by position in this list.
CANDIDATES = {
    CAPEX: (
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        "PaymentsToDevelopRealEstateAssets",
        "PaymentsToAcquireMachineryAndEquipment",
        "PaymentsForCapitalImprovements",
    ),
    DEBT: (
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromIssuanceOfSeniorLongTermDebt",
        "ProceedsFromIssuanceOfDebt",
        "ProceedsFromDebtMaturingInMoreThanThreeMonths",
        "ProceedsFromDebtNetOfIssuanceCosts",
        "ProceedsFromNotesPayable",
        "ProceedsFromIssuanceOfSecuredDebt",
        "ProceedsFromConvertibleDebt",
        "ProceedsFromRelatedPartyDebt",
        "ProceedsFromShortTermDebt",
    ),
    # Gross-basis only. Net cannot anchor capex: depreciation confounds the
    # delta, so an issuer with no gross concept is UNANCHORED, not approximated.
    ANCHOR: (
        "PropertyPlantAndEquipmentGross",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetBeforeAccumulatedDepreciationAndAmortization",
        "RealEstateInvestmentPropertyAtCost",
        "RealEstateGrossAtCarryingValue",
    ),
}

UNRESOLVED = "UNRESOLVED"
UNRESOLVED_MULTILINE = "UNRESOLVED-MULTILINE"

# Kinds where several concepts legitimately co-report the SAME period because
# they describe different instruments, not successive tagging eras.
#
# Measured (WULF, 2025): ProceedsFromIssuanceOfSecuredDebt $3,132,938,000 and
# ProceedsFromConvertibleDebt $975,329,000 both live, while pure recency elected
# ProceedsFromShortTermDebt at $92,750,000 — a 34x undercount reached by exactly
# the mechanism E7 describes, through a different door. Capex is one migrating
# cash-flow line; debt is a stack. Selecting one member of a stack is wrong, and
# summing it is a semantic decision no ruling has made, so this resolver refuses
# and says why (E1, E8).
MULTI_LINE_KINDS = frozenset({DEBT})


class Era:
    """A concept and the period range over which it is the resolved answer."""

    __slots__ = ("concept", "owns_from", "owns_to", "first_seen", "last_seen",
                 "fact_count", "units")

    def __init__(self, concept, owns_from, owns_to, first_seen, last_seen,
                 fact_count, units):
        self.concept = concept
        self.owns_from = owns_from
        self.owns_to = owns_to
        self.first_seen = first_seen
        self.last_seen = last_seen
        self.fact_count = fact_count
        self.units = units

    def covers(self, period_end):
        if self.owns_from and period_end < self.owns_from:
            return False
        if self.owns_to and period_end > self.owns_to:
            return False
        return True

    def __repr__(self):
        return "Era({} owns {}..{} n={})".format(
            self.concept, self.owns_from or "-", self.owns_to or "now", self.fact_count)


class Resolution:
    """Eras plus everything a reader needs to distrust them intelligently."""

    def __init__(self, series_kind, eras, concurrency, candidates_present, units,
                 multi_line_concepts=()):
        self.series_kind = series_kind
        self.eras = eras
        self.concurrency = concurrency
        self.candidates_present = candidates_present
        self.units = units
        # Non-empty only for MULTI_LINE_KINDS with live co-reporting.
        self.multi_line_concepts = tuple(multi_line_concepts)

    @property
    def is_multi_line(self):
        return bool(self.multi_line_concepts)

    @property
    def current_concept(self):
        if self.is_multi_line:
            return UNRESOLVED_MULTILINE
        return self.eras[0].concept if self.eras else UNRESOLVED

    def concept_for(self, period_end):
        if self.is_multi_line:
            return UNRESOLVED_MULTILINE
        for era in self.eras:
            if era.covers(period_end):
                return era.concept
        return UNRESOLVED

    @property
    def is_unresolved(self):
        return not self.eras

    def why(self):
        """Human-readable resolution status — goes on the coverage row."""
        if self.is_multi_line:
            return ("{} concepts co-report the current era ({}); a stack cannot be "
                    "resolved by selection and summation is unruled".format(
                        len(self.multi_line_concepts), ", ".join(self.multi_line_concepts)))
        if self.is_unresolved:
            return "no candidate concept present"
        return "recency-resolved to {} across {} era(s)".format(
            self.current_concept, len(self.eras))

    def __repr__(self):
        return "Resolution({} current={} eras={} concurrent={})".format(
            self.series_kind, self.current_concept, len(self.eras),
            len(self.concurrency))


def _spans(indexed, kind, unit_filter=("USD",)):
    """{concept: (first_end, last_end, n, {units})} for candidates actually present."""
    out = {}
    for concept in CANDIDATES[kind]:
        rows = indexed.get(concept)
        if not rows:
            continue
        rows = [f for f in rows if (not unit_filter or f.unit in unit_filter)]
        if not rows:
            continue
        ends = sorted(f.period_end for f in rows)
        out[concept] = (ends[0], ends[-1], len(rows), {f.unit for f in rows})
    return out


def resolve(indexed, kind, unit_filter=("USD",)):
    """Build the era map for one series kind from a companyfacts index."""
    spans = _spans(indexed, kind, unit_filter)
    if not spans:
        return Resolution(kind, [], [], {}, set())

    # Newest observation first — this ordering IS the resolution rule.
    ordered = sorted(spans.items(), key=lambda kv: (kv[1][1], kv[1][2]), reverse=True)

    eras = []
    boundary = None  # the earliest date already claimed by a newer concept
    for concept, (first, last, n, units) in ordered:
        owns_to = None if boundary is None else _day_before(boundary)
        if owns_to is not None and first > owns_to:
            # Entirely shadowed by newer tags; keep it visible but owning nothing.
            eras.append(Era(concept, None, None, first, last, n, units))
            continue
        eras.append(Era(concept, first, owns_to, first, last, n, units))
        boundary = first if boundary is None else min(boundary, first)

    concurrency = _detect_concurrency(indexed, spans, unit_filter)
    all_units = set()
    for _, _, _, units in spans.values():
        all_units |= units
    live_eras = [e for e in eras if e.owns_from]
    multi = ()
    if kind in MULTI_LINE_KINDS and live_eras:
        multi = _live_co_reporters(indexed, spans, live_eras[0], unit_filter)
    if not multi:
        multi = _ambiguous_overlap(indexed, spans, unit_filter)
    return Resolution(kind, live_eras, concurrency, spans, all_units, multi)


# Two concepts covering one period with the same number are redundant tagging —
# EQIX carries its capex under two concepts in 67 periods, harmlessly. Two
# concepts covering one period with DIFFERENT numbers are a genuine ambiguity
# that recency cannot arbitrate, because neither is more recent. Selecting
# either would be a coin flip presented as an answer, so it refuses (E1).
_VALUE_AGREEMENT_TOLERANCE = 0.005


def _ambiguous_overlap(indexed, spans, unit_filter):
    """Disagreeing co-report NEAR THE FRONTIER, where it would move today's number.

    Scope matters. Disagreement at a decade-old migration boundary is expected —
    AMZN's 2016 handover reads 7.804B under ProductiveAssets against 6.737B under
    the older concept because they measured different things, and the era map
    already assigns each period to one owner. What cannot be tolerated is
    disagreement inside the live window, where the resolver's choice IS the
    published value: RIOT reports 2026Q1 capex as both 16,184,000 and
    115,465,000 under two live concepts, a 7x spread with no recency signal to
    separate them.
    """
    from datetime import date, timedelta
    frontier = max(last for _, last, _, _ in spans.values())
    try:
        cutoff = (date.fromisoformat(frontier) -
                  timedelta(days=LIVE_STACK_WINDOW_DAYS)).isoformat()
    except ValueError:
        return ()
    by_period = {}
    for concept in spans:
        for f in indexed.get(concept, []):
            if unit_filter and f.unit not in unit_filter:
                continue
            if not f.period_end or f.period_end < cutoff:
                continue
            by_period.setdefault((f.period_start, f.period_end), {})[concept] = f.value
    for _, vals in sorted(by_period.items(), key=lambda kv: kv[0][1] or "", reverse=True):
        if len(vals) < 2:
            continue
        nums = [v for v in vals.values() if v is not None]
        if len(nums) < 2:
            continue
        lo, hi = min(nums), max(nums)
        scale = max(abs(lo), abs(hi)) or 1.0
        if (hi - lo) / scale > _VALUE_AGREEMENT_TOLERANCE:
            return tuple(sorted(vals))
    return ()


# A concept counts as part of the live stack only if it is still being tagged
# near the frontier. An era can span 16 years, so "reports somewhere inside the
# current era" is far too loose — it flagged MSFT as multi-line on the strength
# of a ProceedsFromConvertibleDebt fact last tagged in 2010.
LIVE_STACK_WINDOW_DAYS = 400


def _live_co_reporters(indexed, spans, current_era, unit_filter):
    """Candidates still reporting near the frontier — a stack, not a migration.

    Two concepts both tagged within ~a year of the newest observation describe
    different instruments in the same filings. The era machinery would otherwise
    force them into a false sequence and elect whichever was tagged last.
    """
    from datetime import date, timedelta
    frontier = max(last for _, last, _, _ in spans.values())
    try:
        cutoff = (date.fromisoformat(frontier) -
                  timedelta(days=LIVE_STACK_WINDOW_DAYS)).isoformat()
    except ValueError:
        return ()
    live = [c for c, (_, last, _, _) in spans.items() if last >= cutoff]
    if len(live) < 2:
        return ()
    return tuple(sorted(live))


def _day_before(iso):
    from datetime import date, timedelta
    return (date.fromisoformat(iso) - timedelta(days=1)).isoformat()


def _detect_concurrency(indexed, spans, unit_filter):
    """Periods where 2+ candidates both report — co-reporting, not a migration.

    Returns [(period_key, [concepts])]. Summing across co-reported concepts is a
    semantic decision that needs a ruling, so this is reported and left alone.
    """
    by_period = {}
    for concept in spans:
        for f in indexed.get(concept, []):
            if unit_filter and f.unit not in unit_filter:
                continue
            by_period.setdefault((f.period_start, f.period_end), set()).add(concept)
    return sorted(((k, sorted(v)) for k, v in by_period.items() if len(v) > 1),
                  key=lambda kv: kv[0][1] or "")


def series_facts(indexed, resolution, unit_filter=("USD",)):
    """Facts for a series, each tagged with the concept that resolved it.

    Yields (ApiFact, resolved_concept). A fact whose period falls in another
    concept's era is dropped — that is the whole point of the era map.
    """
    out = []
    for era in resolution.eras:
        rows = [f for f in indexed.get(era.concept, [])
                if (not unit_filter or f.unit in unit_filter)]
        for f in facts_api.dedupe_latest_filed(rows):
            if era.covers(f.period_end):
                out.append((f, era.concept))
    return sorted(out, key=lambda t: (t[0].period_end, t[0].period_start or ""))
