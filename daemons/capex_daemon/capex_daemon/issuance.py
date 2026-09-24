"""Debt issuance totals from a concurrent instrument stack (ruling R-B6-1).

Canonical storage is **per-concept rows, always** — nothing is summed away at the
storage layer. The total the credit-to-capex series needs is a *derived view*,
built per issuer per period by a mechanical rule applied in order:

  (a) **Collapse double-tags.** Two concepts carrying byte-identical values
      across every period they co-report are one instrument tagged twice. Keep
      one, record the collapse in provenance. Mechanically decidable.
  (b) **Sum disjoint instruments.** Remaining co-reporters with differing values
      are distinct instruments — short-term vs secured vs convertible. Sum them.
  (c) **Refuse containment ambiguity.** Where a pair is neither identical nor
      clearly disjoint — one persistently at or below the other in the same
      periods, consistent with subset rather than sibling — summing would
      double-count. Refuse the total, emit UNRESOLVED-MULTILINE, and log the
      pair with its values for ruling.

The divergence view consumes the total where it exists and shows coverage status
where it does not. It never shows zero for a refusal (E1).
"""
from . import tagmap

BRANCH_SINGLE = "single"
BRANCH_COLLAPSED = "collapsed-double-tag"
BRANCH_SUMMED = "summed-disjoint"
BRANCH_REFUSED = "refused-containment"

STATUS_OK = "OK"
STATUS_REFUSED = tagmap.UNRESOLVED_MULTILINE

# Values within this relative distance are treated as the same number. Filing
# roundings differ in the last digit; 0.5% is far below any real instrument gap.
IDENTITY_TOLERANCE = 0.005
# A pair is "contained" when the smaller never exceeds the larger in any shared
# period. One shared period is not evidence; require a few before refusing.
MIN_SHARED_PERIODS_FOR_CONTAINMENT = 2
# Only overlap near the frontier can move a published total. Decade-old
# co-reporting is history the era map already owns — EQIX refused on a 2009 pair
# and DLR on 2009-2019 ones, none of which touch any current period. Same
# scoping the concept resolver applies; it simply was not carried across.
LIVE_WINDOW_DAYS = 400

# Counterparty concepts describe WHO lent, not WHAT was borrowed. The same
# borrowing is also tagged by instrument, so summing a counterparty view with
# instrument views double-counts, and its persistent smallness reads as
# containment. WULF's ProceedsFromRelatedPartyDebt sat under three different
# instrument parents for exactly this reason. Excluded from the instrument
# stack by ruling; still stored per-concept, never summed into a total.
COUNTERPARTY_CONCEPTS = frozenset({
    "ProceedsFromRelatedPartyDebt",
})

# --- presentation-semantics gate (R-CD2-1) -------------------------------
# A concept whose VERIFIED line-mapping reads net-of-repayments is not a gross
# inflow and can never enter a gross-issuance sum, at any issuer, permanently.
# Concept name alone cannot decide this — only the filed line label can (E23) —
# so entries here are recorded per concept with the filing language that put
# them there.
#
# Note the distinction that WULF forced: "net of issuance costs" is still a
# gross inflow (it nets fees, not repayments) and does NOT disqualify. Only
# net-of-REPAYMENT presentation does.
NET_PRESENTATION_CONCEPTS = {
    "ProceedsFromRepaymentsOfShortTermDebtMaturingInThreeMonthsOrLess":
        "line nets proceeds against repayments by construction",
    "ProceedsFromRepaymentsOfCommercialPaper":
        "line nets issuance against repayment by construction",
    "ProceedsFromRepaymentsOfLinesOfCredit":
        "line nets draws against repayments by construction",
    "ProceedsFromRepaymentsOfDebt":
        "line nets issuance against repayment by construction",
}

# Generic totals that CONTAIN named components. Where both appear, the total
# takes precedence and the component is annotative, never added (branch b′,
# ruling R-CD2-4). Summing a total with its own component double-counts it.
GENERIC_TOTALS = {
    "ProceedsFromIssuanceOfDebt": (
        "ProceedsFromConvertibleDebt",
        "ProceedsFromIssuanceOfSecuredDebt",
        "ProceedsFromIssuanceOfLongTermDebt",
        "ProceedsFromNotesPayable",
        "ProceedsFromShortTermDebt",
    ),
}


def is_net_presentation(concept):
    return concept in NET_PRESENTATION_CONCEPTS


# --- one raise, two presentations (C6, found via KEEL) --------------------
# "Net of issuance costs" is a gross inflow and stays eligible (the WULF ruling
# above). It does NOT follow that it may be summed with the GROSS tagging of the
# same raise: those are one raise stated twice, and adding them reports roughly
# twice the money.
#
# KEEL is the citing case. Its cash-flow statement reads "Proceeds from
# long-term debt, net of transaction costs 445,124"; Note 13's long-term-debt
# movement tags the same raise gross, "Issuance of long-term debt 458,650", with
# transaction costs on their own line. 458,650 - 13,526 = 445,124. The existing
# rules cannot catch it: the values are not identical (so not a double-tag), and
# the two share ONE live period, below the containment minimum (so they were
# summed as "distinct instruments") — $903,774K for a $458,650K raise.
#
# The relationship here is not inferred from the data, so one shared period is
# enough: the concepts' verified line mappings say outright that one is the
# other net of costs. The test is `net <= gross`, not a tolerance — E8 forbids
# inventing a band, and none is needed.
COST_NET_RESTATEMENTS = {
    "ProceedsFromDebtNetOfIssuanceCosts": (
        ("ProceedsFromIssuanceOfLongTermDebt", "ProceedsFromIssuanceOfDebt",
         "ProceedsFromIssuanceOfSecuredDebt", "ProceedsFromConvertibleDebt",
         "ProceedsFromNotesPayable", "ProceedsFromShortTermDebt"),
        "the same raise stated net of transaction costs and gross "
        "(KEEL 10-Q 0001812477-26-000023: statement line 445,124 net; Note 13 "
        "458,650 gross, costs shown separately)"),
}


def _cost_net_conflict(a, b, series_a, series_b, cutoff=None):
    """(shared, reason) when a and b are one raise stated two ways, else None."""
    for net, (grosses, why) in COST_NET_RESTATEMENTS.items():
        if a == net and b in grosses:
            net_s, gross_s = series_a, series_b
        elif b == net and a in grosses:
            net_s, gross_s = series_b, series_a
        else:
            continue
        shared = sorted(set(net_s) & set(gross_s))
        if cutoff:
            shared = [p for p in shared if p[1] and p[1] >= cutoff]
        live = [p for p in shared if net_s[p] or gross_s[p]]
        if not live:
            return None
        # Identical values are a double-TAG, and rule (a) handles those better
        # than a refusal does: collapse keeps the series, refusal withholds it.
        # CIFR is the case — it tags $167,113,000 as both ProceedsFromConvertible
        # Debt and ProceedsFromDebtNetOfIssuanceCosts in their one shared period.
        # Firing here would have withheld a total that collapses cleanly.
        if all(_close(net_s[p], gross_s[p]) for p in live):
            return None
        # Same raise implies the net side never exceeds the gross side.
        if all(net_s[p] <= gross_s[p] or _close(net_s[p], gross_s[p]) for p in live):
            return live, "{}; summing them would double-count it".format(why)
    return None


class PairVerdict:
    __slots__ = ("a", "b", "branch", "shared_periods", "detail")

    def __init__(self, a, b, branch, shared_periods, detail):
        self.a, self.b = a, b
        self.branch = branch
        self.shared_periods = shared_periods
        self.detail = detail

    def __repr__(self):
        return "PairVerdict({} vs {} -> {})".format(self.a, self.b, self.branch)


def _close(x, y):
    scale = max(abs(x), abs(y)) or 1.0
    return abs(x - y) / scale <= IDENTITY_TOLERANCE


def _frontier_cutoff(*series):
    from datetime import date, timedelta
    ends = [p[1] for s in series for p in s if p[1]]
    if not ends:
        return None
    try:
        return (date.fromisoformat(max(ends)) - timedelta(days=LIVE_WINDOW_DAYS)).isoformat()
    except ValueError:
        return None


def classify_pair(series_a, series_b, cutoff=None):
    """Decide which branch a co-reporting pair lands in.

    ``series_*`` are {period_key: value}. Only shared periods **near the
    frontier** count: overlap the era map has long since owned cannot move a
    current total, and refusing on it withholds a healthy series.

    Zero-valued periods are excluded from the containment test. ``0 <= X`` holds
    trivially, so a concept reporting no activity in a shared period would
    otherwise manufacture containment evidence out of an absence.
    """
    shared_all = sorted(set(series_a) & set(series_b))
    if not shared_all:
        return BRANCH_SUMMED, shared_all, "no shared periods; disjoint by construction"
    if cutoff is None:
        cutoff = _frontier_cutoff(series_a, series_b)
    shared = [p for p in shared_all if not cutoff or (p[1] and p[1] >= cutoff)]
    if not shared:
        return (BRANCH_SUMMED, shared_all,
                "all {} shared period(s) predate the live window; historical overlap "
                "already owned by the era map".format(len(shared_all)))

    if all(_close(series_a[p], series_b[p]) for p in shared):
        return (BRANCH_COLLAPSED, shared,
                "identical across all {} live shared period(s)".format(len(shared)))

    informative = [p for p in shared if series_a[p] or series_b[p]]
    if len(informative) < MIN_SHARED_PERIODS_FOR_CONTAINMENT:
        return (BRANCH_SUMMED, shared,
                "only {} live shared period(s) carry activity; too thin to infer "
                "containment".format(len(informative)))
    a_le = all(series_a[p] <= series_b[p] or _close(series_a[p], series_b[p]) for p in informative)
    b_le = all(series_b[p] <= series_a[p] or _close(series_b[p], series_a[p]) for p in informative)
    if a_le or b_le:
        smaller, larger = ("a", "b") if a_le else ("b", "a")
        return (BRANCH_REFUSED, informative,
                "{} is at or below {} in all {} live periods carrying activity; "
                "subset not ruled out".format(smaller, larger, len(informative)))
    return BRANCH_SUMMED, shared, "values differ in both directions; distinct instruments"


def build_series_map(indexed, concepts, unit_filter=("USD",)):
    """{concept: {(start, end): value}} for the given concepts."""
    from .facts_api import dedupe_latest_filed
    out = {}
    for c in concepts:
        rows = [f for f in indexed.get(c, []) if f.unit in unit_filter and f.period_start]
        if rows:
            out[c] = {(f.period_start, f.period_end): f.value
                      for f in dedupe_latest_filed(rows)}
    return out


class IssuanceResolution:
    def __init__(self, status, contributing, collapsed, verdicts, detail):
        self.status = status
        self.contributing = tuple(contributing)
        self.collapsed = tuple(collapsed)
        self.verdicts = tuple(verdicts)
        self.detail = detail

    @property
    def is_refused(self):
        return self.status == STATUS_REFUSED

    def total_for(self, series_map, period_key):
        """Derived total for one period, or None when refused/absent."""
        if self.is_refused:
            return None
        vals = [series_map[c][period_key] for c in self.contributing
                if c in series_map and period_key in series_map[c]]
        return sum(vals) if vals else None

    def __repr__(self):
        return "IssuanceResolution({} contributing={})".format(
            self.status, len(self.contributing))


def resolve_total(indexed, resolution, unit_filter=("USD",)):
    """Apply R-B6-1 to a debt Resolution, producing a derived-total recipe."""
    concepts = sorted(resolution.candidates_present)
    if not concepts:
        return IssuanceResolution(STATUS_OK, (), (), (), "no debt concept present")
    series_map = build_series_map(indexed, concepts, unit_filter)
    excluded = sorted(c for c in series_map if c in COUNTERPARTY_CONCEPTS)
    # Net-presentation concepts are ineligible for a gross sum, everywhere (R-CD2-1).
    excluded += sorted(c for c in series_map if is_net_presentation(c))
    # Branch b': a generic total supersedes its own named components — but only
    # over periods they actually SHARE. A stale total must not suppress a live
    # component: WULF's ProceedsFromIssuanceOfDebt ends in 2024 while
    # ProceedsFromShortTermDebt carries the current $92,750,000, and a global
    # exclusion silently deleted the only live figure it had.
    for total, components in GENERIC_TOTALS.items():
        if total not in series_map:
            continue
        total_periods = set(series_map[total])
        for c in components:
            if c in series_map and (total_periods & set(series_map[c])):
                excluded.append(c)
    for c in set(excluded):
        series_map.pop(c, None)
    excluded = sorted(set(excluded))
    live = sorted(series_map)
    if not live:
        return IssuanceResolution(STATUS_OK, (), tuple(excluded), (),
                                  "only counterparty concepts present; no instrument line")
    if len(live) == 1:
        return IssuanceResolution(
            STATUS_OK, live, tuple(excluded), (),
            "single concept {}{}".format(
                live[0], "; counterparty views excluded: " + ", ".join(excluded) if excluded else ""))

    cutoff = _frontier_cutoff(*series_map.values())
    verdicts, collapsed, refused = [], set(), []
    for i, a in enumerate(live):
        for b in live[i + 1:]:
            conflict = _cost_net_conflict(a, b, series_map[a], series_map[b], cutoff)
            if conflict:
                shared, detail = conflict
                verdicts.append(PairVerdict(a, b, BRANCH_REFUSED, shared, detail))
                refused.append((a, b, detail))
                continue
            branch, shared, detail = classify_pair(series_map[a], series_map[b], cutoff)
            verdicts.append(PairVerdict(a, b, branch, shared, detail))
            if branch == BRANCH_COLLAPSED:
                collapsed.add(b)          # keep the alphabetically-first member
            elif branch == BRANCH_REFUSED:
                refused.append((a, b, detail))

    if refused:
        return IssuanceResolution(
            STATUS_REFUSED, (), sorted(collapsed), verdicts,
            "; ".join("{} vs {}: {}".format(a, b, d) for a, b, d in refused))
    contributing = [c for c in live if c not in collapsed]
    branch = BRANCH_COLLAPSED if collapsed else (
        BRANCH_SUMMED if len(contributing) > 1 else BRANCH_SINGLE)
    return IssuanceResolution(
        STATUS_OK, contributing, sorted(collapsed), verdicts,
        "{}: {} contributing, {} collapsed".format(branch, len(contributing), len(collapsed)))


def overlap_matrix(indexed, resolution, unit_filter=("USD",)):
    """Period-by-period co-reporting table for verification (B6 deliverable)."""
    concepts = sorted(resolution.candidates_present)
    series_map = build_series_map(indexed, concepts, unit_filter)
    periods = sorted({p for s in series_map.values() for p in s})
    rows = []
    for p in periods:
        present = {c: series_map[c][p] for c in series_map if p in series_map[c]}
        if len(present) > 1:
            rows.append((p, present))
    return rows
