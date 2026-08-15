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


def classify_pair(series_a, series_b):
    """Decide which branch a co-reporting pair lands in.

    ``series_*`` are {period_key: value}. Only shared periods are considered —
    a concept that stopped reporting cannot be double-tagging a live one.
    """
    shared = sorted(set(series_a) & set(series_b))
    if not shared:
        return BRANCH_SUMMED, shared, "no shared periods; disjoint by construction"
    if all(_close(series_a[p], series_b[p]) for p in shared):
        return (BRANCH_COLLAPSED, shared,
                "identical across all {} shared period(s)".format(len(shared)))
    a_le = all(series_a[p] <= series_b[p] or _close(series_a[p], series_b[p]) for p in shared)
    b_le = all(series_b[p] <= series_a[p] or _close(series_b[p], series_a[p]) for p in shared)
    if (a_le or b_le) and len(shared) >= MIN_SHARED_PERIODS_FOR_CONTAINMENT:
        smaller, larger = ("a", "b") if a_le else ("b", "a")
        return (BRANCH_REFUSED, shared,
                "{} is at or below {} in all {} shared periods; subset not ruled out".format(
                    smaller, larger, len(shared)))
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
    live = sorted(series_map)
    if len(live) == 1:
        return IssuanceResolution(STATUS_OK, live, (), (),
                                  "single concept {}".format(live[0]))

    verdicts, collapsed, refused = [], set(), []
    for i, a in enumerate(live):
        for b in live[i + 1:]:
            branch, shared, detail = classify_pair(series_map[a], series_map[b])
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
