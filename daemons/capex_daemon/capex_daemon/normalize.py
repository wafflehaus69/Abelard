"""Discrete-quarter derivation and calendar alignment.

Cash-flow facts arrive as fiscal year-to-date cumulatives for most issuers, so a
discrete quarter is usually a difference of two YTD periods sharing a start date.
Microsoft reports native three-month cash flows; nearly everyone else does not.
**The fiscal-Q4 discrete quarter is never natively tagged by anyone** — it is
always FY minus 9M.

Series key on ``start``/``end`` and never on ``fy``/``fp``: those describe the
report a fact appeared in, not the period it covers. Meta's FY2023 fact carries
``fy=2025`` because it appears as a comparative in the 2025 filing.

SEC's own ``frame`` label is used where present but not trusted for off-calendar
filers: Snowflake's February-April period is labelled ``CY2026Q1`` (January-March),
a one-month shift baked into the API. Offsets are computed and carried per row;
the tolerance for treating an offset quarter as calendar-aligned is OPEN pending
an observed distribution (ruling (b), E8).
"""
from datetime import date

from . import config

DERIVATION_NATIVE = "native"
DERIVATION_YTD_DIFF = "ytd-diff"


class Row:
    """One normalized discrete quarter. Provenance is mandatory (E7)."""

    __slots__ = ("period_start", "period_end", "value", "unit", "scale_basis",
                 "resolved_concept", "derivation", "source_leg", "accession",
                 "calendar_quarter", "calendar_offset_days")

    def __init__(self, period_start, period_end, value, unit, scale_basis,
                 resolved_concept, derivation, source_leg, accession=None):
        self.period_start = period_start
        self.period_end = period_end
        self.value = value
        self.unit = unit
        self.scale_basis = scale_basis
        self.resolved_concept = resolved_concept
        self.derivation = derivation
        self.source_leg = source_leg
        self.accession = accession
        self.calendar_quarter, self.calendar_offset_days = calendar_align(period_end)

    def __repr__(self):
        return "Row({} {} {} [{}])".format(
            self.calendar_quarter, self.period_end, self.value, self.derivation)


def _days(start, end):
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def _in(days, window):
    return window[0] <= days <= window[1]


def calendar_align(period_end):
    """(calendar quarter label, offset in days from that quarter's true end)."""
    e = date.fromisoformat(period_end)
    q = (e.month - 1) // 3 + 1
    true_end_month = q * 3
    last_day = {3: 31, 6: 30, 9: 30, 12: 31}[true_end_month]
    true_end = date(e.year, true_end_month, last_day)
    return "{}Q{}".format(e.year, q), (e - true_end).days


def discrete_quarters(facts_with_concept, unit="USD", source_leg="companyfacts",
                      allow_decreasing=False, dropped=None):
    """Derive discrete quarters from a resolved series.

    ``facts_with_concept`` is [(ApiFact, resolved_concept)] as produced by
    ``tagmap.series_facts``. Native three-month facts pass through; cumulative
    periods are differenced within a fiscal-year cohort (facts sharing a start).

    Pass a list as ``dropped`` to collect non-monotonic cumulative events that
    were refused; ``allow_decreasing=True`` disables the guard for callers that
    genuinely expect a decreasing cumulative (there are none today).
    """
    rows = {}
    cohorts = {}
    dropped = dropped if dropped is not None else []
    for f, concept in facts_with_concept:
        if not f.period_start or f.value is None:
            continue
        d = _days(f.period_start, f.period_end)
        if _in(d, config.QUARTER_DAYS):
            rows[f.period_end] = Row(f.period_start, f.period_end, f.value, unit,
                                     getattr(f, "scale_basis", "api-absolute"),
                                     concept, DERIVATION_NATIVE, source_leg)
        cohorts.setdefault(f.period_start, []).append((f.period_end, d, f.value, concept))

    for start, items in cohorts.items():
        items.sort()
        for i in range(1, len(items)):
            (e0, d0, v0, _), (e1, d1, v1, concept) = items[i - 1], items[i]
            if not _in(d1 - d0, config.QUARTER_DAYS) or e1 in rows:
                continue
            # A year-to-date cumulative cannot decrease. When it does, the issuer
            # re-tagged the item mid-year and backfilled the later period with a
            # different (often zero) value — WULF's ProceedsFromConvertibleDebt
            # reads $975,329,000 at 9M and $0 at FY, and differencing turns that
            # into a phantom -$975M quarter that then propagates into every
            # aggregate downstream. The derived quarter is not trustworthy, so it
            # is withheld and counted, never published as a negative (E1).
            if v1 < v0 and not allow_decreasing:
                dropped.append((concept, e0, e1, v0, v1))
                continue
            rows[e1] = Row(e0, e1, v1 - v0, unit, "api-absolute", concept,
                           DERIVATION_YTD_DIFF, source_leg)
    return [rows[k] for k in sorted(rows)]


def consecutive_run(rows):
    """Length of the unbroken quarterly run ending at the most recent row."""
    if not rows:
        return 0
    ends = [date.fromisoformat(r.period_end) for r in rows]
    run = 1
    for a, b in zip(reversed(ends), list(reversed(ends))[1:]):
        if _in((a - b).days, config.QUARTER_DAYS) or (a - b).days <= 100:
            run += 1
        else:
            break
    return run


def ttm(rows, window=None):
    """Trailing-twelve-month sum over the newest `window` quarters, or None."""
    window = window or config.ANCHOR_WINDOW_QUARTERS
    if len(rows) < window:
        return None
    return sum(r.value for r in rows[-window:])


def ttm_window(rows, window=None):
    window = window or config.ANCHOR_WINDOW_QUARTERS
    if len(rows) < window:
        return None
    sel = rows[-window:]
    return sel[0].period_start, sel[-1].period_end
