"""CD-GAP1 P1/P5 — why a name has no phase state, and what IS known about it.

`INSUFFICIENT-HISTORY` was doing too much work. It covered a builder with six
quarters and $1.16B of TTM capex, a foreign filer that reports annually, a name
whose concept mapping is refused, and a sidecar that is deliberately not
aggregated — four different situations, one label, and a dash in every numeric
column. A dash says "nothing is known here". For FRMI that was false: what is
unknown is its YoY, not its spending.

So the label is split by CAUSE, and every pre-eligible row publishes what it
does have — TTM level, quarters held, the date it becomes classifiable, and an
interim growth read that is clearly NOT a ladder verdict.

**The interim read is deliberately not a phase state.** It is annualised
half-over-half, computed off four quarters, with no dead-band and no
confirmation window. It cannot enter an aggregate, cannot raise an alert, and is
labelled `non-ladder` everywhere it appears. Its job is to stop the dashboard
saying "no information" about a name that is visibly tripling — not to
short-circuit the discipline the ladder exists to impose.

The arithmetic behind the countdown, measured rather than assumed: TTM needs 4
quarters, a TTM YoY needs 8, and the ladder needs `N_CONFIRM + 1 = 3` YoY points
to enter a state. So a name is classifiable at **10 contiguous quarters**.
"""
from datetime import date, timedelta

from . import phases, trend

# 4 for the first TTM, +4 for its year-ago comparison, +2 more YoY observations
# so the ladder has N_CONFIRM+1 = 3 of them.
QUARTERS_TO_CLASSIFY = 10

# 10-Q deadlines run 40 days (large accelerated) to 45 (everyone else) after
# period end. 45 is the conservative choice: a date that arrives and passes is
# a worse failure than one that arrives early, because the first looks like a
# missed filing and the second looks like a filing that came in on time.
FILING_LAG_DAYS = 45
QUARTER_DAYS = 91

CAUSE_THIN_MATURING = "THIN-MATURING"
CAUSE_FPI_ANNUAL = "FPI-ANNUAL-BASIS"
CAUSE_FROM_PROSE = "DERIVED-FROM-PROSE"
CAUSE_SIDECAR = "SIDECAR"
CAUSE_REFUSED = "REFUSED"
CAUSE_NO_DATA = "NO-DATA"
CAUSE_TAGGING_CEASED = "TAGGING-CEASED"

# Coverage statuses that mean the concept layer declined to produce a series.
REFUSAL_STATUSES = ("CAPEX-UNRESOLVED", "CAPEX-MULTILINE", "UNRESOLVED",
                    "UNRESOLVED-MULTILINE")


def _q_end(cq):
    y, n = cq.split("Q")
    m = int(n) * 3
    return date(int(y), m, {3: 31, 6: 30, 9: 30, 12: 31}[m])


def first_eligible(quarters, needed=QUARTERS_TO_CLASSIFY):
    """(date, quarters_short) when this name becomes classifiable, or None.

    An ESTIMATE, and published as one. It assumes the issuer keeps filing every
    quarter without a gap — which is exactly what a thin name most at risk of
    not doing. A gap pushes the date out; the count of quarters still short is
    the honest part and travels beside it.
    """
    if not quarters:
        return None, needed
    n = len(quarters)
    if n >= needed:
        return None, 0
    short = needed - n
    last = _q_end(max(quarters, key=trend._cq_sort))
    return last + timedelta(days=short * QUARTER_DAYS + FILING_LAG_DAYS), short


def expected_by(quarters, today=None):
    """(due_date, is_overdue) for this name's NEXT quarter. P5.

    "Absent" and "late" look identical on a dashboard and mean different things.
    A name whose series stops has a next filing due on a computable date; saying
    so turns a hole into a deadline.

    **Measured against the calendar, not against the panel frontier.** The first
    cut compared each issuer to the most advanced filer in the panel and flagged
    everyone behind it — which marked seven perfectly current names as late,
    because one off-calendar filer (NVIDIA closes January, Micron September) had
    already reached the next calendar quarter. Another issuer's fiscal year has
    no bearing on whether this one is late; only its own deadline does.
    """
    if not quarters:
        return None, False
    today = today or date.today()
    nxt = trend._cq_index(max(quarters, key=trend._cq_sort)) + 1
    y, n = (nxt - 1) // 4, (nxt - 1) % 4 + 1
    due = _q_end("{}Q{}".format(y, n)) + timedelta(days=FILING_LAG_DAYS)
    return due, due < today


def interim_growth(quarters):
    """Annualised half-over-half growth — a NON-LADDER read. None if <4 quarters.

    Deliberately the crudest defensible measure: the most recent two quarters
    against the two before them, annualised. No dead-band, no confirmation, no
    state. It exists so a row that is visibly growing does not render as a dash,
    and it is labelled non-ladder wherever it appears so it can never be mistaken
    for a phase verdict.
    """
    qs = sorted(quarters, key=trend._cq_sort)
    if len(qs) < 4:
        return None
    idx = [trend._cq_index(q) for q in qs[-4:]]
    if idx[-1] - idx[0] != 3:          # the four must be contiguous
        return None
    prior = quarters[qs[-4]] + quarters[qs[-3]]
    recent = quarters[qs[-2]] + quarters[qs[-1]]
    if prior <= 0:
        return None
    return (recent / prior) ** 2 - 1.0     # two halves -> annualised


# A name whose concept still carries facts, but whose newest fact is older than
# this, has stopped tagging rather than merely fallen behind. Two years is well
# past any filing deadline, annual filers included.
STALE_TAGGING_DAYS = 730


def classify(entity, quarters, coverage=(), state=None, today=None,
             last_tagged=None):
    """Why this name has no phase state, plus everything that IS known.

    `last_tagged` is the newest period end the concept layer saw AT ALL, even
    where no quarter was derivable. It separates "reports annually" from
    "stopped reporting", which look identical from the quarterly series and are
    not the same fact — Alibaba's capex concept carries data through 2020 and
    nothing after, which is not an annual-basis issuer.
    """
    quarters = quarters or {}
    n = len(quarters)
    cov = tuple(coverage or ())

    if state and state in phases.REAL_STATES:
        return None                    # classified; nothing to explain

    if any(c in REFUSAL_STATUSES for c in cov):
        cause = CAUSE_REFUSED
    elif entity.bucket == "sidecar":
        cause = CAUSE_SIDECAR
    elif entity.bucket == "fpi":
        cause = CAUSE_FPI_ANNUAL
        if last_tagged:
            age = ((today or date.today()) - date.fromisoformat(last_tagged)).days
            if age > STALE_TAGGING_DAYS:
                cause = CAUSE_TAGGING_CEASED
    elif not n:
        cause = CAUSE_NO_DATA
    else:
        cause = CAUSE_THIN_MATURING

    due, overdue = expected_by(quarters, today=today)
    ttm_map = trend.ttm_by_quarter(quarters) if n >= 4 else {}
    tq = sorted(ttm_map, key=trend._cq_sort)
    eligible_on, short = first_eligible(quarters)
    growth = interim_growth(quarters)
    return {
        "cause": cause,
        "quarters_held": n,
        "quarters_short": short,
        "first_eligible": eligible_on.isoformat() if eligible_on else None,
        "ttm": ttm_map[tq[-1]] if tq else None,
        "latest_quarter": max(quarters, key=trend._cq_sort) if n else None,
        "last_tagged": last_tagged,
        "interim_growth": growth,
        "interim_basis": "annualised half-over-half, non-ladder" if growth is not None else None,
        "expected_by": (due.isoformat() if due else None),
        "filing_overdue": overdue,
        "coverage": list(cov),
    }
