"""PS-1 Phase 3 — analytics over the adjusted view.

**Pure functions.** No database, no clock, no network inside any of them. The
one I/O boundary is :func:`load_panel`, kept at the top and marked, so every
statistic below can be reproduced from a literal dict in a test rather than
from a store that has to be built first.

**Stdlib only.** ``abelard_common`` declares one runtime dependency
(``requests``), and a shared library should not grow numpy so that one consumer
can compute a five-point regression. Everything here is small by construction —
five-point OLS, a rolling mean, a cross-sectional average. CR-1 can reach for
numpy on the 500x500 matrices; these are the primitives underneath.

**The momentum outputs are DESCRIPTIVE, and that is a constraint, not a caveat.**
Handoff §2-E and §3.3: a basket selected on momentum is just a momentum
portfolio, and the prototype's name-level version lost three straight months
when the leverage cycle turned. So momentum *tags activity*; it never gates
membership. Nothing in this module filters, ranks-and-cuts, or selects on a
momentum value, and nothing downstream in PS-1 may either — that is CR-1's
Layer-1 economic-eligibility test to own, and only in that order.

**Missing data returns None; it never returns a number.** A name with 40
sessions has no 200-day moving average, and inventing one from what is there is
the failure this whole substrate exists to prevent. Callers get ``None`` and
decide; the batch helpers report how many they got.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

# MA200, MA100, MA50, MA30, Last -- the ladder rungs, longest window first.
LADDER_WINDOWS = (200, 100, 50, 30)
# Evenly spaced on [0, 1]: the x-axis is "window shortening", not elapsed time.
LADDER_X = (0.0, 0.25, 0.5, 0.75, 1.0)
# Mando's scaling. The raw slope of a 0..1 regression is inconveniently small;
# x10 puts a strong ladder in the low tens, which is how he reads it.
LADDER_SCALE = 10.0

MOMENTUM_LOOKBACK = 63
MOMENTUM_SKIP = 5


# ------------------------------------------------------------- I/O boundary --

def load_panel(
    con: sqlite3.Connection,
    instrument_ids: Sequence[str],
    start: str,
    end: str,
    factor_version: int | None = None,
) -> dict[str, dict[str, float]]:
    """instrument_id -> {date: adj_close}. **The only I/O in this module.**

    Reads ``adjusted_view``, which excludes quarantined and vendor-null sessions
    by construction — a statistic is never computed on a price the store has
    refused to call a fact.

    ``factor_version`` pins the answer to a specific factor generation. Left
    None it takes whatever version each row currently carries, which is what a
    live dashboard wants; set it to reproduce a number published under an older
    generation, which is what an as-of audit wants.
    """
    out: dict[str, dict[str, float]] = {}
    sql = ("SELECT date, adj_close FROM adjusted_view"
           " WHERE instrument_id=? AND date>=? AND date<=? AND adj_close IS NOT NULL")
    params_tail: tuple = ()
    if factor_version is not None:
        sql += " AND factor_version=?"
        params_tail = (factor_version,)
    sql += " ORDER BY date"
    for iid in instrument_ids:
        rows = con.execute(sql, (iid, start, end) + params_tail).fetchall()
        if rows:
            out[iid] = {r[0]: r[1] for r in rows}
    return out


# ------------------------------------------------------------------ returns --

def log_returns(closes: Mapping[str, float] | Sequence[float]) -> list[float]:
    """Session-over-session log returns.

    Log rather than simple, because these get summed across time and averaged
    across names; a simple return does neither correctly.
    """
    series = _ordered(closes)
    out = []
    for i in range(1, len(series)):
        a, b = series[i - 1], series[i]
        if a is None or b is None or a <= 0 or b <= 0:
            continue
        out.append(math.log(b / a))
    return out


def dated_log_returns(closes: Mapping[str, float]) -> dict[str, float]:
    """Log returns keyed by the session they belong to — the shape needed to
    align two names that do not share every date."""
    dates = sorted(closes)
    out: dict[str, float] = {}
    for i in range(1, len(dates)):
        a, b = closes[dates[i - 1]], closes[dates[i]]
        if a and b and a > 0 and b > 0:
            out[dates[i]] = math.log(b / a)
    return out


def aligned_returns(
    panel: Mapping[str, Mapping[str, float]],
    min_sessions: int = 2,
) -> tuple[list[str], dict[str, list[float]]]:
    """Return series restricted to dates EVERY name has.

    Intersecting is the honest default for a correlation input — but it is also
    how a panel silently dates itself to its stalest member (CR-R0 §R1.5, where
    intersecting 497 names truncated the panel to 2026-07-23 with nothing on
    screen saying so). The common dates are therefore returned alongside the
    series, so a caller can see the window it actually got rather than assume
    the one it asked for.
    """
    per_name = {k: dated_log_returns(v) for k, v in panel.items()}
    per_name = {k: v for k, v in per_name.items() if len(v) >= min_sessions - 1}
    if not per_name:
        return [], {}
    common = set.intersection(*(set(v) for v in per_name.values()))
    dates = sorted(common)
    return dates, {k: [v[d] for d in dates] for k, v in per_name.items()}


# ----------------------------------------------------------- moving averages --

def moving_average(closes: Sequence[float], window: int) -> float | None:
    """Mean of the last ``window`` sessions, or None if there are not that many.

    Not a partial average over what happens to be present: a 40-session name has
    no MA200, and a number computed from 40 sessions and labelled MA200 is the
    exact species of quiet wrongness this substrate is built against.
    """
    if window <= 0 or len(closes) < window:
        return None
    tail = closes[-window:]
    if any(c is None for c in tail):
        return None
    return sum(tail) / window


def ma_ladder(closes: Mapping[str, float] | Sequence[float]) -> list[float] | None:
    """The five rungs as ratios to MA200: ``y_k = MA_k / MA200 - 1``.

    Order is [MA200, MA100, MA50, MA30, Last], so ``y[0]`` is always exactly 0
    by construction — MA200 measured against itself. Returns None if the name
    is too short for MA200, or if MA200 is non-positive.
    """
    series = [c for c in _ordered(closes) if c is not None]
    ma200 = moving_average(series, 200)
    if not ma200 or ma200 <= 0:
        return None
    rungs: list[float] = []
    for w in LADDER_WINDOWS:
        ma = moving_average(series, w)
        if ma is None:
            return None
        rungs.append(ma / ma200 - 1.0)
    rungs.append(series[-1] / ma200 - 1.0)
    return rungs


def ols_slope(y: Sequence[float], x: Sequence[float] = LADDER_X) -> float:
    """Least-squares slope of y on x. Five points, closed form, no dependency."""
    n = len(y)
    if n < 2 or n != len(x):
        raise ValueError("ols_slope needs matching sequences of length >= 2")
    xb = sum(x) / n
    yb = sum(y) / n
    den = sum((xi - xb) ** 2 for xi in x)
    if den == 0:
        raise ValueError("ols_slope: x has no spread")
    num = sum((xi - xb) * (yi - yb) for xi, yi in zip(x, y))
    return num / den


def ladder_score(ladder: Sequence[float]) -> float:
    """Mando's momentum score: the ladder's OLS slope, times ten.

    Pinned in tests against two hand-computed ladders — FBRX
    (0, 20.2%, 69.2%, 126.6%, 136.9%) -> 15.2 and MRNA
    (0, 22.8%, 49.7%, 62.3%, 190.4%) -> 16.8. Both are asserted from the LADDER
    values, not from prices, so the test pins the scoring formula and nothing
    else; whether a given price history produces that ladder is a separate
    question with a separate test.
    """
    return ols_slope(ladder) * LADDER_SCALE


@dataclass(frozen=True)
class Momentum:
    ladder: list[float]
    score: float


def momentum_ma_ladder(
    closes: Mapping[str, float] | Sequence[float]
) -> Momentum | None:
    """The ladder and its score together.

    Both, deliberately. The score alone hides its shape: a steady climb and a
    single terminal spike can produce the same slope, and MRNA's ladder — flat
    through MA30 then +190% at the last rung — is exactly that shape. A reader
    who sees only 16.8 cannot tell which they are looking at.

    DESCRIPTIVE OUTPUT. See the module docstring: this tags activity, it does
    not select.
    """
    ladder = ma_ladder(closes)
    if ladder is None:
        return None
    return Momentum(ladder=ladder, score=ladder_score(ladder))


def momentum_return_63_skip_5(
    closes: Mapping[str, float] | Sequence[float],
    lookback: int = MOMENTUM_LOOKBACK,
    skip: int = MOMENTUM_SKIP,
) -> float | None:
    """The handoff's second momentum column: a ``lookback``-session return that
    stops ``skip`` sessions short of today.

    The skip is not decoration. Short-horizon reversal contaminates the most
    recent week, so a 63-day return measured to yesterday partly measures
    mean-reversion rather than trend. Handoff §5.2 uses exactly this form.

    Returns a LOG return, matching :func:`log_returns`, so it composes.
    """
    series = [c for c in _ordered(closes) if c is not None and c > 0]
    need = lookback + skip + 1
    if len(series) < need:
        return None
    end = series[-(skip + 1)]
    begin = series[-(skip + 1 + lookback)]
    if begin <= 0 or end <= 0:
        return None
    return math.log(end / begin)


# ------------------------------------------------------------------ baskets --

def ew_basket_returns(
    members: Mapping[str, Mapping[str, float]],
    leave_out: str | None = None,
) -> dict[str, float]:
    """Equal-weight basket log return per session, optionally leave-one-out.

    ``leave_out`` exists because a name must never sit inside its own
    benchmark. Correlating a member against a basket that contains it
    manufactures agreement in proportion to its own weight — with 20 members
    that is a floor of about 1/20 of the name's own variance, which looks like
    signal and is arithmetic. Handoff §5.2 makes leave-one-out the primary
    construction for exactly this reason.

    Equal-weight, not cap-weight, so the basket describes the sector rather than
    its largest member. A session is included only where at least two members
    have a return, and the average is over whoever is present that day —
    composition therefore varies, which is why :func:`basket_composition` exists
    to report it rather than leaving a caller to assume stability (E14).
    """
    per_name = {
        k: dated_log_returns(v) for k, v in members.items() if k != leave_out
    }
    per_date: dict[str, list[float]] = {}
    for rets in per_name.values():
        for d, r in rets.items():
            per_date.setdefault(d, []).append(r)
    return {d: sum(v) / len(v) for d, v in per_date.items() if len(v) >= 2}


def basket_composition(
    members: Mapping[str, Mapping[str, float]],
    leave_out: str | None = None,
) -> dict[str, int]:
    """How many members actually contributed to each session's basket return."""
    per_name = {
        k: dated_log_returns(v) for k, v in members.items() if k != leave_out
    }
    counts: dict[str, int] = {}
    for rets in per_name.values():
        for d in rets:
            counts[d] = counts.get(d, 0) + 1
    return {d: n for d, n in counts.items() if n >= 2}


def loo_basket_for_each(
    members: Mapping[str, Mapping[str, float]]
) -> dict[str, dict[str, float]]:
    """One leave-one-out basket per member — the shape §5.2 consumes."""
    return {name: ew_basket_returns(members, leave_out=name) for name in members}


# ------------------------------------------------------------------ helpers --

def _ordered(closes: Mapping[str, float] | Sequence[float]) -> list[float]:
    """Accept either a date-keyed mapping or an already-ordered sequence.

    A mapping is sorted by date; a sequence is trusted as given. Anything else
    is a caller error and raises rather than guessing an order — silently
    reversing a price series would invert every momentum sign in the system.
    """
    if isinstance(closes, Mapping):
        return [closes[d] for d in sorted(closes)]
    if isinstance(closes, (list, tuple)):
        return list(closes)
    raise TypeError(
        "closes must be a date-keyed Mapping or an ordered sequence, got {}"
        .format(type(closes).__name__))
