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

**Every window is measured in SESSIONS, never in rows.** This module's first
version walked adjacent rows, which is the same thing only when the series has
no holes — and holes are normal here: a ``vendor_null``, a quarantined session,
anything ``adjusted_view`` excludes. Measured on the live store, **464 of 517
names** had a hole inside their own span (all of them the 2026-08-28 mass
vendor-null), so `log(c[d_i]/c[d_{i-1}])` was emitting a two-session return
keyed to one date, `ew_basket_returns` was averaging it against genuine
single-session returns, and a 200-row mean with k holes was spanning 200+k
sessions while still being labelled MA200.

Every function that spans time therefore takes ``sessions`` — the exchange
calendar's ordered session list from ``calendar.py`` — and either refuses or
reports when the data does not cover it. A return is emitted only between
consecutive sessions; a skipped span is COUNTED and returned, never silently
bridged.
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


def dated_log_returns(
    closes: Mapping[str, float],
    sessions: Sequence[str],
) -> tuple[dict[str, float], list[tuple[str, str]]]:
    """Log returns keyed by the session they belong to, plus the spans skipped.

    Returns ``(returns, gaps)``. A return is emitted **only** where the two
    dates are consecutive entries in ``sessions``; anywhere they are not, the
    span is appended to ``gaps`` as ``(from, to)`` and no return is produced.

    Bridging a hole is not a smaller error than omitting one — it is a larger
    one, because the result looks like a single-session return and gets averaged
    with real ones. Callers that genuinely only want the returns write
    ``dated_log_returns(...)[0]``, and the explicitness is the point.
    """
    order = {d: i for i, d in enumerate(sessions)}
    dates = sorted(closes)
    out: dict[str, float] = {}
    gaps: list[tuple[str, str]] = []
    for i in range(1, len(dates)):
        prev, cur = dates[i - 1], dates[i]
        a, b = closes[prev], closes[cur]
        pi, ci = order.get(prev), order.get(cur)
        if pi is None or ci is None or ci - pi != 1:
            gaps.append((prev, cur))
            continue
        if a and b and a > 0 and b > 0:
            out[cur] = math.log(b / a)
        else:
            gaps.append((prev, cur))
    return out, gaps


def aligned_returns(
    panel: Mapping[str, Mapping[str, float]],
    sessions: Sequence[str],
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
    per_name = {k: dated_log_returns(v, sessions)[0] for k, v in panel.items()}
    per_name = {k: v for k, v in per_name.items() if len(v) >= min_sessions - 1}
    if not per_name:
        return [], {}
    common = set.intersection(*(set(v) for v in per_name.values()))
    dates = sorted(common)
    return dates, {k: [v[d] for d in dates] for k, v in per_name.items()}


# ----------------------------------------------------------- moving averages --

def moving_average(
    closes: Mapping[str, float],
    window: int,
    sessions: Sequence[str],
    as_of: str | None = None,
) -> float | None:
    """Mean of the last ``window`` **sessions** ending at ``as_of``.

    Sessions, not rows. A 200-row mean over a series with k holes spans 200+k
    sessions and is not an MA200 — it is a longer average wearing the label of a
    shorter one, and it moves differently. Returns None unless every one of the
    last ``window`` calendar sessions is present in ``closes``.

    Not a partial average over what happens to be there, either: a 40-session
    name has no MA200, and a number built from 40 sessions and labelled MA200 is
    the exact quiet wrongness this substrate is built against.
    """
    if window <= 0 or not sessions:
        return None
    end = as_of or max(closes) if closes else None
    if end is None or end not in sessions:
        return None
    idx = sessions.index(end)
    if idx + 1 < window:
        return None
    need = sessions[idx + 1 - window: idx + 1]
    vals = []
    for d in need:
        c = closes.get(d)
        if c is None:
            return None          # a hole inside the window
        vals.append(c)
    return sum(vals) / window


def ma_ladder(
    closes: Mapping[str, float],
    sessions: Sequence[str],
    as_of: str | None = None,
) -> list[float] | None:
    """The five rungs as ratios to MA200: ``y_k = MA_k / MA200 - 1``.

    Order is [MA200, MA100, MA50, MA30, Last], so ``y[0]`` is exactly 0 by
    construction — MA200 measured against itself.

    ``Last`` is the **close** at ``as_of``. Worth stating because it is a real
    difference from reading the number off a screen: a live ladder pairs moving
    averages computed to the prior close with an intraday Last, and the two can
    diverge materially. Measured on MRNA, Mando's pinned ladder reproduces its
    MA100/MA50/MA30 rungs exactly as of 2026-08-31 while its Last rung implies
    151.97 against that session's close of 140.34 — 7.7%, worth about 1.8 points
    of score. A stored, versioned system has to use the close, or the same as-of
    date yields a different number every time it is recomputed.

    Returns None if any rung cannot be built. Use :func:`ladder_status` to find
    out which one.
    """
    status = ladder_status(closes, sessions, as_of)
    if not status.ok:
        return None
    return status.ladder


@dataclass(frozen=True)
class LadderStatus:
    """Which rungs could be built, and why any could not."""

    as_of: str | None
    ladder: list[float] | None
    failed: list[str]
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.ladder is not None and not self.failed


def ladder_status(
    closes: Mapping[str, float],
    sessions: Sequence[str],
    as_of: str | None = None,
) -> LadderStatus:
    """The companion to :func:`ma_ladder`: report the failure, do not just
    return None.

    A caller looking at 500 names needs to know whether a missing ladder means
    "too short" or "a hole inside MA30", because those have different fixes —
    one waits for history, the other is a hole to fill.
    """
    if not closes:
        return LadderStatus(None, None, ["MA200"], "no closes")
    end = as_of or max(closes)
    if end not in sessions:
        return LadderStatus(end, None, ["MA200"],
                            "as_of {} is not a trading session".format(end))

    # Every rung is tested, and none short-circuits. The windows nest — MA30's
    # sessions are a subset of MA200's — so a hole near the right edge fails
    # EVERY rung, and an implementation that returned at the first failure would
    # always report MA200 and never reveal how recent the hole was. The shortest
    # failing window is the diagnostic that matters: it says whether the fix is
    # to wait for history or to fill a hole.
    mas: dict[int, float | None] = {
        w: moving_average(closes, w, sessions, end) for w in LADDER_WINDOWS
    }
    failed = ["MA{}".format(w) for w in LADDER_WINDOWS if mas[w] is None]
    last = closes.get(end)
    if last is None:
        failed.append("Last")

    held = sum(1 for d in sessions[:sessions.index(end) + 1] if d in closes)
    if failed:
        shortest = min((w for w in LADDER_WINDOWS if mas[w] is None), default=None)
        if held < 200:
            detail = ("only {} sessions held through {}; MA200 needs 200"
                      .format(held, end))
        else:
            detail = ("hole inside the last {} sessions (failing rungs: {})"
                      .format(shortest, ", ".join(failed)))
        return LadderStatus(end, None, failed, detail)

    ma200 = mas[200]
    if not ma200 or ma200 <= 0:
        return LadderStatus(end, None, ["MA200"], "MA200 is non-positive")
    rungs = [0.0]
    rungs.extend(mas[w] / ma200 - 1.0 for w in LADDER_WINDOWS[1:])
    rungs.append(last / ma200 - 1.0)
    return LadderStatus(end, rungs, [])


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
    closes: Mapping[str, float],
    sessions: Sequence[str],
    as_of: str | None = None,
) -> Momentum | None:
    """The ladder and its score together.

    Both, deliberately. The score alone hides its shape: a steady climb and a
    single terminal spike can produce the same slope, and MRNA's ladder — flat
    through MA30 then +190% at the last rung — is exactly that shape. A reader
    who sees only 16.8 cannot tell which they are looking at.

    DESCRIPTIVE OUTPUT. See the module docstring: this tags activity, it does
    not select.
    """
    ladder = ma_ladder(closes, sessions, as_of)
    if ladder is None:
        return None
    return Momentum(ladder=ladder, score=ladder_score(ladder))


def momentum_return_63_skip_5(
    closes: Mapping[str, float],
    sessions: Sequence[str],
    as_of: str | None = None,
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
    if not closes:
        return None
    anchor = as_of or max(closes)
    if anchor not in sessions:
        return None
    i = sessions.index(anchor)
    end_i, begin_i = i - skip, i - skip - lookback
    if begin_i < 0:
        return None
    end_d, begin_d = sessions[end_i], sessions[begin_i]
    end, begin = closes.get(end_d), closes.get(begin_d)
    # Both endpoints must be real sessions we hold. A 63-session window whose
    # endpoint fell in a hole is not a 63-session return, and sliding to the
    # nearest held row would quietly change the window it claims to measure.
    if end is None or begin is None or begin <= 0 or end <= 0:
        return None
    return math.log(end / begin)


# ------------------------------------------------------------------ baskets --

def ew_basket_returns(
    members: Mapping[str, Mapping[str, float]],
    sessions: Sequence[str],
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
        k: dated_log_returns(v, sessions)[0]
        for k, v in members.items() if k != leave_out
    }
    per_date: dict[str, list[float]] = {}
    for rets in per_name.values():
        for d, r in rets.items():
            per_date.setdefault(d, []).append(r)
    return {d: sum(v) / len(v) for d, v in per_date.items() if len(v) >= 2}


def basket_composition(
    members: Mapping[str, Mapping[str, float]],
    sessions: Sequence[str],
    leave_out: str | None = None,
) -> dict[str, int]:
    """How many members contributed a CONSECUTIVE-SESSION return each session.

    A member whose only available return spans a hole is not counted, because it
    is not a single-session return and averaging it with ones that are is the
    defect this phase exists to remove.
    """
    per_name = {
        k: dated_log_returns(v, sessions)[0]
        for k, v in members.items() if k != leave_out
    }
    counts: dict[str, int] = {}
    for rets in per_name.values():
        for d in rets:
            counts[d] = counts.get(d, 0) + 1
    return {d: n for d, n in counts.items() if n >= 2}


def loo_basket_for_each(
    members: Mapping[str, Mapping[str, float]],
    sessions: Sequence[str],
) -> dict[str, dict[str, float]]:
    """One leave-one-out basket per member — the shape §5.2 consumes."""
    return {name: ew_basket_returns(members, sessions, leave_out=name)
            for name in members}


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
