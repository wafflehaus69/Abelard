"""PS-1 Phase 2 — raw reconstruction, factor series, and the corruption detector.

Pure functions. No I/O, no database, no clock. Everything here is a
deterministic transform of (vendor bars, declared corporate actions), which is
what makes the MNST case reproducible in a test instead of only in production.

THE THREE LAYERS, in the order they run:

1. **Raw reconstruction.** The vendor's ``close`` is split-adjusted
   retroactively and dividend-*un*adjusted, so the true traded price is::

       raw(d) = close(d) * PROD{ ratio(e) : e is a split effective AFTER d }

   O/H/L carry the same factor; volume carries its inverse, because the vendor
   split-adjusts volume too (verified on AAPL's clean 4:1 of 2020-08-31: its
   volume is smooth across the boundary while price is retro-halved).

   Sanity check that pins the direction: AAPL 2015-01-05 ``close`` = 26.5625
   with one 4:1 split effective 2020-08-31, so raw = 26.5625 * 4 = 106.25 —
   and AAPL did trade near $106 that day. Multiply, never divide.

2. **Factor series.** Split factors from declared splits; dividend factors on
   the CRSP convention, ``f(e) = 1 - amount(e) / raw_close(e-1)``, applied
   cumulatively backward. The total-return adjusted price is::

       adj(d) = raw(d) * PROD{ f(e) : e is an ex-date AFTER d }

3. **The corruption detector.** After step 1 a correctly-served series has NO
   split step left in it — reconstruction removed it. A step that survives means
   the vendor's own closes disagree about which scale they are on.

   This is the check that catches MNST and the one a recompute-vs-``adjclose``
   comparison cannot, because both sides of that comparison derive from the same
   per-session ``close``. MNST declares one 2:1 effective 2026-08-11 but serves
   only 6 of 21 pre-split sessions on the post-split scale; reconstruction
   multiplies every pre-split session by 2, so the 15 already-raw sessions come
   out at ~195 while the 6 adjusted ones come out at ~95, and the ~2.0 ratio
   steps survive in the reconstructed series.

   **We do not repair it.** There is no way to know from inside the series which
   side is correct, so the affected span is quarantined — written with
   ``status='quarantined'``, never as a fact — and refetched until the vendor's
   window is internally consistent.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

# A reconstructed price is a float product; two paths to the same value differ in
# the last bits. Relative tolerance for "these are the same fact".
FACT_EPS = 1e-9

# A surviving step is NOT a clean 2.0 -- the scale error is exact, but the
# session's own price move rides on top of it. MNST 97.50 -> 47.72 is 0.4895,
# not 0.5000, because the stock also moved -2.1% that day. So the test divides
# the declared ratio out and asks whether the RESIDUAL looks like a plausible
# session move. A tight tolerance on the ratio itself mislabels every real case.
SPLIT_RESIDUAL_TOL = 0.25

# A single-session move this large with no declared action behind it is not
# credible for an index constituent. Handoff 4.3's rule, kept verbatim.
IMPLAUSIBLE_RETURN = 0.40


def _is_implausible_move(ratio: float) -> bool:
    """Is a day-over-day close ratio too large to be an ordinary session move?

    ARITHMETIC, not logarithmic. The rule is "|return| > 40%", and a return is
    ``ratio - 1``. Testing ``|log(ratio)| > log(1.40)`` instead looks equivalent
    and is not: it puts the downside trigger at **-28.6%**, because 1/1.40 is
    0.714. That silently caught a shelf of real earnings crashes -- NFLX's -35%
    on 2022-04-20, DG's -32%, EW's -31% -- and quarantined two good sessions
    apiece. Log symmetry is the right idea for comparing *returns*; it is the
    wrong idea for a threshold stated as a plain percentage.
    """
    return abs(ratio - 1.0) > IMPLAUSIBLE_RETURN


@dataclass(frozen=True)
class Split:
    """A vendor-DECLARED split. ``ratio`` is 2.0 for a 2:1."""

    effective_date: str
    ratio: float


@dataclass(frozen=True)
class Dividend:
    """A vendor-DECLARED cash dividend at its ex-date."""

    ex_date: str
    amount: float


@dataclass(frozen=True)
class Bar:
    """One session as the vendor served it. ``close`` is None for a session the
    vendor returned with no price -- kept, never dropped (A5)."""

    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int | None = None


@dataclass(frozen=True)
class RawBar:
    """One session reconstructed to true traded scale."""

    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    status: str  # 'ok' | 'vendor_null' | 'quarantined'


@dataclass(frozen=True)
class Anomaly:
    """A surviving discontinuity. Never a repair instruction -- a quarantine
    instruction plus the evidence for a human."""

    date: str
    implied_ratio: float
    kind: str  # matches schema.INFERRED_KINDS
    evidence: dict


@dataclass
class FactChange:
    """The vendor now says something different about a date we already hold.
    A fact changed. Fail loud; never update (order 2.3)."""

    date: str
    held: float
    offered: float

    def message(self) -> str:
        return (
            "raw close for {} changed: held {!r}, vendor now offers {!r} — "
            "a recorded fact does not change".format(self.date, self.held, self.offered)
        )


# ------------------------------------------------------------------ layer 1 --

def split_factor(date: str, splits: Iterable[Split]) -> float:
    """Cumulative ratio of every split effective strictly AFTER ``date``.

    A date on or after the last split has factor 1.0 — its close is already the
    true traded price.
    """
    f = 1.0
    for s in splits:
        if s.effective_date > date:
            f *= s.ratio
    return f


def reconstruct(bars: Sequence[Bar], splits: Sequence[Split]) -> list[RawBar]:
    """Vendor bars -> true traded scale. Prices multiplied, volume divided."""
    ordered = sorted(splits, key=lambda s: s.effective_date)
    out: list[RawBar] = []
    for b in bars:
        f = split_factor(b.date, ordered)
        if b.close is None:
            out.append(RawBar(b.date, None, None, None, None, None, "vendor_null"))
            continue
        out.append(
            RawBar(
                date=b.date,
                open=None if b.open is None else b.open * f,
                high=None if b.high is None else b.high * f,
                low=None if b.low is None else b.low * f,
                close=b.close * f,
                # Inverse: the vendor multiplies historical volume by the ratio.
                volume=None if b.volume is None else int(round(b.volume / f)),
                status="ok",
            )
        )
    return out


# ------------------------------------------------------------------ layer 2 --

def dividend_factors(
    raw_closes: Mapping[str, float],
    dividends: Sequence[Dividend],
) -> dict[str, float]:
    """Per-event CRSP factors ``f(e) = 1 - amount / raw_close(prior session)``.

    Keyed by ex-date. An ex-date whose prior session is not held is skipped and
    reported by the caller — guessing the prior close would fabricate the very
    number the factor depends on.
    """
    dates = sorted(raw_closes)
    out: dict[str, float] = {}
    for d in dividends:
        prior = None
        for cand in reversed(dates):
            if cand < d.ex_date:
                prior = cand
                break
        if prior is None:
            continue
        base = raw_closes[prior]
        if not base or base <= 0:
            continue
        out[d.ex_date] = 1.0 - (d.amount / base)
    return out


def adjustment_factor_series(
    dates: Sequence[str],
    raw_closes: Mapping[str, float],
    dividends: Sequence[Dividend],
    splits: Sequence[Split] = (),
) -> dict[str, float]:
    """The cumulative factor that turns a RAW close into a total-return close.

    Two components, and both are needed::

        factor(d) = dividend_cumulative(d) / split_factor(d)
        adj(d)    = raw(d) * factor(d)

    * The **split** component divides out what reconstruction multiplied in.
      Raw prices step 4-for-1 at a split — that is the true traded price — so a
      returns series must put the adjustment back. Omitting this (as an earlier
      draft did) leaves a −75% phantom return on every split date, which is the
      exact defect this substrate exists to eliminate.
    * The **dividend** component is CRSP:
      ``PROD{ 1 - amount(e)/raw_close(e-1) : e is an ex-date AFTER d }``.

    Both run backward from the right edge, so the newest date is 1.0 and history
    scales down — the same shape as the vendor's own ``adjclose``, which makes
    the two directly comparable.
    """
    ordered_splits = sorted(splits, key=lambda s: s.effective_date)
    per_event = dividend_factors(raw_closes, dividends)
    events = sorted(per_event.items())
    out: dict[str, float] = {}
    for d in dates:
        f = 1.0
        for ex_date, ev in events:
            if ex_date > d:
                f *= ev
        out[d] = f / split_factor(d, ordered_splits)
    return out


def adjusted_closes(
    raw: Sequence[RawBar],
    dividends: Sequence[Dividend],
    splits: Sequence[Split] = (),
) -> dict[str, float]:
    """``adjusted_view`` values: the total-return series analytics reads.

    Quarantined and null sessions are excluded — a view must never be built on a
    price we have refused to call a fact.
    """
    usable = {b.date: b.close for b in raw if b.status == "ok" and b.close is not None}
    factors = adjustment_factor_series(sorted(usable), usable, dividends, splits)
    return {d: usable[d] * factors[d] for d in usable}


# ------------------------------------------------------------------ layer 3 --

def _ratio_matches(ratio: float, target: float, tol: float = SPLIT_RESIDUAL_TOL) -> bool:
    """Does ``ratio`` look like ``target`` (a declared scale error) with an
    ordinary session move riding on it? Divide the target out and judge what is
    left."""
    residual = ratio / target
    return abs(math.log(residual)) <= math.log(1.0 + tol)


def detect_anomalies(
    bars: Sequence[Bar],
    splits: Sequence[Split],
) -> list[Anomaly]:
    """Discontinuities in the VENDOR's own adjusted close series.

    **Which series to test matters, and getting it backwards is easy.** The
    vendor's ``close`` is split-adjusted, so it must be SMOOTH across a split —
    that is the whole point of an adjusted series. The reconstructed raw series
    is the opposite: a split is a real 4-for-1 step in the true traded price, so
    raw *should* jump there. An earlier draft of this function ran on the raw
    series and duly flagged AAPL's perfectly clean 2020 4:1 as corruption while
    quarantining eight good sessions. The invariant lives on the vendor series.

    Two rules, from the amendment sheet:

    * a day-over-day ratio in the vendor's close matching a DECLARED split ratio
      or its inverse — the vendor declared the split but did not apply it
      uniformly to its own history, so its closes sit on two different scales.
      Only ratios that could not themselves be an ordinary session move are
      eligible: a spinoff is declared as a split with a ratio near 1.0, and such
      a ratio matches almost every day in a series (see the note on the gate);
    * any ``|return| > 40%``, arithmetic, with no declared action on that date.

    A step matching a declared ratio is ``vendor_corruption``; an unexplained
    large move is ``unknown``. We never label something a split the vendor has
    not declared.
    """
    declared_dates = {s.effective_date for s in splits}
    targets: list[float] = []
    for s in splits:
        # A declared ratio only carries evidence if a step OF THAT SIZE could
        # not be an ordinary trading day. Yahoo encodes a SPINOFF as a split
        # whose ratio sits near 1.0 -- DHR/Veralto is 1.128, GE HealthCare
        # 1.281, GE Vernova 1.253 -- and with a +/-25% residual window either
        # side, the match window for such a ratio contains a FLAT DAY. Measured
        # on the real series: 1,420 of DHR's 1,421 sessions matched, so the
        # whole five-year history was condemned as vendor corruption. Eighteen
        # index-heavy names went the same way (GE, IBM, MRK, MMM, HON, CMCSA
        # among them) on the first full backfill.
        #
        # Gating on the same implausibility floor Rule 2 uses keeps every case
        # this rule exists for -- MNST's 2.0, AAPL's and NVDA's 4.0, GE's 0.125
        # reverse split -- because a genuine split step is nothing like a
        # trading day. It drops exactly the ratios that never carried evidence.
        if not _is_implausible_move(s.ratio) and not _is_implausible_move(1.0 / s.ratio):
            continue
        targets.extend((s.ratio, 1.0 / s.ratio))

    usable = [b for b in bars if b.close]
    found: list[Anomaly] = []
    for i in range(1, len(usable)):
        prev, cur = usable[i - 1], usable[i]
        ratio = cur.close / prev.close
        ev = {
            "prev_date": prev.date, "prev_close": round(prev.close, 6),
            "date": cur.date, "close": round(cur.close, 6),
            "ratio": round(ratio, 6),
        }
        matched = next((t for t in targets if _ratio_matches(ratio, t)), None)
        if matched is not None:
            found.append(Anomaly(
                date=cur.date, implied_ratio=ratio, kind="vendor_corruption",
                evidence={**ev, "matched_declared_ratio": round(matched, 6),
                          "note": "vendor's adjusted close steps by a ratio it "
                                  "declared: its own history is on two scales"},
            ))
            continue
        if _is_implausible_move(ratio):
            if cur.date in declared_dates:
                continue  # a declared action on this very date explains it
            found.append(Anomaly(
                date=cur.date, implied_ratio=ratio, kind="unknown",
                evidence={**ev, "note": "|return| > 40% with no declared action"},
            ))
    return found


def quarantine_span(
    anomalies: Sequence[Anomaly],
    dates: Sequence[str],
    splits: Sequence[Split] = (),
) -> set[str]:
    """The sessions to write as ``quarantined``.

    Normally the contiguous span from the session BEFORE the first anomaly
    through the last anomaly. Bounded deliberately: quarantining a whole 5-year
    history over one bad week would make the store useless, and quarantining
    only the flagged sessions would leave their untrustworthy neighbours stamped
    as facts.

    **But if a step survived at a declared split's own effective date, the whole
    pre-split region of the window goes too.** That step means the vendor's pre-
    and post-split closes are not on scales differing by the ratio it declared,
    so we cannot tell WHICH pre-split sessions were adjusted and which were not.
    A uniformly mis-scaled stretch contains no step at all and would otherwise
    sail through as fact — which is the MNST shape exactly: every session before
    2026-07-17 sits on one wrong-after-reconstruction scale, internally
    consistent, and invisible to a step detector.
    """
    if not anomalies:
        return set()
    ordered = sorted(dates)
    first, last = min(a.date for a in anomalies), max(a.date for a in anomalies)
    idx = ordered.index(first) if first in ordered else 0
    start = ordered[max(0, idx - 1)]

    flagged = {a.date for a in anomalies}
    for sp in splits:
        landing = next((d for d in ordered if d >= sp.effective_date), None)
        if landing is not None and landing in flagged:
            start = ordered[0]
            last = max(last, landing)
            break
    return {d for d in ordered if start <= d <= last}


def apply_quarantine(raw: Sequence[RawBar], span: set[str]) -> list[RawBar]:
    """Re-stamp the span. Values are kept — a quarantined row is evidence, and
    throwing it away would lose the record of what the vendor said."""
    if not span:
        return list(raw)
    return [
        RawBar(b.date, b.open, b.high, b.low, b.close, b.volume, "quarantined")
        if b.date in span and b.status == "ok" else b
        for b in raw
    ]


# ----------------------------------------------------------- fact integrity --

def fact_changes(
    held: Mapping[str, float],
    offered: Mapping[str, float],
    eps: float = FACT_EPS,
) -> list[FactChange]:
    """Dates where a newly reconstructed close disagrees with one already
    stored. The caller fails loud and skips the name; it never updates."""
    out: list[FactChange] = []
    for date, new in offered.items():
        old = held.get(date)
        if old is None or new is None:
            continue
        if old == 0:
            if new != 0:
                out.append(FactChange(date, old, new))
            continue
        if abs(new / old - 1.0) > eps:
            out.append(FactChange(date, old, new))
    return sorted(out, key=lambda c: c.date)
