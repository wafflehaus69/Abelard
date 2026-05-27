"""Cold-start threshold gate for the attention counter.

Pass E spec (build brief, 2026-05-26): a term crosses signal-over-noise iff

    count(window) >= 10  AND  count(prior_window) < 3

This is the COLD-START rule. A standard-deviation baseline rule (Definition
A from the design discussion) is a future pass once 30+ days of data exist.

The gate returns the list of crossing terms with their counts, ordered by
window count descending (for downstream prioritization if cycle costs
become a concern). When zero terms cross, the gate also surfaces the
top-K near-miss candidates — operator visibility for "what almost fired"
is a Pass E live-smoke validation criterion.
"""

from __future__ import annotations

from dataclasses import dataclass

from .counter import TermCounts


COLD_START_WINDOW_MIN = 10
COLD_START_PRIOR_MAX = 3


@dataclass(frozen=True)
class CrossingTerm:
    """One term that crossed the threshold gate this cycle."""

    term: str
    window_count: int
    prior_count: int


@dataclass(frozen=True)
class CandidateTerm:
    """One near-miss term — failed the gate but close.

    Surfaced when zero terms crossed, to give the operator visibility
    into what's almost-firing without producing actual ATTENTION briefs
    on noise.
    """

    term: str
    window_count: int
    prior_count: int
    reason: str   # short label: "below_window_min" / "above_prior_max"


def evaluate_threshold(counts: TermCounts) -> list[CrossingTerm]:
    """Return terms that cross the cold-start gate, ordered desc by window count.

    Cold-start rule: `count(window) >= 10 AND count(prior_window) < 3`. Both
    counts are per-headline (not per-occurrence). Ties broken alphabetically
    for deterministic ordering.
    """
    crossing: list[CrossingTerm] = []
    for term, window_n in counts.window_counts.items():
        if window_n < COLD_START_WINDOW_MIN:
            continue
        prior_n = counts.prior_counts.get(term, 0)
        if prior_n >= COLD_START_PRIOR_MAX:
            continue
        crossing.append(CrossingTerm(
            term=term, window_count=window_n, prior_count=prior_n,
        ))
    # Sort: highest window count first, then alphabetical for stable tiebreak.
    crossing.sort(key=lambda c: (-c.window_count, c.term))
    return crossing


def top_candidates(counts: TermCounts, *, limit: int = 5) -> list[CandidateTerm]:
    """Surface the top-K candidates that did NOT cross — for operator visibility.

    Two near-miss buckets:
      - below_window_min: high enough relative density to be interesting
        but didn't hit 10 (e.g. count(window)=8, count(prior)=0).
      - above_prior_max: hit 10 in window but prior was too noisy
        (e.g. count(window)=15, count(prior)=5 — recurring topic, not novel).

    Both buckets ordered by window count desc, top `limit` returned.
    Useful in the live-smoke output: tells the operator what's
    almost-firing without producing ATTENTION briefs on noise.
    """
    candidates: list[CandidateTerm] = []
    for term, window_n in counts.window_counts.items():
        if window_n < COLD_START_WINDOW_MIN:
            # Skip extremely low-count terms — would be all noise.
            # Threshold at 5 keeps the visible candidate list meaningful.
            if window_n < 5:
                continue
            candidates.append(CandidateTerm(
                term=term,
                window_count=window_n,
                prior_count=counts.prior_counts.get(term, 0),
                reason="below_window_min",
            ))
            continue
        prior_n = counts.prior_counts.get(term, 0)
        if prior_n >= COLD_START_PRIOR_MAX:
            candidates.append(CandidateTerm(
                term=term,
                window_count=window_n,
                prior_count=prior_n,
                reason="above_prior_max",
            ))
    candidates.sort(key=lambda c: (-c.window_count, c.term))
    return candidates[:limit]


__all__ = [
    "COLD_START_PRIOR_MAX",
    "COLD_START_WINDOW_MIN",
    "CandidateTerm",
    "CrossingTerm",
    "evaluate_threshold",
    "top_candidates",
]
