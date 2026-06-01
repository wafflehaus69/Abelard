"""Frequency diagnostic assembler for Full Brief.

Per Full Brief spec Adjustment 1 (Abelard 2026-05-29): assembles the
near-miss table from Pass E's frequency analysis output. NO fixed-row
cap — surfaces ALL terms meeting inclusion criteria. The 50-row soft
cap is a RENDERING concern (separate module in Stage 2), not a data
concern; the JSON envelope always contains the full list.

Inclusion criteria:
  - freq_window >= MIN_FREQ_FLOOR (default 5)
  - term not in crossings table
  - term not in stopword list

Ordering: freq_window descending, ties broken by delta_ratio descending.

Empirical motivation for the unbounded surfaced list (cycle 2 evidence):
on heavy news days the prior window saturates, all genuine signal lives
in near-misses (drone 17/7, country 17/4, russian 20/7), and the
fixed-25 cap from the original spec would have truncated the long tail
on exactly the days where it matters most.

Reason classification mirrors the threshold module:
  - "below_window_min": term hit freq_floor but not the absolute window
    threshold (freq_window < COLD_START_WINDOW_MIN)
  - "above_prior_max": term hit window threshold but the prior was too
    high (freq_window >= COLD_START_WINDOW_MIN, prior >= COLD_START_PRIOR_MAX)

This is diagnostic surfacing — the actual gate is in
attention/threshold.py and remains the source of truth for which terms
cross. Frequency_diagnostic only labels the near-misses for operator
review.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ..attention.threshold import COLD_START_WINDOW_MIN


MIN_FREQ_FLOOR = 5


@dataclass(frozen=True)
class NearMissTerm:
    """One near-miss term in the frequency diagnostic.

    `delta_ratio` = freq_window / max(freq_prior, 1). The max(., 1) avoids
    division by zero when prior is 0 (which happens for genuinely-novel
    terms that would have crossed except for the absolute floor).

    `reason_not_crossed` discriminates the two near-miss buckets so the
    operator can quickly see why a term is in the table instead of in
    the crossings:
      - "below_window_min": "would have been novel but didn't fire enough"
      - "above_prior_max":  "already saturated in prior, can't be novel"
    """

    term: str
    freq_window: int
    freq_prior: int
    delta_ratio: float
    reason_not_crossed: str


def _delta_ratio(freq_window: int, freq_prior: int) -> float:
    """Compute window/prior ratio with prior=0 treated as 1 to avoid div0."""
    return freq_window / max(freq_prior, 1)


def _classify_reason(freq_window: int) -> str:
    """Label why a term is a near-miss not a crossing.

    Mirrors attention/threshold.top_candidates() classification: if the
    term failed the window-min floor it's "below_window_min"; otherwise
    the term must have met the window-min but failed the prior-max
    ceiling, so it's "above_prior_max".
    """
    if freq_window < COLD_START_WINDOW_MIN:
        return "below_window_min"
    return "above_prior_max"


def assemble_near_misses(
    *,
    window_counts: dict[str, int],
    prior_counts: dict[str, int],
    crossing_terms: Iterable[str],
    stopwords: frozenset[str],
    min_freq_floor: int = MIN_FREQ_FLOOR,
) -> list[NearMissTerm]:
    """Assemble the complete near-miss table per Adjustment 1.

    Args:
      window_counts: term -> count of headlines containing term in live
                     window (from Pass E `TermCounts.window_counts`).
      prior_counts: term -> count of headlines containing term in prior
                    window (from Pass E `TermCounts.prior_counts`).
      crossing_terms: terms that crossed the Pass E threshold this cycle
                      (excluded from the table — they're surfaced
                      separately in the crossings section).
      stopwords: configured stopword frozenset (defensive double-check —
                 the counter already filters these, but exclude here
                 too so a future stopword addition takes effect even if
                 the corpus already contained stale data).
      min_freq_floor: minimum freq_window to include (default
                      MIN_FREQ_FLOOR=5). Configurable for tests and
                      potential future tuning.

    Returns:
      list[NearMissTerm] sorted by freq_window descending, ties broken
      by delta_ratio descending. NO row cap — full list returned.
      Empty list returned when no terms meet inclusion criteria (very
      quiet windows are valid; renderer handles empty case separately).
    """
    crossing_set = frozenset(t.lower() for t in crossing_terms)

    near_misses: list[NearMissTerm] = []
    for term, freq_window in window_counts.items():
        if freq_window < min_freq_floor:
            continue
        if term.lower() in crossing_set:
            continue
        if term.lower() in stopwords:
            continue
        freq_prior = prior_counts.get(term, 0)
        near_misses.append(NearMissTerm(
            term=term,
            freq_window=freq_window,
            freq_prior=freq_prior,
            delta_ratio=_delta_ratio(freq_window, freq_prior),
            reason_not_crossed=_classify_reason(freq_window),
        ))

    # Sort: freq_window desc, then delta_ratio desc for ties (T10 pin).
    near_misses.sort(key=lambda nm: (-nm.freq_window, -nm.delta_ratio))
    return near_misses


__all__ = [
    "MIN_FREQ_FLOOR",
    "NearMissTerm",
    "assemble_near_misses",
]
