"""Convergence analyzer for Full Brief.

Per Full Brief spec Q6 (Abelard 2026-05-29) + Adjustment 5 normalization:
ASCII-lowered substring match between an attention crossing's
`triggering_term` and the headlines of Pass C events.

Match is STRICT-HEADLINE — checks `event.source_headlines[].headline`
ONLY. NOT `headline_summary` (which is synthesizer-written paraphrase;
checking it would suppress orphan signal by self-consistency, per the
cycle 2 `secretary` canonical case where Bloomberg's literal headline
"The Debut Bessent-Warsh Breakfast..." does not contain "secretary"
but Pass C's summary "Treasury Secretary Bessent..." does).

Substring is character-level. Word-boundary detection NOT applied
(intentional — `iran` matches inside `iranian` AND inside `iran-us`).
No Unicode case folding (deferred — ASCII has covered all observed
cycles 1 and 2 data; non-ASCII convergence cases are a follow-up).

Orphan classification is the high-value signal: a term that crossed
Pass E threshold but does not appear in any Pass C event indicates
either (a) emergent signal not yet captured by theme configuration,
or (b) a keyword-gate recall gap (false-negative tagging). Orphans
should be reviewed before convergent crossings — they're how the
daemon tells its operator that the theme taxonomy has a gap.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


ConvergenceStatus = Literal["convergent", "orphan", "unknown"]


@dataclass(frozen=True)
class ConvergenceResult:
    """One attention crossing's convergence judgment.

    `converges_with` is the list of Pass C event_ids whose source
    headlines contain the triggering_term (substring, case-insensitive).
    Empty for orphan or unknown status.

    `orphan_reason` populated only when status == "orphan", with a
    short human-readable explanation.

    "unknown" status reserved for the case where convergence analysis
    failed to run (e.g., upstream Pass E failure left no events to
    compare against AND no crossings to analyze — distinct from a
    valid empty-event-list case which produces orphan).
    """

    triggering_term: str
    status: ConvergenceStatus
    converges_with: list[str]
    orphan_reason: str | None


def term_appears_in(term: str, headline: str) -> bool:
    """Convergence substring match — Q6 strict-headline + Adjustment 5 rule.

    ASCII-lowered substring on both sides. No stemming, no aliasing,
    no word-boundary detection.

    Examples (cycle 1 + cycle 2 empirical cases):

        term_appears_in("hormuz", "US Hits Iran Targets Near Hormuz") -> True
        term_appears_in("HORMUZ", "us hits iran targets near hormuz") -> True
        term_appears_in("iran", "Iran-US Deal Pending")               -> True
        term_appears_in("iranian", "US Hits Iran Targets Near Hormuz")-> False
        term_appears_in("secretary",
                        "The Debut Bessent-Warsh Breakfast Left Fed Rate Cuts Off the Menu"
                       )                                              -> False

    The last example is the cycle 2 canonical orphan case: `secretary`
    does NOT match the Bloomberg headline literal, even though the
    Pass C-synthesized `headline_summary` for the same event does
    contain "Treasury Secretary Bessent". Per Q6 we only check the
    headline, not the summary — the orphan classification is what
    surfaces the noise-vs-signal question for human review.
    """
    if not term or not headline:
        return False
    return term.lower() in headline.lower()


def analyze_convergence(
    *,
    triggering_term: str,
    pass_c_events: list[Any],
) -> ConvergenceResult:
    """Determine whether an attention crossing converges with Pass C events.

    Args:
      triggering_term: term from the attention brief (e.g. "hormuz")
      pass_c_events: list of Pass C event objects. Each must have
                     `.event_id` (str) and `.source_headlines` (iterable
                     of objects with `.headline` str attribute).
                     Empty list is valid input (Q2 no_trigger case).

    Returns:
      ConvergenceResult with:
        - status="convergent" + converges_with=[event_ids] if any event
          has a source_headline whose headline contains the term
          (substring, case-insensitive)
        - status="orphan" + orphan_reason="term crossed threshold but no
          Pass C event contains it" if no event matches (including when
          pass_c_events is empty)

    Iteration discipline: one match per event is sufficient (we list the
    event_id once in converges_with). Multiple matches within one event's
    source_headlines don't double-count it.
    """
    converges_with: list[str] = []
    for event in pass_c_events:
        for source_headline in event.source_headlines:
            if term_appears_in(triggering_term, source_headline.headline):
                converges_with.append(event.event_id)
                break

    if converges_with:
        return ConvergenceResult(
            triggering_term=triggering_term,
            status="convergent",
            converges_with=converges_with,
            orphan_reason=None,
        )
    return ConvergenceResult(
        triggering_term=triggering_term,
        status="orphan",
        converges_with=[],
        orphan_reason="term crossed threshold but no Pass C event contains it",
    )


__all__ = [
    "ConvergenceResult",
    "ConvergenceStatus",
    "analyze_convergence",
    "term_appears_in",
]
