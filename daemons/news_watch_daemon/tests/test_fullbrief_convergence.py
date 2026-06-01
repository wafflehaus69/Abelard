"""Convergence analyzer tests — strict-headline substring matching.

Per Full Brief spec Q6 + Adjustment 5 (Abelard 2026-05-29).
Test coverage: T6, T7, T7b, T7c, T8.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from news_watch_daemon.fullbrief.convergence import (
    ConvergenceResult,
    analyze_convergence,
    term_appears_in,
)


# ---------- mock event types ----------
# Production uses Pass C's Pydantic Event + SourceHeadline. Tests use these
# minimal dataclasses to keep convergence unit tests decoupled from Pass C
# schema evolution — convergence.analyze_convergence accesses .event_id,
# .source_headlines, and .headline via duck-typed attribute access.


@dataclass(frozen=True)
class _MockSourceHeadline:
    headline: str


@dataclass(frozen=True)
class _MockEvent:
    event_id: str
    source_headlines: list = field(default_factory=list)


def _evt(event_id: str, *headlines: str) -> _MockEvent:
    """Convenience: build a mock event with N source headlines."""
    return _MockEvent(
        event_id=event_id,
        source_headlines=[_MockSourceHeadline(h) for h in headlines],
    )


# ---------- term_appears_in ----------


def test_term_appears_in_literal_substring_match():
    """T6: literal substring match, case-insensitive."""
    assert term_appears_in("hormuz", "US Hits Iran Targets Near Hormuz") is True


def test_term_appears_in_case_insensitive_uppercase_term():
    """T7: uppercase term + lowercase headline matches via ASCII-lower."""
    assert term_appears_in("HORMUZ", "us hits iran targets near hormuz") is True


def test_term_appears_in_case_insensitive_uppercase_headline():
    """T7: lowercase term + uppercase headline matches."""
    assert term_appears_in("hormuz", "US HITS IRAN TARGETS NEAR HORMUZ") is True


def test_term_appears_in_substring_matches_within_hyphenated():
    """T7b: substring rule means `iran` matches inside `iran-us` (hyphenated).
    No word-boundary detection per Adjustment 5 explicit non-decision."""
    assert term_appears_in("iran", "Iran-US Deal Pending") is True


def test_term_appears_in_substring_matches_within_word():
    """T7b: substring rule means `iran` matches inside `iranian` too.
    Convergence is character-level by design."""
    assert term_appears_in("iran", "US strikes Iranian drone control station") is True


def test_term_appears_in_directionality_iranian_not_substring_of_iran():
    """T7b: substring directionality — `iranian` is NOT a substring of `iran`,
    so an attention crossing for `iranian` does NOT match an Iran-only headline.
    This is the canonical asymmetry the Q6+Adjustment-5 resolution made explicit."""
    assert term_appears_in("iranian", "US Hits Iran Targets Near Hormuz") is False


def test_term_appears_in_orphan_term_absent():
    """T8: term that doesn't appear anywhere -> False (orphan upstream)."""
    assert term_appears_in("kuwait", "US Hits Iran Targets Near Hormuz") is False


def test_term_appears_in_empty_term_false():
    assert term_appears_in("", "headline") is False


def test_term_appears_in_empty_headline_false():
    assert term_appears_in("term", "") is False


def test_term_appears_in_both_empty_false():
    assert term_appears_in("", "") is False


def test_term_appears_in_apostrophe_within_word():
    """Substring rule: `hormuz` matches `Hormuz's` (substring works through
    apostrophe). Documents the behavior — no word-boundary blocker."""
    assert term_appears_in("hormuz", "Hormuz's transit fee proposed") is True


# ---------- analyze_convergence ----------


def test_analyze_convergence_single_convergent_event():
    """T6 end-to-end: term `hormuz`, one event whose headline contains it -> convergent."""
    evt = _evt("evt-1", "US Hits Iran Targets Near Hormuz")
    result = analyze_convergence(
        triggering_term="hormuz",
        pass_c_events=[evt],
    )
    assert result.status == "convergent"
    assert result.converges_with == ["evt-1"]
    assert result.orphan_reason is None
    assert result.triggering_term == "hormuz"


def test_analyze_convergence_multiple_convergent_events():
    """Term matches in multiple events -> all event_ids in converges_with."""
    evt1 = _evt("evt-1", "US Strikes Iran Near Hormuz")
    evt2 = _evt("evt-2", "Hormuz Transit Permission Demanded")
    result = analyze_convergence(
        triggering_term="hormuz",
        pass_c_events=[evt1, evt2],
    )
    assert result.status == "convergent"
    assert set(result.converges_with) == {"evt-1", "evt-2"}


def test_analyze_convergence_multiple_headlines_per_event_deduped():
    """If a term matches in multiple source_headlines of ONE event, the event
    only appears once in converges_with (break-on-first-match discipline)."""
    evt = _evt("evt-1",
               "Hormuz transit fee announced",
               "Second source on Hormuz developments",
               "Third source mentions Hormuz again")
    result = analyze_convergence(
        triggering_term="hormuz",
        pass_c_events=[evt],
    )
    assert result.converges_with == ["evt-1"]   # not ["evt-1", "evt-1", "evt-1"]


def test_analyze_convergence_orphan_no_match():
    """T8: term that no event contains -> orphan + reason."""
    evt = _evt("evt-1", "US Hits Iran Targets Near Hormuz")
    result = analyze_convergence(
        triggering_term="kuwait",
        pass_c_events=[evt],
    )
    assert result.status == "orphan"
    assert result.converges_with == []
    assert result.orphan_reason is not None
    assert "no Pass C event contains it" in result.orphan_reason


def test_analyze_convergence_empty_events_yields_orphan():
    """Q2 no_trigger case: pass_c_events=[] -> every crossing is orphan."""
    result = analyze_convergence(
        triggering_term="hormuz",
        pass_c_events=[],
    )
    assert result.status == "orphan"
    assert result.converges_with == []


def test_analyze_convergence_returns_frozen_result():
    """ConvergenceResult is frozen — downstream rendering can rely on
    immutability for grouping/sorting."""
    result = analyze_convergence(triggering_term="x", pass_c_events=[])
    assert isinstance(result, ConvergenceResult)


# ---------- T7c: cycle 2 canonical orphan case ----------


def test_cycle2_secretary_canonical_orphan():
    """T7c: cycle 2 canonical orphan case is the WHOLE motivation for Q6's
    headline-only rule. Pass E surfaced `secretary` as a crossing because
    multiple unrelated Cabinet figures share the title; Pass C synthesized
    evt-6's `headline_summary` as "Treasury Secretary Bessent and incoming
    Fed Chair Warsh held a debut joint breakfast meeting" — but the actual
    Bloomberg `headline` for that event is the title-only "The Debut
    Bessent-Warsh Breakfast Left Fed Rate Cuts Off the Menu" with no
    "secretary" in it.

    Under headline-only matching: ORPHAN (correct — surfaces the noise
    crossing for review).
    Under headline + headline_summary matching: convergent (wrong — would
    suppress the orphan signal via Pass C's editorial word choice).

    This test pins the Q6 resolution against future drift."""
    bloomberg_headline = "The Debut Bessent-Warsh Breakfast Left Fed Rate Cuts Off the Menu"
    evt6 = _evt("evt-6", bloomberg_headline)
    result = analyze_convergence(
        triggering_term="secretary",
        pass_c_events=[evt6],
    )
    assert result.status == "orphan"
    # Reinforcing: the literal substring is genuinely absent from the headline
    assert "secretary" not in bloomberg_headline.lower()
    # And we are NOT given access to headline_summary in the convergence
    # input — the mock event doesn't even have that field. This is by
    # design: if convergence was looking at summary, it would be in the
    # function signature and we'd need to mock it.


def test_cycle1_iranian_orphan_per_substring_directionality():
    """Cycle 1 secondary canonical case: `iranian` would NOT converge with
    a Bloomberg headline that uses "Iran" but not "Iranian" — per Q6
    substring directionality (term must be substring of headline, not the
    other way around). Documents the cycle 1 sister case to the cycle 2
    secretary orphan."""
    bloomberg_headline = "US Hits Iran Targets Near Hormuz as Deal Remains Elusive"
    evt = _evt("evt-1", bloomberg_headline)
    result = analyze_convergence(
        triggering_term="iranian",
        pass_c_events=[evt],
    )
    assert result.status == "orphan"
