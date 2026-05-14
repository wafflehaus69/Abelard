"""Pin the Pass-C-close Brief fixture against the live Pydantic schema.

Pass C Step 9g / Step 16 close (2026-05-14). The Brief captured here
is the first successful real synthesis output:

  - brief_id: nwd-2026-05-14T05-30-00Z-34a0414b
  - model: claude-sonnet-4-6
  - trigger: cross_theme:russia_ukraine_war+us_iran_escalation
  - 8 events, materiality 0.58-0.82
  - cache_creation_input_tokens: 1361 (SYSTEM_PROMPT cached)
  - dispatch failed only because signal-cli wasn't installed on Orban

Re-validating this artifact against `Brief.model_validate` on every
test run catches schema drift before it ships. Any change to the
Brief / Event / SynthesisMetadata shape that would have broken the
real synthesis output on 2026-05-14 will fail this test now.
"""

from __future__ import annotations

import json
from pathlib import Path

from news_watch_daemon.synthesize.brief import Brief


_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "checkpoint_4_synthesis_output.json"
)


def test_fixture_loads_and_validates_as_brief():
    """Pass C close artifact: the first real synthesis Brief must
    continue to validate against the canonical schema."""
    raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    brief = Brief.model_validate(raw)

    assert brief.brief_id == "nwd-2026-05-14T05-30-00Z-34a0414b"
    assert brief.synthesis_metadata.model_used == "claude-sonnet-4-6"

    # Cross-theme trigger was the firing condition.
    assert brief.trigger.type == "event"
    assert "cross_theme" in brief.trigger.reason
    assert set(brief.themes_covered) == {"russia_ukraine_war", "us_iran_escalation"}

    # Eight events, scores within the calibration band the prompt asked for.
    assert len(brief.events) == 8
    for event in brief.events:
        assert 0.0 <= event.materiality_score <= 1.0
        # Sub-0.30 events are filtered out by the prompt's Rule 4; the
        # fixture must continue to honor that.
        assert event.materiality_score >= 0.30

    # First successful real call: prefix cached, no cache reads yet.
    md = brief.synthesis_metadata
    assert md.cache_creation_input_tokens == 1361
    assert md.cache_read_input_tokens == 0
    assert md.input_tokens == 7921
    assert md.output_tokens == 5623

    # No theses doc was available — the WARN must be recorded.
    assert md.theses_doc_available is False
    assert "NEWS_WATCH_THESES_PATH not set" in (md.theses_doc_warning or "")
    # All thesis_links should carry null thesis_id (no doc to cite).
    for event in brief.events:
        for link in event.thesis_links:
            assert link.thesis_id is None

    # Dispatch failed only because signal-cli wasn't installed on the
    # smoke host — the audit trail must record that explicitly.
    assert brief.dispatch.alerted is False
    assert brief.dispatch.channel is None
    assert "dispatch_failed" in (brief.dispatch.suppressed_reason or "")
    assert "signal-cli" in (brief.dispatch.suppressed_reason or "")


def test_fixture_narrative_is_analyst_register():
    """Voice-register smoke check. The chief-of-staff doctrine asks
    for analytical framing, not Reuters house style. Pins specific
    phrases from the 2026-05-14 narrative that exemplify the
    register — any drift to wire-style summarization would lose them."""
    raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    brief = Brief.model_validate(raw)
    narrative = brief.narrative.lower()
    # Analytical framings, not just event recounting.
    assert "cascade" in narrative or "feeding" in narrative
    assert "central bank reaction" in narrative or "rate-hike" in narrative
    # Specific portfolio-relevant connection.
    assert "patriot" in narrative or "air-defense" in narrative


def test_fixture_thesis_links_carry_portfolio_reasoning():
    """The prompt asks Sonnet to write thesis_link notes that connect
    events to portfolio reasoning. Pin a couple of representative
    notes so future prompt edits don't erode this property without
    deliberate intent."""
    raw = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
    brief = Brief.model_validate(raw)
    # At least one note must mention a recognizable portfolio concept.
    portfolio_concepts = {
        "cascade", "commodity", "supply", "reaction function",
        "procurement", "fertilizer", "industrial",
    }
    any_match = False
    for event in brief.events:
        for link in event.thesis_links:
            text = (link.note or "").lower()
            if any(concept in text for concept in portfolio_concepts):
                any_match = True
                break
    assert any_match, (
        "no thesis_link note carried a portfolio-reasoning concept; "
        "the synthesis prompt may have drifted toward summary-only output"
    )
