"""Brief schema tests — validation, ID generation, schema fidelity."""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    DriftProposal,
    EnvelopeHealth,
    Event,
    SourceHeadline,
    SynthesisMetadata,
    ThesisLink,
    Trigger,
    TriggerWindow,
)


def _minimal_brief() -> Brief:
    return Brief(
        brief_id="nwd-2026-05-13T14-32-08Z-a1b2c3d4",
        generated_at="2026-05-13T14:32:08Z",
        trigger=Trigger(
            type="event",
            reason="delta_threshold:iran:5",
            window=TriggerWindow(since="2026-05-13T13:30:00Z", until="2026-05-13T14:32:00Z"),
        ),
        themes_covered=["us_iran_escalation"],
        events=[],
        narrative="No material events in window.",
        dispatch=Dispatch(alerted=False, suppressed_reason="below_materiality_threshold"),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-7",
            theses_doc_available=True,
            theses_doc_path="/x/theses.md",
        ),
    )


# ---------- brief_id generation ----------


def test_new_brief_id_format():
    bid = Brief.new_brief_id()
    assert re.fullmatch(
        r"nwd-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z-[0-9a-f]{8}",
        bid,
    ), f"unexpected brief_id: {bid}"


def test_new_brief_id_deterministic_when_passed_fixed_time():
    fixed = datetime(2026, 5, 13, 14, 32, 8, tzinfo=timezone.utc)
    bid1 = Brief.new_brief_id(when=fixed)
    bid2 = Brief.new_brief_id(when=fixed)
    # Timestamp portion identical; suffix differs (uuid4 random).
    assert bid1.startswith("nwd-2026-05-13T14-32-08Z-")
    assert bid2.startswith("nwd-2026-05-13T14-32-08Z-")
    assert bid1 != bid2  # uuid4 suffix differs


def test_new_brief_id_naive_datetime_treated_as_utc():
    naive = datetime(2026, 5, 13, 14, 32, 8)  # no tzinfo
    bid = Brief.new_brief_id(when=naive)
    assert "2026-05-13T14-32-08Z" in bid


def test_new_brief_id_local_tz_converted_to_utc():
    from datetime import timedelta
    eastern = timezone(timedelta(hours=-5))
    local = datetime(2026, 5, 13, 9, 32, 8, tzinfo=eastern)  # = 14:32:08Z
    bid = Brief.new_brief_id(when=local)
    assert "2026-05-13T14-32-08Z" in bid


# ---------- minimal Brief constructs cleanly ----------


def test_minimal_brief_constructs():
    b = _minimal_brief()
    assert b.brief_id == "nwd-2026-05-13T14-32-08Z-a1b2c3d4"
    assert b.trigger.type == "event"
    assert b.events == []
    assert b.dispatch.alerted is False


def test_brief_serialize_roundtrips():
    """Brief -> JSON -> Brief preserves data."""
    b = _minimal_brief()
    dumped = b.model_dump(mode="json")
    restored = Brief.model_validate(dumped)
    assert restored == b


# ---------- nested model validation ----------


def test_materiality_score_must_be_in_unit_interval():
    with pytest.raises(Exception):
        Event(
            event_id="evt-1",
            headline_summary="X",
            themes=["t"],
            materiality_score=1.5,  # > 1.0
        )
    with pytest.raises(Exception):
        Event(
            event_id="evt-1",
            headline_summary="X",
            themes=["t"],
            materiality_score=-0.1,
        )


def test_event_with_score_at_bounds_ok():
    Event(event_id="evt-1", headline_summary="X", themes=["t"], materiality_score=0.0)
    Event(event_id="evt-1", headline_summary="X", themes=["t"], materiality_score=1.0)


def test_trigger_type_must_be_in_enum():
    with pytest.raises(Exception):
        Trigger(type="cron", reason="x", window=TriggerWindow(since="a", until="b"))  # type: ignore[arg-type]


def test_thesis_link_direction_must_be_in_enum():
    with pytest.raises(Exception):
        ThesisLink(thesis_id="t", direction="maybe", note="x")  # type: ignore[arg-type]


def test_dispatch_channel_must_be_in_enum():
    with pytest.raises(Exception):
        Dispatch(alerted=True, channel="email")  # type: ignore[arg-type]


def test_drift_proposal_tier_must_be_in_enum():
    with pytest.raises(Exception):
        DriftProposal(
            proposal_id="p", theme_id="t", proposed_keyword="x",
            suggested_tier="urgent",  # type: ignore[arg-type]
            evidence_count=3, generated_at="t",
        )


def test_drift_proposal_notes_optional():
    """Pass C Step 0 addition: optional `notes` for Haiku rationale."""
    p = DriftProposal(
        proposal_id="p", theme_id="t", proposed_keyword="x",
        suggested_tier="primary", evidence_count=3, generated_at="t",
    )
    assert p.notes is None
    p2 = DriftProposal(
        proposal_id="p", theme_id="t", proposed_keyword="x",
        suggested_tier="primary", evidence_count=3, generated_at="t",
        notes="Haiku saw 'foo bar' phrase across 5 untagged headlines",
    )
    assert p2.notes is not None


def test_drift_proposal_negative_evidence_rejected():
    with pytest.raises(Exception):
        DriftProposal(
            proposal_id="p", theme_id="t", proposed_keyword="x",
            suggested_tier="primary", evidence_count=-1, generated_at="t",
        )


def test_extra_fields_rejected():
    """Pydantic extra='forbid' catches typos and unknown fields."""
    with pytest.raises(Exception):
        Brief.model_validate({
            **_minimal_brief().model_dump(mode="json"),
            "surprise": "boom",
        })


# ---------- prompt-caching telemetry fields ----------


def test_synthesis_metadata_cache_fields_default_zero():
    """cache_creation_input_tokens and cache_read_input_tokens default to 0,
    so the Step 9 prompt-caching telemetry hooks in cleanly."""
    m = SynthesisMetadata(model_used="x", theses_doc_available=False)
    assert m.cache_creation_input_tokens == 0
    assert m.cache_read_input_tokens == 0
    assert m.input_tokens == 0
    assert m.output_tokens == 0
