"""Synthesis orchestrator tests — theses loading + event validation + Brief assembly."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from news_watch_daemon.synthesize.brief import (
    Brief,
    EnvelopeHealth,
    Trigger,
    TriggerWindow,
)
from news_watch_daemon.synthesize.cluster import Cluster, ClusterInput
from news_watch_daemon.synthesize.synthesize import (
    SynthesisError,
    build_anthropic_client,
    synthesize_brief,
)


# ---------- helpers ----------


def _trigger() -> Trigger:
    return Trigger(
        type="event", reason="t",
        window=TriggerWindow(since="2026-05-13T10:00:00Z", until="2026-05-13T14:00:00Z"),
    )


def _cluster() -> Cluster:
    member = ClusterInput(
        headline_id="h-1", headline="A thing happened",
        url="https://x", publisher="Reuters", published_at_unix=1764100000,
    )
    return Cluster(headline_ids=("h-1",), members=(member,))


def _text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _make_response(
    events: list[dict] | None = None,
    narrative: str = "n",
    model: str = "claude-sonnet-4-6-20251029",
    input_tokens: int = 1000,
    output_tokens: int = 200,
    cache_creation_input_tokens: int = 800,
    cache_read_input_tokens: int = 0,
) -> SimpleNamespace:
    if events is None:
        events = [
            {
                "event_id": "evt-1",
                "headline_summary": "An event happened",
                "themes": ["t1"],
                "source_headlines": [
                    {
                        "publisher": "Reuters",
                        "headline": "A thing happened",
                        "url": "https://x",
                        "published_at": "2026-05-13T13:00:00Z",
                    }
                ],
                "materiality_score": 0.7,
                "thesis_links": [],
            }
        ]
    payload = json.dumps({"events": events, "narrative": narrative})
    return SimpleNamespace(
        content=[_text_block(payload)],
        model=model,
        usage=SimpleNamespace(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
        ),
    )


class _FakeClient:
    def __init__(self, response):
        self.last_call_kwargs: dict | None = None
        self.messages = SimpleNamespace(create=self._create)
        self._response = response

    def _create(self, **kwargs):
        self.last_call_kwargs = kwargs
        return self._response


# ---------- _load_theses_doc (via synthesize_brief) ----------


def test_synthesize_records_theses_unset_warning(tmp_path):
    client = _FakeClient(_make_response())
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "brief text"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.synthesis_metadata.theses_doc_available is False
    assert brief.synthesis_metadata.theses_doc_path is None
    assert "not set" in (brief.synthesis_metadata.theses_doc_warning or "")


def test_synthesize_records_theses_missing_file_warning(tmp_path):
    client = _FakeClient(_make_response())
    missing = tmp_path / "no-theses-here.md"
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "brief"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=missing,
    )
    assert brief.synthesis_metadata.theses_doc_available is False
    assert brief.synthesis_metadata.theses_doc_path == str(missing)
    assert "not found" in (brief.synthesis_metadata.theses_doc_warning or "")


def test_synthesize_records_theses_empty_file_warning(tmp_path):
    client = _FakeClient(_make_response())
    empty = tmp_path / "empty.md"
    empty.write_text("   \n  \n", encoding="utf-8")
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=empty,
    )
    assert brief.synthesis_metadata.theses_doc_available is False
    assert "empty" in (brief.synthesis_metadata.theses_doc_warning or "")


def test_synthesize_theses_doc_passed_to_prompt(tmp_path):
    client = _FakeClient(_make_response())
    theses = tmp_path / "theses.md"
    theses.write_text("## thesis-id-x\nDetails here.\n", encoding="utf-8")
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=theses,
    )
    assert brief.synthesis_metadata.theses_doc_available is True
    assert brief.synthesis_metadata.theses_doc_warning is None
    # Verify system blocks include the theses content.
    system_blocks = client.last_call_kwargs["system"]
    assert len(system_blocks) == 2
    assert "thesis-id-x" in system_blocks[1]["text"]


# ---------- Brief assembly ----------


def test_synthesize_assembles_full_brief(tmp_path):
    client = _FakeClient(_make_response())
    now = datetime(2026, 5, 13, 14, 32, 8, tzinfo=timezone.utc)
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["us_iran_escalation"],
        theme_briefs={"us_iran_escalation": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
        now=now,
    )
    assert isinstance(brief, Brief)
    assert brief.brief_id.startswith("nwd-2026-05-13T14-32-08Z-")
    assert brief.generated_at == "2026-05-13T14:32:08Z"
    assert brief.themes_covered == ["us_iran_escalation"]
    assert len(brief.events) == 1
    assert brief.events[0].event_id == "evt-1"
    assert brief.narrative == "n"


def test_synthesize_initial_dispatch_is_alerted_false(tmp_path):
    """The orchestrator sets dispatch.alerted=False; materiality gate
    (a separate component) decides whether to flip it later."""
    client = _FakeClient(_make_response())
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.dispatch.alerted is False
    assert brief.dispatch.channel is None
    assert brief.dispatch.suppressed_reason is None


def test_synthesize_records_cache_telemetry(tmp_path):
    client = _FakeClient(_make_response(
        input_tokens=2400,
        output_tokens=400,
        cache_creation_input_tokens=2000,
        cache_read_input_tokens=0,
    ))
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    md = brief.synthesis_metadata
    assert md.input_tokens == 2400
    assert md.output_tokens == 400
    assert md.cache_creation_input_tokens == 2000
    assert md.cache_read_input_tokens == 0


def test_synthesize_records_resolved_model_id(tmp_path):
    """Records the response.model (resolved with date suffix), not the
    requested family name."""
    client = _FakeClient(_make_response(model="claude-sonnet-4-6-20251029"))
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",  # family name
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.synthesis_metadata.model_used == "claude-sonnet-4-6-20251029"


def test_synthesize_envelope_health_passthrough(tmp_path):
    client = _FakeClient(_make_response())
    health = EnvelopeHealth(
        source_health={"finnhub:general": "ok"},
        heartbeats={"scrape": "2026-05-13T14:00:00Z"},
    )
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
        envelope_health=health,
    )
    assert brief.envelope_health.source_health == {"finnhub:general": "ok"}


def test_synthesize_default_envelope_health_empty(tmp_path):
    client = _FakeClient(_make_response())
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.envelope_health.source_health == {}
    assert brief.envelope_health.heartbeats == {}


def test_synthesize_drift_proposals_empty(tmp_path):
    """Step 9 orchestrator does NOT populate drift_proposals; that's
    Step 10's drift watcher (Haiku, separate call)."""
    client = _FakeClient(_make_response())
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.drift_proposals == []


# ---------- event validation ----------


def test_synthesize_invalid_event_raises_synthesis_error(tmp_path):
    """If Sonnet emits an event with a bad field (e.g. score > 1.0),
    the orchestrator wraps Pydantic's ValidationError in SynthesisError."""
    bad_event = {
        "event_id": "evt-1",
        "headline_summary": "x",
        "themes": ["t1"],
        "source_headlines": [],
        "materiality_score": 1.5,  # >1.0 -> ValidationError
        "thesis_links": [],
    }
    client = _FakeClient(_make_response(events=[bad_event]))
    with pytest.raises(SynthesisError, match="event validation failed"):
        synthesize_brief(
            client=client,
            model="claude-sonnet-4-6",
            max_tokens=2048,
            trigger=_trigger(),
            themes_in_scope=["t1"],
            theme_briefs={"t1": "b"},
            clusters=[_cluster()],
            max_events_per_brief=8,
            theses_path=None,
        )


def test_synthesize_aggregates_multiple_event_errors(tmp_path):
    """If multiple events fail validation, the error message lists each.
    Diagnoses Sonnet-side schema drift faster than failing on the first."""
    bad_events = [
        {
            "event_id": "evt-1",
            "headline_summary": "x",
            "themes": ["t1"],
            "source_headlines": [],
            "materiality_score": 1.5,  # bad
            "thesis_links": [],
        },
        {
            "event_id": "evt-2",
            "headline_summary": "y",
            "themes": ["t1"],
            "source_headlines": [],
            "materiality_score": -0.5,  # bad
            "thesis_links": [],
        },
    ]
    client = _FakeClient(_make_response(events=bad_events))
    with pytest.raises(SynthesisError, match="events\\[1\\]"):
        synthesize_brief(
            client=client,
            model="claude-sonnet-4-6",
            max_tokens=2048,
            trigger=_trigger(),
            themes_in_scope=["t1"],
            theme_briefs={"t1": "b"},
            clusters=[_cluster()],
            max_events_per_brief=8,
            theses_path=None,
        )


def test_synthesize_empty_events_ok(tmp_path):
    """Sonnet may return zero events; that's the 'cycle quiet' path,
    not an error."""
    client = _FakeClient(_make_response(events=[], narrative="No events."))
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[],
        max_events_per_brief=8,
        theses_path=None,
    )
    assert brief.events == []
    assert brief.narrative == "No events."


# ---------- build_anthropic_client ----------


def test_build_anthropic_client_empty_key_raises():
    with pytest.raises(SynthesisError, match="ANTHROPIC_API_KEY is empty"):
        build_anthropic_client("")


def test_build_anthropic_client_with_key():
    """With a (fake) key, the function should construct without raising —
    construction doesn't validate the key, only its presence."""
    pytest.importorskip("anthropic")
    client = build_anthropic_client("sk-ant-fake-key-for-unit-test")
    # The returned client should expose messages.create as a callable.
    assert hasattr(client, "messages")
    assert callable(client.messages.create)


# ---------- timestamp handling ----------


def test_synthesize_now_naive_assumed_utc(tmp_path):
    """Naive datetime is interpreted as UTC, not local-time."""
    client = _FakeClient(_make_response())
    naive = datetime(2026, 5, 13, 12, 0, 0)  # no tzinfo
    brief = synthesize_brief(
        client=client,
        model="claude-sonnet-4-6",
        max_tokens=2048,
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[_cluster()],
        max_events_per_brief=8,
        theses_path=None,
        now=naive,
    )
    assert brief.generated_at == "2026-05-13T12:00:00Z"
