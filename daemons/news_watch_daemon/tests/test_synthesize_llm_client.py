"""LLM client tests — response parsing, text extraction, cache telemetry."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from news_watch_daemon.synthesize.llm_client import (
    SynthesisLLMError,
    SynthesisResponse,
    call_synthesis_llm,
    parse_synthesis_response,
)


# ---------- response stubs ----------


def _text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _thinking_block(text: str = "internal reasoning") -> SimpleNamespace:
    return SimpleNamespace(type="thinking", thinking=text)


def _usage(
    input_tokens: int = 100,
    output_tokens: int = 50,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        cache_read_input_tokens=cache_read_input_tokens,
    )


def _response(
    blocks: list[SimpleNamespace],
    model: str = "claude-sonnet-4-6-20251029",
    **usage_kwargs,
) -> SimpleNamespace:
    return SimpleNamespace(
        content=blocks,
        model=model,
        usage=_usage(**usage_kwargs),
    )


class _FakeStreamContext:
    """Mimics anthropic's MessageStreamManager — yields itself on
    __enter__, surfaces the canned final-message via .get_final_message().

    The production code uses `with client.messages.stream(...) as s:
    s.get_final_message()` after the 2026-05-14 live-smoke fix
    switched off the non-streaming `messages.create()` path.
    """
    def __init__(self, response):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def get_final_message(self):
        return self._response


class _FakeClient:
    """Mock Anthropic client. Captures the last stream() call's kwargs."""

    def __init__(self, response: SimpleNamespace):
        self.response = response
        self.last_call_kwargs: dict | None = None
        self.messages = SimpleNamespace(stream=self._stream)

    def _stream(self, **kwargs):
        self.last_call_kwargs = kwargs
        return _FakeStreamContext(self.response)


# ---------- parse_synthesis_response ----------


def _valid_json() -> str:
    return json.dumps({
        "events": [
            {
                "event_id": "evt-1",
                "headline_summary": "x",
                "themes": ["t1"],
                "source_headlines": [],
                "materiality_score": 0.7,
                "thesis_links": [],
            }
        ],
        "narrative": "summary",
    })


def test_parse_clean_json():
    events, narrative = parse_synthesis_response(_valid_json())
    assert len(events) == 1
    assert events[0]["event_id"] == "evt-1"
    assert narrative == "summary"


def test_parse_fenced_json_with_lang_tag():
    fenced = "```json\n" + _valid_json() + "\n```"
    events, narrative = parse_synthesis_response(fenced)
    assert len(events) == 1
    assert narrative == "summary"


def test_parse_fenced_json_no_lang_tag():
    fenced = "```\n" + _valid_json() + "\n```"
    events, narrative = parse_synthesis_response(fenced)
    assert len(events) == 1


def test_parse_leading_whitespace_tolerated():
    events, _ = parse_synthesis_response("\n   " + _valid_json() + "   \n")
    assert len(events) == 1


def test_parse_malformed_json_raises():
    with pytest.raises(SynthesisLLMError, match="failed to parse"):
        parse_synthesis_response("{not valid json")


def test_parse_root_not_object_raises():
    with pytest.raises(SynthesisLLMError, match="must be a JSON object"):
        parse_synthesis_response("[1, 2, 3]")


def test_parse_missing_events_raises():
    with pytest.raises(SynthesisLLMError, match="events"):
        parse_synthesis_response('{"narrative": "x"}')


def test_parse_missing_narrative_raises():
    with pytest.raises(SynthesisLLMError, match="narrative"):
        parse_synthesis_response('{"events": []}')


def test_parse_events_non_list_raises():
    with pytest.raises(SynthesisLLMError, match="events"):
        parse_synthesis_response('{"events": "string", "narrative": "n"}')


def test_parse_narrative_non_string_raises():
    with pytest.raises(SynthesisLLMError, match="narrative"):
        parse_synthesis_response('{"events": [], "narrative": 42}')


def test_parse_empty_events_list_ok():
    events, narrative = parse_synthesis_response(
        '{"events": [], "narrative": "Cycle produced no material events."}'
    )
    assert events == []
    assert "no material" in narrative


# ---------- call_synthesis_llm ----------


def test_call_passes_correct_kwargs_to_sdk():
    client = _FakeClient(_response([_text_block(_valid_json())]))
    payload = {
        "system": [{"type": "text", "text": "sys"}],
        "messages": [{"role": "user", "content": "hi"}],
    }
    call_synthesis_llm(client=client, model="m-id", max_tokens=2048, payload=payload)
    kwargs = client.last_call_kwargs
    assert kwargs is not None
    assert kwargs["model"] == "m-id"
    assert kwargs["max_tokens"] == 2048
    assert kwargs["system"] == payload["system"]
    assert kwargs["messages"] == payload["messages"]
    # Adaptive thinking — non-trivial task default per claude-api skill.
    assert kwargs["thinking"] == {"type": "adaptive"}


def test_call_uses_streaming_path_not_create():
    """Live smoke #2 (2026-05-14) regression pin: the synthesis call
    MUST go through `client.messages.stream(...)` + .get_final_message(),
    not `client.messages.create(...)`. Non-streaming hits a 3-minute
    server-side disconnect on long synthesis calls with adaptive
    thinking.

    The _FakeClient mock only exposes `stream` — if production code
    ever reverts to `create()`, this test will fail with AttributeError
    before the rest of the suite even gets the chance.
    """
    client = _FakeClient(_response([_text_block(_valid_json())]))
    # Streaming path is what the production code calls. If anyone
    # refactors back to `messages.create()`, this would raise
    # AttributeError when called against our mock.
    assert hasattr(client.messages, "stream")
    assert not hasattr(client.messages, "create")
    # Sanity check: the call goes through and returns a parsed response.
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert isinstance(result, SynthesisResponse)


def test_call_extracts_cache_telemetry():
    client = _FakeClient(_response(
        [_text_block(_valid_json())],
        input_tokens=1500,
        output_tokens=300,
        cache_creation_input_tokens=1200,
        cache_read_input_tokens=0,
    ))
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert isinstance(result, SynthesisResponse)
    assert result.input_tokens == 1500
    assert result.output_tokens == 300
    assert result.cache_creation_input_tokens == 1200
    assert result.cache_read_input_tokens == 0


def test_call_extracts_cache_read_on_second_call():
    """Second-call shape: prefix cached, only delta is new."""
    client = _FakeClient(_response(
        [_text_block(_valid_json())],
        input_tokens=400,
        cache_creation_input_tokens=0,
        cache_read_input_tokens=1200,
    ))
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert result.cache_read_input_tokens == 1200
    assert result.cache_creation_input_tokens == 0


def test_call_returns_model_from_response():
    client = _FakeClient(_response(
        [_text_block(_valid_json())],
        model="claude-sonnet-4-6-20251029",
    ))
    result = call_synthesis_llm(
        client=client, model="claude-sonnet-4-6", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    # Production records the resolved model id (with date suffix) so
    # Checkpoint 4 can correlate cost telemetry to the exact model.
    assert result.model_used == "claude-sonnet-4-6-20251029"


def test_call_skips_thinking_blocks():
    """Adaptive thinking emits a thinking block before the text; we
    only want the assistant's text output for JSON parsing."""
    client = _FakeClient(_response([
        _thinking_block("reasoning..."),
        _text_block(_valid_json()),
    ]))
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert len(result.events_payload) == 1


def test_call_concatenates_multiple_text_blocks():
    """Robust to multi-block text output (rare, but possible)."""
    half_a = '{"events": [], "narrative": "h'
    half_b = 'alf"}'
    client = _FakeClient(_response([_text_block(half_a), _text_block(half_b)]))
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert result.narrative == "half"


def test_call_no_text_blocks_raises():
    """All-thinking response (no spoken text) -> SynthesisLLMError."""
    client = _FakeClient(_response([_thinking_block("only thought")]))
    with pytest.raises(SynthesisLLMError, match="no text content"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_call_empty_content_raises():
    """Empty content list -> SynthesisLLMError."""
    client = _FakeClient(_response([]))
    with pytest.raises(SynthesisLLMError, match="no text content"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


# ---------- diagnostic detail on no-text-blocks (live-smoke 2026-05-14) ----


def _budget_exhausted_response(max_tokens_consumed: int = 2048) -> SimpleNamespace:
    """The shape Anthropic returns when adaptive thinking exhausts the
    output budget before emitting any text block. Captured from the
    first live smoke failure (2026-05-14, Pass C close).

    Pinned here as a fixture so any regression on the diagnostic
    surface (or on the error-construction logic) is caught by
    hermetic tests.
    """
    return SimpleNamespace(
        content=[_thinking_block("long internal reasoning ...")],
        model="claude-sonnet-4-6-20251029",
        stop_reason="max_tokens",
        usage=SimpleNamespace(
            input_tokens=2400,
            output_tokens=max_tokens_consumed,
            cache_creation_input_tokens=1800,
            cache_read_input_tokens=0,
        ),
    )


def test_no_text_error_surfaces_stop_reason():
    """Error message must include stop_reason — operators diagnose
    'max_tokens' vs 'end_turn' vs 'refusal' from the envelope alone."""
    client = _FakeClient(_budget_exhausted_response())
    with pytest.raises(SynthesisLLMError, match="stop_reason='max_tokens'"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_no_text_error_surfaces_output_tokens_used():
    client = _FakeClient(_budget_exhausted_response(max_tokens_consumed=2048))
    with pytest.raises(SynthesisLLMError, match="output_tokens=2048"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_no_text_error_surfaces_max_tokens_requested():
    """The CALLER's max_tokens ceiling must appear in the error so
    operators see the gap between request and consumption."""
    client = _FakeClient(_budget_exhausted_response(max_tokens_consumed=2048))
    with pytest.raises(SynthesisLLMError, match="max_tokens_requested=2048"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_no_text_error_surfaces_block_types():
    """Block types list lets operators see 'only thinking blocks
    came back' without instrumenting a live call."""
    client = _FakeClient(_budget_exhausted_response())
    with pytest.raises(SynthesisLLMError, match=r"block_types=\['thinking'\]"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_no_text_error_includes_remediation_hint():
    """The error must point at the tuning knob — operators shouldn't
    have to read the source to know where to bump max_tokens."""
    client = _FakeClient(_budget_exhausted_response())
    with pytest.raises(
        SynthesisLLMError,
        match="increase synthesis.default_max_tokens",
    ):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_no_text_error_when_stop_reason_missing():
    """If the SDK response lacks stop_reason (older SDK / mock without
    the attr), the error still surfaces with stop_reason='unknown'."""
    response = SimpleNamespace(
        content=[_thinking_block()],
        model="m",
        # No stop_reason attribute.
    )
    client = _FakeClient(response)
    with pytest.raises(SynthesisLLMError, match="stop_reason='unknown'"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )


def test_call_missing_usage_returns_zero_telemetry():
    """If the SDK response lacks `usage` (mock without it), counts default to 0."""
    response = SimpleNamespace(
        content=[_text_block(_valid_json())],
        model="m",
        # No `usage` attribute at all.
    )
    client = _FakeClient(response)
    result = call_synthesis_llm(
        client=client, model="m", max_tokens=2048,
        payload={"system": [], "messages": []},
    )
    assert result.input_tokens == 0
    assert result.cache_creation_input_tokens == 0


def test_call_propagates_malformed_response_text():
    """LLM emits non-JSON -> SynthesisLLMError from parse layer."""
    client = _FakeClient(_response([_text_block("Sure, here is the brief: ...")]))
    with pytest.raises(SynthesisLLMError, match="failed to parse"):
        call_synthesis_llm(
            client=client, model="m", max_tokens=2048,
            payload={"system": [], "messages": []},
        )
