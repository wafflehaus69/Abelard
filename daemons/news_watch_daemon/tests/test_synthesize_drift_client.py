"""Drift LLM client tests — JSON parse, text extraction, cache telemetry."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from news_watch_daemon.synthesize.drift_client import (
    DriftLLMError,
    DriftResponse,
    call_drift_llm,
    parse_drift_response,
)


# ---------- response stubs ----------


def _text_block(text: str) -> SimpleNamespace:
    return SimpleNamespace(type="text", text=text)


def _thinking_block() -> SimpleNamespace:
    return SimpleNamespace(type="thinking", thinking="internal")


def _response(
    blocks: list[SimpleNamespace],
    model: str = "claude-haiku-4-5-20251029",
    input_tokens: int = 1200,
    output_tokens: int = 300,
    cache_creation_input_tokens: int = 0,
    cache_read_input_tokens: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        content=blocks,
        model=model,
        usage=SimpleNamespace(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
        ),
    )


class _FakeStreamContext:
    """Mimics anthropic's MessageStreamManager (post-2026-05-14 fix)."""
    def __init__(self, response):
        self._response = response
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def get_final_message(self):
        return self._response


class _FakeClient:
    def __init__(self, response):
        self.last_call_kwargs: dict | None = None
        self.messages = SimpleNamespace(stream=self._stream)
        self._response = response

    def _stream(self, **kwargs):
        self.last_call_kwargs = kwargs
        return _FakeStreamContext(self._response)


def _valid_proposals_json() -> str:
    return json.dumps({
        "proposals": [
            {
                "theme_id": "t1",
                "proposed_keyword": "phrase one",
                "suggested_tier": "secondary",
                "evidence_count": 5,
                "sample_headlines": ["one", "two"],
                "notes": "n",
            }
        ]
    })


# ---------- parse_drift_response ----------


def test_parse_clean_json():
    proposals = parse_drift_response(_valid_proposals_json())
    assert len(proposals) == 1
    assert proposals[0]["theme_id"] == "t1"


def test_parse_empty_proposals_list_ok():
    proposals = parse_drift_response('{"proposals": []}')
    assert proposals == []


def test_parse_fenced_json_with_lang():
    text = "```json\n" + _valid_proposals_json() + "\n```"
    proposals = parse_drift_response(text)
    assert len(proposals) == 1


def test_parse_fenced_json_no_lang():
    text = "```\n" + _valid_proposals_json() + "\n```"
    proposals = parse_drift_response(text)
    assert len(proposals) == 1


def test_parse_malformed_json_raises():
    with pytest.raises(DriftLLMError, match="failed to parse"):
        parse_drift_response("{not json")


def test_parse_root_not_object_raises():
    with pytest.raises(DriftLLMError, match="must be a JSON object"):
        parse_drift_response("[]")


def test_parse_missing_proposals_raises():
    with pytest.raises(DriftLLMError, match="proposals"):
        parse_drift_response('{"foo": []}')


def test_parse_proposals_non_list_raises():
    with pytest.raises(DriftLLMError, match="must be a list"):
        parse_drift_response('{"proposals": "string"}')


# ---------- call_drift_llm ----------


def test_call_passes_correct_kwargs_to_sdk():
    client = _FakeClient(_response([_text_block(_valid_proposals_json())]))
    payload = {
        "system": [{"type": "text", "text": "sys"}],
        "messages": [{"role": "user", "content": "u"}],
    }
    call_drift_llm(client=client, model="claude-haiku-4-5", max_tokens=1024, payload=payload)
    k = client.last_call_kwargs
    assert k["model"] == "claude-haiku-4-5"
    assert k["max_tokens"] == 1024
    # Drift mirrors synthesis: thinking DISABLED after live smoke #3
    # (2026-05-14). Structured-output task; judgment lives in the prompt.
    assert k["thinking"] == {"type": "disabled"}
    assert k["system"] == payload["system"]
    assert k["messages"] == payload["messages"]


def test_call_does_not_pass_effort_param():
    """Effort param errors on Haiku 4.5 per claude-api skill —
    must NOT be in the SDK call kwargs."""
    client = _FakeClient(_response([_text_block(_valid_proposals_json())]))
    call_drift_llm(
        client=client, model="claude-haiku-4-5", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    k = client.last_call_kwargs
    assert "effort" not in k
    # Also not in output_config.
    assert "output_config" not in k


def test_call_extracts_cache_telemetry_creation():
    client = _FakeClient(_response(
        [_text_block(_valid_proposals_json())],
        cache_creation_input_tokens=1100,
        cache_read_input_tokens=0,
    ))
    result = call_drift_llm(
        client=client, model="m", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    assert isinstance(result, DriftResponse)
    assert result.cache_creation_input_tokens == 1100
    assert result.cache_read_input_tokens == 0


def test_call_extracts_cache_read_on_second_call():
    client = _FakeClient(_response(
        [_text_block(_valid_proposals_json())],
        cache_creation_input_tokens=0,
        cache_read_input_tokens=1100,
    ))
    result = call_drift_llm(
        client=client, model="m", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    assert result.cache_creation_input_tokens == 0
    assert result.cache_read_input_tokens == 1100


def test_call_skips_thinking_blocks():
    client = _FakeClient(_response([
        _thinking_block(),
        _text_block(_valid_proposals_json()),
    ]))
    result = call_drift_llm(
        client=client, model="m", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    assert len(result.proposals_payload) == 1


def test_call_no_text_blocks_raises():
    client = _FakeClient(_response([_thinking_block()]))
    with pytest.raises(DriftLLMError, match="no text content"):
        call_drift_llm(
            client=client, model="m", max_tokens=1024,
            payload={"system": [], "messages": []},
        )


def test_call_returns_resolved_model_id():
    client = _FakeClient(_response(
        [_text_block(_valid_proposals_json())],
        model="claude-haiku-4-5-20251029",
    ))
    result = call_drift_llm(
        client=client, model="claude-haiku-4-5", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    assert result.model_used == "claude-haiku-4-5-20251029"


def test_call_missing_usage_defaults_to_zero():
    response = SimpleNamespace(
        content=[_text_block(_valid_proposals_json())],
        model="m",
    )
    client = _FakeClient(response)
    result = call_drift_llm(
        client=client, model="m", max_tokens=1024,
        payload={"system": [], "messages": []},
    )
    assert result.input_tokens == 0
    assert result.output_tokens == 0
    assert result.cache_creation_input_tokens == 0
    assert result.cache_read_input_tokens == 0
