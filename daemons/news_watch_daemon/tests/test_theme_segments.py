"""Unit tests for fullbrief/theme_segments.py.

Covers status classification (in-scope OR high-tag => active), the batched
prompt build, defensive JSON parsing, the template fallback, and the
single batched call against a fake streaming client.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from news_watch_daemon.fullbrief.theme_segments import (
    ACTIVE_TAG_THRESHOLD,
    ThemeSegmentInput,
    ThemeSegmentsError,
    build_segment_prompt,
    parse_segment_response,
    synthesize_theme_segments,
    template_summary,
)


def _inp(theme_id="t", *, tag_count=0, in_scope=False, headlines=None, conv=None):
    return ThemeSegmentInput(
        theme_id=theme_id,
        display_name=theme_id.title(),
        tag_count=tag_count,
        in_scope=in_scope,
        sample_headlines=headlines or [],
        convergence_terms=conv or [],
    )


# ---------- status classification ----------

def test_status_active_when_in_scope_even_zero_tags():
    assert _inp(in_scope=True, tag_count=0).status() == "active"


def test_status_active_when_tag_count_meets_threshold():
    assert _inp(in_scope=False, tag_count=ACTIVE_TAG_THRESHOLD).status() == "active"


def test_status_quiet_when_below_threshold_and_out_of_scope():
    assert _inp(in_scope=False, tag_count=ACTIVE_TAG_THRESHOLD - 1).status() == "quiet"


# ---------- prompt build ----------

def test_build_segment_prompt_includes_each_theme_and_status():
    inputs = [
        _inp("russia_ukraine_war", tag_count=47, headlines=["Russia strikes Kyiv"]),
        _inp("china_us_decoupling", tag_count=3, headlines=["Chip export ban"]),
    ]
    payload = build_segment_prompt(inputs)
    assert "system" in payload and payload["messages"][0]["role"] == "user"
    text = payload["messages"][0]["content"]
    assert "russia_ukraine_war" in text
    assert "china_us_decoupling" in text
    assert "status: active" in text   # russia 47 >= threshold
    assert "status: quiet" in text    # china 3 < threshold
    assert "Russia strikes Kyiv" in text


# ---------- parse ----------

def test_parse_valid_json():
    text = json.dumps({"segments": {"a": "line a", "b": "line b"}})
    out = parse_segment_response(text, ["a", "b"])
    assert out == {"a": "line a", "b": "line b"}


def test_parse_strips_markdown_fence():
    text = "```json\n" + json.dumps({"segments": {"a": "x"}}) + "\n```"
    assert parse_segment_response(text, ["a"]) == {"a": "x"}


def test_parse_tolerates_missing_theme():
    text = json.dumps({"segments": {"a": "x"}})
    # 'b' expected but absent — tolerated (orchestrator templates it).
    assert parse_segment_response(text, ["a", "b"]) == {"a": "x"}


def test_parse_ignores_unexpected_ids_and_blank_values():
    text = json.dumps({"segments": {"a": "x", "z": "unexpected", "b": "   "}})
    assert parse_segment_response(text, ["a", "b"]) == {"a": "x"}


def test_parse_raises_on_non_object_root():
    with pytest.raises(ThemeSegmentsError):
        parse_segment_response("[1, 2, 3]", ["a"])


def test_parse_raises_on_bad_json():
    with pytest.raises(ThemeSegmentsError):
        parse_segment_response("{not json", ["a"])


def test_parse_raises_when_segments_not_object():
    with pytest.raises(ThemeSegmentsError):
        parse_segment_response(json.dumps({"segments": "nope"}), ["a"])


# ---------- template fallback ----------

def test_template_summary_states_count_and_top_headline():
    line = template_summary(_inp("t", tag_count=12, headlines=["Big thing happened"]))
    assert "12 tagged headlines" in line
    assert "Big thing happened" in line
    assert "summary unavailable" in line


def test_template_summary_handles_no_headlines():
    line = template_summary(_inp("t", tag_count=0))
    assert "no headlines" in line


# ---------- batched call against a fake streaming client ----------

class _FakeStream:
    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def get_final_message(self):
        return self._message


class _FakeMessages:
    def __init__(self, message):
        self._message = message
        self.last_kwargs = None

    def stream(self, **kwargs):
        self.last_kwargs = kwargs
        return _FakeStream(self._message)


class _FakeClient:
    def __init__(self, text, usage):
        message = SimpleNamespace(
            content=[SimpleNamespace(type="text", text=text)],
            usage=usage,
            model="claude-sonnet-4-6",
            stop_reason="end_turn",
        )
        self.messages = _FakeMessages(message)


def test_synthesize_theme_segments_returns_summaries_and_metadata():
    body = json.dumps({"segments": {
        "russia_ukraine_war": "Heavy strikes on Kyiv this window.",
        "china_us_decoupling": "Quiet — routine chip-policy chatter.",
    }})
    usage = SimpleNamespace(
        input_tokens=1200, output_tokens=210,
        cache_creation_input_tokens=0, cache_read_input_tokens=800,
    )
    client = _FakeClient(body, usage)
    inputs = [
        _inp("russia_ukraine_war", tag_count=47, headlines=["Kyiv hit"]),
        _inp("china_us_decoupling", tag_count=3, headlines=["chip ban"]),
    ]
    summaries, meta = synthesize_theme_segments(
        client=client, model="claude-sonnet-4-6", max_tokens=1500, inputs=inputs,
    )
    assert summaries["russia_ukraine_war"].startswith("Heavy strikes")
    assert summaries["china_us_decoupling"].startswith("Quiet")
    assert meta.input_tokens == 1200
    assert meta.output_tokens == 210
    assert meta.cache_read_input_tokens == 800
    # thinking disabled + streaming used
    assert client.messages.last_kwargs["thinking"] == {"type": "disabled"}


def test_synthesize_theme_segments_raises_on_empty_text():
    usage = SimpleNamespace(
        input_tokens=1, output_tokens=0,
        cache_creation_input_tokens=0, cache_read_input_tokens=0,
    )
    client = _FakeClient("", usage)
    with pytest.raises(ThemeSegmentsError):
        synthesize_theme_segments(
            client=client, model="claude-sonnet-4-6", max_tokens=1500,
            inputs=[_inp("t", tag_count=1)],
        )
