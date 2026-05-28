"""Orchestrator tests — end-to-end with mocked LLM, tmp-path archive."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from news_watch_daemon.alert.null_sink import NullSink
from news_watch_daemon.attention.brief_schema import AttentionBrief
from news_watch_daemon.attention.orchestrator import (
    AttentionLLMError,
    AttentionRunResult,
    PerTermOutcome,
    _parse_attention_response,
    run_attention,
)


# ---------- fixtures ----------


NOW = 1_800_000_000   # arbitrary anchor


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        # headline_en added Pass F (2026-05-28); existing inserts leave
        # it NULL, COALESCE(headline_en, headline) returns headline.
        "CREATE TABLE headlines ("
        "headline_id TEXT PRIMARY KEY, source TEXT, headline TEXT, "
        "headline_en TEXT, url TEXT, raw_source TEXT, published_at_unix INTEGER)"
    )
    return conn


def _insert(conn, *, hid: str, headline: str, ts: int = NOW - 1000,
            source: str = "telegram:CIG_telegram") -> None:
    conn.execute(
        "INSERT INTO headlines (headline_id, source, headline, url, raw_source, published_at_unix) "
        "VALUES (?, ?, ?, NULL, NULL, ?)",
        (hid, source, headline, ts),
    )


def _seed_crossing_term(conn, term: str = "hormuz", count: int = 12) -> None:
    """Insert `count` headlines containing only `term` so it's the only crossing
    candidate. Single-word headlines avoid polluting with other tokens that
    would also cross the threshold."""
    for i in range(count):
        _insert(conn, hid=f"h-{term}-{i}", headline=term, ts=NOW - 100 - i)


class _FakeUsage:
    def __init__(self, in_t=100, out_t=200, cc=50, cr=0):
        self.input_tokens = in_t
        self.output_tokens = out_t
        self.cache_creation_input_tokens = cc
        self.cache_read_input_tokens = cr


class _FakeBlock:
    def __init__(self, text: str):
        self.type = "text"
        self.text = text


class _FakeResponse:
    def __init__(self, text: str, model: str = "claude-sonnet-4-6"):
        self.content = [_FakeBlock(text)]
        self.usage = _FakeUsage()
        self.model = model
        self.stop_reason = "end_turn"


class _FakeStream:
    def __init__(self, response: _FakeResponse):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def get_final_message(self):
        return self._response


class _FakeMessages:
    def __init__(self, response_text: str):
        self._response_text = response_text

    def stream(self, **kwargs) -> _FakeStream:
        return _FakeStream(_FakeResponse(self._response_text))


class _FakeClient:
    """Anthropic-shaped client whose stream returns a canned text response."""
    def __init__(self, response_text: str):
        self.messages = _FakeMessages(response_text)


_VALID_LLM_RESPONSE = json.dumps({
    "narrative": "Sample attention narrative paragraph about the term hormuz.",
    "source_mix": {"telegram:CIG_telegram": 12},
    "entities_observed": ["CENTCOM", "Strait of Hormuz"],
    "attention_shape": "multi_source_convergence",
})


# ---------- run_attention end-to-end ----------


def test_run_attention_no_crossings_returns_candidates(tmp_path):
    """Zero crossings → empty per_term, top-K candidates surfaced."""
    conn = _make_conn()
    # 8 headlines = below the 10 floor but above the 5 candidate floor
    for i in range(8):
        _insert(conn, hid=f"h{i}", headline=f"news about chemicals incident {i}", ts=NOW - 100 - i)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=None, model="claude-sonnet-4-6", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=None,
    )
    assert result.crossings_evaluated == 0
    assert result.per_term == []
    # 'chemicals' and 'incident' both appear 8 times — should surface as candidates
    cand_terms = {c.term for c in result.candidates}
    assert "chemicals" in cand_terms
    assert "incident" in cand_terms


def test_run_attention_one_crossing_calls_llm_and_archives(tmp_path):
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    sink = NullSink()
    client = _FakeClient(_VALID_LLM_RESPONSE)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="claude-sonnet-4-6", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=sink,
    )
    assert result.crossings_evaluated == 1
    assert len(result.per_term) == 1
    outcome = result.per_term[0]
    assert outcome.success is True
    assert outcome.term == "hormuz"
    assert outcome.brief_id is not None
    assert outcome.brief_id.startswith("nwd-attn-")
    assert outcome.archive_path is not None
    # Brief was dispatched via NullSink
    assert outcome.dispatch_success is True
    assert len(sink.dispatched) == 1
    dispatched_brief = sink.dispatched[0]
    assert isinstance(dispatched_brief, AttentionBrief)
    assert dispatched_brief.triggering_term == "hormuz"
    assert dispatched_brief.attention_shape == "multi_source_convergence"
    # And the file is on disk
    assert Path(outcome.archive_path).exists()


def test_run_attention_llm_parse_failure_recorded_as_error(tmp_path):
    """Malformed JSON from the LLM → per_term failure, cycle continues
    without crashing. archive write does NOT happen for this term."""
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    client = _FakeClient("not json at all")

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="m", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=None,
    )
    assert len(result.per_term) == 1
    outcome = result.per_term[0]
    assert outcome.success is False
    assert outcome.brief_id is None
    assert "llm_error" in outcome.error


def test_run_attention_invalid_attention_shape_fails_loud(tmp_path):
    """attention_shape outside the closed Literal set → AttentionLLMError.
    Per Pass E Q5: fail loud, do NOT silently coerce."""
    bad_response = json.dumps({
        "narrative": "x",
        "source_mix": {"x": 1},
        "entities_observed": [],
        "attention_shape": "made_up_shape_not_in_literal",
    })
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    client = _FakeClient(bad_response)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="m", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=None,
    )
    outcome = result.per_term[0]
    assert outcome.success is False
    assert "attention_shape" in outcome.error
    assert "made_up_shape_not_in_literal" in outcome.error


def test_run_attention_dispatch_failure_still_archives_brief(tmp_path):
    """If the sink rejects, the brief stays on disk and the per_term outcome
    records dispatch_success=False; success itself is still True (archive worked)."""
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    sink = NullSink(fail_next=True, fail_error="simulated transport failure")
    client = _FakeClient(_VALID_LLM_RESPONSE)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="m", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=sink,
    )
    outcome = result.per_term[0]
    assert outcome.success is True   # archive succeeded
    assert outcome.dispatch_success is False
    assert outcome.dispatch_error == "simulated transport failure"
    assert Path(outcome.archive_path).exists()


def test_run_attention_skips_dispatch_when_sink_is_none(tmp_path):
    """sink=None → archive happens but dispatch is bypassed entirely."""
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    client = _FakeClient(_VALID_LLM_RESPONSE)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="m", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=None,
    )
    outcome = result.per_term[0]
    assert outcome.success is True
    assert outcome.dispatch_success is None   # never attempted


def test_run_attention_records_token_telemetry(tmp_path):
    conn = _make_conn()
    _seed_crossing_term(conn, term="hormuz", count=12)
    client = _FakeClient(_VALID_LLM_RESPONSE)

    result = run_attention(
        conn=conn, now_unix=NOW, stopwords=frozenset(),
        anthropic_client=client, model="m", max_tokens=2048,
        archive_root=tmp_path / "archive", sink=None,
    )
    outcome = result.per_term[0]
    assert outcome.input_tokens == 100
    assert outcome.output_tokens == 200
    assert outcome.cache_creation_input_tokens == 50
    assert outcome.cache_read_input_tokens == 0


# ---------- _parse_attention_response ----------


def test_parse_strips_markdown_fence():
    text = '```json\n{"narrative": "x", "source_mix": {}, "entities_observed": [], "attention_shape": "unclear"}\n```'
    data = _parse_attention_response(text)
    assert data["narrative"] == "x"


def test_parse_rejects_missing_required_key():
    text = json.dumps({"narrative": "x", "source_mix": {}, "entities_observed": []})  # no attention_shape
    with pytest.raises(AttentionLLMError, match="attention_shape"):
        _parse_attention_response(text)


def test_parse_rejects_wrong_type_for_source_mix():
    text = json.dumps({
        "narrative": "x", "source_mix": "not a dict",
        "entities_observed": [], "attention_shape": "unclear",
    })
    with pytest.raises(AttentionLLMError, match="source_mix"):
        _parse_attention_response(text)
