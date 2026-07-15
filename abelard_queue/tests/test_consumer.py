"""Consumer tests — triage rules, haiku escalation, telegram dispatch.

Hermetic: every HTTP call goes through an injected fake post_fn; env
vars via monkeypatch. The token-redaction tests are the paranoid layer
mirroring news_watch's *_readonly test discipline.
"""

from __future__ import annotations

import json

import pytest
import requests

from abelard_common.alert_queue import AlertQueue
from abelard_queue.consumer import (
    BOT_TOKEN_ENV,
    CHAT_ID_ENV,
    ANTHROPIC_KEY_ENV,
    ConsumerError,
    HAIKU_MODEL_ID,
    MAX_MESSAGE_CHARS,
    apply_rules,
    format_message,
    haiku_verdict,
    run_dispatch,
    run_triage,
    send_telegram,
)

TOKEN = "1234567890:FAKE-TOKEN-FOR-TESTS"


@pytest.fixture()
def queue(tmp_path):
    q = AlertQueue(tmp_path / "queue.db")
    yield q
    q.close()


def _enqueue(q, *, dedupe_key="brief-1", kind="attention_brief",
             topic_key="naval", payload=None):
    default_payload = {"narrative": "Something happened.",
                       "attention_shape": "narrow_source_spike"}
    item, _ = q.enqueue(source="news_watch_daemon", kind=kind,
                        topic_key=topic_key, dedupe_key=dedupe_key,
                        payload=payload if payload is not None else default_payload)
    return item


class _FakeResponse:
    def __init__(self, status_code=200, body=None, text=""):
        self.status_code = status_code
        self._body = body
        self.text = text or (json.dumps(body) if body is not None else "")

    def json(self):
        if self._body is None:
            raise ValueError("no JSON")
        return self._body


# ---------- explicit rules ----------


def test_rule_synthesis_brief_pushes(queue):
    item = _enqueue(queue, kind="synthesis_brief",
                    payload={"narrative": "full brief"})
    verdict = apply_rules(queue, item)
    assert verdict.decision == "push"
    assert verdict.rule_name == "synthesis-brief-push"


def test_rule_convergence_pushes(queue):
    item = _enqueue(queue, payload={
        "narrative": "n", "attention_shape": "multi_source_convergence"})
    verdict = apply_rules(queue, item)
    assert verdict.decision == "push"
    assert verdict.rule_name == "convergence-push"


def test_rule_narrow_spike_is_undecided(queue):
    item = _enqueue(queue)
    assert apply_rules(queue, item) is None


def test_cooldown_outranks_push_rules(queue):
    first = _enqueue(queue, dedupe_key="a", kind="synthesis_brief",
                     topic_key="us_iran_escalation",
                     payload={"narrative": "n"})
    queue.mark_interpreted(first.id, decision="push",
                           decided_by="rule:synthesis-brief-push", reason="r")
    second = _enqueue(queue, dedupe_key="b", kind="synthesis_brief",
                      topic_key="us_iran_escalation",
                      payload={"narrative": "n2"})
    verdict = apply_rules(queue, second)
    assert verdict.decision == "suppress"
    assert verdict.rule_name == "cooldown-suppress"


# ---------- triage orchestration ----------


def test_triage_rules_only_leaves_undecided_pending(queue):
    _enqueue(queue)  # narrow spike — no rule decides
    result = run_triage(queue, use_haiku=False, cooldown_s=3600)
    assert result["decided"] == []
    assert len(result["undecided"]) == 1
    assert queue.counts()["pending"] == 1  # never silently pushed/dropped


def test_triage_journals_rule_decisions(queue):
    _enqueue(queue, kind="synthesis_brief", payload={"narrative": "n"})
    result = run_triage(queue, use_haiku=False, cooldown_s=3600)
    assert result["decided"][0]["decided_by"] == "rule:synthesis-brief-push"
    entries = queue.journal()
    assert len(entries) == 1
    assert entries[0].decided_by == "rule:synthesis-brief-push"


def test_triage_haiku_escalation(queue, monkeypatch):
    monkeypatch.setenv(ANTHROPIC_KEY_ENV, "fake-anthropic-key")
    _enqueue(queue)
    fake = _FakeResponse(200, {
        "content": [{"type": "text", "text": json.dumps(
            {"decision": "push", "reason": "escalation confirmed"})}],
    })
    result = run_triage(queue, use_haiku=True, cooldown_s=3600,
                        haiku_post_fn=lambda *a, **k: fake)
    assert result["undecided"] == []
    decided = result["decided"][0]
    assert decided["decision"] == "push"
    assert decided["decided_by"] == f"haiku:{HAIKU_MODEL_ID}"
    assert queue.journal()[0].decided_by == f"haiku:{HAIKU_MODEL_ID}"


def test_triage_haiku_failure_keeps_item_pending(queue, monkeypatch):
    monkeypatch.setenv(ANTHROPIC_KEY_ENV, "fake-anthropic-key")
    item = _enqueue(queue)
    fake = _FakeResponse(200, {"content": [{"type": "text",
                                            "text": "not json at all"}]})
    result = run_triage(queue, use_haiku=True, cooldown_s=3600,
                        haiku_post_fn=lambda *a, **k: fake)
    assert result["decided"] == []
    assert result["undecided"][0]["id"] == item.id
    assert queue.get(item.id).status == "pending"


def test_haiku_verdict_rejects_bad_decision_value(queue):
    item = _enqueue(queue)
    fake = _FakeResponse(200, {"content": [{"type": "text", "text": json.dumps(
        {"decision": "shrug", "reason": "?"})}]})
    with pytest.raises(ConsumerError, match="invalid verdict"):
        haiku_verdict(item, api_key="k", post_fn=lambda *a, **k: fake)


def test_haiku_error_text_is_key_redacted(queue):
    item = _enqueue(queue)
    api_key = "sk-ant-SECRET"

    def _raise(*_a, **_k):
        raise requests.ConnectionError(f"boom https://x/{api_key}")

    with pytest.raises(ConsumerError) as exc_info:
        haiku_verdict(item, api_key=api_key, post_fn=_raise)
    assert api_key not in str(exc_info.value)
    assert "<redacted>" in str(exc_info.value)


# ---------- telegram transport ----------


def test_send_telegram_success():
    calls = {}

    def post(url, data=None, timeout=None):
        calls["url"] = url
        calls["data"] = data
        return _FakeResponse(200, {"ok": True, "result": {"message_id": 7}})

    ok, error = send_telegram("hello", bot_token=TOKEN, chat_id="42",
                              post_fn=post)
    assert ok is True and error is None
    assert calls["url"].endswith("/sendMessage")
    assert calls["data"]["chat_id"] == "42"


def test_send_telegram_non_200_fails_with_redacted_error():
    def post(url, data=None, timeout=None):
        return _FakeResponse(500, None, text=f"server error at /bot{TOKEN}/")

    ok, error = send_telegram("hello", bot_token=TOKEN, chat_id="42",
                              post_fn=post)
    assert ok is False
    assert error.startswith("http_500")
    assert TOKEN not in error
    assert "<redacted>" in error


def test_send_telegram_ok_false_fails():
    def post(url, data=None, timeout=None):
        return _FakeResponse(200, {"ok": False, "description": "chat not found"})

    ok, error = send_telegram("hello", bot_token=TOKEN, chat_id="42",
                              post_fn=post)
    assert ok is False
    assert "chat not found" in error


def test_send_telegram_exception_is_redacted():
    def post(url, data=None, timeout=None):
        raise requests.ConnectionError(
            f"Max retries exceeded with url: /bot{TOKEN}/sendMessage")

    ok, error = send_telegram("hello", bot_token=TOKEN, chat_id="42",
                              post_fn=post)
    assert ok is False
    assert TOKEN not in error
    assert "<redacted>" in error


# ---------- dispatch orchestration ----------


def _pushed(queue, dedupe_key="a", topic_key="naval"):
    item = _enqueue(queue, dedupe_key=dedupe_key, topic_key=topic_key)
    return queue.mark_interpreted(item.id, decision="push",
                                  decided_by="rule:x", reason="r")


def test_dispatch_without_credentials_claims_nothing(queue, monkeypatch):
    monkeypatch.delenv(BOT_TOKEN_ENV, raising=False)
    monkeypatch.delenv(CHAT_ID_ENV, raising=False)
    item = _pushed(queue)
    with pytest.raises(ConsumerError, match="No items were claimed"):
        run_dispatch(queue)
    assert queue.get(item.id).dispatch_attempts == 0


def test_dispatch_success_marks_dispatched(queue, monkeypatch):
    monkeypatch.setenv(BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(CHAT_ID_ENV, "42")
    item = _pushed(queue)
    ok_resp = _FakeResponse(200, {"ok": True})
    result = run_dispatch(queue, post_fn=lambda *a, **k: ok_resp)
    assert result["sent"] == [item.id]
    assert result["failed"] == [] and result["unconfirmed"] == []
    stored = queue.get(item.id)
    assert stored.status == "dispatched"
    assert stored.dispatch_channel == "telegram_bot"


def test_dispatch_is_idempotent_across_runs(queue, monkeypatch):
    monkeypatch.setenv(BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(CHAT_ID_ENV, "42")
    _pushed(queue)
    sends = {"n": 0}

    def post(*_a, **_k):
        sends["n"] += 1
        return _FakeResponse(200, {"ok": True})

    run_dispatch(queue, post_fn=post)
    second = run_dispatch(queue, post_fn=post)
    assert sends["n"] == 1  # no double-push
    assert second["sent"] == []


def test_dispatch_failure_leaves_item_undispatched_and_surfaced(
        queue, monkeypatch):
    monkeypatch.setenv(BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(CHAT_ID_ENV, "42")
    item = _pushed(queue)
    bad = _FakeResponse(502, None, text="bad gateway")
    result = run_dispatch(queue, post_fn=lambda *a, **k: bad)
    assert result["sent"] == []
    assert result["failed"][0]["id"] == item.id
    stored = queue.get(item.id)
    assert stored.status == "interpreted"          # NOT dispatched
    assert stored.last_dispatch_error.startswith("http_502")
    assert stored.claimed_at_unix is None          # known failure — retryable


def test_dispatch_skips_unconfirmed_crash_window_items(queue, monkeypatch):
    monkeypatch.setenv(BOT_TOKEN_ENV, TOKEN)
    monkeypatch.setenv(CHAT_ID_ENV, "42")
    item = _pushed(queue)
    queue.claim_for_dispatch(item.id)  # simulate crash after claim
    sends = {"n": 0}

    def post(*_a, **_k):
        sends["n"] += 1
        return _FakeResponse(200, {"ok": True})

    result = run_dispatch(queue, post_fn=post)
    assert sends["n"] == 0
    assert result["unconfirmed"][0]["id"] == item.id


# ---------- message formatting ----------


def test_format_message_contains_header_and_trailer(queue):
    item = _pushed(queue)
    text = format_message(queue.get(item.id))
    assert text.startswith("[ABELARD] attention_brief — naval")
    assert f"queue_id: {item.id}" in text
    assert "decided_by: rule:x" in text


def test_format_message_respects_cap(queue):
    item = _enqueue(queue, payload={"narrative": "x" * 10_000})
    queue.mark_interpreted(item.id, decision="push",
                           decided_by="rule:x", reason="r")
    text = format_message(queue.get(item.id))
    assert len(text) <= MAX_MESSAGE_CHARS
    assert "[truncated]" in text
