"""CLI tests for the real `synthesize` handler + alert-sink test + trigger-log tail."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from news_watch_daemon import cli as cli_mod
from news_watch_daemon.cli import main
from news_watch_daemon.synthesize.brief import (
    Dispatch,
    Trigger,
    TriggerWindow,
)
from news_watch_daemon.synthesize.trigger import TriggerDecision
from news_watch_daemon.synthesize.trigger_log import write_entry as write_log_entry


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME_DIR = REPO_ROOT / "themes"
SEED_CONFIG = REPO_ROOT / "config" / "synthesis_config.yaml"


# ---------- fixtures ----------


def _seed_schema(db_path: Path) -> None:
    from news_watch_daemon.db import connect, init_db
    conn = connect(db_path)
    try:
        init_db(conn)
    finally:
        conn.close()


@pytest.fixture
def env(monkeypatch, tmp_path):
    db_path = tmp_path / "state.db"
    archive_path = tmp_path / "briefs"
    trigger_log_path = tmp_path / "trigger_log.jsonl"

    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db_path))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("NEWS_WATCH_SYNTHESIS_CONFIG", str(SEED_CONFIG))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(archive_path))
    monkeypatch.setenv("NEWS_WATCH_TRIGGER_LOG", str(trigger_log_path))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")

    _seed_schema(db_path)
    yield {
        "db_path": db_path,
        "archive_path": archive_path,
        "trigger_log_path": trigger_log_path,
        "themes_dir": SEED_THEME_DIR,
    }


def _read_envelope(capsys) -> dict:
    return json.loads(capsys.readouterr().out)


def _insert_tagged_headline(
    db_path: Path,
    *,
    headline_id: str,
    headline: str,
    publisher: str,
    published_at_unix: int,
    theme_ids: list[str],
    tickers: list[str] | None = None,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        ts = datetime.fromtimestamp(published_at_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            "INSERT INTO headlines (headline_id, source, raw_source, headline, url, "
            "published_at_unix, published_at, fetched_at_unix, fetched_at, dedupe_hash, "
            "tickers_json, entities_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                headline_id, "rss:test", publisher, headline, "https://x",
                published_at_unix, ts, published_at_unix, ts, headline_id,
                json.dumps(tickers or []), json.dumps({}),
            ),
        )
        for theme_id in theme_ids:
            conn.execute(
                "INSERT OR IGNORE INTO themes "
                "(theme_id, display_name, status, config_hash, loaded_at_unix, loaded_at) "
                "VALUES (?, ?, 'active', 'h', ?, ?)",
                (theme_id, theme_id, published_at_unix, ts),
            )
            conn.execute(
                "INSERT OR IGNORE INTO headline_theme_tags "
                "(headline_id, theme_id, confidence, tagged_at_unix) "
                "VALUES (?, ?, 'primary', ?)",
                (headline_id, theme_id, published_at_unix),
            )
        conn.commit()
    finally:
        conn.close()


# ---------- helpers for mocking the LLM ----------


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
    """Stand-in for anthropic.Anthropic; captures last call kwargs."""

    def __init__(self, response):
        self.last_call_kwargs: dict | None = None
        self.messages = SimpleNamespace(stream=self._stream)
        self._response = response

    def _stream(self, **kwargs):
        self.last_call_kwargs = kwargs
        return _FakeStreamContext(self._response)


def _llm_response(events: list[dict] | None = None, narrative: str = "n") -> SimpleNamespace:
    if events is None:
        events = [
            {
                "event_id": "evt-1",
                "headline_summary": "An event happened",
                "themes": ["us_iran_escalation"],
                "source_headlines": [
                    {
                        "publisher": "Reuters",
                        "headline": "Iran does a thing",
                        "url": "https://x",
                        "published_at": "2026-05-13T13:00:00Z",
                    }
                ],
                "materiality_score": 0.8,
                "thesis_links": [],
            }
        ]
    payload = json.dumps({"events": events, "narrative": narrative})
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=payload)],
        model="claude-sonnet-4-6-20251029",
        usage=SimpleNamespace(
            input_tokens=2000, output_tokens=400,
            cache_creation_input_tokens=1500, cache_read_input_tokens=0,
        ),
    )


def _patch_anthropic_client(monkeypatch, response: SimpleNamespace) -> _FakeClient:
    """Make `build_anthropic_client` in cli.py return a fake."""
    client = _FakeClient(response)
    monkeypatch.setattr(cli_mod, "build_anthropic_client", lambda api_key: client)
    return client


class _FakeSink:
    """Stand-in for AlertSink. Records the last dispatched brief."""

    def __init__(self, success: bool = True, channel: str = "signal", error: str | None = None):
        self.last_brief = None
        self._success = success
        self._channel = channel
        self._error = error
        self.channel_name = channel

    def dispatch(self, brief):
        self.last_brief = brief
        from news_watch_daemon.alert.sink import DispatchResult
        return DispatchResult(
            success=self._success,
            channel=self._channel,
            error=self._error,
            dispatched_at_unix=int(time.time()),
        )


def _patch_alert_sink(monkeypatch, sink: _FakeSink) -> None:
    monkeypatch.setattr(cli_mod, "build_alert_sink", lambda config: sink)


# ---------- alert-sink test ----------


def test_alert_sink_test_success(env, capsys, monkeypatch):
    sink = _FakeSink(success=True, channel="signal")
    _patch_alert_sink(monkeypatch, sink)
    rc = main(["alert-sink", "test"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["success"] is True
    assert payload["data"]["channel"] == "signal"
    assert payload["data"]["sink_type"] == "signal"
    # Test brief was constructed + dispatched.
    assert sink.last_brief is not None
    assert "self-test" in sink.last_brief.narrative


def test_alert_sink_test_custom_message(env, capsys, monkeypatch):
    sink = _FakeSink(success=True)
    _patch_alert_sink(monkeypatch, sink)
    rc = main(["alert-sink", "test", "--message", "this is my probe"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["narrative"] == "this is my probe"
    assert sink.last_brief.narrative == "this is my probe"


def test_alert_sink_test_dispatch_failure(env, capsys, monkeypatch):
    sink = _FakeSink(success=False, channel="signal", error="signal-cli not found")
    _patch_alert_sink(monkeypatch, sink)
    rc = main(["alert-sink", "test"])
    payload = _read_envelope(capsys)
    # Failed dispatch surfaces as error envelope but with payload preserved.
    assert rc == 1
    assert payload["status"] == "error"
    assert "signal-cli not found" in payload["error_detail"]
    assert payload["data"]["success"] is False


# ---------- trigger-log tail ----------


def test_trigger_log_tail_empty(env, capsys):
    rc = main(["trigger-log", "tail"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 0
    assert payload["data"]["entries"] == []


def test_trigger_log_tail_returns_entries(env, capsys):
    log_path = env["trigger_log_path"]
    write_log_entry(log_path, TriggerDecision(
        fire=True, reason="cross_theme:a+b",
        matched_headline_ids=("h1",),
        themes_in_scope=("a", "b"),
        window_since_unix=1000, window_until_unix=2000,
    ))
    write_log_entry(log_path, TriggerDecision(
        fire=False, reason="below_thresholds",
        window_since_unix=2000, window_until_unix=3000,
    ))
    rc = main(["trigger-log", "tail"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["count"] == 2
    # Oldest-first within the tail.
    assert payload["data"]["entries"][0]["decision"] == "fire"
    assert payload["data"]["entries"][1]["decision"] == "suppress"


def test_trigger_log_tail_limit_respected(env, capsys):
    log_path = env["trigger_log_path"]
    for i in range(5):
        write_log_entry(log_path, TriggerDecision(
            fire=False, reason=f"r{i}",
            window_since_unix=i * 1000, window_until_unix=(i + 1) * 1000,
        ))
    rc = main(["trigger-log", "tail", "--limit", "2"])
    payload = _read_envelope(capsys)
    # tail returns last-N — should be the most recent 2.
    assert payload["data"]["count"] == 2
    reasons = [e["reason"] for e in payload["data"]["entries"]]
    assert reasons == ["r3", "r4"]


# ---------- synthesize: dry-run ----------


def test_synthesize_dry_run_no_headlines_no_trigger(env, capsys):
    """Dry-run with empty DB: gate suppresses, no LLM call needed."""
    rc = main(["synthesize", "--dry-run"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["synthesis_run"] is False
    # Gate suppresses on no_new_headlines.
    assert payload["data"]["trigger_decision"]["fire"] is False


def test_synthesize_dry_run_with_cross_theme_fires_but_no_llm(env, capsys):
    """Dry-run with a cross-theme headline: gate fires, dry-run skips LLM."""
    db_path = env["db_path"]
    now = int(time.time())
    _insert_tagged_headline(
        db_path, headline_id="h-x",
        headline="Iran and Fed move together",
        publisher="Reuters", published_at_unix=now - 100,
        theme_ids=["us_iran_escalation", "fed_policy_path"],
    )
    rc = main(["synthesize", "--dry-run"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["synthesis_run"] is False
    assert "cross_theme" in payload["data"]["trigger_reason"]
    assert payload["data"]["cluster_count"] >= 1


def test_synthesize_dry_run_does_not_require_api_key(env, capsys):
    """--dry-run is the escape hatch for sanity-checking without spending tokens."""
    # No ANTHROPIC_API_KEY in env; --dry-run should still succeed.
    rc = main(["synthesize", "--dry-run"])
    payload = _read_envelope(capsys)
    assert rc == 0


# ---------- synthesize: missing API key ----------


def test_synthesize_no_api_key_errors(env, capsys, monkeypatch):
    """Without --dry-run, ANTHROPIC_API_KEY is required."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    rc = main(["synthesize"])
    payload = _read_envelope(capsys)
    assert rc == 1
    assert "ANTHROPIC_API_KEY" in payload["error_detail"]


# ---------- synthesize: event-mode happy path ----------


def test_synthesize_event_mode_archives_and_dispatches(env, capsys, monkeypatch):
    """Cross-theme headline → gate fires → LLM call → above materiality →
    dispatch → archive write."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake-for-test")
    db_path = env["db_path"]
    now = int(time.time())
    _insert_tagged_headline(
        db_path, headline_id="h-x",
        headline="Iran ceasefire and Fed emergency cut",
        publisher="Reuters", published_at_unix=now - 100,
        theme_ids=["us_iran_escalation", "fed_policy_path"],
    )
    _patch_anthropic_client(monkeypatch, _llm_response(events=[
        {
            "event_id": "evt-1",
            "headline_summary": "Iran ceasefire announced",
            "themes": ["us_iran_escalation"],
            "source_headlines": [
                {"publisher": "Reuters", "headline": "Iran ceasefire and Fed emergency cut",
                 "url": "https://x", "published_at": "2026-05-13T13:00:00Z"}
            ],
            "materiality_score": 0.95,
            "thesis_links": [],
        }
    ]))
    sink = _FakeSink(success=True, channel="signal")
    _patch_alert_sink(monkeypatch, sink)

    rc = main(["synthesize"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["synthesis_run"] is True
    assert payload["data"]["materiality_decision"]["dispatch"] is True
    assert payload["data"]["dispatch_result"]["success"] is True
    assert payload["data"]["dispatch_result"]["channel"] == "signal"
    # Archive write happened.
    assert Path(payload["data"]["archive_path"]).is_file()
    # Telemetry surfaced.
    assert payload["data"]["telemetry"]["cache_creation_input_tokens"] == 1500
    # Sink received the brief.
    assert sink.last_brief is not None


def test_synthesize_event_mode_suppresses_below_threshold(env, capsys, monkeypatch):
    """LLM returns sub-threshold events → materiality gate suppresses →
    brief still archived but dispatch.alerted=False."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    db_path = env["db_path"]
    now = int(time.time())
    _insert_tagged_headline(
        db_path, headline_id="h-cross",
        headline="Iran and Fed both mentioned",
        publisher="Reuters", published_at_unix=now - 100,
        theme_ids=["us_iran_escalation", "fed_policy_path"],
    )
    _patch_anthropic_client(monkeypatch, _llm_response(events=[
        {
            "event_id": "evt-1",
            "headline_summary": "Low-importance event",
            "themes": ["us_iran_escalation"],
            "source_headlines": [],
            "materiality_score": 0.3,  # below 0.55 default
            "thesis_links": [],
        }
    ]))
    sink = _FakeSink()
    _patch_alert_sink(monkeypatch, sink)

    rc = main(["synthesize"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["materiality_decision"]["dispatch"] is False
    assert payload["data"]["materiality_decision"]["reason"] == "below_materiality_threshold"
    # No dispatch attempted.
    assert payload["data"]["dispatch_result"] is None
    assert sink.last_brief is None
    # Brief still archived.
    assert Path(payload["data"]["archive_path"]).is_file()


def test_synthesize_no_trigger_returns_early(env, capsys, monkeypatch):
    """Empty DB → gate suppresses → no LLM call → early return."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    # No headlines inserted.
    rc = main(["synthesize"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["synthesis_run"] is False
    assert payload["data"]["trigger_decision"]["fire"] is False


# ---------- synthesize: pull mode ----------


def test_synthesize_pull_mode_bypasses_gate(env, capsys, monkeypatch):
    """--theme T runs synthesis even when the gate would suppress."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    db_path = env["db_path"]
    now = int(time.time())
    # One tagged headline — not enough for the gate to fire on
    # delta_threshold (3 by default).
    _insert_tagged_headline(
        db_path, headline_id="h-1",
        headline="Iran does a thing",
        publisher="Reuters", published_at_unix=now - 100,
        theme_ids=["us_iran_escalation"],
    )
    _patch_anthropic_client(monkeypatch, _llm_response())
    _patch_alert_sink(monkeypatch, _FakeSink())

    rc = main(["synthesize", "--theme", "us_iran_escalation"])
    payload = _read_envelope(capsys)
    assert rc == 0
    assert payload["data"]["synthesis_run"] is True
    assert payload["data"]["trigger"]["type"] == "pull"


def test_synthesize_pull_mode_rejects_unknown_theme(env, capsys, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    rc = main(["synthesize", "--theme", "no_such_theme"])
    payload = _read_envelope(capsys)
    assert rc == 1
    assert "not in the active themes" in payload["error_detail"]


# ---------- synthesize: trigger_log integration ----------


def test_synthesize_event_mode_appends_to_trigger_log(env, capsys, monkeypatch):
    """Every event-mode synthesis run appends one trigger_log entry."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    db_path = env["db_path"]
    log_path = env["trigger_log_path"]
    now = int(time.time())
    _insert_tagged_headline(
        db_path, headline_id="h-x",
        headline="Iran does a thing",
        publisher="Reuters", published_at_unix=now - 100,
        theme_ids=["us_iran_escalation"],
    )
    _patch_anthropic_client(monkeypatch, _llm_response())
    _patch_alert_sink(monkeypatch, _FakeSink())

    # Initially empty.
    assert not log_path.exists()

    rc = main(["synthesize"])
    payload = _read_envelope(capsys)
    assert rc == 0

    # Log should have one entry (the gate decision).
    assert log_path.is_file()
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["decision"] in ("fire", "suppress")


def test_synthesize_pull_mode_does_not_log_trigger(env, capsys, monkeypatch):
    """Pull-mode bypasses the gate, so no trigger_log entry."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    log_path = env["trigger_log_path"]
    _patch_anthropic_client(monkeypatch, _llm_response())
    _patch_alert_sink(monkeypatch, _FakeSink())

    rc = main(["synthesize", "--theme", "us_iran_escalation"])
    _read_envelope(capsys)
    assert rc == 0
    # No log file created for pull-mode.
    assert not log_path.exists()
