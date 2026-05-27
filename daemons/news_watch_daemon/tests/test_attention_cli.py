"""CLI tests for the `attention` subcommand + scrape-chain integration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from news_watch_daemon.cli import build_parser, main


SEED_THEME_DIR = Path(__file__).resolve().parent.parent / "themes"
SEED_STOPWORDS = Path(__file__).resolve().parent.parent / "config" / "stopwords.yaml"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(SEED_THEME_DIR))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(tmp_path / "archive"))
    monkeypatch.setenv("NEWS_WATCH_STOPWORDS_PATH", str(SEED_STOPWORDS))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    # ANTHROPIC_API_KEY deliberately UNSET — most attention tests verify the
    # skip-not-fail path. The one test that exercises the LLM monkey-patches
    # the build_anthropic_client + run_attention.
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield tmp_path


def _read_envelope(capsys) -> dict:
    captured = capsys.readouterr()
    return json.loads(captured.out)


def test_parser_registers_attention_subcommand():
    parser = build_parser()
    sub_action = next(a for a in parser._actions if a.dest == "command")
    assert "attention" in sub_action.choices


def test_attention_dry_run_without_anthropic_key(env, capsys):
    """Dry-run skips the LLM call regardless of key presence → status=ok."""
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    rc = main(["attention", "--dry-run"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    data = envelope["data"]
    assert data["dry_run"] is True
    assert "crossings_evaluated" in data
    assert "top_candidates" in data
    assert data["crossings_evaluated"] == 0   # empty DB → no crossings


def test_attention_live_without_anthropic_key_skips(env, capsys):
    """No API key, non-dry-run → status=ok with attention_outcome skipped."""
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    rc = main(["attention"])
    assert rc == 0   # build_ok envelope, not error
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "partial"
    data = envelope["data"]
    assert data["status"] == "skipped"
    assert "ANTHROPIC_API_KEY" in data["reason"]


def test_attention_without_db_schema_errors(env, capsys):
    """attention before db init → schema-not-applied error."""
    rc = main(["attention"])
    assert rc == 1
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "error"
    assert "schema applied" in envelope["error_detail"]


def test_attention_missing_stopwords_file_skips(env, capsys, monkeypatch, tmp_path):
    """Stopwords file missing → status=skipped (not error), brief envelope ok."""
    assert main(["db", "init"]) == 0
    capsys.readouterr()
    monkeypatch.setenv("NEWS_WATCH_STOPWORDS_PATH", str(tmp_path / "missing.yaml"))
    rc = main(["attention", "--dry-run"])
    # Even dry-run loads stopwords first; missing file → skip per skip-not-fail
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "partial"
    assert "stopwords_load_failed" in envelope["data"]["reason"]


def test_scrape_envelope_nests_attention_outcome(env, capsys, monkeypatch):
    """After scrape completes, attention_outcome is nested inside data block.
    Without an Anthropic key, attention is skipped — but the nesting happens."""
    from unittest.mock import MagicMock
    from news_watch_daemon.sources.base import FetchedItem, FetchResult, SourcePlugin

    def _fake_build_sources(cfg, themes, http_client):
        src = MagicMock(spec=SourcePlugin)
        src.name = "finnhub:general"
        src.fetch.return_value = FetchResult(
            source="finnhub:general", fetched_at_unix=0, items=[
                FetchedItem(
                    source_item_id="x", headline="Iran tests new missile",
                    url="https://x", published_at_unix=0, raw_source="W",
                ),
            ], status="ok",
        )
        return [src]

    monkeypatch.setattr("news_watch_daemon.cli.build_sources", _fake_build_sources)

    assert main(["db", "init"]) == 0
    capsys.readouterr()
    assert main(["themes", "load"]) == 0
    capsys.readouterr()
    rc = main(["scrape"])
    assert rc == 0
    envelope = _read_envelope(capsys)
    assert envelope["status"] == "ok"
    # The new field must appear and report skip-not-fail when key is unset.
    data = envelope["data"]
    assert "attention_outcome" in data
    assert data["attention_outcome"]["status"] == "skipped"
    # Existing scrape fields still present (no regression on shape):
    assert data["sources_attempted"] == 1
    assert data["sources_succeeded"] == 1
