"""Config .env auto-load (Order 1 + Order 9 Phase D) — the loader picks up a real .env
WITHOUT a manual export, the shell still wins (override=False), a missing .env no-ops,
the sentiment tunable rides in, and the secret keys register for log redaction."""

from __future__ import annotations

import pytest

from chatter_daemon.config import Config

# Keys these tests touch — cleared before AND after so load_dotenv's direct os.environ
# writes never leak into another test.
_KEYS = ("FINNHUB_API_KEY", "ANTHROPIC_API_KEY", "CHATTER_SENTIMENT_MIN", "LOG_LEVEL")


@pytest.fixture
def clean_env(monkeypatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)
    yield monkeypatch
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)


def _write_env(tmp_path, body):
    p = tmp_path / ".env"
    p.write_text(body, encoding="utf-8")
    return p


def test_dotenv_autoloads_keys_without_export(clean_env, tmp_path):
    env = _write_env(tmp_path, "FINNHUB_API_KEY=fk-123\nANTHROPIC_API_KEY=ak-456\n")
    cfg = Config.from_env(dotenv_path=env)
    # loaded straight from the file — no shell export needed
    assert cfg.finnhub_api_key == "fk-123"
    assert cfg.anthropic_api_key == "ak-456"


def test_shell_wins_over_dotenv(clean_env, tmp_path):
    clean_env.setenv("FINNHUB_API_KEY", "shell-key")
    env = _write_env(tmp_path, "FINNHUB_API_KEY=file-key\n")
    cfg = Config.from_env(dotenv_path=env)
    assert cfg.finnhub_api_key == "shell-key"  # override=False: the shell value wins


def test_missing_dotenv_noops(clean_env, tmp_path):
    cfg = Config.from_env(dotenv_path=tmp_path / "nope.env")  # absent file
    assert cfg.finnhub_api_key is None and cfg.anthropic_api_key is None  # no crash, no keys


def test_dotenv_sentiment_min_override(clean_env, tmp_path):
    env = _write_env(tmp_path, "CHATTER_SENTIMENT_MIN=7\n")
    cfg = Config.from_env(dotenv_path=env)
    assert cfg.sentiment_min_mentions == 7  # the Haiku gate is tunable from .env


def test_secrets_registers_keys_for_redaction(clean_env, tmp_path):
    env = _write_env(tmp_path, "FINNHUB_API_KEY=fk-secret\nANTHROPIC_API_KEY=ak-secret\n")
    cfg = Config.from_env(dotenv_path=env)
    assert set(cfg.secrets()) == {"fk-secret", "ak-secret"}  # both scrubbed from logs
