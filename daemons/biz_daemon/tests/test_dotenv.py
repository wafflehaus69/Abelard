"""config.from_env loads .env: gap-fill, shell override, graceful absence."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from biz_daemon.config import Config, ConfigError

_KEYS = ("FINNHUB_API_KEY", "ANTHROPIC_API_KEY")


@pytest.fixture
def clean_keys(monkeypatch):
    """Start with the keys unset; pop any the .env load adds, after the test."""
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)
    yield
    for k in _KEYS:
        os.environ.pop(k, None)


def _write_env(tmp_path: Path, **vals: str) -> Path:
    p = tmp_path / ".env"
    p.write_text("\n".join(f"{k}={v}" for k, v in vals.items()) + "\n", encoding="utf-8")
    return p


def test_dotenv_fills_gap_when_no_shell_var(clean_keys, tmp_path):
    env = _write_env(tmp_path, FINNHUB_API_KEY="from_dotenv", ANTHROPIC_API_KEY="anth_dotenv")
    cfg = Config.from_env(dotenv_path=env)
    assert cfg.finnhub_api_key == "from_dotenv"
    assert cfg.anthropic_api_key == "anth_dotenv"


def test_shell_var_overrides_dotenv(clean_keys, tmp_path, monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "shell_wins")
    env = _write_env(tmp_path, FINNHUB_API_KEY="from_dotenv")
    cfg = Config.from_env(dotenv_path=env)
    assert cfg.finnhub_api_key == "shell_wins"  # session override wins


def test_absent_dotenv_does_not_crash_falls_through_to_config_error(clean_keys, tmp_path):
    missing = tmp_path / "nope.env"
    assert not missing.exists()
    with pytest.raises(ConfigError):  # no .env, no shell var -> loud, no crash
        Config.from_env(dotenv_path=missing)
