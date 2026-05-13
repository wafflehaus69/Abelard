"""Config loader tests — env var handling, validation, and log redaction scaffold."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

from news_watch_daemon.config import (
    Config,
    ConfigError,
    DEFAULT_HTTP_TIMEOUT_S,
    DEFAULT_USER_AGENT,
    REDACTED,
    _RedactingFilter,
    configure_logging,
)


# ---------- from_env ----------


def test_from_env_loads_required_fields(monkeypatch, tmp_path):
    db_path = tmp_path / "state.db"
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db_path))
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("NEWS_WATCH_THEMES_DIR", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.delenv("NEWS_WATCH_USER_AGENT", raising=False)
    monkeypatch.delenv("NEWS_WATCH_HTTP_TIMEOUT_S", raising=False)
    cfg = Config.from_env()
    assert cfg.db_path == db_path
    assert cfg.log_level == "INFO"
    # Default themes_dir resolves to the package-adjacent themes/ folder.
    assert cfg.themes_dir.name == "themes"
    assert cfg.finnhub_api_key is None
    assert cfg.http_user_agent == DEFAULT_USER_AGENT
    assert cfg.http_default_timeout_s == DEFAULT_HTTP_TIMEOUT_S


def test_from_env_themes_dir_override(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(tmp_path / "custom_themes"))
    cfg = Config.from_env()
    assert cfg.themes_dir == tmp_path / "custom_themes"


def test_from_env_relative_themes_dir_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", "relative/themes")
    with pytest.raises(ConfigError, match="NEWS_WATCH_THEMES_DIR"):
        Config.from_env()


def test_from_env_log_level_override(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("LOG_LEVEL", "debug")
    cfg = Config.from_env()
    assert cfg.log_level == "DEBUG"


def test_from_env_missing_db_path_raises(monkeypatch):
    monkeypatch.delenv("NEWS_WATCH_DB_PATH", raising=False)
    with pytest.raises(ConfigError, match="NEWS_WATCH_DB_PATH"):
        Config.from_env()


def test_from_env_empty_db_path_raises(monkeypatch):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", "   ")
    with pytest.raises(ConfigError, match="NEWS_WATCH_DB_PATH"):
        Config.from_env()


def test_from_env_relative_db_path_raises(monkeypatch):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", "relative/path.db")
    with pytest.raises(ConfigError, match="absolute"):
        Config.from_env()


def test_from_env_invalid_log_level_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("LOG_LEVEL", "FANCY")
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        Config.from_env()


def test_secrets_empty_when_no_finnhub_key(tmp_path):
    cfg = Config(db_path=tmp_path / "state.db", log_level="INFO")
    assert cfg.secrets() == ()


def test_secrets_includes_finnhub_key_when_set(tmp_path):
    cfg = Config(
        db_path=tmp_path / "state.db",
        log_level="INFO",
        finnhub_api_key="abc123",
    )
    assert cfg.secrets() == ("abc123",)


def test_from_env_loads_finnhub_key(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("FINNHUB_API_KEY", "  my-key-xyz  ")  # whitespace stripped
    cfg = Config.from_env()
    assert cfg.finnhub_api_key == "my-key-xyz"


def test_from_env_empty_finnhub_key_is_none(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("FINNHUB_API_KEY", "   ")
    cfg = Config.from_env()
    assert cfg.finnhub_api_key is None


def test_from_env_user_agent_override(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_USER_AGENT", "custom-agent/9.9")
    cfg = Config.from_env()
    assert cfg.http_user_agent == "custom-agent/9.9"


def test_from_env_timeout_override(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_HTTP_TIMEOUT_S", "25.5")
    cfg = Config.from_env()
    assert cfg.http_default_timeout_s == 25.5


def test_from_env_invalid_timeout_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_HTTP_TIMEOUT_S", "not-a-number")
    with pytest.raises(ConfigError, match="NEWS_WATCH_HTTP_TIMEOUT_S"):
        Config.from_env()


def test_from_env_negative_timeout_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_HTTP_TIMEOUT_S", "-1.0")
    with pytest.raises(ConfigError, match="positive"):
        Config.from_env()


def test_from_env_zero_timeout_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_HTTP_TIMEOUT_S", "0")
    with pytest.raises(ConfigError, match="positive"):
        Config.from_env()


def test_default_user_agent_format():
    """The default User-Agent must include the package version, no URL."""
    assert DEFAULT_USER_AGENT.startswith("news-watch-daemon/")
    assert "://" not in DEFAULT_USER_AGENT  # no URL portion


# ---------- tracked_tickers_path (Pass C Step 0) ----------


def test_tracked_tickers_path_defaults_to_bundled_config(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.delenv("NEWS_WATCH_TRACKED_TICKERS_PATH", raising=False)
    cfg = Config.from_env()
    assert cfg.tracked_tickers_path.name == "tracked_tickers.yaml"
    assert cfg.tracked_tickers_path.parent.name == "config"


def test_tracked_tickers_path_env_override(monkeypatch, tmp_path):
    custom = tmp_path / "custom" / "tickers.yaml"
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_TRACKED_TICKERS_PATH", str(custom))
    cfg = Config.from_env()
    assert cfg.tracked_tickers_path == custom


def test_tracked_tickers_path_relative_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_TRACKED_TICKERS_PATH", "relative/path.yaml")
    with pytest.raises(ConfigError, match="NEWS_WATCH_TRACKED_TICKERS_PATH"):
        Config.from_env()


# ---------- brief_archive_path (Pass C Step 2) ----------


def test_brief_archive_path_defaults_to_openclaw_workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.delenv("NEWS_WATCH_BRIEF_ARCHIVE", raising=False)
    cfg = Config.from_env()
    # Default resolves to ~/.openclaw/news_watch/briefs
    assert cfg.brief_archive_path.name == "briefs"
    assert cfg.brief_archive_path.parent.name == "news_watch"


def test_brief_archive_path_env_override(monkeypatch, tmp_path):
    custom = tmp_path / "elsewhere" / "briefs"
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(custom))
    cfg = Config.from_env()
    assert cfg.brief_archive_path == custom


def test_brief_archive_path_relative_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", "relative/briefs")
    with pytest.raises(ConfigError, match="NEWS_WATCH_BRIEF_ARCHIVE"):
        Config.from_env()


# ---------- Telegram credentials (Pass B) ----------


def _valid_telegram_env(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_ID", "12345")
    monkeypatch.setenv("TELEGRAM_API_HASH", "a" * 32)
    monkeypatch.setenv("TELEGRAM_SESSION_STRING", "session-blob-xyz")


def test_telegram_creds_all_set_loads(monkeypatch, tmp_path):
    _valid_telegram_env(monkeypatch, tmp_path)
    cfg = Config.from_env()
    assert cfg.telegram_api_id == 12345
    assert cfg.telegram_api_hash == "a" * 32
    assert cfg.telegram_session_string == "session-blob-xyz"
    assert cfg.telegram_creds_complete is True


def test_telegram_creds_default_to_none(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.delenv("TELEGRAM_API_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)
    monkeypatch.delenv("TELEGRAM_SESSION_STRING", raising=False)
    cfg = Config.from_env()
    assert cfg.telegram_api_id is None
    assert cfg.telegram_api_hash is None
    assert cfg.telegram_session_string is None
    assert cfg.telegram_creds_complete is False


def test_telegram_creds_partial_means_incomplete(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_ID", "12345")
    monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)
    monkeypatch.delenv("TELEGRAM_SESSION_STRING", raising=False)
    cfg = Config.from_env()
    assert cfg.telegram_creds_complete is False


def test_telegram_api_id_invalid_int_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_ID", "not-a-number")
    with pytest.raises(ConfigError, match="TELEGRAM_API_ID"):
        Config.from_env()


def test_telegram_api_id_negative_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_ID", "-1")
    with pytest.raises(ConfigError, match="positive"):
        Config.from_env()


def test_telegram_api_hash_wrong_length_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_HASH", "tooshort")
    with pytest.raises(ConfigError, match="32 lowercase hex"):
        Config.from_env()


def test_telegram_api_hash_uppercase_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_HASH", "A" * 32)
    with pytest.raises(ConfigError, match="lowercase"):
        Config.from_env()


def test_telegram_api_hash_non_hex_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("TELEGRAM_API_HASH", "g" * 32)
    with pytest.raises(ConfigError, match="hex"):
        Config.from_env()


def test_secrets_includes_telegram_hash_and_session(tmp_path):
    cfg = Config(
        db_path=tmp_path / "state.db",
        log_level="INFO",
        telegram_api_id=1,
        telegram_api_hash="b" * 32,
        telegram_session_string="abc",
    )
    secrets = cfg.secrets()
    assert "b" * 32 in secrets
    assert "abc" in secrets
    # api_id is NOT a secret
    assert "1" not in secrets


def test_secrets_excludes_telegram_api_id():
    """api_id is a numeric identifier visible on my.telegram.org — not a secret."""
    cfg = Config(
        db_path=Path("/tmp/x"),
        log_level="INFO",
        telegram_api_id=999_999,
        telegram_api_hash=None,
        telegram_session_string=None,
    )
    assert cfg.secrets() == ()


# ---------- logging ----------


def test_configure_logging_writes_to_stderr(tmp_path, capsys):
    cfg = Config(db_path=tmp_path / "state.db", log_level="INFO")
    logger = configure_logging(cfg)
    logger.info("hello stderr")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "hello stderr" in captured.err


def test_configure_logging_is_idempotent(tmp_path):
    cfg = Config(db_path=tmp_path / "state.db", log_level="INFO")
    logger = configure_logging(cfg)
    handler_count = len(logger.handlers)
    configure_logging(cfg)
    assert len(logger.handlers) == handler_count


def test_redacting_filter_replaces_known_secret():
    f = _RedactingFilter(("supersecret",))
    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname="", lineno=0,
        msg="hit url with token=supersecret", args=(), exc_info=None,
    )
    f.filter(record)
    assert "supersecret" not in record.getMessage()
    assert REDACTED in record.getMessage()


def test_redacting_filter_passes_through_when_no_secrets():
    f = _RedactingFilter(())
    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname="", lineno=0,
        msg="no secrets here", args=(), exc_info=None,
    )
    assert f.filter(record) is True
    assert record.getMessage() == "no secrets here"
