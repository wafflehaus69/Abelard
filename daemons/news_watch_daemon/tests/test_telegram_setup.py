"""Tests for the one-time interactive setup script.

The script must:
  - Be invokable as `python -m news_watch_daemon.telegram_setup`
  - Support --help cleanly
  - On success: print the session string in a marked stdout block, exit 0
  - On failure: exit 1 with stderr message, NO partial session string
  - NEVER pass the session string to any logger

All tests are hermetic — Telethon's TelegramClient is patched out so
no real MTProto authentication is attempted.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from news_watch_daemon import telegram_setup


VALID_API_HASH = "a" * 32


def _patch_env(monkeypatch, *, api_id="12345", api_hash=VALID_API_HASH):
    if api_id is None:
        monkeypatch.delenv("TELEGRAM_API_ID", raising=False)
    else:
        monkeypatch.setenv("TELEGRAM_API_ID", api_id)
    if api_hash is None:
        monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)
    else:
        monkeypatch.setenv("TELEGRAM_API_HASH", api_hash)


def _mock_successful_telethon(monkeypatch, session_string: str = "SESSION-XYZ-SECRET"):
    """Patch TelegramClient so client.start() succeeds and returns canned session."""
    mock_client = MagicMock()
    mock_client.start = AsyncMock(return_value=mock_client)
    mock_client.disconnect = AsyncMock()
    mock_client.session = MagicMock()
    mock_client.session.save = MagicMock(return_value=session_string)
    monkeypatch.setattr(
        telegram_setup,
        "TelegramClient",
        MagicMock(return_value=mock_client),
    )
    return mock_client


# ---------- entry surface ----------


def test_help_exits_zero(capsys):
    with pytest.raises(SystemExit) as exc:
        telegram_setup.main(["--help"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    # argparse usage emitted on stdout
    assert "telegram_setup" in captured.out
    assert "TELEGRAM_SESSION_STRING" in captured.out


# ---------- success path ----------


def test_success_path_prints_session_and_exits_zero(monkeypatch, capsys):
    _patch_env(monkeypatch)
    _mock_successful_telethon(monkeypatch, session_string="SESSION-XYZ-SECRET")
    rc = telegram_setup.main([])
    assert rc == 0
    captured = capsys.readouterr()
    # Session string emitted on stdout, surrounded by the marker block.
    assert "SESSION-XYZ-SECRET" in captured.out
    assert "Save this string to TELEGRAM_SESSION_STRING" in captured.out
    assert "password manager" in captured.out
    # Nothing on stderr.
    assert captured.err == ""


def test_session_string_never_appears_in_any_log_call(monkeypatch, capsys, caplog):
    _patch_env(monkeypatch)
    session = "VERY-SECRET-SESSION-STRING"
    _mock_successful_telethon(monkeypatch, session_string=session)
    with caplog.at_level(logging.DEBUG):
        rc = telegram_setup.main([])
    assert rc == 0
    # Walk every captured log record's message — the session string must NEVER appear.
    for record in caplog.records:
        assert session not in record.getMessage(), (
            f"Session string leaked into log record at level "
            f"{record.levelname} from {record.name}: {record.getMessage()!r}"
        )


# ---------- failure paths ----------


def test_authentication_failure_exits_one_with_stderr(monkeypatch, capsys):
    _patch_env(monkeypatch)
    mock_client = MagicMock()
    mock_client.start = AsyncMock(side_effect=RuntimeError("phone code invalid"))
    mock_client.disconnect = AsyncMock()
    monkeypatch.setattr(
        telegram_setup,
        "TelegramClient",
        MagicMock(return_value=mock_client),
    )
    rc = telegram_setup.main([])
    assert rc == 1
    captured = capsys.readouterr()
    assert "Authentication failed" in captured.err
    assert "RuntimeError" in captured.err
    assert "phone code invalid" in captured.err
    # No session string was generated — stdout must not carry the marker block.
    assert "Save this string" not in captured.out


def test_invalid_api_hash_exits_one_before_authentication(monkeypatch, capsys):
    _patch_env(monkeypatch, api_hash="not-a-hex-string")
    tc_mock = MagicMock()
    monkeypatch.setattr(telegram_setup, "TelegramClient", tc_mock)
    rc = telegram_setup.main([])
    assert rc == 1
    captured = capsys.readouterr()
    assert "TELEGRAM_API_HASH" in captured.err
    # Never reached Telethon — no client should have been built.
    tc_mock.assert_not_called()


def test_invalid_api_id_exits_one(monkeypatch, capsys):
    _patch_env(monkeypatch, api_id="not-a-number")
    rc = telegram_setup.main([])
    assert rc == 1
    captured = capsys.readouterr()
    assert "TELEGRAM_API_ID" in captured.err


def test_empty_session_returned_is_treated_as_failure(monkeypatch, capsys):
    """If Telethon returns an empty session string, treat as failure not success."""
    _patch_env(monkeypatch)
    _mock_successful_telethon(monkeypatch, session_string="")
    rc = telegram_setup.main([])
    assert rc == 1
    captured = capsys.readouterr()
    assert "empty" in captured.err.lower()


# ---------- module-level invocation ----------


def test_module_is_runnable_via_python_dash_m():
    """`python -m news_watch_daemon.telegram_setup --help` should work.

    We don't actually spawn a subprocess; we verify the module has an
    importable `main` and an `if __name__ == "__main__":` block, which
    is what `-m` invocation needs.
    """
    import news_watch_daemon.telegram_setup as mod
    assert callable(mod.main)
    src = (
        __import__("pathlib").Path(mod.__file__).read_text(encoding="utf-8")
    )
    assert 'if __name__ == "__main__":' in src
