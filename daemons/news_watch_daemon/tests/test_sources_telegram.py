"""TelegramSource tests — Telethon mocked at the TelegramClient class.

Every test is hermetic. The plugin's only collaborator is the
`TelegramClient` class imported at module level; tests patch it via
`patch("news_watch_daemon.sources.telegram.TelegramClient")` and supply
an instance whose async methods return canned values. No real MTProto.
No real event loop work beyond what `asyncio.run()` does internally.

The async-iterator pattern below mocks `iter_messages` as a helper
class with `__aiter__` / `__anext__` because `AsyncMock` doesn't
quite cover the generator case Telethon uses.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Iterable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from telethon import errors

from news_watch_daemon.sources.telegram import (
    PLUGIN_PREFIX,
    TelegramSource,
    _extract_headline,
)


VALID_API_HASH = "a" * 32


def _synthetic_session_string() -> str:
    """Generate a valid-format StringSession for tests.

    The string is a real Telethon StringSession blob (so the eager
    decode in `TelegramSource.__init__` passes), but the auth_key is
    zeroed and the DC pointer is the documented public-test DC
    address. It is not a real credential.
    """
    from telethon.sessions import StringSession
    s = StringSession()
    s.set_dc(4, "149.154.167.91", 443)
    s.auth_key = type("_FakeKey", (), {"key": b"\x00" * 256})()
    return s.save()


VALID_SESSION = _synthetic_session_string()


# ---------- helpers ----------


def _make_source(
    *,
    channel_username: str = "test_channel",
    cadence_minutes: int = 15,
    max_messages_per_fetch: int = 200,
) -> TelegramSource:
    """Construct a TelegramSource for tests. StringSession decode is bypassed
    by patching it during construction below — but for tests that need a
    real instance, we use a syntactically plausible session string."""
    return TelegramSource(
        channel_username=channel_username,
        api_id=12345,
        api_hash=VALID_API_HASH,
        session_string=VALID_SESSION,
        cadence_minutes=cadence_minutes,
        max_messages_per_fetch=max_messages_per_fetch,
    )


def _fake_message(msg_id: int, when: datetime, text: str | None) -> SimpleNamespace:
    return SimpleNamespace(id=msg_id, date=when, text=text)


class _AsyncIter:
    """Tiny async-iterator wrapper around an in-memory list of items."""

    def __init__(self, items: Iterable):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


def _make_mock_client(
    *,
    is_authorized: bool = True,
    entity: object = None,
    messages: Iterable | None = None,
    connect_exc: BaseException | None = None,
    is_authorized_exc: BaseException | None = None,
    get_entity_exc: BaseException | None = None,
    iter_messages_exc: BaseException | None = None,
) -> MagicMock:
    """Build a mock that stands in for an instantiated `TelegramClient`.

    The plugin calls: connect, is_user_authorized, get_entity,
    iter_messages, disconnect. All async except iter_messages (which
    returns an async iterable).
    """
    client = MagicMock()
    client.connect = AsyncMock(side_effect=connect_exc) if connect_exc else AsyncMock()
    client.is_user_authorized = (
        AsyncMock(side_effect=is_authorized_exc)
        if is_authorized_exc
        else AsyncMock(return_value=is_authorized)
    )
    client.get_entity = (
        AsyncMock(side_effect=get_entity_exc)
        if get_entity_exc
        else AsyncMock(return_value=entity if entity is not None else object())
    )
    if iter_messages_exc is not None:
        client.iter_messages = MagicMock(side_effect=iter_messages_exc)
    else:
        client.iter_messages = MagicMock(return_value=_AsyncIter(messages or []))
    client.disconnect = AsyncMock()
    # Methods the plugin must NOT call — tests verify these stay untouched.
    client.start = AsyncMock()
    return client


# ---------- constructor validation ----------


@pytest.mark.parametrize("bad_username", [
    "ab", "ab-cd", "1abcd", "with space", "a" * 33, "",
])
def test_invalid_channel_username_rejected(bad_username):
    with pytest.raises(ValueError, match="channel_username"):
        _make_source(channel_username=bad_username)


def test_zero_api_id_rejected():
    with pytest.raises(ValueError, match="api_id"):
        TelegramSource(
            channel_username="valid_name",
            api_id=0,
            api_hash=VALID_API_HASH,
            session_string=VALID_SESSION,
        )


def test_negative_api_id_rejected():
    with pytest.raises(ValueError, match="api_id"):
        TelegramSource(
            channel_username="valid_name",
            api_id=-1,
            api_hash=VALID_API_HASH,
            session_string=VALID_SESSION,
        )


def test_bool_api_id_rejected():
    """`bool` is a subclass of `int`; constructor must reject."""
    with pytest.raises(ValueError, match="api_id"):
        TelegramSource(
            channel_username="valid_name",
            api_id=True,  # type: ignore[arg-type]
            api_hash=VALID_API_HASH,
            session_string=VALID_SESSION,
        )


@pytest.mark.parametrize("bad_hash", ["tooshort", "A" * 32, "g" * 32, "", "a" * 31])
def test_invalid_api_hash_rejected(bad_hash):
    with pytest.raises(ValueError, match="api_hash"):
        TelegramSource(
            channel_username="valid_name",
            api_id=1,
            api_hash=bad_hash,
            session_string=VALID_SESSION,
        )


def test_empty_session_string_rejected():
    with pytest.raises(ValueError, match="session_string"):
        TelegramSource(
            channel_username="valid_name",
            api_id=1,
            api_hash=VALID_API_HASH,
            session_string="   ",
        )


def test_malformed_session_string_rejected_at_init():
    """Hypothesis #2: corrupt session blob fails fast at construction,
    not 15 minutes later mid-sweep."""
    with pytest.raises(ValueError, match="session_string is malformed"):
        TelegramSource(
            channel_username="valid_name",
            api_id=1,
            api_hash=VALID_API_HASH,
            session_string="garbage-not-base64-???",
        )


def test_negative_cadence_rejected():
    with pytest.raises(ValueError, match="cadence_minutes"):
        _make_source(cadence_minutes=-1)


def test_max_messages_per_fetch_over_cap_rejected():
    with pytest.raises(ValueError, match="max_messages_per_fetch"):
        _make_source(max_messages_per_fetch=2000)


# ---------- plugin identity ----------


def test_plugin_name_format():
    src = _make_source(channel_username="bloomberg")
    assert src.name == "telegram:bloomberg"
    assert src.name.startswith(PLUGIN_PREFIX)


def test_cadence_minutes_exposed_via_property():
    src = _make_source(cadence_minutes=45)
    assert src.cadence_minutes == 45


def test_rate_limit_budget_is_optimistic():
    assert _make_source().rate_limit_budget_remaining() == 1.0


# ---------- happy path ----------


def test_happy_path_returns_messages_in_window():
    src = _make_source(channel_username="test_channel")
    base = datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)
    since = int(base.timestamp()) + 60  # 1 minute after base
    messages = [
        # newest-first ordering; first two are within window, third older.
        _fake_message(103, base.replace(hour=14), "Iran tests new missile"),
        _fake_message(102, base.replace(hour=13), "Another headline"),
        _fake_message(101, base, "Too old"),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=since)
    assert result.status == "ok"
    assert result.source == "telegram:test_channel"
    assert len(result.items) == 2
    first = result.items[0]
    assert first.source_item_id == "103"
    assert first.headline == "Iran tests new missile"
    assert first.url == "https://t.me/test_channel/103"
    assert first.raw_source is None
    assert first.tickers == []
    assert first.raw_body is None


def test_happy_path_calls_connect_and_disconnect():
    src = _make_source()
    mock_client = _make_mock_client(messages=[])
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        src.fetch(since_unix=0)
    mock_client.connect.assert_called_once()
    mock_client.disconnect.assert_called_once()
    # Never call start() — that's reserved for telegram_setup.py only.
    mock_client.start.assert_not_called()


def test_empty_channel_returns_ok_no_items():
    src = _make_source()
    mock_client = _make_mock_client(messages=[])
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.items == []


# ---------- text extraction ----------


def test_extract_headline_single_line():
    assert _extract_headline("Hello world") == "Hello world"


def test_extract_headline_multiline_uses_first_line():
    text = "Lead line\n\nbody paragraph here"
    assert _extract_headline(text) == "Lead line"


def test_extract_headline_truncates_to_280():
    text = "a" * 400
    assert len(_extract_headline(text)) == 280


def test_extract_headline_strips_whitespace():
    assert _extract_headline("   spaced out   ") == "spaced out"


def test_extract_headline_empty_returns_none():
    assert _extract_headline(None) is None
    assert _extract_headline("") is None
    assert _extract_headline("   ") is None


def test_message_with_no_text_dropped():
    """Photo-only / sticker-only messages have text=None → skipped."""
    src = _make_source()
    base = datetime(2026, 5, 12, tzinfo=timezone.utc)
    messages = [
        _fake_message(1, base.replace(hour=14), None),  # no text
        _fake_message(2, base.replace(hour=13), "Real headline"),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 1
    assert result.items[0].source_item_id == "2"


def test_long_single_line_truncated_to_280():
    src = _make_source()
    long_text = "x" * 400
    base = datetime(2026, 5, 12, tzinfo=timezone.utc)
    mock_client = _make_mock_client(messages=[_fake_message(1, base, long_text)])
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert len(result.items[0].headline) == 280


# ---------- since_unix early termination ----------


def test_iter_messages_breaks_at_since_unix_boundary():
    src = _make_source()
    base = datetime(2026, 5, 12, tzinfo=timezone.utc)
    since = int(base.replace(hour=12).timestamp())
    messages = [
        _fake_message(3, base.replace(hour=14), "newer"),
        _fake_message(2, base.replace(hour=12), "boundary equal — should be excluded"),
        _fake_message(1, base.replace(hour=10), "older — never reached"),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=since)
    # Only the one strictly newer than `since`
    assert [i.source_item_id for i in result.items] == ["3"]


# ---------- auth failures ----------


def test_auth_key_unregistered_returns_error_with_critical_log(caplog):
    src = _make_source()
    mock_client = _make_mock_client(
        is_authorized_exc=errors.AuthKeyUnregisteredError(
            request=None  # type: ignore[arg-type]
        ),
    )
    with caplog.at_level(logging.CRITICAL, logger="news_watch_daemon.sources.telegram"):
        with patch(
            "news_watch_daemon.sources.telegram.TelegramClient",
            return_value=mock_client,
        ):
            result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "session invalidated" in result.error_detail
    # Critical-level log should mention regeneration.
    critical_messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert any("regenerate" in m.lower() for m in critical_messages)
    # Never call start — even on auth failure.
    mock_client.start.assert_not_called()


def test_session_password_needed_returns_error():
    src = _make_source()
    mock_client = _make_mock_client(
        is_authorized_exc=errors.SessionPasswordNeededError(
            request=None  # type: ignore[arg-type]
        ),
    )
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "2FA" in result.error_detail


def test_session_revoked_returns_error():
    src = _make_source()
    mock_client = _make_mock_client(
        is_authorized_exc=errors.SessionRevokedError(
            request=None  # type: ignore[arg-type]
        ),
    )
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "regenerate" in result.error_detail.lower()


def test_is_user_authorized_false_returns_error_no_start():
    """If is_user_authorized() returns False, plugin must NOT call client.start()."""
    src = _make_source()
    mock_client = _make_mock_client(is_authorized=False)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "not authorized" in result.error_detail
    mock_client.start.assert_not_called()


# ---------- channel errors ----------


def test_channel_private_returns_error():
    src = _make_source()
    mock_client = _make_mock_client(
        get_entity_exc=errors.ChannelPrivateError(request=None),  # type: ignore[arg-type]
    )
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "private" in result.error_detail


def test_username_not_occupied_returns_error():
    src = _make_source()
    mock_client = _make_mock_client(
        get_entity_exc=errors.UsernameNotOccupiedError(request=None),  # type: ignore[arg-type]
    )
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "not found" in result.error_detail


# ---------- rate limiting ----------


def test_flood_wait_returns_rate_limited():
    src = _make_source()
    flood = errors.FloodWaitError(request=None)  # type: ignore[arg-type]
    flood.seconds = 42
    mock_client = _make_mock_client(get_entity_exc=flood)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "rate_limited"
    assert "flood_wait_seconds=42" in result.error_detail


# ---------- network failures ----------


def test_connection_error_during_connect_returns_error():
    src = _make_source()
    mock_client = _make_mock_client(connect_exc=ConnectionError("DNS resolution failed"))
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "connection_error" in result.error_detail
    assert "ConnectionError" in result.error_detail


# ---------- defense in depth ----------


def test_fetch_never_raises_on_unexpected_exception():
    src = _make_source()
    # Construct the client itself raises (simulates broken Telethon).
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        side_effect=RuntimeError("unexpected boom"),
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "unexpected" in result.error_detail
    assert "RuntimeError" in result.error_detail


def test_message_with_bad_date_dropped():
    """Messages whose `date` doesn't return a timestamp are dropped."""
    src = _make_source()
    bad_msg = SimpleNamespace(id=1, date=None, text="orphan")
    good_msg = _fake_message(2, datetime(2026, 5, 12, tzinfo=timezone.utc), "real")
    mock_client = _make_mock_client(messages=[bad_msg, good_msg])
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert [i.source_item_id for i in result.items] == ["2"]
