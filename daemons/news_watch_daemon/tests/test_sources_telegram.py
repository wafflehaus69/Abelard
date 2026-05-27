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

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
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
    noise_filter: list[str] | None = None,
    filtered_log_path: Path | None = None,
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
        noise_filter=noise_filter,
        filtered_log_path=filtered_log_path,
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


def test_extract_headline_strips_whitespace():
    assert _extract_headline("   spaced out   ") == "spaced out"


def test_extract_headline_empty_returns_none():
    assert _extract_headline(None) is None
    assert _extract_headline("") is None
    assert _extract_headline("   ") is None


# ---------- Headline-extraction behavior: strip + cap-at-4096, full text preserved ----------
#
# History:
#   Fix 1 (2026-05-25): raised _HEADLINE_MAX_CHARS from 280 -> 4096 to stop
#     silent truncation of long-form posts (notably @real_DonaldJTrump).
#     Kept the first-line restriction unchanged and added observability
#     to measure how often it dropped tail content.
#   This pass (2026-05-26): dropped the first-line restriction after empirical
#     evidence (23 hits in one CIG_telegram scrape, /76087 dropping 4143 of
#     4227 chars of analytical body) showed channels like CIG and Trump's
#     are performing upstream synthesis work the daemon should preserve.
# Function semantics now: strip surrounding whitespace, cap at 4096.


def test_short_text_unchanged():
    """100-char text round-trips intact."""
    text = "x" * 100
    assert _extract_headline(text) == text


def test_text_under_4096_unchanged():
    """~999-char text with no newlines round-trips intact (Fix-1 era regression
    that single-line long posts aren't silently truncated)."""
    text = ("word " * 200).strip()
    assert len(text) > 280
    assert len(text) < 4096
    assert _extract_headline(text) == text


def test_text_over_4096_truncated_at_4096():
    """Sanity cap: input larger than 4096 chars is truncated cleanly at 4096."""
    text = "x" * 5000
    result = _extract_headline(text)
    assert len(result) == 4096
    assert result == "x" * 4096


def test_extract_headline_no_longer_splits_at_newline():
    """Bug regression for the Telegram content-preservation fix: multi-paragraph
    text round-trips intact. The earlier first-line restriction (split('\\n', 1)[0])
    has been removed. Body paragraphs that previously got silently dropped now
    flow through to the headline column for synthesis consumption."""
    text = "Lead line\n\nbody paragraph here\n\nthird paragraph also preserved"
    assert _extract_headline(text) == text


# Real text of CIG_telegram /76087, captured 2026-05-26 ~04:37 UTC via ad-hoc
# Telethon fetch. The original observability log (now removed) recorded:
#   text_len=4227, headline_len=84, newline_idx=85
# i.e. 4143 chars of analytical body were being discarded to preserve an
# 84-char emoji-prefix title. This fixture is the most-severe single example
# from the empirical evidence that motivated dropping the first-line behavior.
CIG_76087_FULL_TEXT = '📝 🇺🇸 🇵🇭 **The U.S. tried to re-colonize part of the Philippines.** | Arnaud Bertrand \n\nThey did so under the so-called "Pax Silica" initiative, the brainchild of - surprise, surprise - an ex-Palantir guy named Jacob Helberg who now runs U.S. economic "diplomacy" from the State Department.\n\nIt\'s causing a big outcry in the Philippines, which is quite a feat given this is by far the most US-friendly country in Southeast Asia. \n\nIf you\'re the US and you\'re getting the Marcos administration - of all governments - to push back on sovereignty, you\'ve really overplayed your hand.\n\nWhat is the "Pax Silica" initiative? In a nutshell it\'s about the US getting other countries to commit to restructuring their AI tech infrastructure around a US-led stack. It\'s basically vendor lock-in: you hand over your critical minerals, align your export controls with Washington\'s, regulate AI the way America wants, and in return you get to be a US "trusted partner," whatever that means these days.\n\nIn essence, let\'s not kid ourselves, it\'s all about China: this is the US\'s initiative to "win the AI race" by getting other countries to contractually commit to keeping China out of their tech supply chains. When you can\'t preserve your lead through innovation, you seek to lock countries in contractually.\n\nFor instance as a country, this would mean telling Huawei they can\'t sell you AI chips, and telling Chinese firms they can\'t invest in your data centers - even if they\'re better and cheaper. It\'s not about choosing the best technology, it\'s about choosing the right flag.\n\nBut in this instance, the US went much further still: they literally tried to carve out 4,000 acres of Philippine territory (in New Clark City, 60 miles north of Manila) to be governed under US common law with diplomatic immunity - the first arrangement of its kind anywhere in the modern world.\n\nThis is according to the [WSJ](https://archive.ph/20260417112635/https://www.wsj.com/world/asia/u-s-to-create-high-tech-manufacturing-zone-in-philippines-017c1668) who ran the story last month as if it was a done deal (it wasn\'t).\n\nHeard about the "French concession" or "British concession" in China during the century of humiliation? Same thing: the US basically asked for an "American concession" in the Philippines.\n\nUnsurprisingly, there was quite a bit of backlash in the country with for instance the Peasant Movement of the Philippines (KMP) calling it a “massive sellout” of the country’s land, minerals, and sovereignty \n\nSo much so that the Philippines\' government - namely Joshua Bingcang, president and chief executive of the Bases Conversion and Development Authority (BCDA) - issued a statement saying that the Philippines had rejected US proposals that would place the project beyond local jurisdiction \n\nNote, by the way, this delicious irony: the BCDA is the government agency that was created in 1992 specifically to convert former US military bases at Clark and Subic Bay after the Philippines spent decades negotiating their closure. New Clark City - where the Pax Silica\'s hub would go - is built on the old Clark Air Base. \n\nSo the agency whose entire reason for existing is to turn former American colonial territory (i.e. US military bases) into sovereign Philippine land is the one now being asked to hand part of that very same land back under US jurisdiction (and, apparently, declined).\n\nOf course though, blocking this specific jurisdiction grab doesn\'t change the bigger picture. The Philippines is still a Pax Silica signatory, and Pax Silica itself is structurally neocolonial: you supply the cheap labor and raw materials, align your export controls and regulations with Washington\'s, cut yourself off from the world\'s rising technological powerhouse - and in exchange you get assembly jobs and the privilege of getting a pat on the head and being called a "trusted partner."\n\nThey dropped the most cartoonishly colonial demand - governing Philippine soil under US law - but the underlying architecture is the same: you serve America\'s supply chain, on America\'s terms, and you relinquish your sovereign right to trade with whoever offers the best deal.\n\n📎 [Arnaud Bertrand](https://fxtwitter.com/i/status/2058728222553186490)'


def test_extract_headline_preserves_cig_76087_full_text():
    """Real-fixture regression: the most-severe pre-fix truncation case.

    Pre-fix: _extract_headline returned only the 84-char first line
    ('📝 🇺🇸 🇵🇭 **The U.S. tried...| Arnaud Bertrand'); the 4143-char
    analytical body (Pax Silica analysis) was silently dropped.

    Post-fix: full text preserved up to the 4096 cap. The first 4096 chars
    of the 4227-char source round-trip intact, body content from beyond
    the first line is visible (test asserts substrings unique to body
    paragraphs are present in the extracted result).
    """
    result = _extract_headline(CIG_76087_FULL_TEXT)
    assert result is not None
    # Source is 4227 chars and has no leading/trailing whitespace.
    # Post-strip = 4227 > 4096 cap → result is first 4096 chars.
    assert len(result) == 4096
    assert result == CIG_76087_FULL_TEXT[:4096]
    # Substrings from body paragraphs that pre-fix would have been DROPPED
    # but post-fix are PRESENT (proves the first-line behavior is gone):
    assert "Pax Silica" in result        # first body paragraph
    assert "vendor lock-in" in result    # mid-body paragraph
    assert "Marcos administration" in result   # mid-body paragraph
    # Title (first line) is still present too:
    assert "tried to re-colonize" in result


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


def test_long_single_line_truncated_to_4096():
    """Integration version of the per-char ceiling check via full fetch path.
    Trump-style 800-char post round-trips intact; cap kicks in only above 4096."""
    src = _make_source()
    long_text = "x" * 5000
    base = datetime(2026, 5, 12, tzinfo=timezone.utc)
    mock_client = _make_mock_client(messages=[_fake_message(1, base, long_text)])
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert len(result.items[0].headline) == 4096


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


# ---------- Task 1: sponsor-noise filter ----------
#
# Added 2026-05-27. Per-channel literal substring filter (case-insensitive)
# applied inside _message_to_item. Drops sponsor / promo / affiliate posts
# before they reach the headlines table. Each drop: INFO log + append to
# noise-filter audit JSONL (see config.filtered_log_path).
#
# Empirical motivation: 90-headline Ateobreaking sample (2026-05-27) showed
# 7 sponsor posts mixed with editorial content. The approved filter list
# catches 6 of those 7; the 7th (Freedom Checker citation in real DeepSeek
# news /170656) was knowingly left out to preserve a genuine news item.


def test_noise_filter_drops_matching_headline():
    """Pattern matches → item not in result. Approved Tier A: ateo.digital."""
    src = _make_source(noise_filter=["ateo.digital"])
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [
        _fake_message(170762, base.replace(hour=15, minute=25), (
            "**На нашем сайте публикуются материалы о цифровой безопасности**\n\n"
            "Рекомендуем ознакомиться: https://ateo.digital/blog/"
        )),
        _fake_message(170763, base.replace(hour=15, minute=30), (
            "Американские военные сейчас не сопровождают коммерческие суда "
            "через Ормузский пролив"  # real news, no sponsor markers
        )),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    # Sponsor (/170762) filtered, news (/170763) passes through.
    assert [i.source_item_id for i in result.items] == ["170763"]


def test_noise_filter_case_insensitive():
    """Pattern `gnuvpn` (lower) matches source text containing `GnuVPN` (mixed)."""
    src = _make_source(noise_filter=["gnuvpn"])
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [
        _fake_message(170758, base, (
            "🦄 Стабильное соединение, когда это действительно важно 🦄\n\n"
            "С GnuVPN не нужно гадать, какой протокол выбрать"  # mixed case
        )),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.items == []  # filtered


def test_noise_filter_empty_list_passes_through():
    """Backwards-compat: no noise_filter configured → sponsor-text passes through.
    Channels without filter config behave identically to pre-Task-1 baseline."""
    src = _make_source(noise_filter=None)  # explicit None — also default
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [
        _fake_message(170758, base, "С GnuVPN не нужно гадать"),  # would match
                                                                  # if filter set
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 1
    assert result.items[0].source_item_id == "170758"


def test_noise_filter_multi_pattern_first_match_wins(caplog):
    """Three patterns configured; headline matches the 2nd. INFO log + audit
    record the 2nd pattern (the one that actually matched), not the 1st."""
    src = _make_source(noise_filter=["ateo.digital", "gnuvpn", "#Реклама"])
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    # Headline contains "gnuvpn" but not "ateo.digital" (and contains #Реклама,
    # which is later in the list). Iteration order matters: ateo.digital is
    # checked first (no match), then gnuvpn (match — stop and report).
    messages = [
        _fake_message(170758, base, "С GnuVPN всё работает.\n\n#Реклама"),
    ]
    mock_client = _make_mock_client(messages=messages)
    with caplog.at_level(logging.INFO, logger="news_watch_daemon.sources.telegram"):
        with patch(
            "news_watch_daemon.sources.telegram.TelegramClient",
            return_value=mock_client,
        ):
            result = src.fetch(since_unix=0)
    assert result.items == []
    # The log should mention pattern='gnuvpn' (the FIRST one that matched in
    # iteration order), not '#Реклама' (which also matches but comes later).
    info_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    filter_msgs = [m for m in info_messages if "sponsor-noise filtered" in m]
    assert len(filter_msgs) == 1
    assert "pattern='gnuvpn'" in filter_msgs[0]
    assert "#Реклама" not in filter_msgs[0]


def test_noise_filter_logs_info_on_drop(caplog):
    """Filter hit → exactly one INFO log line with channel, msg_id, and
    matched pattern. Operator visibility into per-cycle filter activity
    without needing to parse the JSONL audit log."""
    src = _make_source(
        channel_username="Ateobreaking",
        noise_filter=["?start=ateobreakinga"],
    )
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [
        _fake_message(170642, base, (
            "Нейросети уже заменяют носителей языка.\n\n"
            "🇬🇧 Английский: ChattyEnglishBot?start=ateobreakinga"
        )),
    ]
    mock_client = _make_mock_client(messages=messages)
    with caplog.at_level(logging.INFO, logger="news_watch_daemon.sources.telegram"):
        with patch(
            "news_watch_daemon.sources.telegram.TelegramClient",
            return_value=mock_client,
        ):
            result = src.fetch(since_unix=0)
    assert result.items == []
    info_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    filter_msgs = [m for m in info_messages if "sponsor-noise filtered" in m]
    assert len(filter_msgs) == 1
    msg = filter_msgs[0]
    assert "channel=@Ateobreaking" in msg
    assert "msg_id=170642" in msg
    assert "?start=ateobreakinga" in msg


def test_noise_filter_non_matching_passes_through():
    """Real-fixture regression for the /170656 borderline case.

    Pattern `freedomchecker` was knowingly LEFT OUT of the approved filter
    list because /170656 is a real news headline about DeepSeek being
    blocked in Russia where Freedom Checker is cited as the source. This
    test fixes that decision in code: with the approved 6-pattern list, a
    headline containing 'Freedom Checker' but none of the approved
    patterns must pass through.
    """
    approved_filters = [
        "ateo.digital",
        "gnuvpn",
        "Ateo_help_bot",
        "durevpnbot",
        "?start=ateobreakinga",
        "#Реклама",
    ]
    src = _make_source(noise_filter=approved_filters)
    base = datetime(2026, 5, 24, tzinfo=timezone.utc)
    # Real-shape /170656 text (DeepSeek news with FC citation, no Ateo URL).
    # The (https://freedomchecker.ateo.digital/) URL in the ACTUAL post WOULD
    # match the `ateo.digital` pattern — that's a known false positive for
    # this specific headline. Test uses a slightly redacted variant to
    # confirm the OTHER patterns don't false-fire on news content.
    deepseek_news = (
        "**Китайская нейросеть DeepSeek может быть заблокирован в России**\n\n"
        "Пользователи жалуются, что сайт не загружается без VPN — Freedom Checker."
    )
    messages = [_fake_message(170656, base, deepseek_news)]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 1
    assert result.items[0].source_item_id == "170656"


def test_noise_filter_audit_log_jsonl_shape(tmp_path):
    """Filter hit with filtered_log_path set → one JSONL line on disk with
    the full schema: filtered_at_unix, filtered_at, channel, msg_id,
    matched_pattern, full_text (untruncated)."""
    audit_path = tmp_path / "filtered.jsonl"
    full_sponsor_text = (
        "🦄 С**табильное соединение, когда это действительно важно** 🦄\n\n"
        "С GnuVPN не нужно гадать, какой протокол выбрать. " * 5  # ~600 chars
        + "\n\n#Реклама"
    )
    src = _make_source(
        channel_username="Ateobreaking",
        noise_filter=["gnuvpn"],
        filtered_log_path=audit_path,
    )
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [_fake_message(170758, base, full_sponsor_text)]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    assert result.items == []

    # Audit file should exist with exactly one line of valid JSON.
    assert audit_path.is_file()
    lines = audit_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1
    entry = json.loads(lines[0])
    # Required schema fields, all present and well-typed.
    assert set(entry.keys()) == {
        "filtered_at_unix", "filtered_at", "channel",
        "msg_id", "matched_pattern", "full_text",
    }
    assert isinstance(entry["filtered_at_unix"], int)
    assert isinstance(entry["filtered_at"], str)
    assert entry["filtered_at"].endswith("Z")  # UTC ISO-8601
    assert entry["channel"] == "Ateobreaking"
    assert entry["msg_id"] == "170758"
    assert entry["matched_pattern"] == "gnuvpn"
    # full_text is untruncated — the 600+ char sponsor body is preserved.
    assert entry["full_text"] == full_sponsor_text


def test_noise_filter_unicode_case_insensitivity_cyrillic():
    """Pattern `#Реклама` must match Cyrillic in any case. Python str.lower()
    handles Cyrillic correctly, but lock the behavior with a test —
    future advertisers will use mixed/lower case, and the invariant matters
    for ongoing forward-coverage of the Russian advertising disclosure."""
    src = _make_source(noise_filter=["#Реклама"])
    base = datetime(2026, 5, 26, tzinfo=timezone.utc)
    messages = [
        # Three variants: uppercase initial cap (canonical legal form),
        # all-lowercase, all-uppercase.
        _fake_message(1, base.replace(hour=10), "Купите наш VPN!\n\n#Реклама"),
        _fake_message(2, base.replace(hour=11), "Купите наш VPN!\n\n#реклама"),
        _fake_message(3, base.replace(hour=12), "Купите наш VPN!\n\n#РЕКЛАМА"),
        _fake_message(4, base.replace(hour=13), "Обычная новость без рекламы"),
    ]
    mock_client = _make_mock_client(messages=messages)
    with patch(
        "news_watch_daemon.sources.telegram.TelegramClient",
        return_value=mock_client,
    ):
        result = src.fetch(since_unix=0)
    # All three case variants filtered; the news item passes through.
    assert [i.source_item_id for i in result.items] == ["4"]
