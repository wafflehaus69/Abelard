"""Paranoid grep test for TelegramBotSink — sink-symmetric with SignalSink.

Mirrors the SignalSink readonly test pattern. Different specific
patterns (Telegram has no group IDs in the SignalSink sense; instead
the recipient identifiers are Bot API tokens and numeric chat IDs)
but identical architectural discipline: no hardcoded recipient
identifiers in the module text, ever.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PLUGIN_PATH = (
    Path(__file__).resolve().parent.parent
    / "src" / "news_watch_daemon" / "alert" / "telegram_bot_sink.py"
)


# Value-level patterns that must NOT appear in the source. Each
# targets literals, not variable/parameter names.
FORBIDDEN_PATTERNS: tuple[tuple[str, str], ...] = (
    # Bot API tokens: `<8-12 digit bot id>:<35-char base62>` format.
    # E.g. `123456789:ABCdefGhIJklmNoPQrStUvWxYz0123456789`.
    (r"\d{8,}:[A-Za-z0-9_-]{30,}", "Telegram Bot API token literal"),

    # Hardcoded numeric chat IDs in quotes: positive (users) or
    # negative (groups/channels). Length-bound to avoid catching
    # short ints like timeouts.
    (r"['\"]-?\d{8,}['\"]", "hardcoded numeric chat_id literal in quotes"),

    # Hardcoded @-prefix channel usernames in quotes.
    (r"['\"]@[a-zA-Z][a-zA-Z0-9_]{4,31}['\"]", "hardcoded @channel literal"),
)


# Currently empty — `chat_id` and `bot_token` as variable/parameter
# names contain neither digits nor `@` prefixes nor `:` separators,
# so they don't match the patterns above. Future regex tightening
# might require additions here.
WHITELIST: frozenset[str] = frozenset()


@pytest.fixture(scope="module")
def source_text() -> str:
    assert PLUGIN_PATH.is_file(), f"TelegramBotSink module missing at {PLUGIN_PATH}"
    return PLUGIN_PATH.read_text(encoding="utf-8")


@pytest.mark.parametrize("pattern,description", FORBIDDEN_PATTERNS)
def test_no_forbidden_recipient_literal(source_text: str, pattern: str, description: str):
    """No pattern matching a recipient-identifier literal may appear."""
    rx = re.compile(pattern)
    matches = [m.group(0) for m in rx.finditer(source_text)]
    matches = [m for m in matches if m not in WHITELIST]
    assert matches == [], (
        f"Forbidden {description} found in telegram_bot_sink.py: {matches}. "
        f"If this is a deliberate architectural change, update "
        f"FORBIDDEN_PATTERNS or WHITELIST with explicit justification."
    )


def test_credentials_validation_gate_exists_as_greppable_function(source_text: str):
    """Construction-time validation gate must be a named function.

    Mirror of the SignalSink rule: the architectural safeguard
    (no dispatch without verified credentials) must be visible in
    code search, not inlined inside the constructor or dispatch.
    """
    assert re.search(r"\bdef\s+_assert_credentials_present\s*\(", source_text), (
        "Construction-time validation gate must be a named function "
        "`_assert_credentials_present`. Inlining inside __post_init__ "
        "or dispatch() defeats the architecture-as-safeguard discipline."
    )


def test_post_init_calls_credentials_validation_gate(source_text: str):
    """__post_init__() must invoke _assert_credentials_present.

    Belt-and-suspenders alongside runtime tests: verifies the call is
    wired even if a future refactor accidentally drops it.
    """
    assert "_assert_credentials_present(" in source_text, (
        "TelegramBotSink.__post_init__ must call "
        "_assert_credentials_present() — the construction-time gate "
        "is the sole guardrail against misconfigured sinks reaching "
        "dispatch()."
    )


def test_no_imports_from_telethon(source_text: str):
    """TelegramBotSink is Bot API only, not MTProto.

    Pass B's TelegramSource uses Telethon for inbound MTProto reads;
    TelegramBotSink is fully distinct (different namespace, different
    credentials, different transport). Any telethon import here would
    indicate accidental coupling.
    """
    assert not re.search(r"^\s*(from\s+telethon|import\s+telethon)",
                         source_text, re.MULTILINE), (
        "TelegramBotSink must not import from telethon. Inbound MTProto "
        "(Pass B) and outbound Bot API are intentionally separated; "
        "shared credentials/sessions are architecturally prohibited."
    )
