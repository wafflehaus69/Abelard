"""Paranoid read-only enforcement test for `sources/telegram.py`.

The architectural rule (Pass B brief, non-negotiable #1): the Telegram
source plugin must be incapable of writing to Telegram. This is
enforced by the architecture, not by convention or policy. Future
"small additions" that drift toward write capability must surface in
code review, not bury themselves quietly.

The test is a paranoid text grep — it asserts that the source file
does not even MENTION the names of write-capable Telethon methods,
neither in code nor in comments nor in docstrings. A failure here
means someone added something they shouldn't have, OR documented
something in a way that defeats the grep. Both warrant a conversation.

The test also asserts the Telethon import surface is restricted to a
known allow-list. New imports from `telethon.*` require a deliberate
update to this test, which forces the architectural change to be
visible at review time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "news_watch_daemon"

# Paranoid-grep coverage — every file that imports from telethon AND
# uses the burner session must be in this list. Extended Pass F Commit
# 2 (2026-05-28) to include the translation module: it ALSO uses the
# Telethon client + burner session for messages.translateText calls
# (read-only by Telegram's API contract — the request returns translated
# text without modifying source messages), so the same architectural
# read-only invariant must hold.
PROTECTED_FILES: tuple[Path, ...] = (
    _SRC_ROOT / "sources" / "telegram.py",
    _SRC_ROOT / "translation" / "telegram_native.py",
    _SRC_ROOT / "translation" / "runner.py",
)


# Method names that would constitute Telegram write capability. The
# test asserts NONE of these strings appear in any protected file. This
# list comes directly from the Pass B brief.
FORBIDDEN_METHOD_NAMES: tuple[str, ...] = (
    "send_message",
    "forward_messages",
    "join_channel",
    "leave_channel",
    "delete_messages",
    "edit_message",
    "mark_read",
    "send_file",
    "send_photo",
    "send_voice",
    "pin_message",
    "unpin_message",
)


# Allowed imports from `telethon.*` (union across all protected files).
# New entries require a deliberate test update so the architectural
# change is visible at review.
ALLOWED_TELETHON_IMPORTS: frozenset[str] = frozenset({
    "TelegramClient",
    "errors",
    "StringSession",
    # Pass F translation module — read-only RPC against existing
    # Telegram messages, no write surface. Translates server-side and
    # returns text; does not mutate the source channel.
    "TranslateTextRequest",
})


@pytest.fixture(scope="module", params=PROTECTED_FILES, ids=lambda p: p.name)
def plugin_source(request) -> str:
    return request.param.read_text(encoding="utf-8")


@pytest.mark.parametrize("forbidden", FORBIDDEN_METHOD_NAMES)
def test_forbidden_method_name_does_not_appear(plugin_source: str, forbidden: str):
    assert forbidden not in plugin_source, (
        f"Forbidden Telegram write method name {forbidden!r} appears in "
        f"a protected file. The architectural read-only invariant "
        f"prohibits write capability — even in comments or docstrings. "
        f"If this is a legitimate architectural change, update "
        f"FORBIDDEN_METHOD_NAMES with an explicit justification."
    )


def test_telethon_imports_are_restricted(plugin_source: str):
    """Every `from telethon...` import must only pull allowed names."""
    pattern = re.compile(r"from\s+telethon[^\s]*\s+import\s+([^\n]+)")
    found_imports: set[str] = set()
    for m in pattern.finditer(plugin_source):
        # Split on comma, strip "as X" aliases.
        names = [
            n.split(" as ")[0].strip()
            for n in m.group(1).split(",")
        ]
        for name in names:
            if name:
                found_imports.add(name)
    unexpected = found_imports - ALLOWED_TELETHON_IMPORTS
    assert not unexpected, (
        f"Unexpected telethon imports in protected file: {sorted(unexpected)}. "
        f"Allowed: {sorted(ALLOWED_TELETHON_IMPORTS)}. New imports must be added "
        f"to ALLOWED_TELETHON_IMPORTS with explicit justification."
    )


def test_no_telethon_events_or_buttons_imported(plugin_source: str):
    """Belt-and-suspenders: `events` and `Button` suggest interactive use."""
    assert "telethon.events" not in plugin_source
    assert "telethon.tl.custom.Button" not in plugin_source
    # Also catch `from telethon import ... events ...` style
    assert not re.search(r"from\s+telethon[^\n]*\bevents\b", plugin_source)
    assert not re.search(r"from\s+telethon[^\n]*\bButton\b", plugin_source)
