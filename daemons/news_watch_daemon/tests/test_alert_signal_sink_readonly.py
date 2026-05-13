"""Paranoid grep test for SignalSink — architectural safeguard.

Mirrors the Pass B `test_sources_telegram_readonly.py` pattern: a text
grep over the module source file that asserts NO hardcoded recipient
identifiers appear, anywhere — code, comments, docstrings, all of it.

This is the architecture-as-safeguard discipline. Any future change
that introduces a hardcoded phone number, Signal group ID, or
recipient-id-shaped literal must DELIBERATELY update one of:
  - the FORBIDDEN_PATTERNS list (to remove a check), or
  - the WHITELIST set (to except a specific string), or
  - the module itself (to remove the literal).

All three surface the architectural change at code-review time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PLUGIN_PATH = (
    Path(__file__).resolve().parent.parent
    / "src" / "news_watch_daemon" / "alert" / "signal_sink.py"
)


# Patterns that must NOT appear in the SignalSink module source.
# Each is a value-level pattern (matches hardcoded literals in source
# text). Variable names like "recipient" are NOT flagged — only
# literal values that look like recipient identifiers.
FORBIDDEN_PATTERNS: tuple[tuple[str, str], ...] = (
    # International phone numbers: +12345678 or longer.
    (r"\+\d{6,}", "international phone number literal"),

    # North American 10-digit phone format: 555-123-4567, 555.123.4567,
    # 5551234567, etc. Use a tight pattern to avoid catching things
    # like timeouts (which are short ints) or year-style numbers.
    (r"['\"]\d{3}[-.]\d{3}[-.]\d{4}['\"]", "North American phone literal in quotes"),

    # Signal group ID format: `group.XXX...` with base64-ish payload.
    (r"group\.[A-Za-z0-9+/=]{6,}", "Signal group ID literal"),
)


# Strings that ARE permitted in this module. The whitelist exists for
# tokens that might surface false positives in future regex tightening.
# Currently empty — the configured destination `note_to_self` is a
# plain underscore string and doesn't match any FORBIDDEN_PATTERNS.
WHITELIST: frozenset[str] = frozenset()


@pytest.fixture(scope="module")
def source_text() -> str:
    assert PLUGIN_PATH.is_file(), f"SignalSink module missing at {PLUGIN_PATH}"
    return PLUGIN_PATH.read_text(encoding="utf-8")


@pytest.mark.parametrize("pattern,description", FORBIDDEN_PATTERNS)
def test_no_forbidden_recipient_literal(source_text: str, pattern: str, description: str):
    """No pattern matching a recipient-identifier literal may appear."""
    rx = re.compile(pattern)
    matches = [m.group(0) for m in rx.finditer(source_text)]
    # Filter out whitelisted strings (currently empty list).
    matches = [m for m in matches if m not in WHITELIST]
    assert matches == [], (
        f"Forbidden {description} found in signal_sink.py source: {matches}. "
        f"If this is a deliberate architectural change, update FORBIDDEN_PATTERNS "
        f"with explicit justification in the test, OR add the value to WHITELIST."
    )


def test_destination_validation_gate_exists_as_greppable_function(source_text: str):
    """The destination-validation gate must be a named function, not inlined.

    Pass C Step 6 directive from Mando: name the gate as its own
    greppable function so the architectural safeguard is visible in
    code search. Inline-checking inside dispatch() would defeat the
    'architecture is the safeguard' discipline.
    """
    # Must have a `def _assert_destination_allowed` definition
    assert re.search(r"\bdef\s+_assert_destination_allowed\s*\(", source_text), (
        "Destination-validation gate must be a top-level/named function "
        "`_assert_destination_allowed`. Inlining inside dispatch() is "
        "architecturally prohibited per Pass C §7a."
    )


def test_dispatch_calls_destination_validation_gate(source_text: str):
    """The dispatch() method must invoke `_assert_destination_allowed`.

    Belt-and-suspenders alongside the runtime test — verifies the call
    is wired even if the runtime tests miss a refactor.
    """
    assert "_assert_destination_allowed(" in source_text, (
        "dispatch() must call _assert_destination_allowed() — the "
        "destination-validation gate is the sole gatekeeper for "
        "signal-cli invocation."
    )


def test_allowed_destination_is_the_only_destination_constant(source_text: str):
    """Only one destination constant — ALLOWED_DESTINATION = 'note_to_self'."""
    # Find module-level "ALLOWED_DESTINATION = ..." assignments
    matches = re.findall(r"^ALLOWED_DESTINATION\s*=\s*(['\"])(.*?)\1", source_text, re.MULTILINE)
    assert len(matches) == 1, (
        f"Exactly one ALLOWED_DESTINATION constant expected; found {len(matches)}"
    )
    assert matches[0][1] == "note_to_self", (
        f"ALLOWED_DESTINATION must be 'note_to_self'; got {matches[0][1]!r}"
    )
