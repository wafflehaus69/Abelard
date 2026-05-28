"""Counter tests — tokenization + per-headline distinct counting + window math."""

from __future__ import annotations

import sqlite3

import pytest

from news_watch_daemon.attention.counter import (
    WINDOW_SECONDS,
    count_terms,
    tokenize,
)


# ---------- tokenize ----------


def test_tokenize_alphabetic_only_min_two_chars():
    """Regex is `\\b[a-zA-Z]{2,}\\b` — alphabetic, 2+ chars, word-boundary."""
    result = tokenize("Iran 2026 tested AI safety", frozenset())
    assert "iran" in result
    assert "tested" in result
    assert "ai" in result   # 2-char alphabetic OK
    assert "safety" in result
    assert "2026" not in result   # digits don't tokenize
    assert "a" not in result      # single char filtered (regex {2,})


def test_tokenize_lowercases():
    result = tokenize("CENTCOM SWIFT statement", frozenset())
    assert result == {"centcom", "swift", "statement"}


def test_tokenize_filters_stopwords():
    stopwords = frozenset({"the", "of", "swift"})
    result = tokenize("the Strait of Hormuz blocked swift transit", stopwords)
    assert result == {"strait", "hormuz", "blocked", "transit"}


def test_tokenize_returns_set_no_duplicates():
    """Per-headline distinct counting depends on tokenize returning a set."""
    result = tokenize("iran iran IRAN Iran iRaN", frozenset())
    assert result == {"iran"}


def test_tokenize_empty_inputs():
    assert tokenize(None, frozenset()) == set()
    assert tokenize("", frozenset()) == set()
    assert tokenize("   ", frozenset()) == set()
    assert tokenize("!@#$%^&*()", frozenset()) == set()


def test_tokenize_word_boundary_matches_inside_punctuation():
    """`iran's` boundary: `\\b` between word/non-word, so `iran` matches."""
    result = tokenize("iran's nuclear program", frozenset())
    assert "iran" in result
    # The trailing 's becomes its own one-char token — filtered by min-2 rule
    assert "s" not in result


# ---------- count_terms ----------


def _make_conn() -> sqlite3.Connection:
    """Minimal in-memory DB with just enough schema for the counter.

    Pass F (2026-05-28): includes headline_en column so the counter's
    COALESCE(headline_en, headline) read works against this fixture
    schema. Existing tests insert with headline_en defaulting to NULL,
    so the COALESCE returns the original `headline` value — behavior
    is bit-identical to the pre-Pass-F counter semantics on these
    fixtures.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE headlines (headline_id TEXT PRIMARY KEY, headline TEXT, "
        "headline_en TEXT, published_at_unix INTEGER)"
    )
    return conn


def _insert(conn, hid: str, headline: str, published_at_unix: int) -> None:
    conn.execute(
        "INSERT INTO headlines (headline_id, headline, published_at_unix) VALUES (?, ?, ?)",
        (hid, headline, published_at_unix),
    )


NOW = 1_800_000_000  # arbitrary anchor in seconds since epoch


def test_count_terms_distinct_per_headline():
    """One headline mentioning 'iran' 5 times counts ONCE in window_counts.
    Per Pass E spec — attention is per-headline not per-occurrence."""
    conn = _make_conn()
    _insert(conn, "h1", "iran iran iran iran iran", NOW - 1)
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 1


def test_count_terms_aggregates_across_headlines():
    conn = _make_conn()
    _insert(conn, "h1", "iran sanctions", NOW - 1)
    _insert(conn, "h2", "iran nuclear talks", NOW - 100)
    _insert(conn, "h3", "iran missile launch", NOW - 1000)
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 3
    assert counts.window_counts["sanctions"] == 1
    assert counts.window_counts["nuclear"] == 1


def test_count_terms_window_filters_by_published_at():
    """Headlines outside the 24h window aren't counted in window_counts."""
    conn = _make_conn()
    _insert(conn, "live", "iran inside window", NOW - 1000)               # in window
    _insert(conn, "old", "iran outside window", NOW - WINDOW_SECONDS - 1)  # in prior
    _insert(conn, "ancient", "iran way back", NOW - 5 * WINDOW_SECONDS)    # past both
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 1
    assert counts.prior_counts["iran"] == 1


def test_count_terms_filters_stopwords():
    conn = _make_conn()
    _insert(conn, "h1", "the iran tests new missile", NOW - 1)
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset({"the", "new"}))
    assert "the" not in counts.window_counts
    assert "new" not in counts.window_counts
    assert "iran" in counts.window_counts
    assert "missile" in counts.window_counts


def test_count_terms_window_boundary_inclusive_since_inclusive_until():
    """Window is [now-24h, now] inclusive on both ends."""
    conn = _make_conn()
    _insert(conn, "boundary_since", "iran", NOW - WINDOW_SECONDS)   # at since
    _insert(conn, "boundary_until", "iran", NOW)                     # at until
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 2


def test_count_terms_records_window_timestamps():
    conn = _make_conn()
    counts = count_terms(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_since_unix == NOW - WINDOW_SECONDS
    assert counts.window_until_unix == NOW
    assert counts.prior_since_unix == NOW - 2 * WINDOW_SECONDS
    assert counts.prior_until_unix == NOW - WINDOW_SECONDS
