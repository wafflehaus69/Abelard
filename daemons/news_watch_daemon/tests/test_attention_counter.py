"""count_terms_collapsed tests — per-headline distinct counting + window math.

The bag-of-words tokenize/count_terms were retired 2026-07-07 (footgun
cleanup); the ordered tokenizer + collapse are tested in
test_attention_adjacency.py. These tests pin the DB-window plumbing and
bounds validation of the live collapsed counter. The corpora use distinct
single-word content (no recurring adjacent pairs, no soft-stopwords) so the
collapsed counts equal the plain unigram counts.
"""

from __future__ import annotations

import sqlite3

import pytest

from news_watch_daemon.attention.counter import (
    WINDOW_HOURS_MAX,
    WINDOW_HOURS_MIN,
    WINDOW_SECONDS,
    count_terms_collapsed,
)


def _make_conn() -> sqlite3.Connection:
    """Minimal in-memory DB with just enough schema for the counter.

    Includes headline_en so the counter's COALESCE(headline_en, headline)
    read works; inserts leave headline_en NULL so COALESCE returns the
    original `headline`.
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


def test_distinct_per_headline():
    """One headline mentioning 'iran' 5 times counts ONCE in window_counts."""
    conn = _make_conn()
    _insert(conn, "h1", "iran iran iran iran iran", NOW - 1)
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 1


def test_aggregates_across_headlines():
    conn = _make_conn()
    _insert(conn, "h1", "iran sanctions", NOW - 1)
    _insert(conn, "h2", "iran nuclear talks", NOW - 100)
    _insert(conn, "h3", "iran missile launch", NOW - 1000)
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 3
    assert counts.window_counts["sanctions"] == 1
    assert counts.window_counts["nuclear"] == 1


def test_window_filters_by_published_at():
    """Headlines outside the 24h window aren't counted in window_counts."""
    conn = _make_conn()
    _insert(conn, "live", "iran inside zone", NOW - 1000)                  # in window
    _insert(conn, "old", "iran outside zone", NOW - WINDOW_SECONDS - 1)    # in prior
    _insert(conn, "ancient", "iran way back", NOW - 5 * WINDOW_SECONDS)    # past both
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 1
    assert counts.prior_counts["iran"] == 1


def test_filters_stopwords():
    conn = _make_conn()
    _insert(conn, "h1", "the iran tests new missile", NOW - 1)
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset({"the", "new"}))
    assert "the" not in counts.window_counts
    assert "new" not in counts.window_counts
    assert "iran" in counts.window_counts
    assert "missile" in counts.window_counts


def test_window_boundary_inclusive_since_inclusive_until():
    """Window is [now-24h, now] inclusive on both ends."""
    conn = _make_conn()
    _insert(conn, "boundary_since", "iran", NOW - WINDOW_SECONDS)   # at since
    _insert(conn, "boundary_until", "iran", NOW)                     # at until
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_counts["iran"] == 2


def test_records_window_timestamps():
    conn = _make_conn()
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts.window_since_unix == NOW - WINDOW_SECONDS
    assert counts.window_until_unix == NOW
    assert counts.prior_since_unix == NOW - 2 * WINDOW_SECONDS
    assert counts.prior_until_unix == NOW - WINDOW_SECONDS


# ---------- window_hours parameterization ----------


def test_window_hours_default_24_is_bit_identical():
    """Omitting `window_hours` → identical behavior to explicit 24h."""
    conn = _make_conn()
    _insert(conn, "h1", "iran nuclear", NOW - 1)
    _insert(conn, "h_prior", "iran sanctions", NOW - WINDOW_SECONDS - 1000)
    default = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    explicit = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=24)
    assert default.window_since_unix == explicit.window_since_unix
    assert default.window_until_unix == explicit.window_until_unix
    assert default.prior_since_unix == explicit.prior_since_unix
    assert default.prior_until_unix == explicit.prior_until_unix
    assert default.window_counts == explicit.window_counts
    assert default.prior_counts == explicit.prior_counts


def test_window_hours_6_uses_6h_window():
    conn = _make_conn()
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=6)
    assert counts.window_since_unix == NOW - 6 * 3600
    assert counts.window_until_unix == NOW
    assert counts.prior_since_unix == NOW - 12 * 3600
    assert counts.prior_until_unix == NOW - 6 * 3600


def test_window_hours_48_uses_48h_window():
    conn = _make_conn()
    counts = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=48)
    assert counts.window_since_unix == NOW - 48 * 3600
    assert counts.window_until_unix == NOW
    assert counts.prior_since_unix == NOW - 96 * 3600
    assert counts.prior_until_unix == NOW - 48 * 3600


def test_window_hours_6_filters_at_custom_boundary():
    """A 10h-old headline is in the 24h live window but the 6h-prior window."""
    conn = _make_conn()
    _insert(conn, "in_6h", "iran missile", NOW - 3 * 3600)              # 3h ago
    _insert(conn, "in_24h_not_6h", "iran sanctions", NOW - 10 * 3600)   # 10h ago

    counts_24h = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset())
    assert counts_24h.window_counts["iran"] == 2
    assert counts_24h.window_counts.get("missile", 0) == 1
    assert counts_24h.window_counts.get("sanctions", 0) == 1

    counts_6h = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=6)
    assert counts_6h.window_counts["iran"] == 1        # only the 3h headline in live
    assert counts_6h.window_counts.get("missile", 0) == 1
    # The 10h headline lands in the 6h-prior window: 'iran' picks up a prior
    # count of 1. (prior_counts is keyed by WINDOW terms — a prior-only word
    # like 'sanctions' is not a key; consumers only ever .get(window_term).)
    assert counts_6h.prior_counts["iran"] == 1


def test_window_hours_below_min_raises():
    conn = _make_conn()
    with pytest.raises(ValueError, match="window_hours"):
        count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=0)
    with pytest.raises(ValueError, match="window_hours"):
        count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(), window_hours=-1)


def test_window_hours_above_max_raises():
    conn = _make_conn()
    with pytest.raises(ValueError, match="window_hours"):
        count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(),
                              window_hours=WINDOW_HOURS_MAX + 1)


def test_window_hours_bounds_inclusive():
    """Min (1) and max (168) are inclusive — pinned against an off-by-one."""
    conn = _make_conn()
    counts_min = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(),
                                       window_hours=WINDOW_HOURS_MIN)
    counts_max = count_terms_collapsed(conn, now_unix=NOW, stopwords=frozenset(),
                                       window_hours=WINDOW_HOURS_MAX)
    assert counts_min.window_since_unix == NOW - WINDOW_HOURS_MIN * 3600
    assert counts_max.window_since_unix == NOW - WINDOW_HOURS_MAX * 3600
