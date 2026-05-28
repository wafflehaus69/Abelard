"""Cluster tests — term-to-headlines retrieval with case-insensitive
word-boundary discipline (Fix-2 era word-boundary semantics)."""

from __future__ import annotations

import sqlite3

from news_watch_daemon.attention.cluster import cluster_for_term


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        # headline_en added Pass F (2026-05-28); existing inserts leave
        # it NULL, COALESCE(headline_en, headline) returns headline.
        "CREATE TABLE headlines ("
        "headline_id TEXT PRIMARY KEY, "
        "source TEXT, "
        "headline TEXT, "
        "headline_en TEXT, "
        "url TEXT, "
        "raw_source TEXT, "
        "published_at_unix INTEGER)"
    )
    return conn


def _insert(conn, *, hid: str, source: str, headline: str,
            published_at_unix: int, url: str | None = None,
            raw_source: str | None = None) -> None:
    conn.execute(
        "INSERT INTO headlines (headline_id, source, headline, url, "
        "raw_source, published_at_unix) VALUES (?, ?, ?, ?, ?, ?)",
        (hid, source, headline, url, raw_source, published_at_unix),
    )


def test_cluster_finds_simple_match():
    conn = _make_conn()
    _insert(conn, hid="h1", source="finnhub:general",
            headline="Iran tests new missile", published_at_unix=100)
    _insert(conn, hid="h2", source="finnhub:general",
            headline="Unrelated weather story", published_at_unix=200)
    cluster = cluster_for_term(conn, term="iran", window_since_unix=0, window_until_unix=1000)
    assert len(cluster) == 1
    assert cluster[0].headline_id == "h1"


def test_cluster_case_insensitive_match():
    """`iran` matches `Iran`, `IRAN`, `iRaN` alike."""
    conn = _make_conn()
    _insert(conn, hid="h1", source="x", headline="IRAN action", published_at_unix=10)
    _insert(conn, hid="h2", source="x", headline="iran action", published_at_unix=20)
    _insert(conn, hid="h3", source="x", headline="Iran action", published_at_unix=30)
    cluster = cluster_for_term(conn, term="iran", window_since_unix=0, window_until_unix=100)
    assert {h.headline_id for h in cluster} == {"h1", "h2", "h3"}


def test_cluster_enforces_word_boundary():
    """`iran` must NOT match `irancould` or `Iranian`. Same discipline as Fix 2."""
    conn = _make_conn()
    _insert(conn, hid="h1", source="x", headline="Iran sanctions imposed", published_at_unix=10)
    _insert(conn, hid="h2", source="x", headline="Iranian officials commented", published_at_unix=20)
    _insert(conn, hid="h3", source="x", headline="irancould be misspelled", published_at_unix=30)
    cluster = cluster_for_term(conn, term="iran", window_since_unix=0, window_until_unix=100)
    # Only h1 should match — Iranian and irancould both contain 'iran' as a
    # substring but NOT as a whole word.
    assert {h.headline_id for h in cluster} == {"h1"}


def test_cluster_filters_by_published_at_window():
    conn = _make_conn()
    _insert(conn, hid="in", source="x", headline="Iran inside", published_at_unix=500)
    _insert(conn, hid="early", source="x", headline="Iran before", published_at_unix=50)
    _insert(conn, hid="late", source="x", headline="Iran after", published_at_unix=1500)
    cluster = cluster_for_term(conn, term="iran",
                               window_since_unix=100, window_until_unix=1000)
    assert {h.headline_id for h in cluster} == {"in"}


def test_cluster_orders_newest_first():
    conn = _make_conn()
    _insert(conn, hid="old", source="x", headline="Iran first", published_at_unix=10)
    _insert(conn, hid="newest", source="x", headline="Iran latest", published_at_unix=300)
    _insert(conn, hid="mid", source="x", headline="Iran middle", published_at_unix=100)
    cluster = cluster_for_term(conn, term="iran", window_since_unix=0, window_until_unix=1000)
    assert [h.headline_id for h in cluster] == ["newest", "mid", "old"]


def test_cluster_empty_when_no_match():
    conn = _make_conn()
    _insert(conn, hid="h1", source="x", headline="Weather report", published_at_unix=100)
    assert cluster_for_term(conn, term="iran", window_since_unix=0, window_until_unix=1000) == []


def test_cluster_carries_attribution_fields():
    """ClusterHeadline includes source, publisher (raw_source), url, timestamp."""
    conn = _make_conn()
    _insert(
        conn, hid="h1", source="finnhub:general",
        headline="Iran tests new missile",
        url="https://example.com/x", raw_source="Reuters",
        published_at_unix=12345,
    )
    cluster = cluster_for_term(conn, term="iran",
                               window_since_unix=0, window_until_unix=99999)
    assert len(cluster) == 1
    h = cluster[0]
    assert h.source == "finnhub:general"
    assert h.publisher == "Reuters"
    assert h.url == "https://example.com/x"
    assert h.published_at_unix == 12345
