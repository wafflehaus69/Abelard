"""Headline clustering tests — hermetic synthetic cases.

Validates the union-find algorithm, the three signal types (URL,
Jaccard, opt-in publisher+time), stopword handling, and the
newest-first ordering invariants.
"""

from __future__ import annotations

import pytest

from news_watch_daemon.synthesize.cluster import (
    DEFAULT_JACCARD_THRESHOLD,
    DEFAULT_TIME_WINDOW_S,
    Cluster,
    ClusterInput,
    _jaccard,
    _tokens,
    cluster_headlines,
)


def _ci(
    hid: str,
    text: str,
    *,
    url: str | None = None,
    publisher: str | None = None,
    published_at_unix: int = 0,
) -> ClusterInput:
    return ClusterInput(
        headline_id=hid,
        headline=text,
        url=url,
        publisher=publisher,
        published_at_unix=published_at_unix,
    )


# ---------- defaults expose the chosen design ----------


def test_default_jaccard_threshold():
    assert DEFAULT_JACCARD_THRESHOLD == 0.4


def test_default_time_window_is_none():
    """Publisher+time signal disabled by default — Step 3 benchmark finding."""
    assert DEFAULT_TIME_WINDOW_S is None


# ---------- _tokens ----------


def test_tokens_lowercases_and_drops_stopwords():
    assert _tokens("The Fed Cut Rates") == {"fed", "cut", "rates"}


def test_tokens_strips_punctuation():
    assert _tokens("Iran's nuclear program!") == {"iran", "nuclear", "program"}


def test_tokens_drops_short_words():
    """Words shorter than 2 chars dropped (the `a`/`I`/`o` noise floor)."""
    out = _tokens("a I oh me to it")
    assert "a" not in out  # too short
    assert "i" not in out  # too short
    # "oh", "me", "to", "it" are stopwords or 2-char; only "oh" passes length
    # but "oh" isn't in stopwords; verify
    assert "oh" in out


def test_tokens_empty():
    assert _tokens("") == frozenset()
    assert _tokens(None) == frozenset()


# ---------- _jaccard ----------


def test_jaccard_identical_sets():
    assert _jaccard(frozenset({"a", "b"}), frozenset({"a", "b"})) == 1.0


def test_jaccard_disjoint_sets():
    assert _jaccard(frozenset({"a"}), frozenset({"b"})) == 0.0


def test_jaccard_empty_returns_zero():
    assert _jaccard(frozenset(), frozenset({"a"})) == 0.0


def test_jaccard_partial_overlap():
    # |A∩B|=1, |A∪B|=3 → 1/3
    j = _jaccard(frozenset({"a", "b"}), frozenset({"b", "c"}))
    assert abs(j - 1 / 3) < 1e-9


# ---------- cluster_headlines: edge cases ----------


def test_empty_input():
    assert cluster_headlines([]) == []


def test_single_item_one_cluster():
    out = cluster_headlines([_ci("h1", "Iran tests missile")])
    assert len(out) == 1
    assert out[0].size == 1
    assert out[0].leader.headline_id == "h1"


# ---------- URL signal ----------


def test_identical_urls_merge():
    items = [
        _ci("h1", "Different wording one", url="https://x/article/1"),
        _ci("h2", "Different wording two", url="https://x/article/1"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 1
    assert out[0].size == 2


def test_different_urls_dont_force_merge():
    items = [
        _ci("h1", "Stock market hits high", url="https://x/a"),
        _ci("h2", "Quarterly earnings released", url="https://x/b"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 2


def test_none_url_does_not_merge_via_url():
    """Two items with url=None and disjoint content should NOT merge."""
    items = [
        _ci("h1", "Apple earnings beat estimates", url=None),
        _ci("h2", "Tesla recalls vehicles worldwide", url=None),
    ]
    out = cluster_headlines(items)
    assert len(out) == 2


# ---------- Jaccard signal ----------


def test_high_jaccard_merges():
    items = [
        _ci("h1", "Fed signals rate cuts later this year"),
        _ci("h2", "Fed signals rate cuts coming this year"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 1


def test_low_jaccard_no_merge():
    items = [
        _ci("h1", "Fed signals rate cuts"),
        _ci("h2", "Iran tests new ballistic missile"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 2


def test_jaccard_threshold_boundary_below():
    """Pair with Jaccard just below threshold doesn't merge."""
    # "alpha bravo charlie" vs "alpha foo bar" → intersection={alpha},
    # union={alpha,bravo,charlie,foo,bar} → 1/5 = 0.2 (below 0.4)
    items = [
        _ci("h1", "alpha bravo charlie"),
        _ci("h2", "alpha foo bar"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 2


def test_jaccard_threshold_boundary_above():
    """Pair with Jaccard above threshold merges."""
    # "alpha bravo charlie" vs "alpha bravo delta" → intersection={alpha,bravo},
    # union={alpha,bravo,charlie,delta} → 2/4 = 0.5 (above 0.4)
    items = [
        _ci("h1", "alpha bravo charlie"),
        _ci("h2", "alpha bravo delta"),
    ]
    out = cluster_headlines(items)
    assert len(out) == 1


def test_stopwords_do_not_drive_false_matches():
    """Two unrelated headlines sharing only stopwords must not merge."""
    items = [
        _ci("h1", "The cat sat on the mat"),
        _ci("h2", "The dog ran in the park"),
    ]
    # Content tokens: {cat, sat, mat} vs {dog, ran, park} — disjoint.
    out = cluster_headlines(items)
    assert len(out) == 2


# ---------- publisher+time signal (opt-in) ----------


def test_publisher_time_signal_disabled_by_default():
    """Two CNBC headlines published seconds apart but with disjoint content
    must NOT merge by default — the false-positive surfaced in the Step 3
    benchmark against the smoke-DB corpus."""
    items = [
        _ci("h1", "Treasury yield hits new high after producer prices reading",
            publisher="CNBC", published_at_unix=1000),
        _ci("h2", "Unheralded AI stock can go higher despite recent rally",
            publisher="CNBC", published_at_unix=1060),
    ]
    # No content-token overlap; only signal that could merge them is
    # publisher+time, which is disabled by default.
    out = cluster_headlines(items)
    assert len(out) == 2


def test_publisher_time_signal_opt_in_merges():
    """Caller passes time_window_s: same-pub same-window merges even when
    headline tokens are disjoint (the case the signal exists to catch)."""
    items = [
        _ci("h1", "Apple earnings beat estimates",
            publisher="Reuters", published_at_unix=1000),
        _ci("h2", "Tesla recalls vehicles worldwide",
            publisher="Reuters", published_at_unix=1100),  # 100s apart
    ]
    out = cluster_headlines(items, time_window_s=300)
    assert len(out) == 1


def test_publisher_time_signal_outside_window_no_merge():
    items = [
        _ci("h1", "Apple earnings beat estimates",
            publisher="Reuters", published_at_unix=1000),
        _ci("h2", "Tesla recalls vehicles worldwide",
            publisher="Reuters", published_at_unix=1500),  # 500s apart
    ]
    out = cluster_headlines(items, time_window_s=300)
    assert len(out) == 2


def test_publisher_time_signal_different_publishers_no_merge():
    items = [
        _ci("h1", "Apple earnings beat estimates",
            publisher="Reuters", published_at_unix=1000),
        _ci("h2", "Tesla recalls vehicles worldwide",
            publisher="CNBC", published_at_unix=1100),
    ]
    out = cluster_headlines(items, time_window_s=300)
    assert len(out) == 2


# ---------- transitivity ----------


def test_transitive_merge_via_chain():
    """A-B linked, B-C linked, A-C not directly linked → all three merge."""
    items = [
        _ci("h1", "Iran tests new ballistic missile system"),
        _ci("h2", "Iran tests new missile in Persian Gulf system"),  # high overlap with h1
        _ci("h3", "Iran tests Persian Gulf range new"),  # high overlap with h2, weak with h1
    ]
    out = cluster_headlines(items, jaccard_threshold=0.4)
    # All three should end up in one cluster via transitivity.
    assert len(out) == 1
    assert out[0].size == 3


# ---------- ordering invariants ----------


def test_clusters_returned_newest_first_by_leader():
    items = [
        _ci("old", "Old story", published_at_unix=100),
        _ci("new", "New story unrelated", published_at_unix=200),
    ]
    out = cluster_headlines(items)
    # Two singleton clusters, newest first.
    assert out[0].leader.headline_id == "new"
    assert out[1].leader.headline_id == "old"


def test_members_within_cluster_newest_first():
    """Three same-URL items → one cluster with members in newest-first order."""
    items = [
        _ci("m1", "wire one", url="https://x/a", published_at_unix=100),
        _ci("m3", "wire three", url="https://x/a", published_at_unix=300),
        _ci("m2", "wire two", url="https://x/a", published_at_unix=200),
    ]
    out = cluster_headlines(items)
    assert len(out) == 1
    ids = [m.headline_id for m in out[0].members]
    assert ids == ["m3", "m2", "m1"]


def test_cluster_leader_is_newest_member():
    items = [
        _ci("a", "Same story wire one", url="https://x/y", published_at_unix=100),
        _ci("b", "Same story wire two", url="https://x/y", published_at_unix=200),
    ]
    out = cluster_headlines(items)
    assert out[0].leader.headline_id == "b"


def test_headline_ids_tuple_matches_members():
    items = [
        _ci("a", "Story alpha", url="https://x/1", published_at_unix=10),
        _ci("b", "Story alpha variant", url="https://x/1", published_at_unix=20),
    ]
    out = cluster_headlines(items)
    assert out[0].headline_ids == tuple(m.headline_id for m in out[0].members)


# ---------- determinism ----------


def test_clustering_is_deterministic():
    """Same input → same output across runs."""
    items = [
        _ci("a", "Iran tests new missile system today"),
        _ci("b", "Iran tests new ballistic system", published_at_unix=10),
        _ci("c", "Fed signals rate cuts later this year", published_at_unix=20),
    ]
    out1 = cluster_headlines(items)
    out2 = cluster_headlines(items)
    assert [tuple(c.headline_ids) for c in out1] == [tuple(c.headline_ids) for c in out2]
