"""Convergence-grouping tests — same-event crossings collapse, distinct stay."""

from __future__ import annotations

from news_watch_daemon.attention.event_group import (
    MIN_JACCARD,
    group_convergent_crossings,
)
from news_watch_daemon.attention.threshold import CrossingTerm


def _c(term: str, window: int, prior: int = 0) -> CrossingTerm:
    return CrossingTerm(term=term, window_count=window, prior_count=prior)


def _ids(n_start: int, n_end: int) -> set[str]:
    return {f"h{i}" for i in range(n_start, n_end)}


def test_identical_clusters_merge():
    # The Iran case: three phrases, one 16-headline story -> one group.
    crossings = [_c("attacks iran", 17), _c("hormuz tensions", 16), _c("tensions rise", 16)]
    ids = {
        "attacks iran": _ids(0, 16),
        "hormuz tensions": _ids(0, 16),
        "tensions rise": _ids(0, 16),
    }
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 1
    assert [c.term for c in groups[0]][0] == "attacks iran"   # rep = highest window
    assert {c.term for c in groups[0]} == {"attacks iran", "hormuz tensions", "tensions rise"}


def test_distinct_clusters_stay_separate():
    crossings = [_c("stablecoin", 28), _c("nvidia", 23), _c("dtcc", 22)]
    ids = {"stablecoin": _ids(0, 28), "nvidia": _ids(28, 51), "dtcc": _ids(51, 73)}
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 3


def test_broad_and_narrow_do_not_merge():
    # 'new york' (31, many NY stories) vs 'center moratorium' (20, subset).
    # Jaccard ~0.16 -> must NOT merge (distinct-scope signals).
    crossings = [_c("new york", 32), _c("center moratorium", 20)]
    ids = {"new york": _ids(0, 31), "center moratorium": _ids(24, 44)}  # share 7
    j = len(ids["new york"] & ids["center moratorium"]) / len(ids["new york"] | ids["center moratorium"])
    assert j < MIN_JACCARD
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 2


def test_transitive_grouping():
    # A~B (0.8) and B~C (0.8) but A~C lower — union-find still groups all three.
    crossings = [_c("a", 20), _c("b", 18), _c("c", 16)]
    ids = {"a": _ids(0, 10), "b": _ids(1, 11), "c": _ids(2, 12)}
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 1
    assert groups[0][0].term == "a"   # representative = highest window_count


def test_representative_is_highest_window_count():
    crossings = [_c("low", 16), _c("high", 30), _c("mid", 20)]
    ids = {t: _ids(0, 10) for t in ("low", "high", "mid")}   # all identical -> one group
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 1
    assert groups[0][0].term == "high"
    assert groups[0][0].window_count == 30


def test_empty_cluster_is_singleton():
    crossings = [_c("real", 20), _c("ghost", 16)]
    ids = {"real": _ids(0, 20), "ghost": set()}
    groups = group_convergent_crossings(crossings, ids)
    assert len(groups) == 2


def test_no_convergence_preserves_order():
    # No merges -> groups reproduce evaluate_threshold's window-count-desc order.
    crossings = [_c("a", 30), _c("b", 25), _c("c", 20)]
    ids = {"a": _ids(0, 10), "b": _ids(10, 20), "c": _ids(20, 30)}
    groups = group_convergent_crossings(crossings, ids)
    assert [g[0].term for g in groups] == ["a", "b", "c"]


def test_empty_input():
    assert group_convergent_crossings([], {}) == []
