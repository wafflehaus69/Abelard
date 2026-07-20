"""Threshold tests — cold-start gate semantics + top-candidates near-miss view."""

from __future__ import annotations

from news_watch_daemon.attention.counter import TermCounts
from news_watch_daemon.attention.threshold import (
    COLD_START_PRIOR_MAX,
    COLD_START_WINDOW_MIN,
    evaluate_threshold,
    top_candidates,
)


def _counts(window: dict[str, int], prior: dict[str, int]) -> TermCounts:
    return TermCounts(
        window_counts=window,
        prior_counts=prior,
        window_since_unix=0, window_until_unix=86400,
        prior_since_unix=-86400, prior_until_unix=0,
    )


# ---------- evaluate_threshold ----------


def test_threshold_fires_at_exactly_window_min_with_zero_prior():
    """count==WINDOW_MIN window, count=0 prior → fires (>= floor AND 0 < 3)."""
    c = _counts({"hormuz": COLD_START_WINDOW_MIN}, {})
    crossings = evaluate_threshold(c)
    assert len(crossings) == 1
    assert crossings[0].term == "hormuz"
    assert crossings[0].window_count == COLD_START_WINDOW_MIN
    assert crossings[0].prior_count == 0


def test_threshold_does_not_fire_at_window_min_minus_one():
    """count=9 window: does NOT fire."""
    c = _counts({"hormuz": COLD_START_WINDOW_MIN - 1}, {})
    assert evaluate_threshold(c) == []


def test_threshold_does_not_fire_at_prior_max():
    """count=10 window, count=3 prior: does NOT fire (3 is NOT < 3)."""
    c = _counts({"hormuz": COLD_START_WINDOW_MIN}, {"hormuz": COLD_START_PRIOR_MAX})
    assert evaluate_threshold(c) == []


def test_threshold_fires_at_prior_max_minus_one():
    """count=10 window, count=2 prior: fires (2 < 3)."""
    c = _counts({"hormuz": COLD_START_WINDOW_MIN}, {"hormuz": COLD_START_PRIOR_MAX - 1})
    crossings = evaluate_threshold(c)
    assert len(crossings) == 1


def test_threshold_does_not_fire_at_prior_above_max():
    """count=20 window, count=10 prior: does NOT fire (prior way above max)."""
    c = _counts({"iran": 20}, {"iran": 10})
    assert evaluate_threshold(c) == []


def test_threshold_handles_term_missing_from_prior_as_zero():
    """A term absent from prior dict is treated as count=0, which passes < 3."""
    c = _counts({"hormuz": COLD_START_WINDOW_MIN}, {"unrelated": 8})
    crossings = evaluate_threshold(c)
    assert len(crossings) == 1
    assert crossings[0].term == "hormuz"
    assert crossings[0].prior_count == 0


def test_threshold_orders_by_window_count_descending():
    # Values expressed relative to the floor so the ordering assertion is
    # robust to threshold tuning (NW-SRC-3 raised the floor 10 -> 12).
    m = COLD_START_WINDOW_MIN
    c = _counts({"alpha": m + 5, "beta": m, "gamma": m + 2}, {})
    crossings = evaluate_threshold(c)
    assert [x.term for x in crossings] == ["alpha", "gamma", "beta"]


def test_threshold_ties_broken_alphabetically():
    m = COLD_START_WINDOW_MIN
    c = _counts({"zebra": m, "alpha": m, "mango": m}, {})
    crossings = evaluate_threshold(c)
    assert [x.term for x in crossings] == ["alpha", "mango", "zebra"]


def test_threshold_returns_empty_when_no_crossings():
    c = _counts({"low": 5, "below": 8}, {})
    assert evaluate_threshold(c) == []


# ---------- top_candidates (near-miss visibility) ----------


def test_top_candidates_surfaces_below_window_min():
    """count=8 (below the 10 floor but above 5) → near-miss candidate."""
    c = _counts({"hormuz": 8}, {})
    cands = top_candidates(c, limit=5)
    assert len(cands) == 1
    assert cands[0].term == "hormuz"
    assert cands[0].reason == "below_window_min"


def test_top_candidates_surfaces_above_prior_max():
    """count=15 window, count=5 prior → near-miss (prior too noisy)."""
    c = _counts({"iran": 15}, {"iran": 5})
    cands = top_candidates(c, limit=5)
    assert len(cands) == 1
    assert cands[0].term == "iran"
    assert cands[0].reason == "above_prior_max"


def test_top_candidates_skips_very_low_counts():
    """count<5 in window is too noisy to be a useful candidate — skipped."""
    c = _counts({"noise": 4, "hormuz": 8}, {})
    cands = top_candidates(c, limit=5)
    assert {x.term for x in cands} == {"hormuz"}


def test_top_candidates_respects_limit():
    c = _counts(
        {f"term_{i:02d}": 7 for i in range(20)},
        {},
    )
    cands = top_candidates(c, limit=5)
    assert len(cands) == 5


def test_top_candidates_orders_by_window_count_descending():
    c = _counts({"low": 6, "mid": 8, "high": 9}, {})
    cands = top_candidates(c, limit=5)
    assert [x.term for x in cands] == ["high", "mid", "low"]


def test_top_candidates_empty_when_no_near_misses():
    """Nothing above the 5-count floor → empty candidate list."""
    c = _counts({"a": 2, "b": 3}, {})
    assert top_candidates(c) == []
