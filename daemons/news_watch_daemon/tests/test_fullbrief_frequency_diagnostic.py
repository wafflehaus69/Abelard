"""Frequency diagnostic tests — near-miss table assembly.

Per Full Brief spec Adjustment 1 (Abelard 2026-05-29).
Test coverage: T9 (no fixed cap, filtering), T9b (empty case), T10 (ordering).
"""

from __future__ import annotations

import pytest

from news_watch_daemon.attention.threshold import COLD_START_WINDOW_MIN
from news_watch_daemon.fullbrief.frequency_diagnostic import (
    MIN_FREQ_FLOOR,
    NearMissTerm,
    assemble_near_misses,
)


# ---------- basic filtering ----------


def test_assemble_near_misses_returns_qualifying_terms():
    """Sanity case: 4 terms, 3 qualify (1 below floor)."""
    nm = assemble_near_misses(
        window_counts={"iran": 50, "trump": 30, "small": 3, "other": 8},
        prior_counts={"iran": 25, "trump": 28},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    terms = [n.term for n in nm]
    assert "iran" in terms
    assert "trump" in terms
    assert "other" in terms
    assert "small" not in terms      # below MIN_FREQ_FLOOR
    assert len(nm) == 3


def test_assemble_near_misses_excludes_crossings():
    """Terms that already crossed are excluded — they're surfaced in the
    crossings section, not duplicated in near-misses."""
    nm = assemble_near_misses(
        window_counts={"iran": 50, "secretary": 11},
        prior_counts={"iran": 25, "secretary": 2},
        crossing_terms=["secretary"],
        stopwords=frozenset(),
    )
    terms = [n.term for n in nm]
    assert "secretary" not in terms
    assert "iran" in terms


def test_assemble_near_misses_excludes_crossings_case_insensitive():
    """Crossing exclusion is case-insensitive on both sides."""
    nm = assemble_near_misses(
        window_counts={"SECRETARY": 11},
        prior_counts={"SECRETARY": 2},
        crossing_terms=["secretary"],
        stopwords=frozenset(),
    )
    assert nm == []


def test_assemble_near_misses_excludes_stopwords():
    """Stopwords filtered defensively even though Pass E counter already
    filters them (catches the case where stopwords.yaml gets a new entry
    after the corpus was indexed — until next counter run, stale entries
    would otherwise leak through)."""
    nm = assemble_near_misses(
        window_counts={"iran": 50, "reuters": 26},
        prior_counts={"iran": 25, "reuters": 24},
        crossing_terms=[],
        stopwords=frozenset({"reuters"}),
    )
    terms = [n.term for n in nm]
    assert "reuters" not in terms
    assert "iran" in terms


def test_assemble_near_misses_excludes_stopwords_case_insensitive():
    """Stopword exclusion case-insensitive on both sides (matches
    counter's casing semantic which lowercases before stopword check)."""
    nm = assemble_near_misses(
        window_counts={"Reuters": 26},
        prior_counts={"Reuters": 24},
        crossing_terms=[],
        stopwords=frozenset({"reuters"}),
    )
    assert nm == []


# ---------- T9b: empty case ----------


def test_assemble_near_misses_empty_for_quiet_window():
    """T9b: when no terms meet the floor, return empty list (not error).
    Renderer handles empty case separately."""
    nm = assemble_near_misses(
        window_counts={"a": 2, "b": 1, "c": 4},
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm == []


def test_assemble_near_misses_empty_window_counts():
    """Truly empty input -> empty output, no crash."""
    nm = assemble_near_misses(
        window_counts={},
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm == []


# ---------- reason classification ----------


def test_assemble_near_misses_reason_above_prior_max():
    """Term with freq_window >= COLD_START_WINDOW_MIN (10) -> above_prior_max.
    Empirically matches cycle 2 `iran` 37/75 case."""
    nm = assemble_near_misses(
        window_counts={"iran": 37},
        prior_counts={"iran": 75},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert len(nm) == 1
    assert nm[0].reason_not_crossed == "above_prior_max"


def test_assemble_near_misses_reason_below_window_min():
    """Term with MIN_FREQ_FLOOR <= freq_window < COLD_START_WINDOW_MIN ->
    below_window_min. Empirically matches cycle 1 `air` 11/4 case (well
    actually 11 is >= 10, let me pick 8)."""
    nm = assemble_near_misses(
        window_counts={"drone": 8},
        prior_counts={"drone": 1},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert len(nm) == 1
    assert nm[0].reason_not_crossed == "below_window_min"


def test_assemble_near_misses_reason_boundary_at_window_min():
    """Right at COLD_START_WINDOW_MIN boundary -> above_prior_max (the term
    HIT the window_min, just didn't beat the prior_max — by definition the
    only way to be a near-miss at freq_window >= COLD_START_WINDOW_MIN is
    if prior was too high)."""
    nm = assemble_near_misses(
        window_counts={"x": COLD_START_WINDOW_MIN},
        prior_counts={"x": 50},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm[0].reason_not_crossed == "above_prior_max"


# ---------- delta_ratio ----------


def test_assemble_near_misses_delta_ratio_computed():
    """delta_ratio = freq_window / max(freq_prior, 1).
    Empirically matches cycle 2 `drone` 17/7 (delta=2.43)."""
    nm = assemble_near_misses(
        window_counts={"drone": 17},
        prior_counts={"drone": 7},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm[0].delta_ratio == pytest.approx(17 / 7)


def test_assemble_near_misses_delta_ratio_zero_prior_no_divzero():
    """When prior is 0, delta_ratio uses max(prior, 1) — no division by zero.
    Empirically matches cycle 2 `country` 17/4 case... actually `country`
    had prior=4 not 0. Pick a synthetic case."""
    nm = assemble_near_misses(
        window_counts={"x": 17},
        prior_counts={},   # x missing -> prior_counts.get(x, 0) -> 0
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm[0].delta_ratio == pytest.approx(17.0)   # 17 / max(0, 1) = 17


# ---------- T9: no fixed cap ----------


def test_assemble_near_misses_full_list_no_25_cap():
    """T9: NO fixed 25-row cap per Adjustment 1. 100 qualifying terms -> 100 rows
    in the data layer. Renderer applies its own soft cap separately.

    All freq_window values >= 100 so none get filtered by the floor; this
    pins the "no fixed-25 cap" guarantee, not floor behavior (which is
    covered separately by test_assemble_near_misses_freq_floor_dominates_count)."""
    window_counts = {f"term_{i:03d}": 100 + i for i in range(100)}    # 100..199
    prior_counts = {f"term_{i:03d}": 50 for i in range(100)}
    nm = assemble_near_misses(
        window_counts=window_counts,
        prior_counts=prior_counts,
        crossing_terms=[],
        stopwords=frozenset(),
    )
    # ALL 100 surface, not just top 25
    assert len(nm) == 100


def test_assemble_near_misses_freq_floor_dominates_count():
    """Demonstrate: count of returned rows is determined by floor + exclusions,
    NOT by an arbitrary cap. Test with 40 terms above floor and 10 below."""
    window_counts = {f"high_{i:02d}": 50 - i for i in range(40)}   # all >= 11
    window_counts.update({f"low_{i:02d}": 3 for i in range(10)})    # all below floor 5
    nm = assemble_near_misses(
        window_counts=window_counts,
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert len(nm) == 40
    assert all(n.term.startswith("high_") for n in nm)


# ---------- T10: ordering ----------


def test_assemble_near_misses_sorted_freq_window_descending():
    """T10 part 1: rows sorted by freq_window descending."""
    nm = assemble_near_misses(
        window_counts={"a": 20, "b": 50, "c": 30},
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert nm[0].term == "b"     # 50
    assert nm[1].term == "c"     # 30
    assert nm[2].term == "a"     # 20


def test_assemble_near_misses_ties_broken_by_delta_ratio_descending():
    """T10 part 2: ties on freq_window broken by delta_ratio desc.
    Cycle 2 dynamic: at freq_window=12 cycle 2 had several terms tied
    (israel, near, oil, etc.) and operator wants the highest-delta-ratio
    ones surfaced first."""
    nm = assemble_near_misses(
        window_counts={"a": 20, "b": 20, "c": 30},
        prior_counts={"a": 10, "b": 2, "c": 15},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    # c: freq=30 (highest), wins outright
    # b: freq=20, delta=10.0
    # a: freq=20, delta=2.0
    # Expected order: c, b, a
    assert nm[0].term == "c"
    assert nm[1].term == "b"   # higher delta wins tiebreak
    assert nm[2].term == "a"


# ---------- configurable floor ----------


def test_assemble_near_misses_min_freq_floor_override():
    """min_freq_floor kwarg allows custom threshold (tests + future tuning)."""
    nm = assemble_near_misses(
        window_counts={"a": 8, "b": 12},
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
        min_freq_floor=10,
    )
    terms = [n.term for n in nm]
    assert "a" not in terms     # 8 < 10
    assert "b" in terms          # 12 >= 10


def test_assemble_near_misses_default_floor_is_min_freq_floor():
    """Documented default of MIN_FREQ_FLOOR (5) applied when kwarg omitted."""
    assert MIN_FREQ_FLOOR == 5    # constant pin
    nm = assemble_near_misses(
        window_counts={"a": 4, "b": 5, "c": 6},
        prior_counts={},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    terms = {n.term for n in nm}
    assert "a" not in terms      # 4 < 5
    assert "b" in terms           # 5 >= 5 (inclusive)
    assert "c" in terms           # 6 >= 5


# ---------- shape / frozenness ----------


def test_assemble_near_misses_returns_frozen_dataclass():
    """NearMissTerm is frozen — downstream rendering can rely on
    immutability for sort key stability + hashing."""
    nm = assemble_near_misses(
        window_counts={"x": 10},
        prior_counts={"x": 5},
        crossing_terms=[],
        stopwords=frozenset(),
    )
    assert isinstance(nm[0], NearMissTerm)
    with pytest.raises(Exception):
        nm[0].freq_window = 999   # type: ignore[misc]
