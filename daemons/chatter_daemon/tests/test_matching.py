"""Shared matcher — the name_match gating invariant /smg/ and Reddit both depend on.

The load-bearing one (Fix 2 makes it newly necessary): a `name_match:false` ticker
that now carries a populated `names[]` (so Google Trends can query its company name)
must NOT match by that name in FREE TEXT. Trends and the matcher read `names[]`
through different doors; this proves they stay decoupled.
"""

from __future__ import annotations

from chatter_daemon.matching import Matcher, audit_name_match, build_name_map
from chatter_daemon.watchlist import WatchlistConfig

_EMPTY = frozenset()


def _matcher(wl, *, shared_map=None):
    return Matcher.for_watchlist(
        wl,
        shared_map=shared_map or {},
        blacklist=_EMPTY,
        common_words=_EMPTY,
        allowlist=_EMPTY,
    )


def test_name_match_false_with_names_does_not_match_by_name():
    # CAT carries names=["Caterpillar"] for Trends, but name_match:false ->
    # the company name must NOT leak into free-text matching.
    wl = WatchlistConfig(
        name="t",
        tickers=[{"symbol": "CAT", "name_match": False, "names": ["Caterpillar"]}],
    )
    m = _matcher(wl)
    assert m.match("Caterpillar earnings were strong") == {}  # name must NOT match
    assert "CAT" in m.match("$CAT to the moon")  # cashtag still matches
    assert "CAT" in m.match("CAT looks cheap here")  # bare symbol still matches


def test_build_name_map_excludes_name_match_false():
    wl = WatchlistConfig(
        name="t",
        tickers=[
            {"symbol": "NVDA", "names": ["Nvidia"]},  # name_match:true -> in the map
            {"symbol": "CAT", "name_match": False, "names": ["Caterpillar"]},  # excluded
        ],
    )
    name_map = build_name_map(wl, {})
    assert name_map.get("nvidia") == "NVDA"
    assert "caterpillar" not in name_map  # name_match:false names never enter the matcher


def test_name_match_true_still_matches_by_name():
    wl = WatchlistConfig(name="t", tickers=[{"symbol": "NVDA", "names": ["Nvidia"]}])
    m = _matcher(wl)
    assert "NVDA" in m.match("nvidia keeps ripping")  # name_match:true -> name matches


def test_audit_only_covers_name_match_true():
    wl = WatchlistConfig(
        name="t",
        tickers=[
            {"symbol": "NVDA", "names": ["Nvidia"]},
            {"symbol": "CAT", "name_match": False, "names": ["Caterpillar"]},
        ],
    )
    audit = audit_name_match(wl, {})
    assert audit.get("NVDA") == ["nvidia"]
    assert "CAT" not in audit  # name_match:false tickers aren't audited


def test_for_universe_proposes_any_symbol_in_set():
    # ATTENTION universe-mode: any ticker-shaped token in the validation set counts;
    # cashtag + bare symbol both resolve, names never do (no resolver).
    m = Matcher.for_universe(
        {"GME", "AMC", "NVDA"}, blacklist=_EMPTY, common_words=_EMPTY, allowlist=_EMPTY
    )
    hits = m.match("$GME squeeze and AMC too")
    assert "GME" in hits and "AMC" in hits
    assert "name" not in hits.get("GME", set())  # no name kind in universe mode
    assert m.match("TSLA mooning") == {}  # not in this universe -> not proposed
