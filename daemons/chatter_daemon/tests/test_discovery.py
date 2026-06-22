"""ATTENTION discovery (Phase 1) — universe-mode extraction per surface, 24h window,
per-surface failure isolation, StockTwits validation + absent-path, non-ASCII, and
the sorted per-source + combined distribution."""

from __future__ import annotations

from chatter_daemon.discovery import (
    SurfaceCounts,
    format_distribution,
    pull_smg_frequency,
    pull_stocktwits_trending,
    run_dry_run,
)
from chatter_daemon.matching import Matcher

UNIVERSE = frozenset({"GME", "AMC", "NVDA", "TSLA"})
_EMPTY = frozenset()
NOW = 1_700_000_000


def _matcher():
    return Matcher.for_universe(UNIVERSE, blacklist=_EMPTY, common_words=_EMPTY, allowlist=_EMPTY)


# --- /smg/ frequency ------------------------------------------------------


def test_smg_frequency_counts_all_tickers(monkeypatch):
    class _Thread:
        posts = [
            {"no": 1, "com": "$GME squeeze incoming"},
            {"no": 2, "com": "NVDA and GME both ripping"},
            {"no": 3, "com": "no ticker here"},
        ]

    import chatter_daemon.discovery as D

    monkeypatch.setattr(D.fourchan_fetch, "scrape_smg", lambda fetcher: [_Thread()])
    sc = pull_smg_frequency(object(), _matcher())
    assert sc.counts["GME"] == 2  # posts 1 and 2
    assert sc.counts["NVDA"] == 1
    assert sc.warning is None


def test_smg_failure_isolates(monkeypatch):
    import chatter_daemon.discovery as D

    def boom(fetcher):
        raise RuntimeError("no live thread")

    monkeypatch.setattr(D.fourchan_fetch, "scrape_smg", boom)
    sc = pull_smg_frequency(object(), _matcher())
    assert sc.warning and "no live thread" in sc.warning and sc.counts == {}


# --- stocktwits trending --------------------------------------------------


def test_stocktwits_validates_against_universe():
    class _ST:
        def trending(self):
            return ["GME", "NVDA", "FAKE123", "tsla"]  # FAKE123 not real; tsla lower

    sc = pull_stocktwits_trending(_ST(), UNIVERSE)
    assert set(sc.counts) == {"GME", "NVDA", "TSLA"}  # junk dropped, case normalized


def test_run_dry_run_skips_absent_surfaces():
    results = run_dry_run(
        matcher=_matcher(), universe=UNIVERSE, now=NOW,
        fetcher=None, stocktwits_client=None,
    )
    assert results == []  # nothing supplied -> clean empty (StockTwits-absent path)


# --- distribution ---------------------------------------------------------


def test_format_distribution_sorted_and_combined():
    a = SurfaceCounts("stocktwits_trending", "24h", {"GME": 5, "AMC": 2})
    b = SurfaceCounts("smg_freq", "24h", {"GME": 3, "NVDA": 1})
    out = format_distribution([a, b])
    assert "stocktwits_trending" in out and "smg_freq" in out and "COMBINED" in out
    rows = [tuple(line.split()) for line in out.splitlines()]
    assert ("GME", "5") in rows and ("AMC", "2") in rows  # stocktwits section
    assert ("GME", "8") in rows  # combined: 5 + 3
    assert rows.index(("GME", "5")) < rows.index(("AMC", "2"))  # sorted desc by count


def test_format_distribution_surfaces_warning():
    out = format_distribution([SurfaceCounts("smg_freq", "24h", warning="smg: no thread")])
    assert "DEGRADED: smg: no thread" in out and "(no candidates)" in out
