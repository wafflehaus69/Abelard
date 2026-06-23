"""ATTENTION discovery (Phase 1) — universe-mode extraction per surface, 24h window,
per-surface failure isolation, StockTwits rich-parse + null-guard + CF-isolation (no
universe-validation: the top-30 self-gates), and the sorted per-source distribution."""

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


def _trending_item(symbol, score, *, rank=None, watchlist_count=None, sector=None, summary=None):
    item: dict = {"symbol": symbol}
    if score is not None:
        item["trending_score"] = score
    if rank is not None:
        item["rank"] = rank
    if watchlist_count is not None:
        item["watchlist_count"] = watchlist_count
    if sector is not None:
        item["sector"] = sector
    item["trends"] = {"summary": summary} if summary is not None else None
    return item


def test_stocktwits_parses_rich_fields_and_rounds_score():
    class _ST:
        def trending(self):
            return [
                _trending_item("GME", 88.4, rank=1, watchlist_count=123, sector="Consumer", summary="up big"),
                _trending_item("nvda", 12.7, rank=2),  # lower-case symbol, sparse extras
            ]

    sc = pull_stocktwits_trending(_ST())
    # count is the ROUNDED trending_score (the velocity/salience axis); symbol upper-cased
    assert sc.counts == {"GME": 88, "NVDA": 13}
    assert sc.meta["GME"] == {
        "rank": 1, "trending_score": 88.4, "watchlist_count": 123,
        "sector": "Consumer", "summary": "up big",
    }
    assert sc.meta["NVDA"]["trending_score"] == 12.7
    assert sc.meta["NVDA"]["summary"] is None and sc.meta["NVDA"]["sector"] is None
    assert sc.warning is None


def test_stocktwits_skips_universe_validation():
    # cashtag-native + exchange-validated at the source: an off-universe symbol survives.
    class _ST:
        def trending(self):
            return [_trending_item("OBSCURE", 5.0)]

    sc = pull_stocktwits_trending(_ST())
    assert "OBSCURE" in sc.counts  # NOT filtered against any universe


def test_stocktwits_null_trends_does_not_crash():
    # a live `trends: null` crashed a naive .get — the parse must guard it.
    class _ST:
        def trending(self):
            return [{"symbol": "AMC", "trending_score": 7.0, "trends": None}]

    sc = pull_stocktwits_trending(_ST())
    assert sc.counts["AMC"] == 7 and sc.meta["AMC"]["summary"] is None


def test_stocktwits_missing_score_counts_zero():
    class _ST:
        def trending(self):  # ETF-shaped: no trending_score, no fundamentals, null trends
            return [_trending_item("SPY", None)]

    sc = pull_stocktwits_trending(_ST())
    assert sc.counts["SPY"] == 0 and sc.meta["SPY"]["trending_score"] is None


def test_stocktwits_cf_wall_isolates():
    class _ST:
        def trending(self):
            raise RuntimeError("CF wall or transport")

    sc = pull_stocktwits_trending(_ST())
    assert sc.warning and "CF wall" in sc.warning and sc.counts == {} and sc.meta == {}


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
