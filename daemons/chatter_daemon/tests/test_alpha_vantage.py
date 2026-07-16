"""Alpha Vantage NEWS_SENTIMENT source (CH-SRC-1) — the mandatory in-band error guard, the
relevance trust gate, relevance-weighted per-ticker aggregation, and the one-call-per-scan shape.
Hermetic — a fake get_json client, no network, no key."""

from __future__ import annotations

import pytest

from chatter_daemon.sources.alpha_vantage import AlphaVantageError, AlphaVantageSource, _band
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600
WL = WatchlistConfig(name="w", tickers=[{"symbol": "NVDA"}, {"symbol": "MU"}])


def _ctx():
    return ScanContext(scan_mode="watchlist", canonical_unix=FIXED, canonical_ts=iso_z(FIXED),
                       windows=derive_windows(FIXED))


class _FakeClient:
    def __init__(self, payload):
        self._payload = payload
        self.calls: list = []

    def get_json(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append(params)
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _ts(ticker, relevance, sentiment):
    return {"ticker": ticker, "relevance_score": str(relevance), "ticker_sentiment_score": str(sentiment)}


def test_aggregates_relevance_weighted_per_ticker():
    payload = {"feed": [
        {"ticker_sentiment": [_ts("NVDA", 0.9, 0.5)]},
        {"ticker_sentiment": [_ts("NVDA", 0.3, -0.1)]},
    ]}
    res = AlphaVantageSource(api_key="k", client=_FakeClient(payload)).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    ns = by["NVDA"].news_sentiment
    assert ns.articles == 2
    assert abs(ns.score - 0.35) < 1e-6  # (0.9*0.5 + 0.3*-0.1) / (0.9+0.3) = 0.35
    assert ns.label == "Bullish"
    assert by["NVDA"].metrics.mention_count == 2
    assert by["NVDA"].source == "alpha_vantage" and by["NVDA"].sentiment.method == "none"
    assert by["MU"].news_sentiment is None and by["MU"].metrics.mention_count == 0  # honest zero


def test_in_band_error_guard_raises_on_every_error_key():
    for key in ("Note", "Information", "Error Message"):
        with pytest.raises(AlphaVantageError) as ei:
            AlphaVantageSource(api_key="k", client=_FakeClient({key: "rate limited"})).fetch(WL, context=_ctx())
        assert key in str(ei.value)  # loud, names the key — never a silent fake-empty


def test_relevance_gate_drops_low_relevance_mentions():
    payload = {"feed": [{"ticker_sentiment": [
        _ts("NVDA", 0.5, 0.3),    # kept
        _ts("NVDA", 0.02, 0.9),   # gated (below 0.1)
    ]}]}
    res = AlphaVantageSource(api_key="k", relevance_min=0.1, client=_FakeClient(payload)).fetch(WL, context=_ctx())
    ns = {r.ticker: r for r in res.records}["NVDA"].news_sentiment
    assert ns.articles == 1 and abs(ns.score - 0.3) < 1e-6
    assert any("below relevance" in w for w in res.warnings)


def test_missing_key_raises():
    with pytest.raises(AlphaVantageError):
        AlphaVantageSource(api_key=None, client=_FakeClient({"feed": []})).fetch(WL, context=_ctx())


def test_feed_not_a_list_raises():
    with pytest.raises(AlphaVantageError):
        AlphaVantageSource(api_key="k", client=_FakeClient({"feed": "nope"})).fetch(WL, context=_ctx())


def test_ignores_unwatched_tickers():
    payload = {"feed": [{"ticker_sentiment": [_ts("TSLA", 0.9, 0.5)]}]}  # TSLA not in WL
    res = AlphaVantageSource(api_key="k", client=_FakeClient(payload)).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert set(by) == {"NVDA", "MU"} and by["NVDA"].news_sentiment is None


def test_band_thresholds():
    assert _band(-0.5) == "Bearish" and _band(-0.2) == "Somewhat-Bearish"
    assert _band(0.0) == "Neutral" and _band(0.2) == "Somewhat-Bullish" and _band(0.5) == "Bullish"


def test_one_call_covers_whole_watchlist():
    fake = _FakeClient({"feed": []})
    AlphaVantageSource(api_key="k", client=fake).fetch(WL, context=_ctx())
    assert len(fake.calls) == 1
    p = fake.calls[0]
    assert p["function"] == "NEWS_SENTIMENT" and p["tickers"] == "NVDA,MU" and p["apikey"] == "k"
