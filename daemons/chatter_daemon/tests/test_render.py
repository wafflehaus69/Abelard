"""read-chatter render — source-labeled counts, bull/bear, diversity, anomaly tags,
and the degraded/sources/cost/errors surfacing that closes read-brief's gap."""

from __future__ import annotations

from chatter_daemon.render import render_attention, render_chatter
from chatter_daemon.schema import (
    AggregatedScanResult,
    AggregatedTicker,
    Anomaly,
    AttentionResult,
    AttentionSignal,
    AttentionSurfaceStatus,
    AttentionTicker,
    CostTelemetry,
    Metrics,
    Sentiment,
    SourceSignal,
    SourceStatus,
)


def _result(*, degraded=False, sources=None, tickers=None, errors=None, cost=None):
    return AggregatedScanResult(
        scan_id="cd-2026-06-19T14-32-08Z-abcd1234",
        scan_mode="watchlist",
        canonical_ts="2026-06-19T14:32:08Z",
        windows=[],
        tickers=tickers or [],
        sources=sources or [],
        degraded=degraded,
        cost=cost or CostTelemetry(),
        errors=errors or [],
    )


def _sig(source, anomaly, *, mention_count=0, i24=None, i7=None, im=None, sentiment=None, flags=None):
    return SourceSignal(
        source=source,
        metrics=Metrics(mention_count=mention_count, interest_24h=i24, interest_7d=i7, interest_monthly=im),
        sentiment=sentiment or Sentiment(method="none"),
        flags=flags or [],
        anomaly=anomaly,
    )


def test_source_labeled_counts_and_sentiment():
    t = AggregatedTicker(
        watchlist="w",
        ticker="NVDA",
        source_diversity=3,
        sources=[
            _sig("finnhub_news", Anomaly(kind="count", state="ok", z=0.5, observations=8), mention_count=4),
            _sig(
                "reddit",
                Anomaly(kind="count", state="spike", z=3.2, observations=8),
                mention_count=20,
                sentiment=Sentiment(method="haiku", bullish=12, bearish=3, neutral=5),
                flags=["sentiment_classified"],
            ),
            _sig("google_trends", Anomaly(kind="trend", state="spike", ratio=2.0), i24=80.0, i7=40.0, im=30.0),
        ],
    )
    out = render_chatter(_result(tickers=[t]))
    assert "4 headlines" in out  # Finnhub semantics
    assert "20 mentions" in out  # Reddit semantics
    assert "interest 80.0 (7d 40.0 / mo 30.0)" in out  # Trends semantics
    assert "12/3/5" in out  # bull/bear/neutral
    assert "diversity 3" in out
    assert "SPIKE z=3.2" in out and "SPIKE x2.0" in out


def test_surfaces_degraded_and_sources():
    out = render_chatter(
        _result(
            degraded=True,
            sources=[
                SourceStatus(source="reddit", ok=True, record_count=5),
                SourceStatus(source="google_trends", ok=False, record_count=0, error="429"),
            ],
        )
    )
    assert "DEGRADED" in out
    assert "reddit=ok(5)" in out
    assert "google_trends=FAILED(0)" in out


def test_surfaces_cost():
    out = render_chatter(_result(cost=CostTelemetry(haiku_calls=2, input_tokens=120, output_tokens=30)))
    assert "2 haiku calls" in out and "in=120" in out and "out=30" in out


def test_surfaces_rarity_and_building():
    t = AggregatedTicker(
        watchlist="w",
        ticker="AAPL",
        source_diversity=1,
        sources=[
            _sig("smg", Anomaly(kind="count", state="building", observations=2), mention_count=1, flags=["rarity_hit"]),
        ],
    )
    out = render_chatter(_result(tickers=[t]))
    assert "rarity_hit" in out
    assert "building 2 obs" in out


def test_errors_listed():
    out = render_chatter(_result(errors=["google_trends: 429 too many requests"]))
    assert "google_trends: 429 too many requests" in out


def test_etf_no_interest_renders_na():
    t = AggregatedTicker(
        watchlist="w",
        ticker="ITA",
        source_diversity=0,
        sources=[_sig("google_trends", Anomaly(kind="trend", state="none", note="no interest"))],
    )
    out = render_chatter(_result(tickers=[t]))
    assert "interest n/a" in out and "no signal" in out


def test_render_attention_view():
    res = AttentionResult(
        scan_id="cd-2023-11-14T00-00-00Z-deadbeef",
        canonical_ts="2023-11-14T00:00:00Z",
        surfaces=[
            AttentionSurfaceStatus(source="smg_freq", ok=True, candidates=2, floor=3),
            AttentionSurfaceStatus(source="reddit_rising", ok=False, candidates=0, floor=10, warning="praw down"),
        ],
        tickers=[
            AttentionTicker(
                ticker="GME", salience=40, amplified=True, on_watchlists=["barber_growth"],
                flags=["spike", "cold_start"],
                signals=[AttentionSignal(source="smg_freq", semantics="24h", count=40,
                                         anomaly=Anomaly(kind="count", state="spike", z=5.0, observations=8))],
            ),
            AttentionTicker(
                ticker="AMC", salience=4,
                signals=[AttentionSignal(source="smg_freq", semantics="24h", count=4,
                                         anomaly=Anomaly(kind="count", state="ok", z=0.3, observations=8))],
            ),
        ],
        pruned=3,
        degraded=True,
        errors=["reddit_rising: praw down"],
    )
    out = render_attention(res)
    assert "ATTENTION scan" in out
    assert "smg_freq=ok(2, floor 3)" in out and "reddit_rising=FAILED" in out
    assert "DEGRADED" in out and "pruned: 3" in out
    assert "SALIENCE" in out and "GME" in out and "AMC" in out
    assert "SPIKE" in out and "AMPLIFIED barber_growth" in out and "cold-start" in out
    assert "ACCELERATING" in out and "z=5.0" in out
    assert "AMPLIFIED (also on a watchlist)" in out
