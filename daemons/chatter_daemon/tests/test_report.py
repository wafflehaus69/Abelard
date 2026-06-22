"""PDF report — ranking (peak magnitude beats diversity), quiet-tail, degraded banner,
headline suppression, amplified-first, defensive StockTwits enrichment, valid-PDF smoke."""

from __future__ import annotations

from chatter_daemon.report import (
    _stocktwits_extras,
    _summary_of,
    attention_accelerating,
    attention_amplified,
    degraded_banner,
    headline_sample,
    quiet_watchlist,
    rank_watchlist,
    render_report,
)
from chatter_daemon.schema import (
    AggregatedScanResult,
    AggregatedTicker,
    Anomaly,
    AttentionResult,
    AttentionSignal,
    AttentionSurfaceStatus,
    AttentionTicker,
    CostTelemetry,
    Headline,
    Metrics,
    Sentiment,
    SourceSignal,
    SourceStatus,
)


def _wsig(source, *, count=0, i24=None, i7=None, im=None, headlines=None, state="building"):
    return SourceSignal(
        source=source,
        metrics=Metrics(mention_count=count, interest_24h=i24, interest_7d=i7, interest_monthly=im, headlines=headlines),
        sentiment=Sentiment(method="none"),
        anomaly=Anomaly(kind="trend" if source == "google_trends" else "count", state=state),
    )


def _wt(ticker, sources, diversity):
    return AggregatedTicker(watchlist="barber_growth", ticker=ticker, sources=sources, source_diversity=diversity)


def _wresult(tickers, *, degraded=False, sources=None):
    return AggregatedScanResult(
        scan_id="cd-2026-06-19T14-32-08Z-abcd1234",
        scan_mode="watchlist",
        canonical_ts="2026-06-19T14:32:08Z",
        watchlists=[],
        tickers=tickers,
        sources=sources or [],
        degraded=degraded,
        cost=CostTelemetry(),
    )


def test_ranking_peak_magnitude_beats_diversity():
    nvda = _wt("NVDA", [_wsig("finnhub_news", count=218)], 1)  # loud on ONE source
    weak = _wt("XYZ", [_wsig("smg", count=2), _wsig("stocktwits", count=1), _wsig("google_trends", i24=5.0)], 3)
    ranked = rank_watchlist(_wresult([weak, nvda]))
    assert [t.ticker for t in ranked] == ["NVDA", "XYZ"]  # 218 outranks the weak triple


def test_quiet_tail_is_diversity_zero():
    loud = _wt("NVDA", [_wsig("finnhub_news", count=5)], 1)
    quiet = quiet_watchlist(_wresult([loud, _wt("BAI", [], 0), _wt("DE", [], 0)]))
    assert quiet == ["BAI", "DE"]


def test_degraded_banner():
    sources = [
        SourceStatus(source="stocktwits", ok=True, record_count=5),
        SourceStatus(source="google_trends", ok=False, record_count=0, error="429"),
    ]
    assert degraded_banner(sources, True) == "Partial scan: Google Trends unavailable this run."
    assert degraded_banner(sources, False) is None


def test_headline_suppression():
    heads = [Headline(title=f"headline {i}", url="http://x") for i in range(218)]
    count, titles = headline_sample(_wsig("finnhub_news", count=218, headlines=heads))
    assert count == 218
    assert titles == ["headline 0", "headline 1", "headline 2"]  # top 3 only, never 218


def test_stocktwits_enrichment_defensive():
    # Folds in the moment Order 9 adds these fields; renders nothing until then.
    class _ST:
        source = "stocktwits_trending"
        rank = 3
        trending_score = 88.0
        watchlist_count = 1200
        summary = "Short-squeeze chatter resurging"

    extras = _stocktwits_extras(_ST())
    assert "rank 3" in extras and "score 88.0" in extras and "1200 watchers" in extras
    assert _summary_of(_ST()) == "Short-squeeze chatter resurging"

    class _Plain:
        source = "smg_freq"

    assert _stocktwits_extras(_Plain()) == "" and _summary_of(_Plain()) is None


def test_attention_amplified_and_accelerating():
    amp = AttentionTicker(
        ticker="MU", salience=5, amplified=True, on_watchlists=["barber_growth"],
        signals=[AttentionSignal(source="smg_freq", semantics="24h", count=5,
                                 anomaly=Anomaly(kind="count", state="ok", z=0.5))],
    )
    spiking = AttentionTicker(
        ticker="GME", salience=40, flags=["spike"],
        signals=[AttentionSignal(source="smg_freq", semantics="24h", count=40,
                                 anomaly=Anomaly(kind="count", state="spike", z=5.0))],
    )
    res = AttentionResult(scan_id="cd-x", canonical_ts="2026-06-19T14:32:08Z", tickers=[spiking, amp], cost=CostTelemetry())
    assert [t.ticker for t in attention_amplified(res)] == ["MU"]
    assert [t.ticker for t in attention_accelerating(res)] == ["GME"]


def test_render_watchlist_pdf_is_valid(tmp_path):
    nvda = _wt("NVDA", [_wsig("finnhub_news", count=218, headlines=[Headline(title="NVDA beats", url="http://x")])], 1)
    res = _wresult(
        [nvda, _wt("BAI", [], 0)],
        degraded=True,
        sources=[SourceStatus(source="google_trends", ok=False, record_count=0, error="429")],
    )
    out = tmp_path / "report.pdf"
    render_report(res, out)
    data = out.read_bytes()
    assert data[:5] == b"%PDF-" and len(data) > 1000


def test_render_attention_pdf_is_valid(tmp_path):
    t = AttentionTicker(
        ticker="GME", salience=40, amplified=True, on_watchlists=["barber_growth"], flags=["spike"],
        signals=[AttentionSignal(source="smg_freq", semantics="24h", count=40,
                                 anomaly=Anomaly(kind="count", state="spike", z=5.0))],
    )
    res = AttentionResult(
        scan_id="cd-2026-06-19T14-32-08Z-abcd1234",
        canonical_ts="2026-06-19T14:32:08Z",
        surfaces=[AttentionSurfaceStatus(source="smg_freq", ok=True, candidates=1, floor=3)],
        tickers=[t],
        cost=CostTelemetry(),
    )
    out = tmp_path / "attn.pdf"
    render_report(res, out)
    assert out.read_bytes()[:5] == b"%PDF-"
