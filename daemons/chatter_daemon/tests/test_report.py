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
    NativeStance,
    Sentiment,
    SourceSignal,
    SourceStatus,
)


def _wsig(source, *, count=0, i24=None, i7=None, im=None, headlines=None, state="building", sentiment=None):
    return SourceSignal(
        source=source,
        metrics=Metrics(mention_count=count, interest_24h=i24, interest_7d=i7, interest_monthly=im, headlines=headlines),
        sentiment=sentiment or Sentiment(method="none"),
        anomaly=Anomaly(kind="trend" if source == "google_trends" else "count", state=state),
    )


def _sent(method="none", b=0, r=0, n=0, native=None):
    nat = NativeStance(**native) if native else None
    return Sentiment(method=method, bullish=b, bearish=r, neutral=n, native=nat)


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


# --- Order 11: stance, divergence, page-size de-noise, headline relevance, UTF-8 ----


def test_stance_in_digest_and_detail():
    from chatter_daemon.report import _watchlist_block, _watchlist_digest

    mu = _wt("MU", [
        _wsig("finnhub_news", count=40, headlines=[Headline(title="Micron MU falls", url="http://x")]),
        _wsig("smg", count=69, sentiment=_sent("haiku", 23, 29, 17)),
        _wsig("stocktwits", count=30, sentiment=_sent("haiku", 11, 8, 11, native={"bullish": 9, "bearish": 3, "tagged": 12, "messages": 30})),
    ], 3)
    digest = " ".join(_watchlist_digest(_wresult([mu])))
    assert "StockTwits 11/8 bull (native 9/3)" in digest  # direction-first, native carried
    assert "/smg/ 23/29 bear" in digest
    block = _watchlist_block(mu)
    assert "StockTwits: Haiku 11 bull / 8 bear / 11 neutral" in block
    assert "30 msgs classified" in block
    assert "/smg/: Haiku 23 bull / 29 bear / 17 neutral" in block


def test_no_stance_for_method_none():
    from chatter_daemon.report import _stance_detail, _stance_phrase

    fin = _wsig("finnhub_news", count=10, headlines=[Headline(title="x", url="http://x")])
    trend = _wsig("google_trends", i24=50.0)
    assert _stance_phrase(fin) is None and _stance_detail(fin) is None  # never fabricated
    assert _stance_phrase(trend) is None and _stance_detail(trend) is None


def test_divergence_callout_fires_on_disagreement():
    from chatter_daemon.report import _divergence, _watchlist_digest

    mu = _wt("MU", [
        _wsig("smg", count=10, sentiment=_sent("haiku", 2, 9, 1)),                  # bear
        _wsig("stocktwits", count=30, sentiment=_sent("haiku", 12, 3, 15, native={"bullish": 5, "bearish": 1, "tagged": 6, "messages": 30})),  # bull
    ], 2)
    assert _divergence(mu) == {"smg": "bear", "stocktwits": "bull"}
    digest = " ".join(_watchlist_digest(_wresult([mu])))
    assert "Sources split" in digest and "MU (/smg/ bear vs StockTwits bull)" in digest


def test_divergence_silent_when_sources_agree():
    from chatter_daemon.report import _divergence, _watchlist_digest

    amd = _wt("AMD", [
        _wsig("smg", count=10, sentiment=_sent("haiku", 8, 1, 1)),                  # bull
        _wsig("stocktwits", count=30, sentiment=_sent("haiku", 9, 2, 19, native={"bullish": 4, "bearish": 1, "tagged": 5, "messages": 30})),  # bull
    ], 2)
    assert _divergence(amd) is None
    assert "Sources split" not in " ".join(_watchlist_digest(_wresult([amd])))


def test_stocktwits_count_not_a_volume_metric():
    from chatter_daemon.report import _watchlist_block, _watchlist_phrase

    stw = _wsig("stocktwits", count=30, sentiment=_sent("haiku", 6, 10, 14, native={"bullish": 0, "bearish": 1, "tagged": 1, "messages": 30}))
    assert _watchlist_phrase(stw, "AAPL") is None  # no "30 mentions" volume phrase
    block = _watchlist_block(_wt("AAPL", [stw], 1))
    assert "30 StockTwits mentions" not in block and "30 mentions" not in block
    assert "StockTwits: Haiku 6 bull / 10 bear / 14 neutral" in block  # stance instead
    assert "30 msgs classified" in block  # page size labeled honestly, not as volume


def test_stocktwits_constant_count_does_not_set_rank():
    from chatter_daemon.report import watchlist_peak

    # a name loud only on StockTwits' constant 30 must NOT outrank a real 12-headline name
    only_stw = _wt("PNW", [_wsig("stocktwits", count=30, sentiment=_sent("haiku", 5, 1, 24))], 1)
    real = _wt("JPM", [_wsig("finnhub_news", count=12)], 1)
    ranked = rank_watchlist(_wresult([only_stw, real]))
    assert watchlist_peak(only_stw) == 0.0  # StockTwits page size excluded from peak
    assert [t.ticker for t in ranked] == ["JPM", "PNW"]


def test_headline_relevance_samples_own_news():
    heads = [
        Headline(title="Alphabet joins the Dow", url="http://x"),       # cross-tag noise
        Headline(title="Why NVDA stock is up today", url="http://x"),   # symbol-relevant
        Headline(title="$10 trillion company coming", url="http://x"),  # noise
        Headline(title="Nvidia ships new GPU", url="http://x"),         # name-alias relevant
    ]
    count, titles = headline_sample(_wsig("finnhub_news", count=4, headlines=heads), "NVDA", ["nvidia"])
    assert count == 4
    assert titles == ["Why NVDA stock is up today", "Nvidia ships new GPU"]


def test_headline_relevance_falls_back_to_feed_order():
    heads = [Headline(title="Broad market selloff", url="http://x"), Headline(title="Fed talks rates", url="http://x")]
    _, titles = headline_sample(_wsig("finnhub_news", count=2, headlines=heads), "NVDA", ["nvidia"])
    assert titles == ["Broad market selloff", "Fed talks rates"]  # none match -> feed order


def test_headline_relevance_symbol_word_boundary_and_raw_untouched():
    from chatter_daemon.report import _title_relevant

    assert _title_relevant("I will DECIDE today", "DE", None) is False  # \bDE\b not in DECIDE
    assert _title_relevant("Deere DE earnings beat", "DE", None) is True
    assert _title_relevant("Duke Energy raises dividend", "DUK", ["duke energy"]) is True
    heads = [Headline(title=f"h{i}", url="http://x") for i in range(5)]
    sig = _wsig("finnhub_news", count=5, headlines=heads)
    headline_sample(sig, "NVDA", ["nvidia"])
    assert len(sig.metrics.headlines) == 5  # the record's raw array is NOT mutated


def test_utf8_headline_roundtrips_persist_load_render(tmp_path):
    from chatter_daemon.persist import load_result, write_result

    smart = "Apple’s “magic” — café"  # smart quotes, em-dash, accent
    nvda = _wt("NVDA", [_wsig("finnhub_news", count=1, headlines=[Headline(title=smart, url="http://x")])], 1)
    path = write_result(tmp_path / "archive", _wresult([nvda]))
    loaded = load_result(path)
    assert loaded.tickers[0].sources[0].metrics.headlines[0].title == smart  # survived round-trip
    out = tmp_path / "r.pdf"
    render_report(loaded, out)
    assert out.read_bytes()[:5] == b"%PDF-"  # rendered without crashing
