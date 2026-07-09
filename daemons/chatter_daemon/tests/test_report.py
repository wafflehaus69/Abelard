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
    StockTwitsAggregate,
)


def _wsig(source, *, count=0, i24=None, i7=None, im=None, headlines=None, state="building", sentiment=None, st_aggregate=None, news_summary=None, observed_window=None, twitter_summary=None):
    return SourceSignal(
        source=source,
        metrics=Metrics(mention_count=count, interest_24h=i24, interest_7d=i7, interest_monthly=im, headlines=headlines),
        sentiment=sentiment or Sentiment(method="none"),
        st_aggregate=st_aggregate,
        news_summary=news_summary,
        observed_window=observed_window,
        twitter_summary=twitter_summary,
        anomaly=Anomaly(kind="trend" if source == "google_trends" else "count", state=state),
    )


def _stagg(**kw):
    return StockTwitsAggregate(**kw)


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
    from chatter_daemon.report import _news_lines, _social_band_html, _src, _watchlist_digest

    mu = _wt("MU", [
        _wsig("finnhub_news", count=40, headlines=[Headline(title="Micron MU falls", url="http://x")]),
        _wsig("smg", count=69, sentiment=_sent("haiku", 23, 29, 17)),
        _wsig("stocktwits", st_aggregate=_blze_agg(),
              sentiment=_sent("native", 6, 1, 0, native={"bullish": 6, "bearish": 1, "tagged": 7, "messages": 30})),
    ], 3)
    # digest carries the StockTwits aggregate read + the /smg/ stance
    digest = " ".join(_watchlist_digest(_wresult([mu])))
    assert "StockTwits NOW EXTREMELY_BULLISH 98" in digest
    assert "/smg/ 23/29 bear" in digest
    # detail bands: the /smg/ stance lives in the news band, the aggregate in the social band
    news = " ".join(_news_lines(mu))
    assert "23 bull / 29 bear" in news and "Micron MU falls" in news
    social = _social_band_html(_src(mu, "stocktwits"))
    assert "STOCKTWITS" in social and "EXTREMELY_BULLISH 98" in social


def test_no_stance_for_method_none():
    from chatter_daemon.report import _social_band_html, _stance_phrase

    fin = _wsig("finnhub_news", count=10, headlines=[Headline(title="x", url="http://x")])
    trend = _wsig("google_trends", i24=50.0)
    # Finnhub/Trends carry no stance — absent from the digest stance AND the social band.
    assert _stance_phrase(fin) is None and _stance_phrase(trend) is None
    assert _social_band_html(fin) is None and _social_band_html(trend) is None


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
    from chatter_daemon.report import _meta_bits, _social_band_html, _watchlist_phrase

    agg = _stagg(sent_now_norm=30, sent_now_label="BEARISH", sent_24h_norm=28, sent_gap=2,
                 vol_now_norm=37, vol_now_raw=82000, participation_norm=37, confidence="low")
    stw = _wsig("stocktwits", st_aggregate=agg,
                sentiment=_sent("native", 0, 1, 0, native={"bullish": 0, "bearish": 1, "tagged": 1, "messages": 30}))
    assert _watchlist_phrase(stw, "AAPL") is None       # no count phrase
    assert _meta_bits(_wt("AAPL", [stw], 1)) == []      # no StockTwits header count ("30")
    social = _social_band_html(stw)
    assert "30 StockTwits mentions" not in social and "30 msgs classified" not in social
    assert "vol LOW 82k" in social                      # REAL volume, not page-size 30


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


# --- Order 12: StockTwits aggregate gap-led report ---------------------------------


def _blze_agg():
    return _stagg(sent_now_norm=98, sent_now_label="EXTREMELY_BULLISH", sent_24h_norm=40,
                  sent_24h_label="BEARISH", sent_gap=58, vol_now_norm=98, vol_now_raw=41730,
                  participation_norm=74, confidence="high")


def test_report_aggregate_gap_led_when_igniting():
    from chatter_daemon.report import _social_band_html, _src, _watchlist_digest

    blze = _wt("BLZE", [_wsig("stocktwits", st_aggregate=_blze_agg(),
                              sentiment=_sent("native", 6, 1, 0, native={"bullish": 6, "bearish": 1, "tagged": 7, "messages": 30}))], 1)
    digest = " ".join(_watchlist_digest(_wresult([blze])))
    assert "StockTwits NOW EXTREMELY_BULLISH 98" in digest      # now-primary, not 24h
    assert "gap +58 IGNITING" in digest and "24h 40" in digest  # gap leads a moving name
    assert "vol EXTREMELY_HIGH (41k)" in digest                 # real volume, banded
    assert "participation HIGH 74" in digest
    social = _social_band_html(_src(blze, "stocktwits"))
    assert "native 6/1" in social                               # native companion
    assert "30 msgs classified" not in social                   # page-size phrasing gone


def test_report_steady_when_stable_no_manufactured_spike():
    from chatter_daemon.report import _watchlist_digest

    steady = _stagg(sent_now_norm=50, sent_now_label="NEUTRAL", sent_24h_norm=51, sent_gap=-1,
                    vol_now_norm=45, vol_now_raw=900, participation_norm=42, confidence=None)
    t = _wt("XYZ", [_wsig("stocktwits", st_aggregate=steady)], 1)
    digest = " ".join(_watchlist_digest(_wresult([t])))
    assert "StockTwits NOW NEUTRAL 50 (steady)" in digest
    assert "IGNITING" not in digest and "COOLING" not in digest


def test_report_confidence_flag_surfaced():
    from chatter_daemon.report import _st_phrase

    pump = _stagg(sent_now_norm=80, sent_now_label="BULLISH", sent_24h_norm=78, sent_gap=2,
                  vol_now_norm=90, vol_now_raw=50000, participation_norm=20, confidence="pump_suspect")
    assert "possible pump" in _st_phrase(pump)
    quiet = _stagg(sent_now_norm=14, sent_now_label="EXTREMELY_BEARISH", sent_24h_norm=10, sent_gap=4,
                   vol_now_norm=6, vol_now_raw=655, participation_norm=66, confidence="quiet")
    # quiet = trustworthy small crowd -> NOT flagged low-confidence
    assert "low-confidence" not in _st_phrase(quiet) and "pump" not in _st_phrase(quiet)


def test_report_divergence_uses_aggregate_direction():
    from chatter_daemon.report import _divergence

    bull_agg = _stagg(sent_now_norm=98, sent_now_label="EXTREMELY_BULLISH", sent_24h_norm=40, sent_gap=58)
    mu = _wt("MU", [_wsig("smg", count=10, sentiment=_sent("haiku", 2, 9, 1)),      # bear
                    _wsig("stocktwits", st_aggregate=bull_agg)], 2)                 # aggregate bull
    assert _divergence(mu) == {"smg": "bear", "stocktwits": "bull"}


def test_report_gateway_down_falls_back_to_native_stance():
    from chatter_daemon.report import _watchlist_digest

    # no st_aggregate (gateway down) but native tags present -> bull/bear shown
    nv = _wt("NVDA", [_wsig("stocktwits", sentiment=_sent("native", 7, 1, 0, native={"bullish": 7, "bearish": 1, "tagged": 8, "messages": 30}))], 1)
    digest = " ".join(_watchlist_digest(_wresult([nv])))
    assert "StockTwits 7/1 bull" in digest  # fell back to the native tag read


# --- Order 13: banded rows + Eastern EST/EDT timestamp -----------------------------


def test_eastern_stamp_summer_edt_and_date_rollback():
    from chatter_daemon.report import eastern_stamp
    # 01:13 UTC rolls back to the prior Eastern evening; June -> EDT; MM-DD-YYYY dashes
    assert eastern_stamp("2026-06-24T01:13:36Z") == "06-23-2026 21:13 EDT"


def test_eastern_stamp_winter_est():
    from chatter_daemon.report import eastern_stamp
    assert eastern_stamp("2026-01-15T03:00:00Z") == "01-14-2026 22:00 EST"  # winter -> EST


def test_eastern_stamp_format_and_malformed():
    import re

    from chatter_daemon.report import eastern_stamp
    assert re.fullmatch(r"\d{2}-\d{2}-\d{4} \d{2}:\d{2} E[DS]T", eastern_stamp("2026-07-04T13:05:00Z"))
    assert eastern_stamp("not-a-timestamp") == "not-a-timestamp"  # never crash


def test_report_default_filename_carries_eastern_timestamp():
    from chatter_daemon.report import report_default_filename
    # filesystem-safe: no colon/space; the Eastern timestamp is in the name
    assert report_default_filename("2026-06-24T01:13:36Z") == "chatter-report_06-23-2026_2113_EDT.pdf"


# --- Order 15: named-news summary line in the news band ----------------------------


def test_news_band_shows_summary_and_omits_when_none():
    from chatter_daemon.report import _news_lines

    with_sum = _wt("NVDA", [_wsig("finnhub_news", count=5,
                                  headlines=[Headline(title="NVDA beats", url="http://x")],
                                  news_summary="Strong data-center demand drove the quarter.")], 1)
    lines = " ".join(_news_lines(with_sum))
    assert "summary &middot;" in lines and "Strong data-center demand" in lines
    # None -> the summary line is omitted entirely (no "summary", no empty row, no "null")
    no_sum = _wt("XYZ", [_wsig("finnhub_news", count=5, headlines=[Headline(title="XYZ news", url="http://x")])], 1)
    assert "summary" not in " ".join(_news_lines(no_sum))


def test_report_relevance_filter_is_the_shared_matching_function():
    from chatter_daemon.matching import title_mentions_ticker
    from chatter_daemon.report import _title_relevant
    # one source of truth: the report's filter IS the matcher's, shared with Finnhub's gate
    assert _title_relevant is title_mentions_ticker


def test_meta_bits_compact_counts_no_titles():
    from chatter_daemon.report import _meta_bits

    t = _wt("MU", [
        _wsig("finnhub_news", count=131, headlines=[Headline(title="x", url="http://x")]),
        _wsig("google_trends", i24=48.2),
        _wsig("smg", count=47, sentiment=_sent("haiku", 14, 19, 0)),
        _wsig("stocktwits", st_aggregate=_blze_agg()),
    ], 4)
    assert _meta_bits(t) == ["131 headlines", "interest 48", "47 /smg/"]  # no titles, no ST count


def test_news_lines_omits_absent_smg():
    from chatter_daemon.report import _news_lines

    t = _wt("ABBV", [_wsig("finnhub_news", count=10, headlines=[Headline(title="ABBV deal", url="http://x")])], 1)
    lines = _news_lines(t)
    assert len(lines) == 1 and "ABBV deal" in lines[0] and "/smg/" not in " ".join(lines)


def test_social_band_color_encodes_direction():
    from chatter_daemon.report import _GREEN, _RED, _social_band_html

    bull = _social_band_html(_wsig("stocktwits", st_aggregate=_stagg(
        sent_now_norm=90, sent_now_label="EXTREMELY_BULLISH", sent_24h_norm=88, sent_gap=2,
        vol_now_norm=70, vol_now_raw=5000, participation_norm=70, confidence="high")))
    assert _GREEN in bull and _RED not in bull  # bullish -> green, no red
    bear = _social_band_html(_wsig("stocktwits", st_aggregate=_stagg(
        sent_now_norm=12, sent_now_label="EXTREMELY_BEARISH", sent_24h_norm=14, sent_gap=-2,
        vol_now_norm=60, vol_now_raw=3000, participation_norm=70, confidence="high")))
    assert _RED in bear  # bearish -> red


def test_social_band_pump_flag_danger_tone_on_bullish():
    from chatter_daemon.report import _DANGER, _GREEN, _social_band_html

    agg = _stagg(sent_now_norm=80, sent_now_label="BULLISH", sent_24h_norm=78, sent_gap=2,
                 vol_now_norm=90, vol_now_raw=50000, participation_norm=20, confidence="pump_suspect")
    html = _social_band_html(_wsig("stocktwits", st_aggregate=agg))
    assert _GREEN in html                                # bullish read = green
    assert _DANGER in html and "possible pump" in html   # warning: danger tone AND words (grayscale-safe)


def test_twitter_band_renders_stance_and_summary():
    from chatter_daemon.report import _twitter_band_html
    from chatter_daemon.schema import ObservedWindow

    s = _wsig(
        "twitter", count=39,
        sentiment=_sent(method="haiku", b=17, r=3, n=19),
        observed_window=ObservedWindow(earliest="2026-07-09T02:11:00+00:00", latest="2026-07-09T12:51:00+00:00"),
        twitter_summary="Bulls tout AI demand; a few flag valuation.",
    )
    html = _twitter_band_html(s)
    assert "TWITTER" in html and "39 tweets" in html
    assert "17 bull / 3 bear / 19 neutral" in html                 # stance in the band
    assert "commentary" in html and "Bulls tout AI demand" in html  # the <=3-sentence summary
    assert "02:11-12:51 UTC" in html                                # observed span


def test_twitter_band_none_when_no_signal():
    from chatter_daemon.report import _twitter_band_html
    assert _twitter_band_html(_wsig("twitter", count=0)) is None  # no tweets + no summary -> omitted
    assert _twitter_band_html(None) is None


def test_banded_pdf_renders_valid(tmp_path):
    mu = _wt("MU", [
        _wsig("finnhub_news", count=40, state="spike", headlines=[Headline(title="Micron MU", url="http://x")]),
        _wsig("smg", count=20, sentiment=_sent("haiku", 2, 9, 1)),
        _wsig("stocktwits", st_aggregate=_blze_agg(), state="spike"),
    ], 3)
    res = _wresult([mu, _wt("BAI", [], 0)], degraded=True,
                   sources=[SourceStatus(source="google_trends", ok=False, record_count=0, error="429")])
    out = tmp_path / "banded.pdf"
    render_report(res, out)
    data = out.read_bytes()
    assert data[:5] == b"%PDF-" and len(data) > 1000  # banded layout + degraded banner + quiet tail
