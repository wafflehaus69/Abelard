"""StockTwits source (Order 9) — trending parse, symbol-stream parse, native-tag
extraction, and the StockTwitsSource native+Haiku sentiment blend. Degrade-clean CF
mapping throughout. No live network: a fake HttpClient drives `get_json`, a fake client
drives `symbol_stream`, and a fake Anthropic client drives the Haiku path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.stocktwits import (
    SENTIMENT_URL,
    StockTwitsBlocked,
    StockTwitsClient,
    StockTwitsSource,
    TRENDING_URL,
    native_tag,
    parse_sentiment_aggregate,
)
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600

WL = WatchlistConfig(name="w", tickers=[{"symbol": "NVDA"}, {"symbol": "AMC"}])


def _ctx():
    return ScanContext(
        scan_mode="watchlist",
        canonical_unix=FIXED,
        canonical_ts=iso_z(FIXED),
        windows=derive_windows(FIXED),
    )


def _msg(mid, body, tag=None):
    m = {"id": mid, "body": body}
    if tag is not None:
        m["entities"] = {"sentiment": {"basic": tag}}
    return m


class _FakeHttp:
    """HttpClient-shaped stub: returns a fixed payload or raises a fixed exception."""

    def __init__(self, *, payload=None, exc=None):
        self._payload = payload
        self._exc = exc
        self.calls: list[str] = []

    def get_json(self, url, **kwargs):
        self.calls.append(url)
        if self._exc is not None:
            raise self._exc
        return self._payload


class _FakeST:
    """Fake StockTwitsClient: per-symbol canned gateway payloads (sentiment_detail) AND
    streams (symbol_stream). A symbol mapped to "BLOCK", or absent from `gateways`,
    raises StockTwitsBlocked on that path (the per-path degrade)."""

    def __init__(self, *, gateways=None, streams=None):
        self._gateways = gateways or {}
        self._streams = streams or {}
        self.gateway_calls: list[str] = []
        self.stream_calls: list[str] = []

    def sentiment_detail(self, symbol):
        self.gateway_calls.append(symbol)
        g = self._gateways.get(symbol)
        if g is None or g == "BLOCK":
            raise StockTwitsBlocked(f"gateway unavailable {symbol}")
        return g

    def symbol_stream(self, symbol):
        self.stream_calls.append(symbol)
        s = self._streams.get(symbol)
        if s == "BLOCK":
            raise StockTwitsBlocked(f"stream wall {symbol}")
        return list(s or [])


_FIXTURES = Path(__file__).parent / "fixtures"


def _gateway(ticker):
    """A captured live sentiment-API payload (PLTR/BLZE/XOVR) as a gateway fixture."""
    return json.loads((_FIXTURES / f"sentiment_{ticker}.json").read_text(encoding="utf-8"))


class _Block:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _Usage:
    def __init__(self):
        self.input_tokens = 10
        self.output_tokens = 5
        self.cache_read_input_tokens = 0
        self.cache_creation_input_tokens = 0


class _Resp:
    def __init__(self, text, stop):
        self.content = [_Block(text)]
        self.usage = _Usage()
        self.stop_reason = stop


class _FakeMessages:
    def __init__(self, text, stop):
        self._text = text
        self._stop = stop
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _Resp(self._text, self._stop)


class _FakeAnthropic:
    def __init__(self, text='{"classifications":[]}', stop="end_turn"):
        self.messages = _FakeMessages(text, stop)


def test_trending_returns_symbol_dicts():
    http = _FakeHttp(payload={"symbols": [{"symbol": "GME", "trending_score": 9.0}, {"symbol": "AMC"}]})
    client = StockTwitsClient(client=http)
    out = client.trending()
    assert [s["symbol"] for s in out] == ["GME", "AMC"]
    assert http.calls == [TRENDING_URL]  # hit the trending endpoint


def test_trending_filters_non_dict_entries():
    http = _FakeHttp(payload={"symbols": [{"symbol": "GME"}, "junk", 42]})
    out = StockTwitsClient(client=http).trending()
    assert out == [{"symbol": "GME"}]  # only dict entries survive


def test_trending_html_challenge_raises_blocked():
    # A CF wall returns 200 + HTML; the shared client's .json() raises ValueError, which
    # we map to a soft StockTwitsBlocked (degrade-clean), never a crash.
    http = _FakeHttp(exc=ValueError("Expecting value: line 1 column 1 (char 0)"))
    with pytest.raises(StockTwitsBlocked) as ei:
        StockTwitsClient(client=http).trending()
    assert "CF wall or transport" in str(ei.value)


def test_trending_missing_symbols_key_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload={"not_symbols": []})).trending()


def test_trending_empty_list_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload={"symbols": []})).trending()


def test_trending_non_dict_payload_raises_blocked():
    # Parsed JSON that isn't the expected object (e.g. an HTML body that happened to
    # decode to a string) is still a block, not a crash.
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload="<html>challenge</html>")).trending()


# --- symbol stream + native tag -------------------------------------------


def test_symbol_stream_returns_message_dicts():
    http = _FakeHttp(payload={"messages": [{"id": 1, "body": "a"}, {"id": 2, "body": "b"}, "junk"]})
    out = StockTwitsClient(client=http).symbol_stream("NVDA")
    assert [m["id"] for m in out] == [1, 2]  # non-dict entries dropped


def test_symbol_stream_empty_is_honest_zero_not_block():
    out = StockTwitsClient(client=_FakeHttp(payload={"messages": []})).symbol_stream("NVDA")
    assert out == []  # a quiet ticker is a real zero, NOT a CF block (unlike trending)


def test_symbol_stream_missing_messages_key_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload={"nope": 1})).symbol_stream("NVDA")


def test_symbol_stream_html_challenge_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(exc=ValueError("not json"))).symbol_stream("NVDA")


def test_native_tag_reads_basic_and_guards_nulls():
    assert native_tag(_msg(1, "x", "Bullish")) == "bullish"
    assert native_tag(_msg(2, "x", "Bearish")) == "bearish"
    assert native_tag(_msg(3, "x")) is None  # untagged
    assert native_tag({"id": 4, "entities": None}) is None  # null entities
    assert native_tag({"id": 5, "entities": {"sentiment": None}}) is None  # null sentiment
    assert native_tag({"id": 6, "entities": {"sentiment": {"basic": "Neutral"}}}) is None  # not bull/bear


# --- sentiment-API parser (Order 12): now-primary, gap, raw-ignored, 5-band ---------


def test_parser_now_primary_not_24h():
    # BLZE: now EXTREMELY_BULLISH 98; 24h is the stale BEARISH 40. We report NOW.
    a = parse_sentiment_aggregate(_gateway("BLZE"))
    assert a.sent_now_norm == 98 and a.sent_now_label == "EXTREMELY_BULLISH"
    assert a.sent_24h_norm == 40 and a.sent_24h_label == "BEARISH"  # baseline, NOT the headline
    # XOVR: now EXTREMELY_BEARISH 14
    x = parse_sentiment_aggregate(_gateway("XOVR"))
    assert x.sent_now_norm == 14 and x.sent_now_label == "EXTREMELY_BEARISH"


def test_parser_gap_computed():
    assert parse_sentiment_aggregate(_gateway("BLZE")).sent_gap == 58   # 98 - 40 igniting
    assert parse_sentiment_aggregate(_gateway("XOVR")).sent_gap == 4    # 14 - 10 stable
    assert parse_sentiment_aggregate(_gateway("PLTR")).sent_gap == -2   # ~0 steady


def test_parser_ignores_raw_label_uses_normalized():
    # PLTR sentiment raw label = EXTREMELY_BULLISH, labelNormalized = NEUTRAL. Use normalized.
    a = parse_sentiment_aggregate(_gateway("PLTR"))
    assert a.sent_now_label == "NEUTRAL" and a.sent_now_norm == 53  # NOT EXTREMELY_BULLISH


def test_parser_normalized_inversion_volume():
    # Synthetic: raw label EXTREMELY_HIGH but labelNormalized EXTREMELY_LOW -> report LOW.
    raw = {"data": {
        "messageVolume": {"now": {"loaded": True, "value": 12, "valueNormalized": 8,
                                  "label": "EXTREMELY_HIGH", "labelNormalized": "EXTREMELY_LOW", "change": 0}},
        "sentiment": {"now": {"loaded": True, "value": -0.5, "valueNormalized": 20,
                              "label": "BULLISH", "labelNormalized": "BEARISH", "change": 0}},
    }}
    a = parse_sentiment_aggregate(raw)
    assert a.vol_now_norm == 8 and a.sent_now_norm == 20 and a.sent_now_label == "BEARISH"


def test_parser_5_band_scale_preserved():
    # the 0-100 score is carried, not collapsed to 3 buckets: XOVR-14 != a mild-bear-40
    assert parse_sentiment_aggregate(_gateway("XOVR")).sent_now_norm == 14
    assert parse_sentiment_aggregate(_gateway("BLZE")).sent_24h_norm == 40


def test_parser_participation_and_confidence_gate():
    # BLZE: vol high + part high -> high; XOVR: vol low + part high -> quiet (real but quiet);
    # PLTR: vol high + part low -> pump_suspect.
    assert parse_sentiment_aggregate(_gateway("BLZE")).confidence == "high"
    x = parse_sentiment_aggregate(_gateway("XOVR"))
    assert x.participation_norm == 66 and x.confidence == "quiet"
    assert parse_sentiment_aggregate(_gateway("PLTR")).confidence == "pump_suspect"


def test_parser_confidence_quadrants_synthetic():
    def agg(vol, part):
        raw = {"data": {
            "messageVolume": {"now": {"loaded": True, "value": 1, "valueNormalized": vol, "labelNormalized": "X"}},
            "sentiment": {"now": {"loaded": True, "value": 0, "valueNormalized": 50, "labelNormalized": "NEUTRAL"}},
            "timeframes": {"1D": {"participationScore": {"loaded": True, "value": 0.5, "valueNormalized": part, "labelNormalized": "X"}}},
        }}
        return parse_sentiment_aggregate(raw).confidence
    assert agg(10, 80) == "quiet"          # low vol, high part -> real but quiet
    assert agg(10, 20) == "low"            # low vol, low part -> thin noise
    assert agg(90, 80) == "high"           # high vol, high part -> genuine surge
    assert agg(90, 20) == "pump_suspect"   # high vol, low part -> possible pump


def test_parser_real_volume_not_page_size():
    assert parse_sentiment_aggregate(_gateway("BLZE")).vol_now_raw == 41730  # not 30
    assert parse_sentiment_aggregate(_gateway("XOVR")).vol_now_raw == 655


def test_parser_ignores_timeframes_ge_1w():
    a = parse_sentiment_aggregate(_gateway("PLTR"))
    # 1W timeframe sentiment is BEARISH 38; we must NOT have consumed it as the read.
    assert a.sent_now_norm == 53 and a.sent_now_label == "NEUTRAL"  # from `now`, not 1W


def test_parser_guards_unloaded_metric():
    raw = {"data": {
        "sentiment": {"now": {"loaded": False, "valueNormalized": 99, "labelNormalized": "EXTREMELY_BULLISH"},
                      "24h": {"loaded": True, "valueNormalized": 50, "labelNormalized": "NEUTRAL"}},
        "messageVolume": {"now": {"loaded": True, "value": 5, "valueNormalized": 30, "labelNormalized": "LOW"}},
    }}
    a = parse_sentiment_aggregate(raw)
    assert a.sent_now_norm is None and a.sent_24h_norm == 50  # unloaded `now` not consumed
    assert a.sent_gap is None  # no now -> no gap


def test_parser_no_data_returns_none():
    assert parse_sentiment_aggregate({"nope": 1}) is None
    assert parse_sentiment_aggregate({"data": {"timeframes": {}}}) is None  # no now/vol -> degrade


# --- StockTwitsSource: aggregate primary + native fallback + demoted Haiku (Order 12) ---


def test_source_aggregate_primary_zero_haiku():
    # gateway returns BLZE; stream gives native tags; default Haiku OFF -> 0 calls.
    client = _FakeST(
        gateways={"NVDA": _gateway("BLZE")},
        streams={"NVDA": [_msg(1, "calls", "Bullish"), _msg(2, "long", "Bullish")], "AMC": []},
    )
    res = StockTwitsSource(client=client, sleep=lambda: None).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    agg = nvda.st_aggregate
    assert agg is not None
    assert agg.sent_now_norm == 98 and agg.sent_now_label == "EXTREMELY_BULLISH"  # now-primary
    assert agg.sent_24h_norm == 40 and agg.sent_gap == 58                         # baseline + gap
    assert agg.vol_now_raw == 41730 and agg.confidence == "high"
    assert nvda.metrics.mention_count == 41730  # REAL volume, not page-size 30
    assert nvda.sentiment.method == "native"    # native carried; NO Haiku
    assert res.cost.haiku_calls == 0


def test_source_gateway_down_falls_back_to_native():
    # gateway blocked, stream works -> aggregate null but native read still ships (the
    # gateway is NOT a single point of failure); the surface does NOT degrade.
    client = _FakeST(gateways={"NVDA": "BLOCK"}, streams={"NVDA": [_msg(1, "x", "Bullish")], "AMC": []})
    res = StockTwitsSource(client=client, sleep=lambda: None).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.st_aggregate is None
    assert nvda.sentiment.method == "native" and nvda.sentiment.bullish == 1
    assert res.error is None  # a read was produced -> not degraded


def test_source_both_dead_degrades_per_ticker():
    client = _FakeST(gateways={"NVDA": "BLOCK"}, streams={"NVDA": "BLOCK", "AMC": []})
    res = StockTwitsSource(client=client, sleep=lambda: None).fetch(WL, context=_ctx())
    tickers = {r.ticker for r in res.records}
    assert "NVDA" not in tickers and "AMC" in tickers  # both paths dead -> no record
    assert res.error and "NVDA" in res.error and "unavailable" in res.error


def test_source_haiku_off_by_default_even_above_floor():
    streams = {"NVDA": [_msg(i, "body", "Bullish") for i in range(1, 5)], "AMC": []}
    client = _FakeST(gateways={"NVDA": _gateway("PLTR")}, streams=streams)
    res = StockTwitsSource(
        sentiment_min_mentions=3, client=client,
        anthropic_client=_FakeAnthropic(), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert res.cost.haiku_calls == 0 and nvda.sentiment.method == "native"  # DEMOTED


def test_source_haiku_opt_in_fires_aggregate_still_primary():
    streams = {"NVDA": [_msg(1, "calls", "Bullish"), _msg(2, "long", "Bullish"),
                        _msg(3, "puts", "Bearish"), _msg(4, "hold")], "AMC": []}
    classifications = {"classifications": [
        {"post_id": "1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "2", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "3", "ticker": "NVDA", "stance": "bearish"},
        {"post_id": "4", "ticker": "NVDA", "stance": "neutral"},
    ]}
    client = _FakeST(gateways={"NVDA": _gateway("BLZE")}, streams=streams)
    res = StockTwitsSource(
        sentiment_min_mentions=3, haiku_enabled=True, client=client,
        anthropic_client=_FakeAnthropic(text=json.dumps(classifications)), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "haiku"
    assert (nvda.sentiment.bullish, nvda.sentiment.bearish, nvda.sentiment.neutral) == (2, 1, 1)
    assert nvda.sentiment.native is not None and res.cost.haiku_calls == 1
    assert nvda.st_aggregate.sent_now_norm == 98  # aggregate STILL primary alongside Haiku


def test_source_haiku_opt_in_failure_falls_back_to_native():
    streams = {"NVDA": [_msg(i, "body", "Bullish") for i in range(1, 5)], "AMC": []}
    client = _FakeST(gateways={"NVDA": _gateway("PLTR")}, streams=streams)
    res = StockTwitsSource(
        sentiment_min_mentions=3, haiku_enabled=True, client=client,
        anthropic_client=_FakeAnthropic(text="not json"), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "native" and nvda.sentiment.bullish == 4
    assert any("NVDA" in w and "Haiku failed" in w for w in res.warnings)


def test_source_courtesy_sleep_two_calls_per_ticker():
    n = {"sleeps": 0}

    def _sleep():
        n["sleeps"] += 1

    client = _FakeST(streams={"NVDA": [], "AMC": []})  # no gateways -> both raise; streams empty
    StockTwitsSource(client=client, sleep=_sleep).fetch(WL, context=_ctx())
    # 2 tickers x (gateway, stream); sleep before each call except the very first -> 3
    assert n["sleeps"] == 3
