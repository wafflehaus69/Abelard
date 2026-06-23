"""StockTwits source (Order 9) — trending parse, symbol-stream parse, native-tag
extraction, and the StockTwitsSource native+Haiku sentiment blend. Degrade-clean CF
mapping throughout. No live network: a fake HttpClient drives `get_json`, a fake client
drives `symbol_stream`, and a fake Anthropic client drives the Haiku path.
"""

from __future__ import annotations

import json

import pytest

from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.stocktwits import (
    StockTwitsBlocked,
    StockTwitsClient,
    StockTwitsSource,
    TRENDING_URL,
    native_tag,
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
    """Fake StockTwitsClient: per-symbol canned streams; a symbol mapped to "BLOCK"
    raises StockTwitsBlocked (the CF-walled-ticker path)."""

    def __init__(self, streams):
        self._streams = streams
        self.calls: list[str] = []

    def symbol_stream(self, symbol):
        self.calls.append(symbol)
        s = self._streams.get(symbol)
        if s == "BLOCK":
            raise StockTwitsBlocked(f"CF wall {symbol}")
        return list(s or [])


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


# --- StockTwitsSource native + Haiku blend ---------------------------------


def test_source_native_only_below_floor():
    # 2 messages < floor 3 -> Haiku never runs; the native tally is the primary read.
    streams = {"NVDA": [_msg(1, "to the moon", "Bullish"), _msg(2, "meh")], "AMC": []}
    src = StockTwitsSource(sentiment_min_mentions=3, client=_FakeST(streams), sleep=lambda: None)
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.metrics.mention_count == 2 and nvda.sentiment.method == "native"
    assert nvda.sentiment.bullish == 1 and nvda.sentiment.bearish == 0
    assert nvda.sentiment.native.tagged == 1 and nvda.sentiment.native.messages == 2
    assert res.cost.haiku_calls == 0  # gate held


def test_source_haiku_above_floor_carries_native_distinct():
    streams = {
        "NVDA": [
            _msg(1, "calls", "Bullish"), _msg(2, "long", "Bullish"),
            _msg(3, "puts", "Bearish"), _msg(4, "just holding"),
        ],
        "AMC": [],
    }
    classifications = {"classifications": [
        {"post_id": "1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "2", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "3", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "4", "ticker": "NVDA", "stance": "bearish"},
    ]}
    src = StockTwitsSource(
        sentiment_min_mentions=3,
        client=_FakeST(streams),
        anthropic_client=_FakeAnthropic(text=json.dumps(classifications)),
        sleep=lambda: None,
    )
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    # Haiku is the PRIMARY read (full coverage); the native tally rides alongside, distinct.
    assert nvda.sentiment.method == "haiku"
    assert (nvda.sentiment.bullish, nvda.sentiment.bearish, nvda.sentiment.neutral) == (3, 1, 0)
    assert nvda.sentiment.native.bullish == 2 and nvda.sentiment.native.bearish == 1
    assert nvda.sentiment.native.tagged == 3 and nvda.sentiment.native.messages == 4
    assert res.cost.haiku_calls == 1  # NVDA only (AMC empty -> below gate, skipped)


def test_source_degrades_per_ticker_on_block():
    streams = {"NVDA": [_msg(1, "x", "Bullish")], "AMC": "BLOCK"}
    src = StockTwitsSource(sentiment_min_mentions=3, client=_FakeST(streams), sleep=lambda: None)
    res = src.fetch(WL, context=_ctx())
    tickers = {r.ticker for r in res.records}
    assert "NVDA" in tickers and "AMC" not in tickers  # blocked ticker -> no record, others ship
    # a CF block marks the surface degraded (orchestrator flips `degraded` via ok=False)
    assert res.error and "AMC" in res.error and "walled" in res.error


def test_source_haiku_failure_falls_back_to_native():
    streams = {"NVDA": [_msg(i, "body", "Bullish") for i in range(1, 5)], "AMC": []}
    src = StockTwitsSource(
        sentiment_min_mentions=3,
        client=_FakeST(streams),
        anthropic_client=_FakeAnthropic(text="not json at all"),  # parse error -> SentimentError
        sleep=lambda: None,
    )
    res = src.fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "native" and nvda.sentiment.bullish == 4  # fell back to native
    assert any("NVDA" in w and "Haiku failed" in w for w in res.warnings)


def test_source_courtesy_sleep_between_tickers():
    n = {"sleeps": 0}

    def _sleep():
        n["sleeps"] += 1

    StockTwitsSource(client=_FakeST({"NVDA": [], "AMC": []}), sleep=_sleep).fetch(WL, context=_ctx())
    assert n["sleeps"] == 1  # 2 tickers -> 1 inter-ticker delay (none before the first)
