"""sentiment: aggregation math, thresholds, attribution, loud-fail, cost."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from biz_daemon import sentiment
from abelard_common.ticker_noise import TickerHits


class FakeUsage:
    def __init__(self, inp=10, out=5, cr=0, cc=0):
        self.input_tokens = inp
        self.output_tokens = out
        self.cache_read_input_tokens = cr
        self.cache_creation_input_tokens = cc


def _resp(classifications, usage=None, stop_reason="end_turn"):
    block = SimpleNamespace(type="text", text=json.dumps({"classifications": classifications}))
    return SimpleNamespace(content=[block], usage=usage or FakeUsage(), stop_reason=stop_reason)


class FakeClient:
    def __init__(self, response=None, raise_exc=None):
        self._response = response
        self._raise = raise_exc
        self.calls = 0
        self.last_kwargs = None
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        if self._raise is not None:
            raise self._raise
        return self._response


def _table(**by_ticker):
    return {t: TickerHits(ticker=t, post_ids=set(ids)) for t, ids in by_ticker.items()}


# --- aggregation math --------------------------------------------------------


def test_pct_over_directional_neutral_excluded():
    client = FakeClient(_resp([
        {"post_id": 1, "ticker": "GME", "stance": "bullish"},
        {"post_id": 2, "ticker": "GME", "stance": "bullish"},
        {"post_id": 3, "ticker": "GME", "stance": "bearish"},
        {"post_id": 4, "ticker": "GME", "stance": "neutral"},
    ]))
    out = sentiment.run_sentiment(
        attention_tickers={"GME"},
        table=_table(GME=[1, 2, 3, 4]),
        posts_by_no={1: "", 2: "", 3: "", 4: ""},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    s = out.reads["GME"]
    assert s["directional"] == 3
    assert s["neutral"] == 1
    assert s["pct_bullish"] == 67
    assert s["pct_bearish"] == 33
    assert s["read"] == "bullish"


def test_read_threshold_boundary_is_strict():
    # exactly 55% is NOT > 55 -> mixed
    mixed = sentiment._aggregate("X", {"bullish": 11, "bearish": 9}, read_bull_pct=55, read_bear_pct=55)
    assert mixed["pct_bullish"] == 55 and mixed["read"] == "mixed"
    bull = sentiment._aggregate("X", {"bullish": 12, "bearish": 8}, read_bull_pct=55, read_bear_pct=55)
    assert bull["read"] == "bullish"
    bear = sentiment._aggregate("X", {"bullish": 8, "bearish": 12}, read_bull_pct=55, read_bear_pct=55)
    assert bear["read"] == "bearish"


def test_directional_zero_guard():
    s = sentiment._aggregate("X", {"neutral": 3}, read_bull_pct=55, read_bear_pct=55)
    assert s["directional"] == 0
    assert s["pct_bullish"] is None
    assert s["pct_bearish"] is None
    assert s["read"] == "mixed"


def test_multi_ticker_post_attributed_per_ticker():
    client = FakeClient(_resp([
        {"post_id": 1, "ticker": "GME", "stance": "bullish"},
        {"post_id": 1, "ticker": "AMD", "stance": "bearish"},
    ]))
    out = sentiment.run_sentiment(
        attention_tickers={"GME", "AMD"},
        table=_table(GME=[1], AMD=[1]),
        posts_by_no={1: "$GME up, AMD down"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    assert out.reads["GME"]["read"] == "bullish"
    assert out.reads["AMD"]["read"] == "bearish"


# --- loud fail ---------------------------------------------------------------


def test_haiku_error_fails_pass_without_fabricating_neutral():
    client = FakeClient(raise_exc=RuntimeError("503 overloaded"))
    out = sentiment.run_sentiment(
        attention_tickers={"GME"},
        table=_table(GME=[1]),
        posts_by_no={1: "GME"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    assert "error" in out.reads["GME"]
    assert out.reads["GME"].get("read") is None  # no fabricated read
    assert out.errors and "sentiment" in out.errors[0]


# --- request shaping / tail exclusion ---------------------------------------


def test_only_attention_posts_are_sent():
    request_posts, pairs = sentiment._build_request_posts(
        {"GME"}, _table(GME=[1, 2], NTR=[9]), {1: "a", 2: "b", 9: "tail"}
    )
    sent_post_ids = {p["post_id"] for p in request_posts}
    assert sent_post_ids == {1, 2}  # NTR (tail) post 9 not sent
    assert (1, "GME") in pairs and (9, "NTR") not in pairs


def test_empty_attention_makes_no_call():
    client = FakeClient(_resp([]))
    out = sentiment.run_sentiment(
        attention_tickers=set(),
        table={},
        posts_by_no={},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    assert client.calls == 0
    assert out.cost.haiku_calls == 0
    assert out.reads == {}


# --- cost + request contract -------------------------------------------------


class MultiCallClient:
    """Returns a fixed classifications list on every call; counts calls."""

    def __init__(self, classifications):
        self._classifications = classifications
        self.calls = 0
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls += 1
        block = SimpleNamespace(
            type="text", text=json.dumps({"classifications": self._classifications})
        )
        return SimpleNamespace(content=[block], usage=FakeUsage(), stop_reason="end_turn")


# --- truncation cliff --------------------------------------------------------


def test_truncated_response_fails_batch_loud():
    client = FakeClient(_resp(
        [{"post_id": 1, "ticker": "GME", "stance": "bullish"}],
        stop_reason="max_tokens",
    ))
    out = sentiment.run_sentiment(
        attention_tickers={"GME"},
        table=_table(GME=[1]),
        posts_by_no={1: "GME"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    assert client.calls == 1  # call happened (cost recorded)
    assert out.cost.haiku_calls == 1
    assert "error" in out.reads["GME"]
    assert out.reads["GME"].get("read") is None  # no fabricated read
    assert out.errors and "max_tokens" in out.errors[0]


# --- batch chunking ----------------------------------------------------------


def test_attention_set_is_chunked_into_bounded_batches():
    classifications = [
        {"post_id": 1, "ticker": "GME", "stance": "bullish"},
        {"post_id": 2, "ticker": "AMD", "stance": "bearish"},
        {"post_id": 3, "ticker": "NVDA", "stance": "bullish"},
    ]
    client = MultiCallClient(classifications)
    out = sentiment.run_sentiment(
        attention_tickers={"GME", "AMD", "NVDA"},
        table=_table(GME=[1], AMD=[2], NVDA=[3]),
        posts_by_no={1: "x", 2: "y", 3: "z"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
        batch_size=2,
    )
    # 3 tickers, batch_size 2 -> 2 calls
    assert client.calls == 2
    assert set(out.reads) == {"GME", "AMD", "NVDA"}
    assert out.reads["GME"]["read"] == "bullish"
    assert out.reads["AMD"]["read"] == "bearish"
    assert out.reads["NVDA"]["read"] == "bullish"


def test_one_failed_batch_does_not_sink_the_others():
    # first batch (AMD,GME) raises; second batch (NVDA) succeeds
    class FlakyClient:
        def __init__(self):
            self.calls = 0
            self.messages = SimpleNamespace(create=self._create)

        def _create(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("boom")
            block = SimpleNamespace(
                type="text",
                text=json.dumps({"classifications": [
                    {"post_id": 3, "ticker": "NVDA", "stance": "bullish"}
                ]}),
            )
            return SimpleNamespace(content=[block], usage=FakeUsage(), stop_reason="end_turn")

    client = FlakyClient()
    out = sentiment.run_sentiment(
        attention_tickers={"GME", "AMD", "NVDA"},
        table=_table(GME=[1], AMD=[2], NVDA=[3]),
        posts_by_no={1: "x", 2: "y", 3: "z"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
        batch_size=2,
    )
    assert "error" in out.reads["GME"] and "error" in out.reads["AMD"]
    assert out.reads["NVDA"]["read"] == "bullish"  # survived


def test_cost_captured_and_request_is_cached_structured():
    client = FakeClient(_resp(
        [{"post_id": 1, "ticker": "GME", "stance": "bullish"}],
        usage=FakeUsage(inp=120, out=30, cr=80, cc=40),
    ))
    out = sentiment.run_sentiment(
        attention_tickers={"GME"},
        table=_table(GME=[1]),
        posts_by_no={1: "GME"},
        client=client,
        read_bull_pct=55,
        read_bear_pct=55,
    )
    assert out.cost.haiku_calls == 1
    assert out.cost.input_tokens == 120
    assert out.cost.output_tokens == 30
    assert out.cost.cache_read_input_tokens == 80
    assert out.cost.cache_creation_input_tokens == 40

    kwargs = client.last_kwargs
    assert kwargs["model"] == sentiment.HAIKU_MODEL_ID
    assert kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert kwargs["output_config"]["format"]["type"] == "json_schema"
