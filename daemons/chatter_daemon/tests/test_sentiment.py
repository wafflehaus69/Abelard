"""Haiku stance classifier (Order 6/9, repointed to StockTwits message bodies) — the
per-ticker tally, the requested-(post,ticker)-pair guard, truncation-fails-loud, and
immediate cost capture. A fake Anthropic client stands in for the SDK (no network)."""

from __future__ import annotations

import json

import pytest

from chatter_daemon.schema import CostTelemetry
from chatter_daemon.sentiment import SentimentError, classify_stance


class _Block:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _Usage:
    def __init__(self):
        self.input_tokens = 100
        self.output_tokens = 20
        self.cache_read_input_tokens = 7
        self.cache_creation_input_tokens = 0


class _Resp:
    def __init__(self, text, stop="end_turn"):
        self.content = [_Block(text)]
        self.usage = _Usage()
        self.stop_reason = stop


class _FakeMessages:
    def __init__(self, text, stop):
        self._text, self._stop = text, stop

    def create(self, **kwargs):
        return _Resp(self._text, self._stop)


class _FakeAnthropic:
    def __init__(self, text, stop="end_turn"):
        self.messages = _FakeMessages(text, stop)


# A two-message batch; message 2 is about two tickers (the per-ticker attribution case).
_POSTS = [
    {"post_id": "1", "text": "NVDA to the moon", "tickers": ["NVDA"]},
    {"post_id": "2", "text": "GME and NVDA both", "tickers": ["GME", "NVDA"]},
]


def test_classify_tallies_per_ticker():
    text = json.dumps({"classifications": [
        {"post_id": "1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "2", "ticker": "NVDA", "stance": "bearish"},
        {"post_id": "2", "ticker": "GME", "stance": "neutral"},
    ]})
    cost = CostTelemetry()
    out = classify_stance(posts=_POSTS, client=_FakeAnthropic(text), model="m", cost=cost)
    assert out["NVDA"] == {"bullish": 1, "bearish": 1}
    assert out["GME"] == {"neutral": 1}
    # cost captured immediately, before any record is built (doctrine #8)
    assert cost.haiku_calls == 1 and cost.input_tokens == 100 and cost.output_tokens == 20
    assert cost.cache_read_input_tokens == 7


def test_classify_drops_unrequested_pairs():
    text = json.dumps({"classifications": [
        {"post_id": "1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "1", "ticker": "TSLA", "stance": "bullish"},  # TSLA not requested for post 1
        {"post_id": "99", "ticker": "NVDA", "stance": "bearish"},  # post 99 not in the batch
    ]})
    out = classify_stance(posts=_POSTS, client=_FakeAnthropic(text), model="m", cost=CostTelemetry())
    assert out == {"NVDA": {"bullish": 1}}  # only the requested (post_id, ticker) pair survives


def test_classify_truncation_raises_but_keeps_cost():
    cost = CostTelemetry()
    with pytest.raises(SentimentError):
        classify_stance(posts=_POSTS, client=_FakeAnthropic("{}", stop="max_tokens"), model="m", cost=cost)
    assert cost.haiku_calls == 1  # spend captured BEFORE the truncation guard raised


def test_classify_empty_posts_makes_no_call():
    cost = CostTelemetry()
    out = classify_stance(posts=[], client=_FakeAnthropic("irrelevant"), model="m", cost=cost)
    assert out == {} and cost.haiku_calls == 0  # no posts -> no call, no cost
