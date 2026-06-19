"""Reddit plugin — shared-matcher reuse, 24h activity windowing, the mention-floor
LLM gate, mocked Haiku stance, cost-telemetry-before-failure, non-ASCII, auth/degrade.

Fully hermetic: PRAW and the Anthropic SDK are both injected as fakes, so neither
`praw` nor `anthropic` need be installed and no network is touched. The gate
assertion (`fake.calls == []` below the floor) is the load-bearing check that the
LLM never runs on noise; the 24h test proves activity outside the window is dropped.
"""

from __future__ import annotations

import json as _json

import pytest

from chatter_daemon.config import (
    _default_common_words_path,
    _default_company_names_path,
    _default_slang_blacklist_path,
)
from chatter_daemon.orchestrator import run_scan
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.reddit import RedditAuthError, RedditPost, RedditSource
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600

WL = WatchlistConfig(
    name="t",
    tickers=[
        {"symbol": "NVDA"},  # name_match:true -> "nvidia" from the shared map
        {"symbol": "DE", "name_match": False},  # ticker-only
        {"symbol": "MU", "name_match": False},  # ticker-only
    ],
)


def _ctx() -> ScanContext:
    return ScanContext(
        scan_mode="watchlist",
        canonical_unix=FIXED,
        canonical_ts=iso_z(FIXED),
        windows=derive_windows(FIXED),
    )


def _post(post_id: str, text: str, *, age: int = 0) -> RedditPost:
    """A Reddit item `age` seconds before the canonical anchor (default: at it)."""
    return RedditPost(post_id=post_id, text=text, created_unix=FIXED - age)


# --- fakes ----------------------------------------------------------------


class FakeReddit:
    """Injected RedditClient — returns canned posts, ignores subreddits/limit."""

    def __init__(self, posts: list[RedditPost]) -> None:
        self._posts = posts

    def posts(self, subreddits, *, limit):
        return list(self._posts)


class _Block:
    type = "text"

    def __init__(self, text: str) -> None:
        self.text = text


class _Usage:
    def __init__(self, i=0, o=0, cr=0, cc=0) -> None:
        self.input_tokens = i
        self.output_tokens = o
        self.cache_read_input_tokens = cr
        self.cache_creation_input_tokens = cc


class _Resp:
    def __init__(self, text, usage, stop_reason="end_turn") -> None:
        self.content = [_Block(text)]
        self.usage = usage
        self.stop_reason = stop_reason


class FakeAnthropic:
    """Injected Anthropic client. Records every call so tests can assert the gate
    (no call below the floor) and one call above it."""

    def __init__(self, response=None, exc=None) -> None:
        self._response = response
        self._exc = exc
        self.calls: list[dict] = []
        self.messages = self  # client.messages.create -> self.create

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self._exc is not None:
            raise self._exc
        return self._response


def _haiku(classifications, *, i=0, o=0, cr=0, cc=0, stop_reason="end_turn") -> _Resp:
    text = _json.dumps({"classifications": classifications})
    return _Resp(text, _Usage(i=i, o=o, cr=cr, cc=cc), stop_reason=stop_reason)


def _reddit(reddit_client, anthropic_client, *, min_mentions) -> RedditSource:
    return RedditSource(
        company_names_path=_default_company_names_path(),
        common_words_path=_default_common_words_path(),
        slang_blacklist_path=_default_slang_blacklist_path(),
        anthropic_api_key="test-key",  # injected client wins; key not consulted
        reddit_client_id=None,
        reddit_client_secret=None,
        reddit_user_agent=None,
        min_mentions=min_mentions,
        reddit_client=reddit_client,
        anthropic_client=anthropic_client,
    )


def _by(res):
    return {r.ticker: r for r in res.records}


# --- shared-matcher reuse -------------------------------------------------


def test_reuses_shared_matcher_provenance():
    # Cashtag + name dedup to 2 distinct posts; bare symbol tagged "symbol".
    posts = [
        _post("p1", "$NVDA looking strong"),
        _post("p2", "nvidia is great"),
        _post("p3", "MU looks cheap here"),
    ]
    fake = FakeAnthropic(response=_haiku([]))
    by = _by(_reddit(FakeReddit(posts), fake, min_mentions=99).fetch(WL, context=_ctx()))
    assert by["NVDA"].metrics.mention_count == 2
    assert set(by["NVDA"].matched_by) == {"cashtag", "name"}
    assert by["MU"].metrics.mention_count == 1
    assert by["MU"].matched_by == ["symbol"]
    assert fake.calls == []  # floor=99: nothing eligible -> no LLM


# --- 24h activity windowing -----------------------------------------------


def test_only_24h_activity_counted():
    posts = [
        _post("recent", "$NVDA today", age=3600),  # 1h ago -> counts
        _post("stale", "$NVDA a week ago", age=8 * 24 * 3600),  # 8d ago -> dropped
    ]
    fake = FakeAnthropic(response=_haiku([]))
    res = _reddit(FakeReddit(posts), fake, min_mentions=99).fetch(WL, context=_ctx())
    assert _by(res)["NVDA"].metrics.mention_count == 1  # only the in-window item
    assert fake.calls == []


# --- the mention-floor LLM gate ------------------------------------------


def test_below_floor_skips_haiku():
    posts = [_post("p1", "$NVDA up"), _post("p2", "$NVDA again")]
    fake = FakeAnthropic(response=_haiku([]))
    res = _reddit(FakeReddit(posts), fake, min_mentions=3).fetch(WL, context=_ctx())
    assert fake.calls == []  # 2 mentions < floor 3 -> Haiku never called
    by = _by(res)
    assert by["NVDA"].metrics.mention_count == 2
    assert by["NVDA"].sentiment.method == "none"
    assert res.cost.haiku_calls == 0


def test_above_floor_haiku_stance():
    posts = [
        _post("p1", "$NVDA to the moon"),
        _post("p2", "nvidia earnings great"),
        _post("p3", "$NVDA is going to dump"),
    ]
    cls = [
        {"post_id": "p1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "p2", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "p3", "ticker": "NVDA", "stance": "bearish"},
    ]
    fake = FakeAnthropic(response=_haiku(cls, i=120, o=30))
    res = _reddit(FakeReddit(posts), fake, min_mentions=2).fetch(WL, context=_ctx())
    assert len(fake.calls) == 1  # one batched call above the floor
    n = _by(res)["NVDA"]
    assert n.metrics.mention_count == 3
    assert n.sentiment.method == "haiku"
    assert (n.sentiment.bullish, n.sentiment.bearish, n.sentiment.neutral) == (2, 1, 0)
    assert "sentiment_classified" in n.flags
    assert _by(res)["MU"].sentiment.method == "none"  # below-floor stays "none"


def test_model_id_pinned_and_passed():
    # The pinned Haiku id flows into the SDK call (verified via claude-api skill).
    posts = [_post(f"p{i}", "$NVDA") for i in range(2)]
    fake = FakeAnthropic(response=_haiku([{"post_id": "p0", "ticker": "NVDA", "stance": "neutral"}]))
    _reddit(FakeReddit(posts), fake, min_mentions=2).fetch(WL, context=_ctx())
    assert fake.calls[0]["model"] == "claude-haiku-4-5"


# --- cost telemetry -------------------------------------------------------


def test_cost_telemetry_captured():
    posts = [_post(f"p{i}", "$NVDA") for i in range(2)]
    cls = [{"post_id": "p0", "ticker": "NVDA", "stance": "bullish"}]
    fake = FakeAnthropic(response=_haiku(cls, i=90, o=20, cr=64))
    res = _reddit(FakeReddit(posts), fake, min_mentions=2).fetch(WL, context=_ctx())
    assert res.cost.haiku_calls == 1
    assert res.cost.input_tokens == 90
    assert res.cost.output_tokens == 20
    assert res.cost.cache_read_input_tokens == 64


def test_cost_survives_haiku_truncation():
    # stop_reason=max_tokens -> classify_stance raises AFTER capturing usage; the
    # plugin degrades but the already-spent cost rides back (doctrine #8).
    posts = [_post(f"p{i}", "$NVDA") for i in range(2)]
    fake = FakeAnthropic(response=_haiku([], i=200, o=10, stop_reason="max_tokens"))
    res = _reddit(FakeReddit(posts), fake, min_mentions=2).fetch(WL, context=_ctx())
    assert res.error is not None and "truncated" in res.error
    assert res.cost.haiku_calls == 1
    assert res.cost.input_tokens == 200  # captured BEFORE the failure
    assert _by(res)["NVDA"].sentiment.method == "none"  # never fabricated


# --- non-ASCII boundary ---------------------------------------------------


def test_nonascii_ticker_extracts():
    posts = [_post("p1", "café season — $NVDA ripping, nvidia \U0001f680")]
    fake = FakeAnthropic(response=_haiku([]))
    res = _reddit(FakeReddit(posts), fake, min_mentions=99).fetch(WL, context=_ctx())
    assert _by(res)["NVDA"].metrics.mention_count == 1  # adjacent to non-ASCII, still extracts
    assert fake.calls == []


# --- auth / degrade paths -------------------------------------------------


def test_missing_reddit_creds_raises():
    # No injected client and no creds -> PrawClient raises at fetch (orchestrator isolates).
    src = RedditSource(
        company_names_path=_default_company_names_path(),
        common_words_path=_default_common_words_path(),
        slang_blacklist_path=_default_slang_blacklist_path(),
        anthropic_api_key=None,
        reddit_client_id=None,
        reddit_client_secret=None,
        reddit_user_agent=None,
    )
    with pytest.raises(RedditAuthError):
        src.fetch(WL, context=_ctx())


def test_missing_anthropic_key_degrades():
    # Reddit works, a ticker crosses the floor, but no Anthropic key/client ->
    # build_anthropic_client raises SentimentError -> degrade, don't crash.
    posts = [_post(f"p{i}", "$NVDA") for i in range(3)]
    src = RedditSource(
        company_names_path=_default_company_names_path(),
        common_words_path=_default_common_words_path(),
        slang_blacklist_path=_default_slang_blacklist_path(),
        anthropic_api_key=None,
        reddit_client_id=None,
        reddit_client_secret=None,
        reddit_user_agent=None,
        min_mentions=2,
        reddit_client=FakeReddit(posts),
    )
    res = src.fetch(WL, context=_ctx())
    assert res.error is not None and "ANTHROPIC" in res.error
    assert res.cost.haiku_calls == 0
    assert _by(res)["NVDA"].sentiment.method == "none"


# --- orchestrator integration --------------------------------------------


def test_run_scan_aggregates_cost():
    posts = [_post(f"p{i}", "$NVDA") for i in range(3)]
    cls = [{"post_id": f"p{i}", "ticker": "NVDA", "stance": "bullish"} for i in range(3)]
    fake = FakeAnthropic(response=_haiku(cls, i=90, o=20))
    src = _reddit(FakeReddit(posts), fake, min_mentions=2)
    env = run_scan([WL], sources=[src], now=FIXED)
    assert env.degraded is False
    assert env.sources[0].source == "reddit" and env.sources[0].ok is True
    assert env.cost.haiku_calls == 1
    assert env.cost.input_tokens == 90  # folded into the envelope before return
