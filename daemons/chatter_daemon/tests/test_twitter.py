"""Twitter/X cashtag source (Order 17) — the first subprocess source. Transport (smoke,
argv, JSON/NDJSON parse, degrade-clean blocking, UTF-8 decode) via a fake runner; source
(per-ticker loop, precise-window + min-likes + dedupe filter stack, Haiku-or-none, cost,
observed_window) via a fake TwitterClient. Fully hermetic — no real subprocess/network.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.twitter import (
    TwitterBlocked,
    TwitterClient,
    TwitterCliError,
    TwitterSource,
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


def _iso(unix: int) -> str:
    return iso_z(unix)


def _tweet(tid, text, *, ago_h, likes=10):
    """A tweet dict using the transcription's primary field names (createdAtISO/likes)."""
    return {"id": tid, "text": text, "createdAtISO": _iso(FIXED - int(ago_h * 3600)), "likes": likes}


# --- fakes ---------------------------------------------------------------------------


class _FakeRunner:
    """Injected into a REAL TwitterClient — returns (rc, stdout_bytes) or raises, keyed on
    whether argv is the --version smoke or a search. Records every argv."""

    def __init__(self, *, version=(0, b"twitter 2.1.0\n"), search=(0, b"[]"),
                 version_exc=None, search_exc=None):
        self._version = version
        self._search = search
        self._version_exc = version_exc
        self._search_exc = search_exc
        self.argvs: list[list[str]] = []

    def __call__(self, argv, timeout):
        self.argvs.append(argv)
        if "--version" in argv:
            if self._version_exc is not None:
                raise self._version_exc
            return self._version
        if self._search_exc is not None:
            raise self._search_exc
        return self._search


class _FakeTwitter:
    """Injected into TwitterSource — canned per-symbol search results (or "BLOCK"), and a
    trivial smoke. Records the args each ticker was searched with (to prove `since` is
    derived from the run context, not the clock)."""

    def __init__(self, *, searches=None, smoke_version="fake 1.0", smoke_exc=None):
        self._searches = searches or {}
        self._smoke_version = smoke_version
        self._smoke_exc = smoke_exc
        self.search_calls: list[tuple] = []
        self.smoked = 0

    def smoke(self):
        self.smoked += 1
        if self._smoke_exc is not None:
            raise self._smoke_exc
        return self._smoke_version

    def search(self, cashtag, *, since_iso, max_n, min_likes):
        self.search_calls.append((cashtag, since_iso, max_n, min_likes))
        s = self._searches.get(cashtag)
        if s == "BLOCK":
            raise TwitterBlocked(f"blocked {cashtag}")
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


def _client(**kw):
    return TwitterClient(runner=_FakeRunner(**kw))


# --- transport: startup smoke -------------------------------------------------------


def test_smoke_ok_returns_version():
    assert TwitterClient(runner=_FakeRunner(version=(0, b"twitter 2.1.0\n"))).smoke() == "twitter 2.1.0"


def test_smoke_binary_absent_fails_loud():
    c = TwitterClient(runner=_FakeRunner(version_exc=FileNotFoundError("no twitter")))
    with pytest.raises(TwitterCliError) as ei:
        c.smoke()
    assert "not found" in str(ei.value)  # clear error, NOT a silent skip


def test_smoke_unparseable_version_fails_loud():
    with pytest.raises(TwitterCliError):
        TwitterClient(runner=_FakeRunner(version=(0, b"no-digits-here\n"))).smoke()


def test_smoke_nonzero_exit_fails_loud():
    with pytest.raises(TwitterCliError):
        TwitterClient(runner=_FakeRunner(version=(2, b""))).smoke()


# --- transport: search (argv, parse, degrade-clean, UTF-8) --------------------------


def test_search_builds_argv_and_parses_array():
    tw = json.dumps([_tweet("1", "a", ago_h=1), _tweet("2", "b", ago_h=2)]).encode()
    runner = _FakeRunner(search=(0, tw))
    out = TwitterClient(runner=runner, binary="twitter").search(
        "NVDA", since_iso=iso_z(FIXED - 24 * 3600), max_n=50, min_likes=2
    )
    assert len(out) == 2
    argv = runner.argvs[-1]
    assert argv[:3] == ["twitter", "search", "$NVDA"]  # cashtag query
    assert "-t" in argv and "latest" in argv
    assert "--exclude" in argv and "links" in argv  # CLI-side link filter
    assert "--min-likes" in argv and "2" in argv and "-n" in argv and "50" in argv
    assert "--json" in argv
    since_val = argv[argv.index("--since") + 1]
    assert len(since_val) == 10  # date-granular YYYY-MM-DD


def test_search_nonzero_exit_blocks():
    with pytest.raises(TwitterBlocked):
        _client(search=(1, b"boom")).search("NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1)


def test_search_empty_stdout_blocks():
    with pytest.raises(TwitterBlocked) as ei:
        _client(search=(0, b"   ")).search("NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1)
    assert "empty" in str(ei.value)


def test_search_non_json_blocks():
    with pytest.raises(TwitterBlocked):
        _client(search=(0, b"<html>cf challenge</html>")).search(
            "NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1
        )


def test_search_timeout_blocks():
    runner = _FakeRunner(search_exc=subprocess.TimeoutExpired(cmd=["twitter"], timeout=1.0))
    with pytest.raises(TwitterBlocked):
        TwitterClient(runner=runner).search("NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1)


def test_search_ndjson_stream_parsed():
    nd = (json.dumps(_tweet("1", "a", ago_h=1)) + "\n" + json.dumps(_tweet("2", "b", ago_h=1))).encode()
    out = _client(search=(0, nd)).search("NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1)
    assert len(out) == 2


def test_search_stdout_utf8_decoded():
    # non-ASCII in UTF-8 bytes survives the subprocess-stdout decode obligation + parse.
    tw = json.dumps([_tweet("1", "café déjà — señor NVDA", ago_h=1)], ensure_ascii=False).encode("utf-8")
    out = _client(search=(0, tw)).search("NVDA", since_iso=iso_z(FIXED), max_n=10, min_likes=1)
    assert out[0]["text"] == "café déjà — señor NVDA"


# --- source: loop, filter stack, sentiment, observed_window, degrade, cost ----------


def test_source_happy_path_counts_survivors():
    tweets = [_tweet("1", "calls on nvda", ago_h=1), _tweet("2", "nvda moon", ago_h=2),
              _tweet("3", "buy nvda", ago_h=3)]
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.mention_count == 3
    assert by["NVDA"].source == "twitter" and by["NVDA"].matched_by == ["cashtag"]
    assert by["AMC"].metrics.mention_count == 0  # readable-but-empty -> honest zero
    assert res.error is None and res.cost.haiku_calls == 0  # no key -> no Haiku
    assert fake.smoked == 1  # startup smoke ran once


def test_since_derived_from_context_not_clock():
    fake = _FakeTwitter(searches={"NVDA": [], "AMC": []})
    TwitterSource(client=fake, window_hours=24, sleep=lambda: None).fetch(WL, context=_ctx())
    since_isos = {c[1] for c in fake.search_calls}
    assert since_isos == {iso_z(FIXED - 24 * 3600)}  # WINDOW_HOURS back from canonical_unix


def test_precise_window_drops_out_of_window_tweet():
    tweets = [_tweet("1", "in window", ago_h=1), _tweet("2", "too old", ago_h=30)]  # 30h > 24h
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, window_hours=24, sleep=lambda: None).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 1  # coarse --since leaked


def test_per_ticker_degrade_surfaces():
    fake = _FakeTwitter(searches={"NVDA": "BLOCK", "AMC": []})
    res = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx())
    tickers = {r.ticker for r in res.records}
    assert "NVDA" not in tickers and "AMC" in tickers  # blocked ticker dropped
    assert res.error and "NVDA" in res.error and "unavailable" in res.error


def test_filter_min_likes_reenforced():
    tweets = [_tweet("1", "high engagement", ago_h=1, likes=5),
              _tweet("2", "zero-like spam", ago_h=1, likes=0)]
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, min_likes=2, sleep=lambda: None).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 1  # 0-likes dropped


def test_filter_dedupes_near_identical_text():
    tweets = [_tweet("1", "NVDA to the moon!!! https://a.co/x", ago_h=1),
              _tweet("2", "nvda to the moon  https://b.co/y", ago_h=2)]  # same text, diff url/case/punct
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 1  # deduped


def test_haiku_off_without_key_method_none():
    tweets = [_tweet(str(i), f"nvda take {i}", ago_h=1) for i in range(1, 6)]  # 5 > floor
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, min_tweets_haiku=3, sleep=lambda: None).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "none" and res.cost.haiku_calls == 0


def test_haiku_on_above_floor_classifies_no_native():
    tweets = [_tweet("1", "calls", ago_h=1), _tweet("2", "long", ago_h=1),
              _tweet("3", "puts", ago_h=1), _tweet("4", "hold", ago_h=1)]
    classifications = {"classifications": [
        {"post_id": "1", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "2", "ticker": "NVDA", "stance": "bullish"},
        {"post_id": "3", "ticker": "NVDA", "stance": "bearish"},
        {"post_id": "4", "ticker": "NVDA", "stance": "neutral"},
    ]}
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(
        client=fake, min_tweets_haiku=3,
        anthropic_client=_FakeAnthropic(text=json.dumps(classifications)), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "haiku"
    assert (nvda.sentiment.bullish, nvda.sentiment.bearish, nvda.sentiment.neutral) == (2, 1, 1)
    assert nvda.sentiment.native is None  # Twitter has no native stance (mirror /smg/)
    assert res.cost.haiku_calls == 1


def test_haiku_below_floor_method_none():
    tweets = [_tweet("1", "a", ago_h=1), _tweet("2", "b", ago_h=1)]  # 2 < floor 3
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(
        client=fake, min_tweets_haiku=3, anthropic_client=_FakeAnthropic(), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.sentiment.method == "none" and res.cost.haiku_calls == 0


def test_observed_window_is_min_max_of_survivors():
    tweets = [_tweet("1", "x", ago_h=5), _tweet("2", "y", ago_h=1), _tweet("3", "z", ago_h=3)]
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx())
    ow = {r.ticker: r for r in res.records}["NVDA"].observed_window
    assert ow is not None
    assert ow.earliest == iso_z(FIXED - 5 * 3600)  # oldest survivor
    assert ow.latest == iso_z(FIXED - 1 * 3600)  # newest survivor


def test_observed_window_null_when_zero_survivors():
    fake = _FakeTwitter(searches={"NVDA": [], "AMC": []})
    res = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.observed_window is None and nvda.metrics.mention_count == 0


def test_cost_accumulated_on_haiku_path():
    tweets = [_tweet(str(i), f"nvda {i}", ago_h=1) for i in range(1, 5)]
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    res = TwitterSource(
        client=fake, min_tweets_haiku=3,
        anthropic_client=_FakeAnthropic(text='{"classifications":[]}'), sleep=lambda: None,
    ).fetch(WL, context=_ctx())
    assert res.cost.haiku_calls == 1
    assert res.cost.input_tokens == 10 and res.cost.output_tokens == 5  # accumulated (doctrine #8)


def test_observed_window_round_trips_through_aggregate_to_json(tmp_path):
    # Order item 2: observed_window must survive source -> envelope -> aggregate -> the
    # exact JSON the CLI emits on stdout (result.model_dump(mode="json")).
    from chatter_daemon.aggregate import build_aggregate
    from chatter_daemon.baseline import connect, init_db
    from chatter_daemon.schema import ScanEnvelope

    tweets = [_tweet("1", "x", ago_h=3), _tweet("2", "y", ago_h=1)]
    fake = _FakeTwitter(searches={"NVDA": tweets, "AMC": []})
    records = TwitterSource(client=fake, sleep=lambda: None).fetch(WL, context=_ctx()).records
    env = ScanEnvelope(
        scan_mode="watchlist", canonical_ts=iso_z(FIXED),
        windows=list(derive_windows(FIXED).values()), records=records,
    )
    conn = connect(tmp_path / "b.sqlite3")
    init_db(conn)
    result = build_aggregate(
        env, conn=conn, scan_id="cd-x", source_floors={}, baseline_window=20,
        baseline_min_obs=5, spike_z_threshold=2.0, now=FIXED,
    )
    dumped = result.model_dump(mode="json")
    sig = next(
        s
        for t in dumped["tickers"] if t["ticker"] == "NVDA"
        for s in t["sources"] if s["source"] == "twitter"
    )
    assert sig["observed_window"] == {
        "earliest": iso_z(FIXED - 3 * 3600),
        "latest": iso_z(FIXED - 1 * 3600),
    }
