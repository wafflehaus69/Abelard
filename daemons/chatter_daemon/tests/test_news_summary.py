"""CH-SRC-2 news summarizer — Finnhub + Yahoo (+ AV) headlines analyzed TOGETHER, one summary per
ticker. Covers: combined-feed input, cross-feed dedup + freshest-first order, the Yahoo-only gap,
the named-mention gate, cost cap, degrade-clean, headline cap, no-key auto-off, Sonnet model."""

from __future__ import annotations

from chatter_daemon.config import DEFAULT_SUMMARY_MODEL
from chatter_daemon.news_summary import NewsSummarizer
from chatter_daemon.schema import CostTelemetry, Headline, Metrics, NormalizedRecord, Sentiment
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600
_WIN = derive_windows(FIXED)["24h"]
# NVDA gates on the symbol token (no spec names); MU carries a name_match:false alias ("micron")
# that news headlines can trust — the summarizer must honor it (the CH-SRC-1 full-map behavior).
WL = WatchlistConfig(
    name="x",
    tickers=[{"symbol": "NVDA"}, {"symbol": "MU", "names": ["Micron"], "name_match": False}],
)


# --- fake Anthropic (records each create() call) --------------------------------------------------
class _Blk:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _Usage:
    def __init__(self, i, o):
        self.input_tokens = i
        self.output_tokens = o
        self.cache_read_input_tokens = 0
        self.cache_creation_input_tokens = 0


class _Resp:
    def __init__(self, text, i, o):
        self.content = [_Blk(text)]
        self.usage = _Usage(i, o)
        self.stop_reason = "end_turn"


class _Msgs:
    def __init__(self, text, i, o):
        self._t, self._i, self._o = text, i, o
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _Resp(self._t, self._i, self._o)


class _FakeAnthropic:
    def __init__(self, text="Factual one-paragraph summary.", i=50, o=40):
        self.messages = _Msgs(text, i, o)


def _rec(source, ticker, titles):
    return NormalizedRecord(
        watchlist="x",
        scan_mode="watchlist",
        canonical_ts=iso_z(FIXED),
        window=_WIN,
        source=source,
        ticker=ticker,
        matched_by=["symbol"],
        metrics=Metrics(
            mention_count=len(titles),
            headlines=[Headline(title=t, url="http://x") for t in titles],
        ),
        sentiment=Sentiment(method="none"),
        flags=[],
    )


def _summarizer(anthropic, *, cap=1.0):
    # no company_names_path -> aliases come from spec.names only (NVDA symbol-only; MU -> "micron")
    return NewsSummarizer(anthropic_client=anthropic, summary_cost_cap_usd=cap)


def _run(records, anthropic, *, cap=1.0):
    cost = CostTelemetry()
    out, warnings = _summarizer(anthropic, cap=cap).summarize(records, [WL], cost=cost)
    return out, warnings, cost


def test_summarizes_over_combined_finnhub_and_yahoo():
    fake = _FakeAnthropic(text="NVDA data-center demand and a fresh chip launch.")
    records = [
        _rec("finnhub_news", "NVDA", ["NVDA earnings beat expectations"]),
        _rec("yahoo_rss", "NVDA", ["NVDA unveils a new GPU today"]),
    ]
    out, _, cost = _run(records, fake)
    assert out[("x", "NVDA")] == "NVDA data-center demand and a fresh chip launch."
    assert len(fake.messages.calls) == 1 and cost.haiku_calls == 1
    user = fake.messages.calls[0]["messages"][0]["content"]
    # BOTH feeds' headlines are analyzed together
    assert "NVDA earnings beat expectations" in user
    assert "NVDA unveils a new GPU today" in user


def test_dedup_across_feeds_and_yahoo_first_order():
    fake = _FakeAnthropic()
    records = [
        _rec("finnhub_news", "NVDA", ["NVDA alpha", "NVDA beta"]),
        _rec("yahoo_rss", "NVDA", ["NVDA beta", "NVDA gamma"]),  # beta dups Finnhub; gamma is fresh
    ]
    _run(records, fake)
    user = fake.messages.calls[0]["messages"][0]["content"]
    assert user.count("NVDA beta") == 1  # the cross-feed duplicate folds to one line
    # freshest feed (Yahoo) leads: its gamma precedes Finnhub's alpha
    assert user.index("NVDA gamma") < user.index("NVDA alpha")


def test_yahoo_only_ticker_still_summarized():
    # Finnhub found nothing for NVDA (honest zero), but Yahoo has fresh news -> summary still runs.
    fake = _FakeAnthropic(text="Fresh Yahoo-sourced NVDA news.")
    records = [
        _rec("finnhub_news", "NVDA", []),  # honest zero
        _rec("yahoo_rss", "NVDA", ["NVDA lands a hyperscaler order"]),
    ]
    out, _, _ = _run(records, fake)
    assert out[("x", "NVDA")] == "Fresh Yahoo-sourced NVDA news."


def test_gated_on_named_mention():
    fake = _FakeAnthropic()
    records = [
        _rec("finnhub_news", "NVDA", ["NVDA jumps on earnings"]),  # names NVDA -> summarize
        _rec("finnhub_news", "MU", ["broad market selloff today"]),  # names neither MU nor Micron
    ]
    out, _, _ = _run(records, fake)
    assert ("x", "NVDA") in out
    assert ("x", "MU") not in out  # no named news -> skipped (no call)
    assert len(fake.messages.calls) == 1


def test_name_match_false_alias_summarized():
    # MU's "micron" is name_match:false but a headline naming Micron is real news -> summarize.
    fake = _FakeAnthropic(text="Micron memory pricing update.")
    records = [_rec("yahoo_rss", "MU", ["Micron guides memory prices higher"])]
    out, _, _ = _run(records, fake)
    assert out[("x", "MU")] == "Micron memory pricing update."


def test_no_key_no_call():
    out, warnings, cost = _run([_rec("finnhub_news", "NVDA", ["NVDA jumps"])], None)
    assert out == {} and cost.haiku_calls == 0


def test_cost_cap_enforced_and_visible():
    # each call's output ~ $1.00 (200k out * $5/M); the 2nd ticker trips the cap.
    fake = _FakeAnthropic(text="summary", i=0, o=200_000)
    records = [
        _rec("finnhub_news", "NVDA", ["NVDA news today"]),
        _rec("yahoo_rss", "MU", ["Micron news today"]),
    ]
    out, warnings, _ = _run(records, fake, cap=1.0)
    assert ("x", "NVDA") in out  # first call lands (cost was $0 before it)
    assert ("x", "MU") not in out  # cap tripped before the 2nd call
    assert len(fake.messages.calls) == 1
    assert any("MU" in w and "cost cap" in w for w in warnings)  # fail-loud, visible


def test_degrades_on_llm_failure():
    class _Boom:
        def __init__(self):
            self.messages = self

        def create(self, **kwargs):
            raise RuntimeError("api down")

    out, warnings, _ = _run([_rec("finnhub_news", "NVDA", ["NVDA news"])], _Boom())
    assert ("x", "NVDA") not in out  # degraded to no summary
    assert any("NVDA" in w and "summary failed" in w for w in warnings)


def test_headline_cap_bounds_the_call():
    fake = _FakeAnthropic(text="ok")
    records = [_rec("finnhub_news", "NVDA", [f"NVDA item {i}" for i in range(250)])]
    _run(records, fake)
    user = fake.messages.calls[0]["messages"][0]["content"]
    assert user.count("NVDA item") == 15  # only the capped top-15 fed, not 250


def test_summary_runs_on_sonnet():
    fake = _FakeAnthropic(text="ok")
    _run([_rec("finnhub_news", "NVDA", ["NVDA earnings"])], fake)
    assert fake.messages.calls[0]["model"] == DEFAULT_SUMMARY_MODEL
