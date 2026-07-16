"""Finnhub company-news plugin — counts, honest zeros, raises, decode, end-to-end."""

from __future__ import annotations

import json as _json

import pytest
import requests

from abelard_common.http_client import HttpClient, NotFound, RateLimited, TransportError
from chatter_daemon.orchestrator import run_scan
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.finnhub_news import FinnhubError, FinnhubNewsSource
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600

# active = NVDA, ITA (P is enabled=False, excluded from scanning)
WL = WatchlistConfig(
    name="x",
    tickers=[
        {"symbol": "NVDA"},
        {"symbol": "ITA", "is_etf": True, "name_match": False},
        {"symbol": "P", "enabled": False},
    ],
)


def _ctx():
    return ScanContext(
        scan_mode="watchlist",
        canonical_unix=FIXED,
        canonical_ts=iso_z(FIXED),
        windows=derive_windows(FIXED),
    )


class _FakeClient:
    def __init__(self, payloads):
        self._payloads = list(payloads)
        self.calls = []

    def get_json(self, url, *, params=None, headers=None, timeout=None):
        self.calls.append(params)
        p = self._payloads.pop(0)
        if isinstance(p, Exception):
            raise p
        return p


def test_counts_headlines_and_record_shape():
    client = _FakeClient([
        [
            {"headline": "NVDA pops", "url": "http://a", "datetime": 1},
            {"headline": "NVDA guides up", "url": "http://b", "datetime": 2},
        ],
        [],  # ITA: ETF, no company-news -> honest zero
    ])
    res = FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert set(by) == {"NVDA", "ITA"}  # P excluded (disabled)
    assert by["NVDA"].metrics.mention_count == 2
    assert len(by["NVDA"].metrics.headlines) == 2
    assert by["NVDA"].source == "finnhub_news"
    assert by["NVDA"].matched_by == ["symbol"]
    assert by["NVDA"].sentiment.method == "none"
    assert by["ITA"].metrics.mention_count == 0  # honest zero record


def test_not_found_is_honest_zero():
    client = _FakeClient([NotFound("404"), []])
    res = FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.mention_count == 0


def test_missing_key_raises():
    with pytest.raises(FinnhubError):
        FinnhubNewsSource(api_key=None, client=_FakeClient([])).fetch(WL, context=_ctx())


def test_rate_limit_raises():
    client = _FakeClient([RateLimited("429")])
    with pytest.raises(RateLimited):
        FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())


def test_auth_transport_raises():
    client = _FakeClient([TransportError("403 from ...")])
    with pytest.raises(TransportError):
        FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())


def test_malformed_payload_raises():
    client = _FakeClient([{"not": "a list"}])
    with pytest.raises(FinnhubError):
        FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())


# --- Order 15: named-news summary (gated on mention, cost-capped, degrade-clean) ----


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


def _finnhub(client, *, anthropic=None, cap=1.0):
    # no company_names_path -> aliases empty -> the gate is symbol-word-boundary only
    return FinnhubNewsSource(api_key="k", client=client, anthropic_client=anthropic, summary_cost_cap_usd=cap)


def test_summary_gated_on_direct_mention():
    fake = _FakeAnthropic(text="NVDA reported strong data-center results.")
    client = _FakeClient([
        [{"headline": "NVDA jumps on earnings", "url": "http://x"}],     # named -> summarize
        [{"headline": "broad market selloff today", "url": "http://x"}], # ITA unnamed -> skip
    ])
    res = _finnhub(client, anthropic=fake).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].news_summary == "NVDA reported strong data-center results."
    assert by["ITA"].news_summary is None                # no named news -> skip (no call)
    assert len(fake.messages.calls) == 1 and res.cost.haiku_calls == 1


def test_summary_no_key_no_call():
    client = _FakeClient([
        [{"headline": "NVDA jumps", "url": "http://x"}],
        [{"headline": "x", "url": "http://x"}],
    ])
    res = _finnhub(client, anthropic=None).fetch(WL, context=_ctx())  # no Anthropic client
    assert {r.ticker: r for r in res.records}["NVDA"].news_summary is None
    assert res.cost.haiku_calls == 0


def test_summary_cost_cap_enforced_and_visible():
    # each call's output ~ $1.00 (200k out * $5/M); the 2nd ticker trips the cap.
    fake = _FakeAnthropic(text="summary", i=0, o=200_000)
    client = _FakeClient([
        [{"headline": "NVDA news today", "url": "http://x"}],
        [{"headline": "ITA fund news about ITA", "url": "http://x"}],  # named, but capped
    ])
    res = _finnhub(client, anthropic=fake, cap=1.0).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].news_summary == "summary"      # first call lands (cost was $0 before it)
    assert by["ITA"].news_summary is None            # cap tripped before the 2nd call
    assert len(fake.messages.calls) == 1             # NO call issued once over cap
    assert any("ITA" in w and "cost cap" in w for w in res.warnings)  # fail-loud, visible


def test_summary_degrades_on_haiku_failure():
    class _Boom:
        def __init__(self):
            self.messages = self

        def create(self, **kwargs):
            raise RuntimeError("api down")

    client = _FakeClient([
        [{"headline": "NVDA news", "url": "http://x"}],
        [{"headline": "x", "url": "http://x"}],
    ])
    res = _finnhub(client, anthropic=_Boom()).fetch(WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.news_summary is None                       # degraded to None
    assert nvda.metrics.mention_count == 1                 # count UNAFFECTED
    assert nvda.metrics.headlines[0].title == "NVDA news"  # headlines UNAFFECTED
    assert any("NVDA" in w and "summary failed" in w for w in res.warnings)


def test_summary_headline_cap_bounds_the_call():
    fake = _FakeAnthropic(text="ok")
    heads = [{"headline": f"NVDA item {i}", "url": "http://x"} for i in range(250)]
    client = _FakeClient([heads, [{"headline": "x", "url": "http://x"}]])
    _finnhub(client, anthropic=fake).fetch(WL, context=_ctx())
    user = fake.messages.calls[0]["messages"][0]["content"]
    assert user.count("NVDA item") == 15  # only the capped top-15 fed, not 250


def test_summary_uses_shared_relevance_filter():
    fake = _FakeAnthropic(text="ok")
    heads = [
        {"headline": "NVDA earnings beat", "url": "http://x"},  # direct mention -> fed
        {"headline": "broad market memo", "url": "http://x"},   # no mention -> filtered out
    ]
    client = _FakeClient([heads, [{"headline": "x", "url": "http://x"}]])
    _finnhub(client, anthropic=fake).fetch(WL, context=_ctx())
    user = fake.messages.calls[0]["messages"][0]["content"]
    assert "NVDA earnings beat" in user and "broad market memo" not in user


def test_nonascii_headline_decodes_through_real_client():
    # A real Response holding UTF-8 bytes with a MIS-set encoding, through the real
    # HttpClient -> the adapter's non-ASCII regression (the decode obligation).
    def _resp(obj):
        r = requests.Response()
        r.status_code = 200
        r._content = _json.dumps(obj, ensure_ascii=False).encode("utf-8")
        r.encoding = "ISO-8859-1"
        return r

    class _FakeSession:
        def __init__(self, responses):
            self._responses = list(responses)

        def get(self, url, params=None, headers=None, timeout=None):
            return self._responses.pop(0)

    session = _FakeSession([
        _resp([{"headline": "Nvidia — café déjà vu", "url": "http://x", "datetime": 1}]),
        _resp([]),
    ])
    client = HttpClient(user_agent="t", session=session)
    # relevance_gate=False: this is the DECODE regression — the head names NVDA by company name
    # ("Nvidia") not the "NVDA" symbol, and with no company_names_path the gate is symbol-only, so
    # gating is orthogonal here. Turn it off to keep the test about UTF-8, not the alias map.
    res = FinnhubNewsSource(api_key="k", client=client, relevance_gate=False).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.headlines[0].title == "Nvidia — café déjà vu"


def test_end_to_end_via_run_scan():
    client = _FakeClient([
        [{"headline": "h", "url": "http://x", "datetime": 1}],  # NVDA
        [],  # ITA
    ])
    env = run_scan([WL], sources=[FinnhubNewsSource(api_key="k", client=client)], now=FIXED)
    assert env.degraded is False
    assert env.sources[0].source == "finnhub_news"
    assert env.sources[0].ok is True
    assert env.sources[0].record_count == 2
    assert len(env.records) == 2
    assert all(r.schema_version == "1" for r in env.records)


def test_summary_on_sonnet_and_headlines_collected():
    # Order 19: prose summary runs on Sonnet by default; headlines flow to the raw history.
    fake = _FakeAnthropic(text="NVDA strong data-center demand.")
    client = _FakeClient([
        [{"headline": "NVDA jumps on earnings", "url": "http://x"}],  # named -> summary
        [],  # ITA honest zero
    ])
    res = _finnhub(client, anthropic=fake).fetch(WL, context=_ctx())
    assert "NVDA\tNVDA jumps on earnings" in res.raw_items          # headlines -> history dump
    assert fake.messages.calls[0]["model"] == "claude-sonnet-4-6"  # summary on Sonnet


# --- CH-SRC-1: relevance gate (keep a head only if its title names THIS ticker) -----------------
# Finnhub cross-tags peer/macro stories onto every symbol's feed; live, only ~23% of returned heads
# name the ticker. The gate drops the cross-tags (measured: dupes 35%->8%, no ticker zeroed).

_GATE_WL = WatchlistConfig(name="x", tickers=[{"symbol": "NVDA"}, {"symbol": "AMD"}])


def test_relevance_gate_drops_cross_tagged_peer_and_macro_heads():
    client = _FakeClient([
        [
            {"headline": "NVDA ships a new GPU", "url": "http://a"},        # names NVDA -> keep
            {"headline": "AMD wins a cloud contract", "url": "http://b"},  # peer cross-tag -> drop
            {"headline": "Dow movers rally today", "url": "http://c"},     # macro, no ticker -> drop
        ],
        [],  # AMD honest zero
    ])
    res = FinnhubNewsSource(api_key="k", client=client, relevance_gate=True).fetch(_GATE_WL, context=_ctx())
    nvda = {r.ticker: r for r in res.records}["NVDA"]
    assert nvda.metrics.mention_count == 1
    assert [h.title for h in nvda.metrics.headlines] == ["NVDA ships a new GPU"]


def test_relevance_gate_disabled_keeps_cross_tags():
    client = _FakeClient([
        [
            {"headline": "NVDA ships a new GPU", "url": "http://a"},
            {"headline": "AMD wins a cloud contract", "url": "http://b"},
            {"headline": "Dow movers rally today", "url": "http://c"},
        ],
        [],
    ])
    res = FinnhubNewsSource(api_key="k", client=client, relevance_gate=False).fetch(_GATE_WL, context=_ctx())
    assert {r.ticker: r for r in res.records}["NVDA"].metrics.mention_count == 3  # gate off -> all kept


def test_relevance_gate_keeps_name_match_false_alias():
    # MU's "micron" name is name_match:false (a length-unit collision unsafe in social feeds), so it
    # is absent from build_name_map. The gate must still keep a Finnhub head that names Micron by
    # NAME — watchlist_alias_map folds in the name_match:false alias for a scoped headline feed.
    wl = WatchlistConfig(name="x", tickers=[{"symbol": "MU", "names": ["Micron"], "name_match": False}])
    client = _FakeClient([
        [
            {"headline": "Micron stock jumps on HBM demand", "url": "http://a"},  # names Micron -> keep
            {"headline": "SK Hynix soars on AI memory", "url": "http://b"},        # peer, unnamed -> drop
        ],
    ])
    res = FinnhubNewsSource(api_key="k", client=client, relevance_gate=True).fetch(wl, context=_ctx())
    mu = {r.ticker: r for r in res.records}["MU"]
    assert mu.metrics.mention_count == 1
    assert [h.title for h in mu.metrics.headlines] == ["Micron stock jumps on HBM demand"]
