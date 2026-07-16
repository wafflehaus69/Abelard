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


def test_headlines_flow_to_raw_items():
    # Order 19: the kept headlines go to the raw-history dump as "TICKER\ttitle" lines. (The
    # per-ticker news SUMMARY moved to test_news_summary.py — Finnhub is LLM-free now.)
    client = _FakeClient([
        [{"headline": "NVDA jumps on earnings", "url": "http://x"}],  # names NVDA -> kept
        [],  # ITA honest zero
    ])
    res = FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())
    assert "NVDA\tNVDA jumps on earnings" in res.raw_items
    assert res.cost is None  # no LLM in Finnhub anymore


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
