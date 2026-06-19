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
    res = FinnhubNewsSource(api_key="k", client=client).fetch(WL, context=_ctx())
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
