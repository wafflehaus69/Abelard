"""Google Trends plugin — name-keyed interest, noisy_query, 429-degrade, shape-raise."""

from __future__ import annotations

import pytest

from abelard_common.company_aliases import load_name_map
from chatter_daemon.config import _default_company_names_path
from chatter_daemon.orchestrator import run_scan
from chatter_daemon.sources.base import ScanContext
from chatter_daemon.sources.google_trends import (
    GoogleTrendsSource,
    TrendsRateLimited,
    TrendsShapeError,
    query_name,
)
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600

WL = WatchlistConfig(
    name="t",
    tickers=[
        {"symbol": "NVDA"},  # name_match:true -> shared-map alias
        {"symbol": "MELI", "names": ["MercadoLibre", "Mercado Libre"]},  # inline alias
        {"symbol": "ITA", "is_etf": True, "name_match": False},  # ETF -> noisy_query
        {"symbol": "CAT", "name_match": False},  # collision -> noisy_query
        {"symbol": "P", "enabled": False},  # excluded
    ],
)

_SHARED = load_name_map(_default_company_names_path())
_NVDA_Q = query_name(WL.tickers[0], _SHARED)  # the exact alias the matcher resolves
_MELI_Q = "MercadoLibre"


def _ctx():
    return ScanContext(
        scan_mode="watchlist",
        canonical_unix=FIXED,
        canonical_ts=iso_z(FIXED),
        windows=derive_windows(FIXED),
    )


class _FakeClient:
    def __init__(self, interest_map=None, raise_on=None):
        self.interest_map = interest_map or {}
        self.raise_on = raise_on
        self.queries: list[tuple[str, str]] = []

    def interest(self, query, timeframe):
        self.queries.append((query, timeframe))
        if self.raise_on is not None:
            raise self.raise_on
        return self.interest_map.get(query)


def _src(client):
    return GoogleTrendsSource(company_names_path=_default_company_names_path(), client=client)


def test_query_name_uses_alias_not_ticker():
    assert _NVDA_Q is not None and _NVDA_Q != "NVDA"
    assert query_name(WL.tickers[1], _SHARED) == "MercadoLibre"  # names[0]
    assert query_name(WL.tickers[2], _SHARED) is None  # ETF, name_match:false
    assert query_name(WL.tickers[3], _SHARED) is None  # collision, name_match:false


def test_clean_name_three_windows():
    client = _FakeClient(interest_map={_NVDA_Q: 75.0, _MELI_Q: 40.0})
    res = _src(client).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.interest_24h == 75.0
    assert by["NVDA"].metrics.interest_7d == 75.0
    assert by["NVDA"].metrics.interest_monthly == 75.0
    assert by["NVDA"].matched_by == ["name"]
    assert by["NVDA"].flags == []
    assert by["NVDA"].sentiment.method == "none"
    # queried by the matcher's name across the three anchored timeframes
    assert (_NVDA_Q, "now 1-d") in client.queries
    assert (_NVDA_Q, "now 7-d") in client.queries
    assert (_NVDA_Q, "today 1-m") in client.queries
    assert all(q[0] != "NVDA" for q in client.queries)  # never the bare ticker
    assert res.error is None


def test_noisy_query_null_interest():
    client = _FakeClient(interest_map={_NVDA_Q: 50.0, _MELI_Q: 50.0})
    res = _src(client).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    for sym in ("ITA", "CAT"):
        assert by[sym].flags == ["noisy_query"]
        assert by[sym].metrics.interest_24h is None
        assert by[sym].metrics.interest_monthly is None
        assert by[sym].matched_by == []
    # the noisy names were never queried (no fabricated precision)
    queried = {q[0] for q in client.queries}
    assert queried == {_NVDA_Q, _MELI_Q}


def test_429_degrades_not_raises():
    client = _FakeClient(raise_on=TrendsRateLimited("429 too many requests"))
    res = _src(client).fetch(WL, context=_ctx())  # must NOT raise
    assert res.error is not None and "429" in res.error
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.interest_24h is None  # null, not fabricated
    assert len(res.records) == 4  # all active tickers still emitted


def test_shape_change_raises():
    client = _FakeClient(raise_on=TrendsShapeError("interest_over_time missing column"))
    with pytest.raises(TrendsShapeError):
        _src(client).fetch(WL, context=_ctx())  # fail loud


def test_nonascii_query_passes_through():
    wl = WatchlistConfig(name="t", tickers=[{"symbol": "NVDA", "names": ["Nvidiá café"]}])
    client = _FakeClient(interest_map={"Nvidiá café": 10.0})
    res = _src(client).fetch(wl, context=_ctx())
    assert ("Nvidiá café", "now 1-d") in client.queries  # unicode intact at the boundary
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.interest_24h == 10.0


def test_end_to_end_run_scan_degraded_but_alive():
    client = _FakeClient(raise_on=TrendsRateLimited("429"))
    env = run_scan([WL], sources=[_src(client)], now=FIXED)
    assert env.sources[0].source == "google_trends"
    assert env.sources[0].ok is False  # 429 -> source degraded
    assert env.degraded is True
    assert len(env.records) == 4  # the scan did not sink
