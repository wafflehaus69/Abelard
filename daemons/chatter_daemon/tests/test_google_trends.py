"""Google Trends — query decoupled from name_match, noisy_query for ambiguous terms
(real value) AND for no-term ETFs (null), 429-degrade, shape-raise, non-ASCII."""

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
        {"symbol": "NVDA"},  # name_match:true -> shared-map alias, clean
        {"symbol": "MELI", "names": ["MercadoLibre", "Mercado Libre"]},  # inline, clean
        {"symbol": "DE", "name_match": False, "names": ["John Deere"]},  # name_match:false, still queried
        {"symbol": "CAT", "name_match": False, "names": ["Caterpillar"], "ambiguous_name": True},  # queried + noisy
        {"symbol": "ITA", "is_etf": True, "name_match": False},  # ETF, no name -> null
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


def test_query_name_decoupled_from_name_match():
    assert _NVDA_Q is not None and _NVDA_Q != "NVDA"  # name_match:true via shared map
    assert query_name(WL.tickers[1], _SHARED) == "MercadoLibre"  # names[0]
    # the decoupling: name_match:false tickers WITH names[] are still queried
    assert query_name(WL.tickers[2], _SHARED) == "John Deere"
    assert query_name(WL.tickers[3], _SHARED) == "Caterpillar"
    assert query_name(WL.tickers[4], _SHARED) is None  # ETF, no name -> no query


def test_clean_names_three_windows():
    client = _FakeClient(interest_map={_NVDA_Q: 75.0, _MELI_Q: 40.0, "John Deere": 20.0})
    res = _src(client).fetch(WL, context=_ctx())
    by = {r.ticker: r for r in res.records}
    for sym, val in (("NVDA", 75.0), ("MELI", 40.0), ("DE", 20.0)):
        assert by[sym].metrics.interest_24h == val
        assert by[sym].metrics.interest_7d == val
        assert by[sym].metrics.interest_monthly == val
        assert by[sym].matched_by == ["name"]
        assert by[sym].flags == []
        assert by[sym].sentiment.method == "none"
    assert all(q[0] != "NVDA" for q in client.queries)  # never the bare ticker
    assert ("John Deere", "now 1-d") in client.queries  # name_match:false ticker still queried
    assert res.error is None


def test_ambiguous_term_queries_with_noisy_flag():
    # CAT (ambiguous_name) queries "Caterpillar" AND returns a REAL value + noisy_query
    # (the §C rework: was null, now a discounted-but-present number).
    client = _FakeClient(
        interest_map={_NVDA_Q: 50.0, _MELI_Q: 50.0, "John Deere": 50.0, "Caterpillar": 88.0}
    )
    res = _src(client).fetch(WL, context=_ctx())
    cat = {r.ticker: r for r in res.records}["CAT"]
    assert cat.metrics.interest_24h == 88.0  # NOT null
    assert cat.flags == ["noisy_query"]
    assert cat.matched_by == ["name"]
    assert ("Caterpillar", "now 1-d") in client.queries


def test_etf_no_query_null():
    client = _FakeClient(
        interest_map={_NVDA_Q: 50.0, _MELI_Q: 50.0, "John Deere": 50.0, "Caterpillar": 50.0}
    )
    res = _src(client).fetch(WL, context=_ctx())
    ita = {r.ticker: r for r in res.records}["ITA"]
    assert ita.metrics.interest_24h is None  # ETF -> null, never fabricated
    assert ita.flags == []  # "no signal" -> NO noisy_query (kept distinct from ambiguous)
    assert ita.matched_by == []
    assert "ITA" not in {q[0] for q in client.queries}  # never queried (no clean term)


def test_429_degrades_not_raises():
    client = _FakeClient(raise_on=TrendsRateLimited("429 too many requests"))
    res = _src(client).fetch(WL, context=_ctx())  # must NOT raise
    assert res.error is not None and "429" in res.error
    by = {r.ticker: r for r in res.records}
    assert by["NVDA"].metrics.interest_24h is None  # null, not fabricated
    assert len(res.records) == 5  # all active tickers still emitted


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
    assert len(env.records) == 5  # the scan did not sink
