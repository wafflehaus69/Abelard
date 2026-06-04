"""ticker_universe: cache TTL, live fetch, fallback, key safety."""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from biz_daemon import storage, ticker_universe
from biz_daemon.ticker_universe import UniverseError, fetch_us_symbols_live, load_universe


class FakeResp:
    def __init__(self, status_code, payload=None, malformed=False):
        self.status_code = status_code
        self._payload = payload
        self._malformed = malformed

    def json(self):
        if self._malformed:
            raise ValueError("nope")
        return self._payload


class FakeSession:
    def __init__(self, resp=None, raise_exc=None):
        self._resp = resp
        self._raise = raise_exc
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        if self._raise is not None:
            raise self._raise
        return self._resp


def _fallback_file(tmp_path: Path) -> Path:
    p = tmp_path / "fallback.txt"
    p.write_text("# seed\nGME\nAMD\nNTR\n", encoding="utf-8")
    return p


# --- live fetch --------------------------------------------------------------


def test_live_fetch_parses_and_uppercases():
    session = FakeSession(FakeResp(200, [{"symbol": "gme"}, {"symbol": "AMD"}, {}]))
    symbols = fetch_us_symbols_live("k", session=session)
    assert symbols == {"GME", "AMD"}


def test_missing_key_loud_fail():
    with pytest.raises(UniverseError):
        fetch_us_symbols_live("", session=FakeSession(FakeResp(200, [])))


def test_403_raises_universe_error():
    with pytest.raises(UniverseError):
        fetch_us_symbols_live("k", session=FakeSession(FakeResp(403)))


def test_key_never_appears_in_raised_error():
    key = "SUPERSECRET123"
    exc = requests.RequestException(f"failed url ...token={key}")
    with pytest.raises(UniverseError) as ei:
        fetch_us_symbols_live(key, session=FakeSession(raise_exc=exc))
    assert key not in str(ei.value)


# --- cache TTL ---------------------------------------------------------------


def test_cache_hit_within_ttl_skips_network(conn):
    storage.write_cached_universe(conn, symbols={"GME", "AMD"}, source="finnhub", now=1000)
    session = FakeSession(FakeResp(500))  # would fail if called
    result = load_universe(
        conn, api_key="k", fallback_path=Path("nope"), ttl_s=3600, now=1500, session=session
    )
    assert result.source == "cache"
    assert result.symbols == frozenset({"GME", "AMD"})
    assert session.calls == 0


def test_cache_expired_refetches(conn):
    storage.write_cached_universe(conn, symbols={"OLD"}, source="finnhub", now=1000)
    session = FakeSession(FakeResp(200, [{"symbol": "NEW"}]))
    result = load_universe(
        conn, api_key="k", fallback_path=Path("nope"), ttl_s=3600, now=99999, session=session
    )
    assert result.source == "finnhub"
    assert result.symbols == frozenset({"NEW"})
    assert session.calls == 1


# --- static fallback ---------------------------------------------------------


def test_unavailable_endpoint_falls_back_to_static(conn, tmp_path):
    fallback = _fallback_file(tmp_path)
    session = FakeSession(FakeResp(403))
    result = load_universe(
        conn, api_key="k", fallback_path=fallback, ttl_s=3600, now=1000, session=session
    )
    assert result.source == "static_fallback"
    assert result.symbols == frozenset({"GME", "AMD", "NTR"})
    assert result.warning is not None and "fallback" in result.warning


def test_static_fallback_loader_ignores_comments(tmp_path):
    fallback = _fallback_file(tmp_path)
    assert ticker_universe.load_static_fallback(fallback) == {"GME", "AMD", "NTR"}
