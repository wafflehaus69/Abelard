"""Finnhub universe cache — live parse/uppercase, fail-loud shapes, 24h cache + TTL
expiry, static fallback on live failure, no-fallback raises, key redaction."""

from __future__ import annotations

import pytest

from chatter_daemon.baseline import connect
from chatter_daemon.ticker_universe import (
    UniverseError,
    fetch_us_symbols_live,
    init_universe_table,
    load_static_fallback,
    load_universe,
)

_PAYLOAD = [
    {"symbol": "GME"},
    {"symbol": "amc"},  # lower -> uppercased
    {"symbol": "NVDA"},
    {"description": "row with no symbol"},  # dropped
]


class _FakeClient:
    def __init__(self, data=None, exc=None):
        self._data = data
        self._exc = exc
        self.calls = 0

    def get_json(self, url, *, params=None, headers=None, timeout=None):
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        return self._data


def _conn(tmp_path):
    conn = connect(tmp_path / "u.sqlite3")
    init_universe_table(conn)
    return conn


def test_fetch_live_parses_and_uppercases():
    assert fetch_us_symbols_live(_FakeClient(data=_PAYLOAD), "key") == {"GME", "AMC", "NVDA"}


def test_fetch_empty_key_raises():
    with pytest.raises(UniverseError, match="empty"):
        fetch_us_symbols_live(_FakeClient(data=_PAYLOAD), "")


def test_fetch_non_list_raises():
    with pytest.raises(UniverseError, match="did not return a list"):
        fetch_us_symbols_live(_FakeClient(data={"x": 1}), "key")


def test_fetch_empty_list_raises():
    with pytest.raises(UniverseError, match="empty list"):
        fetch_us_symbols_live(_FakeClient(data=[]), "key")


def test_load_universe_fetches_then_caches(tmp_path):
    conn = _conn(tmp_path)
    client = _FakeClient(data=_PAYLOAD)
    r1 = load_universe(conn, client=client, api_key="key", ttl_s=86400, now=1000)
    assert r1.source == "finnhub" and "GME" in r1.symbols and client.calls == 1
    r2 = load_universe(conn, client=client, api_key="key", ttl_s=86400, now=2000)
    assert r2.source == "cache" and client.calls == 1  # served from cache, no refetch


def test_load_universe_ttl_expiry_refetches(tmp_path):
    conn = _conn(tmp_path)
    client = _FakeClient(data=_PAYLOAD)
    load_universe(conn, client=client, api_key="key", ttl_s=100, now=1000)
    r = load_universe(conn, client=client, api_key="key", ttl_s=100, now=1300)  # past TTL
    assert r.source == "finnhub" and client.calls == 2


def test_load_universe_falls_back_on_live_failure(tmp_path):
    fb = tmp_path / "fallback.txt"
    fb.write_text("GME\nAMC\n# comment\n", encoding="utf-8")
    conn = _conn(tmp_path)
    client = _FakeClient(exc=RuntimeError("403 not available on this tier"))
    r = load_universe(
        conn, client=client, api_key="key", ttl_s=86400, now=1000, fallback_path=fb
    )
    assert r.source == "static_fallback"
    assert r.symbols == frozenset({"GME", "AMC"})
    assert r.warning and "static fallback" in r.warning


def test_load_universe_no_fallback_raises(tmp_path):
    conn = _conn(tmp_path)
    client = _FakeClient(exc=RuntimeError("403"))
    with pytest.raises(UniverseError):
        load_universe(conn, client=client, api_key="key", ttl_s=86400, now=1000)


def test_static_fallback_parses(tmp_path):
    fb = tmp_path / "f.txt"
    fb.write_text("# header\nGME\namc\n\nNVDA\n", encoding="utf-8")
    assert load_static_fallback(fb) == {"GME", "AMC", "NVDA"}


def test_redacts_key_in_error():
    client = _FakeClient(exc=RuntimeError("GET failed: ...token=SECRET123 ..."))
    with pytest.raises(UniverseError) as ei:
        fetch_us_symbols_live(client, "SECRET123")
    assert "SECRET123" not in str(ei.value)
