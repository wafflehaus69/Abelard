"""StockTwits client (Order 9) — trending parse + degrade-clean CF/transport mapping.

No live network: a fake HttpClient drives `get_json`. The CF wall (200 + HTML) is
modeled as `.json()` raising, which the client must map to a soft `StockTwitsBlocked`,
never let crash.
"""

from __future__ import annotations

import pytest

from chatter_daemon.sources.stocktwits import (
    StockTwitsBlocked,
    StockTwitsClient,
    TRENDING_URL,
)


class _FakeHttp:
    """HttpClient-shaped stub: returns a fixed payload or raises a fixed exception."""

    def __init__(self, *, payload=None, exc=None):
        self._payload = payload
        self._exc = exc
        self.calls: list[str] = []

    def get_json(self, url, **kwargs):
        self.calls.append(url)
        if self._exc is not None:
            raise self._exc
        return self._payload


def test_trending_returns_symbol_dicts():
    http = _FakeHttp(payload={"symbols": [{"symbol": "GME", "trending_score": 9.0}, {"symbol": "AMC"}]})
    client = StockTwitsClient(client=http)
    out = client.trending()
    assert [s["symbol"] for s in out] == ["GME", "AMC"]
    assert http.calls == [TRENDING_URL]  # hit the trending endpoint


def test_trending_filters_non_dict_entries():
    http = _FakeHttp(payload={"symbols": [{"symbol": "GME"}, "junk", 42]})
    out = StockTwitsClient(client=http).trending()
    assert out == [{"symbol": "GME"}]  # only dict entries survive


def test_trending_html_challenge_raises_blocked():
    # A CF wall returns 200 + HTML; the shared client's .json() raises ValueError, which
    # we map to a soft StockTwitsBlocked (degrade-clean), never a crash.
    http = _FakeHttp(exc=ValueError("Expecting value: line 1 column 1 (char 0)"))
    with pytest.raises(StockTwitsBlocked) as ei:
        StockTwitsClient(client=http).trending()
    assert "CF wall or transport" in str(ei.value)


def test_trending_missing_symbols_key_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload={"not_symbols": []})).trending()


def test_trending_empty_list_raises_blocked():
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload={"symbols": []})).trending()


def test_trending_non_dict_payload_raises_blocked():
    # Parsed JSON that isn't the expected object (e.g. an HTML body that happened to
    # decode to a string) is still a block, not a crash.
    with pytest.raises(StockTwitsBlocked):
        StockTwitsClient(client=_FakeHttp(payload="<html>challenge</html>")).trending()
