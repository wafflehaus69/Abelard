"""Source fetchers: parsing, envelope extraction, and the funding-graph key gate."""

from __future__ import annotations

import pytest

from consensus.errors import DataLayerError
from consensus.fetching import build_data_layer
from consensus.sources_kalshi import get_kalshi_markets
from consensus.sources_polygon import get_erc20_transfers
from consensus.sources_polymarket import (
    get_market_meta,
    get_market_trades,
    get_wallet_positions,
)
from tests.conftest import (
    GAMMA_MARKETS,
    KALSHI_ENV,
    ETHERSCAN_EMPTY,
    ETHERSCAN_OK,
    POSITIONS,
    TRADES,
    make_loaded,
)


def test_get_market_trades_parses_and_drops(dl, requests_mock):
    requests_mock.get("https://data-api.polymarket.com/trades", json=TRADES)
    trades = get_market_trades(dl, "0xCID")
    assert len(trades) == 2  # third record unusable
    assert {t.side for t in trades} == {"BUY", "SELL"}


def test_get_wallet_positions_parses(dl, requests_mock):
    requests_mock.get("https://data-api.polymarket.com/positions", json=POSITIONS)
    positions = get_wallet_positions(dl, "0xWALLET")
    assert len(positions) == 1
    assert positions[0].mergeable is True
    # Displayed PnL is carried but flagged diagnostic-only in the model docs.
    assert positions[0].realized_pnl == 0.0


def test_get_market_meta_from_gamma(dl, requests_mock):
    requests_mock.get("https://gamma-api.polymarket.com/markets", json=GAMMA_MARKETS)
    meta = get_market_meta(dl, "0xCID")
    assert meta is not None
    assert meta.outcomes == ["Yes", "No"]
    assert meta.outcome_prices == [0.205, 0.795]


def test_get_market_meta_empty_returns_none(dl, requests_mock):
    requests_mock.get("https://gamma-api.polymarket.com/markets", json=[])
    assert get_market_meta(dl, "0xNOPE") is None


def test_get_market_meta_falls_back_to_closed(dl, requests_mock):
    """Gamma hides resolved markets unless closed=true — the helper must retry
    with the closed filter before concluding the market doesn't exist."""
    m = requests_mock.get(
        "https://gamma-api.polymarket.com/markets",
        [{"json": []}, {"json": GAMMA_MARKETS}],
    )
    meta = get_market_meta(dl, "0xCID")
    assert meta is not None
    assert m.call_count == 2
    assert m.request_history[1].qs["closed"] == ["true"]


def test_kalshi_envelope_extracted(dl, requests_mock):
    requests_mock.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets", json=KALSHI_ENV
    )
    markets = get_kalshi_markets(dl, limit=5)
    assert len(markets) == 1  # second record has no ticker -> dropped
    assert markets[0].ticker == "KXTEST-1"
    from decimal import Decimal
    assert markets[0].yes_bid == Decimal("0.45")


def test_kalshi_non_envelope_raises(dl, requests_mock):
    requests_mock.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets", json=[{"ticker": "X"}]
    )
    with pytest.raises(DataLayerError):
        get_kalshi_markets(dl, limit=5)


def test_polygon_without_key_is_loud(dl):
    with pytest.raises(DataLayerError):
        get_erc20_transfers(dl, "0xADDR")


def test_polygon_with_key_parses_and_excludes_secret(tmp_path, requests_mock):
    loaded = make_loaded(tmp_path, etherscan_key="SECRETKEY")
    dl = build_data_layer(loaded)
    try:
        requests_mock.get("https://api.etherscan.io/v2/api", json=ETHERSCAN_OK)
        transfers = get_erc20_transfers(dl, "0xADDR")
        assert len(transfers) == 1
        assert transfers[0].value_normalized == 1.0

        # The API key must never be written to the cache key.
        cache_params = {
            "chainid": 137, "module": "account", "action": "tokentx", "address": "0xADDR",
            "startblock": 0, "endblock": 999_999_999, "sort": "asc",
        }
        cached = dl.cache.latest(source="etherscan_polygon", endpoint="", params=cache_params)
        assert cached is not None
        assert "apikey" not in cached.params

        # And the secret never appears in what we persisted.
        assert "SECRETKEY" not in str(cached.params)
    finally:
        dl.cache.close()


def test_polygon_no_transactions_is_empty_not_error(tmp_path, requests_mock):
    loaded = make_loaded(tmp_path, etherscan_key="SECRETKEY")
    dl = build_data_layer(loaded)
    try:
        requests_mock.get("https://api.etherscan.io/v2/api", json=ETHERSCAN_EMPTY)
        assert get_erc20_transfers(dl, "0xADDR") == []
    finally:
        dl.cache.close()


def test_polygon_error_status_is_loud_not_empty(tmp_path, requests_mock):
    """A bad-key / rate-limit style Polygonscan error (status 0, result is an
    error STRING) must raise, never be swallowed into an empty list."""
    loaded = make_loaded(tmp_path, etherscan_key="SECRETKEY")
    dl = build_data_layer(loaded)
    try:
        requests_mock.get(
            "https://api.etherscan.io/v2/api",
            json={"status": "0", "message": "NOTOK", "result": "Max rate limit reached"},
        )
        with pytest.raises(DataLayerError) as ei:
            get_erc20_transfers(dl, "0xADDR")
        assert "NOTOK" in str(ei.value)
    finally:
        dl.cache.close()


def test_polygon_replay_serves_cache_without_key(tmp_path, requests_mock):
    """Replay mode never touches the wire, so cached funding data must stay
    servable with NO api key configured (deterministic offline replay)."""
    # Live populate with a key...
    loaded = make_loaded(tmp_path, etherscan_key="SECRETKEY")
    dl_live = build_data_layer(loaded)
    requests_mock.get("https://api.etherscan.io/v2/api", json=ETHERSCAN_OK)
    assert len(get_erc20_transfers(dl_live, "0xADDR")) == 1
    dl_live.cache.close()

    # ...then replay on the same cache WITHOUT the key.
    loaded_nokey = make_loaded(tmp_path, etherscan_key=None)
    dl_replay = build_data_layer(loaded_nokey, replay=True)
    try:
        transfers = get_erc20_transfers(dl_replay, "0xADDR")
        assert len(transfers) == 1
        assert transfers[0].value_normalized == 1.0
    finally:
        dl_replay.cache.close()


def test_kalshi_null_markets_is_loud(dl, requests_mock):
    requests_mock.get(
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        json={"cursor": None, "markets": None},
    )
    with pytest.raises(DataLayerError):
        get_kalshi_markets(dl, limit=5)


# ---------------------------------------------------------------------------
# pagination
# ---------------------------------------------------------------------------


def _mk_trade(i: int, *, bad: bool = False) -> dict:
    d = {
        "proxyWallet": f"0x{i:040x}", "side": "BUY", "conditionId": "0xCID",
        "size": 1.0, "price": 0.5, "timestamp": 1000 + i, "transactionHash": f"0xt{i}",
    }
    if bad:
        del d["price"]  # unusable -> parser drops it
    return d


def test_paginate_continues_past_page_with_drops(dl, requests_mock):
    """A FULL raw page containing an unparseable record must not end the walk —
    terminating on the parsed count would silently truncate trade history."""
    from consensus.sources_polymarket import paginate_market_trades

    page1 = [_mk_trade(0), _mk_trade(1, bad=True), _mk_trade(2)]   # full (3 raw)
    page2 = [_mk_trade(3)]                                          # short -> end
    m = requests_mock.get(
        "https://data-api.polymarket.com/trades",
        [{"json": page1}, {"json": page2}],
    )
    trades = paginate_market_trades(dl, "0xCID", page_size=3)
    assert m.call_count == 2          # walked past the dropped-record page
    assert len(trades) == 3           # 2 parsed from page1 + 1 from page2
    assert {t.timestamp for t in trades} == {1000, 1002, 1003}


def test_paginate_stops_on_short_raw_page(dl, requests_mock):
    from consensus.sources_polymarket import paginate_market_trades

    m = requests_mock.get(
        "https://data-api.polymarket.com/trades", json=[_mk_trade(0), _mk_trade(1)]
    )
    trades = paginate_market_trades(dl, "0xCID", page_size=3)
    assert m.call_count == 1
    assert len(trades) == 2


def test_paginate_max_records_trims_even_short_history(dl, requests_mock):
    from consensus.sources_polymarket import paginate_market_trades

    requests_mock.get(
        "https://data-api.polymarket.com/trades",
        json=[_mk_trade(0), _mk_trade(1), _mk_trade(2)],
    )
    trades = paginate_market_trades(dl, "0xCID", page_size=100, max_records=2)
    assert len(trades) == 2


def test_paginate_wallet_trades_walks_by_user(dl, requests_mock):
    from consensus.sources_polymarket import paginate_wallet_trades

    page1 = [_mk_trade(0), _mk_trade(1), _mk_trade(2)]
    page2 = [_mk_trade(3)]
    m = requests_mock.get(
        "https://data-api.polymarket.com/trades",
        [{"json": page1}, {"json": page2}],
    )
    trades = paginate_wallet_trades(dl, "0xWALLET", page_size=3)
    assert m.call_count == 2
    assert len(trades) == 4
    # Both requests targeted the wallet (user=), not a market.
    for req in m.request_history:
        assert req.qs["user"] == ["0xwallet"]
        assert "market" not in req.qs


# ---------------------------------------------------------------------------
# remaining M1 fetchers
# ---------------------------------------------------------------------------


def test_get_markets_serialises_bool_params(dl, requests_mock):
    from consensus.sources_polymarket import get_markets

    m = requests_mock.get("https://gamma-api.polymarket.com/markets", json=GAMMA_MARKETS)
    markets = get_markets(dl, active=True, closed=False, limit=50)
    assert len(markets) == 1
    qs = m.last_request.qs
    assert qs["active"] == ["true"]
    assert qs["closed"] == ["false"]
    assert qs["limit"] == ["50"]


def test_get_wallet_activity_parses(dl, requests_mock):
    from consensus.sources_polymarket import get_wallet_activity

    requests_mock.get(
        "https://data-api.polymarket.com/activity",
        json=[{
            "proxyWallet": "0xW", "timestamp": 1783729631, "type": "REDEEM",
            "usdcSize": 55.5, "conditionId": "0xCID",
        }],
    )
    acts = get_wallet_activity(dl, "0xW")
    assert len(acts) == 1
    assert acts[0].type == "REDEEM" and acts[0].usdc_size == 55.5
