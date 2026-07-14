"""Shared fixtures and sample payloads for the CONSENSUS test suite.

Every test is hermetic: no network (requests_mock intercepts), and the cache
lives in a per-test tmp dir. Sample payloads mirror the real API shapes verified
live on 2026-07-10 (see the reference memo), trimmed to the fields the parsers
read.
"""

from __future__ import annotations

import copy

import pytest
import yaml

from consensus.cache import RawCache
from consensus.config import Config, LoadedConfig, Secrets
from consensus.fetching import build_data_layer

BASE_CONFIG: dict = {
    "meta": {"regime_floor_date": "2026-06-01"},
    "logging": {"level": "WARNING"},
    "categories": {"targets": ["geopolitics"]},
    "data_layer": {
        "cache_path": "cache.db",
        "http": {"user_agent": "test-ua", "timeout": 5.0, "max_retries": 2, "base_backoff": 0.0},
        "endpoints": {
            "polymarket_data_api": "https://data-api.polymarket.com",
            "polymarket_gamma_api": "https://gamma-api.polymarket.com",
            "polymarket_clob_api": "https://clob.polymarket.com",
            "kalshi_api": "https://api.elections.kalshi.com/trade-api/v2",
            "etherscan_v2_api": "https://api.etherscan.io/v2/api",
            "goldsky_subgraph": ("https://api.goldsky.com/api/public/"
                                 "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
                                 "orderbook-subgraph/prod/gn"),
        },
        "smoke": {
            "market_condition_id": "0xCID",
            "wallet_proxy": "0xWALLET",
            "kalshi_markets_limit": 3,
        },
    },
    "collector": {
        "tape_path": "l2_tape.db",
        "tags": ["geopolitics"],
        "enumeration_interval_minutes": 30,
        "gamma_page_limit": 100,
        "request_spacing_ms": 0,  # no politeness sleeps in tests
        "page_size": 1000,
        "max_pages": 4,
        "global_lane_enabled": True,
        "envelope_log": None,
        "tiers": {
            "hot_interval_minutes": 2,
            "quiet_interval_minutes": 15,
            "dormant_interval_minutes": 60,
            "hot_threshold_new_fills": 50,
            "hot_ttl_minutes": 30,
            "quiet_if_fill_within_hours": 24,
        },
    },
}

# ---- sample upstream payloads --------------------------------------------

TRADES = [
    {
        "proxyWallet": "0xEEB69A3468FEDB81D1073B4187BB8B50F1D2CC67",
        "side": "BUY", "asset": "7953...", "conditionId": "0xCID",
        "size": 4.521738, "price": 0.2299998363, "timestamp": 1783729631,
        "title": "Will BTC be above $62,000 on July 17?", "slug": "btc-62k",
        "outcome": "No", "outcomeIndex": 1, "transactionHash": "0xhash1", "name": "Darthc0der",
    },
    {
        "proxyWallet": "0xeeb69a3468fedb81d1073b4187bb8b50f1d2cc67",
        "side": "sell", "conditionId": "0xCID",
        "size": 16.08, "price": 0.49, "timestamp": 1783729400,
        "transactionHash": "0xhash2",
    },
    # unusable: missing price -> dropped, never fabricated
    {"proxyWallet": "0xabc", "side": "BUY", "conditionId": "0xCID", "size": 1.0, "timestamp": 1},
]

POSITIONS = [
    {
        "proxyWallet": "0xWALLET", "asset": "7953...", "conditionId": "0xCID",
        "size": 11480.4975, "avgPrice": 0.229, "initialValue": 2629.1142,
        "currentValue": 2353.5019, "cashPnl": -275.6123, "percentPnl": -10.483,
        "totalBought": 11462.4105, "realizedPnl": 0, "curPrice": 0.205,
        "redeemable": False, "mergeable": True, "title": "BTC 62k",
    },
]

GAMMA_MARKETS = [
    {
        "id": "2871319", "conditionId": "0xCID",
        "question": "Will the price of Bitcoin be above $62,000 on July 17?",
        "slug": "btc-62k", "category": "crypto",
        "outcomes": '["Yes", "No"]', "outcomePrices": '["0.205", "0.795"]',
        "volume": "12345.6", "volume24hr": "234.5", "liquidity": "13017.9102",
        "active": True, "closed": False,
        "startDate": "2026-07-10T16:00:31Z", "endDate": "2026-07-17T16:00:00Z",
        "description": "Resolves Yes if ...",
        "clobTokenIds": '["111000111", "222000222"]',
    },
]

SUBGRAPH_URL = ("https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw"
                "/subgraphs/orderbook-subgraph/prod/gn")


def subgraph_event(i: int, *, ts: int | None = None, asset: str = "111000111") -> dict:
    return {
        "id": f"0x{i:04x}tx_0x{i:04x}order", "timestamp": str(ts if ts is not None else 1000 + i),
        "maker": f"0xMAKER{i}", "taker": "0xE111", "makerAssetId": "0",
        "takerAssetId": asset, "makerAmountFilled": str(220_000 + i),
        "takerAmountFilled": "1000000", "fee": "0",
    }


def subgraph_meta_body() -> dict:
    return {
        "_meta": {"block": {"number": 87_814_766}, "hasIndexingErrors": False},
        "newest": [{"timestamp": "1777374040"}],
        "oldest": [{"timestamp": "1669060209"}],
    }

KALSHI_ENV = {
    "cursor": "abc",
    "markets": [
        {
            "ticker": "KXTEST-1", "event_ticker": "KXTEST",
            "title": "Test market", "status": "active",
            "yes_bid_dollars": "0.4500", "yes_ask_dollars": "0.4700",
            "no_bid_dollars": "0.5300", "no_ask_dollars": "0.5500",
            "last_price_dollars": "0.4600", "close_time": "2026-07-24T23:30:00Z",
            "open_time": "2026-07-01T00:00:00Z",
            "rules_primary": "Resolves yes if ...", "rules_secondary": "",
        },
        {"no_ticker_here": True},  # unusable -> dropped
    ],
}

ETHERSCAN_OK = {
    "status": "1", "message": "OK",
    "result": [
        {
            "blockNumber": "12345", "timeStamp": "1700000000", "hash": "0xtx",
            "from": "0xFROM", "to": "0xTO", "value": "1000000",
            "tokenSymbol": "USDC", "tokenDecimal": "6",
            "contractAddress": "0xCONTRACT",
        },
    ],
}

ETHERSCAN_EMPTY = {"status": "0", "message": "No transactions found", "result": []}


# ---- fixtures -------------------------------------------------------------


def make_loaded(config_dir, *, etherscan_key=None, log_level=None, overrides=None) -> LoadedConfig:
    data = copy.deepcopy(BASE_CONFIG)
    if overrides:
        data.update(overrides)
    cfg = Config.model_validate(data)
    secrets = Secrets(etherscan_api_key=etherscan_key, log_level_override=log_level)
    return LoadedConfig(cfg, config_dir=config_dir, secrets=secrets)


@pytest.fixture
def loaded(tmp_path):
    return make_loaded(tmp_path)


@pytest.fixture
def dl(loaded):
    d = build_data_layer(loaded)
    yield d
    d.cache.close()


@pytest.fixture
def cache(tmp_path):
    c = RawCache(tmp_path / "c.db")
    yield c
    c.close()


@pytest.fixture
def config_file(tmp_path):
    """A real on-disk config.yaml (cache path resolves under tmp_path)."""
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(BASE_CONFIG), encoding="utf-8")
    return path
