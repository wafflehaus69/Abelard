"""Parser tests — the Rule-1 boundary: bad records drop to None, never fabricate."""

from __future__ import annotations

from consensus.models import (
    Activity,
    Erc20Transfer,
    KalshiMarket,
    MarketMeta,
    Position,
    Trade,
)


def test_trade_valid_normalises_side_and_wallet():
    t = Trade.from_api({
        "proxyWallet": "0xABC", "side": "buy", "conditionId": "0xC",
        "size": 4.5, "price": 0.23, "timestamp": 1783729631, "outcomeIndex": 1,
    })
    assert t is not None
    assert t.proxy_wallet == "0xabc"  # lowercased
    assert t.side == "BUY"            # uppercased
    assert t.size == 4.5 and t.price == 0.23 and t.timestamp == 1783729631


def test_trade_missing_price_dropped():
    assert Trade.from_api({
        "proxyWallet": "0xABC", "side": "BUY", "conditionId": "0xC",
        "size": 1.0, "timestamp": 1,
    }) is None


def test_trade_zero_price_preserved_not_fabricated():
    t = Trade.from_api({
        "proxyWallet": "0xABC", "side": "BUY", "conditionId": "0xC",
        "size": 1.0, "price": 0, "timestamp": 1,
    })
    assert t is not None and t.price == 0.0


def test_position_valid_and_displayed_pnl_carried():
    p = Position.from_api({
        "proxyWallet": "0xW", "conditionId": "0xC", "asset": "a",
        "size": 100.0, "avgPrice": 0.2, "realizedPnl": 0, "redeemable": False, "mergeable": True,
    })
    assert p is not None
    assert p.size == 100.0 and p.avg_price == 0.2
    assert p.realized_pnl == 0.0
    assert p.redeemable is False and p.mergeable is True


def test_position_missing_size_dropped():
    assert Position.from_api({"proxyWallet": "0xW", "conditionId": "0xC", "asset": "a"}) is None


def test_market_meta_decodes_json_string_outcomes():
    m = MarketMeta.from_api({
        "conditionId": "0xC", "question": "Q?",
        "outcomes": '["Yes", "No"]', "outcomePrices": '["0.2", "0.8"]',
        "volume": "12.5", "liquidity": "3.0", "active": True, "closed": False,
    })
    assert m is not None
    assert m.outcomes == ["Yes", "No"]
    assert m.outcome_prices == [0.2, 0.8]
    assert m.volume == 12.5 and m.active is True


def test_market_meta_partial_prices_drop_to_empty_no_misalignment():
    m = MarketMeta.from_api({
        "conditionId": "0xC", "outcomes": '["Yes","No"]',
        "outcomePrices": '["0.2", "not-a-number"]',
    })
    assert m is not None
    # A misaligned outcome->price mapping would be fabricated; refuse it.
    assert m.outcome_prices == []
    assert m.outcomes == ["Yes", "No"]


def test_market_meta_requires_condition_id():
    assert MarketMeta.from_api({"question": "Q?"}) is None


def test_unparseable_outcomes_logs_a_gap(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="consensus.models"):
        m = MarketMeta.from_api({"conditionId": "0xC", "outcomes": "not json ["})
    assert m is not None and m.outcomes == []
    assert any("unparseable" in r.getMessage() for r in caplog.records)


def test_absent_outcomes_logs_nothing(caplog):
    import logging

    with caplog.at_level(logging.WARNING, logger="consensus.models"):
        m = MarketMeta.from_api({"conditionId": "0xC"})
    assert m is not None and m.outcomes == []
    assert not caplog.records


def test_kalshi_parses_dollar_prices_as_exact_decimal():
    from decimal import Decimal

    m = KalshiMarket.from_api({
        "ticker": "KXT-1", "title": "t", "status": "active",
        "yes_bid_dollars": "0.4500", "yes_ask_dollars": "0.4700",
        "last_price_dollars": "0.4600", "rules_primary": "r",
    })
    assert m is not None
    # Binding rule: Decimal-from-string, never float (addendum v1.1 §2.2).
    assert isinstance(m.yes_bid, Decimal)
    assert m.yes_bid == Decimal("0.45")
    assert m.yes_ask == Decimal("0.47")
    assert m.last_price == Decimal("0.46")
    # Exactness: a float() parse of "0.4500" would not round-trip like this.
    assert str(m.yes_bid) == "0.4500"


def test_kalshi_missing_ticker_dropped():
    assert KalshiMarket.from_api({"title": "t"}) is None


def test_activity_carries_type_and_usdc():
    a = Activity.from_api({
        "proxyWallet": "0xW", "timestamp": 1783729631, "type": "trade",
        "usdcSize": 1.09, "size": 4.5, "price": 0.23, "side": "BUY",
    })
    assert a is not None
    assert a.type == "TRADE" and a.usdc_size == 1.09


def test_erc20_normalises_by_decimals():
    tr = Erc20Transfer.from_api({
        "hash": "0xtx", "from": "0xFROM", "to": "0xTO",
        "value": "1000000", "tokenDecimal": "6", "tokenSymbol": "USDC",
    })
    assert tr is not None
    assert tr.value_raw == 1_000_000
    assert tr.value_normalized == 1.0
    assert tr.from_addr == "0xfrom" and tr.to_addr == "0xto"


def test_erc20_unknown_decimals_leaves_normalized_none():
    tr = Erc20Transfer.from_api({"hash": "0xtx", "from": "0xF", "to": "0xT", "value": "5"})
    assert tr is not None
    assert tr.value_raw == 5
    assert tr.value_normalized is None  # never guessed


def test_erc20_missing_value_dropped():
    assert Erc20Transfer.from_api({"hash": "0xtx", "from": "0xF", "to": "0xT"}) is None
