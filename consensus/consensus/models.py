"""Typed records parsed from raw upstream responses.

Every model has a ``from_api(dict) -> Model | None`` classmethod that parses
*defensively*: if a field that makes the record meaningful is missing or
unparseable, it returns ``None`` so the caller can count a gap and move on
(spec Rule 1 — "log the gap and move on", never fabricate). Optional numeric
fields parse to ``None`` when absent rather than a fabricated ``0.0``; a real
zero from upstream is preserved.

These parsed views are for computation. The raw responses they came from are
persisted verbatim by ``cache.py`` — that on-disk record, not this object, is
the Rule-1 audit trail a signal traces back to.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

_log = logging.getLogger("consensus.models")


def _opt_float(value: Any) -> float | None:
    """Parse to float, or None if absent/unparseable. Preserves a real 0.0."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _opt_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        # Accept "123", 123, 123.0 — but not "12.3" as an int silently.
        return int(value)
    except (TypeError, ValueError):
        try:
            f = float(value)
        except (TypeError, ValueError):
            return None
        return int(f) if f.is_integer() else None


def _opt_decimal(value: Any) -> Decimal | None:
    """Exact decimal from a decimal-string field, or None if absent/unparseable.

    Binding rule (addendum v1.1 §2.2): venue price fields published as decimal
    STRINGS (Kalshi ``*_dollars``) are parsed with Decimal, never float() —
    floats corrupt exact venue prices, and venue prices are what the owner's
    PnL settles on.
    """
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _opt_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
    return None


def _json_list(value: Any) -> list[Any]:
    """Gamma encodes ``outcomes``/``outcomePrices`` as JSON *strings*. Decode to a
    list; on anything present-but-unparseable, log the gap and return [] (a
    warning, not a crash — and never a guessed value). An absent field (None)
    is normal and logs nothing."""
    if isinstance(value, list):
        return value
    if value is None:
        return []
    if not isinstance(value, str):
        _log.warning("gamma list field: expected JSON string, got %s", type(value).__name__)
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        _log.warning("gamma list field: unparseable JSON string %.80r", value)
        return []
    if not isinstance(parsed, list):
        _log.warning("gamma list field: decoded to %s, not a list", type(parsed).__name__)
        return []
    return parsed


# ---------------------------------------------------------------------------
# Polymarket data-api
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Trade:
    """One fill from data-api ``/trades``. Identity keys on ``proxy_wallet``
    (the Polymarket proxy contract, not the human — spec §2)."""

    proxy_wallet: str
    condition_id: str
    side: str  # "BUY" | "SELL"
    size: float
    price: float
    timestamp: int  # unix seconds
    asset: str | None = None  # ERC-1155 outcome-token id
    outcome: str | None = None
    outcome_index: int | None = None
    transaction_hash: str | None = None
    title: str | None = None
    slug: str | None = None
    name: str | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "Trade | None":
        proxy = _opt_str(d.get("proxyWallet"))
        cond = _opt_str(d.get("conditionId"))
        side = _opt_str(d.get("side"))
        size = _opt_float(d.get("size"))
        price = _opt_float(d.get("price"))
        ts = _opt_int(d.get("timestamp"))
        # A fill is meaningless without who/where/how-much/when. Missing any of
        # these → drop the record (gap), do not invent.
        if not (proxy and cond and side) or size is None or price is None or ts is None:
            return None
        return cls(
            proxy_wallet=proxy.lower(),
            condition_id=cond,
            side=side.upper(),
            size=size,
            price=price,
            timestamp=ts,
            asset=_opt_str(d.get("asset")),
            outcome=_opt_str(d.get("outcome")),
            outcome_index=_opt_int(d.get("outcomeIndex")),
            transaction_hash=_opt_str(d.get("transactionHash")),
            title=_opt_str(d.get("title")),
            slug=_opt_str(d.get("slug")),
            name=_opt_str(d.get("name")),
        )


@dataclass(frozen=True)
class Position:
    """A wallet's current holding from data-api ``/positions``.

    WARNING: ``realized_pnl``/``cash_pnl``/``percent_pnl`` are Polymarket's own
    *displayed* stats. Per spec Rule 1 and §M3 they are diagnostic cross-checks
    ONLY — skill metrics are recomputed from raw resolved trades, never ranked
    from these fields.
    """

    proxy_wallet: str
    condition_id: str
    asset: str
    size: float
    avg_price: float | None = None
    initial_value: float | None = None
    current_value: float | None = None
    cash_pnl: float | None = None
    percent_pnl: float | None = None
    total_bought: float | None = None
    realized_pnl: float | None = None
    percent_realized_pnl: float | None = None
    cur_price: float | None = None
    redeemable: bool | None = None
    mergeable: bool | None = None
    title: str | None = None
    slug: str | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "Position | None":
        proxy = _opt_str(d.get("proxyWallet"))
        cond = _opt_str(d.get("conditionId"))
        asset = _opt_str(d.get("asset"))
        size = _opt_float(d.get("size"))
        if not (proxy and cond and asset) or size is None:
            return None
        return cls(
            proxy_wallet=proxy.lower(),
            condition_id=cond,
            asset=asset,
            size=size,
            avg_price=_opt_float(d.get("avgPrice")),
            initial_value=_opt_float(d.get("initialValue")),
            current_value=_opt_float(d.get("currentValue")),
            cash_pnl=_opt_float(d.get("cashPnl")),
            percent_pnl=_opt_float(d.get("percentPnl")),
            total_bought=_opt_float(d.get("totalBought")),
            realized_pnl=_opt_float(d.get("realizedPnl")),
            percent_realized_pnl=_opt_float(d.get("percentRealizedPnl")),
            cur_price=_opt_float(d.get("curPrice")),
            redeemable=_opt_bool(d.get("redeemable")),
            mergeable=_opt_bool(d.get("mergeable")),
            title=_opt_str(d.get("title")),
            slug=_opt_str(d.get("slug")),
        )


@dataclass(frozen=True)
class Activity:
    """A row from data-api ``/activity`` — a superset of trades that also carries
    ``type`` (TRADE/REDEEM/…) and ``usdc_size`` (USDC notional), used by the
    unusual-activity scan (M10) for size scoring."""

    proxy_wallet: str
    timestamp: int
    type: str
    condition_id: str | None = None
    size: float | None = None
    usdc_size: float | None = None
    price: float | None = None
    side: str | None = None
    outcome: str | None = None
    outcome_index: int | None = None
    asset: str | None = None
    transaction_hash: str | None = None
    title: str | None = None
    slug: str | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "Activity | None":
        proxy = _opt_str(d.get("proxyWallet"))
        ts = _opt_int(d.get("timestamp"))
        typ = _opt_str(d.get("type"))
        if not (proxy and typ) or ts is None:
            return None
        return cls(
            proxy_wallet=proxy.lower(),
            timestamp=ts,
            type=typ.upper(),
            condition_id=_opt_str(d.get("conditionId")),
            size=_opt_float(d.get("size")),
            usdc_size=_opt_float(d.get("usdcSize")),
            price=_opt_float(d.get("price")),
            side=_opt_str(d.get("side")),
            outcome=_opt_str(d.get("outcome")),
            outcome_index=_opt_int(d.get("outcomeIndex")),
            asset=_opt_str(d.get("asset")),
            transaction_hash=_opt_str(d.get("transactionHash")),
            title=_opt_str(d.get("title")),
            slug=_opt_str(d.get("slug")),
        )


# ---------------------------------------------------------------------------
# Polymarket gamma-api
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MarketMeta:
    """Market metadata from gamma-api ``/markets``. ``outcomes``/``outcome_prices``
    arrive JSON-string-encoded upstream and are decoded here."""

    condition_id: str
    market_id: str | None = None
    question: str | None = None
    slug: str | None = None
    category: str | None = None
    outcomes: list[str] = field(default_factory=list)
    outcome_prices: list[float] = field(default_factory=list)
    volume: float | None = None
    volume_24h: float | None = None
    liquidity: float | None = None
    active: bool | None = None
    closed: bool | None = None
    start_date: str | None = None
    end_date: str | None = None
    description: str | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "MarketMeta | None":
        cond = _opt_str(d.get("conditionId"))
        if not cond:
            return None
        raw_outcomes = _json_list(d.get("outcomes"))
        raw_prices = _json_list(d.get("outcomePrices"))
        outcomes = [str(o) for o in raw_outcomes]
        prices: list[float] = []
        for p in raw_prices:
            f = _opt_float(p)
            if f is not None:
                prices.append(f)
        # If prices were present but partially unparseable, drop to [] rather
        # than emit a misaligned outcome/price mapping (would be fabricated data).
        if raw_prices and len(prices) != len(raw_prices):
            prices = []
        return cls(
            condition_id=cond,
            market_id=_opt_str(d.get("id")),
            question=_opt_str(d.get("question")),
            slug=_opt_str(d.get("slug")),
            category=_opt_str(d.get("category")),
            outcomes=outcomes,
            outcome_prices=prices,
            volume=_opt_float(d.get("volume")),
            volume_24h=_opt_float(d.get("volume24hr")),
            liquidity=_opt_float(d.get("liquidity")),
            active=_opt_bool(d.get("active")),
            closed=_opt_bool(d.get("closed")),
            start_date=_opt_str(d.get("startDate")),
            end_date=_opt_str(d.get("endDate")),
            description=_opt_str(d.get("description")),
        )


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KalshiMarket:
    """A Kalshi market (execution-venue side). The current elections-host schema
    prices in ``*_dollars`` strings — parsed as exact :class:`Decimal`, never
    float (addendum v1.1 §2.2). ``rules_primary``/``rules_secondary`` carry the
    resolution text the M7 crosswalk needs to judge resolution-match.

    ``*_fp`` fixed-point size/volume fields are NOT parsed here: their scale is
    unverified, so until it is empirically confirmed (gated before M7/M8 math)
    they are display-only and live in the cached raw response, not the model.
    """

    ticker: str
    event_ticker: str | None = None
    title: str | None = None
    yes_sub_title: str | None = None
    no_sub_title: str | None = None
    status: str | None = None
    result: str | None = None
    yes_bid: Decimal | None = None  # dollars, exact
    yes_ask: Decimal | None = None
    no_bid: Decimal | None = None
    no_ask: Decimal | None = None
    last_price: Decimal | None = None
    open_time: str | None = None
    close_time: str | None = None
    rules_primary: str | None = None
    rules_secondary: str | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "KalshiMarket | None":
        ticker = _opt_str(d.get("ticker"))
        if not ticker:
            return None
        return cls(
            ticker=ticker,
            event_ticker=_opt_str(d.get("event_ticker")),
            title=_opt_str(d.get("title")),
            yes_sub_title=_opt_str(d.get("yes_sub_title")),
            no_sub_title=_opt_str(d.get("no_sub_title")),
            status=_opt_str(d.get("status")),
            result=_opt_str(d.get("result")),
            yes_bid=_opt_decimal(d.get("yes_bid_dollars")),
            yes_ask=_opt_decimal(d.get("yes_ask_dollars")),
            no_bid=_opt_decimal(d.get("no_bid_dollars")),
            no_ask=_opt_decimal(d.get("no_ask_dollars")),
            last_price=_opt_decimal(d.get("last_price_dollars")),
            open_time=_opt_str(d.get("open_time")),
            close_time=_opt_str(d.get("close_time")),
            rules_primary=_opt_str(d.get("rules_primary")),
            rules_secondary=_opt_str(d.get("rules_secondary")),
        )


# ---------------------------------------------------------------------------
# Polygon chain (Etherscan V2, chainid=137) — feeds the M5 funding graph
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Erc20Transfer:
    """An ERC-20 transfer (USDC.e / pUSD) from the Etherscan V2 ``tokentx``
    action (Polygon, chainid 137). Schema is unchanged from the V1 shape.

    ``value_normalized`` divides the raw integer ``value`` by ``10**decimals``;
    if decimals is unknown it stays ``None`` (never guessed) so downstream code
    can tell a real amount from a missing one (Rule 1)."""

    tx_hash: str
    from_addr: str
    to_addr: str
    value_raw: int
    timestamp: int | None = None
    block_number: int | None = None
    token_symbol: str | None = None
    token_decimals: int | None = None
    contract_address: str | None = None
    value_normalized: float | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "Erc20Transfer | None":
        tx = _opt_str(d.get("hash"))
        frm = _opt_str(d.get("from"))
        to = _opt_str(d.get("to"))
        value_raw = _opt_int(d.get("value"))
        if not (tx and frm and to) or value_raw is None:
            return None
        decimals = _opt_int(d.get("tokenDecimal"))
        normalized = value_raw / (10 ** decimals) if decimals is not None else None
        return cls(
            tx_hash=tx,
            from_addr=frm.lower(),
            to_addr=to.lower(),
            value_raw=value_raw,
            timestamp=_opt_int(d.get("timeStamp")),
            block_number=_opt_int(d.get("blockNumber")),
            token_symbol=_opt_str(d.get("tokenSymbol")),
            token_decimals=decimals,
            contract_address=(_opt_str(d.get("contractAddress")) or "").lower() or None,
            value_normalized=normalized,
        )
