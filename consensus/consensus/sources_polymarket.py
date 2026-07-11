"""Polymarket international data fetchers (read-only).

data-api  → per-wallet / per-market fills, positions, activity.
gamma-api → market metadata (question, category, outcomes, prices, liquidity).

Every function returns typed models parsed from a cached raw response. None of
this code can place, sign, or stage an order — it only reads (spec Rule 2).
"""

from __future__ import annotations

from typing import Any

from .fetching import DataLayer
from .models import Activity, MarketMeta, Position, Trade

_DATA = "polymarket_data"
_GAMMA = "polymarket_gamma"


def _bool_param(value: bool) -> str:
    return "true" if value else "false"


# ---------------------------------------------------------------------------
# data-api
# ---------------------------------------------------------------------------


def get_market_trades(
    dl: DataLayer, condition_id: str, *, limit: int = 1000, offset: int = 0
) -> list[Trade]:
    """Fills in one market (newest first). One page; use
    :func:`paginate_market_trades` for full history."""
    body = dl.fetch(
        source=_DATA,
        base_url=dl.endpoints.polymarket_data_api,
        endpoint="/trades",
        request_params={"market": condition_id, "limit": limit, "offset": offset},
    )
    return dl.parse_records(body, parser=Trade.from_api, source=_DATA, endpoint="/trades")


def get_wallet_trades(
    dl: DataLayer, proxy: str, *, limit: int = 1000, offset: int = 0
) -> list[Trade]:
    """Fills by one proxy wallet (newest first)."""
    body = dl.fetch(
        source=_DATA,
        base_url=dl.endpoints.polymarket_data_api,
        endpoint="/trades",
        request_params={"user": proxy, "limit": limit, "offset": offset},
    )
    return dl.parse_records(body, parser=Trade.from_api, source=_DATA, endpoint="/trades")


def get_wallet_positions(
    dl: DataLayer, proxy: str, *, limit: int | None = None
) -> list[Position]:
    """Current holdings for one proxy wallet."""
    params: dict[str, Any] = {"user": proxy}
    if limit is not None:
        params["limit"] = limit
    body = dl.fetch(
        source=_DATA,
        base_url=dl.endpoints.polymarket_data_api,
        endpoint="/positions",
        request_params=params,
    )
    return dl.parse_records(body, parser=Position.from_api, source=_DATA, endpoint="/positions")


def get_wallet_activity(
    dl: DataLayer, proxy: str, *, limit: int = 500, offset: int = 0
) -> list[Activity]:
    """Activity feed for one proxy wallet (trades + redemptions/merges/etc.)."""
    body = dl.fetch(
        source=_DATA,
        base_url=dl.endpoints.polymarket_data_api,
        endpoint="/activity",
        request_params={"user": proxy, "limit": limit, "offset": offset},
    )
    return dl.parse_records(body, parser=Activity.from_api, source=_DATA, endpoint="/activity")


def paginate_market_trades(
    dl: DataLayer,
    condition_id: str,
    *,
    page_size: int = 1000,
    max_records: int | None = None,
) -> list[Trade]:
    """Walk ``/trades`` by offset until a short page (end of history) or
    ``max_records``. Each page is cached independently, so a later replay
    reproduces the exact same walk.

    Termination is decided on the RAW upstream page length, never the parsed
    count: a full page containing an unparseable (dropped) record is still a
    full page of history, and ending the walk there would silently truncate
    the audit-critical trade record (Rule 1). The drops themselves are logged
    as gaps by ``parse_records``.
    """
    out: list[Trade] = []
    offset = 0
    while True:
        raw = dl.fetch(
            source=_DATA,
            base_url=dl.endpoints.polymarket_data_api,
            endpoint="/trades",
            request_params={"market": condition_id, "limit": page_size, "offset": offset},
        )
        page = dl.parse_records(raw, parser=Trade.from_api, source=_DATA, endpoint="/trades")
        out.extend(page)
        if max_records is not None and len(out) >= max_records:
            del out[max_records:]
            break
        if len(raw) < page_size:  # parse_records guarantees raw is a list
            break
        offset += page_size
    return out


def paginate_wallet_trades(
    dl: DataLayer,
    proxy: str,
    *,
    page_size: int = 1000,
    max_records: int | None = None,
) -> list[Trade]:
    """Walk a wallet's ``/trades`` by offset until a short RAW page or
    ``max_records`` — same raw-length termination rule as
    :func:`paginate_market_trades` (a full page with dropped records is still a
    full page of history; ending there would truncate the audit record).

    Exhausting a wallet's history is how factor F derives "first Polymarket
    trade" (M0-F freshness, source (a))."""
    out: list[Trade] = []
    offset = 0
    while True:
        raw = dl.fetch(
            source=_DATA,
            base_url=dl.endpoints.polymarket_data_api,
            endpoint="/trades",
            request_params={"user": proxy, "limit": page_size, "offset": offset},
        )
        page = dl.parse_records(raw, parser=Trade.from_api, source=_DATA, endpoint="/trades")
        out.extend(page)
        if max_records is not None and len(out) >= max_records:
            del out[max_records:]
            break
        if len(raw) < page_size:  # parse_records guarantees raw is a list
            break
        offset += page_size
    return out


# ---------------------------------------------------------------------------
# gamma-api
# ---------------------------------------------------------------------------


def get_market_meta(
    dl: DataLayer, condition_id: str, *, closed: bool | None = None
) -> MarketMeta | None:
    """Metadata for a single market by condition id. Returns ``None`` only if
    gamma knows no such market at all (a legitimate empty result, not an error).

    Gamma quirk (verified 2026-07-10): ``/markets?condition_ids=`` silently
    filters to OPEN markets by default — a resolved market returns ``[]`` unless
    ``closed=true`` is passed. With ``closed=None`` (the default) this helper
    queries open first, then closed, so callers get metadata for any market —
    resolved-market metadata is exactly what the backtests need.
    """
    if closed is None:
        param_sets: list[dict[str, Any]] = [
            {"condition_ids": condition_id},
            {"condition_ids": condition_id, "closed": "true"},
        ]
    elif closed:
        param_sets = [{"condition_ids": condition_id, "closed": "true"}]
    else:
        param_sets = [{"condition_ids": condition_id}]

    for params in param_sets:
        body = dl.fetch(
            source=_GAMMA,
            base_url=dl.endpoints.polymarket_gamma_api,
            endpoint="/markets",
            request_params=params,
        )
        metas = dl.parse_records(
            body, parser=MarketMeta.from_api, source=_GAMMA, endpoint="/markets"
        )
        if metas:
            return metas[0]
    return None


def get_markets(
    dl: DataLayer,
    *,
    active: bool = True,
    closed: bool = False,
    limit: int = 200,
    order: str = "volume",
    ascending: bool = False,
    category: str | None = None,
) -> list[MarketMeta]:
    """List markets by the gamma filter set (used by the M2 universe builder)."""
    params: dict[str, Any] = {
        "active": _bool_param(active),
        "closed": _bool_param(closed),
        "limit": limit,
        "order": order,
        "ascending": _bool_param(ascending),
    }
    if category is not None:
        params["category"] = category
    body = dl.fetch(
        source=_GAMMA,
        base_url=dl.endpoints.polymarket_gamma_api,
        endpoint="/markets",
        request_params=params,
    )
    return dl.parse_records(body, parser=MarketMeta.from_api, source=_GAMMA, endpoint="/markets")
