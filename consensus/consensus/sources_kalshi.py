"""Kalshi public market-data fetchers (read-only).

Phases 1–4 touch ONLY public market data — no authenticated endpoints, no
trading (auth enters at M9, gated). Kalshi wraps its list responses in a
``{"cursor": ..., "markets": [...]}`` envelope, unlike Polymarket's bare arrays,
so the list is extracted before parsing.
"""

from __future__ import annotations

from typing import Any

from .errors import DataLayerError
from .fetching import DataLayer
from .models import KalshiMarket

_KALSHI = "kalshi"


def get_kalshi_markets(
    dl: DataLayer,
    *,
    limit: int = 100,
    status: str | None = None,
    event_ticker: str | None = None,
    series_ticker: str | None = None,
    cursor: str | None = None,
) -> list[KalshiMarket]:
    """List Kalshi markets. Optional filters map to the v2 ``/markets`` query
    params. Returns one page; the response cursor is not surfaced yet (added when
    a consumer needs full pagination)."""
    params: dict[str, Any] = {"limit": limit}
    if status is not None:
        params["status"] = status
    if event_ticker is not None:
        params["event_ticker"] = event_ticker
    if series_ticker is not None:
        params["series_ticker"] = series_ticker
    if cursor is not None:
        params["cursor"] = cursor

    body = dl.fetch(
        source=_KALSHI,
        base_url=dl.endpoints.kalshi_api,
        endpoint="/markets",
        request_params=params,
    )
    if not isinstance(body, dict):
        raise DataLayerError(
            f"{_KALSHI}/markets: expected an object with a 'markets' array, "
            f"got {type(body).__name__}",
            source=_KALSHI,
        )
    raw_markets = body.get("markets")
    if not isinstance(raw_markets, list):
        # A missing or null 'markets' is a malformed response, not an empty
        # result — surfacing it as [] would silently swallow the breakage.
        raise DataLayerError(
            f"{_KALSHI}/markets: 'markets' is not an array "
            f"(got {type(raw_markets).__name__})",
            source=_KALSHI,
        )
    return dl.parse_records(
        raw_markets, parser=KalshiMarket.from_api, source=_KALSHI, endpoint="/markets"
    )
