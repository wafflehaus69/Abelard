"""fetch_quote — current price + day range + 52-week range for a ticker.

Two Finnhub calls:
  /quote          — primary, authoritative for price and day range
  /stock/metric   — secondary, 52-week high/low

Failure semantics follow the "fail loudly but partially" pattern:
  - Primary fails  → status=error/rate_limited/not_found, completeness=none.
  - Primary ok, secondary fails → status=ok, completeness=partial, degraded
    fields carry structured warnings with reason codes.
  - Volume is never returned (Finnhub free-tier /quote lacks it) — a
    standing info warning documents the permanent gap. Does not downgrade
    completeness on its own; Abelard filters by reason if he cares.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import Config
from .envelope import Completeness, build_error, build_ok, make_warning
from .http_client import HttpClient, NotFound, RateLimited, TransportError


FINNHUB_BASE = "https://finnhub.io/api/v1"

_log = logging.getLogger("research_daemon.fetch_quote")


def fetch_quote(
    ticker: str,
    *,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Return a quote envelope for `ticker`."""
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(
            status="error",
            source="finnhub",
            detail="ticker must be a non-empty string",
        )

    symbol = ticker.strip().upper()
    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    try:
        quote_payload = http.get_json(
            f"{FINNHUB_BASE}/quote",
            params={"symbol": symbol, "token": cfg.finnhub_api_key},
        )
    except NotFound:
        return build_error(
            status="not_found", source="finnhub", detail=f"ticker {symbol} not found"
        )
    except RateLimited as exc:
        return build_error(status="rate_limited", source="finnhub", detail=str(exc))
    except TransportError as exc:
        return build_error(status="error", source="finnhub", detail=str(exc))

    if not _has_quote_data(quote_payload):
        return build_error(
            status="not_found",
            source="finnhub",
            detail=f"no quote data for ticker {symbol}",
        )

    data: dict[str, Any] = {
        "ticker": symbol,
        "price": quote_payload.get("c"),
        "change": quote_payload.get("d"),
        "change_pct": quote_payload.get("dp"),
        "day_open": quote_payload.get("o"),
        "day_high": quote_payload.get("h"),
        "day_low": quote_payload.get("l"),
        "previous_close": quote_payload.get("pc"),
        "quote_time_unix": quote_payload.get("t"),
        "volume": None,
        "week_52_high": None,
        "week_52_low": None,
    }

    warnings: list[dict[str, Any]] = [
        make_warning(
            field="volume",
            reason="not_available_on_free_tier",
            source="finnhub",
            suggestion="upgrade Finnhub or add a secondary source (e.g. yfinance)",
        ),
    ]

    completeness: Completeness = "complete"

    try:
        metric_payload = http.get_json(
            f"{FINNHUB_BASE}/stock/metric",
            params={"symbol": symbol, "metric": "price", "token": cfg.finnhub_api_key},
        )
        metric = (metric_payload or {}).get("metric") or {}
        data["week_52_high"] = metric.get("52WeekHigh")
        data["week_52_low"] = metric.get("52WeekLow")
        for field in ("week_52_high", "week_52_low"):
            if data[field] is None:
                warnings.append(
                    make_warning(field=field, reason="missing_field", source="finnhub")
                )
                completeness = "partial"
    except RateLimited as exc:
        _log.warning("secondary /stock/metric rate-limited for %s: %s", symbol, exc)
        for field in ("week_52_high", "week_52_low"):
            warnings.append(
                make_warning(
                    field=field,
                    reason="rate_limited",
                    source="finnhub",
                    suggestion="retry after backoff window",
                )
            )
        completeness = "partial"
    except TransportError as exc:
        _log.warning("secondary /stock/metric failed for %s: %s", symbol, exc)
        for field in ("week_52_high", "week_52_low"):
            warnings.append(
                make_warning(field=field, reason="upstream_error", source="finnhub")
            )
        completeness = "partial"
    except NotFound as exc:
        _log.warning("secondary /stock/metric 404 for %s: %s", symbol, exc)
        for field in ("week_52_high", "week_52_low"):
            warnings.append(
                make_warning(field=field, reason="not_found", source="finnhub")
            )
        completeness = "partial"

    return build_ok(
        data,
        source="finnhub",
        data_completeness=completeness,
        warnings=warnings,
    )


def _has_quote_data(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(payload.get(k) for k in ("c", "h", "l", "o", "pc"))
