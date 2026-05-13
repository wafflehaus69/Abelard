"""fetch_insider_transactions — Form 4 insider buys/sells within a day window.

Single Finnhub call: /stock/insider-transactions?symbol=X&from=...&to=...
The Finnhub payload is `{"symbol": ..., "data": [ {...}, ... ]}`.

Per-item schema (stable — Abelard iterates in batches across capabilities):

    {
      "insider_name":       str,         # required, non-empty (e.g. "COOK TIMOTHY D")
      "insider_role":       str | null,  # position/title; null if absent
      "transaction_code":   str,         # raw SEC Form 4 code (e.g. "S", "P", "A")
      "transaction_type":   str,         # mapped lowercase; "other" for unmapped codes
      "shares":             int,         # signed; sign indicates direction (+ buy, - sell)
      "shares_held_after":  int | null,  # position size after this transaction
      "price_per_share":    float | null, # 0.0 preserved (grants/gifts); null if absent/unparseable
      "currency":           str | null,  # usually "USD"
      "is_derivative":      bool,        # defaults to false when absent
      "transacted_at_unix": int,         # midnight UTC of transactionDate
      "transacted_at":      str,         # ISO-8601 UTC, midnight ("YYYY-MM-DDT00:00:00Z")
      "filed_at_unix":      int,         # midnight UTC of filingDate
      "filed_at":           str,         # ISO-8601 UTC, midnight
    }

Required for parsing (drop item if missing): insider_name, transaction_code,
change (→shares), transactionDate, filingDate. Drops increment
`data.dropped_count` and add a single envelope-level `parse_error` warning.

Date precision: Form 4 upstream timestamps are date-only, so `transacted_at`
and `filed_at` are always midnight UTC for that date. Abelard should not
read sub-day precision into these fields.

transaction_type mapping is conservative — only the most common SEC Form 4
codes are mapped. `transaction_code` carries the raw letter for any finer-
grained filtering Abelard needs.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .config import Config
from .envelope import Completeness, build_error, build_ok, make_warning
from .http_client import HttpClient, NotFound, RateLimited, TransportError


FINNHUB_BASE = "https://finnhub.io/api/v1"
MIN_DAYS = 1
MAX_DAYS = 365

# Conservative map of SEC Form 4 general transaction codes to normalised
# lowercase labels. Extend as needed; unmapped codes fall through to "other".
_CODE_TO_TYPE: dict[str, str] = {
    "P": "purchase",
    "S": "sale",
    "A": "award",
    "G": "gift",
    "D": "disposition",
    "F": "tax_payment",
    "M": "option_exercise",
    "X": "option_exercise",
    "C": "conversion",
}

_log = logging.getLogger("research_daemon.fetch_insider_transactions")


def fetch_insider_transactions(
    ticker: str,
    days: int = 30,
    *,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Return an insider-transactions envelope for `ticker` over the last `days` days."""
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(
            status="error",
            source="finnhub",
            detail="ticker must be a non-empty string",
        )
    if not isinstance(days, int) or isinstance(days, bool):
        return build_error(
            status="error", source="finnhub", detail="days must be an integer"
        )
    if days < MIN_DAYS or days > MAX_DAYS:
        return build_error(
            status="error",
            source="finnhub",
            detail=f"days must be between {MIN_DAYS} and {MAX_DAYS}",
        )

    symbol = ticker.strip().upper()
    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    today = _today_utc()
    from_date = today - timedelta(days=days)

    try:
        payload = http.get_json(
            f"{FINNHUB_BASE}/stock/insider-transactions",
            params={
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": today.isoformat(),
                "token": cfg.finnhub_api_key,
            },
        )
    except NotFound:
        return build_error(
            status="not_found", source="finnhub", detail=f"ticker {symbol} not found"
        )
    except RateLimited as exc:
        return build_error(status="rate_limited", source="finnhub", detail=str(exc))
    except TransportError as exc:
        return build_error(status="error", source="finnhub", detail=str(exc))

    raw_items = _extract_data_list(payload)
    if raw_items is None:
        return build_error(
            status="error",
            source="finnhub",
            detail="expected {data: [...]} from /stock/insider-transactions",
        )

    items: list[dict[str, Any]] = []
    dropped = 0
    for raw in raw_items:
        parsed = _parse_item(raw)
        if parsed is None:
            dropped += 1
        else:
            items.append(parsed)

    data: dict[str, Any] = {
        "ticker": symbol,
        "window_days": days,
        "from_date": from_date.isoformat(),
        "to_date": today.isoformat(),
        "item_count": len(items),
        "dropped_count": dropped,
        "items": items,
    }

    warnings: list[dict[str, Any]] = []
    completeness: Completeness = "complete"
    if dropped > 0:
        _log.warning(
            "dropped %d malformed insider transaction(s) for %s (%s..%s)",
            dropped, symbol, from_date.isoformat(), today.isoformat(),
        )
        warnings.append(
            make_warning(
                field="items",
                reason="parse_error",
                source="finnhub",
                suggestion=(
                    f"{dropped} upstream item(s) dropped; see data.dropped_count"
                ),
            )
        )
        completeness = "partial"

    return build_ok(
        data,
        source="finnhub",
        data_completeness=completeness,
        warnings=warnings,
    )


def _extract_data_list(payload: Any) -> list[Any] | None:
    if not isinstance(payload, dict):
        return None
    inner = payload.get("data")
    if not isinstance(inner, list):
        return None
    return inner


def _parse_item(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    name = raw.get("name")
    code = raw.get("transactionCode")
    change = raw.get("change")
    if not (isinstance(name, str) and name.strip()):
        return None
    if not (isinstance(code, str) and code.strip()):
        return None
    if not isinstance(change, (int, float)) or isinstance(change, bool):
        return None

    transacted = _parse_finnhub_date(raw.get("transactionDate"))
    filed = _parse_finnhub_date(raw.get("filingDate"))
    if transacted is None or filed is None:
        return None

    position = raw.get("position")
    role = position.strip() if isinstance(position, str) and position.strip() else None

    shares_after_raw = raw.get("share")
    shares_held_after: int | None = None
    if isinstance(shares_after_raw, (int, float)) and not isinstance(shares_after_raw, bool):
        shares_held_after = int(shares_after_raw)

    price_raw = raw.get("transactionPrice")
    price: float | None = None
    if isinstance(price_raw, (int, float)) and not isinstance(price_raw, bool):
        price = float(price_raw)

    currency_raw = raw.get("currency")
    currency = (
        currency_raw.strip()
        if isinstance(currency_raw, str) and currency_raw.strip()
        else None
    )

    is_derivative_raw = raw.get("isDerivative")
    is_derivative = bool(is_derivative_raw) if isinstance(is_derivative_raw, bool) else False

    return {
        "insider_name": name.strip(),
        "insider_role": role,
        "transaction_code": code.strip(),
        "transaction_type": _CODE_TO_TYPE.get(code.strip().upper(), "other"),
        "shares": int(change),
        "shares_held_after": shares_held_after,
        "price_per_share": price,
        "currency": currency,
        "is_derivative": is_derivative,
        "transacted_at_unix": transacted[0],
        "transacted_at": transacted[1],
        "filed_at_unix": filed[0],
        "filed_at": filed[1],
    }


def _parse_finnhub_date(s: Any) -> tuple[int, str] | None:
    """Parse 'YYYY-MM-DD' (or longer ISO-8601) to (unix_midnight_utc, iso_midnight_utc)."""
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        d = date.fromisoformat(s.strip()[:10])
    except ValueError:
        return None
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt.timestamp()), f"{d.isoformat()}T00:00:00Z"


def _today_utc() -> date:
    """Today's date in UTC. Isolated for test monkeypatching."""
    return datetime.now(timezone.utc).date()
