"""fetch_news — recent company news for a ticker, within a day window.

Single Finnhub call: /company-news?symbol=X&from=YYYY-MM-DD&to=YYYY-MM-DD

Per-item schema (stable across this capability — Abelard iterates in batches):

    {
      "id":                 str,        # Finnhub id coerced to str; "" if missing
      "headline":           str,        # required, non-empty
      "summary":            str | null, # empty string normalised to null
      "source":             str,        # required, non-empty (brand name verbatim)
      "url":                str,        # required, non-empty
      "published_at_unix":  int,        # UTC epoch seconds, > 0
      "published_at":       str,        # ISO-8601 UTC, derived from unix
    }

Items missing any required field (headline/url/datetime/source) are dropped.
`data.dropped_count` reports how many; a single envelope-level warning with
reason="parse_error" signals that at least one item was dropped. Partial on
drops; complete otherwise (including a valid empty window).
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

_log = logging.getLogger("research_daemon.fetch_news")


def fetch_news(
    ticker: str,
    days: int = 7,
    *,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Return a news envelope for `ticker` over the last `days` days."""
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(
            status="error",
            source="finnhub",
            detail="ticker must be a non-empty string",
        )
    # bool is a subclass of int — exclude it explicitly.
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
            f"{FINNHUB_BASE}/company-news",
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

    if not isinstance(payload, list):
        return build_error(
            status="error",
            source="finnhub",
            detail=f"expected list from /company-news, got {type(payload).__name__}",
        )

    items: list[dict[str, Any]] = []
    dropped = 0
    for raw in payload:
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
            "dropped %d malformed news item(s) for %s (from %s to %s)",
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


def _parse_item(raw: Any) -> dict[str, Any] | None:
    """Return a normalised item dict, or None if the raw entry is malformed."""
    if not isinstance(raw, dict):
        return None

    headline = raw.get("headline")
    url = raw.get("url")
    dt = raw.get("datetime")
    source = raw.get("source")

    if not (isinstance(headline, str) and headline.strip()):
        return None
    if not (isinstance(url, str) and url.strip()):
        return None
    if not isinstance(dt, (int, float)) or isinstance(dt, bool) or dt <= 0:
        return None
    if not (isinstance(source, str) and source.strip()):
        return None

    dt_int = int(dt)
    published_at = (
        datetime.fromtimestamp(dt_int, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )

    raw_id = raw.get("id")
    id_str = str(raw_id) if raw_id is not None else ""

    summary_raw = raw.get("summary")
    summary: str | None = None
    if isinstance(summary_raw, str) and summary_raw.strip():
        summary = summary_raw

    return {
        "id": id_str,
        "headline": headline,
        "summary": summary,
        "source": source,
        "url": url,
        "published_at_unix": dt_int,
        "published_at": published_at,
    }


def _today_utc() -> date:
    """Today's date in UTC. Isolated for test monkeypatching."""
    return datetime.now(timezone.utc).date()
