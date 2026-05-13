"""fetch_institutional_holdings — top-N 13F holders + QoQ changes for a ticker.

Single Finnhub call: /institutional/ownership?symbol=X&from=...&to=...

Two response shapes controlled by `num_quarters`:
  - num_quarters=1 (default): backward-compatible flat shape with the most
    recent quarter's top-N holders.
  - num_quarters>=2: list-of-quarters shape under `data.quarters`, ordered
    most-recent-first. Used by detect_institutional_changes for QoQ diffing.

13F data is stale by construction: filings are due ~45 days after
quarter-end, so the "most recent quarter" may be 1–4.5 months behind
calendar time. `as_of_quarter`, `reported_at`, and `latest_filed_at` are
all in `data` so Abelard can reason about staleness.

Small-cap names often return [] because smaller institutions don't always
file 13Fs at scale. Empty payloads are `data_completeness: "complete"`,
not "partial" — absence of data is not a failure.

Per-holder schema (stable across both response shapes):

    {
      "name":               str,         # required, non-empty
      "cik":                str | null,  # institution CIK, as-is from Finnhub
      "shares":             int,         # required, > 0
      "shares_change_qoq":  int | null,  # Finnhub `change`; null if absent
      "portfolio_percent":  float | null, # % of holder's portfolio in this ticker
      "filed_at_unix":      int,         # midnight UTC of this filing's date
      "filed_at":           str,         # ISO-8601 UTC, midnight
    }

Required per-item fields for parsing (drop if missing): name, share > 0,
reportDate, filingDate. Drops produce a single `parse_error` envelope
warning and `data.dropped_count`.

Top-N caveat: top_n (max 100) is applied per quarter. Holders outside the
top-100 of either quarter are invisible to the response. For QoQ change
detection this means positions held outside the top-100 in either quarter
may be misclassified — acceptable tradeoff given 13F endpoint constraints.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .config import Config
from .envelope import Completeness, build_error, build_ok, make_warning
from .http_client import HttpClient, NotFound, RateLimited, TransportError


FINNHUB_BASE = "https://finnhub.io/api/v1"
MIN_TOP_N = 1
MAX_TOP_N = 100
MIN_NUM_QUARTERS = 1
MAX_NUM_QUARTERS = 8

# 13Fs are filed ~45 days after quarter-end; base 200 days covers one quarter,
# and each additional quarter needs ~91 more days of lookback to catch its
# filings without missing later revisions.
_BASE_LOOKBACK_DAYS = 200
_DAYS_PER_ADDITIONAL_QUARTER = 91

_log = logging.getLogger("research_daemon.fetch_institutional_holdings")


def fetch_institutional_holdings(
    ticker: str,
    top_n: int = 10,
    *,
    num_quarters: int = 1,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Return an envelope with 13F holders for `ticker`.

    When num_quarters=1 (default), returns the single most recent quarter
    in a flat data shape. When num_quarters>=2, returns a `quarters` list
    ordered most-recent-first.
    """
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(
            status="error",
            source="finnhub",
            detail="ticker must be a non-empty string",
        )
    if not isinstance(top_n, int) or isinstance(top_n, bool):
        return build_error(
            status="error", source="finnhub", detail="top_n must be an integer"
        )
    if top_n < MIN_TOP_N or top_n > MAX_TOP_N:
        return build_error(
            status="error",
            source="finnhub",
            detail=f"top_n must be between {MIN_TOP_N} and {MAX_TOP_N}",
        )
    if not isinstance(num_quarters, int) or isinstance(num_quarters, bool):
        return build_error(
            status="error", source="finnhub", detail="num_quarters must be an integer"
        )
    if num_quarters < MIN_NUM_QUARTERS or num_quarters > MAX_NUM_QUARTERS:
        return build_error(
            status="error",
            source="finnhub",
            detail=(
                f"num_quarters must be between {MIN_NUM_QUARTERS} and {MAX_NUM_QUARTERS}"
            ),
        )

    symbol = ticker.strip().upper()
    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    today = _today_utc()
    lookback_days = _BASE_LOOKBACK_DAYS + (num_quarters - 1) * _DAYS_PER_ADDITIONAL_QUARTER
    from_date = today - timedelta(days=lookback_days)

    try:
        payload = http.get_json(
            f"{FINNHUB_BASE}/institutional/ownership",
            params={
                "symbol": symbol,
                "cusip": "",
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

    raw_items = _extract_ownership(payload)
    if raw_items is None:
        return build_error(
            status="error",
            source="finnhub",
            detail="expected {ownership: [...]} from /institutional/ownership",
        )

    parsed: list[dict[str, Any]] = []
    dropped = 0
    for raw in raw_items:
        item = _parse_item(raw)
        if item is None:
            dropped += 1
        else:
            parsed.append(item)

    # Group items by their report date (each unique reportDate is one quarter).
    by_quarter: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for item in parsed:
        by_quarter[item["_report_unix"]].append(item)

    # Sort quarter keys most-recent-first, take up to num_quarters.
    selected_keys = sorted(by_quarter.keys(), reverse=True)[:num_quarters]
    quarter_summaries = [
        _build_quarter_summary(by_quarter[k], top_n=top_n) for k in selected_keys
    ]

    if num_quarters == 1:
        data = _build_single_quarter_shape(
            symbol=symbol,
            top_n=top_n,
            dropped=dropped,
            quarter=quarter_summaries[0] if quarter_summaries else None,
        )
    else:
        data = _build_multi_quarter_shape(
            symbol=symbol,
            top_n=top_n,
            num_quarters=num_quarters,
            dropped=dropped,
            quarter_summaries=quarter_summaries,
        )

    warnings: list[dict[str, Any]] = []
    completeness: Completeness = "complete"
    if dropped > 0:
        _log.warning(
            "dropped %d malformed institutional holder(s) for %s", dropped, symbol
        )
        warnings.append(
            make_warning(
                field="holders",
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


# ---------------------------------------------------------------------------
# Quarter summary construction
# ---------------------------------------------------------------------------

def _build_quarter_summary(items: list[dict[str, Any]], *, top_n: int) -> dict[str, Any]:
    """Sort one quarter's items, slice to top_n, build the public dict."""
    items_sorted = sorted(items, key=lambda h: h["shares"], reverse=True)
    top = items_sorted[:top_n]

    reported_at_unix = items[0]["_report_unix"]
    reported_at = items[0]["_report_iso"]
    as_of_quarter = _date_to_quarter_label(date.fromisoformat(reported_at[:10]))

    filed_values = [(h["filed_at_unix"], h["filed_at"]) for h in items]
    earliest = min(filed_values)
    latest = max(filed_values)

    holders = [
        {k: v for k, v in h.items() if not k.startswith("_")}
        for h in top
    ]

    return {
        "as_of_quarter": as_of_quarter,
        "reported_at_unix": reported_at_unix,
        "reported_at": reported_at,
        "earliest_filed_at_unix": earliest[0],
        "earliest_filed_at": earliest[1],
        "latest_filed_at_unix": latest[0],
        "latest_filed_at": latest[1],
        "holders_returned": len(holders),
        "holders_total_in_quarter": len(items),
        "holders": holders,
    }


def _build_single_quarter_shape(
    *,
    symbol: str,
    top_n: int,
    dropped: int,
    quarter: dict[str, Any] | None,
) -> dict[str, Any]:
    """Flat backward-compatible shape for num_quarters=1."""
    if quarter is None:
        return {
            "ticker": symbol,
            "top_n": top_n,
            "as_of_quarter": None,
            "reported_at_unix": None,
            "reported_at": None,
            "earliest_filed_at_unix": None,
            "earliest_filed_at": None,
            "latest_filed_at_unix": None,
            "latest_filed_at": None,
            "holders_returned": 0,
            "holders_total_in_quarter": 0,
            "dropped_count": dropped,
            "holders": [],
        }
    return {
        "ticker": symbol,
        "top_n": top_n,
        "as_of_quarter": quarter["as_of_quarter"],
        "reported_at_unix": quarter["reported_at_unix"],
        "reported_at": quarter["reported_at"],
        "earliest_filed_at_unix": quarter["earliest_filed_at_unix"],
        "earliest_filed_at": quarter["earliest_filed_at"],
        "latest_filed_at_unix": quarter["latest_filed_at_unix"],
        "latest_filed_at": quarter["latest_filed_at"],
        "holders_returned": quarter["holders_returned"],
        "holders_total_in_quarter": quarter["holders_total_in_quarter"],
        "dropped_count": dropped,
        "holders": quarter["holders"],
    }


def _build_multi_quarter_shape(
    *,
    symbol: str,
    top_n: int,
    num_quarters: int,
    dropped: int,
    quarter_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    """List-of-quarters shape for num_quarters>=2, ordered most-recent-first."""
    return {
        "ticker": symbol,
        "top_n": top_n,
        "num_quarters_requested": num_quarters,
        "quarters_returned": len(quarter_summaries),
        "dropped_count": dropped,
        "quarters": quarter_summaries,
    }


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _extract_ownership(payload: Any) -> list[Any] | None:
    if not isinstance(payload, dict):
        return None
    inner = payload.get("ownership")
    if not isinstance(inner, list):
        return None
    return inner


def _parse_item(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    name = raw.get("name")
    if not (isinstance(name, str) and name.strip()):
        return None

    share = raw.get("share")
    if not isinstance(share, (int, float)) or isinstance(share, bool) or share <= 0:
        return None

    reported = _parse_finnhub_date(raw.get("reportDate"))
    if reported is None:
        return None
    filed = _parse_finnhub_date(raw.get("filingDate"))
    if filed is None:
        return None

    change_raw = raw.get("change")
    change: int | None = None
    if isinstance(change_raw, (int, float)) and not isinstance(change_raw, bool):
        change = int(change_raw)

    cik_raw = raw.get("cik")
    cik: str | None = None
    if cik_raw is not None and cik_raw != "":
        cik_str = str(cik_raw).strip()
        cik = cik_str or None

    pct_raw = raw.get("portfolioPercent")
    pct: float | None = None
    if isinstance(pct_raw, (int, float)) and not isinstance(pct_raw, bool):
        pct = float(pct_raw)

    return {
        "name": name.strip(),
        "cik": cik,
        "shares": int(share),
        "shares_change_qoq": change,
        "portfolio_percent": pct,
        "filed_at_unix": filed[0],
        "filed_at": filed[1],
        "_report_unix": reported[0],
        "_report_iso": reported[1],
    }


def _parse_finnhub_date(s: Any) -> tuple[int, str] | None:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        d = date.fromisoformat(s.strip()[:10])
    except ValueError:
        return None
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt.timestamp()), f"{d.isoformat()}T00:00:00Z"


def _date_to_quarter_label(d: date) -> str:
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"


def _today_utc() -> date:
    """Today's date in UTC. Isolated for test monkeypatching."""
    return datetime.now(timezone.utc).date()
