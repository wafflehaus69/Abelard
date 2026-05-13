"""detect_insider_activity — material insider buys across a ticker list.

Thin wrapper on `fetch_insider_transactions`. Filters for signal, drops
routine grants and vesting, flags:

  - large_buys: purchase transactions with shares * price >= min_value_usd
  - cluster_buy_detected: >= 2 DISTINCT insiders with any-size purchase in window
  - is_first_time_filer (per-buy flag): insider absent from the baseline
    window (baseline is first_time_lookback_days, excluding the recent window)

BUYS ONLY. Sales are excluded by design — Finnhub's insider-transactions
payload does not expose the 10b5-1 plan flag, and insiders sell for many
uncorrelated reasons (tax, diversification, liquidity) which makes sells
low-signal without that flag. Insiders buy for essentially one reason.
Use the deep-read `fetch_insider_transactions` when sales are needed.

API cost: one `fetch_insider_transactions` call per ticker per sweep.
When `include_first_time_detection=True` the call fetches the full
`first_time_lookback_days` window and splits client-side into recent
and baseline; when False it fetches only `lookback_days` to keep sweeps
cheap.

Partial-failure handling: per-ticker `error: {reason, detail} | null`.
When any ticker fails, envelope `data_completeness="partial"` plus a
single aggregate `upstream_error` warning.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from .config import Config
from .envelope import build_error, build_ok, make_warning
from .fetch_insider_transactions import (
    MAX_DAYS as INSIDER_MAX_DAYS,
    fetch_insider_transactions,
)
from .http_client import HttpClient


MIN_TICKERS = 1
MAX_TICKERS = 100
MIN_LOOKBACK_DAYS = 1
MAX_LOOKBACK_DAYS = INSIDER_MAX_DAYS  # mirror fetch_insider_transactions cap
MIN_VALUE_USD = 1
MAX_VALUE_USD = 1_000_000_000
MIN_FIRST_TIME_LOOKBACK_DAYS = 1
MAX_FIRST_TIME_LOOKBACK_DAYS = INSIDER_MAX_DAYS

_log = logging.getLogger("research_daemon.detect_insider_activity")


def detect_insider_activity(
    tickers: list[str],
    lookback_days: int = 30,
    min_value_usd: int = 100_000,
    *,
    include_first_time_detection: bool = True,
    first_time_lookback_days: int = 365,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Scan tickers for material insider BUY activity."""
    # Validation
    if not isinstance(tickers, list):
        return build_error(
            status="error", source="finnhub", detail="tickers must be a list of strings"
        )
    if len(tickers) < MIN_TICKERS or len(tickers) > MAX_TICKERS:
        return build_error(
            status="error",
            source="finnhub",
            detail=f"tickers count must be between {MIN_TICKERS} and {MAX_TICKERS}",
        )
    for t in tickers:
        if not isinstance(t, str) or not t.strip():
            return build_error(
                status="error",
                source="finnhub",
                detail="every ticker must be a non-empty string",
            )

    if not isinstance(lookback_days, int) or isinstance(lookback_days, bool):
        return build_error(status="error", source="finnhub",
                           detail="lookback_days must be an integer")
    if lookback_days < MIN_LOOKBACK_DAYS or lookback_days > MAX_LOOKBACK_DAYS:
        return build_error(
            status="error", source="finnhub",
            detail=f"lookback_days must be between {MIN_LOOKBACK_DAYS} and {MAX_LOOKBACK_DAYS}",
        )

    if not isinstance(min_value_usd, int) or isinstance(min_value_usd, bool):
        return build_error(status="error", source="finnhub",
                           detail="min_value_usd must be an integer")
    if min_value_usd < MIN_VALUE_USD or min_value_usd > MAX_VALUE_USD:
        return build_error(
            status="error", source="finnhub",
            detail=f"min_value_usd must be between {MIN_VALUE_USD} and {MAX_VALUE_USD}",
        )

    if not isinstance(include_first_time_detection, bool):
        return build_error(status="error", source="finnhub",
                           detail="include_first_time_detection must be a bool")

    if not isinstance(first_time_lookback_days, int) or isinstance(first_time_lookback_days, bool):
        return build_error(status="error", source="finnhub",
                           detail="first_time_lookback_days must be an integer")
    if (first_time_lookback_days < MIN_FIRST_TIME_LOOKBACK_DAYS
            or first_time_lookback_days > MAX_FIRST_TIME_LOOKBACK_DAYS):
        return build_error(
            status="error", source="finnhub",
            detail=(
                f"first_time_lookback_days must be between "
                f"{MIN_FIRST_TIME_LOOKBACK_DAYS} and {MAX_FIRST_TIME_LOOKBACK_DAYS}"
            ),
        )
    if include_first_time_detection and first_time_lookback_days <= lookback_days:
        return build_error(
            status="error", source="finnhub",
            detail=(
                "first_time_lookback_days must be greater than lookback_days "
                "when include_first_time_detection=True"
            ),
        )

    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    today = _today_utc()
    recent_start_unix = _midnight_utc_unix(today - timedelta(days=lookback_days))

    per_ticker: list[dict[str, Any]] = []
    failed = 0
    for raw_ticker in tickers:
        symbol = raw_ticker.strip().upper()
        result = _analyze_ticker(
            symbol=symbol,
            lookback_days=lookback_days,
            min_value_usd=min_value_usd,
            include_first_time_detection=include_first_time_detection,
            first_time_lookback_days=first_time_lookback_days,
            recent_start_unix=recent_start_unix,
            cfg=cfg,
            http=http,
        )
        per_ticker.append(result)
        if result["error"] is not None:
            failed += 1

    data: dict[str, Any] = {
        "ticker_count": len(tickers),
        "tickers_analyzed": len(tickers) - failed,
        "tickers_failed": failed,
        "lookback_days": lookback_days,
        "min_value_usd": min_value_usd,
        "include_first_time_detection": include_first_time_detection,
        "first_time_lookback_days": (
            first_time_lookback_days if include_first_time_detection else None
        ),
        "per_ticker": per_ticker,
    }

    warnings: list[dict[str, Any]] = []
    completeness = "complete"
    if failed > 0:
        _log.warning(
            "detect_insider_activity: %d of %d tickers failed", failed, len(tickers)
        )
        warnings.append(
            make_warning(
                field="per_ticker",
                reason="upstream_error",
                source="finnhub",
                suggestion=(
                    f"{failed} of {len(tickers)} ticker(s) failed; "
                    "see each per_ticker.error for reason"
                ),
            )
        )
        completeness = "partial"

    return build_ok(
        data,
        source="finnhub",
        data_completeness=completeness,  # type: ignore[arg-type]
        warnings=warnings,
    )


def _analyze_ticker(
    *,
    symbol: str,
    lookback_days: int,
    min_value_usd: int,
    include_first_time_detection: bool,
    first_time_lookback_days: int,
    recent_start_unix: int,
    cfg: Config,
    http: HttpClient,
) -> dict[str, Any]:
    # Single-fetch strategy: if first-time detection is on, pull the full
    # baseline window and split client-side into recent vs baseline. If off,
    # pull only the recent window to minimise API cost on sweeps.
    days_to_fetch = (
        first_time_lookback_days if include_first_time_detection else lookback_days
    )

    env = fetch_insider_transactions(
        symbol, days=days_to_fetch, config=cfg, client=http,
    )

    if env["status"] != "ok":
        return _empty_ticker_result(
            symbol=symbol,
            error={
                "reason": env["status"],
                "detail": env.get("error_detail") or f"{symbol}: upstream returned {env['status']}",
            },
        )

    all_items = env["data"]["items"]

    # Split into recent-window and baseline (for first-time detection).
    recent_items = [i for i in all_items if i["transacted_at_unix"] >= recent_start_unix]
    if include_first_time_detection:
        baseline_names = {
            i["insider_name"]
            for i in all_items
            if i["transacted_at_unix"] < recent_start_unix
        }
    else:
        baseline_names = set()

    # Purchases only — drop routine grants, vesting, tax, option exercises.
    purchases = [i for i in recent_items if i["transaction_type"] == "purchase"]

    # Large buys filter: shares * price >= min_value_usd.
    large_buys: list[dict[str, Any]] = []
    for item in purchases:
        shares = item["shares"]
        price = item["price_per_share"]
        if price is None:
            continue
        # A purchase has positive `change` → positive `shares` in our schema,
        # but defensively use abs() in case upstream flips signs.
        value = abs(shares) * price
        if value < min_value_usd:
            continue

        is_first_time = (
            include_first_time_detection
            and item["insider_name"] not in baseline_names
        )

        large_buys.append({
            "insider_name": item["insider_name"],
            "insider_role": item["insider_role"],
            "transaction_code": item["transaction_code"],
            "shares": shares,
            "price_per_share": price,
            "value_usd": round(value, 2),
            "transacted_at_unix": item["transacted_at_unix"],
            "transacted_at": item["transacted_at"],
            "filed_at_unix": item["filed_at_unix"],
            "filed_at": item["filed_at"],
            "is_first_time_filer": is_first_time,
        })

    large_buys.sort(key=lambda b: b["value_usd"], reverse=True)

    # Cluster buy: distinct insiders with ANY purchase in the window,
    # regardless of size — the signal is coordination, not magnitude.
    distinct_buyers = {i["insider_name"] for i in purchases}
    cluster_buy_detected = len(distinct_buyers) >= 2

    # First-time buyer count: distinct first-time filers among large buys.
    first_time_buyer_names = {
        b["insider_name"] for b in large_buys if b["is_first_time_filer"]
    }

    return {
        "ticker": symbol,
        "large_buys": large_buys,
        "cluster_buy_detected": cluster_buy_detected,
        "distinct_buyers": len(distinct_buyers),
        "first_time_buyer_count": len(first_time_buyer_names),
        "error": None,
    }


def _empty_ticker_result(*, symbol: str, error: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "ticker": symbol,
        "large_buys": [],
        "cluster_buy_detected": False,
        "distinct_buyers": 0,
        "first_time_buyer_count": 0,
        "error": error,
    }


def _midnight_utc_unix(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def _today_utc() -> date:
    """Today's date in UTC. Isolated for test monkeypatching."""
    return datetime.now(timezone.utc).date()
