"""detect_institutional_changes — QoQ position diff across a ticker list.

Thin wrapper on `fetch_institutional_holdings(num_quarters=2)`. For each
ticker, pulls the two most recent quarters' top-100 holders, set-diffs
them on (name, cik), and classifies holder activity into four buckets:

  new_positions       — holder present in current quarter, absent in prior
  closed_positions    — holder present in prior quarter, absent in current
  increased_positions — holder in both; shares grew by >= min_change_pct
  reduced_positions   — holder in both; shares shrank by >= min_change_pct

Changes are computed from the two snapshots we hold, NOT from Finnhub's
per-holder `shares_change_qoq` field. This keeps the classification self-
consistent with the data we return.

Top-100 cap caveat: any holder outside top-100 in either quarter is
invisible to this function. A holder who was position 105 in Q3 and
position 80 in Q4 would be classified as `new` (not in our Q3 slice),
even though they actually just moved up. Acceptable tradeoff given the
monitoring role — the long tail is noise anyway. Abelard can call the
deep-read `fetch_institutional_holdings` for a specific ticker if he
suspects a miss.

Partial-failure handling: per-ticker `error: {reason, detail} | null`.
When any ticker fails, envelope `data_completeness="partial"` plus a
single aggregate `upstream_error` warning. Succeeded tickers still return
their full bucket data alongside the failures.

Empty-result semantics: a ticker with zero changes above threshold is
NOT an error — it's a clean `error: null` entry with all four buckets
empty. Small-caps legitimately have no 13F activity most quarters.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import Config
from .envelope import build_error, build_ok, make_warning
from .fetch_institutional_holdings import fetch_institutional_holdings
from .http_client import HttpClient


MIN_TICKERS = 1
MAX_TICKERS = 100
MIN_CHANGE_PCT = 1
MAX_CHANGE_PCT = 1000
_MONITORING_TOP_N = 100  # widest net within fetch_institutional_holdings' cap

_log = logging.getLogger("research_daemon.detect_institutional_changes")


def detect_institutional_changes(
    tickers: list[str],
    min_change_pct: int = 10,
    *,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Scan tickers for QoQ position changes above `min_change_pct`."""
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
    if not isinstance(min_change_pct, int) or isinstance(min_change_pct, bool):
        return build_error(
            status="error",
            source="finnhub",
            detail="min_change_pct must be an integer",
        )
    if min_change_pct < MIN_CHANGE_PCT or min_change_pct > MAX_CHANGE_PCT:
        return build_error(
            status="error",
            source="finnhub",
            detail=(
                f"min_change_pct must be between {MIN_CHANGE_PCT} and {MAX_CHANGE_PCT}"
            ),
        )

    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    per_ticker: list[dict[str, Any]] = []
    failed = 0
    for raw_ticker in tickers:
        symbol = raw_ticker.strip().upper()
        result = _analyze_ticker(symbol, min_change_pct, cfg, http)
        per_ticker.append(result)
        if result["error"] is not None:
            failed += 1

    data: dict[str, Any] = {
        "ticker_count": len(tickers),
        "tickers_analyzed": len(tickers) - failed,
        "tickers_failed": failed,
        "min_change_pct": min_change_pct,
        "per_ticker": per_ticker,
    }

    warnings: list[dict[str, Any]] = []
    completeness = "complete"
    if failed > 0:
        _log.warning(
            "detect_institutional_changes: %d of %d tickers failed", failed, len(tickers)
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
    symbol: str,
    min_change_pct: int,
    cfg: Config,
    http: HttpClient,
) -> dict[str, Any]:
    env = fetch_institutional_holdings(
        symbol,
        top_n=_MONITORING_TOP_N,
        num_quarters=2,
        config=cfg,
        client=http,
    )

    if env["status"] != "ok":
        return _empty_ticker_result(
            symbol=symbol,
            error={
                "reason": env["status"],
                "detail": env.get("error_detail") or f"{symbol}: upstream returned {env['status']}",
            },
        )

    quarters = env["data"].get("quarters", [])
    if len(quarters) < 2:
        return _empty_ticker_result(
            symbol=symbol,
            current_quarter=(quarters[0]["as_of_quarter"] if quarters else None),
            error={
                "reason": "insufficient_history",
                "detail": (
                    f"{symbol}: prior quarter unavailable "
                    f"(got {len(quarters)} of 2 quarters)"
                ),
            },
        )

    current, prior = quarters[0], quarters[1]

    current_by_key = {_holder_key(h): h for h in current["holders"]}
    prior_by_key = {_holder_key(h): h for h in prior["holders"]}

    new_keys = current_by_key.keys() - prior_by_key.keys()
    closed_keys = prior_by_key.keys() - current_by_key.keys()
    common_keys = current_by_key.keys() & prior_by_key.keys()

    new_positions = [
        {
            "name": current_by_key[k]["name"],
            "cik": current_by_key[k]["cik"],
            "shares": current_by_key[k]["shares"],
            "portfolio_percent": current_by_key[k]["portfolio_percent"],
        }
        for k in new_keys
    ]
    new_positions.sort(key=lambda e: e["shares"], reverse=True)

    closed_positions = [
        {
            "name": prior_by_key[k]["name"],
            "cik": prior_by_key[k]["cik"],
            "prior_shares": prior_by_key[k]["shares"],
        }
        for k in closed_keys
    ]
    closed_positions.sort(key=lambda e: e["prior_shares"], reverse=True)

    increased_positions: list[dict[str, Any]] = []
    reduced_positions: list[dict[str, Any]] = []
    for k in common_keys:
        p = prior_by_key[k]
        c = current_by_key[k]
        if p["shares"] <= 0:
            # Can't compute pct change against a non-positive base. Data oddity.
            continue
        delta = c["shares"] - p["shares"]
        change_pct = (delta / p["shares"]) * 100
        if abs(change_pct) < min_change_pct:
            continue
        entry = {
            "name": c["name"],
            "cik": c["cik"],
            "prior_shares": p["shares"],
            "current_shares": c["shares"],
            "change_pct": round(change_pct, 2),
        }
        if delta > 0:
            increased_positions.append(entry)
        else:
            reduced_positions.append(entry)

    increased_positions.sort(key=lambda e: e["change_pct"], reverse=True)
    reduced_positions.sort(key=lambda e: e["change_pct"])  # most-negative first

    return {
        "ticker": symbol,
        "current_quarter": current["as_of_quarter"],
        "prior_quarter": prior["as_of_quarter"],
        "new_positions": new_positions,
        "closed_positions": closed_positions,
        "increased_positions": increased_positions,
        "reduced_positions": reduced_positions,
        "error": None,
    }


def _holder_key(h: dict[str, Any]) -> tuple[str, str | None]:
    """Dedup key for a holder. Prefer CIK when present (name variants exist)."""
    return (h["name"], h["cik"])


def _empty_ticker_result(
    *,
    symbol: str,
    current_quarter: str | None = None,
    prior_quarter: str | None = None,
    error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ticker": symbol,
        "current_quarter": current_quarter,
        "prior_quarter": prior_quarter,
        "new_positions": [],
        "closed_positions": [],
        "increased_positions": [],
        "reduced_positions": [],
        "error": error,
    }
