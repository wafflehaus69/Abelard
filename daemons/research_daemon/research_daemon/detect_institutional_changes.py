"""detect_institutional_changes — QoQ position deltas across a ticker list.

Thin wrapper on `fetch_institutional_holdings(num_quarters=1)`. For each
ticker, iterates the top-100 holders and uses per-holder `qoq_pct_change`
(derived from yfinance's `pctChange` column) to classify positions as
`increased_positions` or `reduced_positions` above `min_change_pct`.

**`new_positions` and `closed_positions` are ALWAYS EMPTY** on this data
source. yfinance returns a snapshot of the current top holders only; a
fully-exited holder is invisible, and a genuinely-new holder can't be
distinguished from "always held, just moved into top-N". The envelope
carries a standing `insufficient_history` warning so Abelard doesn't
misread the empty buckets as "no new or closed activity". If real
new/closed detection is needed, the daemon would need to persist snapshot
history — deliberately not done for now.

Partial-failure handling: per-ticker `error: {reason, detail} | None`.
When any ticker fails, envelope carries an aggregate `upstream_error`
warning alongside the standing `insufficient_history` warning.
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
_MONITORING_TOP_N = 100

_log = logging.getLogger("research_daemon.detect_institutional_changes")


def detect_institutional_changes(
    tickers: list[str],
    min_change_pct: int = 10,
    *,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Scan tickers for per-holder QoQ position changes above `min_change_pct`."""
    if not isinstance(tickers, list):
        return build_error(
            status="error", source="yahoo", detail="tickers must be a list of strings"
        )
    if len(tickers) < MIN_TICKERS or len(tickers) > MAX_TICKERS:
        return build_error(
            status="error",
            source="yahoo",
            detail=f"tickers count must be between {MIN_TICKERS} and {MAX_TICKERS}",
        )
    for t in tickers:
        if not isinstance(t, str) or not t.strip():
            return build_error(
                status="error",
                source="yahoo",
                detail="every ticker must be a non-empty string",
            )
    if not isinstance(min_change_pct, int) or isinstance(min_change_pct, bool):
        return build_error(
            status="error",
            source="yahoo",
            detail="min_change_pct must be an integer",
        )
    if min_change_pct < MIN_CHANGE_PCT or min_change_pct > MAX_CHANGE_PCT:
        return build_error(
            status="error",
            source="yahoo",
            detail=(
                f"min_change_pct must be between {MIN_CHANGE_PCT} and {MAX_CHANGE_PCT}"
            ),
        )

    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    threshold_fraction = min_change_pct / 100.0

    per_ticker: list[dict[str, Any]] = []
    failed = 0
    for raw_ticker in tickers:
        symbol = raw_ticker.strip().upper()
        result = _analyze_ticker(symbol, threshold_fraction, cfg, http)
        per_ticker.append(result)
        if result["error"] is not None:
            failed += 1

    data: dict[str, Any] = {
        "ticker_count": len(tickers),
        "tickers_analyzed": len(tickers) - failed,
        "tickers_failed": failed,
        "min_change_pct": min_change_pct,
        "source_supports": {
            "new_and_closed_detection": False,
            "increased_and_reduced_detection": True,
        },
        "per_ticker": per_ticker,
    }

    # Standing warning: inherent to yfinance, applies to every response.
    # Envelope is always at least partial for this capability.
    warnings: list[dict[str, Any]] = [
        make_warning(
            field="per_ticker.new_positions,per_ticker.closed_positions",
            reason="insufficient_history",
            source="yahoo",
            suggestion=(
                "yfinance returns only current-snapshot top holders. new/closed "
                "position detection requires persistent snapshot history — not "
                "provided by this source. increased/reduced buckets are populated."
            ),
        ),
    ]

    completeness = "partial"
    if failed > 0:
        _log.warning(
            "detect_institutional_changes: %d of %d tickers failed", failed, len(tickers)
        )
        warnings.append(
            make_warning(
                field="per_ticker",
                reason="upstream_error",
                source="yahoo",
                suggestion=(
                    f"{failed} of {len(tickers)} ticker(s) failed; "
                    "see each per_ticker.error for reason"
                ),
            )
        )

    return build_ok(
        data,
        source="yahoo",
        data_completeness=completeness,  # type: ignore[arg-type]
        warnings=warnings,
    )


def _analyze_ticker(
    symbol: str,
    threshold_fraction: float,
    cfg: Config,
    http: HttpClient,
) -> dict[str, Any]:
    env = fetch_institutional_holdings(
        symbol,
        top_n=_MONITORING_TOP_N,
        num_quarters=1,
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

    d = env["data"]
    current_quarter = d.get("as_of_quarter")
    holders = d.get("holders") or []

    increased_positions: list[dict[str, Any]] = []
    reduced_positions: list[dict[str, Any]] = []

    for h in holders:
        pct = h.get("qoq_pct_change")
        if pct is None:
            continue  # no delta signal for this holder
        if abs(pct) < threshold_fraction:
            continue

        entry = {
            "name": h["name"],
            "cik": h["cik"],
            "holder_type": h["holder_type"],
            "current_shares": h["shares"],
            "shares_change_qoq": h["shares_change_qoq"],
            "change_pct": round(pct * 100, 2),
        }
        if pct > 0:
            increased_positions.append(entry)
        else:
            reduced_positions.append(entry)

    increased_positions.sort(key=lambda e: e["change_pct"], reverse=True)
    reduced_positions.sort(key=lambda e: e["change_pct"])  # most-negative first

    return {
        "ticker": symbol,
        "current_quarter": current_quarter,
        "prior_quarter": None,  # yfinance doesn't expose the prior quarter as a distinct object
        "new_positions": [],
        "closed_positions": [],
        "increased_positions": increased_positions,
        "reduced_positions": reduced_positions,
        "error": None,
    }


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
