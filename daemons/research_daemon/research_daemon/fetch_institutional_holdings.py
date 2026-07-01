"""fetch_institutional_holdings — top-N 13F-ish holders for a ticker via yfinance.

Data source: Yahoo Finance (via the yfinance Python library). Merges the
`institutional_holders` and `mutualfund_holders` DataFrames into a single
top-N holders list, tagging each entry with `holder_type`.

**Not** Finnhub — the `/institutional/ownership` endpoint used by an
earlier revision is Enterprise-tier at Finnhub and 403s universally on
free-tier keys. Yahoo/yfinance is the working free fallback.

Per-holder schema (stable):

    {
      "name":                   str,                      # required, non-empty
      "cik":                    None,                     # yfinance does not expose CIK
      "holder_type":            "institution" | "mutual_fund",
      "shares":                 int,                      # required, > 0
      "shares_change_qoq":      int | None,               # derived from qoq_pct_change
      "qoq_pct_change":         float | None,             # yfinance `pctChange` (fraction)
      "portfolio_percent":      None,                     # Finnhub-specific; always null
      "percent_of_shares_held": float | None,             # yfinance `pctHeld` (fraction)
      "filed_at_unix":          int,                      # midnight UTC of Date Reported
      "filed_at":               str,                      # ISO-8601 UTC, midnight
    }

`num_quarters >= 2` is rejected — yfinance returns only the current
snapshot per holder. QoQ delta info lives per-holder in
`qoq_pct_change` / `shares_change_qoq`, not as a second-quarter roll.

Semantic notes vs. the earlier Finnhub schema:
- `portfolio_percent` (Finnhub: % of the holder's portfolio in this ticker)
  is not available. Set to null on every row. Consumers should switch to
  `percent_of_shares_held` (Yahoo: % of the stock's float held by this
  holder), which is a strictly different metric.
- `cik` is not exposed by yfinance. Always null.
- `qoq_pct_change` is a decimal fraction (e.g. -0.0088 = -0.88%), not
  a percentage number.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import yfinance as yf

from .config import Config
from .envelope import Completeness, build_error, build_ok, make_warning
from .http_client import HttpClient  # kept for signature compatibility (unused)


MIN_TOP_N = 1
MAX_TOP_N = 100
MIN_NUM_QUARTERS = 1
MAX_NUM_QUARTERS = 1  # yfinance snapshot is single-quarter

_log = logging.getLogger("research_daemon.fetch_institutional_holdings")


def fetch_institutional_holdings(
    ticker: str,
    top_n: int = 10,
    *,
    num_quarters: int = 1,
    config: Config | None = None,  # noqa: ARG001 — signature stability
    client: HttpClient | None = None,  # noqa: ARG001 — signature stability
) -> dict[str, Any]:
    """Return an envelope with top-N 13F-ish holders for `ticker`."""
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(
            status="error",
            source="yahoo",
            detail="ticker must be a non-empty string",
        )
    if not isinstance(top_n, int) or isinstance(top_n, bool):
        return build_error(
            status="error", source="yahoo", detail="top_n must be an integer"
        )
    if top_n < MIN_TOP_N or top_n > MAX_TOP_N:
        return build_error(
            status="error",
            source="yahoo",
            detail=f"top_n must be between {MIN_TOP_N} and {MAX_TOP_N}",
        )
    if not isinstance(num_quarters, int) or isinstance(num_quarters, bool):
        return build_error(
            status="error", source="yahoo", detail="num_quarters must be an integer"
        )
    if num_quarters != 1:
        return build_error(
            status="error",
            source="yahoo",
            detail=(
                "num_quarters must be 1: the Yahoo/yfinance source returns "
                "only the current snapshot. Per-holder QoQ delta is available "
                "on each holder via qoq_pct_change / shares_change_qoq fields."
            ),
        )

    symbol = ticker.strip().upper()

    try:
        ticker_obj = yf.Ticker(symbol)
        inst_df = ticker_obj.institutional_holders
        mf_df = ticker_obj.mutualfund_holders
    except Exception as exc:  # noqa: BLE001 — yfinance can raise many exception types
        return build_error(
            status="error",
            source="yahoo",
            detail=f"yfinance call failed: {exc.__class__.__name__}: {exc}",
        )

    parsed: list[dict[str, Any]] = []
    dropped = 0

    for holder_type, df in (("institution", inst_df), ("mutual_fund", mf_df)):
        for row in _iter_rows(df):
            item = _parse_row(row, holder_type=holder_type)
            if item is None:
                dropped += 1
            else:
                parsed.append(item)

    parsed.sort(key=lambda h: h["shares"], reverse=True)
    holders_top = parsed[:top_n]

    warnings: list[dict[str, Any]] = []
    completeness: Completeness = "complete"

    if dropped > 0:
        _log.warning(
            "dropped %d malformed holder row(s) for %s", dropped, symbol
        )
        warnings.append(
            make_warning(
                field="holders",
                reason="parse_error",
                source="yahoo",
                suggestion=(
                    f"{dropped} upstream row(s) dropped; see data.dropped_count"
                ),
            )
        )
        completeness = "partial"

    if parsed:
        report_unixes = [h["_report_unix"] for h in parsed]
        report_isos = [h["_report_iso"] for h in parsed]
        max_idx = report_unixes.index(max(report_unixes))
        min_idx = report_unixes.index(min(report_unixes))
        reported_at_unix = report_unixes[max_idx]
        reported_at = report_isos[max_idx]
        as_of_quarter = _date_to_quarter_label(date.fromisoformat(reported_at[:10]))
        earliest_filed_at_unix = report_unixes[min_idx]
        earliest_filed_at = report_isos[min_idx]
        latest_filed_at_unix = reported_at_unix
        latest_filed_at = reported_at
        holders_public = [
            {k: v for k, v in h.items() if not k.startswith("_")}
            for h in holders_top
        ]
    else:
        reported_at_unix = None
        reported_at = None
        as_of_quarter = None
        earliest_filed_at_unix = None
        earliest_filed_at = None
        latest_filed_at_unix = None
        latest_filed_at = None
        holders_public = []

    data: dict[str, Any] = {
        "ticker": symbol,
        "top_n": top_n,
        "as_of_quarter": as_of_quarter,
        "reported_at_unix": reported_at_unix,
        "reported_at": reported_at,
        "earliest_filed_at_unix": earliest_filed_at_unix,
        "earliest_filed_at": earliest_filed_at,
        "latest_filed_at_unix": latest_filed_at_unix,
        "latest_filed_at": latest_filed_at,
        "holders_returned": len(holders_public),
        "holders_total_in_quarter": len(parsed),
        "dropped_count": dropped,
        "holders": holders_public,
    }

    return build_ok(
        data,
        source="yahoo",
        data_completeness=completeness,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _iter_rows(df: Any) -> list[dict[str, Any]]:
    """Yield rows from a yfinance DataFrame.

    Accepts pandas DataFrames (real usage) and lists of dicts (tests).
    Returns an empty list when the input is None or empty.
    """
    if df is None:
        return []
    if hasattr(df, "to_dict") and hasattr(df, "columns"):
        try:
            if len(df) == 0:  # type: ignore[arg-type]
                return []
            return list(df.to_dict(orient="records"))  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001
            return []
    if isinstance(df, list):
        return [r for r in df if isinstance(r, dict)]
    return []


def _parse_row(row: dict[str, Any], *, holder_type: str) -> dict[str, Any] | None:
    name = row.get("Holder")
    if not (isinstance(name, str) and name.strip()):
        return None

    shares_raw = row.get("Shares")
    if not isinstance(shares_raw, (int, float)) or isinstance(shares_raw, bool):
        return None
    if shares_raw <= 0:
        return None
    shares = int(shares_raw)

    parsed_date = _parse_yf_timestamp(row.get("Date Reported"))
    if parsed_date is None:
        return None

    pct_held_raw = row.get("pctHeld")
    percent_of_shares_held: float | None = None
    if isinstance(pct_held_raw, (int, float)) and not isinstance(pct_held_raw, bool):
        percent_of_shares_held = float(pct_held_raw)

    pct_change_raw = row.get("pctChange")
    qoq_pct: float | None = None
    if isinstance(pct_change_raw, (int, float)) and not isinstance(pct_change_raw, bool):
        qoq_pct = float(pct_change_raw)

    shares_change_qoq: int | None = None
    if qoq_pct is not None and (1 + qoq_pct) > 0:
        prior_shares = shares / (1 + qoq_pct)
        shares_change_qoq = int(round(shares - prior_shares))

    return {
        "name": name.strip(),
        "cik": None,
        "holder_type": holder_type,
        "shares": shares,
        "shares_change_qoq": shares_change_qoq,
        "qoq_pct_change": qoq_pct,
        "portfolio_percent": None,
        "percent_of_shares_held": percent_of_shares_held,
        "filed_at_unix": parsed_date[0],
        "filed_at": parsed_date[1],
        "_report_unix": parsed_date[0],
        "_report_iso": parsed_date[1],
    }


def _parse_yf_timestamp(ts: Any) -> tuple[int, str] | None:
    """Convert a pandas Timestamp / date / string to (unix_midnight_utc, iso)."""
    if ts is None:
        return None
    try:
        import pandas as pd
        if pd.isna(ts):  # handles NaT
            return None
    except (ImportError, TypeError, ValueError):
        pass

    if isinstance(ts, datetime):
        d = ts.date()
    elif isinstance(ts, date):
        d = ts
    elif hasattr(ts, "date") and callable(ts.date):
        try:
            d = ts.date()
        except Exception:  # noqa: BLE001
            return None
    elif isinstance(ts, str) and ts.strip():
        try:
            d = date.fromisoformat(ts.strip()[:10])
        except ValueError:
            return None
    else:
        return None

    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt.timestamp()), f"{d.isoformat()}T00:00:00Z"


def _date_to_quarter_label(d: date) -> str:
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"
