"""Google Trends plugin (Order 5) — name-keyed search-interest, three windows.

For each ACTIVE ticker that has a company-name search term, query Google Trends
(pytrends) by that NAME — never the bare ticker (a symbol is noise as a search
term). The query is **independent of `name_match`**: `name_match` governs free-text
matching (/smg/, Reddit), but a collision-word ticker's full company name (DE ->
"John Deere", MU -> "Micron") is a perfectly distinctive SEARCH term. Three interest
figures from the single canonical anchor: 24h (`now 1-d`), 7d (`now 7-d`), monthly
(`today 1-m`, trailing ~30d). `method="none"`, no LLM, `matched_by=["name"]` when
queried.

Disciplines (Trends is the flakiest upstream — treat breakage as first-class):
  - **429 -> degrade, never sink.** A rate-limit returns the source with an `error`
    set (so it shows `ok=False` / the scan is `degraded`) and the OTHER sources
    still carry the scan. It does NOT raise.
  - **upstream-shape change -> RAISE.** An unexpected structure fails loud — never
    emit a guessed / zero interest.
  - **noisy_query** flag, STRICT: set ONLY when the search term is ambiguous
    (`ambiguous_name`: Apple / Oracle / Caterpillar / Parsons / Palomar) — the
    company dominates that search volume so Abelard discounts the number, but the
    interest is REAL. A ticker with NO clean search term (ETF) gets null interest
    + `matched_by=[]` + NO flag — "no signal", kept distinct from this "weak signal"
    so the Order-7 anomaly layer never conflates them.

pytrends owns its own transport and is **lazy-imported** behind an injected
`TrendsClient`, so this module loads (and tests run) without it. Non-ASCII
company-name queries pass through intact.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from abelard_common.company_aliases import load_name_map

from ..schema import Metrics, NormalizedRecord, Sentiment, Window
from ..watchlist import TickerSpec, WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "google_trends"
WINDOW_LABEL = "24h"

# metrics field -> pytrends timeframe string (all anchored to the same now).
_TIMEFRAMES: dict[str, str] = {
    "interest_24h": "now 1-d",
    "interest_7d": "now 7-d",
    "interest_monthly": "today 1-m",
}


class TrendsRateLimited(RuntimeError):
    """pytrends 429 — degrade to a warning; do NOT raise out of the source."""


class TrendsShapeError(RuntimeError):
    """Upstream returned an unexpected structure — fail loud (raise)."""


class TrendsClient(Protocol):
    def interest(self, query: str, timeframe: str) -> float | None:
        """Mean relative interest (0-100) for `query` over `timeframe`, or None if
        Trends has no data. Raises `TrendsRateLimited` on 429, `TrendsShapeError`
        on an unexpected response."""
        ...


class PytrendsClient:
    """Default client — lazy-imports pytrends; one TrendReq per instance."""

    def __init__(self, hl: str = "en-US", tz: int = 360) -> None:
        from pytrends import exceptions as exc  # lazy: keeps the module import-light
        from pytrends.request import TrendReq

        self._req = TrendReq(hl=hl, tz=tz)
        self._exc = exc

    def interest(self, query: str, timeframe: str) -> float | None:
        try:
            self._req.build_payload(kw_list=[query], timeframe=timeframe)
            df = self._req.interest_over_time()
        except self._exc.TooManyRequestsError as exc:  # 429
            raise TrendsRateLimited(str(exc)) from exc
        except self._exc.ResponseError as exc:
            if "429" in str(exc):
                raise TrendsRateLimited(str(exc)) from exc
            raise TrendsShapeError(str(exc)) from exc

        try:
            if df is None or df.empty:
                return None
            if query not in df.columns:
                raise TrendsShapeError(f"interest_over_time missing column {query!r}")
            return round(float(df[query].mean()), 2)
        except TrendsShapeError:
            raise
        except Exception as exc:  # any pandas/structure surprise == shape change
            raise TrendsShapeError(f"unexpected interest_over_time structure: {exc}") from exc


def query_name(spec: TickerSpec, shared_map: dict[str, str]) -> str | None:
    """The single Trends query term for a ticker — its primary alias, INDEPENDENT
    of `name_match` (this is a search term, not a free-text matcher). `names[0]` if
    present, else the shortest shared-map alias for the symbol, else None (no clean
    search term — e.g. an ETF)."""
    if spec.names:
        return spec.names[0]
    candidates = sorted((n for n, sym in shared_map.items() if sym == spec.symbol), key=len)
    return candidates[0] if candidates else None


class GoogleTrendsSource:
    """Source adapter for Google Trends. Name-keyed, three windows, no LLM."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        company_names_path: str | Path,
        client: TrendsClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.google_trends")
        self._shared_map = load_name_map(Path(company_names_path))
        self._client = client  # injected in tests; built lazily otherwise

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        window = context.windows[WINDOW_LABEL]
        client = self._client or PytrendsClient()
        records: list[NormalizedRecord] = []
        degraded: str | None = None  # set on the first 429 -> remaining names skipped

        for spec in watchlist.active_tickers:
            query = query_name(spec, self._shared_map)
            if query is None:
                # No clean search term (ETF) -> null, no name match, NO flag.
                # "no signal" stays distinct from the ambiguous "weak signal".
                records.append(
                    self._record(watchlist.name, spec, context, window, {}, matched_by=[], noisy=False)
                )
                continue
            if degraded is not None:
                # Already rate-limited this run -> null interest, no fabrication.
                records.append(
                    self._record(watchlist.name, spec, context, window, {}, matched_by=["name"], noisy=False)
                )
                continue
            interest: dict[str, float | None] = {}
            try:
                for field, timeframe in _TIMEFRAMES.items():
                    interest[field] = client.interest(query, timeframe)
            except TrendsRateLimited as exc:
                # Degrade: mark the source failed, keep the scan alive. NOT a raise.
                degraded = f"rate limited (429); interest skipped from {spec.symbol}: {exc}"
                self._log.warning("google_trends %s", degraded)
                records.append(
                    self._record(watchlist.name, spec, context, window, {}, matched_by=["name"], noisy=False)
                )
                continue
            # TrendsShapeError intentionally propagates -> orchestrator isolates loudly.
            records.append(
                self._record(
                    watchlist.name, spec, context, window, interest,
                    matched_by=["name"], noisy=spec.ambiguous_name,
                )
            )

        return SourceResult(source=SOURCE_NAME, records=records, error=degraded)

    def _record(
        self,
        watchlist_name: str,
        spec: TickerSpec,
        context: ScanContext,
        window: Window,
        interest: dict[str, float | None],
        *,
        matched_by: list[str],
        noisy: bool,
    ) -> NormalizedRecord:
        return NormalizedRecord(
            watchlist=watchlist_name,
            scan_mode=context.scan_mode,
            canonical_ts=context.canonical_ts,
            window=window,
            source=SOURCE_NAME,
            ticker=spec.symbol,
            matched_by=matched_by,
            metrics=Metrics(
                interest_24h=interest.get("interest_24h"),
                interest_7d=interest.get("interest_7d"),
                interest_monthly=interest.get("interest_monthly"),
            ),
            sentiment=Sentiment(method="none"),
            flags=["noisy_query"] if noisy else [],
        )
