"""Alpha Vantage NEWS_SENTIMENT source (CH-SRC-1) — the per-ticker NEWS-SENTIMENT axis.

Chatter's third independent sentiment read: StockTwits = crowd mood, Finnhub = factual named-news
count, and now Alpha Vantage = news sentiment. Its unique value is ``ticker_sentiment[]`` — per
article, a per-ticker ``relevance_score`` + ``ticker_sentiment_score`` — the richest structured
per-ticker metadata in the roster. Chatter emits the axis; **Abelard joins the three reads and
reconciles divergence — the daemon never does** (locked architecture).

ONE call/scan covers the whole watchlist (``function=NEWS_SENTIMENT&tickers=<all>&limit=1000``),
trivially under AV's 25/day free cap. Aggregation: for each article's ``ticker_sentiment[]``,
keep the watchlist symbols whose ``relevance_score`` clears the trust gate, then per ticker take
the relevance-weighted mean sentiment + article count + mean relevance -> a ``NewsSentiment``.

⚠️ IN-BAND ERROR GUARD (mandatory, THE footgun): AV returns errors as HTTP 200 with an
``Information`` / ``Note`` / ``Error Message`` body and NO ``feed`` — a rate-limit would read as a
fake-empty success. ``_guard_in_band_error`` checks for those keys and raises loud BEFORE ``feed``
is touched, so the orchestrator isolates it and no fake-empty result is ever persisted.

Keyed: OFF unless a key is present (gated in the registry). ``method="none"`` — the sentiment
rides in ``news_sentiment``, not the bull/bear tally. The apikey is passed as a param, never in a
logged URL (the shared client redacts ``apikey=`` too); config lists it in ``secrets()``.
"""

from __future__ import annotations

import logging
from typing import Any

from abelard_common.http_client import HttpClient

from ..config import DEFAULT_AV_LIMIT, DEFAULT_AV_RELEVANCE_MIN, DEFAULT_AV_SORT
from ..errors import ChatterDaemonError
from ..schema import Metrics, NewsSentiment, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

AV_BASE = "https://www.alphavantage.co/query"
SOURCE_NAME = "alpha_vantage"
WINDOW_LABEL = "24h"
# AV returns errors as HTTP 200 with one of these keys instead of `feed` — the in-band footgun.
_AV_ERROR_KEYS = ("Error Message", "Information", "Note")


class AlphaVantageError(ChatterDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage=SOURCE_NAME)


def _band(score: float) -> str:
    """AV's overall_sentiment_label bands, applied to the aggregate score ([-1..+1], +=bull)."""
    if score <= -0.35:
        return "Bearish"
    if score <= -0.15:
        return "Somewhat-Bearish"
    if score < 0.15:
        return "Neutral"
    if score < 0.35:
        return "Somewhat-Bullish"
    return "Bullish"


def _as_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _aggregate(pairs: list[tuple[float, float]]) -> NewsSentiment | None:
    """(relevance, sentiment) pairs -> relevance-weighted mean sentiment + count + mean relevance.
    None when there is no qualifying article (an honest absence, never a fabricated neutral)."""
    if not pairs:
        return None
    wsum = sum(r for r, _ in pairs)
    score = (
        sum(r * s for r, s in pairs) / wsum
        if wsum > 0
        else sum(s for _, s in pairs) / len(pairs)
    )
    mean_rel = sum(r for r, _ in pairs) / len(pairs)
    return NewsSentiment(
        score=round(score, 4), label=_band(score),
        articles=len(pairs), mean_relevance=round(mean_rel, 4),
    )


class AlphaVantageSource:
    """Alpha Vantage NEWS_SENTIMENT — one keyed, in-band-error-guarded, relevance-gated call/scan,
    aggregated to a per-ticker news-sentiment read (method='none'; axis in `news_sentiment`)."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        api_key: str | None,
        relevance_min: float = DEFAULT_AV_RELEVANCE_MIN,
        limit: int = DEFAULT_AV_LIMIT,
        sort: str = DEFAULT_AV_SORT,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api_key = api_key
        self._relevance_min = relevance_min
        self._limit = limit
        self._sort = sort
        self._log = logger or logging.getLogger("chatter_daemon.alpha_vantage")
        self.client = client or HttpClient(user_agent="chatter-daemon", logger=self._log)

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext, **_: object) -> SourceResult:
        if not self.api_key:
            raise AlphaVantageError("ALPHAVANTAGE_API_KEY not set")
        actives = watchlist.active_tickers
        window = context.windows[WINDOW_LABEL]
        symbols = [s.symbol for s in actives]

        payload = self.client.get_json(
            AV_BASE,
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": ",".join(symbols),
                "limit": str(self._limit),
                "sort": self._sort,
                "apikey": self.api_key,  # param only — never in a logged URL
            },
        )
        self._guard_in_band_error(payload)  # MANDATORY before reading feed[]

        feed = payload.get("feed") if isinstance(payload, dict) else None
        if not isinstance(feed, list):
            raise AlphaVantageError(
                f"expected a 'feed' list from NEWS_SENTIMENT, got {type(feed).__name__}"
            )

        wanted = {s.upper() for s in symbols}
        per: dict[str, list[tuple[float, float]]] = {s: [] for s in wanted}
        gated = 0
        for article in feed:
            if not isinstance(article, dict):
                continue
            for ts in article.get("ticker_sentiment") or []:
                if not isinstance(ts, dict):
                    continue
                sym = str(ts.get("ticker", "")).upper()
                if sym not in wanted:
                    continue
                rel = _as_float(ts.get("relevance_score"))
                sen = _as_float(ts.get("ticker_sentiment_score"))
                if rel is None or sen is None:
                    continue
                if rel < self._relevance_min:
                    gated += 1  # low-relevance mention -> noise (the trust gate)
                    continue
                per[sym].append((rel, sen))

        warnings: list[str] = []
        if gated:
            warnings.append(
                f"alpha_vantage: {gated} ticker mentions below relevance {self._relevance_min}"
            )

        by_sym = {s.symbol.upper(): s for s in actives}
        records = [
            NormalizedRecord(
                watchlist=watchlist.name,
                scan_mode=context.scan_mode,
                canonical_ts=context.canonical_ts,
                window=window,
                source=SOURCE_NAME,
                ticker=by_sym[sym].symbol,  # preserve the watchlist's casing
                matched_by=["symbol"],
                metrics=Metrics(mention_count=len(per[sym])),
                sentiment=Sentiment(method="none"),
                news_sentiment=_aggregate(per[sym]),
                flags=[],
            )
            for sym in wanted
        ]
        return SourceResult(source=SOURCE_NAME, records=records, warnings=warnings)

    def _guard_in_band_error(self, payload: Any) -> None:
        """AV signals rate-limit / bad-call as HTTP 200 with an Information / Note / Error Message
        body and NO feed. Reading feed[] past this would persist a fake-empty result — THE footgun.
        Raise loud so the orchestrator isolates the source; never a silent empty."""
        if isinstance(payload, dict):
            for key in _AV_ERROR_KEYS:
                if key in payload:
                    raise AlphaVantageError(f"AV in-band {key}: {str(payload[key])[:160]}")


__all__ = ["SOURCE_NAME", "AlphaVantageError", "AlphaVantageSource"]
