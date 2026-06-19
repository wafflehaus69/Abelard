"""Finnhub company-news plugin — symbol-keyed 24h headline count (Order 2).

For each ACTIVE watchlist ticker, one Finnhub ``/company-news`` call over the
canonical 24h window. Emits a NormalizedRecord per ticker:
  - ``metrics.mention_count`` = headline count, ``metrics.headlines`` = the raw
    titles+URLs for Abelard to read (count plus heads, no classification).
  - ``matched_by = ["symbol"]`` — symbol-keyed; no company-name scan.
  - ``sentiment.method = "none"`` — news carries no stance.

Honest zeros emit records (empty list / ETF coverage gap / 404). A missing key,
a 429, or an auth/transport error RAISES — the orchestrator isolates the source
into the envelope's ``errors`` / ``sources`` and the other sources still run.

UTF-8 is forced by the shared ``HttpClient`` (decode obligation centralised in the
transport); the non-ASCII regression test drives a real Response through it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from abelard_common.http_client import HttpClient, NotFound

from ..config import DEFAULT_USER_AGENT
from ..errors import ChatterDaemonError
from ..schema import Headline, Metrics, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

FINNHUB_BASE = "https://finnhub.io/api/v1"
SOURCE_NAME = "finnhub_news"
WINDOW_LABEL = "24h"


class FinnhubError(ChatterDaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage=SOURCE_NAME)


class FinnhubNewsSource:
    """Source adapter for Finnhub company-news. Symbol-keyed, no LLM, no stance."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        api_key: str | None,
        user_agent: str = DEFAULT_USER_AGENT,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api_key = api_key
        self._log = logger or logging.getLogger("chatter_daemon.finnhub_news")
        # Inject the daemon logger so chatter's redaction filter covers the
        # transport's logs (the http_client also redacts token= in URLs).
        self.client = client or HttpClient(user_agent=user_agent, logger=self._log)

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        if not self.api_key:
            raise FinnhubError("FINNHUB_API_KEY not set")

        to_date = datetime.fromtimestamp(context.canonical_unix, tz=timezone.utc).date()
        from_date = datetime.fromtimestamp(
            context.canonical_unix - 86_400, tz=timezone.utc
        ).date()
        window = context.windows[WINDOW_LABEL]

        records: list[NormalizedRecord] = []
        for spec in watchlist.active_tickers:
            try:
                payload = self.client.get_json(
                    f"{FINNHUB_BASE}/company-news",
                    params={
                        "symbol": spec.symbol,
                        "from": from_date.isoformat(),
                        "to": to_date.isoformat(),
                        "token": self.api_key,
                    },
                )
            except NotFound:
                # No company-news coverage for this symbol -> honest zero, not a
                # source failure. (RateLimited / TransportError propagate: raise.)
                payload = []

            if not isinstance(payload, list):
                raise FinnhubError(
                    f"expected a list from /company-news for {spec.symbol}, "
                    f"got {type(payload).__name__}"
                )

            heads = _parse_headlines(payload)
            records.append(
                NormalizedRecord(
                    watchlist=watchlist.name,
                    scan_mode=context.scan_mode,
                    canonical_ts=context.canonical_ts,
                    window=window,
                    source=SOURCE_NAME,
                    ticker=spec.symbol,
                    matched_by=["symbol"],
                    metrics=Metrics(mention_count=len(heads), headlines=heads),
                    sentiment=Sentiment(method="none"),
                    flags=[],
                )
            )
        return SourceResult(source=SOURCE_NAME, records=records)


def _parse_headlines(payload: list[Any]) -> list[Headline]:
    """Keep items with a non-empty headline + url; drop the rest (Finnhub already
    bounds the window via from/to, so every kept item is an in-window headline)."""
    heads: list[Headline] = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        title = raw.get("headline")
        url = raw.get("url")
        if isinstance(title, str) and title.strip() and isinstance(url, str) and url.strip():
            heads.append(Headline(title=title, url=url))
    return heads
