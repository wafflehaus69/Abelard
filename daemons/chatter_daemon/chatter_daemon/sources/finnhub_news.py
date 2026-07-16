"""Finnhub company-news plugin — symbol-keyed 24h headline count (Order 2).

For each ACTIVE watchlist ticker, one Finnhub ``/company-news`` call over the
canonical 24h window. Emits a NormalizedRecord per ticker:
  - ``metrics.mention_count`` = headline count, ``metrics.headlines`` = the raw
    titles+URLs for Abelard to read (count plus heads, no classification).
  - ``matched_by = ["symbol"]`` — the query is symbol-keyed. CH-SRC-1 then applies
    a per-ticker title relevance gate (keep a head only if its title names the
    ticker) to drop Finnhub's peer/macro cross-tags; ``relevance_gate=False`` keeps
    every cross-tag (the pre-CH-SRC-1 behaviour).
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
from pathlib import Path
from typing import Any

from abelard_common.company_aliases import load_name_map
from abelard_common.http_client import HttpClient, NotFound

from ..config import DEFAULT_FINNHUB_RELEVANCE_GATE, DEFAULT_USER_AGENT
from ..errors import ChatterDaemonError
from ..matching import title_mentions_ticker, watchlist_alias_map
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
        company_names_path: str | Path | None = None,
        relevance_gate: bool = DEFAULT_FINNHUB_RELEVANCE_GATE,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api_key = api_key
        self._log = logger or logging.getLogger("chatter_daemon.finnhub_news")
        # Inject the daemon logger so chatter's redaction filter covers the
        # transport's logs (the http_client also redacts token= in URLs).
        self.client = client or HttpClient(user_agent=user_agent, logger=self._log)
        # CH-SRC-1: keep a head under T only if its title names T (drops Finnhub's peer/macro
        # cross-tags — ~67% of returned heads name no ticker). False = keep every cross-tag.
        # The company-name map backs that gate. (CH-SRC-2: the per-ticker news SUMMARY moved out
        # to news_summary.py, which reads Finnhub + Yahoo together — Finnhub is now LLM-free.)
        self._relevance_gate = relevance_gate
        self._shared_map = load_name_map(Path(company_names_path)) if company_names_path else {}

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext, **_: object) -> SourceResult:
        if not self.api_key:
            raise FinnhubError("FINNHUB_API_KEY not set")

        to_date = datetime.fromtimestamp(context.canonical_unix, tz=timezone.utc).date()
        from_date = datetime.fromtimestamp(
            context.canonical_unix - 86_400, tz=timezone.utc
        ).date()
        window = context.windows[WINDOW_LABEL]

        # CH-SRC-1: the FULL alias map (all tickers, incl name_match:false names like "micron" that a
        # news headline can trust though a noisy social source can't) — backs the relevance gate.
        aliases = watchlist_alias_map(watchlist, self._shared_map)

        records: list[NormalizedRecord] = []
        raw_items: list[str] = []  # Order 19: headlines for the history dump
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
            if self._relevance_gate:
                # Keep only heads whose title names THIS ticker — drops Finnhub's peer/macro
                # cross-tags (~67% of returned heads name no watchlist ticker), the main
                # cross-ticker duplicate. Summaries already gate the same way, so this only
                # removes noise the report would otherwise show + count.
                names = aliases.get(spec.symbol, ())
                heads = [h for h in heads if title_mentions_ticker(h.title, spec.symbol, names)]
            raw_items.extend(f"{spec.symbol}\t{h.title}" for h in heads)
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
        # No cost: the per-ticker news summary moved to news_summary.py (CH-SRC-2). Finnhub is a
        # pure headline count/list now — the summary reads its heads alongside Yahoo's, after the fan-out.
        return SourceResult(source=SOURCE_NAME, records=records, raw_items=raw_items)


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
