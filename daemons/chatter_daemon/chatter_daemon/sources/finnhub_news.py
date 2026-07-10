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
from pathlib import Path
from typing import Any

from abelard_common.company_aliases import load_name_map
from abelard_common.http_client import HttpClient, NotFound

from ..config import (
    DEFAULT_SUMMARY_COST_CAP_USD,
    DEFAULT_SUMMARY_MODEL,
    DEFAULT_USER_AGENT,
    HAIKU_MODEL_ID,
)
from ..errors import ChatterDaemonError
from ..matching import build_name_map, title_mentions_ticker
from ..schema import CostTelemetry, Headline, Metrics, NormalizedRecord, Sentiment
from ..sentiment import AnthropicProvider, SentimentError, summarize_news, summary_cost_usd
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

FINNHUB_BASE = "https://finnhub.io/api/v1"
SOURCE_NAME = "finnhub_news"
WINDOW_LABEL = "24h"
_SUMMARY_HEADLINE_CAP = 15  # top-N relevant headlines per summary call (recency-ordered)


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
        anthropic_api_key: str | None = None,
        haiku_model: str = HAIKU_MODEL_ID,
        summary_model: str = DEFAULT_SUMMARY_MODEL,
        summary_cost_cap_usd: float = DEFAULT_SUMMARY_COST_CAP_USD,
        client: HttpClient | None = None,
        anthropic_client: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api_key = api_key
        self._log = logger or logging.getLogger("chatter_daemon.finnhub_news")
        # Inject the daemon logger so chatter's redaction filter covers the
        # transport's logs (the http_client also redacts token= in URLs).
        self.client = client or HttpClient(user_agent=user_agent, logger=self._log)
        # Order 15: named-news summary — shared provider (auto-off without a key), the
        # company-name map for the direct-mention gate, and the per-scan cost cap.
        self._anthropic = AnthropicProvider(
            api_key=anthropic_api_key, client=anthropic_client, logger=self._log
        )
        self._haiku_model = haiku_model
        self._summary_model = summary_model  # Order 19: Sonnet for the prose summary
        self._cost_cap = summary_cost_cap_usd
        self._shared_map = load_name_map(Path(company_names_path)) if company_names_path else {}

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext, **_: object) -> SourceResult:
        if not self.api_key:
            raise FinnhubError("FINNHUB_API_KEY not set")

        to_date = datetime.fromtimestamp(context.canonical_unix, tz=timezone.utc).date()
        from_date = datetime.fromtimestamp(
            context.canonical_unix - 86_400, tz=timezone.utc
        ).date()
        window = context.windows[WINDOW_LABEL]

        aliases = self._aliases(watchlist)            # {SYMBOL: [name words]} for the gate
        anthropic = self._anthropic.get()             # None without a key -> no summaries
        cost = CostTelemetry()
        warnings: list[str] = []

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
            raw_items.extend(f"{spec.symbol}\t{h.title}" for h in heads)
            summary = self._summarize(spec.symbol, heads, aliases, anthropic, cost, warnings)
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
                    news_summary=summary,
                    flags=[],
                )
            )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, cost=cost, raw_items=raw_items
        )

    def _aliases(self, watchlist: WatchlistConfig) -> dict[str, list[str]]:
        """`{SYMBOL: [name words]}` from the shared company-name map — the SAME map the
        report's relevance filter uses (one source of truth; name_match:false tickers
        contribute no names, so they gate on the symbol token alone)."""
        out: dict[str, list[str]] = {}
        for name, sym in build_name_map(watchlist, self._shared_map).items():
            out.setdefault(sym, []).append(name)
        return out

    def _summarize(self, symbol, heads, aliases, anthropic, cost, warnings) -> str | None:
        """One Haiku summary of the headlines that NAME this ticker — gated on evidence
        (>=1 direct mention) and the per-scan cost cap. None (no call, no spend) when there
        is no named news; None + a fail-loud warning on cap-hit or Haiku failure. Never
        poisons the count/headlines, which stand on their own."""
        if anthropic is None:
            return None  # no Anthropic key -> no summaries (auto-gated)
        names = aliases.get(symbol, ())
        relevant = [h.title for h in heads if title_mentions_ticker(h.title, symbol, names)]
        if not relevant:
            return None  # no named news -> the skip condition (normal, not a failure)
        if summary_cost_usd(cost) >= self._cost_cap:
            warnings.append(f"{symbol}: summary skipped — scan cost cap ${self._cost_cap:.2f}")
            return None
        company = names[0].title() if names else symbol
        try:
            return (
                summarize_news(
                    titles=relevant[:_SUMMARY_HEADLINE_CAP],
                    ticker=symbol,
                    company=company,
                    client=anthropic,
                    model=self._summary_model,
                    cost=cost,
                )
                or None
            )
        except SentimentError as exc:
            self._log.warning("finnhub summary failed for %s: %s", symbol, exc)
            warnings.append(f"{symbol}: news summary failed ({exc})")
            return None


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
