"""Per-ticker news summary over ALL article feeds (CH-SRC-2 — the analyzed "why").

Finnhub, Yahoo (and Alpha Vantage when keyed) each surface per-ticker HEADLINES. This step reads
them TOGETHER and emits ONE summary per ticker, so the "why" reflects every news feed rather than
Finnhub alone. It supersedes Finnhub's former inline per-source summary — Finnhub is now a pure
headline count/list, and this runs once after the source fan-out (see the CLI).

INPUT ORDER: the freshest feed (Yahoo) leads so its net-new items are never truncated by the top-N
cap; then Finnhub's named news; then AV — deduped by normalized title (first occurrence wins). Each
feed's own heads already passed a per-ticker relevance gate, and this re-gates on evidence (>=1
title that NAMES the ticker) so a summary never runs on empty/irrelevant input.

DEGRADE-CLEAN: gated on the per-scan cost cap and on evidence; a cap-hit or an LLM failure yields
no summary for that ticker plus a loud warning — never a fabrication, never a sunk scan. No key ->
no summaries at all (auto-gated). Keeps Chatter's locked per-ticker architecture (no cross-ticker
logic; a ticker's summary sees only its own headlines)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from abelard_common.company_aliases import load_name_map

from .config import DEFAULT_SUMMARY_COST_CAP_USD, DEFAULT_SUMMARY_MODEL
from .matching import title_mentions_ticker, watchlist_alias_map
from .schema import CostTelemetry, NormalizedRecord
from .sentiment import AnthropicProvider, SentimentError, summarize_news, summary_cost_usd
from .watchlist import WatchlistConfig

# The article/headline feeds, in summary-input PRIORITY order (freshest first so the top-N cap
# keeps it). Social sources (/smg/, StockTwits, Twitter) carry posts, not articles — excluded.
NEWS_SOURCES = ("yahoo_rss", "finnhub_news", "alpha_vantage")
_SUMMARY_HEADLINE_CAP = 15  # top-N titles per summary call (freshest-feed-first)
_NONWORD_RE = re.compile(r"[^a-z0-9]+")


def _norm_title(title: str) -> str:
    """Dedup key — lowercase, punctuation-stripped, whitespace-collapsed (matches the Yahoo/Finnhub
    dedup normalization, so the same story across feeds folds to one input line)."""
    return " ".join(_NONWORD_RE.sub(" ", title.lower()).split())


class NewsSummarizer:
    """One per-ticker news summary over the union of every article feed's headlines."""

    name = "news_summary"

    def __init__(
        self,
        *,
        anthropic_api_key: str | None = None,
        company_names_path: str | Path | None = None,
        summary_model: str = DEFAULT_SUMMARY_MODEL,
        summary_cost_cap_usd: float = DEFAULT_SUMMARY_COST_CAP_USD,
        anthropic_client: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.news_summary")
        # Shared provider — auto-off (get() -> None) without a key, so no-key scans skip summaries.
        self._anthropic = AnthropicProvider(
            api_key=anthropic_api_key, client=anthropic_client, logger=self._log
        )
        self._summary_model = summary_model  # Sonnet for the prose summary (Order 19)
        self._cost_cap = summary_cost_cap_usd
        self._shared_map = load_name_map(Path(company_names_path)) if company_names_path else {}

    def summarize(
        self,
        records: list[NormalizedRecord],
        watchlists: list[WatchlistConfig],
        *,
        cost: CostTelemetry,
    ) -> tuple[dict[tuple[str, str], str], list[str]]:
        """Return ``{(watchlist, ticker): summary}`` for every ticker with >=1 named headline across
        the news feeds. Accumulates token usage into ``cost`` (doctrine #8). Warnings (cap-hit, LLM
        failure) are returned, not raised — one bad ticker never sinks the rest."""
        warnings: list[str] = []
        out: dict[tuple[str, str], str] = {}
        anthropic = self._anthropic.get()
        if anthropic is None:
            return out, warnings  # no Anthropic key -> no summaries (auto-gated)

        alias_maps = {w.name: watchlist_alias_map(w, self._shared_map) for w in watchlists}
        grouped = self._group_headlines(records)  # {(wl, ticker): [titles]} freshest-first, deduped

        for (wl, ticker), titles in grouped.items():
            names = alias_maps.get(wl, {}).get(ticker, ())
            relevant = [t for t in titles if title_mentions_ticker(t, ticker, names)]
            if not relevant:
                continue  # no NAMED news -> skip (normal, not a failure)
            if summary_cost_usd(cost) >= self._cost_cap:
                warnings.append(f"{ticker}: summary skipped — scan cost cap ${self._cost_cap:.2f}")
                continue
            company = names[0].title() if names else ticker
            try:
                text = summarize_news(
                    titles=relevant[:_SUMMARY_HEADLINE_CAP],
                    ticker=ticker,
                    company=company,
                    client=anthropic,
                    model=self._summary_model,
                    cost=cost,
                )
            except SentimentError as exc:
                self._log.warning("news summary failed for %s: %s", ticker, exc)
                warnings.append(f"{ticker}: news summary failed ({exc})")
                continue
            if text:
                out[(wl, ticker)] = text
        return out, warnings

    def _group_headlines(
        self, records: list[NormalizedRecord]
    ) -> dict[tuple[str, str], list[str]]:
        """``{(watchlist, ticker): [titles]}`` merged across NEWS_SOURCES in priority order
        (Yahoo -> Finnhub -> AV), deduped by normalized title so a story carried by two feeds
        appears once. Ticker order follows first appearance in ``records`` (deterministic)."""
        per_src: dict[str, dict[tuple[str, str], list[str]]] = {s: {} for s in NEWS_SOURCES}
        order: list[tuple[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()
        for r in records:
            if r.source not in per_src:
                continue
            key = (r.watchlist, r.ticker)
            if key not in seen_keys:
                seen_keys.add(key)
                order.append(key)
            heads = getattr(r.metrics, "headlines", None) or []
            if heads:
                per_src[r.source].setdefault(key, []).extend(h.title for h in heads)

        out: dict[tuple[str, str], list[str]] = {}
        for key in order:
            seen: set[str] = set()
            merged: list[str] = []
            for src in NEWS_SOURCES:  # freshest feed first
                for title in per_src[src].get(key, ()):
                    nk = _norm_title(title)
                    if nk and nk not in seen:
                        seen.add(nk)
                        merged.append(title)
            out[key] = merged
        return out


__all__ = ["NEWS_SOURCES", "NewsSummarizer"]
