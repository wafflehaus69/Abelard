"""/smg/ plugin (Order 3) — dual-scan over 4chan /smg/, watchlist-scoped.

A free-text source. For each ACTIVE watchlist ticker, count distinct /smg/ posts
mentioning it by **cashtag**, **bare symbol**, or **company name** — via the shared
`matching.Matcher` (the dual-scan + `\\b` discipline reused by Reddit, Order 6).
The universe is the watchlist's own symbols (watchlist-scoped; universe-wide
extraction is ATTENTION, Order 8).

  - `matched_by` records which path hit: cashtag / symbol (bare) / name.
  - Collision-word names are **ticker-only** (`name_match:false` → no name aliases).
  - `rarity_hit` is set whenever a ticker appears at all (count >= 1): these
    serious large-caps surfacing in degen territory is the signal, not magnitude.
  - Haiku classifies the matched post text for stance, gated above the sentiment
    floor (Order 9); below the floor / no key / Haiku failure → `method="none"`.

The Fetcher gets chatter's logger injected so the 4chan transport's records route
through chatter's `_RedactingFilter` (the redaction loop §A set up). A no-live-
thread / fetch failure RAISES (NoSmgThreadError / FourchanError) and the
orchestrator isolates the source.
"""

from __future__ import annotations

import html
import logging
import re
from pathlib import Path

from abelard_common import fourchan_fetch, ticker_noise
from abelard_common.company_aliases import load_name_map

from ..config import (
    DEFAULT_SENTIMENT_MIN_MENTIONS,
    DEFAULT_USER_AGENT,
    DEFAULT_WORD_TICKER_ALLOWLIST,
    HAIKU_MODEL_ID,
)
from ..matching import Matcher, audit_name_match, build_name_map  # re-exported below
from ..schema import CostTelemetry, Metrics, NormalizedRecord, Sentiment
from ..sentiment import AnthropicProvider, SentimentError, classify_stance
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "smg"
WINDOW_LABEL = "24h"

# Re-exported for callers/tests that import the audit + name-map helpers from here.
__all__ = ["SmgSource", "audit_name_match", "build_name_map"]

_TAG_RE = re.compile(r"<[^>]+>")


def _clean_com(com: str) -> str:
    """4chan `com` is HTML (greentext spans, <br>, quote links). Strip tags + unescape
    entities so Haiku reads clean prose; the matcher still runs on the raw `com`."""
    return html.unescape(_TAG_RE.sub(" ", com)).strip()


class SmgSource:
    """Source adapter for 4chan /smg/. Free-text, dual-scan, watchlist-scoped."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        company_names_path: str | Path,
        common_words_path: str | Path,
        slang_blacklist_path: str | Path,
        word_ticker_allowlist: frozenset[str] = DEFAULT_WORD_TICKER_ALLOWLIST,
        user_agent: str = DEFAULT_USER_AGENT,
        anthropic_api_key: str | None = None,
        haiku_model: str = HAIKU_MODEL_ID,
        sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS,
        anthropic_client=None,
        fetcher: fourchan_fetch.Fetcher | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.smg")
        self._shared_map = load_name_map(Path(company_names_path))
        self._common_words = ticker_noise.load_common_words(Path(common_words_path))
        self._blacklist = ticker_noise.load_blacklist(Path(slang_blacklist_path))
        self._allowlist = frozenset(word_ticker_allowlist)
        self._user_agent = user_agent
        self._anthropic = AnthropicProvider(
            api_key=anthropic_api_key, client=anthropic_client, logger=self._log
        )
        self._haiku_model = haiku_model
        self._floor = sentiment_min_mentions
        self._fetcher = fetcher  # injected in tests; built per-fetch otherwise

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        matcher = Matcher.for_watchlist(
            watchlist,
            shared_map=self._shared_map,
            blacklist=self._blacklist,
            common_words=self._common_words,
            allowlist=self._allowlist,
        )
        window = context.windows[WINDOW_LABEL]

        fetcher = self._fetcher or fourchan_fetch.Fetcher(
            user_agent=self._user_agent, logger=self._log
        )
        # Loud-fail: NoSmgThreadError / FourchanError propagate -> orchestrator isolates.
        threads = fourchan_fetch.scrape_smg(fetcher)
        posts = [p for thread in threads for p in thread.posts]

        counts: dict[str, set[int]] = {}
        kinds: dict[str, set[str]] = {}
        texts: dict[str, dict[int, str]] = {}  # ticker -> {post_no: clean text} for Haiku
        for post in posts:
            com = post.get("com", "")
            hits = matcher.match(com)
            if not hits:
                continue
            post_no = int(post["no"])
            clean = _clean_com(com)
            for sym, ks in hits.items():
                counts.setdefault(sym, set()).add(post_no)
                kinds.setdefault(sym, set()).update(ks)
                if clean:
                    texts.setdefault(sym, {})[post_no] = clean

        cost = CostTelemetry()
        warnings: list[str] = []
        records: list[NormalizedRecord] = []
        for spec in watchlist.active_tickers:
            mentions = len(counts.get(spec.symbol, ()))
            sentiment = self._classify(
                spec.symbol, texts.get(spec.symbol, {}), cost, warnings
            )
            records.append(
                NormalizedRecord(
                    watchlist=watchlist.name,
                    scan_mode=context.scan_mode,
                    canonical_ts=context.canonical_ts,
                    window=window,
                    source=SOURCE_NAME,
                    ticker=spec.symbol,
                    matched_by=sorted(kinds.get(spec.symbol, set())),
                    metrics=Metrics(mention_count=mentions),
                    sentiment=sentiment,
                    flags=["rarity_hit"] if mentions >= 1 else [],
                )
            )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, cost=cost
        )

    def _classify(self, symbol, posts_text, cost, warnings):
        """Haiku stance over a ticker's /smg/ posts, gated above the sentiment floor.
        /smg/ has no native tags, so it's Haiku-or-none: below the floor, with no
        Anthropic key, or on a Haiku failure, method stays "none" (the count still
        ships). One call per ticker keeps each batch small — no output truncation."""
        if len(posts_text) < self._floor:
            return Sentiment(method="none")
        anthropic = self._anthropic.get()
        if anthropic is None:
            return Sentiment(method="none")
        posts = [
            {"post_id": str(no), "text": txt, "tickers": [symbol]}
            for no, txt in posts_text.items()
            if txt.strip()
        ]
        if not posts:
            return Sentiment(method="none")
        try:
            tallies = classify_stance(
                posts=posts, client=anthropic, model=self._haiku_model, cost=cost
            )
        except SentimentError as exc:
            self._log.warning("smg Haiku failed for %s: %s", symbol, exc)
            warnings.append(f"{symbol}: smg Haiku failed ({exc})")
            return Sentiment(method="none")
        t = tallies.get(symbol.upper(), {})
        return Sentiment(
            method="haiku",
            bullish=int(t.get("bullish", 0)),
            bearish=int(t.get("bearish", 0)),
            neutral=int(t.get("neutral", 0)),
        )
