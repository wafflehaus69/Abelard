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
  - `sentiment.method = "none"` — no LLM, no stance.

The Fetcher gets chatter's logger injected so the 4chan transport's records route
through chatter's `_RedactingFilter` (the redaction loop §A set up). A no-live-
thread / fetch failure RAISES (NoSmgThreadError / FourchanError) and the
orchestrator isolates the source.
"""

from __future__ import annotations

import logging
from pathlib import Path

from abelard_common import fourchan_fetch, ticker_noise
from abelard_common.company_aliases import load_name_map

from ..config import DEFAULT_USER_AGENT, DEFAULT_WORD_TICKER_ALLOWLIST
from ..matching import Matcher, audit_name_match, build_name_map  # re-exported below
from ..schema import Metrics, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "smg"
WINDOW_LABEL = "24h"

# Re-exported for callers/tests that import the audit + name-map helpers from here.
__all__ = ["SmgSource", "audit_name_match", "build_name_map"]


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
        fetcher: fourchan_fetch.Fetcher | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.smg")
        self._shared_map = load_name_map(Path(company_names_path))
        self._common_words = ticker_noise.load_common_words(Path(common_words_path))
        self._blacklist = ticker_noise.load_blacklist(Path(slang_blacklist_path))
        self._allowlist = frozenset(word_ticker_allowlist)
        self._user_agent = user_agent
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
        for post in posts:
            hits = matcher.match(post.get("com", ""))
            if not hits:
                continue
            post_no = int(post["no"])
            for sym, ks in hits.items():
                counts.setdefault(sym, set()).add(post_no)
                kinds.setdefault(sym, set()).update(ks)

        records: list[NormalizedRecord] = []
        for spec in watchlist.active_tickers:
            mentions = len(counts.get(spec.symbol, ()))
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
                    sentiment=Sentiment(method="none"),
                    flags=["rarity_hit"] if mentions >= 1 else [],
                )
            )
        return SourceResult(source=SOURCE_NAME, records=records)
