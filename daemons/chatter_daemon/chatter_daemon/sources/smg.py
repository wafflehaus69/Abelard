"""/smg/ plugin (Order 3) — dual-scan over 4chan /smg/, watchlist-scoped.

A free-text source. For each ACTIVE watchlist ticker, count distinct /smg/ posts
mentioning it by **cashtag**, **bare symbol**, or **company name** — using the
shared four-layer noise filter + alias resolver, all `\\b`-anchored. The universe
is the watchlist's own symbols (watchlist-scoped, not universe-wide extraction —
that is ATTENTION, Order 8).

  - `matched_by` records which path hit: cashtag / symbol (bare) / name.
  - Collision-word names are **ticker-only**: `name_match:false` tickers
    contribute no name aliases, so they match by symbol/cashtag only.
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
import re
from pathlib import Path

from abelard_common import fourchan_fetch, ticker_noise
from abelard_common.company_aliases import build_name_resolver, load_name_map

from ..config import DEFAULT_USER_AGENT, DEFAULT_WORD_TICKER_ALLOWLIST
from ..schema import Metrics, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "smg"
WINDOW_LABEL = "24h"

# Provenance: the cashtag path, mirroring ticker_noise's cashtag regex so the
# plugin can tag matched_by without reaching into the shared module's internals.
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5}(?:\.[A-Za-z])?)\b")


def build_name_map(watchlist: WatchlistConfig, shared_map: dict[str, str]) -> dict[str, str]:
    """`name(lower) -> SYMBOL` for `name_match:true` tickers only.

    Inline `names[]` take precedence; a name_match:true ticker without inline
    names falls back to the shared alias map's entries for its symbol.
    `name_match:false` tickers contribute NO names (ticker-only).
    """
    out: dict[str, str] = {}
    for spec in watchlist.active_tickers:
        if not spec.name_match:
            continue
        if spec.names:
            for n in spec.names:
                out[n.lower()] = spec.symbol
        else:
            for name, sym in shared_map.items():
                if sym == spec.symbol:
                    out[name] = spec.symbol
    return out


def audit_name_match(
    watchlist: WatchlistConfig, shared_map: dict[str, str]
) -> dict[str, list[str]]:
    """`{symbol: resolved_names}` for every `name_match:true` ticker.

    A symbol mapping to `[]` resolves NOTHING — the silent can't-match bug the
    audit gate forbids. The caller flips those to `name_match:false`.
    """
    name_map = build_name_map(watchlist, shared_map)
    inverted: dict[str, list[str]] = {}
    for name, sym in name_map.items():
        inverted.setdefault(sym, []).append(name)
    return {
        spec.symbol: sorted(inverted.get(spec.symbol, []))
        for spec in watchlist.active_tickers
        if spec.name_match
    }


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
        universe = frozenset(s.symbol for s in watchlist.active_tickers)
        name_map = build_name_map(watchlist, self._shared_map)
        resolver = build_name_resolver(name_map)
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
            com = post.get("com", "")
            full = ticker_noise.tickers_in_post(
                com,
                universe=universe,
                blacklist=self._blacklist,
                common_words=self._common_words,
                allowlist=self._allowlist,
                name_resolver=resolver,
            )
            if not full:
                continue
            post_no = int(post["no"])
            cashtag = {m.upper() for m in _CASHTAG_RE.findall(com)} & universe
            named = resolver.tickers_in(com, universe) if resolver else set()
            for sym in full:
                counts.setdefault(sym, set()).add(post_no)
                ks = kinds.setdefault(sym, set())
                if sym in cashtag:
                    ks.add("cashtag")
                if sym in named:
                    ks.add("name")
                if sym not in cashtag and sym not in named:
                    ks.add("symbol")

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
