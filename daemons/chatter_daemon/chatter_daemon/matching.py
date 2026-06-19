"""Shared free-text ticker matcher — dual-scan (symbol + company name), watchlist-
scoped, with match provenance. Used by /smg/ (Order 3) and Reddit (Order 6).

Reuses the abelard_common four-layer filter + alias resolver. `Matcher.match()`
returns `{ticker: {kinds}}` where each kind ⊆ {cashtag, symbol, name}. The universe
is the watchlist's own active symbols, so only watchlist tickers ever count;
`name_match:false` tickers contribute no name aliases (ticker-only). `\\b`-anchoring
and the collision word-lists come from the shared filter.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from abelard_common import ticker_noise
from abelard_common.company_aliases import NameResolver, build_name_resolver

from .watchlist import WatchlistConfig

# Provenance: the cashtag path, mirroring ticker_noise's cashtag regex so callers
# can tag `matched_by` without reaching into the shared module's internals.
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5}(?:\.[A-Za-z])?)\b")


def build_name_map(watchlist: WatchlistConfig, shared_map: dict[str, str]) -> dict[str, str]:
    """`name(lower) -> SYMBOL` for `name_match:true` tickers only.

    Inline `names[]` take precedence; a name_match:true ticker without inline names
    falls back to the shared alias map's entries for its symbol. `name_match:false`
    tickers contribute NO names (ticker-only).
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
    """`{symbol: resolved_names}` for every `name_match:true` ticker. A symbol
    mapping to `[]` resolves NOTHING — the silent can't-match bug the audit forbids."""
    name_map = build_name_map(watchlist, shared_map)
    inverted: dict[str, list[str]] = {}
    for name, sym in name_map.items():
        inverted.setdefault(sym, []).append(name)
    return {
        spec.symbol: sorted(inverted.get(spec.symbol, []))
        for spec in watchlist.active_tickers
        if spec.name_match
    }


@dataclass(frozen=True)
class Matcher:
    """Watchlist-scoped dual-scan matcher with match provenance."""

    universe: frozenset[str]
    blacklist: frozenset[str]
    common_words: frozenset[str]
    allowlist: frozenset[str]
    resolver: NameResolver | None

    @classmethod
    def for_watchlist(
        cls,
        watchlist: WatchlistConfig,
        *,
        shared_map: dict[str, str],
        blacklist,
        common_words,
        allowlist,
    ) -> "Matcher":
        return cls(
            universe=frozenset(s.symbol for s in watchlist.active_tickers),
            blacklist=frozenset(blacklist),
            common_words=frozenset(common_words),
            allowlist=frozenset(allowlist),
            resolver=build_name_resolver(build_name_map(watchlist, shared_map)),
        )

    def match(self, text: str) -> dict[str, set[str]]:
        """`{ticker: {kinds}}` for one post's text; kind ⊆ {cashtag, symbol, name}."""
        full = ticker_noise.tickers_in_post(
            text,
            universe=self.universe,
            blacklist=self.blacklist,
            common_words=self.common_words,
            allowlist=self.allowlist,
            name_resolver=self.resolver,
        )
        if not full:
            return {}
        cashtag = {m.upper() for m in _CASHTAG_RE.findall(text)} & self.universe
        named = self.resolver.tickers_in(text, self.universe) if self.resolver else set()
        out: dict[str, set[str]] = {}
        for sym in full:
            kinds: set[str] = set()
            if sym in cashtag:
                kinds.add("cashtag")
            if sym in named:
                kinds.add("name")
            if sym not in cashtag and sym not in named:
                kinds.add("symbol")
            out[sym] = kinds
        return out
