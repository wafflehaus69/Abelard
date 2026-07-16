"""Shared free-text ticker matcher — dual-scan (symbol + company name), watchlist-
scoped, with match provenance. Used by /smg/ (Order 3) and ATTENTION discovery (Order 8).

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


def title_mentions_ticker(title: str, symbol: str, aliases=None) -> bool:
    """Does a (news) headline NAME this ticker — its symbol as a whole word (so 'DE' does
    not match 'DECIDE'), or any company-name alias as a substring ('duke energy')? The ONE
    source of truth for 'direct mention', shared by the report's headline-relevance filter
    (Order 11) and the Finnhub news-summary gate (Order 15)."""
    t = title.lower()
    if symbol and re.search(rf"\b{re.escape(symbol.lower())}\b", t):
        return True
    return any(a and a in t for a in (aliases or ()))


def watchlist_alias_map(watchlist: WatchlistConfig, shared_map: dict[str, str]) -> dict[str, list[str]]:
    """`{SYMBOL: [lowercased name aliases]}` for the headline relevance / market-roundup gates:
    the shared S&P name map (name_match:true tickers) PLUS each ticker's OWN spec `names`. Unlike
    `build_name_map`, this INCLUDES a name_match:false collision ticker's names (MU -> 'micron') —
    which is safe here because these gates run over a SCOPED headline (a ticker's own feed) or
    COUNT how many tickers a headline names, not free-text /smg/ where the collision bites."""
    out: dict[str, list[str]] = {}
    for name, sym in build_name_map(watchlist, shared_map).items():
        out.setdefault(sym, []).append(name)
    for spec in watchlist.active_tickers:
        bucket = out.setdefault(spec.symbol, [])
        for n in spec.names:
            nl = n.lower()
            if nl not in bucket:
                bucket.append(nl)
    return out


def count_named_tickers(title: str, alias_map: dict[str, list[str]]) -> int:
    """How many DISTINCT watchlist tickers a headline title names (symbol or alias) — the
    'market roundup' gauge. A 'Dow movers: AAPL, MSFT, NVDA, AMD' headline names many; a
    single-name headline names one. Used to drop broad-market cross-tags (Finnhub + Yahoo)."""
    return sum(1 for sym, aliases in alias_map.items() if title_mentions_ticker(title, sym, aliases))


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

    @classmethod
    def for_universe(
        cls,
        universe,
        *,
        blacklist,
        common_words,
        allowlist,
        resolver: NameResolver | None = None,
    ) -> "Matcher":
        """Universe-mode (ATTENTION, Order 8): propose ANY ticker-shaped token that is
        in `universe` (the validated Finnhub symbol set) — not just a watchlist's. No
        name resolver by default (off-watchlist discovery is cashtag/bare-symbol only;
        there's no company-name map for the whole universe), so `match` yields only
        the `cashtag`/`symbol` kinds, never `name`."""
        return cls(
            universe=frozenset(universe),
            blacklist=frozenset(blacklist),
            common_words=frozenset(common_words),
            allowlist=frozenset(allowlist),
            resolver=resolver if resolver is not None else build_name_resolver({}),
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
