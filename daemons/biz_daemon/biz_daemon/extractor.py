"""Ticker extraction — the precision core.

Per post's cleaned `com`, two confidence paths with a four-layer filter on the
bare path. Filters apply in this order:

  a. Cashtag (high): `$AAPL`, `$MOG.A`. Uppercased, validated against the
     universe. A cashtag BYPASSES every filter below — it is never blocked by
     the length, wordlist, or denylist rules. It must still be a real symbol.
  b. Length rule: a BARE (non-cashtag) candidate of 1 letter is rejected.
     Single-letter tickers (A, F, T, ...) require a cashtag to count; bare
     2-char tickers (MU, MA, ...) pass.
  c. Wordlist rule: a bare candidate whose lowercased form is a common English
     word is rejected, UNLESS it is in the word_ticker_allowlist (real tickers
     that collide with words, e.g. NOW/META/CORN).
  d. Denylist rule: a bare candidate in the /biz/-slang denylist is rejected.
     Comparison is case-insensitive (both sides uppercased).

A bare candidate must also be a real symbol (in the universe). The `{1,5}`
length cap in the pattern is the cheap pre-filter — 6+ char all-caps
(BAGHOLDER, JANNIES) never reach validation.

The mention metric is DISTINCT posts mentioning a ticker, not raw occurrences —
one post spamming `GME` ten times counts once.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

# Capture keeps class-share symbols (MOG.A) intact; the tokenizer does not
# split on `.` inside an uppercase run.
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{1,5}(?:\.[A-Za-z])?)\b")
_BARE_RE = re.compile(r"\b[A-Z]{1,5}(?:\.[A-Z])?\b")

# Bare candidates with fewer than this many letters are rejected (the length
# rule). Single-letter tickers require a cashtag; bare 2-char tickers pass.
BARE_MIN_LEN = 2

_EMPTY: frozenset[str] = frozenset()


@dataclass
class TickerHits:
    ticker: str
    post_ids: set[int] = field(default_factory=set)

    @property
    def mention_count(self) -> int:
        return len(self.post_ids)


@dataclass(frozen=True)
class NameResolver:
    """Resolves S&P 500 company names in prose to tickers (whole-word, ci).

    A name only resolves if its ticker is in the universe — consistency with
    the symbol/bare validation. Folding the resolved ticker into the same
    per-post set means "Nvidia" and "NVDA" in one post count once.
    """

    pattern: "re.Pattern[str]"
    mapping: dict[str, str]

    def tickers_in(self, text: str, universe: frozenset[str] | set[str]) -> set[str]:
        out: set[str] = set()
        if not text:
            return out
        for match in self.pattern.finditer(text):
            ticker = self.mapping.get(match.group(0).lower())
            if ticker and ticker in universe:
                out.add(ticker)
        return out


def load_name_map(path: Path) -> dict[str, str]:
    """Load the `name<TAB or ws>TICKER` map. Name lowercased, ticker uppercased.

    The ticker is the final whitespace-delimited token, so multi-word names
    (`home depot HD`) parse correctly. Blank lines and #comments ignored.
    """
    mapping: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.rsplit(None, 1)  # split on the last whitespace run
        if len(parts) != 2:
            continue
        name, ticker = parts[0].strip().lower(), parts[1].strip().upper()
        if name and ticker:
            mapping[name] = ticker
    return mapping


def build_name_resolver(mapping: dict[str, str]) -> NameResolver | None:
    """Compile a whole-word, case-insensitive resolver from a name map."""
    if not mapping:
        return None
    # Longest names first so multi-word names win over any shorter prefix.
    names = sorted(mapping.keys(), key=len, reverse=True)
    pattern = re.compile(
        r"\b(?:" + "|".join(re.escape(n) for n in names) + r")\b",
        re.IGNORECASE,
    )
    return NameResolver(pattern=pattern, mapping=mapping)


def _letter_len(sym: str) -> int:
    """Letter count, ignoring the class-share dot (MOG.A -> 4)."""
    return len(sym.replace(".", ""))


def tickers_in_post(
    com: str,
    *,
    universe: frozenset[str] | set[str],
    blacklist: frozenset[str] | set[str],
    common_words: frozenset[str] | set[str] = _EMPTY,
    allowlist: frozenset[str] | set[str] = _EMPTY,
    name_resolver: NameResolver | None = None,
) -> set[str]:
    """Return the set of valid tickers mentioned in one post's cleaned text.

    `blacklist` is the /biz/-slang denylist; `common_words` is the lowercased
    common-English-word set; `allowlist` is the uppercase set of real tickers
    that collide with common words (overrides the wordlist rule only).
    `name_resolver` additionally resolves S&P 500 company names in prose; its
    hits fold into the same per-post set, so a name + its symbol count once.
    """
    found: set[str] = set()

    # (a) Cashtag path — bypasses every bare-path filter below.
    for raw in _CASHTAG_RE.findall(com):
        sym = raw.upper()
        if sym in universe:
            found.add(sym)

    # Bare path — length -> universe -> wordlist (allowlist) -> denylist.
    for raw in _BARE_RE.findall(com):
        sym = raw.upper()
        if sym in found:
            continue
        # (b) length rule
        if _letter_len(sym) < BARE_MIN_LEN:
            continue
        # universe validation — a bare candidate must be a real symbol
        if sym not in universe:
            continue
        # (c) wordlist rule, with allowlist override
        if sym not in allowlist and sym.lower() in common_words:
            continue
        # (d) denylist rule (case-insensitive: both sides uppercased)
        if sym in blacklist:
            continue
        found.add(sym)

    # Name resolution — additive; resolver already gates on the universe.
    if name_resolver is not None:
        found |= name_resolver.tickers_in(com, universe)

    return found


def extract(
    posts: Iterable[dict[str, Any]],
    *,
    universe: frozenset[str] | set[str],
    blacklist: frozenset[str] | set[str],
    common_words: frozenset[str] | set[str] = _EMPTY,
    allowlist: frozenset[str] | set[str] = _EMPTY,
    name_resolver: NameResolver | None = None,
) -> dict[str, TickerHits]:
    """Build the per-scrape frequency table over all validated tickers.

    `posts` are post dicts with `no` and cleaned `com`. Returns
    {ticker: TickerHits} where mention_count == distinct posts.
    """
    table: dict[str, TickerHits] = {}
    for post in posts:
        post_no = int(post["no"])
        for sym in tickers_in_post(
            post.get("com", ""),
            universe=universe,
            blacklist=blacklist,
            common_words=common_words,
            allowlist=allowlist,
            name_resolver=name_resolver,
        ):
            table.setdefault(sym, TickerHits(ticker=sym)).post_ids.add(post_no)
    return table
