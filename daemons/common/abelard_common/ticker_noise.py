"""Ticker extraction — the precision core (four-layer noise filter).

Per post's cleaned text, two confidence paths with a four-layer filter on the
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
  d. Denylist rule: a bare candidate in the slang denylist is rejected.
     Comparison is case-insensitive (both sides uppercased).

A bare candidate must also be a real symbol (in the universe). The `{1,5}`
length cap in the pattern is the cheap pre-filter — 6+ char all-caps
(BAGHOLDER, JANNIES) never reach validation.

The mention metric is DISTINCT posts mentioning a ticker, not raw occurrences —
one post spamming `GME` ten times counts once.

This module also owns the denylist / common-word list loaders and their
CLI-backed maintenance helpers (formerly ``biz_daemon.blacklist``): the filter
and the word-lists it consults travel together. Each consuming daemon passes the
paths to its own bundled list files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .company_aliases import NameResolver
from .errors import DaemonError

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

    `blacklist` is the slang denylist; `common_words` is the lowercased
    common-English-word set; `allowlist` is the uppercase set of real tickers
    that collide with common words (overrides the wordlist rule only).
    `name_resolver` additionally resolves company names in prose; its hits fold
    into the same per-post set, so a name + its symbol count once.
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


# --- denylist / common-word list loaders + maintenance (formerly blacklist.py) ---
#
# The denylist rejects bare-token ticker candidates that collide with finance/
# forum slang (FUD, DD, ATH, ...). An explicit `$CASHTAG` bypasses it — see
# `tickers_in_post`. The lists are config-file backed so they grow after live
# scrapes without a code change.


class BlacklistError(DaemonError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="blacklist")


def load_blacklist(path: Path) -> frozenset[str]:
    """Load the uppercase slang denylist. Blank lines and #comments ignored.

    Tokens are uppercased on load so denylist enforcement is case-insensitive
    against the (also-uppercased) bare candidates in the extractor.
    """
    if not path.exists():
        raise BlacklistError(f"blacklist file not found: {path}")

    tokens: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        token = line.strip()
        if not token or token.startswith("#"):
            continue
        tokens.add(token.upper())
    return frozenset(tokens)


_CLI_SECTION = "# --- added via CLI ---"


def _normalize_tokens(tokens: list[str]) -> list[str]:
    """Uppercase, strip, drop blanks, dedupe preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        u = tok.strip().upper()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def add_tokens(path: Path, tokens: list[str]) -> tuple[list[str], list[str]]:
    """Append new denylist tokens. Uppercases, dedupes vs the file, appends.

    Returns (added, skipped) where skipped were already present. The file is
    re-read fresh on the next scrape, so the change takes effect immediately.
    """
    normalized = _normalize_tokens(tokens)
    existing = load_blacklist(path) if path.exists() else frozenset()
    added = [t for t in normalized if t not in existing]
    skipped = [t for t in normalized if t in existing]

    if added:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        chunk = ""
        if text and not text.endswith("\n"):
            chunk += "\n"
        if _CLI_SECTION not in text:
            chunk += f"\n{_CLI_SECTION}\n"
        chunk += "".join(f"{t}\n" for t in added)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(chunk)
    return added, skipped


def remove_tokens(path: Path, tokens: list[str]) -> list[str]:
    """Remove denylist token lines from the file. Returns the tokens removed."""
    if not path.exists():
        return []
    targets = set(_normalize_tokens(tokens))
    removed: list[str] = []
    kept: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and stripped.upper() in targets:
            removed.append(stripped.upper())
            continue
        kept.append(line)
    path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    # dedupe preserving order
    seen: set[str] = set()
    return [t for t in removed if not (t in seen or seen.add(t))]


def load_common_words(path: Path) -> frozenset[str]:
    """Load the lowercased common-English-word set for the wordlist filter."""
    if not path.exists():
        raise BlacklistError(f"common-words file not found: {path}")

    words: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        word = line.strip()
        if not word or word.startswith("#"):
            continue
        words.add(word.lower())
    return frozenset(words)
