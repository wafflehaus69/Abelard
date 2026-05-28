"""Single-word frequency counter over a published-at-windowed headline corpus.

Pass E architectural piece (counter → threshold → cluster → orchestrator).
Counts case-insensitive alphabetic tokens (≥2 chars) across two adjacent
windows: the live window (`[now-24h, now]`) and the prior window
(`[now-48h, now-24h]`). Stopwords filtered before counting.

Tokenization: `\\b[a-zA-Z]{2,}\\b` then lowercased. Word-boundary discipline
matches Fix 2 (orchestrator keyword regex). Consequences accepted per Pass E
Q1: short acronyms colliding with stopwords (US/us, IT/it) are filtered;
punctuated forms (U.S.) don't tokenize. Mitigation via themes/tickers.

Returns a `TermCounts` dataclass with the two count dicts plus the
generating window timestamps. The threshold module reads from this; the
orchestrator passes it through.
"""

from __future__ import annotations

import re
import sqlite3
from collections import Counter
from dataclasses import dataclass


# Alphabetic-only, 2+ chars, word-boundary anchored. Excludes digits,
# apostrophes, dashes — keeps the counter focused on natural-language
# tokens. Matches Fix 2's word-boundary discipline.
_TOKEN_RE = re.compile(r"\b[a-zA-Z]{2,}\b")

# Window definitions are in seconds, anchored to a now_unix the caller
# passes in. Per Pass E spec, both windows are 24h.
WINDOW_SECONDS = 24 * 3600


@dataclass(frozen=True)
class TermCounts:
    """Snapshot of single-word frequencies across the live + prior windows.

    `window_counts`: term -> count of headlines in `[since, until]` that
    contain the term at least once (per-headline, not per-occurrence).
    `prior_counts`: term -> count of headlines in `[prior_since, since]`
    containing the term, same per-headline semantic.

    Per-headline (not per-occurrence) semantic is deliberate: a single
    headline that mentions "Iran" 5 times counts ONCE, not 5x. Matches
    the "attention shape" framing — 10 distinct headlines about a term
    is more interesting signal than 1 headline mentioning it 10 times.

    Window timestamps are recorded for downstream auditing and for the
    threshold module to surface in the brief envelope.
    """

    window_counts: dict[str, int]
    prior_counts: dict[str, int]
    window_since_unix: int
    window_until_unix: int
    prior_since_unix: int
    prior_until_unix: int


def tokenize(text: str | None, stopwords: frozenset[str]) -> set[str]:
    """Extract distinct lowercase tokens from `text`, filtering stopwords.

    Returns a SET (not list) to enforce per-headline distinct counting in
    the count_terms function. Tokens lowercased before stopword match.
    """
    if not text:
        return set()
    out: set[str] = set()
    for m in _TOKEN_RE.finditer(text):
        token = m.group(0).lower()
        if token not in stopwords:
            out.add(token)
    return out


def count_terms(
    conn: sqlite3.Connection,
    *,
    now_unix: int,
    stopwords: frozenset[str],
) -> TermCounts:
    """Build the two-window count dicts for one attention cycle.

    Live window:  `[now - 24h, now]`
    Prior window: `[now - 48h, now - 24h]`

    Both windows filter by `published_at_unix` (matches Pass C trigger
    semantics — content is "in window" by when it was published, not
    when it was fetched).
    """
    window_since = now_unix - WINDOW_SECONDS
    window_until = now_unix
    prior_since = now_unix - 2 * WINDOW_SECONDS
    prior_until = window_since

    # Pass F (2026-05-28): tokenize translated text when available,
    # fall back to original headline. Russian content with non-NULL
    # headline_en surfaces to the ATTENTION tokenizer via the translated
    # text; English content (headline_en IS NULL by design) falls
    # through unchanged via COALESCE.
    window_rows = conn.execute(
        "SELECT COALESCE(headline_en, headline) AS headline FROM headlines "
        "WHERE published_at_unix >= ? AND published_at_unix <= ?",
        (window_since, window_until),
    ).fetchall()
    prior_rows = conn.execute(
        "SELECT COALESCE(headline_en, headline) AS headline FROM headlines "
        "WHERE published_at_unix >= ? AND published_at_unix < ?",
        (prior_since, prior_until),
    ).fetchall()

    window_counter: Counter[str] = Counter()
    for row in window_rows:
        for token in tokenize(row[0], stopwords):
            window_counter[token] += 1

    prior_counter: Counter[str] = Counter()
    for row in prior_rows:
        for token in tokenize(row[0], stopwords):
            prior_counter[token] += 1

    return TermCounts(
        window_counts=dict(window_counter),
        prior_counts=dict(prior_counter),
        window_since_unix=window_since,
        window_until_unix=window_until,
        prior_since_unix=prior_since,
        prior_until_unix=window_since,
    )


__all__ = ["TermCounts", "WINDOW_SECONDS", "count_terms", "tokenize"]
