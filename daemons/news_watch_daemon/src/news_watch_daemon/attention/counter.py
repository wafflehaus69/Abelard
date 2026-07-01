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

# Default window length: 24 hours. Both live and prior windows are the same
# length (configurable per-call via `window_hours` kwarg to `count_terms`).
# WINDOW_SECONDS is preserved as a module-level export for backwards
# compatibility with code that imported the constant directly (and as the
# numeric default for `window_hours=24` invocations).
WINDOW_SECONDS = 24 * 3600

# Bounds for the `window_hours` kwarg. Matches the synthesize CLI's [1, 168]
# range. Lower bound is 1h (the smallest meaningful window for headline
# frequency analysis); upper bound is 168h = 7 days (anything longer makes
# "novel" framing meaningless).
WINDOW_HOURS_MIN = 1
WINDOW_HOURS_MAX = 168


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
    window_hours: int = 24,
) -> TermCounts:
    """Build the two-window count dicts for one attention cycle.

    Live window:  `[now - window_hours*3600, now]`
    Prior window: `[now - 2*window_hours*3600, now - window_hours*3600]`

    Both windows filter by `published_at_unix` (matches Pass C trigger
    semantics — content is "in window" by when it was published, not
    when it was fetched).

    The default `window_hours=24` makes this function bit-identical to
    its pre-2026-05-29 behavior — Pass E's auto-attention inside scrape
    and the standalone `attention` CLI both continue to use 24h windows
    without code change. Full Brief (Commit A foundation, 2026-05-29)
    plumbs custom windows through for spec test T11 and downstream
    Full Brief composition.

    Note on threshold tuning: the cold-start constants
    `COLD_START_WINDOW_MIN=10` and `COLD_START_PRIOR_MAX=3` (defined in
    `attention/threshold.py`) are absolute and tuned for 24h windows.
    They do NOT scale automatically with `window_hours`. Non-24h windows
    may produce fewer crossings (sub-24h: 10 hits is a high bar) or more
    crossings (>24h: 10 hits is a low bar). This is a deliberate v1
    choice; threshold scaling is a separate design pass once empirical
    crossings-per-cycle data exists at non-24h windows.
    """
    if not WINDOW_HOURS_MIN <= window_hours <= WINDOW_HOURS_MAX:
        raise ValueError(
            f"window_hours must be in [{WINDOW_HOURS_MIN}, {WINDOW_HOURS_MAX}]; "
            f"got {window_hours}"
        )
    window_seconds = window_hours * 3600
    window_since = now_unix - window_seconds
    window_until = now_unix
    prior_since = now_unix - 2 * window_seconds
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


def count_terms_collapsed(
    conn: sqlite3.Connection,
    *,
    now_unix: int,
    stopwords: frozenset[str],
    window_hours: int = 24,
) -> TermCounts:
    """Bigram-collapsed variant of `count_terms` (Order: adjacency map).

    Fetches the same two windows as `count_terms` but routes the raw
    headlines through `attention.adjacency.build_attention_list`, which
    counts adjacent bigrams and collapses a promoted pair's two constituent
    unigrams into the single pair. The returned `TermCounts` is keyed by the
    COLLAPSED term text ("supreme court" as one key; "birthright citizenship"
    as one key) rather than fragmented unigrams.

    This is the single seam that makes the whole Pass E surface — the
    threshold gate, the near-miss table, and convergence — operate on
    collapsed terms, so a multi-word story crosses ONCE (one attention
    brief) instead of firing a redundant brief per constituent word.

    `prior_counts` carries the prior-window count for each surviving term
    (bigram-vs-bigram, unigram-vs-unigram), which is exactly what the gate's
    prior<3 novelty test consumes. Window math is identical to `count_terms`
    (window inclusive both ends; prior inclusive lower, exclusive upper).
    """
    if not WINDOW_HOURS_MIN <= window_hours <= WINDOW_HOURS_MAX:
        raise ValueError(
            f"window_hours must be in [{WINDOW_HOURS_MIN}, {WINDOW_HOURS_MAX}]; "
            f"got {window_hours}"
        )
    window_seconds = window_hours * 3600
    window_since = now_unix - window_seconds
    window_until = now_unix
    prior_since = now_unix - 2 * window_seconds
    prior_until = window_since

    window_heads = [
        row[0]
        for row in conn.execute(
            "SELECT COALESCE(headline_en, headline) AS headline FROM headlines "
            "WHERE published_at_unix >= ? AND published_at_unix <= ?",
            (window_since, window_until),
        ).fetchall()
    ]
    prior_heads = [
        row[0]
        for row in conn.execute(
            "SELECT COALESCE(headline_en, headline) AS headline FROM headlines "
            "WHERE published_at_unix >= ? AND published_at_unix < ?",
            (prior_since, prior_until),
        ).fetchall()
    ]

    # Local import avoids a module-load cycle risk and keeps counter.py's
    # foundational surface import-light; adjacency imports nothing from here.
    from .adjacency import build_attention_list

    terms = build_attention_list(window_heads, prior_heads, stopwords)
    window_counts = {t.text: t.window_count for t in terms}
    prior_counts = {t.text: t.prior_count for t in terms}

    return TermCounts(
        window_counts=window_counts,
        prior_counts=prior_counts,
        window_since_unix=window_since,
        window_until_unix=window_until,
        prior_since_unix=prior_since,
        prior_until_unix=prior_until,
    )


__all__ = [
    "TermCounts",
    "WINDOW_HOURS_MAX",
    "WINDOW_HOURS_MIN",
    "WINDOW_SECONDS",
    "count_terms",
    "count_terms_collapsed",
    "tokenize",
]
