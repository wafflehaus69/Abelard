"""Term-to-headlines retrieval for ATTENTION synthesis.

Given a crossing term and the live window's published-at range, retrieve
every headline whose text contains the term as a whole word (case-insensitive,
word-boundary verified). This produces the "cluster" for ATTENTION — same
naming convention as Pass C clusters but a simpler set-membership relation.

Performance note: SQLite's `LIKE` is ASCII-case-insensitive by default for
the `%term%` pattern, but it has no word-boundary semantics. We use a
LIKE-filter pre-narrow at the DB layer and post-verify word boundaries in
Python with a compiled regex. The two-stage approach keeps the regex check
off the full headline corpus while still enforcing boundary discipline.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterHeadline:
    """One headline in an ATTENTION cluster — view shape for the prompt.

    Matches `synthesize.cluster.ClusterInput` fields where they overlap.
    `headline_id` lets the orchestrator reference original rows for the
    archive trail; `publisher` is the headline's `raw_source` for display.
    """

    headline_id: str
    source: str
    headline: str
    url: str | None
    publisher: str | None
    published_at_unix: int
    # Original-content language ('en' or a source language). Folded into the
    # cluster row so Pass F's cross-language check needs no second per-crossing
    # language query. Defaults None for callers that don't need it.
    language: str | None = None


def _compile_term_pattern(term: str) -> re.Pattern[str]:
    """Compile a word-boundary, case-insensitive regex for `term`.

    Escapes regex special chars in the term itself (defensive — terms
    come from headline tokenization so they're alphabetic, but
    re.escape costs nothing and prevents surprises if the tokenizer
    ever widens).
    """
    return re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)


def cluster_for_term(
    conn: sqlite3.Connection,
    *,
    term: str,
    window_since_unix: int,
    window_until_unix: int,
) -> list[ClusterHeadline]:
    """Retrieve all headlines containing `term` (whole-word, case-insensitive)
    in the published-at window. Results ordered newest-first by published_at.
    """
    # Two-stage filter: SQL LIKE pre-narrows (cheap on indexable substrings)
    # and word-boundary regex post-verifies (correct semantic, ~free at the
    # cluster's small N).
    # Pass F (2026-05-28): match against COALESCE(headline_en, headline) so
    # Russian content surfaces via its translated text. The token regex
    # is Latin-only and won't match Cyrillic anyway, but the LIKE pre-
    # narrow must look at the translated text to find candidate rows.
    like_pattern = f"%{term}%"
    rows = conn.execute(
        "SELECT headline_id, source, COALESCE(headline_en, headline) AS headline, "
        "       url, raw_source, published_at_unix, language "
        "FROM headlines "
        "WHERE published_at_unix >= ? AND published_at_unix <= ? "
        "AND COALESCE(headline_en, headline) LIKE ? "
        "ORDER BY published_at_unix DESC",
        (window_since_unix, window_until_unix, like_pattern),
    ).fetchall()

    boundary_re = _compile_term_pattern(term)
    out: list[ClusterHeadline] = []
    for row in rows:
        if boundary_re.search(row[2]) is None:
            continue   # LIKE matched a substring but not a whole word
        out.append(ClusterHeadline(
            headline_id=row[0],
            source=row[1],
            headline=row[2],
            url=row[3],
            publisher=row[4],
            published_at_unix=row[5],
            language=row[6],
        ))
    return out


__all__ = ["ClusterHeadline", "cluster_for_term"]
