"""Headline normalization and dedup-hash computation.

Two stories from different aggregators with slightly different casing or
punctuation should collide on `compute_dedupe_hash`. The normalization
is deliberately aggressive — case-insensitive, punctuation-stripped,
whitespace-collapsed, truncated. This errs on the side of dropping
legitimate variants rather than letting near-duplicates through.

The 32-char prefix of SHA256 is overkill for distinguishing headlines
within the 72-hour dedup window, and it leaves room for future fields
(e.g. publication date bucket) without changing the column type.
"""

from __future__ import annotations

import hashlib
import re


_WHITESPACE_RE = re.compile(r"\s+")
_DROP_CHARS_RE = re.compile(r"[^a-z0-9 ]")


def normalize_headline(headline: str) -> str:
    """Pure-text normalization step. Exposed for tests and Pass B reuse.

    Steps (brief's order with two cleanup passes so the result is stable
    regardless of where punctuation or whitespace appear):

      1. Lowercase.
      2. Normalize all whitespace (incl. tabs/newlines) to single space.
      3. Drop characters outside [a-z0-9 ].
      4. Collapse any whitespace runs created by step 3 + strip ends.
      5. Truncate to first 80 chars.
    """
    if headline is None:
        return ""
    lowered = headline.lower()
    spaces_only = _WHITESPACE_RE.sub(" ", lowered)
    cleaned = _DROP_CHARS_RE.sub("", spaces_only)
    collapsed = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return collapsed[:80]


def compute_dedupe_hash(headline: str) -> str:
    """SHA256(normalized_headline)[:32].

    The normalize step is intentionally idempotent and reversible only
    in the sense that the same input always produces the same hash —
    different surface forms of the same story converge to the same key.
    """
    normalized = normalize_headline(headline)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


__all__ = ["compute_dedupe_hash", "normalize_headline"]
