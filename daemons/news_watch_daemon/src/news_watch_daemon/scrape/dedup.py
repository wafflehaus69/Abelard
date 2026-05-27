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

# Drop characters outside the union of:
#   - Original ASCII allow-set: lowercase letters, digits, space
#   - Major non-Latin script blocks that appear (or are likely to appear)
#     in tracked-source headlines.
#
# Added Task 2.5 (2026-05-27) — defensive fix for the latent bug that a
# hypothetical Cyrillic-only headline lacking any ASCII content (no t.me
# self-reference URL, no msg_id, no brand name) would normalize to "" and
# collide on dedupe_hash with every other such headline within the 72h
# window. Empirical probe against the persistent DB's 90 Ateobreaking rows
# showed all 90 have distinct hashes today because real posts contain
# enough ASCII leakage (t.me URLs, dates, numbers) to differentiate, but
# the invariant should be Unicode-aware regardless of accidental content
# shape. Future-proofs against Russian-government press releases or any
# tracked source that posts pure-script content without Latin fragments.
#
# Script-block selection covers major Asian, Middle Eastern, and European
# scripts. Future expansion (Tamil, Thai, Devanagari, etc.) requires an
# explicit additive change here PLUS a regression test confirming the
# existing English-content hashes stay invariant.
#
# Latin Extended (á, ü, ğ, é, etc.) is INTENTIONALLY NOT included: those
# diacritical variants are dropped pre AND post fix, preserving the
# pre-existing behavior on accented European names (Flávio, Türkiye,
# Erdoğan). Adding Latin Extended would be a quality improvement but
# requires its own scoped task with corpus re-inspection.
_DROP_CHARS_RE = re.compile(
    r"[^"
    r"a-z0-9 "                             # ASCII letters, digits, space
    r"Ѐ-ӿ"                       # Cyrillic
    r"一-鿿"                       # CJK Unified Ideographs
    r"぀-ゟ゠-ヿ"          # Hiragana + Katakana
    r"가-힯"                       # Hangul Syllables
    r"؀-ۿ"                       # Arabic
    r"֐-׿"                       # Hebrew
    r"Ͱ-Ͽ"                       # Greek
    r"]"
)


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
