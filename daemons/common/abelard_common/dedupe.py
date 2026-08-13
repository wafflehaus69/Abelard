"""Text normalization and dedupe-hash computation, shared across daemons.

Hoisted 2026-08-11 from `news_watch_daemon/scrape/dedup.py`, which was the
original, after `scout_daemon` copied it and the two began drifting apart in
name (never in behaviour). Consolidating here ends the copy-per-daemon pattern
that already produced three HttpClient implementations.

CANONICAL NAME IS `normalize_text`. News Watch called it `normalize_headline`
and Scout called it `normalize_title`; the function normalizes a string and
knows nothing about headlines or titles, so neither domain name earns the slot
in a shared library. Consumers pass whatever string they dedupe on.

THE CHARACTER CLASS IS LOAD-BEARING AND FROZEN. Any change to `_DROP_CHARS_RE`
alters every stored hash in every consuming daemon, silently: old rows stop
colliding with new ones and duplicates start appearing days later with no error
anywhere. A change here requires a regression test proving existing hashes stay
invariant on BOTH corpora, per the original module's own docstring.

Latin Extended is INTENTIONALLY EXCLUDED. Accented forms (Flávio, Türkiye,
Erdoğan) are dropped rather than folded — that was the original behaviour and
preserving it matters more than handling accents well, because changing it
would rewrite history.

WHAT THIS MODULE DOES NOT DO: it computes a weak, collision-tolerant key for
noticing that two records look like the same thing. It is not an identity
function. Callers that need stable identity should key on a source's own
immutable id, not on normalized text -- Scout learned this when a Dework task
and an Opire reward both turned out to be titled "c1work".
"""

from __future__ import annotations

import hashlib
import re

_WHITESPACE_RE = re.compile(r"\s+")

# Transcribed byte-for-byte from news_watch_daemon/scrape/dedup.py. Verified
# identical to scout_daemon's copy before the hoist: both regexes matched
# exactly, and the two implementations agreed on all 710 strings tested
# (525 Scout titles, 167 real News Watch headlines of which 114 non-ASCII,
# plus 18 edge cases) with zero mismatches.
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


def normalize_text(text: str | None) -> str:
    """Lowercase, collapse whitespace, drop out-of-class characters, truncate.

    Steps, in an order chosen so the result is stable regardless of where
    punctuation or whitespace appear:

      1. Lowercase.
      2. Normalize all whitespace (incl. tabs/newlines) to single spaces.
      3. Drop characters outside the allowed class.
      4. Collapse whitespace runs created by step 3, and strip the ends.
      5. Truncate to the first 80 characters.

    `None` returns `""` -- both source implementations already did this, so
    the differing type hints (`str` vs `str | None`) were cosmetic.
    """
    if text is None:
        return ""
    lowered = text.lower()
    spaces_only = _WHITESPACE_RE.sub(" ", lowered)
    cleaned = _DROP_CHARS_RE.sub("", spaces_only)
    collapsed = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return collapsed[:80]


def compute_dedupe_hash(text: str | None) -> str:
    """SHA256(normalize_text(text))[:32].

    32 hex chars is overkill for distinguishing records inside any realistic
    dedup window, and the headroom leaves room to fold in another field later
    without changing the column type.
    """
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()[:32]


__all__ = ["normalize_text", "compute_dedupe_hash"]
