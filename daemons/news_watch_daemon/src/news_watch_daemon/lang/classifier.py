"""Cyrillic-vs-Latin script-ratio language classifier.

Used at orchestrator-level headline ingest (single call site:
`scrape.orchestrator._insert_headline_and_tags`) and by the
`db backfill-language` CLI subcommand for retroactive classification
of pre-migration rows.

Algorithm:

  1. Count Cyrillic chars in U+0400..U+04FF (Cyrillic + Cyrillic
     Supplement), the range that covers Russian, Ukrainian, Belarusian,
     and the Caucasus/Central-Asian Slavic alphabets the Ateobreaking
     channel uses.
  2. Count Latin alphabetic chars (`ch.isascii() and ch.isalpha()`).
  3. ratio = cyr / (cyr + lat); 0.0 if total == 0.
  4. Bucket:
       cyr_ratio >= 0.50            -> "ru"
       cyr_ratio <= 0.20            -> "en"   (lat_ratio >= 0.80)
       0.20 < cyr_ratio < 0.50      -> "mixed"
  5. Zero-alphabetic-char text (empty, emoji-only, URL-only,
     punctuation-only) -> "en" (operational default: the Pass F gate
     is `WHERE language != 'en'`, so `en` correctly skips translation).

Non-Cyrillic / non-Latin scripts (CJK, Arabic, Hebrew, Greek, etc.)
currently count as zero on both sides and fall into the
no-alphabetic-chars branch, yielding `en`. When such corpora become
operationally relevant, the classifier learns a third script class
and the Literal expands to include `other`.

The classifier is pure — no I/O, no logging, deterministic on input.
Cheap enough to run on every headline insertion (microseconds per
4096-char message).
"""

from __future__ import annotations

from typing import Literal


# The full set of language labels persisted to the headlines.language
# column. Pass F's translation logic reads against this set; any
# extension (e.g. adding "other") requires deliberate downstream
# updates and is therefore explicit here.
Language = Literal["ru", "en", "mixed"]

LANGUAGES: tuple[str, ...] = ("ru", "en", "mixed")


# Thresholds locked 2026-05-27 (Task 2). cyr_ratio_ru is the LOWER bound
# for the `ru` class (inclusive); cyr_ratio_en_max is the UPPER bound for
# the `en` class (inclusive). The (cyr_ratio_en_max, cyr_ratio_ru) open
# interval is `mixed`.
_CYR_RATIO_RU_MIN = 0.50
_CYR_RATIO_EN_MAX = 0.20


def classify_language(text: str | None) -> Language:
    """Return the language label for a headline text.

    Args:
        text: Headline text. `None` or empty/whitespace-only treated
            as zero-alphabetic-chars → returns `en`.

    Returns:
        One of `Language` literals: `"ru"`, `"en"`, or `"mixed"`.
    """
    if text is None:
        return "en"
    cyr = 0
    lat = 0
    for ch in text:
        # Cyrillic block: U+0400 (Ѐ) through U+04FF (ӿ). Covers Russian
        # (А-Я / а-я), Ukrainian (Ї, І, Є, Ґ), Belarusian (Ў), and
        # related Slavic alphabets. Cyrillic Extended blocks (U+0500+)
        # are not currently observed in tracked-source corpora; if they
        # appear (Komi, Aleut, etc.) the upper bound expands.
        if "Ѐ" <= ch <= "ӿ":
            cyr += 1
        elif ch.isascii() and ch.isalpha():
            lat += 1
    total = cyr + lat
    if total == 0:
        # Edge-case branch: empty / whitespace-only / emoji-only /
        # URL-only / punctuation-only / digits-only / any text with
        # zero alphabetic chars. Returns `en` so the Pass F gate
        # `WHERE language != 'en'` correctly skips it (no translatable
        # content to translate).
        return "en"
    cyr_ratio = cyr / total
    if cyr_ratio >= _CYR_RATIO_RU_MIN:
        return "ru"
    if cyr_ratio <= _CYR_RATIO_EN_MAX:
        return "en"
    return "mixed"


__all__ = ["LANGUAGES", "Language", "classify_language"]
