"""Language detection at headline-ingest.

Surfaces a per-row `language` Literal so Pass F's translation gate
(`WHERE language != 'en'`) can find Russian and mixed-script headlines
efficiently. Foundation commit landed Task 2 (2026-05-27); the
translation pass itself ships separately in Pass F.

Classification is intentionally narrow at this pass:

  - ru:    cyr_ratio >= 0.50      (majority Cyrillic by letter count)
  - en:    cyr_ratio <= 0.20      (equivalently lat_ratio >= 0.80)
  - mixed: 0.20 < cyr_ratio < 0.50

The "other" class is reserved for future non-Cyrillic / non-Latin
scripts (CJK, Arabic, etc.) but the current classifier does not assign
it. When a third script class becomes operationally relevant, the
Literal type expands and downstream consumers update with awareness.

Edge cases (empty, whitespace-only, emoji-only, URL-only, punctuation-
only, any text with zero alphabetic chars) all classify as `en` —
operationally correct because the Pass F gate is `language != 'en'`,
so `en` means "skip translation". Mis-labeling a punctuation-only
Russian message as `en` has no translatable content lost.
"""

from .classifier import LANGUAGES, Language, classify_language

__all__ = ["LANGUAGES", "Language", "classify_language"]
