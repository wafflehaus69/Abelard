"""Translation pass (Pass F foundation).

Telegram-native primary path via MTProto `messages.translateText`.
DeepL fallback architecturally documented but deferred to a later
commit; the YAML config flag for the fallback flip lands in
Commit 2 wire-up.

Module added Pass F Commit 1 (2026-05-28). Translation results
populate the `headlines.headline_en` column (schema v4) at scrape
time (Commit 2 orchestrator integration); downstream consumers
(theme tagger, Pass E ATTENTION counter) read with
COALESCE(headline_en, headline) fallback so English-content rows
pass through unchanged.

Per the Pass F doctrine (commit 284a340):
  - Telegram-native chosen over DeepL after Task 0 probe confirmed
    non-Premium burner can use messages.translateText with acceptable
    translation quality and 233ms-batch latency
  - DeepL kept as YAML-config-flippable documented fallback for
    Premium re-gating / sustained FloodWait / quality-degradation
    scenarios
  - The `premium_required` status in TranslationStatus is the
    structurally-important discriminator that lets the orchestrator
    detect Premium-gating without parsing error_detail strings
"""

from .telegram_native import (
    DEFAULT_BATCH_SIZE,
    MAX_FLOOD_WAIT_S,
    MAX_RETRIES,
    translate_telegram_messages,
)
from .types import TRANSLATION_STATUSES, TranslationResult, TranslationStatus

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "MAX_FLOOD_WAIT_S",
    "MAX_RETRIES",
    "TRANSLATION_STATUSES",
    "TranslationResult",
    "TranslationStatus",
    "translate_telegram_messages",
]
