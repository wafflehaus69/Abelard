"""Translation result schema — closed-Literal status discrimination.

Pure data. No I/O. Consumed by translation.telegram_native (producer)
and Pass F orchestrator integration (consumer, Commit 2).

The status Literal is the discriminator that downstream code branches
on. Each value maps to a specific outcome class:

  - ok                       : translation succeeded; translated_text populated
  - rate_limited             : Telegram FloodWait exhausted retries; retry next cycle
  - message_deleted          : MessageIdInvalidError — message gone from peer
  - channel_inaccessible     : ChannelInvalidError / ChannelPrivateError on get_entity
  - translation_error        : generic RPCError; translation rejected for unknown reason
  - premium_required         : Telegram Premium-gated this API path; flip to DeepL
                               fallback (architecture-of-record per Pass F doctrine)
  - network_error            : ConnectionError / OSError / Timeout exhausted retries
  - skipped_already_english  : caller-provided language == 'en'; no API call made
  - skipped_no_text          : original_text empty or whitespace-only after strip

The `premium_required` status is structurally important: if Telegram
ever re-gates messages.translateText to Premium-only (a known risk
flagged in the Pass F doctrine), the orchestrator must detect this
without parsing error_detail strings to flip the YAML config to the
DeepL fallback path. The status discriminator preserves that
operational option.

Module added Pass F Commit 1 (2026-05-28).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


TranslationStatus = Literal[
    "ok",
    "rate_limited",
    "message_deleted",
    "channel_inaccessible",
    "translation_error",
    "premium_required",
    "network_error",
    "skipped_already_english",
    "skipped_no_text",
]


# Closed enumeration accessor for downstream tests and audit tooling
# (mirrors lang.classifier.LANGUAGES pattern).
TRANSLATION_STATUSES: tuple[str, ...] = (
    "ok",
    "rate_limited",
    "message_deleted",
    "channel_inaccessible",
    "translation_error",
    "premium_required",
    "network_error",
    "skipped_already_english",
    "skipped_no_text",
)


class TranslationResult(BaseModel):
    """One translation outcome — succeeded or any failure class.

    One TranslationResult is produced per input msg_id, regardless of
    batching boundaries on the Telethon side. Batching is an internal
    implementation detail of telegram_native.translate_telegram_messages;
    the result list has 1:1 cardinality with the input msg_id list.

    `latency_ms` is per-message attribution from the batch call's
    round-trip time (batch latency / batch size). When status != "ok",
    latency_ms still records the time-to-failure for the batch.

    `attempts` is 1 for a single-try success; 2 or 3 for retries.
    `attempts` is also incremented for failed retries, so a rate_limited
    result with attempts=3 indicates all retry budget was consumed.
    """

    model_config = ConfigDict(extra="forbid")

    source_msg_id: str = Field(min_length=1)
    channel_username: str = Field(min_length=1)
    original_text: str
    translated_text: str | None = None
    status: TranslationStatus
    error_detail: str | None = None
    latency_ms: int = Field(ge=0)
    attempts: int = Field(ge=1, le=10)


__all__ = ["TRANSLATION_STATUSES", "TranslationResult", "TranslationStatus"]
