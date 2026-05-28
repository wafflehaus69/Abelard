"""Telegram-native translation via MTProto `messages.translateText`.

Pass F (2026-05-28). Translates Telegram-source headlines from their
native language to English by referencing the original message via
peer + msg_id, leveraging Telegram's server-side translation feature.

Architecture per Pass F doctrine (commit 284a340 / Task 0 probe
outcome 2026-05-27): Telegram-native is locked as the primary
translation path; DeepL stays as documented YAML-config-flippable
fallback for Premium re-gating / sustained FloodWait scenarios.

This module:

  - Accepts a list of msg_ids from a single channel (peer)
  - Batches calls internally (default 10 per batch; tunable)
  - Handles FloodWait with bounded retry (3 attempts, jittered, hard-
    capped at 300s per single sleep)
  - Maps Telethon exceptions to closed-Literal TranslationResult.status
    values — including premium_required for the Telegram-Premium-gating
    failure mode the Pass F doctrine anticipates
  - Returns one TranslationResult per input msg_id (1:1 cardinality)

Failure semantics (Commit 1 design, surfaces in Commit 2 orchestrator):
rate-limited messages return TranslationResult with status='rate_limited';
**the caller is expected to retry on a subsequent cycle, not within
the same call**. Rate-limited messages stay in the translation-pending
queue (`WHERE language != 'en' AND headline_en IS NULL`) and get
re-attempted on the next scrape cycle naturally — no special re-queue
logic needed at the orchestrator level. The daemon continues
translating remaining batches after a rate_limited result; it does not
abort the cycle.

Form-(a) peer+id call shape (matches Task 0 probe verbatim). Form-(b)
text-arbitrary is supported by the MTProto method but deferred until a
non-Telegram non-English source actually arrives. Tonight's queue
(134 ru + 2 mixed) is 100% Ateobreaking-sourced; form (a) covers it.

Side effect of form (a): translation operates on Telegram's current
server-side message state. If a message was edited or deleted between
scrape and translation, we get the current state or a
message_deleted status respectively. For news channels with rare
edits, the current-state behavior is acceptable; deletion is a clean
error path.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from typing import Any

from telethon import errors
from telethon.tl.functions.messages import TranslateTextRequest

from .types import TranslationResult, TranslationStatus


_LOG = logging.getLogger("news_watch_daemon.translation.telegram_native")


# ---------- tunables ----------

# Batch size constants. Per-call value is parameterized on the public
# entry point (`batch_size=` kwarg) so Commit 2 wiring can override
# from config/translation.yaml. Default 10 is conservative pending
# empirical rate-limit calibration. Task 0 (2026-05-27) probed
# 3-message batch at 233ms; 10-message batch estimated ~500ms based on
# linear amortization. Raise after empirical validation.
DEFAULT_BATCH_SIZE = 10
_BATCH_SIZE_MIN = 1
_BATCH_SIZE_MAX = 100   # hard ceiling for input validation

# FloodWait retry policy. Telegram's e.seconds is the minimum wait;
# jitter is added on top to prevent thundering-herd across concurrent
# batches. Hard cap on per-sleep wait prevents silent 10+min stalls
# when Telegram reports account-level throttling.
MAX_RETRIES = 3
FLOOD_WAIT_JITTER_S = (1.0, 5.0, 15.0)  # progressive jitter per retry
MAX_FLOOD_WAIT_S = 300                   # hard cap; > this surfaces immediately

# Network-error retry policy (separate from FloodWait).
NETWORK_RETRY_DELAY_S = 2.0
NETWORK_MAX_RETRIES = 1

# Telegram channel username validation (mirrors sources/telegram.py).
_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$")


# ---------- public entry point ----------


async def translate_telegram_messages(
    client: Any,
    *,
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    to_lang: str = "en",
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[TranslationResult]:
    """Translate a list of Telegram message IDs from a single channel.

    Batches internally; returns one TranslationResult per input msg_id
    (1:1 cardinality, in input order).

    Args:
        client: Connected Telethon TelegramClient. Caller owns the
            client lifecycle (this function does NOT call connect /
            disconnect / is_user_authorized).
        channel_username: Source channel name without `@` prefix.
            Validated against Telegram's 5-32 char username
            constraint.
        msg_ids: List of integer message IDs from the channel.
        original_texts: dict mapping msg_id → original headline text
            (the verbatim text the daemon stored). Used to populate
            TranslationResult.original_text per msg_id so callers can
            see what was sent for translation. Caller must supply one
            entry per msg_id in msg_ids.
        to_lang: ISO 639-1 target language code (default "en").
        batch_size: Per-batch msg_id count. Default DEFAULT_BATCH_SIZE.
            Bounded by [_BATCH_SIZE_MIN, _BATCH_SIZE_MAX].

    Returns:
        list[TranslationResult] in input order, 1:1 with msg_ids.

    Raises:
        ValueError: malformed channel_username, empty/invalid msg_ids,
            missing original_texts entries, batch_size out of bounds.
        (No other exceptions propagate; Telethon failures map to
         TranslationResult.status values.)

    Failure semantics (relied on by Commit 2 orchestrator integration):
    rate-limited messages return TranslationResult with
    status='rate_limited'; the caller is expected to retry on a
    subsequent cycle, not within the same call. The function continues
    translating remaining batches after a rate_limited result.
    """
    # ---- input validation ----
    if not isinstance(channel_username, str) or not _USERNAME_RE.match(channel_username):
        raise ValueError(
            "channel_username must match Telegram's 5-32 char username "
            f"constraint; got {channel_username!r}"
        )
    if not isinstance(msg_ids, list):
        raise ValueError(f"msg_ids must be a list of ints; got {type(msg_ids).__name__}")
    for mid in msg_ids:
        if not isinstance(mid, int) or isinstance(mid, bool) or mid <= 0:
            raise ValueError(f"msg_ids entries must be positive ints; got {mid!r}")
    if not isinstance(original_texts, dict):
        raise ValueError("original_texts must be dict[int, str]")
    missing = [m for m in msg_ids if m not in original_texts]
    if missing:
        raise ValueError(
            f"original_texts missing entries for msg_ids: {missing[:5]}"
            + (" ..." if len(missing) > 5 else "")
        )
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise ValueError(f"batch_size must be an int; got {type(batch_size).__name__}")
    if not (_BATCH_SIZE_MIN <= batch_size <= _BATCH_SIZE_MAX):
        raise ValueError(
            f"batch_size must be in [{_BATCH_SIZE_MIN}, {_BATCH_SIZE_MAX}]; "
            f"got {batch_size}"
        )

    # ---- empty input early-return ----
    if not msg_ids:
        return []

    # ---- entity resolution (once per call) ----
    try:
        entity = await client.get_entity(f"@{channel_username}")
    except errors.ChannelPrivateError as exc:
        return _all_inaccessible_results(
            channel_username, msg_ids, original_texts,
            error_detail=f"ChannelPrivateError: {exc}",
        )
    except errors.UsernameNotOccupiedError as exc:
        return _all_inaccessible_results(
            channel_username, msg_ids, original_texts,
            error_detail=f"UsernameNotOccupiedError: {exc}",
        )
    except errors.UsernameInvalidError as exc:
        return _all_inaccessible_results(
            channel_username, msg_ids, original_texts,
            error_detail=f"UsernameInvalidError: {exc}",
        )

    # ---- batch loop ----
    results: list[TranslationResult] = []
    for batch_start in range(0, len(msg_ids), batch_size):
        batch = msg_ids[batch_start:batch_start + batch_size]
        batch_results = await _translate_batch(
            client=client,
            entity=entity,
            channel_username=channel_username,
            batch_msg_ids=batch,
            original_texts=original_texts,
            to_lang=to_lang,
        )
        results.extend(batch_results)
    return results


# ---------- internal helpers ----------


async def _translate_batch(
    *,
    client: Any,
    entity: Any,
    channel_username: str,
    batch_msg_ids: list[int],
    original_texts: dict[int, str],
    to_lang: str,
) -> list[TranslationResult]:
    """Translate one batch with FloodWait + network-error retry.

    Returns one TranslationResult per msg_id in the batch. All
    msg_ids in a single batch share the same status when the call
    succeeds or fails wholesale; only per-message attribution differs
    in latency_ms and original_text.
    """
    attempt = 0
    network_attempt = 0
    while True:
        attempt += 1
        start_perf = time.perf_counter()
        try:
            response = await client(TranslateTextRequest(
                peer=entity,
                id=batch_msg_ids,
                to_lang=to_lang,
            ))
        except errors.FloodWaitError as exc:
            elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
            wait_seconds = getattr(exc, "seconds", 0) or 0
            # Hard-cap surface: too-long waits surface immediately
            # without sleeping (account-level throttling indicator).
            if wait_seconds > MAX_FLOOD_WAIT_S:
                _LOG.warning(
                    "translate batch FloodWait %ds exceeds cap %ds; surfacing rate_limited "
                    "(channel=@%s batch_size=%d attempts=%d)",
                    wait_seconds, MAX_FLOOD_WAIT_S, channel_username,
                    len(batch_msg_ids), attempt,
                )
                return _all_rate_limited_results(
                    channel_username, batch_msg_ids, original_texts,
                    error_detail=f"FloodWait {wait_seconds}s exceeds cap {MAX_FLOOD_WAIT_S}s",
                    latency_ms=elapsed_ms,
                    attempts=attempt,
                )
            # Within cap: maybe retry
            if attempt < MAX_RETRIES:
                jitter = FLOOD_WAIT_JITTER_S[attempt - 1]
                sleep_total = float(wait_seconds) + jitter
                _LOG.info(
                    "translate batch FloodWait %ds (attempt %d/%d); sleeping %.1fs (jitter %.1fs) "
                    "(channel=@%s batch_size=%d)",
                    wait_seconds, attempt, MAX_RETRIES, sleep_total, jitter,
                    channel_username, len(batch_msg_ids),
                )
                await asyncio.sleep(sleep_total)
                continue  # retry
            # Retries exhausted
            _LOG.warning(
                "translate batch FloodWait retries exhausted (channel=@%s batch_size=%d attempts=%d)",
                channel_username, len(batch_msg_ids), attempt,
            )
            return _all_rate_limited_results(
                channel_username, batch_msg_ids, original_texts,
                error_detail=f"FloodWait retries exhausted ({MAX_RETRIES})",
                latency_ms=elapsed_ms,
                attempts=attempt,
            )

        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
            network_attempt += 1
            if network_attempt <= NETWORK_MAX_RETRIES:
                _LOG.info(
                    "translate batch network error %s (network-retry %d/%d); sleeping %.1fs "
                    "(channel=@%s batch_size=%d)",
                    type(exc).__name__, network_attempt, NETWORK_MAX_RETRIES,
                    NETWORK_RETRY_DELAY_S, channel_username, len(batch_msg_ids),
                )
                await asyncio.sleep(NETWORK_RETRY_DELAY_S)
                continue
            _LOG.warning(
                "translate batch network error retries exhausted (channel=@%s batch_size=%d): %s: %s",
                channel_username, len(batch_msg_ids), type(exc).__name__, exc,
            )
            return _all_network_error_results(
                channel_username, batch_msg_ids, original_texts,
                error_detail=f"{type(exc).__name__}: {exc}",
                latency_ms=elapsed_ms,
                attempts=attempt,
            )

        except errors.RPCError as exc:
            elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
            # Premium-gating detection — substring match on error string
            # is robust across Telethon version differences and exact
            # exception name variations. Pass F doctrine flagged this
            # as the load-bearing failure-mode-detection path; routes
            # to a distinct status so the orchestrator can flip to the
            # DeepL fallback without parsing error_detail strings.
            err_str = str(exc).upper()
            if "PREMIUM" in err_str:
                _LOG.critical(
                    "translate batch Premium-gated by Telegram (channel=@%s batch_size=%d): %s. "
                    "Pass F architecture flag must flip to DeepL fallback.",
                    channel_username, len(batch_msg_ids), exc,
                )
                return _all_premium_required_results(
                    channel_username, batch_msg_ids, original_texts,
                    error_detail=f"{type(exc).__name__}: {exc}",
                    latency_ms=elapsed_ms,
                    attempts=attempt,
                )
            # Message-id invalid: messages were deleted or edited beyond
            # recovery. Per-message status because Telegram returns
            # batch-level error in this case (we can't distinguish
            # which msg_id was the offender without per-message retry).
            if isinstance(exc, errors.MessageIdInvalidError) or "MESSAGE_ID_INVALID" in err_str:
                return _all_message_deleted_results(
                    channel_username, batch_msg_ids, original_texts,
                    error_detail=f"{type(exc).__name__}: {exc}",
                    latency_ms=elapsed_ms,
                    attempts=attempt,
                )
            # Generic RPC error — no retry, mark batch translation_error
            _LOG.warning(
                "translate batch RPCError (channel=@%s batch_size=%d): %s: %s",
                channel_username, len(batch_msg_ids), type(exc).__name__, exc,
            )
            return _all_translation_error_results(
                channel_username, batch_msg_ids, original_texts,
                error_detail=f"{type(exc).__name__}: {exc}",
                latency_ms=elapsed_ms,
                attempts=attempt,
            )

        # ---- success path ----
        elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
        per_message_latency_ms = max(1, elapsed_ms // max(1, len(batch_msg_ids)))
        translated_entries = getattr(response, "result", None) or []
        # Defensive: if response.result is shorter than the batch (Telegram
        # rare edge case), the remaining msg_ids get translation_error.
        results: list[TranslationResult] = []
        for i, mid in enumerate(batch_msg_ids):
            if i >= len(translated_entries):
                results.append(TranslationResult(
                    source_msg_id=str(mid),
                    channel_username=channel_username,
                    original_text=original_texts[mid],
                    translated_text=None,
                    status="translation_error",
                    error_detail=(
                        f"response.result length mismatch: expected "
                        f"{len(batch_msg_ids)}, got {len(translated_entries)}"
                    ),
                    latency_ms=per_message_latency_ms,
                    attempts=attempt,
                ))
                continue
            entry = translated_entries[i]
            translated_text = getattr(entry, "text", None)
            # Empty-string and None both signal "Telegram refused" —
            # Telegram returns empty for short messages, all-emoji,
            # copyrighted snippets per Telegram policy. We surface this
            # as status="ok" with translated_text=""; downstream
            # consumers (Commit 2) fall back to original headline.
            if translated_text is None:
                translated_text = ""
            results.append(TranslationResult(
                source_msg_id=str(mid),
                channel_username=channel_username,
                original_text=original_texts[mid],
                translated_text=translated_text,
                status="ok",
                error_detail=None,
                latency_ms=per_message_latency_ms,
                attempts=attempt,
            ))
        return results


# ---------- result-shape helpers ----------


def _per_message_latency(batch_size: int, total_ms: int) -> int:
    """Attribute batch latency to per-message latency_ms."""
    return max(1, total_ms // max(1, batch_size))


def _all_rate_limited_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
    latency_ms: int,
    attempts: int,
) -> list[TranslationResult]:
    per_msg = _per_message_latency(len(msg_ids), latency_ms)
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="rate_limited",
            error_detail=error_detail,
            latency_ms=per_msg,
            attempts=attempts,
        )
        for mid in msg_ids
    ]


def _all_inaccessible_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
) -> list[TranslationResult]:
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="channel_inaccessible",
            error_detail=error_detail,
            latency_ms=0,
            attempts=1,
        )
        for mid in msg_ids
    ]


def _all_message_deleted_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
    latency_ms: int,
    attempts: int,
) -> list[TranslationResult]:
    per_msg = _per_message_latency(len(msg_ids), latency_ms)
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="message_deleted",
            error_detail=error_detail,
            latency_ms=per_msg,
            attempts=attempts,
        )
        for mid in msg_ids
    ]


def _all_translation_error_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
    latency_ms: int,
    attempts: int,
) -> list[TranslationResult]:
    per_msg = _per_message_latency(len(msg_ids), latency_ms)
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="translation_error",
            error_detail=error_detail,
            latency_ms=per_msg,
            attempts=attempts,
        )
        for mid in msg_ids
    ]


def _all_premium_required_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
    latency_ms: int,
    attempts: int,
) -> list[TranslationResult]:
    per_msg = _per_message_latency(len(msg_ids), latency_ms)
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="premium_required",
            error_detail=error_detail,
            latency_ms=per_msg,
            attempts=attempts,
        )
        for mid in msg_ids
    ]


def _all_network_error_results(
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    *,
    error_detail: str,
    latency_ms: int,
    attempts: int,
) -> list[TranslationResult]:
    per_msg = _per_message_latency(len(msg_ids), latency_ms)
    return [
        TranslationResult(
            source_msg_id=str(mid),
            channel_username=channel_username,
            original_text=original_texts[mid],
            translated_text=None,
            status="network_error",
            error_detail=error_detail,
            latency_ms=per_msg,
            attempts=attempts,
        )
        for mid in msg_ids
    ]


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "MAX_FLOOD_WAIT_S",
    "MAX_RETRIES",
    "translate_telegram_messages",
]
