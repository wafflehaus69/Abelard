"""Cross-channel translation pass — wraps Telethon client lifecycle.

Pass F Commit 2 (2026-05-28). Provides the synchronous bridge function
that the scrape orchestrator + CLI subcommands call to translate a
batch of pending messages across multiple Telegram channels in a
single connected-client session.

Why this layer exists: `translate_telegram_messages()` from
`telegram_native.py` takes an already-connected TelegramClient and a
single channel's msg_ids. This runner owns the client lifecycle
(connect → translate per-channel → disconnect) and provides a single
`asyncio.run()` entry point for sync callers (orchestrator,
`db backfill-translation` CLI).

Architectural contract — RE-QUEUE SEMANTICS (cross-referencing
translate_telegram_messages() docstring):

  Translation failures (rate_limited, network_error, message_deleted,
  channel_inaccessible, premium_required, translation_error) return
  TranslationResult with the corresponding status and translated_text=None.
  The orchestrator inserts these rows with headline_en=NULL, and they
  remain in the translation-pending queue (WHERE language != 'en' AND
  headline_en IS NULL). The next scrape cycle's translation pass —
  OR the `db backfill-translation` CLI subcommand — naturally retries
  them. There is no special re-queue logic at this layer.

  The orchestrator CONTINUES translating remaining batches and
  CONTINUES inserting remaining rows after any individual failure.
  Translation outages do NOT block headline insertion; they degrade
  to the COALESCE(headline_en, headline) fallback for downstream
  consumers (theme tagger, Pass E ATTENTION counter).

  See translation/telegram_native.py's translate_telegram_messages()
  docstring for the per-message failure-mode contract.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

from .deepl_stub import translate_deepl
from .telegram_native import translate_telegram_messages
from .types import TranslationResult


_LOG = logging.getLogger("news_watch_daemon.translation.runner")


async def _translate_all_channels_native(
    *,
    api_id: int,
    api_hash: str,
    session_string: str,
    pending_by_channel: dict[str, list[tuple[int, str]]],
    batch_size: int,
    to_lang: str,
) -> dict[tuple[str, int], TranslationResult]:
    """Internal async core: connect Telethon client once, translate all
    pending msg_ids per channel, return mapping (channel, msg_id) -> result.

    On client-level failures (connection error, session invalidated),
    all pending msg_ids across all channels get a network_error /
    translation_error status — the orchestrator handles uniformly.
    """
    output: dict[tuple[str, int], TranslationResult] = {}
    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,
    )
    try:
        try:
            await client.connect()
        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            _LOG.warning(
                "translation pass connection error %s: %s",
                type(exc).__name__, exc,
            )
            return _all_pending_as_status(
                pending_by_channel,
                status="network_error",
                error_detail=f"{type(exc).__name__}: {exc}",
            )

        try:
            authorized = await client.is_user_authorized()
        except errors.RPCError as exc:
            _LOG.critical(
                "translation pass session invalidated (%s); cannot translate. "
                "Regenerate via `python -m news_watch_daemon.telegram_setup`.",
                type(exc).__name__,
            )
            return _all_pending_as_status(
                pending_by_channel,
                status="translation_error",
                error_detail=f"session invalidated: {type(exc).__name__}",
            )
        if not authorized:
            _LOG.critical(
                "translation pass not authorized; cannot translate. "
                "Regenerate via `python -m news_watch_daemon.telegram_setup`.",
            )
            return _all_pending_as_status(
                pending_by_channel,
                status="translation_error",
                error_detail="session invalidated: not authorized",
            )

        for channel, items in pending_by_channel.items():
            msg_ids = [mid for mid, _text in items]
            originals = {mid: text for mid, text in items}
            results = await translate_telegram_messages(
                client,
                channel_username=channel,
                msg_ids=msg_ids,
                original_texts=originals,
                to_lang=to_lang,
                batch_size=batch_size,
            )
            for r in results:
                output[(channel, int(r.source_msg_id))] = r

    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001 — disconnect failures non-fatal
            pass

    return output


def _all_pending_as_status(
    pending_by_channel: dict[str, list[tuple[int, str]]],
    *,
    status: str,
    error_detail: str,
) -> dict[tuple[str, int], TranslationResult]:
    """Pre-emptive failure path: build one TranslationResult per pending
    msg_id when client-level setup failed (connection, auth)."""
    out: dict[tuple[str, int], TranslationResult] = {}
    for channel, items in pending_by_channel.items():
        for msg_id, original in items:
            out[(channel, msg_id)] = TranslationResult(
                source_msg_id=str(msg_id),
                channel_username=channel,
                original_text=original,
                translated_text=None,
                status=status,  # type: ignore[arg-type]
                error_detail=error_detail,
                latency_ms=0,
                attempts=1,
            )
    return out


def run_translation_pass(
    *,
    api_id: int,
    api_hash: str,
    session_string: str,
    pending_by_channel: dict[str, list[tuple[int, str]]],
    batch_size: int = 10,
    to_lang: str = "en",
    translation_source: str = "telegram_native",
) -> dict[tuple[str, int], TranslationResult]:
    """Synchronous bridge — runs the async translation pass via asyncio.run.

    Args:
        api_id, api_hash, session_string: Telegram credentials. Caller is
            responsible for verifying these are configured before calling.
        pending_by_channel: mapping channel_username -> list of
            (msg_id, original_text) tuples to translate.
        batch_size: per-call batch size. Loaded from config/translation.yaml
            by caller.
        to_lang: ISO 639-1 target language code (default "en").
        translation_source: "telegram_native" or "deepl". When "deepl",
            calls the stub which raises NotImplementedError immediately
            (intentional — Pass F doctrine: DeepL is a future flip, not
            a tonight-ready path).

    Returns:
        Mapping (channel_username, msg_id) -> TranslationResult.
        1:1 with input msg_ids across all channels.

    Raises:
        NotImplementedError: when translation_source == "deepl" (the stub
            path). Caller (orchestrator, CLI handler) should catch and
            log, then proceed with NULL headline_en for all pending rows.
        ValueError: malformed pending_by_channel shape.
    """
    if not isinstance(pending_by_channel, dict):
        raise ValueError(
            f"pending_by_channel must be dict; got {type(pending_by_channel).__name__}"
        )
    if not pending_by_channel:
        return {}

    if translation_source == "deepl":
        # Stub path — raises NotImplementedError. Caller's responsibility.
        # We call through to translate_deepl with one channel's items to
        # surface the NotImplementedError with the stub's full message.
        first_channel = next(iter(pending_by_channel))
        first_items = pending_by_channel[first_channel]
        msg_ids = [m for m, _ in first_items]
        originals = {m: t for m, t in first_items}
        asyncio.run(translate_deepl(
            channel_username=first_channel,
            msg_ids=msg_ids,
            original_texts=originals,
            to_lang=to_lang,
            batch_size=batch_size,
        ))
        # Unreachable — translate_deepl always raises
        raise NotImplementedError("deepl path unreachable")

    if translation_source != "telegram_native":
        raise ValueError(
            f"translation_source must be 'telegram_native' or 'deepl'; "
            f"got {translation_source!r}"
        )

    return asyncio.run(_translate_all_channels_native(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        pending_by_channel=pending_by_channel,
        batch_size=batch_size,
        to_lang=to_lang,
    ))


__all__ = ["run_translation_pass"]
