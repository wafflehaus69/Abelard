"""Telegram (MTProto) source plugin — read-only by construction.

Architectural constraint: this module is restricted to a narrow Telethon
surface (TelegramClient, errors, sessions.StringSession). Any write
operation against the Telegram API is architecturally prohibited and
verified by a paranoid grep test in
`tests/test_sources_telegram_readonly.py`. If future work needs write
capability, that test will fail in code review — surfacing the
architectural change rather than letting it slip through.

Auth-failure policy: the burner account this daemon runs against is
not recoverable if its session is invalidated. The plugin therefore
never attempts re-authentication. Any auth-related exception class
returns `FetchResult.status="error"` with a CRITICAL-level log message
making the regeneration requirement loud and unambiguous. The operator
must regenerate the session via the one-time
`python -m news_watch_daemon.telegram_setup` flow.

Message-to-headline mapping: Telegram messages don't carry a natural
"headline" field. The plugin stores the message text verbatim
(stripped of surrounding whitespace), capped at 4096 chars (Telegram's
max single-message length). Messages with no text (photo-only,
sticker-only, etc.) are dropped.

Design note: earlier versions kept only the first newline-delimited
line. That behavior was dropped after the Fix-1 observability
instrumentation surfaced empirical evidence (2026-05-26 scrape, 23
hits on CIG_telegram in a single sweep, max severity /76087 dropping
4143 chars of analytical body to preserve an 84-char emoji-prefix
title) that channels like @CIG_telegram and @real_DonaldJTrump are
performing upstream synthesis work — their posts are analytical
briefs structured as `[emoji header]\\n\\n[multi-paragraph analysis]`.
The daemon's job is to preserve those briefs intact for downstream
synthesis consumption, not extract titles from them. The "headline"
column name is retained for schema continuity; downstream code
consumes the column's content unchanged.

`max_messages_per_fetch` is a safety cap of 200 per cycle by default
(brief Artifact 2 hypothesis #4). For the channels currently tracked
this is far above expected volume; if real usage ever hits the cap
the limit can be raised in the theme config.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Any

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

from .base import FetchedItem, FetchResult, SourcePlugin
from .noise_filter_log import write_filter_entry


_LOG = logging.getLogger("news_watch_daemon.sources.telegram")

_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$")
_API_HASH_RE = re.compile(r"^[0-9a-f]{32}$")

_HEADLINE_MAX_CHARS = 4096
_MAX_MAX_MESSAGES_PER_FETCH = 1000

PLUGIN_PREFIX = "telegram:"


def _extract_headline(text: str | None) -> str | None:
    """Return the message text verbatim (stripped, capped at 4096 chars).

    Returns None if `text` is None, empty, or whitespace-only after
    stripping. Truncation has no ellipsis appended (clean cut on
    character 4096 — Telegram's max single-message length, so any
    single MTProto message round-trips intact).

    Multi-paragraph posts are preserved in full. The earlier first-line
    restriction was dropped after empirical evidence showed it was
    silently discarding analytical body content from channels that
    perform upstream synthesis (see module docstring).
    """
    if text is None:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    return stripped[:_HEADLINE_MAX_CHARS]


class TelegramSource(SourcePlugin):
    """Pulls public-channel messages via MTProto. Read-only by construction."""

    def __init__(
        self,
        *,
        channel_username: str,
        api_id: int,
        api_hash: str,
        session_string: str,
        cadence_minutes: int = 15,
        max_messages_per_fetch: int = 200,
        noise_filter: list[str] | None = None,
        filtered_log_path: Path | None = None,
    ) -> None:
        if not isinstance(channel_username, str) or not _USERNAME_RE.match(channel_username):
            raise ValueError(
                "channel_username must match Telegram's 5–32 char username "
                f"constraint; got {channel_username!r}"
            )
        if not isinstance(api_id, int) or isinstance(api_id, bool) or api_id <= 0:
            raise ValueError(f"api_id must be a positive int; got {api_id!r}")
        if not isinstance(api_hash, str) or not _API_HASH_RE.match(api_hash):
            raise ValueError(
                f"api_hash must be 32 lowercase hex chars; got length {len(api_hash) if isinstance(api_hash, str) else 'non-string'}"
            )
        if not isinstance(session_string, str) or not session_string.strip():
            raise ValueError("session_string must be a non-empty string")
        if not isinstance(cadence_minutes, int) or isinstance(cadence_minutes, bool) or cadence_minutes <= 0:
            raise ValueError(f"cadence_minutes must be a positive int; got {cadence_minutes!r}")
        if (
            not isinstance(max_messages_per_fetch, int)
            or isinstance(max_messages_per_fetch, bool)
            or max_messages_per_fetch <= 0
            or max_messages_per_fetch > _MAX_MAX_MESSAGES_PER_FETCH
        ):
            raise ValueError(
                f"max_messages_per_fetch must be a positive int ≤ {_MAX_MAX_MESSAGES_PER_FETCH}; "
                f"got {max_messages_per_fetch!r}"
            )
        if noise_filter is not None:
            if not isinstance(noise_filter, list):
                raise ValueError(
                    f"noise_filter must be a list of strings or None; got {type(noise_filter).__name__}"
                )
            for pat in noise_filter:
                if not isinstance(pat, str) or not pat.strip():
                    raise ValueError(
                        f"noise_filter entries must be non-empty strings; got {pat!r}"
                    )

        # Eagerly validate session string format (hypothesis #2: fail loud at startup,
        # not 15 minutes later in the middle of a sweep).
        try:
            StringSession(session_string)
        except Exception as exc:  # noqa: BLE001 — decode errors vary by Telethon version
            raise ValueError(f"session_string is malformed: {type(exc).__name__}: {exc}") from exc

        self._channel_username = channel_username
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_string = session_string
        self._cadence_minutes = cadence_minutes
        self._max_messages_per_fetch = max_messages_per_fetch
        # Store noise_filter as a list of (original, lowered) tuples so the
        # per-message match path doesn't redo .lower() on each pattern every
        # call. Original is preserved for INFO log + audit-trail clarity.
        self._noise_filter: list[tuple[str, str]] = (
            [(p, p.lower()) for p in noise_filter] if noise_filter else []
        )
        self._filtered_log_path = filtered_log_path

    @property
    def name(self) -> str:
        return f"{PLUGIN_PREFIX}{self._channel_username}"

    @property
    def cadence_minutes(self) -> int | None:
        return self._cadence_minutes

    @property
    def channel_username(self) -> str:
        return self._channel_username

    def rate_limit_budget_remaining(self) -> float:
        # Telethon manages its own internal limits and raises FloodWaitError
        # when hit. We don't track call counts here; orchestrator backoff
        # is driven by source_health.consecutive_failure_count.
        return 1.0

    # ---- fetch entry point ----

    def fetch(self, since_unix: int) -> FetchResult:
        """Synchronous wrapper around the async fetch. Never raises.

        The async path is wrapped in `asyncio.run()`. The catch-all
        below is the load-bearing piece for the "must not raise"
        contract — even if Telethon throws something we didn't
        anticipate, we surface it as status="error" rather than
        propagating up to the orchestrator.
        """
        fetched_at = int(time.time())
        try:
            return asyncio.run(self._async_fetch(since_unix, fetched_at))
        except Exception as exc:  # noqa: BLE001 — never-raise contract
            return self._error_result(
                fetched_at,
                f"unexpected: {type(exc).__name__}: {exc}",
            )

    async def _async_fetch(self, since_unix: int, fetched_at: int) -> FetchResult:
        client = self._build_client()
        try:
            try:
                await client.connect()
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                return self._error_result(
                    fetched_at,
                    f"connection_error: {type(exc).__name__}: {exc}",
                )

            try:
                authorized = await client.is_user_authorized()
            except errors.AuthKeyUnregisteredError as exc:
                self._log_session_invalidated(exc)
                return self._error_result(
                    fetched_at, f"session invalidated: {type(exc).__name__}"
                )
            except errors.AuthKeyDuplicatedError as exc:
                self._log_session_invalidated(exc)
                return self._error_result(
                    fetched_at, f"session invalidated: {type(exc).__name__}"
                )
            except errors.SessionRevokedError as exc:
                self._log_session_invalidated(exc)
                return self._error_result(
                    fetched_at,
                    "session revoked: regenerate via setup script",
                )
            except errors.SessionPasswordNeededError as exc:
                self._log_session_invalidated(exc)
                return self._error_result(
                    fetched_at, "2FA required: session needs regeneration"
                )
            except errors.UserDeactivatedError as exc:
                self._log_session_invalidated(exc)
                return self._error_result(
                    fetched_at, f"session invalidated: {type(exc).__name__}"
                )

            if not authorized:
                self._log_session_invalidated(None)
                return self._error_result(
                    fetched_at, "session invalidated: not authorized"
                )

            try:
                entity = await client.get_entity(f"@{self._channel_username}")
            except errors.ChannelPrivateError:
                return self._error_result(
                    fetched_at, "channel is private or has been deleted"
                )
            except errors.UsernameNotOccupiedError:
                return self._error_result(
                    fetched_at, f"channel @{self._channel_username} not found"
                )
            except errors.UsernameInvalidError:
                return self._error_result(
                    fetched_at, f"channel @{self._channel_username} not found"
                )
            except errors.ChatAdminRequiredError as exc:
                _LOG.warning(
                    "channel %s unexpectedly requires admin rights: %s",
                    self._channel_username, exc,
                )
                return self._error_result(
                    fetched_at, "channel requires admin rights"
                )
            except errors.FloodWaitError as exc:
                return self._rate_limited_result(fetched_at, exc)
            except errors.RPCError as exc:
                return self._error_result(
                    fetched_at, f"rpc_error: {type(exc).__name__}: {exc}"
                )

            items: list[FetchedItem] = []
            try:
                async for message in client.iter_messages(
                    entity, limit=self._max_messages_per_fetch
                ):
                    msg_unix = self._message_unix(message)
                    if msg_unix is None:
                        continue
                    if msg_unix <= since_unix:
                        # iter_messages is newest-first; once we hit the
                        # cutoff, everything after is older — bail early.
                        break
                    item = self._message_to_item(message, msg_unix)
                    if item is not None:
                        items.append(item)
            except errors.FloodWaitError as exc:
                return self._rate_limited_result(fetched_at, exc)
            except errors.RPCError as exc:
                return self._error_result(
                    fetched_at, f"rpc_error: {type(exc).__name__}: {exc}"
                )

            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=items,
                status="ok",
            )
        finally:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001 — disconnect failures are non-fatal
                pass

    # ---- helpers ----

    def _build_client(self) -> TelegramClient:
        return TelegramClient(
            StringSession(self._session_string),
            self._api_id,
            self._api_hash,
        )

    def _message_to_item(self, message: Any, msg_unix: int) -> FetchedItem | None:
        text = getattr(message, "text", None)
        headline = _extract_headline(text)
        if headline is None:
            return None
        msg_id = getattr(message, "id", None)
        if msg_id is None:
            return None
        # Sponsor-noise filter (Task 1, 2026-05-27). Per-channel literal
        # substring filter — drops sponsor / promo / affiliate messages
        # before they reach the headlines table. Match is against the
        # extracted headline (post _extract_headline) — currently the
        # full verbatim message text capped at 4096 chars, so the six
        # current patterns (all short, all appear early in sponsor
        # posts) match reliably. A future body-only pattern proposal
        # confusingly missing would point back to this comment.
        matched = self._match_noise_pattern(headline)
        if matched is not None:
            self._on_filtered(headline=headline, msg_id=msg_id, pattern=matched)
            return None
        return FetchedItem(
            source_item_id=str(msg_id),
            headline=headline,
            url=f"https://t.me/{self._channel_username}/{msg_id}",
            published_at_unix=msg_unix,
            raw_source=None,
            tickers=[],
            raw_body=None,
        )

    def _match_noise_pattern(self, headline: str) -> str | None:
        """First noise_filter pattern that matches the headline (case-
        insensitive substring) wins. Returns the original pattern string,
        not the lowered comparison form, for audit clarity.

        Match is performed against the headline text already extracted by
        `_extract_headline` (i.e. stripped, truncated to 4096 chars) —
        NOT against the raw Telethon Message body. For Ateobreaking's
        current six patterns this is fine: every sponsor marker appears
        well within the first paragraph. If a future pattern targets
        content past the 4096-char cap, this comment is the breadcrumb.
        """
        if not self._noise_filter:
            return None
        lowered_headline = headline.lower()
        for original, lowered in self._noise_filter:
            if lowered in lowered_headline:
                return original
        return None

    def _on_filtered(self, *, headline: str, msg_id: int, pattern: str) -> None:
        """Handle a filter hit: INFO log + best-effort audit-log append.

        INFO log: surfaces to the operator console / log aggregation so
        per-cycle filter activity is visible without parsing JSONL.

        Audit JSONL: forensic trail — six months from now, "did the
        filter eat post X?" must be answerable. The audit append is
        best-effort; disk-full / permission errors WARN but never
        abort the scrape (matches `_log_cross_source` discipline).
        """
        _LOG.info(
            "sponsor-noise filtered: channel=@%s msg_id=%s pattern=%r",
            self._channel_username, msg_id, pattern,
        )
        if self._filtered_log_path is None:
            return
        try:
            write_filter_entry(
                self._filtered_log_path,
                channel=self._channel_username,
                msg_id=str(msg_id),
                matched_pattern=pattern,
                full_text=headline,
            )
        except OSError as exc:
            _LOG.warning(
                "filtered audit log append failed (channel=@%s msg_id=%s): %s",
                self._channel_username, msg_id, exc,
            )

    @staticmethod
    def _message_unix(message: Any) -> int | None:
        date = getattr(message, "date", None)
        if date is None:
            return None
        try:
            return int(date.timestamp())
        except Exception:  # noqa: BLE001 — odd date types just skip
            return None

    def _error_result(self, fetched_at: int, detail: str) -> FetchResult:
        return FetchResult(
            source=self.name,
            fetched_at_unix=fetched_at,
            items=[],
            status="error",
            error_detail=detail,
        )

    def _rate_limited_result(
        self, fetched_at: int, exc: errors.FloodWaitError
    ) -> FetchResult:
        seconds = getattr(exc, "seconds", None)
        detail = f"flood_wait_seconds={seconds}" if seconds is not None else "flood_wait"
        return FetchResult(
            source=self.name,
            fetched_at_unix=fetched_at,
            items=[],
            status="rate_limited",
            error_detail=detail,
        )

    def _log_session_invalidated(self, exc: BaseException | None) -> None:
        # CRITICAL-level so the human operator notices via stderr / log
        # aggregation. The session-string is sensitive and must NEVER
        # be included in this log line — the redacting filter would catch
        # it but defense in depth says don't put it there in the first place.
        _LOG.critical(
            "Telegram session invalidated for channel @%s (%s). "
            "Regenerate via `python -m news_watch_daemon.telegram_setup`.",
            self._channel_username,
            type(exc).__name__ if exc is not None else "is_user_authorized=False",
        )


__all__ = ["PLUGIN_PREFIX", "TelegramSource"]
