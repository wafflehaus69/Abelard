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
"headline" field. The plugin uses the message text up to the first
newline, truncated to 280 chars, as the headline. Messages with no
text (photo-only, sticker-only, etc.) are dropped.

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
from typing import Any

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

from .base import FetchedItem, FetchResult, SourcePlugin


_LOG = logging.getLogger("news_watch_daemon.sources.telegram")

_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$")
_API_HASH_RE = re.compile(r"^[0-9a-f]{32}$")

_HEADLINE_MAX_CHARS = 280
_MAX_MAX_MESSAGES_PER_FETCH = 1000

PLUGIN_PREFIX = "telegram:"


def _extract_headline(text: str | None) -> str | None:
    """Return the first non-empty line of `text`, truncated to 280 chars.

    Returns None if `text` is None, empty, or whitespace-only after
    stripping. Truncation has no ellipsis appended (clean cut on
    character 280).
    """
    if text is None:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    first_line = stripped.split("\n", 1)[0].strip()
    if not first_line:
        return None
    return first_line[:_HEADLINE_MAX_CHARS]


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
        return FetchedItem(
            source_item_id=str(msg_id),
            headline=headline,
            url=f"https://t.me/{self._channel_username}/{msg_id}",
            published_at_unix=msg_unix,
            raw_source=None,
            tickers=[],
            raw_body=None,
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
