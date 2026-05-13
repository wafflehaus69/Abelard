"""TelegramBotSink — Bot API alert transport (documented fallback to Signal).

Architectural symmetry with SignalSink (Step 6):

  - Configured destination (bot_token + chat_id) is resolved from
    env vars at sink construction, never hardcoded in the module.
  - The paranoid grep test (test_alert_telegram_bot_sink_readonly.py)
    enforces by asserting no Bot API token literals or chat ID
    literals appear in this module's source text. Same "architecture
    is the safeguard" discipline as SignalSink.
  - Constructor validates that both bot_token and chat_id are
    non-empty — fail-loud on misconfiguration before dispatch is
    ever called.

Distinct from Pass B's TelegramSource (inbound MTProto via Telethon):
this is OUTBOUND only, via the Bot API. Different API surface,
different namespace, different credentials. Inbound and outbound do
NOT share session state or auth.

dispatch() never raises. HTTP / API / parse failures surface as
DispatchResult(success=False, error="..."). Single retry with
RETRY_BACKOFF_S delay on transient transport failure; no retry storm.

Transport: stdlib `urllib.request` POST to Bot API endpoint. No
external HTTP dep (kept consistent with the daemon's Pass A discipline
of urllib-only transport).
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional

from ..synthesize.brief import Brief
from .sink import DispatchResult


CHANNEL_NAME = "telegram_bot"   # matches Brief.dispatch.channel literal exactly
RETRY_BACKOFF_S = 2.0
DEFAULT_API_BASE = "https://api.telegram.org"
MAX_MESSAGE_CHARS = 4000  # Bot API hard limit is 4096; reserve room for trailer

_LOG = logging.getLogger("news_watch_daemon.alert.telegram_bot")


class _MisconfigurationError(RuntimeError):
    """Internal: raised by the construction-time validation gate when
    bot_token or chat_id is missing/empty. Caught at the construction
    call site (factory) — never surfaces from dispatch()."""


def _assert_credentials_present(bot_token: str, chat_id: str) -> None:
    """Construction-time validation gate. Mirror of SignalSink's
    `_assert_destination_allowed` — its own greppable function so the
    architectural rule (no dispatch without verified credentials) is
    visible in code search.

    Refuses empty or whitespace-only values. The factory catches and
    declines to wire the sink rather than surfacing at dispatch time.
    """
    if not isinstance(bot_token, str) or not bot_token.strip():
        raise _MisconfigurationError(
            "TelegramBotSink: bot_token is empty; resolve from env var first"
        )
    if not isinstance(chat_id, str) or not chat_id.strip():
        raise _MisconfigurationError(
            "TelegramBotSink: chat_id is empty; resolve from env var first"
        )


def _format_message_body(brief: Brief) -> str:
    """Render a Brief into a Bot API message. 4000-char cap, trailer
    with brief_id + themes for traceability. Same shape as SignalSink's
    formatter so the same Brief produces equivalent messages on either
    channel — operator can switch sinks without re-learning the format."""
    narrative = brief.narrative.strip()
    trailer_lines = [f"[brief_id: {brief.brief_id}]"]
    if brief.themes_covered:
        trailer_lines.append(f"[themes: {', '.join(brief.themes_covered)}]")
    trailer = "\n\n" + "\n".join(trailer_lines)

    # Reserve room for the trailer; truncate narrative if needed.
    max_narrative = MAX_MESSAGE_CHARS - len(trailer)
    if len(narrative) > max_narrative:
        narrative = narrative[:max_narrative - len("\n[truncated]")] + "\n[truncated]"
    return narrative + trailer


@dataclass
class TelegramBotSink:
    """Bot API alert transport. Fail-loud, no retry storm, sink-symmetric."""

    bot_token: str
    chat_id: str
    timeout_s: float
    api_base: str = DEFAULT_API_BASE

    def __post_init__(self) -> None:
        # Construction-time gate — fail loud here so misconfigured
        # sinks never reach dispatch().
        _assert_credentials_present(self.bot_token, self.chat_id)

    @property
    def channel_name(self) -> str:
        return CHANNEL_NAME

    def dispatch(self, brief: Brief) -> DispatchResult:
        """Never raises. Returns DispatchResult.channel == 'telegram_bot'
        on successful delivery, matching the Brief.dispatch.channel literal.
        """
        body = _format_message_body(brief)
        outcome = self._invoke(body)
        if outcome.success:
            return outcome
        _LOG.warning(
            "TelegramBotSink first attempt failed (%s); retrying after %.1fs",
            outcome.error, RETRY_BACKOFF_S,
        )
        time.sleep(RETRY_BACKOFF_S)
        return self._invoke(body)

    def _invoke(self, body: str) -> DispatchResult:
        """One HTTPS POST to /bot{token}/sendMessage. Never raises."""
        url = f"{self.api_base}/bot{self.bot_token}/sendMessage"
        form = urllib.parse.urlencode({
            "chat_id": self.chat_id,
            "text": body,
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(
            url, data=form, method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        now = int(time.time())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            try:
                tail = exc.read().decode("utf-8", errors="replace")[-300:]
            except Exception:  # noqa: BLE001 — diagnostic read; never propagate
                tail = ""
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"http_{exc.code}: {tail or exc.reason}",
                dispatched_at_unix=now,
            )
        except urllib.error.URLError as exc:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"URLError: {exc.reason}",
                dispatched_at_unix=now,
            )
        except (TimeoutError, OSError) as exc:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"{type(exc).__name__}: {exc}",
                dispatched_at_unix=now,
            )

        # 200 response — parse the JSON and check `ok` field.
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"telegram_api: malformed JSON: {exc}",
                dispatched_at_unix=now,
            )
        if parsed.get("ok") is True:
            return DispatchResult(
                success=True, channel=CHANNEL_NAME, dispatched_at_unix=now,
            )
        # API returned ok=False — surface the description if present.
        description = parsed.get("description", "unknown error")
        return DispatchResult(
            success=False, channel=CHANNEL_NAME,
            error=f"telegram_api: {description}",
            dispatched_at_unix=now,
        )


__all__ = [
    "CHANNEL_NAME",
    "RETRY_BACKOFF_S",
    "TelegramBotSink",
]
