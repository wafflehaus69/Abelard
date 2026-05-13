"""SignalSink — the daemon's only external write surface.

Defense in depth (Pass C brief §7a):

  1. Destination-validation gate (`_assert_destination_allowed`) — a
     dedicated greppable function, called by every dispatch path
     before any subprocess invocation. Refuses anything other than
     the configured allowed value. Raises an internal exception that
     dispatch() catches and surfaces as DispatchResult(success=False).

  2. Paranoid grep test (`tests/test_alert_signal_sink_readonly.py`)
     asserts no hardcoded recipient identifiers (phone numbers,
     Signal group IDs) appear anywhere in this module's text. Any
     future addition of such a literal would have to deliberately
     update the grep test, surfacing the architectural change at
     review time.

Behavior:

  - Note-to-Self via signal-cli's `--note-to-self` flag. The flag's
    availability is detected via `signal-cli send --help` on first
    use and cached. If the installed signal-cli lacks the flag,
    dispatch fails loud with a CRITICAL log telling the operator to
    upgrade signal-cli or manually configure a self-only group per
    SETUP.md. No runtime fallback to group sending is implemented —
    the destination-validation gate's single-value comparison
    requires one canonical destination.

  - Subprocess failures (non-zero exit, timeout, signal-cli not
    found) return DispatchResult(success=False). Single retry with
    `RETRY_BACKOFF_S` delay on transient transport failure; no
    retry storm. Auth-key-style errors are surfaced verbatim in the
    error string; operator decides whether to regenerate.

  - This module never raises from dispatch(). Same fail-loud
    contract as Pass B's TelegramSource.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from typing import Optional

from ..synthesize.brief import Brief
from .sink import DispatchResult


CHANNEL_NAME = "signal"   # matches Brief.dispatch.channel literal exactly
ALLOWED_DESTINATION = "note_to_self"
RETRY_BACKOFF_S = 2.0
DEFAULT_HELP_TIMEOUT_S = 5.0

_LOG = logging.getLogger("news_watch_daemon.alert.signal")


class _DestinationMismatchError(RuntimeError):
    """Raised by `_assert_destination_allowed` when the configured
    destination doesn't match `ALLOWED_DESTINATION`. Internal to this
    module — dispatch() catches and surfaces via DispatchResult."""


def _assert_destination_allowed(configured: str) -> None:
    """The destination-validation gate. Sole gatekeeper for this sink.

    Architectural rule: every code path in this module that reaches
    a subprocess.run invoking signal-cli MUST flow through this
    function first. The paranoid grep test enforces by asserting no
    recipient-identifier value patterns appear anywhere else in this
    module's text.

    Refuses anything except the literal `ALLOWED_DESTINATION` value.
    Raises rather than returning a bool — the failure is exceptional
    and must be impossible to silently swallow at the call site.
    """
    if configured != ALLOWED_DESTINATION:
        raise _DestinationMismatchError(
            f"refused: configured destination {configured!r} "
            f"does not match allowed {ALLOWED_DESTINATION!r}"
        )


def _format_message_body(brief: Brief) -> str:
    """Render a brief into a Signal-friendly text body.

    Optimized for reading on a phone Signal client. Narrative first
    (the substance Mando reads); brief_id and themes appended as a
    short trailer for traceability.
    """
    lines = [brief.narrative.strip()]
    lines.append("")
    lines.append(f"[brief_id: {brief.brief_id}]")
    if brief.themes_covered:
        lines.append(f"[themes: {', '.join(brief.themes_covered)}]")
    return "\n".join(lines)


@dataclass
class SignalSink:
    """signal-cli subprocess wrapper. Only-write-surface of the daemon."""

    cli_path: str
    destination: str
    timeout_s: float
    _supports_note_to_self: Optional[bool] = field(default=None, repr=False)

    @property
    def channel_name(self) -> str:
        return CHANNEL_NAME

    def dispatch(self, brief: Brief) -> DispatchResult:
        """Never raises. Returns DispatchResult.channel == 'signal' on
        successful delivery, matching the Brief.dispatch.channel literal.
        """
        # 1. Destination validation gate — refuses any non-allowed value.
        try:
            _assert_destination_allowed(self.destination)
        except _DestinationMismatchError as exc:
            _LOG.critical("SignalSink destination validation refused: %s", exc)
            return DispatchResult(
                success=False,
                channel=CHANNEL_NAME,
                error=f"destination_mismatch: {exc}",
                dispatched_at_unix=int(time.time()),
            )

        # 2. Detect --note-to-self support (cached). If absent, refuse
        #    to send — operator must upgrade signal-cli or configure a
        #    self-only group manually per SETUP.md.
        if not self._detect_note_to_self_support():
            err = (
                "signal-cli on this host lacks --note-to-self; upgrade "
                "or configure a self-only group manually per SETUP.md"
            )
            _LOG.critical(err)
            return DispatchResult(
                success=False,
                channel=CHANNEL_NAME,
                error=err,
                dispatched_at_unix=int(time.time()),
            )

        # 3. Build argv and invoke. Single retry on transient failure.
        body = _format_message_body(brief)
        argv = [self.cli_path, "send", "--note-to-self", "-m", body]
        outcome = self._invoke(argv)
        if outcome.success:
            return outcome
        # Transient retry once.
        _LOG.warning(
            "SignalSink first attempt failed (%s); retrying after %.1fs",
            outcome.error, RETRY_BACKOFF_S,
        )
        time.sleep(RETRY_BACKOFF_S)
        return self._invoke(argv)

    def _invoke(self, argv: list[str]) -> DispatchResult:
        """Run signal-cli once. Never raises; outcome wrapped in DispatchResult."""
        try:
            result = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=self.timeout_s,
                check=False,
            )
        except FileNotFoundError:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"signal-cli not found at {self.cli_path!r}",
                dispatched_at_unix=int(time.time()),
            )
        except subprocess.TimeoutExpired:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"signal-cli timed out after {self.timeout_s}s",
                dispatched_at_unix=int(time.time()),
            )
        except OSError as exc:
            return DispatchResult(
                success=False, channel=CHANNEL_NAME,
                error=f"OSError invoking signal-cli: {exc}",
                dispatched_at_unix=int(time.time()),
            )

        now = int(time.time())
        if result.returncode == 0:
            return DispatchResult(
                success=True, channel=CHANNEL_NAME, dispatched_at_unix=now,
            )
        # Non-zero exit: surface stderr for diagnosis.
        stderr_tail = (result.stderr or "").strip()[-500:] or "(no stderr)"
        return DispatchResult(
            success=False, channel=CHANNEL_NAME,
            error=f"signal-cli exit={result.returncode}: {stderr_tail}",
            dispatched_at_unix=now,
        )

    def _detect_note_to_self_support(self) -> bool:
        """Run `signal-cli send --help` once; cache `--note-to-self` presence."""
        if self._supports_note_to_self is not None:
            return self._supports_note_to_self
        try:
            result = subprocess.run(
                [self.cli_path, "send", "--help"],
                capture_output=True, text=True,
                timeout=DEFAULT_HELP_TIMEOUT_S,
                check=False,
            )
            haystack = (result.stdout or "") + (result.stderr or "")
            self._supports_note_to_self = "--note-to-self" in haystack
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
            _LOG.warning("signal-cli --help probe failed: %s", exc)
            self._supports_note_to_self = False
        return self._supports_note_to_self


__all__ = [
    "ALLOWED_DESTINATION",
    "CHANNEL_NAME",
    "RETRY_BACKOFF_S",
    "SignalSink",
]
