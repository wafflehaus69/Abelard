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
    use and cached. Detection distinguishes a missing binary (install
    signal-cli) from a present-but-old binary (upgrade signal-cli) and
    caches the specific reason; if support can't be confirmed, dispatch
    fails loud with a CRITICAL log carrying that reason. No runtime
    fallback to group sending is implemented — the destination-
    validation gate's single-value comparison requires one canonical
    destination.

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

from ..attention.brief_schema import AttentionBrief
from ..synthesize.brief import Brief
from .sink import DispatchableBrief, DispatchResult


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
    """Render a Pass C Brief into a Signal-friendly text body.

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


def _format_attention_message_body(brief: AttentionBrief) -> str:
    """Render a Pass E AttentionBrief into a Signal-friendly text body.

    Prefixed with [ATTENTION] so Mando can visually distinguish ATTENTION
    briefs from theme-event briefs in his Signal inbox. Header line
    surfaces the triggering term, frequency stats, and shape. Narrative
    follows; source mix and observed entities trail for traceability.
    """
    header = (
        f"[ATTENTION] {brief.triggering_term}  "
        f"(window: {brief.term_frequency_window}, "
        f"prior: {brief.term_frequency_prior}, "
        f"shape: {brief.attention_shape})"
    )
    lines = [header, "", brief.narrative.strip()]
    if brief.source_mix:
        # Compact "source(N)" form, sorted by count desc for readability.
        items = sorted(brief.source_mix.items(), key=lambda kv: (-kv[1], kv[0]))
        rendered = ", ".join(f"{src}({n})" for src, n in items)
        lines.append("")
        lines.append(f"sources: {rendered}")
    if brief.entities_observed:
        lines.append(f"entities: {', '.join(brief.entities_observed)}")
    lines.append("")
    lines.append(f"[brief_id: {brief.brief_id}]")
    return "\n".join(lines)


@dataclass
class SignalSink:
    """signal-cli subprocess wrapper. Only-write-surface of the daemon."""

    cli_path: str
    destination: str
    timeout_s: float
    _supports_note_to_self: Optional[bool] = field(default=None, repr=False)
    # Specific reason support could not be confirmed (binary-missing vs
    # flag-missing vs probe-error), cached alongside the bool so dispatch()
    # can surface an accurate message instead of a one-size-fits-all string.
    _note_to_self_error: Optional[str] = field(default=None, repr=False)

    @property
    def channel_name(self) -> str:
        return CHANNEL_NAME

    def dispatch(self, brief: DispatchableBrief) -> DispatchResult:
        """Never raises. Returns DispatchResult.channel == 'signal' on
        successful delivery, matching Brief.dispatch.channel literal.

        Accepts either Pass C Brief or Pass E AttentionBrief; formatter
        branches on type, transport (signal-cli subprocess) is shared.
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
            err = self._note_to_self_error or (
                "signal-cli lacks --note-to-self support; upgrade signal-cli "
                "or configure a self-only destination manually"
            )
            _LOG.critical(err)
            return DispatchResult(
                success=False,
                channel=CHANNEL_NAME,
                error=err,
                dispatched_at_unix=int(time.time()),
            )

        # 3. Build argv and invoke. Single retry on transient failure.
        if isinstance(brief, AttentionBrief):
            body = _format_attention_message_body(brief)
        else:
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
        """Run `signal-cli send --help` once; cache `--note-to-self` presence.

        Distinguishes the failure modes so dispatch() can report an accurate
        reason — a missing binary is NOT a missing flag, and conflating them
        (the pre-2026-07-10 behavior) sent the operator chasing the wrong
        problem and a nonexistent SETUP.md:
          - binary absent (FileNotFoundError) -> install signal-cli
          - binary present but flag absent    -> upgrade signal-cli
          - probe error (timeout/OSError)     -> transient/host issue
        The specific reason is cached in `_note_to_self_error`.
        """
        if self._supports_note_to_self is not None:
            return self._supports_note_to_self
        try:
            result = subprocess.run(
                [self.cli_path, "send", "--help"],
                capture_output=True, text=True,
                timeout=DEFAULT_HELP_TIMEOUT_S,
                check=False,
            )
        except FileNotFoundError:
            self._note_to_self_error = (
                f"signal-cli not found at {self.cli_path!r}: install signal-cli "
                "(and its Java runtime) and register a Signal account, then set "
                "alert.signal.cli_path. This is NOT a --note-to-self flag issue."
            )
            _LOG.warning(self._note_to_self_error)
            self._supports_note_to_self = False
            return False
        except (subprocess.TimeoutExpired, OSError) as exc:
            self._note_to_self_error = (
                f"signal-cli --help probe failed ({exc}); cannot confirm "
                "--note-to-self support on this host"
            )
            _LOG.warning(self._note_to_self_error)
            self._supports_note_to_self = False
            return False

        haystack = (result.stdout or "") + (result.stderr or "")
        if "--note-to-self" in haystack:
            self._supports_note_to_self = True
            self._note_to_self_error = None
        else:
            self._supports_note_to_self = False
            self._note_to_self_error = (
                f"signal-cli at {self.cli_path!r} is installed but its `send` "
                "command lacks --note-to-self; upgrade signal-cli to a version "
                "that supports it."
            )
            _LOG.warning(self._note_to_self_error)
        return self._supports_note_to_self


__all__ = [
    "ALLOWED_DESTINATION",
    "CHANNEL_NAME",
    "RETRY_BACKOFF_S",
    "SignalSink",
]
