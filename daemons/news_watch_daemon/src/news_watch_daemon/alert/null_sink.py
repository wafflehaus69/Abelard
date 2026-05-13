"""NullSink — test-only AlertSink that records dispatches in memory.

Hermetic-test workhorse. Lets test code assert "would have alerted" or
"called dispatch with brief X" without touching SignalCLI subprocesses
or Telegram Bot API endpoints.

Conforms to the `AlertSink` Protocol structurally — no inheritance is
required because the protocol is `@runtime_checkable`.

NOT a production sink. Brief.dispatch.channel is constrained to
Literal["signal", "telegram_bot"] in the schema; NullSink declares
channel="null" which would fail that validation if persisted. Tests
that exercise the orchestrator's brief-archiving path use a real
sink-shaped fixture rather than NullSink for the channel field.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from ..synthesize.brief import Brief
from .sink import DispatchResult


CHANNEL_NAME = "null"


@dataclass
class NullSink:
    """In-memory AlertSink for hermetic tests.

    Attributes:
      dispatched:   list of every Brief passed to dispatch() so far.
      fail_next:    when True, the next dispatch returns success=False.
                    Auto-resets to False after firing once — one-shot
                    failure injection per test case.
      fail_error:   the error string returned when fail_next fires.
    """

    fail_next: bool = False
    fail_error: str = "simulated failure"
    dispatched: list[Brief] = field(default_factory=list)

    @property
    def channel_name(self) -> str:
        return CHANNEL_NAME

    def dispatch(self, brief: Brief) -> DispatchResult:
        if self.fail_next:
            self.fail_next = False  # one-shot
            return DispatchResult(
                success=False,
                channel=CHANNEL_NAME,
                error=self.fail_error,
                dispatched_at_unix=int(time.time()),
            )
        self.dispatched.append(brief)
        return DispatchResult(
            success=True,
            channel=CHANNEL_NAME,
            dispatched_at_unix=int(time.time()),
        )


__all__ = ["CHANNEL_NAME", "NullSink"]
