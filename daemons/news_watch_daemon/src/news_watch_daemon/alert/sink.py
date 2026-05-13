"""AlertSink protocol — the one-method interface every transport implements.

Concrete sinks (SignalSink in Step 6, TelegramBotSink in Step 7,
NullSink in this step for tests) conform structurally to this Protocol.
No inheritance is required — Pydantic-style duck typing.

Brief §7 architectural rule: the sink is *dumb transport*. The
materiality gate (Step 8) decides whether dispatch is called at all;
the sink's job is to deliver the brief to its target channel without
judgment. Failures surface via `DispatchResult.success=False`; no
exceptions cross the protocol boundary.

Channel-name discipline:
  - `AlertSink.channel_name` is the sink's declared identity (free
    string). The orchestrator uses it for logging and for picking
    the right sink instance when multiple are wired.
  - `Brief.dispatch.channel` is the persisted channel field, a
    closed Literal["signal", "telegram_bot"] in the schema. Mapping
    happens at the orchestrator layer when writing the Brief.
  - `DispatchResult.channel` is the sink-side echo of what it
    delivered to — also a free string. Production sinks return
    "signal" or "telegram_bot" matching the Brief schema; the test
    NullSink returns "null".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from ..synthesize.brief import Brief


@dataclass(frozen=True)
class DispatchResult:
    """Outcome of one AlertSink.dispatch() call.

    `success=True` means the sink delivered the brief to its target.
    `success=False` means a transport failure (network, auth, queue
    full, recipient-validation mismatch). Materiality-gate
    suppressions are NOT routed through this — the gate decides
    ahead of time whether to call dispatch at all.
    """

    success: bool
    channel: str
    error: Optional[str] = None
    dispatched_at_unix: int = 0


@runtime_checkable
class AlertSink(Protocol):
    """Structural interface for alert transports.

    Two members: `channel_name` (the sink's declared identity) and
    `dispatch(brief)` (delivery). Implementations MUST NOT raise from
    dispatch; transport-level errors return as
    `DispatchResult(success=False, error=...)`.
    """

    @property
    def channel_name(self) -> str:
        """Stable channel identifier for this sink instance."""
        ...

    def dispatch(self, brief: Brief) -> DispatchResult:
        """Deliver the brief. Never raises."""
        ...


__all__ = ["AlertSink", "DispatchResult"]
