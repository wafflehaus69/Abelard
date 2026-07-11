"""Loud, structured failures for CONSENSUS.

Rooted in the monorepo-wide ``DaemonError`` contract (``abelard_common.errors``)
so a failure carries a ``stage`` tag and a ``to_error()`` rendering, and an
orchestrator can fold it into an ``errors`` array without fabricating data —
the same discipline the OpenClaw daemons use. New native error → subclass here,
per the monorepo convention (a component defines its own error rooted in
``DaemonError`` rather than importing another daemon's).

Rule 1 corollary: these are raised on *failure to obtain* data (network dead,
retries exhausted, unparseable upstream, missing config). They are NOT raised
for a legitimately empty result — "this wallet made zero trades" is an empty
list, not an error. Never invent data to avoid raising.
"""

from __future__ import annotations

from abelard_common.errors import DaemonError


class ConsensusError(DaemonError):
    """Base for loud, structured CONSENSUS failures."""


class ConfigError(ConsensusError):
    """Configuration missing or invalid. The system must never run half-configured."""

    def __init__(self, message: str, *, stage: str = "config") -> None:
        super().__init__(message, stage=stage)


class DataLayerError(ConsensusError):
    """A data-layer fetch failed (transport, upstream error, or parse failure).

    ``source`` records which upstream failed (e.g. ``polymarket_data``,
    ``kalshi``) for observability; it is folded into the ``stage`` tag.
    """

    def __init__(self, message: str, *, source: str) -> None:
        super().__init__(message, stage=f"data_layer.{source}")
        self.source = source


class CacheError(ConsensusError):
    """The on-disk raw-response cache could not be read or written."""

    def __init__(self, message: str, *, stage: str = "cache") -> None:
        super().__init__(message, stage=stage)
