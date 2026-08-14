"""The daemon's error contract.

Mirrors `abelard_common.errors`' shape (one root, narrow subclasses) rather than
importing it, because the scout's failure taxonomy carries two kinds the shared
contract has no concept of: a containment breach (`QuarantineError`) and a
classification refusal (`ClassificationError`). Both are fail-loud by
construction — there is no soft-fail path that returns a plausible-looking
empty success.

Doctrine: leaf modules are total over valid inputs; these are raised by the
orchestrator and by the guards, not scattered through the adapters.
"""

from __future__ import annotations


class ScoutError(RuntimeError):
    """Root of every error this daemon raises deliberately."""


class ConfigError(ScoutError):
    """Required configuration missing or invalid."""


class FetchError(ScoutError):
    """A source could not be read.

    Raised on transport failure AND on provider-error-in-response-text: an
    HTTP 200 carrying an error payload is a failure, not a success with zero
    items. The distinction matters because 'ok with zero items' preserves the
    watermark while 'error' must not.
    """


class ClassificationError(ScoutError):
    """The batched classification pass could not produce a trustworthy result.

    Raised on truncation (`stop_reason == "max_tokens"`), empty response text,
    unparseable JSON, or a shape mismatch. Never swallowed into a GREEN: the
    asymmetric-error ruling says an unclassifiable item lands YELLOW with the
    failure as its reason, because the safe default for an admission decision
    is 'needs judgment', not 'admissible'.
    """


class QuarantineError(ScoutError):
    """Fetched content carried agent-directed instruction-shaped material.

    This is not a parse failure — it is the containment boundary doing its job.
    The payload is quarantined, the source row flagged, and the event surfaced.
    Raised only where the caller must not proceed; the guard's normal path
    returns a verdict object rather than raising.
    """


__all__ = [
    "ScoutError",
    "ConfigError",
    "FetchError",
    "ClassificationError",
    "QuarantineError",
]
