"""The Builder's error contract.

Same shape as `scout_daemon.errors` (one root, narrow subclasses) and for the
same reason: fail-loud by construction, with no soft-fail path that returns a
plausible-looking empty success.

NOTE THE ABSENCE. There is no `SubmissionError`, because there is no
submission. There is no `AuthError`, because nothing here authenticates. If
either ever appears in this file, invariant 2 or invariant 3 has been broken and
the structural tests in `tests/test_soul.py` should have caught it first.
"""

from __future__ import annotations


class BuilderError(RuntimeError):
    """Root of every error this daemon raises deliberately."""


class ConfigError(BuilderError):
    """Required configuration missing or invalid."""


class FetchError(BuilderError):
    """A repository or referenced material could not be read.

    Raised on transport failure AND on provider-error-in-200. Inherited from
    scout unchanged: an HTTP 200 carrying an error payload is a failure, not a
    success with nothing in it.
    """


class GoneError(FetchError):
    """The server answered that the resource is permanently gone (HTTP 410).

    A SUBCLASS RATHER THAN A SIBLING, because it IS a fetch failure -- but a
    determinate one. 410 is the server stating a fact ("issues are disabled on
    this repository"), not the network failing to deliver an answer. Measured
    2026-09-02, two of the 21 rows on the live code queue return 410 from the
    issues API, and treating that as a transport error aborted the run instead
    of concluding, correctly, that there is no issue to work on.

    Typed here rather than in `abelard_common.http_client` because that client
    is shared with four other daemons and widening its taxonomy is a decision
    outside this build.
    """


class IntakeError(BuilderError):
    """The work item is not something this daemon may act on.

    Raised when a row fails the input contract -- not admitted, not code-PR
    shaped, or missing the fields a gate needs. Deliberately loud rather than
    filtered away: a row that reaches intake and is rejected is a fact Mando
    needs, because it usually means the admission and the daemon disagree about
    what was admitted.
    """


class ContainmentError(BuilderError):
    """Fetched content carried agent-directed instruction-shaped material.

    Invariant 7. This daemon reads issue bodies, maintainer comments and
    CONTRIBUTING files -- all stranger-authored, any of which may contain text
    addressed to an autonomous agent. Such text is evidence about the
    repository, never an instruction to follow.
    """


__all__ = [
    "BuilderError",
    "ConfigError",
    "FetchError",
    "IntakeError",
    "ContainmentError",
]
