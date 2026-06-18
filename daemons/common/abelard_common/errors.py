"""Canonical structured-error contract for the OpenClaw daemons.

``DaemonError`` is the monorepo-wide base for loud, structured failures. A leaf
module raises it (or a stage-specific subclass) and the orchestrator folds
``to_error()`` into the ``errors`` array of the output contract without
fabricating data.

New daemons define their own native error rooted here, e.g.
``class ChatterDaemonError(DaemonError): ...``. BizDaemon aliases its historical
``BizDaemonError`` to this exact class for backward compatibility (see
``biz_daemon.config``) — that alias is biz's compat shim, not the pattern new
daemons copy.
"""

from __future__ import annotations


class DaemonError(RuntimeError):
    """Base for loud, structured daemon failures.

    Carries a ``stage`` tag and a ``to_error()`` rendering so leaf modules can
    fail loudly and an orchestrator can fold the failure into its ``errors``
    array without fabricating data.
    """

    def __init__(self, message: str, *, stage: str) -> None:
        super().__init__(message)
        self.stage = stage

    def to_error(self) -> str:
        return f"{self.stage}: {self}"
