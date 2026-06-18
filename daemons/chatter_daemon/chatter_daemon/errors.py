"""ChatterDaemon's native structured-error type.

Per the monorepo error doctrine (abelard_common is the canonical home of the
`DaemonError(stage=…, to_error())` contract), ChatterDaemon roots its loud,
structured failures at `DaemonError`. Stage-specific failures subclass
`ChatterDaemonError` and fix their `stage` (e.g. `WatchlistError` in
`watchlist.py`). This is the forward pattern new daemons follow — unlike
BizDaemon's backward-compat `BizDaemonError = DaemonError` alias.
"""

from __future__ import annotations

from abelard_common.errors import DaemonError


class ChatterDaemonError(DaemonError):
    """Base for ChatterDaemon's loud, structured failures.

    Inherits the `stage` tag and `to_error()` rendering from `DaemonError`, so the
    orchestrator can fold any failure into the `errors` array of the output
    contract without fabricating data.
    """
