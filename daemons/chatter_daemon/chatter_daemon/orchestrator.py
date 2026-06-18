"""Orchestrator spine — one canonical timestamp, windows derived once, source fan-out.

Order 1 owns the spine only. It stamps ONE `canonical_unix` per run, derives every
window from it once (`windows.derive_windows`), and exposes a registered-`Source`
fan-out. No plugins exist yet, so a run over zero sources yields an empty record
list with the windows and watchlist summaries populated — proving the spine
end-to-end. Per-source fan-out detail, aggregation, and the anomaly layer land in
later orders; the loop here is the stub they slot into, already carrying the
per-source failure isolation the protocol promises.
"""

from __future__ import annotations

import logging
import time

from .schema import NormalizedRecord, ScanEnvelope, ScanMode, WatchlistSummary
from .sources.base import ScanContext, Source
from .watchlist import WatchlistConfig
from .windows import derive_windows, iso_z

_log = logging.getLogger("chatter_daemon.orchestrator")


def run_scan(
    watchlists: list[WatchlistConfig],
    *,
    sources: list[Source] | None = None,
    scan_mode: ScanMode = "watchlist",
    now: int | None = None,
) -> ScanEnvelope:
    """Run one scan and return the `ScanEnvelope`.

    `now` is injectable for hermetic tests; in production it is the ONLY clock read
    in the daemon. `canonical_ts` and every window derive from this single value —
    no leaf module reads the clock.
    """
    canonical_unix = int(time.time()) if now is None else int(now)
    canonical_ts = iso_z(canonical_unix)
    windows = derive_windows(canonical_unix)
    context = ScanContext(
        canonical_unix=canonical_unix,
        canonical_ts=canonical_ts,
        windows=windows,
    )

    registered = sources or []
    records: list[NormalizedRecord] = []
    errors: list[str] = []

    # Source fan-out. No plugins are registered at Order 1, so this is a no-op that
    # yields an empty record list — but it already isolates per-source failure so
    # the plugins (Orders 2-6) slot in without changing the spine's contract.
    for source in registered:
        for watchlist in watchlists:
            try:
                result = source.fetch(watchlist, context=context)
            except Exception as exc:  # one dead source never sinks the scan
                name = getattr(source, "name", "source")
                _log.warning("source %r failed: %s", name, exc)
                errors.append(f"{name}: {exc}")
                continue
            records.extend(result.records)
            errors.extend(result.warnings)
            if result.error:
                errors.append(f"{result.source}: {result.error}")

    summaries = [
        WatchlistSummary(
            name=w.name, tickers=len(w.tickers), active=len(w.active_tickers)
        )
        for w in watchlists
    ]
    return ScanEnvelope(
        scan_mode=scan_mode,
        canonical_ts=canonical_ts,
        windows=list(windows.values()),
        watchlists=summaries,
        records=records,
        errors=errors,
    )
