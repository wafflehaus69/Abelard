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

from .schema import (
    CostTelemetry,
    NormalizedRecord,
    ScanEnvelope,
    ScanMode,
    SourceStatus,
    WatchlistSummary,
)
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
        scan_mode=scan_mode,
        canonical_unix=canonical_unix,
        canonical_ts=canonical_ts,
        windows=windows,
    )

    registered = sources or []
    records: list[NormalizedRecord] = []
    errors: list[str] = []
    sources_status: list[SourceStatus] = []
    raw_items: list[str] = []  # Order 19: source-prefixed raw scrape lines for the history dump
    cost = CostTelemetry()  # LLM cost, folded in before the envelope is returned

    # Source fan-out with per-source failure isolation: a source that raises (or
    # returns a fatal error) is recorded ok=False in `sources` and folded into
    # `errors`, and the OTHER sources still produce output. Honest zeros (records
    # with mention_count=0) are data, not failures.
    for source in registered:
        src_records: list[NormalizedRecord] = []
        src_error: str | None = None
        for watchlist in watchlists:
            try:
                result = source.fetch(watchlist, context=context)
            except Exception as exc:  # one dead source never sinks the scan
                src_error = str(exc)
                _log.warning("source %r failed: %s", source.name, exc)
                errors.append(f"{source.name}: {exc}")
                break  # stop this source's remaining watchlists; others continue
            src_records.extend(result.records)
            errors.extend(result.warnings)
            raw_items.extend(f"{result.source}\t{item}" for item in result.raw_items)
            if result.cost is not None:
                cost.haiku_calls += result.cost.haiku_calls
                cost.input_tokens += result.cost.input_tokens
                cost.output_tokens += result.cost.output_tokens
                cost.cache_read_input_tokens += result.cost.cache_read_input_tokens
                cost.cache_creation_input_tokens += result.cost.cache_creation_input_tokens
            if result.error:
                src_error = result.error
                errors.append(f"{result.source}: {result.error}")
        records.extend(src_records)
        sources_status.append(
            SourceStatus(
                source=source.name,
                ok=src_error is None,
                record_count=len(src_records),
                error=src_error,
            )
        )

    summaries = [
        WatchlistSummary(
            name=w.name, tickers=len(w.tickers), active=len(w.active_tickers)
        )
        for w in watchlists
    ]
    degraded = any(not s.ok for s in sources_status)
    return ScanEnvelope(
        scan_mode=scan_mode,
        canonical_ts=canonical_ts,
        windows=list(windows.values()),
        watchlists=summaries,
        sources=sources_status,
        records=records,
        cost=cost,
        degraded=degraded,
        errors=errors,
        raw_items=raw_items,
    )
