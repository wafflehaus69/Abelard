"""ATTENTION scan (Order 8 Phase 2) — gate -> store -> salience + velocity -> amplified.

The front half is Phase 1's discovery (`SurfaceCounts`). This back half:
  1. GATE — admit a (ticker, source) when its count >= the per-source floor. The junk
     already died at the filter/blacklist; the floor is the real-but-quiet cutoff.
  2. VELOCITY — for count surfaces (smg_freq / reddit_rising): z-score the current
     count vs the ticker's trailing baseline in the rolling store, reusing Order-7
     anomaly states (building < N_min -> thin < floor -> z, sigma=0 guard). Baselines
     are read BEFORE the current scan is appended (exclude-current invariant).
  3. SALIENCE — "loud right now" = the per-surface counts; no baseline needed, so a
     brand-new ticker from zero surfaces immediately (flagged `cold_start`).
  4. AMPLIFIED — a discovered ticker also on a loaded watchlist: the crowd found one
     of his names on its own.

StockTwits trending is point-in-time -> salience only (no velocity, no store).

PRUNE = roll-up-to-cold: events past 14 days aggregate to a `ColdRollup`, get ARCHIVED,
and only then leave the hot table (archive before delete, nothing lost). Descriptive
throughout; Abelard judges materiality.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from . import attention_store
from .anomaly import compute_count_anomaly
from .discovery import SurfaceCounts
from .persist import make_rollup_id, write_cold_rollup
from .schema import (
    AttentionResult,
    AttentionSignal,
    AttentionSurfaceStatus,
    AttentionTicker,
    ColdRollup,
    CostTelemetry,
)

# Surfaces whose count is a time series we z-score for velocity. StockTwits trending is
# point-in-time -> salience only.
VELOCITY_SOURCES = frozenset({"smg_freq", "reddit_rising"})


def run_attention_scan(
    *,
    conn,
    surfaces: list[SurfaceCounts],
    watchlist_symbols: dict[str, set[str]],
    floors: dict[str, int],
    scan_id: str,
    canonical_ts: str,
    now: int,
    baseline_window: int,
    baseline_min_obs: int,
    spike_z_threshold: float,
) -> AttentionResult:
    """Gate the discovery counts, compute velocity against the rolling baseline, then
    append the current scan. `watchlist_symbols` is `{name: {active symbols}}` for the
    amplified intersection."""
    per_ticker: dict[str, list[AttentionSignal]] = {}
    order: list[str] = []
    surface_status: list[AttentionSurfaceStatus] = []
    errors: list[str] = []
    pending: list[tuple[str, str, int]] = []  # (ticker, source, count) to append

    # PASS 1 — gate + read baselines + compute velocity (no appends yet).
    for sc in surfaces:
        floor = floors.get(sc.source, 0)
        if sc.warning:
            surface_status.append(
                AttentionSurfaceStatus(
                    source=sc.source, ok=False, candidates=0, floor=floor, warning=sc.warning
                )
            )
            errors.append(f"{sc.source}: {sc.warning}")
            continue
        admitted = 0
        for ticker, count in sc.counts.items():
            if count < floor:
                continue  # real-but-quiet -> below the floor, not admitted
            admitted += 1
            anomaly = None
            if sc.source in VELOCITY_SOURCES:
                baseline = attention_store.read_baseline(
                    conn, ticker=ticker, source=sc.source, window=baseline_window, now=now
                )
                anomaly = compute_count_anomaly(
                    baseline,
                    count=count,
                    floor=floor,
                    min_obs=baseline_min_obs,
                    z_threshold=spike_z_threshold,
                )
                pending.append((ticker, sc.source, count))
            if ticker not in per_ticker:
                per_ticker[ticker] = []
                order.append(ticker)
            per_ticker[ticker].append(
                AttentionSignal(
                    source=sc.source, semantics=sc.semantics, count=count, anomaly=anomaly
                )
            )
        surface_status.append(
            AttentionSurfaceStatus(source=sc.source, ok=True, candidates=admitted, floor=floor)
        )

    # PASS 2 — append the current scan's counts AFTER every baseline was read.
    for ticker, source, count in pending:
        attention_store.append_observation(
            conn, ticker=ticker, source=source, scan_ts=now, count=count
        )

    tickers: list[AttentionTicker] = []
    for ticker in order:
        signals = per_ticker[ticker]
        on_wl = sorted(name for name, syms in watchlist_symbols.items() if ticker in syms)
        flags: list[str] = []
        if any(s.anomaly is not None and s.anomaly.state == "building" for s in signals):
            flags.append("cold_start")
        if any(s.anomaly is not None and s.anomaly.state == "spike" for s in signals):
            flags.append("spike")
        tickers.append(
            AttentionTicker(
                ticker=ticker,
                signals=signals,
                salience=sum(s.count for s in signals),
                on_watchlists=on_wl,
                amplified=bool(on_wl),
                flags=flags,
            )
        )
    tickers.sort(key=lambda t: (-t.salience, t.ticker))

    return AttentionResult(
        scan_id=scan_id,
        canonical_ts=canonical_ts,
        surfaces=surface_status,
        tickers=tickers,
        pruned=0,
        degraded=any(not s.ok for s in surface_status),
        cost=CostTelemetry(),  # ATTENTION carries no LLM; zeros kept for symmetry
        errors=errors,
    )


def prune_cold(
    conn,
    *,
    now: int,
    archive_root: Path,
    generated_ts: int,
    hot_days: int = attention_store.HOT_WINDOW_DAYS,
) -> int:
    """Roll up + archive + delete hot events past the window. Archive BEFORE delete: if
    the cold write fails, nothing is removed (retried next run). Returns rows pruned."""
    cutoff = attention_store.hot_cutoff(now, hot_days=hot_days)
    rollups = attention_store.collect_prunable_rollups(conn, cutoff=cutoff)
    if not rollups:
        return 0
    day = datetime.fromtimestamp(int(generated_ts), tz=timezone.utc).strftime("%Y-%m-%d")
    rollup = ColdRollup(
        rollup_id=make_rollup_id(day, generated_ts),
        generated_ts=int(generated_ts),
        cutoff_ts=cutoff,
        rollups=rollups,
    )
    write_cold_rollup(archive_root, rollup)  # archive FIRST (fail loud if unwritable)
    return attention_store.delete_pruned(conn, cutoff=cutoff)  # then delete


__all__ = ["VELOCITY_SOURCES", "prune_cold", "run_attention_scan"]
