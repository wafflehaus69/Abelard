"""Aggregation layer (Order 7) — roll per-(ticker, source) NormalizedRecords into a
per-ticker cross-source view with an anomaly read, against the trailing baseline.

ORDERING INVARIANT (the load-bearing one): every anomaly is computed against the
PRIOR baseline, and only THEN are the current scan's count observations appended —
so a scan never sits in its own baseline. We do it in two passes: read+compute for
all (ticker, source) pairs, then append. (`read_baseline` already excludes `< now`,
so the two-pass split is belt-and-suspenders, but it keeps the invariant obvious.)

The plugin `NormalizedRecord` is untouched; this builds a separate type.
"""

from __future__ import annotations

import sqlite3

from .anomaly import compute_count_anomaly
from .baseline import append_observation, read_baseline
from .schema import (
    AggregatedScanResult,
    AggregatedTicker,
    Anomaly,
    ScanEnvelope,
    SourceSignal,
)

# Sources whose signal is a count z-scored against the baseline store. StockTwits is
# left OUT (Order 12) — its velocity is the aggregate's now-vs-24h gap, not a rolling
# count, so it never touches the rolling store.
COUNT_SOURCES = frozenset({"finnhub_news", "smg"})
STOCKTWITS_SOURCE = "stocktwits"
ST_GAP_SPIKE = 15  # |now - 24h| sentiment points that flags an igniting/cooling name


def _stocktwits_gap_anomaly(agg) -> Anomaly:
    """The StockTwits aggregate's velocity = the now-vs-24h sentiment gap (Order 12).
    |gap| >= ST_GAP_SPIKE -> spike (igniting or cooling); else ok; no aggregate -> none."""
    if agg is None or agg.sent_gap is None:
        return Anomaly(kind="count", state="none", note="no StockTwits aggregate")
    gap = agg.sent_gap
    state = "spike" if abs(gap) >= ST_GAP_SPIKE else "ok"
    return Anomaly(kind="count", state=state, note=f"sentiment gap now-24h {gap:+d}")


def build_aggregate(
    envelope: ScanEnvelope,
    *,
    conn: sqlite3.Connection,
    scan_id: str,
    source_floors: dict[str, int],
    baseline_window: int,
    baseline_min_obs: int,
    spike_z_threshold: float,
    now: int,
    max_age_s: int | None = None,
) -> AggregatedScanResult:
    """Build the persisted aggregate from one scan envelope + the baseline store.

    `now` is the run's canonical_unix (the single clock). Count observations are
    appended at `now` AFTER every anomaly is computed.
    """
    by_ticker: dict[tuple[str, str], list] = {}
    order: list[tuple[str, str]] = []
    for rec in envelope.records:
        key = (rec.watchlist, rec.ticker)
        if key not in by_ticker:
            by_ticker[key] = []
            order.append(key)
        by_ticker[key].append(rec)

    tickers_out: list[AggregatedTicker] = []
    pending: list[tuple[str, str, str, int]] = []  # (watchlist, ticker, source, count)

    for wl, ticker in order:
        signals: list[SourceSignal] = []
        diversity = 0
        for rec in by_ticker[(wl, ticker)]:
            if rec.source == STOCKTWITS_SOURCE:
                # Order 12: velocity = the aggregate's now-vs-24h gap (no rolling store).
                # Presence of an aggregate read = signal (the page-size count is retired).
                anomaly = _stocktwits_gap_anomaly(rec.st_aggregate)
                signaled = rec.st_aggregate is not None or rec.metrics.mention_count > 0
            else:
                count = rec.metrics.mention_count
                baseline = read_baseline(
                    conn,
                    watchlist=wl,
                    ticker=ticker,
                    source=rec.source,
                    window=baseline_window,
                    now=now,
                    max_age_s=max_age_s,
                )
                anomaly = compute_count_anomaly(
                    baseline,
                    count=count,
                    floor=source_floors.get(rec.source, 0),
                    min_obs=baseline_min_obs,
                    z_threshold=spike_z_threshold,
                )
                signaled = count > 0
                pending.append((wl, ticker, rec.source, count))

            if signaled:
                diversity += 1
            signals.append(
                SourceSignal(
                    source=rec.source,
                    metrics=rec.metrics,
                    sentiment=rec.sentiment,
                    st_aggregate=rec.st_aggregate,
                    news_summary=rec.news_summary,
                    matched_by=rec.matched_by,
                    flags=rec.flags,
                    anomaly=anomaly,
                )
            )
        tickers_out.append(
            AggregatedTicker(
                watchlist=wl, ticker=ticker, sources=signals, source_diversity=diversity
            )
        )

    # PASS 2 — append the current scan's counts, AFTER every baseline was read.
    for wl, ticker, source, count in pending:
        append_observation(
            conn, watchlist=wl, ticker=ticker, source=source, canonical_unix=now, count=count
        )

    return AggregatedScanResult(
        scan_id=scan_id,
        scan_mode=envelope.scan_mode,
        canonical_ts=envelope.canonical_ts,
        windows=envelope.windows,
        watchlists=envelope.watchlists,
        tickers=tickers_out,
        sources=envelope.sources,
        degraded=envelope.degraded,
        cost=envelope.cost,
        errors=envelope.errors,
    )


__all__ = ["COUNT_SOURCES", "build_aggregate"]
