"""Anomaly computation (Order 7) — the mechanical count read (Abelard interprets).

Count sources (Finnhub / /smg/ / StockTwits): z-score the current count
against the trailing baseline, gated by a per-source min-volume floor and a minimum
history depth.

Pure functions over a `Baseline` (already read, current scan excluded) — the store
read/append + per-source floor selection live in the aggregation layer. States:
building (history < N_min) | thin (count < floor) | ok | spike | none. A sigma=0
baseline yields no z (flagged), never a fabricated number.
"""

from __future__ import annotations

from .baseline import Baseline
from .schema import Anomaly


def compute_count_anomaly(
    baseline: Baseline,
    *,
    count: int,
    floor: int,
    min_obs: int,
    z_threshold: float,
) -> Anomaly:
    """Count-source anomaly. Ordered guards: build first (no history), then thin
    (below floor), then z (with a sigma=0 escape)."""
    mean = baseline.mean if baseline.n else None
    std = baseline.std if baseline.n else None

    if baseline.n < min_obs:
        return Anomaly(
            kind="count",
            state="building",
            mean=mean,
            std=std,
            observations=baseline.n,
            note=f"{baseline.n}/{min_obs} observations",
        )
    if count < floor:
        return Anomaly(
            kind="count",
            state="thin",
            mean=mean,
            std=std,
            observations=baseline.n,
            note=f"count {count} < floor {floor}",
        )
    if baseline.std == 0:
        return Anomaly(
            kind="count",
            state="ok",
            mean=mean,
            std=0.0,
            observations=baseline.n,
            note="sigma_zero: constant baseline, no z-score",
        )
    z = round((count - baseline.mean) / baseline.std, 4)
    return Anomaly(
        kind="count",
        state="spike" if z >= z_threshold else "ok",
        z=z,
        mean=mean,
        std=std,
        observations=baseline.n,
    )


__all__ = ["compute_count_anomaly"]
