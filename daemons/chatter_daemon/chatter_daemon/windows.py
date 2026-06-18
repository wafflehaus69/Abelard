"""Window derivation — pure, from the single canonical timestamp.

The orchestrator stamps ONE canonical Unix timestamp at entry and calls
`derive_windows(canonical_unix)`; every window (24h / 7d / monthly) is computed from
that single value. This module never reads the clock — it is total over its integer
input, so "all windows trace to one canonical_ts" holds by construction. The 7d and
monthly windows are the baseline context the Google Trends plugin (Order 5) reads;
the 24h window is the primary scan window for the other sources.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .schema import Window

DAY_S = 86_400
WEEK_S = 7 * DAY_S
# "monthly" = trailing 30 days. Calendar months are ambiguous for fixed-offset
# arithmetic; Google Trends' own `today 1-m` query is likewise a rolling ~month.
MONTH_S = 30 * DAY_S


def iso_z(unix: int) -> str:
    """Format a Unix timestamp as ISO-8601 UTC with a trailing Z."""
    return (
        datetime.fromtimestamp(unix, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _window(canonical_unix: int, span_s: int, label: str) -> Window:
    return Window(
        start=iso_z(canonical_unix - span_s),
        end=iso_z(canonical_unix),
        label=label,
    )


def derive_windows(canonical_unix: int) -> dict[str, Window]:
    """Return the 24h / 7d / monthly windows, all anchored to one timestamp.

    Every window's `end` is `iso_z(canonical_unix)` — identical across all three —
    so the single-timestamp invariant is visible in the output itself.
    """
    return {
        "24h": _window(canonical_unix, DAY_S, "24h"),
        "7d": _window(canonical_unix, WEEK_S, "7d"),
        "monthly": _window(canonical_unix, MONTH_S, "monthly"),
    }
