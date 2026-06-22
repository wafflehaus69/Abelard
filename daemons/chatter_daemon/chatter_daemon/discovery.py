"""ATTENTION discovery (Order 8, Phase 1) — off-watchlist candidate extraction.

Hit the discovery surfaces, universe-mode tokenize (`Matcher.for_universe`), noise-
filter + Finnhub-validate (the matcher's universe IS the validated symbol set), and
count per `(ticker, source)`. Phase 1 is the calibration FRONT HALF: NO store, gate,
baseline, velocity, or persistence — it exists to print the per-source mention
distribution so the volume floor + blacklist get set by LOOKING, not guessing.

Per-source count semantics differ and are LABELED, not forced uniform:
  - smg_freq — trailing-24h counts (posts are timestamped).
  - stocktwits_trending — a point-in-time "trending now" snapshot (no window).

The two noise problems stay separate: junk strings are the filter+validation's job
(already applied here); real-but-quiet tickers are the FLOOR's job (Phase 2) — the
distribution this prints is exactly what tells the two tails apart.

A surface that fails is ISOLATED (returns a warning + empty counts); the pull runs on
whatever surfaces succeed. StockTwits is included only when a client is supplied (the
residential-curl gate); absent, the pull runs clean on /smg/ alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from abelard_common import fourchan_fetch

from .matching import Matcher

SMG_SOURCE = "smg_freq"
STOCKTWITS_SOURCE = "stocktwits_trending"

_DAY_SECONDS = 24 * 60 * 60


@dataclass
class SurfaceCounts:
    """One surface's candidate distribution: `{ticker: count}` plus the semantics
    label and an optional `warning` set when the surface degraded."""

    source: str
    semantics: str
    counts: dict[str, int] = field(default_factory=dict)
    warning: str | None = None


@runtime_checkable
class StockTwitsTrendingClient(Protocol):
    def trending(self) -> list[str]:
        """Return the current trending symbols (point-in-time)."""
        ...


def pull_smg_frequency(fetcher: Any, matcher: Matcher) -> SurfaceCounts:
    """/smg/ universe-frequency — distinct posts per validated ticker (ALL tickers,
    not watchlist-scoped)."""
    semantics = "24h /smg/ posts"
    try:
        threads = fourchan_fetch.scrape_smg(fetcher)
    except Exception as exc:
        return SurfaceCounts(SMG_SOURCE, semantics, warning=f"smg: {exc}")
    seen: dict[str, set[int]] = {}
    for thread in threads:
        for post in thread.posts:
            post_no = int(post["no"])
            for sym in matcher.match(post.get("com", "")):
                seen.setdefault(sym, set()).add(post_no)
    return SurfaceCounts(SMG_SOURCE, semantics, {s: len(ids) for s, ids in seen.items()})


def pull_stocktwits_trending(client: Any, universe: frozenset[str]) -> SurfaceCounts:
    """StockTwits trending — a point-in-time symbol list, validated against the
    universe. Count is presence (1 each); rank survives as list order upstream."""
    semantics = "point-in-time trending"
    try:
        symbols = client.trending()
    except Exception as exc:
        return SurfaceCounts(STOCKTWITS_SOURCE, semantics, warning=f"stocktwits: {exc}")
    counts: dict[str, int] = {}
    for raw in symbols:
        sym = str(raw).upper()
        if sym in universe:
            counts[sym] = counts.get(sym, 0) + 1
    return SurfaceCounts(STOCKTWITS_SOURCE, semantics, counts)


def run_dry_run(
    *,
    matcher: Matcher,
    universe: frozenset[str],
    now: int,
    fetcher: Any | None = None,
    stocktwits_client: Any | None = None,
) -> list[SurfaceCounts]:
    """Run every supplied surface (each self-isolating) and return their counts.
    A surface whose client/fetcher is None is skipped cleanly."""
    results: list[SurfaceCounts] = []
    if fetcher is not None:
        results.append(pull_smg_frequency(fetcher, matcher))
    if stocktwits_client is not None:
        results.append(pull_stocktwits_trending(stocktwits_client, universe))
    return results


def format_distribution(results: list[SurfaceCounts]) -> str:
    """Render the per-source distribution (sorted desc by count) + a combined view —
    the calibration artifact Mando reads to set floors and expand the blacklist."""
    lines: list[str] = [
        "ATTENTION calibration pull (--dry-run) — per-source mention distribution",
        "",
    ]
    combined: dict[str, int] = {}
    for r in results:
        lines.append(f"== {r.source}  ({r.semantics}) ==")
        if r.warning:
            lines.append(f"  DEGRADED: {r.warning}")
        if not r.counts:
            lines.append("  (no candidates)")
        for sym, n in sorted(r.counts.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"  {sym:8} {n}")
            combined[sym] = combined.get(sym, 0) + n
        lines.append(
            f"  [{len(r.counts)} tickers, {sum(r.counts.values())} total mentions]"
        )
        lines.append("")

    lines.append("== COMBINED (all sources) ==")
    for sym, n in sorted(combined.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"  {sym:8} {n}")
    lines.append(f"  [{len(combined)} unique tickers across surfaces]")
    return "\n".join(lines)


__all__ = [
    "SMG_SOURCE",
    "STOCKTWITS_SOURCE",
    "StockTwitsTrendingClient",
    "SurfaceCounts",
    "format_distribution",
    "pull_smg_frequency",
    "pull_stocktwits_trending",
    "run_dry_run",
]
