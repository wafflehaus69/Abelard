"""ATTENTION discovery (Order 8, Phase 1) — off-watchlist candidate extraction.

Hit the discovery surfaces, universe-mode tokenize (`Matcher.for_universe`), noise-
filter + Finnhub-validate (the matcher's universe IS the validated symbol set), and
count per `(ticker, source)`. Phase 1 is the calibration FRONT HALF: NO store, gate,
baseline, velocity, or persistence — it exists to print the per-source mention
distribution so the volume floor + blacklist get set by LOOKING, not guessing.

Per-source count semantics differ and are LABELED, not forced uniform:
  - smg_freq — trailing-24h distinct-post counts (posts are timestamped).
  - stocktwits_trending — the top-30 "trending now" snapshot; the count is the rounded
    trending_score (momentum), and the API's ranking self-gates (no noise-filter or
    universe-validation — the symbols are cashtag-native and exchange-validated).

The two noise problems stay separate: junk strings are the filter+validation's job
(already applied here); real-but-quiet tickers are the FLOOR's job (Phase 2) — the
distribution this prints is exactly what tells the two tails apart.

A surface that fails is ISOLATED (returns a warning + empty counts); the pull runs on
whatever surfaces succeed. StockTwits is a public endpoint (browser UA, no key) but
degrade-clean: a CloudFlare wall surfaces as a warning, and the pull runs on /smg/
alone when StockTwits is dark.
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
    label and an optional `warning` set when the surface degraded. `meta` carries
    per-ticker source extras (StockTwits trending: rank / trending_score /
    watchlist_count / sector / summary) — empty for count-only surfaces like /smg/."""

    source: str
    semantics: str
    counts: dict[str, int] = field(default_factory=dict)
    warning: str | None = None
    meta: dict[str, dict] = field(default_factory=dict)


@runtime_checkable
class StockTwitsTrendingClient(Protocol):
    def trending(self) -> list[dict]:
        """Return the current trending symbol objects (top-30, pre-ranked). Each is a
        dict with at least `symbol`; `rank` / `trending_score` / `watchlist_count` /
        `sector` and a nullable `trends.summary` ride along. Raises on a CF wall."""
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


def pull_stocktwits_trending(client: Any) -> SurfaceCounts:
    """StockTwits trending — the top-30 pre-ranked symbols. The API's ranking IS the
    gate, so we do NOT noise-filter or universe-validate here (the symbols are cashtag-
    native and exchange-validated at the source). The velocity/salience count is the
    rounded `trending_score` (the momentum axis); rank / score / watchlist_count /
    sector and a nullable summary ride along in `meta`. Every field is null-guarded — a
    live `trends: null` or an ETF with no fundamentals must not crash the parse."""
    semantics = "point-in-time trending (top 30)"
    try:
        symbols = client.trending()
    except Exception as exc:
        return SurfaceCounts(STOCKTWITS_SOURCE, semantics, warning=f"stocktwits: {exc}")
    counts: dict[str, int] = {}
    meta: dict[str, dict] = {}
    for raw in symbols:
        if not isinstance(raw, dict):
            continue
        sym = str(raw.get("symbol", "")).strip().upper()
        if not sym:
            continue
        score = raw.get("trending_score")
        score_f = float(score) if isinstance(score, (int, float)) else None
        trends = raw.get("trends")  # NULLABLE upstream — guard before .get
        summary = trends.get("summary") if isinstance(trends, dict) else None
        rank = raw.get("rank")
        wl = raw.get("watchlist_count")
        sector = raw.get("sector")
        # trending_score is the velocity axis; round to the int the count store holds.
        # The z-score is scale-invariant, so rounding doesn't distort velocity; salience
        # then tracks the score magnitude (live-calibratable per the order).
        counts[sym] = int(round(score_f)) if score_f is not None else 0
        meta[sym] = {
            "rank": rank if isinstance(rank, int) else None,
            "trending_score": score_f,
            "watchlist_count": wl if isinstance(wl, int) else None,
            "sector": sector if isinstance(sector, str) and sector.strip() else None,
            "summary": summary if isinstance(summary, str) and summary.strip() else None,
        }
    return SurfaceCounts(STOCKTWITS_SOURCE, semantics, counts, meta=meta)


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
        results.append(pull_stocktwits_trending(stocktwits_client))  # universe-free: top-30 self-gates
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
