"""One-line 'highest volume of activity' summary of a persisted chatter scan —
the flavor line for the morning-briefs email (Mando 2026-07-15).

Reads an AggregatedScanResult JSON and prints ONE sentence naming the loudest
ticker, reusing the report's OWN vetted ranking (rank_watchlist / watchlist_peak)
so the email's claim always matches the attached PDF's #1 row. Runs under the
chatter venv (it imports chatter_daemon).

Fail-soft by contract: any problem -> print nothing to stdout, exit 0. The email
wrapper captures stdout; an empty capture simply omits the chatter line rather
than blocking the send. Diagnostics go to stderr.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Volume noun per source (Finnhub/​/smg/ are the true count-sources; Trends is interest).
_NOUN = {"finnhub_news": "headlines", "smg": "/smg/ posts"}


def _peak_source(ticker):
    """The single source that sets this ticker's rank, and its magnitude — mirrors
    watchlist_peak's inclusion rules (Finnhub / /smg/ counts, or Trends interest;
    StockTwits + Twitter excluded). Returns (source_name, magnitude, is_interest)."""
    best_src, best_mag, best_is_interest = None, 0.0, False
    for s in ticker.sources:
        if s.source in ("stocktwits", "twitter"):
            continue
        if s.source == "google_trends":
            mag, is_interest = float(s.metrics.interest_24h or 0.0), True
        else:
            mag, is_interest = float(s.metrics.mention_count), False
        if mag > best_mag:
            best_src, best_mag, best_is_interest = s.source, mag, is_interest
    return best_src, best_mag, best_is_interest


def summarize(path: Path) -> str:
    from chatter_daemon.report import friendly_source, rank_watchlist
    from chatter_daemon.schema import AggregatedScanResult

    result = AggregatedScanResult.model_validate_json(path.read_text())
    ranked = rank_watchlist(result)
    if not ranked:
        return "Market chatter was quiet across the watchlist — no ticker cleared a volume signal."
    top = ranked[0]
    src, mag, is_interest = _peak_source(top)
    diversity = top.source_diversity
    corrob = (f", corroborated across {diversity} sources"
              if diversity > 1 else " on a single source")
    if src is None or mag <= 0:
        return (f"{top.ticker} led market chatter{corrob}.")
    if is_interest:
        return (f"{top.ticker} drew the heaviest market chatter — search interest "
                f"{int(mag)} on {friendly_source(src)}{corrob}.")
    noun = _NOUN.get(src, "mentions")
    return (f"{top.ticker} drew the heaviest market chatter — {int(mag)} "
            f"{friendly_source(src)} {noun}{corrob}.")


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print("scan_summary: usage: scan_summary.py <scan.json>", file=sys.stderr)
        return 0
    try:
        line = summarize(Path(argv[0]).expanduser())
    except Exception as exc:  # fail-soft: never block the email
        print(f"scan_summary: skipped ({type(exc).__name__}: {exc})", file=sys.stderr)
        return 0
    print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
