"""Raw-scrape history dump (Order 19).

The orchestrator collects one ``source\\tTICKER\\ttext`` line per raw item on the scan
envelope (Finnhub headlines, StockTwits message bodies, Twitter survivor tweets). This
groups them into readable per-source sections and writes ``history/chatter-raw_<ts>.txt``
beside the aggregate archive — the raw evidence behind each scan, kept off to the side of
the structured JSON.

Best-effort: a write failure is a warning, never a scan failure. Gitignored (like archive/).
The Twitter section holds the SURVIVOR tweets (post promo/spam filter) — the substantive
commentary the summary is built from, not the ~80% promo the filter drops.
"""

from __future__ import annotations

from pathlib import Path

from .report import eastern_stamp

# (source ids, section heading), in display order. The two headline sources — Finnhub and Yahoo's
# fresh net-new heads — MERGE into one HEADLINES section per ticker (CH-SRC-1: Yahoo folds in with
# Finnhub, not a separate section).
_SECTIONS: list[tuple[tuple[str, ...], str]] = [
    (("finnhub_news", "yahoo_rss"), "HEADLINES"),
    (("stocktwits",), "STOCKTWITS"),
    (("twitter",), "TWITTER (promo-filtered)"),
]


def render_history(raw_items: list[str], *, scan_id: str, stamp: str) -> str:
    """Group source-prefixed raw lines (``source\\tTICKER\\ttext``) into sections, each grouped by
    ticker. A section may span several sources (the HEADLINES section merges Finnhub + Yahoo per
    ticker — CH-SRC-1). Malformed lines are skipped."""
    by_source: dict[str, list[tuple[str, str]]] = {}
    for line in raw_items:
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        source, ticker, text = parts
        by_source.setdefault(source, []).append((ticker, text))

    out: list[str] = [f"ChatterDaemon raw scrape   {stamp}   {scan_id}", "=" * 74, ""]
    for sources, heading in _SECTIONS:
        # Merge the section's sources per ticker, in first-seen (watchlist) order.
        per_ticker: dict[str, list[str]] = {}
        order: list[str] = []
        for src in sources:
            for ticker, text in by_source.get(src, []):
                if ticker not in per_ticker:
                    per_ticker[ticker] = []
                    order.append(ticker)
                per_ticker[ticker].append(text)
        count = sum(len(v) for v in per_ticker.values())
        out.append(f"### {heading}   ({count} items)")
        out.append("-" * 74)
        if not order:
            out.append("(none)")
        for ticker in order:
            out.append("")
            out.append(f"[{ticker}]")
            out.extend(f"  - {text}" for text in per_ticker[ticker])
        out.append("")
    return "\n".join(out)


def _safe_stamp(stamp: str) -> str:
    """'07-09-2026 09:42 EDT' -> '07-09-2026_0942_EDT' (filesystem-safe)."""
    return stamp.replace(":", "").replace(" ", "_")


def write_history(root: Path, raw_items: list[str], *, scan_id: str, canonical_ts: str) -> Path:
    """Write the raw-scrape .txt under ``root`` (created if missing). Filename carries the
    Eastern timestamp, mirroring the PDF report. Returns the written path."""
    stamp = eastern_stamp(canonical_ts)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"chatter-raw_{_safe_stamp(stamp)}.txt"
    path.write_text(render_history(raw_items, scan_id=scan_id, stamp=stamp), encoding="utf-8")
    return path


__all__ = ["render_history", "write_history"]
