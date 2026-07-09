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

# Source id -> section heading, in display order. Only these three are dumped (Order 19).
_SECTIONS: list[tuple[str, str]] = [
    ("finnhub_news", "HEADLINES (Finnhub)"),
    ("stocktwits", "STOCKTWITS"),
    ("twitter", "TWITTER (promo-filtered)"),
]


def render_history(raw_items: list[str], *, scan_id: str, stamp: str) -> str:
    """Group source-prefixed raw lines (``source\\tTICKER\\ttext``) into per-source sections,
    each grouped by ticker. Malformed lines are skipped."""
    by_source: dict[str, list[tuple[str, str]]] = {}
    for line in raw_items:
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        source, ticker, text = parts
        by_source.setdefault(source, []).append((ticker, text))

    out: list[str] = [f"ChatterDaemon raw scrape   {stamp}   {scan_id}", "=" * 74, ""]
    for source, heading in _SECTIONS:
        items = by_source.get(source, [])
        out.append(f"### {heading}   ({len(items)} items)")
        out.append("-" * 74)
        if not items:
            out.append("(none)")
        else:
            last: str | None = None
            for ticker, text in items:
                if ticker != last:
                    out.append("" if last is None else "")
                    out.append(f"[{ticker}]")
                    last = ticker
                out.append(f"  - {text}")
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
