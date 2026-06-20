"""read-chatter renderer (Order 7) — human-readable per-ticker view over a persisted
AggregatedScanResult, mirroring News Watch's read-brief.

Pure: takes the validated result, returns a string (the CLI owns IO + fail-loud
loading). Counts are labeled by SOURCE SEMANTICS — "N headlines" (Finnhub), "N
mentions" (Reddit, /smg/), "interest X (7d Y / mo Z)" (Trends). Bull/bear shown where
`method != none`. Per ticker: `source_diversity` + per-source anomaly tag + flags.

Surfaces the run's `degraded` + per-source state + a cost summary — closing
read-brief's exit-0/1-only limitation: the operator sees which sources failed, that
the scan was partial, and what the Haiku batch cost.
"""

from __future__ import annotations

from .schema import Anomaly, AggregatedScanResult, AttentionResult, SourceSignal

_COUNT_NOUN = {
    "finnhub_news": "headlines",
    "reddit": "mentions",
    "smg": "mentions",
    "stocktwits": "mentions",
}


def render_chatter(result: AggregatedScanResult) -> str:
    lines: list[str] = []
    lines.append(f"chatter scan {result.scan_id}")
    lines.append(f"  at {result.canonical_ts}  mode={result.scan_mode}")
    if result.watchlists:
        wl = ", ".join(f"{w.name}({w.active}/{w.tickers} active)" for w in result.watchlists)
        lines.append(f"  watchlists: {wl}")

    src_bits = [
        f"{s.source}={'ok' if s.ok else 'FAILED'}({s.record_count})" for s in result.sources
    ]
    lines.append(f"  sources: {', '.join(src_bits) if src_bits else '(none)'}")
    if result.degraded:
        lines.append("  DEGRADED: one or more sources failed — partial scan")

    c = result.cost
    lines.append(
        f"  cost: {c.haiku_calls} haiku calls, in={c.input_tokens} out={c.output_tokens} "
        f"(cache r={c.cache_read_input_tokens}/w={c.cache_creation_input_tokens})"
    )
    if result.errors:
        lines.append("  errors:")
        lines.extend(f"    - {e}" for e in result.errors)
    lines.append("")

    # Strongest first: more corroborating sources, then alphabetical.
    for t in sorted(result.tickers, key=lambda x: (-x.source_diversity, x.ticker)):
        lines.append(f"{t.ticker}  [diversity {t.source_diversity}]")
        for sig in t.sources:
            lines.append("    " + _render_signal(sig))
    return "\n".join(lines)


def _render_signal(sig: SourceSignal) -> str:
    if sig.source == "google_trends":
        m = sig.metrics
        if m.interest_24h is None:
            body = "interest n/a"
        else:
            body = f"interest {m.interest_24h} (7d {m.interest_7d} / mo {m.interest_monthly})"
    else:
        noun = _COUNT_NOUN.get(sig.source, "items")
        body = f"{sig.metrics.mention_count} {noun}"

    sent = ""
    if sig.sentiment.method != "none":
        s = sig.sentiment
        sent = f"  bull/bear/neutral {s.bullish}/{s.bearish}/{s.neutral} ({sig.sentiment.method})"

    flags = f"  flags={','.join(sig.flags)}" if sig.flags else ""
    return f"{sig.source:14} {body}{sent}  [{_anomaly_tag(sig.anomaly)}]{flags}"


def _anomaly_tag(a: Anomaly) -> str:
    if a.state == "spike":
        if a.kind == "count" and a.z is not None:
            tag = f"SPIKE z={a.z}"
        elif a.kind == "trend" and a.ratio is not None:
            tag = f"SPIKE x{a.ratio}"
        else:
            tag = "SPIKE"
        return tag + (" (discounted)" if a.discounted else "")
    if a.state == "building":
        return f"building {a.observations} obs"
    if a.state == "thin":
        return "thin"
    if a.state == "none":
        return "no signal"
    # ok
    if a.kind == "count" and a.z is not None:
        return f"ok z={a.z}"
    if a.kind == "trend" and a.ratio is not None:
        return f"ok x{a.ratio}" + (" (discounted)" if a.discounted else "")
    return f"ok ({a.note})" if a.note else "ok"


def render_attention(result: AttentionResult) -> str:
    """Human-readable ATTENTION view: salience (loud now), accelerating (velocity vs
    trailing baseline), and amplified (also on a watchlist) — plus the run's
    degraded/surfaces/cost/prune state. Source-labeled, descriptive."""
    lines: list[str] = []
    lines.append(f"ATTENTION scan {result.scan_id}")
    lines.append(f"  at {result.canonical_ts}  mode={result.scan_mode}")
    src_bits = [
        f"{s.source}={'ok' if s.ok else 'FAILED'}({s.candidates}, floor {s.floor})"
        for s in result.surfaces
    ]
    lines.append(f"  surfaces: {', '.join(src_bits) if src_bits else '(none)'}")
    if result.degraded:
        lines.append("  DEGRADED: one or more discovery surfaces failed — partial scan")
    c = result.cost
    lines.append(f"  cost: {c.haiku_calls} haiku calls, in={c.input_tokens} out={c.output_tokens}")
    lines.append(f"  pruned: {result.pruned} hot rows rolled to cold archive")
    if result.errors:
        lines.append("  errors:")
        lines.extend(f"    - {e}" for e in result.errors)
    lines.append("")

    lines.append("SALIENCE (loud right now):")
    if not result.tickers:
        lines.append("  (nothing above the floor)")
    for t in result.tickers:  # already sorted by salience desc
        sig = "  ".join(f"{s.source}:{s.count}" for s in t.signals)
        tags = []
        if "spike" in t.flags:
            tags.append("SPIKE")
        if "cold_start" in t.flags:
            tags.append("cold-start")
        if t.amplified:
            tags.append("AMPLIFIED " + "/".join(t.on_watchlists))
        suffix = f"  [{', '.join(tags)}]" if tags else ""
        lines.append(f"  {t.ticker:8} {t.salience:>4}  {sig}{suffix}")
    lines.append("")

    accel = [t for t in result.tickers if "spike" in t.flags]
    lines.append("ACCELERATING (vs trailing baseline):")
    if not accel:
        lines.append("  (none)")
    for t in accel:
        zbits = "  ".join(
            f"{s.source} z={s.anomaly.z}"
            for s in t.signals
            if s.anomaly is not None and s.anomaly.state == "spike"
        )
        lines.append(f"  {t.ticker:8} {zbits}")
    lines.append("")

    amp = [t for t in result.tickers if t.amplified]
    lines.append("AMPLIFIED (also on a watchlist):")
    if not amp:
        lines.append("  (none)")
    for t in amp:
        lines.append(f"  {t.ticker:8} on {', '.join(t.on_watchlists)}")
    return "\n".join(lines)


__all__ = ["render_attention", "render_chatter"]
