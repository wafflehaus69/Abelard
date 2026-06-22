"""PDF report deliverable (Order 10) — a client-facing presentation over a persisted
scan. Reads the SAME artifact `read-chatter` reads (AggregatedScanResult / AttentionResult)
and renders an editorial PDF that LEADS WITH SIGNAL and SUPPRESSES NOISE, instead of the
flat terminal dump.

Rank-and-suppress (all display-layer — no scan/aggregation/anomaly logic here):
  - Ranked by PEAK source magnitude first, diversity as tiebreak — a name loud on one
    source outranks a name weakly present on three (fixes the flat mis-sort).
  - Headlines collapse to count + top-3 sample, never the full array.
  - The quiet / no-signal tail collapses to a single line.
  - A DEGRADED BANNER is mandatory when the scan was partial (report-layer fail-loud).
  - No fabricated precision: "baselines building" while history < N_min, not empty z's.

ReportLab is lazy-imported inside the render functions (heavy dep, contained). The pure
ranking/suppression helpers are import-light and unit-tested directly; the drawing is
smoke-tested for a valid PDF.

StockTwits enrichment (rank / trending_score / watchlist_count / summary) is read
DEFENSIVELY via `getattr` — it renders the moment Order 9 adds those fields, no rebuild.
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape

from .schema import AggregatedScanResult, AttentionResult

_HEADLINE_SAMPLE = 3
_DIGEST_LOUDEST = 5

_SOURCE_LABEL = {
    "finnhub_news": "Finnhub news",
    "smg": "/smg/",
    "reddit": "Reddit",
    "google_trends": "Google Trends",
    "stocktwits": "StockTwits",
    # attention surfaces
    "smg_freq": "/smg/",
    "reddit_rising": "WSB rising",
    "stocktwits_trending": "StockTwits",
}
_COUNT_NOUN = {
    "finnhub_news": "headlines",
    "smg": "/smg/ mentions",
    "reddit": "Reddit mentions",
    "stocktwits": "StockTwits mentions",
}


def friendly_source(source: str) -> str:
    return _SOURCE_LABEL.get(source, source)


# --- watchlist: ranking + suppression (pure, unit-tested) -------------------------


def watchlist_peak(ticker) -> float:
    """Peak single-source magnitude — the largest count across count-sources, or the
    24h Trends interest. The ranking key (loudest-on-any-source wins)."""
    peak = 0.0
    for s in ticker.sources:
        if s.source == "google_trends":
            peak = max(peak, s.metrics.interest_24h or 0.0)
        else:
            peak = max(peak, float(s.metrics.mention_count))
    return peak


def rank_watchlist(result: AggregatedScanResult) -> list:
    """Tickers with any signal (diversity >= 1), sorted by peak magnitude desc, then
    diversity desc, then ticker."""
    signal = [t for t in result.tickers if t.source_diversity > 0]
    return sorted(signal, key=lambda t: (-watchlist_peak(t), -t.source_diversity, t.ticker))


def quiet_watchlist(result: AggregatedScanResult) -> list[str]:
    """The no-signal tail (diversity 0) — collapsed to one line by the renderer."""
    return sorted(t.ticker for t in result.tickers if t.source_diversity == 0)


def degraded_banner(sources, degraded: bool) -> str | None:
    """The mandatory partial-scan banner. None when the scan was whole."""
    if not degraded:
        return None
    failed = [friendly_source(s.source) for s in sources if not s.ok]
    if not failed:
        return "Partial scan: one or more sources were unavailable this run."
    return f"Partial scan: {', '.join(failed)} unavailable this run."


def headline_sample(signal) -> tuple[int, list[str]]:
    """(count, top-3 titles) — the single biggest reportability fix: never dump the
    full headline array."""
    heads = signal.metrics.headlines or []
    titles = [h.title for h in heads[:_HEADLINE_SAMPLE]]
    return signal.metrics.mention_count, titles


def _watchlist_phrase(s) -> str | None:
    """One source's contribution, source-labeled — or None if it carries no signal."""
    if s.source == "google_trends":
        i = s.metrics.interest_24h
        if i is None:
            return None
        extra = ""
        if s.metrics.interest_7d is not None or s.metrics.interest_monthly is not None:
            extra = f" (7d {s.metrics.interest_7d} / mo {s.metrics.interest_monthly})"
        return f"interest {i}{extra}"
    n = s.metrics.mention_count
    if n <= 0:
        return None
    if s.source == "finnhub_news":
        count, titles = headline_sample(s)
        if titles:
            sample = "; ".join(escape(t) for t in titles)
            return f"{count} headlines (top: {sample})"
        return f"{count} headlines"
    noun = _COUNT_NOUN.get(s.source, "mentions")
    phrase = f"{n} {noun}"
    extra = _stocktwits_extras(s)  # defensive — empty until Order 9
    return phrase + extra


def _has_spike(ticker) -> bool:
    return any(s.anomaly is not None and s.anomaly.state == "spike" for s in ticker.sources)


def _stocktwits_extras(signal) -> str:
    """StockTwits enrichment, read defensively so it appears the moment Order 9 adds
    rank / trending_score / watchlist_count / summary. Empty string until then."""
    if friendly_source(getattr(signal, "source", "")) != "StockTwits":
        return ""
    bits = []
    rank = getattr(signal, "rank", None)
    score = getattr(signal, "trending_score", None)
    wl = getattr(signal, "watchlist_count", None)
    if rank is not None:
        bits.append(f"rank {rank}")
    if score is not None:
        bits.append(f"score {score}")
    if wl is not None:
        bits.append(f"{wl} watchers")
    return f" ({', '.join(bits)})" if bits else ""


def _summary_of(signal) -> str | None:
    s = getattr(signal, "summary", None)
    return s if isinstance(s, str) and s.strip() else None


# --- attention: amplified-first ordering (pure) -----------------------------------


def attention_amplified(result: AttentionResult) -> list:
    return [t for t in result.tickers if t.amplified]


def attention_accelerating(result: AttentionResult) -> list:
    return [t for t in result.tickers if "spike" in t.flags]


def _attention_phrase(s) -> str:
    noun = _COUNT_NOUN.get(s.source, "mentions") if s.source != "stocktwits_trending" else "trending"
    base = f"{s.count} {noun}" if s.source != "stocktwits_trending" else "trending"
    return base + _stocktwits_extras(s)


# --- PDF rendering (ReportLab, lazy) ----------------------------------------------


def render_report(result, out_path: Path) -> Path:
    """Dispatch on artifact type and render the PDF. Returns the output path."""
    if isinstance(result, AttentionResult):
        return _render_attention_pdf(result, out_path)
    return _render_watchlist_pdf(result, out_path)


def _styles():
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet

    base = getSampleStyleSheet()
    base.add(
        ParagraphStyle(
            "Banner", parent=base["Normal"], textColor=colors.white,
            backColor=colors.HexColor("#B00020"), borderPadding=6, spaceAfter=10,
            fontSize=10, leading=13,
        )
    )
    base.add(
        ParagraphStyle("Quiet", parent=base["Normal"], textColor=colors.grey, fontSize=9, leading=12)
    )
    base.add(
        ParagraphStyle("Foot", parent=base["Normal"], textColor=colors.grey, fontSize=8, leading=10)
    )
    base.add(ParagraphStyle("Block", parent=base["Normal"], spaceAfter=6, leading=13))
    return base


def _render_watchlist_pdf(result: AggregatedScanResult, out_path: Path) -> Path:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    st = _styles()
    story = []
    wl = ", ".join(escape(w.name) for w in result.watchlists) or "(none)"
    story.append(Paragraph("Chatter Report", st["Title"]))
    story.append(
        Paragraph(f"Watchlist: {wl} &nbsp;&middot;&nbsp; {escape(result.canonical_ts)} "
                  f"&nbsp;&middot;&nbsp; {result.scan_mode}", st["Normal"])
    )
    story.append(Spacer(1, 8))

    banner = degraded_banner(result.sources, result.degraded)
    if banner:
        story.append(Paragraph(escape(banner), st["Banner"]))

    story.append(Paragraph("Digest", st["Heading2"]))
    for line in _watchlist_digest(result):
        story.append(Paragraph(line, st["Normal"]))
    story.append(Spacer(1, 10))

    ranked = rank_watchlist(result)
    story.append(Paragraph("Detail", st["Heading2"]))
    if not ranked:
        story.append(Paragraph("No chatter on any name this scan.", st["Normal"]))
    for t in ranked:
        story.append(Paragraph(_watchlist_block(t), st["Block"]))

    quiet = quiet_watchlist(result)
    if quiet:
        story.append(Spacer(1, 6))
        story.append(
            Paragraph(
                f"Quiet this scan (no chatter): {', '.join(escape(q) for q in quiet)} "
                f"({len(quiet)}).",
                st["Quiet"],
            )
        )

    story.append(Spacer(1, 14))
    c = result.cost
    story.append(
        Paragraph(
            f"{c.haiku_calls} Haiku calls &middot; {c.input_tokens}+{c.output_tokens} tokens "
            f"&middot; {escape(result.scan_id)}",
            st["Foot"],
        )
    )
    SimpleDocTemplate(str(out_path), pagesize=letter, title="Chatter Report").build(story)
    return out_path


def _watchlist_digest(result: AggregatedScanResult) -> list[str]:
    ranked = rank_watchlist(result)
    lines: list[str] = []
    for t in ranked[:_DIGEST_LOUDEST]:
        phrases = [p for p in (_watchlist_phrase(s) for s in _by_magnitude(t)) if p]
        lines.append(f"<b>{escape(t.ticker)}</b> — {'; '.join(phrases)}")
    multi = [escape(t.ticker) for t in ranked if t.source_diversity >= 2]
    if multi:
        lines.append(f"Across multiple sources: {', '.join(multi)}.")
    spikes = [escape(t.ticker) for t in ranked if _has_spike(t)]
    if spikes:
        lines.append(f"Spiking vs baseline: {', '.join(spikes)}.")
    else:
        lines.append("Anomaly baselines still building — no spikes flagged yet.")
    return lines


def _by_magnitude(ticker):
    def mag(s):
        return s.metrics.interest_24h or 0.0 if s.source == "google_trends" else float(s.metrics.mention_count)
    return sorted(ticker.sources, key=lambda s: -mag(s))


def _watchlist_block(ticker) -> str:
    phrases = [p for p in (_watchlist_phrase(s) for s in _by_magnitude(ticker)) if p]
    for s in ticker.sources:  # StockTwits "why it's trending" — folds in with Order 9
        summ = _summary_of(s)
        if summ:
            phrases.append(f"<i>{escape(summ)}</i>")
    parts = [f"<b>{escape(ticker.ticker)}</b>"]
    if phrases:
        parts.append("<br/>" + "<br/>".join(phrases))
    tags = []
    if _has_spike(ticker):
        tags.append("SPIKE")
    if ticker.source_diversity >= 2:
        tags.append(f"{ticker.source_diversity} sources")
    if tags:
        parts.append(f"  [{', '.join(tags)}]")
    return "".join(parts)


def _render_attention_pdf(result: AttentionResult, out_path: Path) -> Path:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    st = _styles()
    story = []
    story.append(Paragraph("Chatter — Discovery Report", st["Title"]))
    story.append(
        Paragraph(f"Off-watchlist attention &nbsp;&middot;&nbsp; {escape(result.canonical_ts)} "
                  f"&nbsp;&middot;&nbsp; {result.scan_mode}", st["Normal"])
    )
    story.append(Spacer(1, 8))

    banner = _attention_banner(result)
    if banner:
        story.append(Paragraph(escape(banner), st["Banner"]))

    story.append(Paragraph("Digest", st["Heading2"]))
    for line in _attention_digest(result):
        story.append(Paragraph(line, st["Normal"]))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Detail", st["Heading2"]))
    if not result.tickers:
        story.append(Paragraph("Nothing above the floor this scan.", st["Normal"]))
    for t in result.tickers:  # already salience-sorted
        story.append(Paragraph(_attention_block(t), st["Block"]))

    story.append(Spacer(1, 14))
    story.append(
        Paragraph(
            f"{result.pruned} rows rolled to cold &middot; {escape(result.scan_id)}", st["Foot"]
        )
    )
    SimpleDocTemplate(str(out_path), pagesize=letter, title="Chatter Discovery Report").build(story)
    return out_path


def _attention_banner(result: AttentionResult) -> str | None:
    if not result.degraded:
        return None
    failed = [friendly_source(s.source) for s in result.surfaces if not s.ok]
    if not failed:
        return "Partial discovery: a surface was unavailable this run."
    return f"Partial discovery: {', '.join(failed)} unavailable this run."


def _attention_digest(result: AttentionResult) -> list[str]:
    lines: list[str] = []
    amp = attention_amplified(result)
    if amp:
        bits = ", ".join(f"{escape(t.ticker)} ({'/'.join(escape(w) for w in t.on_watchlists)})" for t in amp)
        lines.append(f"<b>Amplified</b> — the crowd surfaced a watchlist name on its own: {bits}.")
    for t in result.tickers[:_DIGEST_LOUDEST]:
        phrases = "; ".join(_attention_phrase(s) for s in t.signals)
        lines.append(f"<b>{escape(t.ticker)}</b> — salience {t.salience}; {phrases}")
    accel = [escape(t.ticker) for t in attention_accelerating(result)]
    if accel:
        lines.append(f"Accelerating vs baseline: {', '.join(accel)}.")
    else:
        lines.append("Velocity baselines still building — no spikes flagged yet.")
    return lines


def _attention_block(ticker) -> str:
    phrases = [_attention_phrase(s) for s in ticker.signals]
    for s in ticker.signals:  # StockTwits "why it's trending" — folds in with Order 9
        summ = _summary_of(s)
        if summ:
            phrases.append(f"<i>{escape(summ)}</i>")
    parts = [f"<b>{escape(ticker.ticker)}</b> &mdash; salience {ticker.salience}"]
    if phrases:
        parts.append("<br/>" + "<br/>".join(phrases))
    tags = []
    if "spike" in ticker.flags:
        tags.append("SPIKE")
    if "cold_start" in ticker.flags:
        tags.append("cold-start")
    if ticker.amplified:
        tags.append("AMPLIFIED " + "/".join(escape(w) for w in ticker.on_watchlists))
    if tags:
        parts.append(f"  [{', '.join(tags)}]")
    return "".join(parts)


__all__ = [
    "attention_accelerating",
    "attention_amplified",
    "degraded_banner",
    "friendly_source",
    "headline_sample",
    "quiet_watchlist",
    "rank_watchlist",
    "render_report",
    "watchlist_peak",
]
