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

from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from .matching import title_mentions_ticker as _title_relevant
from .schema import AggregatedScanResult, AttentionResult

_HEADLINE_SAMPLE = 3
_DIGEST_LOUDEST = 5

_SOURCE_LABEL = {
    "finnhub_news": "Finnhub news",
    "smg": "/smg/",
    "google_trends": "Google Trends",
    "stocktwits": "StockTwits",
    "yahoo_rss": "Yahoo",
    "alpha_vantage": "AV news-sentiment",
    # attention surfaces
    "smg_freq": "/smg/",
    "stocktwits_trending": "StockTwits",
}
# StockTwits is intentionally absent: its symbol-stream count is a fixed 30-message
# page (zero information), so StockTwits speaks through STANCE, not a volume noun.
_COUNT_NOUN = {
    "finnhub_news": "headlines",
    "smg": "/smg/ mentions",
}

_STANCE_LABEL = {"stocktwits": "StockTwits", "smg": "/smg/"}


def friendly_source(source: str) -> str:
    return _SOURCE_LABEL.get(source, source)


# --- watchlist: ranking + suppression (pure, unit-tested) -------------------------


def watchlist_peak(ticker) -> float:
    """Peak single-source magnitude — the largest real count across count-sources, or
    the 24h Trends interest. The ranking key (loudest-on-any-source wins). StockTwits and
    Twitter are EXCLUDED: StockTwits' count is a constant 30-message page (not volume),
    and Twitter is a Detail-only social read (Order 20) — both contribute stance, not
    rank. Ranking is driven by the true volume sources (Finnhub, /smg/) + Trends interest."""
    peak = 0.0
    for s in ticker.sources:
        if s.source in ("stocktwits", "twitter"):
            continue
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


def headline_sample(signal, ticker_symbol: str = "", aliases=None) -> tuple[int, list[str]]:
    """(count, top-3 titles). Finnhub cross-tags broad-market stories onto every large-
    cap, so the sample is filtered to titles that actually name the ticker (symbol or a
    company-name alias); only when NONE match does it fall back to feed order. The raw
    `headlines` array on the record is untouched — this filters the SAMPLE only."""
    heads = signal.metrics.headlines or []
    relevant = [h for h in heads if _title_relevant(h.title, ticker_symbol, aliases)]
    pool = relevant if relevant else heads
    titles = [h.title for h in pool[:_HEADLINE_SAMPLE]]
    return signal.metrics.mention_count, titles


def _watchlist_phrase(s, ticker_symbol: str = "", aliases=None) -> str | None:
    """One source's VOLUME / interest contribution, source-labeled — or None if it
    carries none. StockTwits returns None here: its count is page size, so StockTwits
    speaks through `_stance_phrase`, not a volume figure."""
    if s.source == "google_trends":
        i = s.metrics.interest_24h
        if i is None:
            return None
        extra = ""
        if s.metrics.interest_7d is not None or s.metrics.interest_monthly is not None:
            extra = f" (7d {s.metrics.interest_7d} / mo {s.metrics.interest_monthly})"
        return f"interest {i}{extra}"
    if s.source == "stocktwits":
        return None  # page-size count is noise; stance carries StockTwits (see below)
    if s.source == "twitter":
        return None  # Order 20: Twitter is Detail-only — no volume phrase in the digest
    n = s.metrics.mention_count
    if n <= 0:
        return None
    if s.source == "finnhub_news":
        count, titles = headline_sample(s, ticker_symbol, aliases)
        if titles:
            sample = "; ".join(escape(t) for t in titles)
            return f"{count} headlines (top: {sample})"
        return f"{count} headlines"
    noun = _COUNT_NOUN.get(s.source, "mentions")
    return f"{n} {noun}"


# --- stance (Order 11): direction-first, divergence-aware --------------------------


def _net_dir(sentiment) -> str | None:
    """Net direction of a stance: 'bull' | 'bear' | 'flat', or None when the source
    classified nothing (method=none). Finnhub/Trends are always None — never fabricate."""
    if sentiment is None or sentiment.method == "none":
        return None
    if sentiment.bullish > sentiment.bearish:
        return "bull"
    if sentiment.bearish > sentiment.bullish:
        return "bear"
    return "flat"


_ST_SPIKE_GAP = 15  # |now - 24h| points read as igniting/cooling (matches aggregate.ST_GAP_SPIKE)
_CONF_FLAG = {"low": "thin, low-confidence", "pump_suspect": "possible pump, low participation"}


def _band(norm) -> str:
    """0-100 -> a 5-band label for the StockTwits volume / participation display."""
    if norm is None:
        return "?"
    if norm >= 80:
        return "EXTREMELY_HIGH"
    if norm >= 60:
        return "HIGH"
    if norm >= 40:
        return "NORMAL"
    if norm >= 20:
        return "LOW"
    return "EXTREMELY_LOW"


def _fmt_msgs(n) -> str:
    if n is None:
        return "?"
    return f"{n // 1000}k" if n >= 1000 else str(n)


def _st_phrase(agg) -> str | None:
    """The StockTwits aggregate read (Order 12) — gap-LED when igniting/cooling, marked
    steady otherwise; real volume + participation + a low-confidence flag. None when the
    aggregate is absent (gateway down -> the caller falls back to native tags)."""
    if agg is None or agg.sent_now_norm is None:
        return None
    out = f"StockTwits NOW {agg.sent_now_label or _band(agg.sent_now_norm)} {agg.sent_now_norm}"
    gap = agg.sent_gap
    if gap is not None and abs(gap) >= _ST_SPIKE_GAP:
        out += f" (24h {agg.sent_24h_norm}, gap {gap:+d} {'IGNITING' if gap > 0 else 'COOLING'})"
    elif gap is not None:
        out += " (steady)"
    if agg.vol_now_norm is not None:
        out += f" &middot; vol {_band(agg.vol_now_norm)} ({_fmt_msgs(agg.vol_now_raw)})"
    if agg.participation_norm is not None:
        out += f" &middot; participation {_band(agg.participation_norm)} {agg.participation_norm}"
    flag = _CONF_FLAG.get(agg.confidence)
    if flag:
        out += f" [{flag}]"
    return out


def _signal_dir(s) -> str | None:
    """Net direction for divergence. StockTwits speaks via its aggregate NOW sentiment
    (>50 bull, <50 bear); other sources via the bull/bear tally."""
    if s.source == "stocktwits" and s.st_aggregate is not None and s.st_aggregate.sent_now_norm is not None:
        n = s.st_aggregate.sent_now_norm
        return "bull" if n > 50 else "bear" if n < 50 else "flat"
    return _net_dir(s.sentiment)


def _stance_phrase(s) -> str | None:
    """Direction-first stance for the digest. StockTwits speaks through its sentiment-API
    aggregate (gap-led, Order 12); /smg/ through its Haiku bull/bear. None when neither
    carries a read."""
    if s.source == "stocktwits":
        agg = _st_phrase(s.st_aggregate)
        if agg:
            return agg
        # gateway down -> fall back to the native tag read below
    if s.source == "twitter":
        return None  # Order 20: Twitter is Detail-only — its stance lives in the band, not the digest
    d = _net_dir(s.sentiment)
    if d is None:
        return None
    label = _STANCE_LABEL.get(s.source, friendly_source(s.source))
    out = f"{label} {s.sentiment.bullish}/{s.sentiment.bearish} {d}"
    nat = s.sentiment.native
    if nat is not None:
        out += f" (native {nat.bullish}/{nat.bearish})"
    return out


def _divergence(ticker):
    """`{'smg': dir, 'stocktwits': dir}` when the two retail crowds disagree in
    DIRECTION on this ticker (one net-bull, one net-bear) — the high-value cross-source
    signal. None when they agree, or either lacks a directional read. The daemon carries
    both and reconciles nothing; the report just surfaces the gap."""
    dirs = {}
    for s in ticker.sources:
        if s.source in ("smg", "stocktwits"):
            d = _signal_dir(s)
            if d in ("bull", "bear"):
                dirs[s.source] = d
    if "smg" in dirs and "stocktwits" in dirs and dirs["smg"] != dirs["stocktwits"]:
        return dirs
    return None


def _has_spike(ticker) -> bool:
    return any(s.anomaly is not None and s.anomaly.state == "spike" for s in ticker.sources)


# --- Order 13: banded rendering (colors encode DIRECTION; the WORDS carry warnings) ---

_GREEN = "#1a7f37"   # bullish
_RED = "#b00020"     # bearish / danger
_MUTED = "#666666"   # neutral / secondary meta
_DANGER = "#b00020"  # confidence / pump / thin warning flags
_SHADE = "#f0f0f2"   # social-band background tint
_TWITTER_SHADE = "#eaf1fb"  # Twitter band tint (Order 18) — distinct from the StockTwits band
_RULE = "#bbbbbb"    # divider / box rule


def eastern_stamp(iso_utc: str) -> str:
    """UTC ISO-8601 (Z) -> 'MM-DD-YYYY HH:MM TZ' in US Eastern, with the correct EST/EDT
    for that date (zoneinfo knows the DST rules — never hardcode the offset). 01:13 UTC
    rolls back to the prior Eastern evening; a hardcoded -5 would mis-date and mislabel."""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        east = dt.astimezone(ZoneInfo("America/New_York"))
        return f"{east:%m-%d-%Y %H:%M} {east.tzname()}"
    except (ValueError, TypeError):
        return iso_utc  # malformed -> raw, never crash


def report_default_filename(iso_utc: str) -> str:
    """Filesystem-safe default report name carrying the Eastern timestamp, e.g.
    'chatter-report_06-23-2026_2113_EDT.pdf' (no colon/space — Windows-safe)."""
    stamp = eastern_stamp(iso_utc).replace(":", "").replace(" ", "_")
    return f"chatter-report_{stamp}.pdf"


def _src(ticker, name):
    return next((s for s in ticker.sources if s.source == name), None)


def _dir_color(norm) -> str:
    if norm is None:
        return _MUTED
    return _GREEN if norm > 50 else _RED if norm < 50 else _MUTED


def _dir_color_counts(bull, bear) -> str:
    return _GREEN if bull > bear else _RED if bear > bull else _MUTED


def _av_color(score) -> str:
    """AV news-sentiment score [-1..+1] -> direction color, with a small dead-band around 0."""
    if score is None:
        return _MUTED
    return _GREEN if score > 0.05 else _RED if score < -0.05 else _MUTED


def _meta_bits(ticker) -> list[str]:
    """Compact header counts (no titles): 'N headlines', 'interest X', 'N /smg/'."""
    bits = []
    # CH-SRC-1: Finnhub + Yahoo's fresh net-new heads share one 'headlines' count.
    fin = _src(ticker, "finnhub_news")
    yah = _src(ticker, "yahoo_rss")
    heads = (fin.metrics.mention_count if fin else 0) + (yah.metrics.mention_count if yah else 0)
    if heads > 0:
        bits.append(f"{heads} headlines")
    tr = _src(ticker, "google_trends")
    if tr and tr.metrics.interest_24h is not None:
        bits.append(f"interest {int(round(tr.metrics.interest_24h))}")
    smg = _src(ticker, "smg")
    if smg and smg.metrics.mention_count > 0:
        bits.append(f"{smg.metrics.mention_count} /smg/")
    return bits


def _news_lines(ticker, aliases=None) -> list[str]:
    """News-band html: 'news · <top relevant headline>' and '/smg/ · X bull / Y bear'.
    A source carrying nothing is omitted (no empty rows)."""
    lines = []
    fin = _src(ticker, "finnhub_news")
    yah = _src(ticker, "yahoo_rss")
    # CH-SRC-1: Yahoo's fresh net-new heads fold into the Finnhub headline stream (no separate
    # line) — the news line shows Finnhub's named-news top, or Yahoo's freshest when Finnhub is
    # empty; both feed the combined 'headlines' count in the header meta.
    if fin and fin.metrics.mention_count > 0:
        _, titles = headline_sample(fin, ticker.ticker, (aliases or {}).get(ticker.ticker))
        if titles:
            lines.append(f'<font color="{_MUTED}">news &middot;</font> {escape(titles[0])}')
    elif yah and yah.metrics.headlines:
        lines.append(f'<font color="{_MUTED}">news &middot;</font> {escape(yah.metrics.headlines[0].title)}')
    # CH-SRC-2: the "why" is now ONE ticker-level summary over Finnhub + Yahoo (+ AV) headlines
    # analyzed together. Fall back to the legacy per-source Finnhub field so pre-CH-SRC-2 archives
    # still render their summary.
    summary = ticker.news_summary or (fin.news_summary if fin else None)
    if summary:
        lines.append(f'<font color="{_MUTED}">summary &middot;</font> {escape(summary)}')
    # CH-SRC-1: Alpha Vantage per-ticker news-sentiment axis (label + signed score + article count).
    av = _src(ticker, "alpha_vantage")
    ns = getattr(av, "news_sentiment", None) if av else None
    if ns is not None and ns.score is not None:
        col = _av_color(ns.score)
        lines.append(
            f'<font color="{_MUTED}">AV sentiment &middot;</font> '
            f'<font color="{col}">{escape(ns.label or "?")} {ns.score:+.2f}</font>'
            f'<font color="{_MUTED}"> ({ns.articles} articles)</font>'
        )
    smg = _src(ticker, "smg")
    if smg and smg.sentiment and smg.sentiment.method != "none":
        s = smg.sentiment
        col = _dir_color_counts(s.bullish, s.bearish)
        lines.append(
            f'<font color="{_MUTED}">/smg/ &middot;</font> '
            f'<font color="{col}">{s.bullish} bull / {s.bearish} bear</font>'
        )
    return lines


def _social_band_html(s) -> str | None:
    """The SHADED StockTwits social band — the Order-12 aggregate read, colored by
    DIRECTION (green bull / red bear / gray neutral) with the confidence flag in a danger
    tone. The warning WORDS carry the meaning so it survives a grayscale print. None when
    there is no StockTwits read at all."""
    if s is None:
        return None
    agg = s.st_aggregate
    if agg is not None and agg.sent_now_norm is not None:
        n = agg.sent_now_norm
        col = _dir_color(n)
        head = f'<b>STOCKTWITS</b>  <font color="{col}"><b>{agg.sent_now_label or _band(n)} {n}</b></font>'
        gap = agg.sent_gap
        if gap is not None and abs(gap) >= _ST_SPIKE_GAP:
            head += f'  <font color="{col}">gap {gap:+d} {"igniting" if gap > 0 else "cooling"}</font>'
        elif gap is not None:
            head += f'  <font color="{_MUTED}">(24h {agg.sent_24h_norm}, steady)</font>'
        line2 = ""
        if agg.vol_now_norm is not None:
            line2 = f'<font color="{_MUTED}">vol {_band(agg.vol_now_norm)} {_fmt_msgs(agg.vol_now_raw)}'
            if agg.participation_norm is not None:
                line2 += f"  &middot;  participation {agg.participation_norm}"
            line2 += "</font>"
        flag = _CONF_FLAG.get(agg.confidence)
        if flag:
            line2 += f'  <font color="{_DANGER}"><b>{flag}</b></font>'
        nat = s.sentiment.native if s.sentiment else None
        line3 = (
            f'<font color="{_MUTED}">native {nat.bullish}/{nat.bearish}</font>'
            if nat is not None and (nat.bullish or nat.bearish)
            else ""
        )
        return "<br/>".join(p for p in (head, line2, line3) if p)
    # gateway down -> the native tag read is the StockTwits band
    sent = s.sentiment
    if sent is not None and sent.method == "native":
        col = _dir_color_counts(sent.bullish, sent.bearish)
        return f'<b>STOCKTWITS</b>  <font color="{col}">native {sent.bullish} bull / {sent.bearish} bear</font>'
    return None


def _observed_span(ow) -> str:
    """The surviving-tweet window rendered compactly for the Twitter band. Same-day
    survivors read as plain `HH:MM-HH:MM`; when they straddle UTC midnight (the Twitter
    `--since` is a bare date, so survivors can cross into later days) a `+Nd` marker is
    appended — e.g. an earliest of 15:02 the prior day against a 13:43 latest renders
    `15:02-13:43 +1d`, not a reversed-looking `15:02-13:43`. N generalizes past 1 (a 7d
    scan can span a full week). Display-only: earliest/latest on the record stay full,
    correctly-ordered ISO. Parse failure degrades to the bare time span (never crashes the
    render), matching eastern_stamp's defensive posture."""
    span = f"{ow.earliest[11:16]}-{ow.latest[11:16]}"
    try:
        days = (datetime.fromisoformat(ow.latest[:10]).date()
                - datetime.fromisoformat(ow.earliest[:10]).date()).days
    except (ValueError, TypeError):
        return span
    return f"{span} +{days}d" if days else span


def _twitter_band_html(s) -> str | None:
    """The Twitter band (Order 18), rendered UNDER the StockTwits band: the crowd read
    (N tweets + bull/bear/neutral stance colored by direction + the surviving-tweet time
    span) then the <=3-sentence commentary summary. None when Twitter has no signal here."""
    if s is None:
        return None
    n = s.metrics.mention_count
    summary = getattr(s, "twitter_summary", None)
    if n == 0 and not summary:
        return None
    sent = s.sentiment
    if sent is not None and sent.method == "haiku":
        col = _dir_color_counts(sent.bullish, sent.bearish)
        head = (
            f'<b>TWITTER</b>  {n} tweets  '
            f'<font color="{col}">{sent.bullish} bull / {sent.bearish} bear / '
            f'{sent.neutral} neutral</font>'
        )
    else:
        head = f'<b>TWITTER</b>  {n} tweets'
    ow = getattr(s, "observed_window", None)
    if ow is not None:
        head += f'  <font color="{_MUTED}">[{_observed_span(ow)} UTC]</font>'
    if summary:
        head += f'<br/><font color="{_MUTED}">commentary &middot;</font> {escape(summary)}'
    return head


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


def render_report(result, out_path: Path, *, name_aliases=None) -> Path:
    """Dispatch on artifact type and render the PDF. `name_aliases` ({SYMBOL: [name
    words]}) sharpens the watchlist report's headline-relevance filter; None falls back
    to symbol-only matching. Returns the output path."""
    if isinstance(result, AttentionResult):
        return _render_attention_pdf(result, out_path)
    return _render_watchlist_pdf(result, out_path, name_aliases)


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
    base.add(ParagraphStyle("Band", parent=base["Normal"], fontSize=9, leading=12))
    base.add(ParagraphStyle("BandR", parent=base["Normal"], fontSize=9, leading=12, alignment=2))
    return base


def _ticker_table(ticker, aliases, st):
    """One ticker as a bounded two-band row (Order 13): header (ticker + compact meta +
    a right-aligned flags pill), a white news band, then a SHADED StockTwits social band
    under a divider rule. A band a source doesn't have is omitted (no empty rows)."""
    from reportlab.lib import colors
    from reportlab.platypus import KeepTogether, Paragraph, Table, TableStyle

    meta = "  &middot;  ".join(_meta_bits(ticker))
    hdr = f'<font size="11"><b>{escape(ticker.ticker)}</b></font>'
    if meta:
        hdr += f'   <font size="8" color="{_MUTED}">{meta}</font>'
    flags = []
    if _has_spike(ticker):
        flags.append("SPIKE")
    if _divergence(ticker):
        flags.append("SPLIT")
    flag_html = f'<font size="8" color="{_DANGER}"><b>[{"&middot;".join(flags)}]</b></font>' if flags else ""

    data = [[Paragraph(hdr, st["Band"]), Paragraph(flag_html, st["BandR"])]]
    style = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(_RULE)),
    ]
    row = 1
    news = _news_lines(ticker, aliases)
    if news:
        data.append([Paragraph("<br/>".join(news), st["Band"]), ""])
        style.append(("SPAN", (0, row), (1, row)))
        row += 1
    social = _social_band_html(_src(ticker, "stocktwits"))
    if social:
        data.append([Paragraph(social, st["Band"]), ""])
        style += [
            ("SPAN", (0, row), (1, row)),
            ("BACKGROUND", (0, row), (1, row), colors.HexColor(_SHADE)),
            ("LINEABOVE", (0, row), (1, row), 0.5, colors.HexColor(_RULE)),
        ]
        row += 1
    twit = _twitter_band_html(_src(ticker, "twitter"))
    if twit:
        data.append([Paragraph(twit, st["Band"]), ""])
        style += [
            ("SPAN", (0, row), (1, row)),
            ("BACKGROUND", (0, row), (1, row), colors.HexColor(_TWITTER_SHADE)),
            ("LINEABOVE", (0, row), (1, row), 0.5, colors.HexColor(_RULE)),
        ]
        row += 1
    tbl = Table(data, colWidths=[398, 70])
    tbl.setStyle(TableStyle(style))
    return KeepTogether([tbl])


def _render_watchlist_pdf(result: AggregatedScanResult, out_path: Path, name_aliases=None) -> Path:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    st = _styles()
    story = []
    wl = ", ".join(escape(w.name) for w in result.watchlists) or "(none)"
    story.append(Paragraph("Chatter Report", st["Title"]))
    story.append(
        Paragraph(f"{wl} &nbsp;&middot;&nbsp; {escape(eastern_stamp(result.canonical_ts))} "
                  f"&nbsp;&middot;&nbsp; {result.scan_mode}", st["Normal"])
    )
    story.append(Spacer(1, 8))

    banner = degraded_banner(result.sources, result.degraded)
    if banner:
        story.append(Paragraph(escape(banner), st["Banner"]))

    story.append(Paragraph("Digest", st["Heading2"]))
    for line in _watchlist_digest(result, name_aliases):
        story.append(Paragraph(line, st["Normal"]))
    story.append(Spacer(1, 10))

    ranked = rank_watchlist(result)
    story.append(Paragraph("Detail", st["Heading2"]))
    if not ranked:
        story.append(Paragraph("No chatter on any name this scan.", st["Normal"]))
    for t in ranked:
        story.append(_ticker_table(t, name_aliases, st))
        story.append(Spacer(1, 4))

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


def _watchlist_digest(result: AggregatedScanResult, aliases=None) -> list[str]:
    ranked = rank_watchlist(result)
    lines: list[str] = []
    for t in ranked[:_DIGEST_LOUDEST]:
        al = (aliases or {}).get(t.ticker)
        vol = [p for p in (_watchlist_phrase(s, t.ticker, al) for s in _by_magnitude(t)) if p]
        stance = [p for p in (_stance_phrase(s) for s in _by_magnitude(t)) if p]
        lines.append(f"<b>{escape(t.ticker)}</b> — {'; '.join(vol + stance)}")
    # Cross-source divergence — the crowds disagreeing IS the signal; surface it loud,
    # not buried in the detail.
    splits = []
    for t in ranked:
        d = _divergence(t)
        if d:
            splits.append(f"{escape(t.ticker)} (/smg/ {d['smg']} vs StockTwits {d['stocktwits']})")
    if splits:
        lines.append(f"<b>Sources split</b> (crowds disagree on direction): {', '.join(splits)}.")
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
        if s.source == "google_trends":
            return s.metrics.interest_24h or 0.0
        if s.source == "stocktwits":
            return 0.0  # page size, not magnitude — don't let the constant 30 sort it up
        return float(s.metrics.mention_count)
    return sorted(ticker.sources, key=lambda s: -mag(s))


def _render_attention_pdf(result: AttentionResult, out_path: Path) -> Path:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    st = _styles()
    story = []
    story.append(Paragraph("Chatter — Discovery Report", st["Title"]))
    story.append(
        Paragraph(f"Off-watchlist attention &nbsp;&middot;&nbsp; {escape(eastern_stamp(result.canonical_ts))} "
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
    "eastern_stamp",
    "friendly_source",
    "headline_sample",
    "quiet_watchlist",
    "rank_watchlist",
    "render_report",
    "report_default_filename",
    "watchlist_peak",
]
