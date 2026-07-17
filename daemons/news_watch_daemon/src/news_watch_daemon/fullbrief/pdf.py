"""PDF render target for the Full Brief.

Consumes the SAME structured `FullBriefEnvelope` that `fullbrief/render.py`
renders to text -- no content is re-derived; this is a display layer over the
existing object. ReportLab Platypus, lazy-imported (heavy dep, contained).

Self-contained in `news_watch_daemon` -- does NOT import ChatterDaemon's PDF
code (the no-daemon-imports-another-daemon rule). Hoisting a shared `pdf_render`
into `abelard_common` is filed convergence debt, deferred.

Unicode: registers reportlab's bundled Bitstream Vera family (Vera is DejaVu's
parent; Latin + Latin-Extended -- covers the Spanish + `headline_en` translation
content the brief carries). Bundled => portable to the mini with no system-font
dependency. The brief renders English synthesis output; the labels this module
controls stay ASCII (no Greek/arrows, which Vera lacks). Raw Cyrillic/CJK would
need a DejaVu swap -- noted; the brief layer does not carry it.

Degrade-clean: fails loud (`PdfRenderError`) on a missing font, a structurally
empty brief, or a zero-byte result -- never a silent empty PDF.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape
from zoneinfo import ZoneInfo

from .brief import FullBriefEnvelope


class PdfRenderError(RuntimeError):
    """Raised when a Full Brief PDF cannot be rendered -- missing font,
    structurally empty brief, or zero-byte output. Fail-loud, never a
    silent empty PDF."""


# Direction colors (words also carry the meaning, so it survives grayscale).
_CONFIRM = "#1a7f37"
_BREAK = "#b00020"
_AMBIG = "#9a6700"
_MUTED = "#666666"
_THEME_BG = "#33475b"
_WARN_BG = "#9a6700"
_RULE = "#bbbbbb"
_HEAD_BG = "#e8eaf0"
# Theme-segment title colors (bold): deep red for ACTIVE, deep yellow for quiet.
_SEG_ACTIVE = "#b00020"
_SEG_QUIET = "#9a6700"

# --- boxed-section layout (2026-07-17 visual pass) ---
# Every section renders inside a bordered box with an accent title bar. The
# page content width is letter (612pt) minus the 44pt L/R margins = 524pt.
_CONTENT_W = 612 - 44 - 44
_BOX_BORDER = "#c9ced6"          # neutral box outline
_SECTION_ACCENT = "#2c3e50"      # section title-bar background (dark slate), white text
# Active-vs-passive signal coding: a "hot"/active piece gets a warm red-tinted
# fill + red rule; a quiet/passive piece gets a cool muted-gray fill. The tag
# words ("ACTIVE"/"quiet") also carry the meaning so it survives grayscale.
_ACTIVE_FILL = "#fdecea"         # light red tint
_ACTIVE_RULE = "#b00020"
_QUIET_FILL = "#f2f3f5"          # light cool gray
_QUIET_RULE = "#9aa0a8"

# The materiality bar a Pass C event must clear to render as a SPECIFIC headline
# (Mando 2026-07-17: raise to 0.70 — only "confirmed shift" band and above).
_SPECIFIC_HEADLINE_MIN = 0.70

# Max sentences rendered for an orphan-crossing read (kept tight at 5, as
# requested). Theme segments get one more (6) — completeness of the distinct
# developments matters there, so we do not compress them as hard.
_MAX_SENTENCES = 5
_SEGMENT_MAX_SENTENCES = 6

_NEARMISS_CAP = 40  # near-miss rows in the PDF; the full list lives in the JSON

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")

# Abbreviations whose internal/trailing period must NOT read as a sentence
# boundary — otherwise a truncation that lands right after e.g. "U.S." cuts
# mid-thought ("...reach China's military despite U.S."). We mask their periods
# before the split and restore after, so the cut lands on a real sentence end.
# Case-sensitive (protects "co." in "Cisco." etc.); longest-first so "U.S.A."
# is masked before "U.S." can nibble its prefix.
_ABBREVS = sorted(
    (
        "U.S.A.", "U.S.", "U.K.", "U.N.", "E.U.", "U.A.E.", "D.C.",
        "a.m.", "p.m.", "vs.", "etc.", "Inc.", "Corp.", "Ltd.", "Co.",
        "Mr.", "Ms.", "Mrs.", "Dr.", "Sen.", "Rep.", "Gov.", "Gen.",
        "Lt.", "Col.", "Sgt.", "St.", "Jr.", "Sr.",
    ),
    key=len,
    reverse=True,
)
_ABBR_MASK = "\x00"  # sentinel period; never occurs in feed text


def _first_sentences(text: str | None, n: int = _MAX_SENTENCES) -> str:
    """First `n` sentences of `text`, whitespace-collapsed. Returns the whole
    text when it has <= n sentences. A crude but robust split on .!? + space,
    with common abbreviations shielded so the cut never lands mid-abbreviation."""
    collapsed = " ".join((text or "").split())
    if not collapsed:
        return ""
    masked = collapsed
    for abbr in _ABBREVS:
        if abbr in masked:
            masked = masked.replace(abbr, abbr.replace(".", _ABBR_MASK))
    parts = _SENT_SPLIT.split(masked)
    if len(parts) <= n:
        return collapsed
    return " ".join(parts[:n]).rstrip().replace(_ABBR_MASK, ".")

_FONT = "NWUni"
_FONT_B = "NWUni-Bold"
_FONTS_REGISTERED = False


def _dir_color(direction: str | None) -> str:
    return {"confirm": _CONFIRM, "break": _BREAK, "ambiguous": _AMBIG}.get(
        direction or "", _MUTED
    )


def eastern_stamp(iso_utc: str) -> str:
    """UTC ISO-8601 (Z) -> 'MM-DD-YYYY HH:MM TZ' in US Eastern (zoneinfo knows
    the DST rule). Malformed input returns raw, never crashes."""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        east = dt.astimezone(ZoneInfo("America/New_York"))
        return f"{east:%m-%d-%Y %H:%M} {east.tzname()}"
    except (ValueError, TypeError):
        return iso_utc


def default_pdf_filename(envelope: FullBriefEnvelope) -> str:
    """`<brief_id>.pdf` -- the brief_id already carries a filesystem-safe stamp."""
    return f"{envelope.brief_id}.pdf"


def _register_fonts() -> None:
    """Register reportlab's bundled Vera family as the Unicode face. Idempotent
    within a process. Fails loud if the bundled fonts are somehow absent."""
    global _FONTS_REGISTERED
    if _FONTS_REGISTERED:
        return
    import reportlab
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    fdir = Path(reportlab.__file__).resolve().parent / "fonts"
    faces = [
        ("NWUni", "Vera.ttf"),
        ("NWUni-Bold", "VeraBd.ttf"),
        ("NWUni-Italic", "VeraIt.ttf"),
        ("NWUni-BoldItalic", "VeraBI.ttf"),
    ]
    for name, fn in faces:
        p = fdir / fn
        if not p.is_file():
            raise PdfRenderError(
                f"bundled Unicode font missing: {p}; cannot render a Unicode-safe PDF"
            )
        pdfmetrics.registerFont(TTFont(name, str(p)))
    pdfmetrics.registerFontFamily(
        "NWUni", normal="NWUni", bold="NWUni-Bold",
        italic="NWUni-Italic", boldItalic="NWUni-BoldItalic",
    )
    _FONTS_REGISTERED = True


def _styles() -> dict[str, Any]:
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle

    def P(name, **kw):
        kw.setdefault("fontName", _FONT)
        return ParagraphStyle(name, **kw)

    return {
        "Title": P("nwTitle", fontName=_FONT_B, fontSize=16, leading=20, spaceAfter=3),
        "Sub": P("nwSub", fontSize=8.5, leading=11, textColor=colors.HexColor(_MUTED),
                 spaceAfter=8),
        "H2": P("nwH2", fontName=_FONT_B, fontSize=12, leading=15, spaceBefore=12,
                spaceAfter=4),
        "Body": P("nwBody", fontSize=9.5, leading=13, spaceAfter=3),
        "Narr": P("nwNarr", fontSize=10, leading=14.5, spaceAfter=6),
        "Foot": P("nwFoot", fontSize=8, leading=10.5, textColor=colors.HexColor(_MUTED)),
        "SecTitle": P("nwSecTitle", fontName=_FONT_B, fontSize=11.5, leading=14,
                      textColor=colors.white),
        "Theme": P("nwTheme", fontName=_FONT_B, fontSize=10.5, leading=14,
                   textColor=colors.white, backColor=colors.HexColor(_THEME_BG),
                   borderPadding=4, spaceBefore=8, spaceAfter=3),
        "Warn": P("nwWarn", fontName=_FONT_B, fontSize=9, leading=12,
                  textColor=colors.white, backColor=colors.HexColor(_WARN_BG),
                  borderPadding=5, spaceAfter=8),
        "Band": P("nwBand", fontSize=9.5, leading=12.5),
    }


def _section(title: str, body: list[Any], S: dict[str, Any], *, accent: str = _SECTION_ACCENT):
    """A bordered section box: accent title bar on top, `body` flowables below.

    Returns a list `[Table, Spacer]` to append to the story. The box keeps each
    section as one visually distinct 'piece'.
    """
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

    rows: list[list[Any]] = [[Paragraph(title, S["SecTitle"])]]
    rows += [[b] for b in body]
    t = Table(rows, colWidths=[_CONTENT_W])
    t.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 1.0, colors.HexColor(_BOX_BORDER)),
        ("BACKGROUND", (0, 0), (0, 0), colors.HexColor(accent)),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (0, 0), 5),
        ("BOTTOMPADDING", (0, 0), (0, 0), 5),
        ("TOPPADDING", (0, 1), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return [t, Spacer(1, 9)]


def _signal_box(inner: list[Any], *, active: bool):
    """A per-item sub-box tinted by active(hot)/quiet(passive) signal state."""
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle

    fill = _ACTIVE_FILL if active else _QUIET_FILL
    rule = _ACTIVE_RULE if active else _QUIET_RULE
    t = Table([[inner]], colWidths=[_CONTENT_W - 20])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(fill)),
        ("LINEBEFORE", (0, 0), (0, -1), 3.0, colors.HexColor(rule)),  # colored left rule
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(_BOX_BORDER)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def render_full_brief_pdf(envelope: FullBriefEnvelope, out_path: Path | str) -> Path:
    """Render a `FullBriefEnvelope` to a PDF at `out_path`. Returns the path.

    Sections mirror the text render + chatter's banded structure: header,
    executive summary + narrative, orphan-crossing highlight, theme-banded
    event synthesis, attention crossings, near-miss table, Pass F footprint,
    a reserved Prediction Markets placeholder (PolymarketDaemon fills it),
    envelope health + cost.

    Fail-loud: wrong type, structurally empty brief, missing font, or a
    zero-byte result all raise `PdfRenderError`.
    """
    if not isinstance(envelope, FullBriefEnvelope):
        raise PdfRenderError(
            f"expected FullBriefEnvelope, got {type(envelope).__name__}"
        )
    es = envelope.executive_summary
    ts = envelope.theme_synthesis
    asyn = envelope.attention_synthesis
    has_content = bool((es.narrative or "").strip()) or bool(ts.events) or bool(asyn.crossings)
    if not has_content:
        raise PdfRenderError(
            "refusing to render an empty Full Brief (no narrative, events, or crossings)"
        )

    _register_fonts()
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import (
        KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    )

    S = _styles()
    story: list[Any] = []
    sep = ' <font color="%s">&middot;</font> ' % _MUTED

    # --- header ---
    w = envelope.window
    story.append(Paragraph("News Watch &mdash; Full Brief", S["Title"]))
    story.append(Paragraph(
        f"{escape(envelope.brief_id)}{sep}window {escape(w.since)} to {escape(w.until)} "
        f"({w.duration_hours}h){sep}generated {escape(eastern_stamp(envelope.generated_at))}",
        S["Sub"],
    ))

    # --- executive summary (boxed) ---
    themes = ", ".join(escape(t) for t in es.dominant_themes) or "(none)"
    hm = (f"{es.highest_materiality_score:.2f}"
          if es.highest_materiality_score is not None else "(no events)")
    exec_body: list[Any] = [
        Paragraph(f"<b>Dominant themes:</b> {themes}", S["Body"]),
        Paragraph(
            f"<b>Material events:</b> {es.material_event_count} above threshold{sep}"
            f"<b>attention crossings:</b> {es.attention_crossings_count} "
            f"({es.orphan_crossings_count} orphan){sep}"
            f"<b>highest materiality:</b> {hm}",
            S["Body"],
        ),
    ]
    if es.narrative:
        exec_body.append(Spacer(1, 3))
        exec_body.append(Paragraph(escape(es.narrative), S["Narr"]))
    if ts.theses_doc_warning:
        exec_body.append(Paragraph(
            f"THESES DOC NOT WIRED: {escape(ts.theses_doc_warning)}", S["Warn"]
        ))
    story += _section("Executive Summary", exec_body, S)

    # --- orphan crossings (review first): the hottest items. Sorted by DISTINCT
    #     PUBLISHERS first (news industry pushing the story), then window volume;
    #     each read capped at 5 sentences for scannability. ---
    orphans = sorted(
        [c for c in asyn.crossings if c.convergence.status == "orphan"],
        key=lambda c: (-c.source_count, -c.freq_window, c.term),
    )
    if orphans:
        orphan_body: list[Any] = []
        for c in orphans:
            pubs = f"{c.source_count} pub" + ("s" if c.source_count != 1 else "")
            head = Paragraph(
                f"<b>{escape(c.term)}</b>{sep}{pubs}{sep}"
                f"{c.freq_window}/{c.freq_prior}{sep}{escape(c.shape)}",
                S["Band"],
            )
            summary = _first_sentences(c.llm_read_summary, _MAX_SENTENCES)
            inner = [head]
            if summary:
                inner.append(Paragraph(escape(summary), S["Body"]))
            orphan_body.append(_signal_box(inner, active=True))
            orphan_body.append(Spacer(1, 4))
        story += _section("Orphan Attention Crossings &mdash; review first",
                          orphan_body, S, accent=_ACTIVE_RULE)

    # --- theme segments: each an active(hot)/passive(quiet) signal box, ordered
    #     by material utility (active first, then tagged-headline volume). ---
    tseg = envelope.theme_segments
    if tseg.status != "skipped" and tseg.segments:
        seg_body: list[Any] = []
        if tseg.status == "failed" or tseg.llm_degraded:
            note = tseg.failure_reason or "summaries degraded to templates"
            seg_body.append(Paragraph(f"(degraded: {escape(note)})", S["Foot"]))
        ordered = sorted(
            tseg.segments,
            key=lambda s: (s.status != "active", -s.tagged_headline_count, s.theme_id),
        )
        for seg in ordered:
            active = seg.status == "active"
            tag = "ACTIVE" if active else "quiet"
            title_color = _SEG_ACTIVE if active else _SEG_QUIET
            flag = (
                f'{sep}<i>hot &mdash; outside Pass C scope</i>'
                if (active and not seg.in_pass_c_scope) else ""
            )
            head = (
                f'<font color="{title_color}"><b>[{tag}] {escape(seg.display_name)}</b></font> '
                f'<font color="{_MUTED}">({escape(seg.theme_id)}){sep}'
                f'{seg.tagged_headline_count} tagged</font>{flag}'
            )
            body = escape(_first_sentences(seg.summary, _SEGMENT_MAX_SENTENCES))
            if seg.convergence_terms:
                body += (
                    f'<br/><font color="{_MUTED}">attention: '
                    f'{escape(", ".join(seg.convergence_terms))}</font>'
                )
            seg_body.append(_signal_box(
                [Paragraph(head, S["Band"]), Paragraph(body, S["Body"])],
                active=active,
            ))
            seg_body.append(Spacer(1, 4))
        story += _section("Theme Segments &mdash; every tracked theme", seg_body, S)

    # --- theme-event synthesis (Pass C), banded by theme. Only events clearing
    #     the specific-headline materiality bar (>= 0.70) render as headlines. ---
    passc_body: list[Any] = []
    if ts.status == "no_trigger":
        passc_body.append(Paragraph(
            escape(ts.narrative or "Pass C trigger gate did not fire this window."),
            S["Body"],
        ))
        if ts.no_trigger_reason:
            passc_body.append(Paragraph(f"Gate reason: {escape(ts.no_trigger_reason)}", S["Foot"]))
    elif ts.status == "failed":
        passc_body.append(Paragraph(
            f"Pass C failed: {escape(ts.failure_reason or 'unknown')}", S["Body"]
        ))
    elif not ts.events:
        passc_body.append(Paragraph("(no events surfaced)", S["Body"]))
    else:
        specific = [e for e in ts.events if e.materiality_score >= _SPECIFIC_HEADLINE_MIN]
        omitted = len(ts.events) - len(specific)
        passc_body.append(Paragraph(
            f'<font color="{_MUTED}">Specific headlines shown at materiality '
            f'&ge; {_SPECIFIC_HEADLINE_MIN:.2f}.</font>', S["Foot"]))
        bands = list(ts.themes_covered) or sorted({th for e in specific for th in e.themes})
        for theme in bands:
            evs = sorted(
                [e for e in specific if theme in e.themes],
                key=lambda e: -e.materiality_score,
            )
            if not evs:
                continue
            passc_body.append(Paragraph(escape(theme), S["Theme"]))
            for e in evs:
                col = _dir_color(e.direction)
                head = (
                    f'<b>{escape(e.event_id)}</b>  '
                    f'<font color="{col}"><b>{e.materiality_score:.2f} '
                    f'{escape(e.direction or "-")}</b></font>  '
                    f'<font color="{_MUTED}">{escape(", ".join(e.themes))}'
                    f'{sep}{e.source_count} src</font>'
                )
                passc_body.append(Paragraph(head, S["Band"]))
                passc_body.append(Paragraph(escape(e.headline_summary), S["Body"]))
                passc_body.append(Spacer(1, 3))
        if not specific:
            passc_body.append(Paragraph(
                f"(no events at or above {_SPECIFIC_HEADLINE_MIN:.2f} materiality this cycle)",
                S["Body"]))
        if omitted > 0:
            passc_body.append(Paragraph(
                f"{omitted} lower-materiality event(s) omitted from specific headlines "
                f"(retained in the JSON envelope).", S["Foot"]))
        if ts.direction_tally:
            t = ts.direction_tally
            passc_body.append(Paragraph(
                f"Direction tally: {t.get('confirm', 0)} confirm{sep}"
                f"{t.get('ambiguous', 0)} ambiguous{sep}{t.get('break', 0)} break",
                S["Foot"],
            ))
        if ts.brief_id:
            passc_body.append(Paragraph(f"Pass C brief: {escape(ts.brief_id)}", S["Foot"]))
    story += _section("Theme-Event Synthesis (Pass C)", passc_body, S)

    # --- attention synthesis (Pass E, full crossing list) ---
    pe_body: list[Any] = []
    if asyn.status == "failed":
        pe_body.append(Paragraph(
            f"Pass E failed: {escape(asyn.failure_reason or 'unknown')}", S["Body"]))
    elif not asyn.crossings:
        pe_body.append(Paragraph("(no crossings this cycle)", S["Body"]))
    else:
        for c in asyn.crossings:
            if c.convergence.status == "convergent":
                conv = "converges with " + ", ".join(
                    escape(x) for x in c.convergence.converges_with)
            else:
                conv = c.convergence.status.upper()
            pe_body.append(Paragraph(
                f"<b>{escape(c.term)}</b> {c.freq_window}/{c.freq_prior} "
                f"(ratio {c.delta_ratio:.1f}){sep}{c.source_count} pub{sep}"
                f"{escape(c.shape)}{sep}{escape(conv)}",
                S["Body"],
            ))
    story += _section("Attention Synthesis (Pass E)", pe_body, S)

    # --- frequency diagnostic (near-miss table) ---
    fd = envelope.frequency_diagnostic
    fd_body: list[Any] = []
    if fd.threshold_note:
        fd_body.append(Paragraph(escape(fd.threshold_note), S["Foot"]))
    if not fd.near_misses:
        fd_body.append(Paragraph("No elevated terms in window (quiet cycle).", S["Body"]))
    else:
        rows = [["Term", "Window", "Prior", "Ratio", "Reason"]]
        for nm in fd.near_misses[:_NEARMISS_CAP]:
            rows.append([
                nm.term, str(nm.freq_window), str(nm.freq_prior),
                f"{nm.delta_ratio:.2f}", nm.reason_not_crossed,
            ])
        tbl = Table(rows, colWidths=[150, 48, 48, 48, 150], repeatRows=1)
        tbl.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), _FONT),
            ("FONTNAME", (0, 0), (-1, 0), _FONT_B),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(_HEAD_BG)),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor(_RULE)),
            ("ALIGN", (1, 0), (3, -1), "RIGHT"),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        fd_body.append(tbl)
        extra = len(fd.near_misses) - _NEARMISS_CAP
        if extra > 0:
            fd_body.append(Paragraph(
                f"... {extra} more near-miss terms in the JSON envelope.", S["Foot"]))
    story += _section("Frequency Diagnostic &mdash; near-miss terms", fd_body, S)

    # --- Pass F footprint ---
    pf = envelope.pass_f_footprint
    enabled = ", ".join(escape(t) for t in pf.attention_crossings_enabled_by_pass_f) or "(none)"
    pf_body: list[Any] = [Paragraph(
        f"Translated rows in window: {pf.translated_rows_in_window}{sep}"
        f"cross-language merges: {pf.cross_language_event_merges}{sep}"
        f"crossings enabled by Pass F: {enabled}",
        S["Body"],
    )]
    if pf.url_match_warnings is not None:
        pf_body.append(Paragraph(
            f"URL match warnings: {pf.url_match_warnings} "
            f"(source_headline URLs not matched in DB &mdash; audit signal)", S["Foot"]))
    story += _section("Pass F Footprint", pf_body, S)

    # --- prediction markets (reserved for PolymarketDaemon) ---
    pm = getattr(envelope, "prediction_markets", None)  # defensive: renders the moment it lands
    if pm:
        pm_body: list[Any] = [Paragraph(escape(str(line)), S["Body"])
                              for line in (pm if isinstance(pm, (list, tuple)) else [str(pm)])]
    else:
        pm_body = [Paragraph(
            "Reserved &mdash; PolymarketDaemon will populate event-probability "
            "context for the covered themes here.", S["Foot"])]
    story += _section("Prediction Markets", pm_body, S)

    # --- envelope health + cost ---
    h = envelope.envelope_health
    health_body: list[Any] = [Paragraph(
        f"Scrape {h.scrape.status.upper()}{sep}Pass C {h.pass_c.status.upper()}{sep}"
        f"Pass E {h.pass_e.status.upper()}{sep}"
        f"Convergence {h.convergence_analysis.status.upper()}{sep}"
        f"Frequency {h.frequency_diagnostic.status.upper()}",
        S["Body"],
    )]
    for pfail in envelope.pass_failures:
        rec = "recovered" if pfail.recovered else "NOT RECOVERED"
        health_body.append(Paragraph(
            f"FAILURE {escape(pfail.step)}: {escape(pfail.reason)} [{rec}]", S["Foot"]))
    c = envelope.cost
    health_body.append(Spacer(1, 4))
    health_body.append(Paragraph(
        f"Cost ${c.total_usd:.3f} ({escape(c.model)}, rates as of "
        f"{escape(c.rates_as_of)}){sep}{escape(envelope.brief_id)}",
        S["Foot"],
    ))
    story += _section("Envelope Health &amp; Cost", health_body, S)

    # --- build (degrade-clean) ---
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        SimpleDocTemplate(
            str(out_path), pagesize=letter,
            title=f"News Watch Full Brief {envelope.brief_id}",
            leftMargin=44, rightMargin=44, topMargin=44, bottomMargin=40,
        ).build(story)
    except Exception as exc:  # noqa: BLE001 -- surface any reportlab failure loud
        raise PdfRenderError(f"PDF build failed: {exc}") from exc
    if not out_path.is_file() or out_path.stat().st_size == 0:
        raise PdfRenderError(f"PDF render produced an empty/zero-byte file at {out_path}")
    return out_path


__all__ = ["PdfRenderError", "render_full_brief_pdf", "default_pdf_filename", "eastern_stamp"]
