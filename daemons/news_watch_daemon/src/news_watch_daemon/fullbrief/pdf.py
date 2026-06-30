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

_NEARMISS_CAP = 40  # near-miss rows in the PDF; the full list lives in the JSON
_SUMMARY_EXCERPT = 240

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
        "Theme": P("nwTheme", fontName=_FONT_B, fontSize=10.5, leading=14,
                   textColor=colors.white, backColor=colors.HexColor(_THEME_BG),
                   borderPadding=4, spaceBefore=8, spaceAfter=3),
        "Warn": P("nwWarn", fontName=_FONT_B, fontSize=9, leading=12,
                  textColor=colors.white, backColor=colors.HexColor(_WARN_BG),
                  borderPadding=5, spaceAfter=8),
        "Band": P("nwBand", fontSize=9.5, leading=12.5),
    }


def _excerpt(text: str | None, cap: int = _SUMMARY_EXCERPT) -> str:
    if not text:
        return ""
    flat = " ".join(text.split())
    return flat[:cap].rstrip() + "..." if len(flat) > cap else flat


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

    # --- executive summary ---
    themes = ", ".join(escape(t) for t in es.dominant_themes) or "(none)"
    hm = (f"{es.highest_materiality_score:.2f}"
          if es.highest_materiality_score is not None else "(no events)")
    story.append(Paragraph(f"<b>Dominant themes:</b> {themes}", S["Body"]))
    story.append(Paragraph(
        f"<b>Material events:</b> {es.material_event_count} above threshold{sep}"
        f"<b>attention crossings:</b> {es.attention_crossings_count} "
        f"({es.orphan_crossings_count} orphan){sep}"
        f"<b>highest materiality:</b> {hm}",
        S["Body"],
    ))
    if es.narrative:
        story.append(Spacer(1, 4))
        story.append(Paragraph(escape(es.narrative), S["Narr"]))
    if ts.theses_doc_warning:
        story.append(Paragraph(
            f"THESES DOC NOT WIRED: {escape(ts.theses_doc_warning)}", S["Warn"]
        ))

    # --- orphan crossings (review first) ---
    orphans = [c for c in asyn.crossings if c.convergence.status == "orphan"]
    if orphans:
        story.append(Paragraph("Orphan Attention Crossings &mdash; review first", S["H2"]))
        for c in orphans:
            story.append(Paragraph(
                f"<b>{escape(c.term)}</b> ({c.freq_window}/{c.freq_prior}, "
                f"{escape(c.shape)}) &mdash; {escape(_excerpt(c.llm_read_summary))}",
                S["Body"],
            ))

    # --- theme-event synthesis, banded by theme ---
    story.append(Paragraph("Theme-Event Synthesis (Pass C)", S["H2"]))
    if ts.status == "no_trigger":
        story.append(Paragraph(
            escape(ts.narrative or "Pass C trigger gate did not fire this window."),
            S["Body"],
        ))
        if ts.no_trigger_reason:
            story.append(Paragraph(f"Gate reason: {escape(ts.no_trigger_reason)}", S["Foot"]))
    elif ts.status == "failed":
        story.append(Paragraph(
            f"Pass C failed: {escape(ts.failure_reason or 'unknown')}", S["Body"]
        ))
    elif not ts.events:
        story.append(Paragraph("(no events surfaced)", S["Body"]))
    else:
        bands = list(ts.themes_covered) or sorted({th for e in ts.events for th in e.themes})
        for theme in bands:
            evs = sorted(
                [e for e in ts.events if theme in e.themes],
                key=lambda e: -e.materiality_score,
            )
            if not evs:
                continue
            story.append(Paragraph(escape(theme), S["Theme"]))
            for e in evs:
                col = _dir_color(e.direction)
                head = (
                    f'<b>{escape(e.event_id)}</b>  '
                    f'<font color="{col}"><b>{e.materiality_score:.2f} '
                    f'{escape(e.direction or "-")}</b></font>  '
                    f'<font color="{_MUTED}">{escape(", ".join(e.themes))}'
                    f'{sep}{e.source_count} src</font>'
                )
                story.append(KeepTogether([
                    Paragraph(head, S["Band"]),
                    Paragraph(escape(e.headline_summary), S["Body"]),
                    Spacer(1, 3),
                ]))
        if ts.direction_tally:
            t = ts.direction_tally
            story.append(Paragraph(
                f"Direction tally: {t.get('confirm', 0)} confirm{sep}"
                f"{t.get('ambiguous', 0)} ambiguous{sep}{t.get('break', 0)} break",
                S["Foot"],
            ))
        if ts.brief_id:
            story.append(Paragraph(f"Pass C brief: {escape(ts.brief_id)}", S["Foot"]))

    # --- attention synthesis ---
    story.append(Paragraph("Attention Synthesis (Pass E)", S["H2"]))
    if asyn.status == "failed":
        story.append(Paragraph(
            f"Pass E failed: {escape(asyn.failure_reason or 'unknown')}", S["Body"]
        ))
    elif not asyn.crossings:
        story.append(Paragraph("(no crossings this cycle)", S["Body"]))
    else:
        for c in asyn.crossings:
            if c.convergence.status == "convergent":
                conv = "converges with " + ", ".join(
                    escape(x) for x in c.convergence.converges_with
                )
            else:
                conv = c.convergence.status.upper()
            story.append(Paragraph(
                f"<b>{escape(c.term)}</b> {c.freq_window}/{c.freq_prior} "
                f"(ratio {c.delta_ratio:.1f}){sep}{escape(c.shape)}{sep}{escape(conv)}",
                S["Body"],
            ))

    # --- frequency diagnostic (near-miss table) ---
    fd = envelope.frequency_diagnostic
    story.append(Paragraph("Frequency Diagnostic &mdash; near-miss terms", S["H2"]))
    if fd.threshold_note:
        story.append(Paragraph(escape(fd.threshold_note), S["Foot"]))
    if not fd.near_misses:
        story.append(Paragraph("No elevated terms in window (quiet cycle).", S["Body"]))
    else:
        rows = [["Term", "Window", "Prior", "Ratio", "Reason"]]
        for nm in fd.near_misses[:_NEARMISS_CAP]:
            rows.append([
                nm.term, str(nm.freq_window), str(nm.freq_prior),
                f"{nm.delta_ratio:.2f}", nm.reason_not_crossed,
            ])
        tbl = Table(rows, colWidths=[120, 52, 52, 52, 160], repeatRows=1)
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
        story.append(tbl)
        extra = len(fd.near_misses) - _NEARMISS_CAP
        if extra > 0:
            story.append(Paragraph(
                f"... {extra} more near-miss terms in the JSON envelope.", S["Foot"]
            ))

    # --- Pass F footprint ---
    pf = envelope.pass_f_footprint
    story.append(Paragraph("Pass F Footprint", S["H2"]))
    enabled = ", ".join(escape(t) for t in pf.attention_crossings_enabled_by_pass_f) or "(none)"
    story.append(Paragraph(
        f"Translated rows in window: {pf.translated_rows_in_window}{sep}"
        f"cross-language merges: {pf.cross_language_event_merges}{sep}"
        f"crossings enabled by Pass F: {enabled}",
        S["Body"],
    ))
    if pf.url_match_warnings is not None:
        story.append(Paragraph(
            f"URL match warnings: {pf.url_match_warnings} "
            f"(source_headline URLs not matched in DB &mdash; audit signal)",
            S["Foot"],
        ))

    # --- prediction markets (reserved for PolymarketDaemon) ---
    story.append(Paragraph("Prediction Markets", S["H2"]))
    pm = getattr(envelope, "prediction_markets", None)  # defensive: renders the moment it lands
    if pm:
        for line in (pm if isinstance(pm, (list, tuple)) else [str(pm)]):
            story.append(Paragraph(escape(str(line)), S["Body"]))
    else:
        story.append(Paragraph(
            "Reserved &mdash; PolymarketDaemon will populate event-probability "
            "context for the covered themes here.",
            S["Foot"],
        ))

    # --- envelope health + cost ---
    h = envelope.envelope_health
    story.append(Paragraph("Envelope Health", S["H2"]))
    story.append(Paragraph(
        f"Scrape {h.scrape.status.upper()}{sep}Pass C {h.pass_c.status.upper()}{sep}"
        f"Pass E {h.pass_e.status.upper()}{sep}"
        f"Convergence {h.convergence_analysis.status.upper()}{sep}"
        f"Frequency {h.frequency_diagnostic.status.upper()}",
        S["Body"],
    ))
    for pfail in envelope.pass_failures:
        rec = "recovered" if pfail.recovered else "NOT RECOVERED"
        story.append(Paragraph(
            f"FAILURE {escape(pfail.step)}: {escape(pfail.reason)} [{rec}]", S["Foot"]
        ))
    c = envelope.cost
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"Cost ${c.total_usd:.3f} ({escape(c.model)}, rates as of "
        f"{escape(c.rates_as_of)}){sep}{escape(envelope.brief_id)}",
        S["Foot"],
    ))

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
