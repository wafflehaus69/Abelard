"""Generic ReportLab Platypus toolkit (fonts, styles, boxes, fail-loud builder).

Hoisted from ``news_watch_daemon/fullbrief/pdf.py``; the news-watch-specific
document (its sections) stays there. This module carries only reusable mechanics:

  register_unicode_fonts() — register ReportLab's bundled Bitstream Vera family as
    the Unicode face (Latin + Latin-Extended; bundled with reportlab, so portable
    to the mini with no system-font dependency). Idempotent, fail-loud on a missing
    font. Raw Cyrillic/CJK would need a DejaVu swap — noted, not carried here.
  default_styles() — a ParagraphStyle set (title, headings, body, footnote, section
    title-bar, band, warn) over the registered Unicode face.
  section_box() / signal_box() — the bordered-section and tinted-signal-box layout.
  build_pdf() — SimpleDocTemplate build that fails loud (PdfRenderError) on an empty
    story or a zero-byte result — never a silent empty PDF.
  eastern_stamp() / first_sentences() — small text helpers.

ReportLab is lazy-imported (heavy dep, contained) so importing abelard_common does
not pull it in; a consuming daemon declares reportlab as its own dependency.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..errors import DaemonError


class PdfRenderError(DaemonError):
    """A PDF could not be rendered — missing bundled font, structurally empty
    story, or zero-byte output. Fail-loud, never a silent empty PDF."""

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="render.pdf")


# --- palette (words carry meaning too, so it survives grayscale) ---
MUTED = "#666666"
RULE = "#bbbbbb"
HEAD_BG = "#e8eaf0"
BOX_BORDER = "#c9ced6"
SECTION_ACCENT = "#2c3e50"       # section title-bar (dark slate), white text
ACTIVE_FILL = "#fdecea"          # light red tint (hot/active)
ACTIVE_RULE = "#b00020"
QUIET_FILL = "#f2f3f5"           # light cool gray (quiet/passive)
QUIET_RULE = "#9aa0a8"

# Letter (612pt) minus 44pt L/R margins — the default content width used by the
# box helpers. Pass content_width to override if a consumer uses other margins.
CONTENT_W = 612 - 44 - 44

_FONT = "ACUni"
_FONT_B = "ACUni-Bold"
_FONT_I = "ACUni-Italic"
_FONT_BI = "ACUni-BoldItalic"
_FONTS_REGISTERED = False

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+")
_ABBREVS = sorted(
    (
        "U.S.A.", "U.S.", "U.K.", "U.N.", "E.U.", "U.A.E.", "D.C.",
        "a.m.", "p.m.", "vs.", "etc.", "Inc.", "Corp.", "Ltd.", "Co.",
        "Mr.", "Ms.", "Mrs.", "Dr.", "Sen.", "Rep.", "Gov.", "Gen.",
        "Lt.", "Col.", "Sgt.", "St.", "Jr.", "Sr.",
    ),
    key=len, reverse=True,
)
_ABBR_MASK = "\x00"


def register_unicode_fonts() -> tuple[str, str]:
    """Register ReportLab's bundled Vera family as the Unicode face. Idempotent
    within a process; fails loud if the bundled fonts are absent. Returns the
    (regular, bold) font names for use in styles."""
    global _FONTS_REGISTERED
    if _FONTS_REGISTERED:
        return _FONT, _FONT_B
    import reportlab
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    fdir = Path(reportlab.__file__).resolve().parent / "fonts"
    faces = [
        (_FONT, "Vera.ttf"),
        (_FONT_B, "VeraBd.ttf"),
        (_FONT_I, "VeraIt.ttf"),
        (_FONT_BI, "VeraBI.ttf"),
    ]
    for name, fn in faces:
        p = fdir / fn
        if not p.is_file():
            raise PdfRenderError(
                f"bundled Unicode font missing: {p}; cannot render a Unicode-safe PDF"
            )
        pdfmetrics.registerFont(TTFont(name, str(p)))
    pdfmetrics.registerFontFamily(
        _FONT, normal=_FONT, bold=_FONT_B, italic=_FONT_I, boldItalic=_FONT_BI,
    )
    _FONTS_REGISTERED = True
    return _FONT, _FONT_B


def default_styles() -> dict[str, Any]:
    """A ParagraphStyle set over the registered Unicode face. Registers the fonts
    if not already done, so a consumer can call this directly."""
    register_unicode_fonts()
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle

    def P(name, **kw):
        kw.setdefault("fontName", _FONT)
        return ParagraphStyle(name, **kw)

    return {
        "Title": P("acTitle", fontName=_FONT_B, fontSize=16, leading=20, spaceAfter=3),
        "Sub": P("acSub", fontSize=8.5, leading=11, textColor=colors.HexColor(MUTED), spaceAfter=8),
        "H2": P("acH2", fontName=_FONT_B, fontSize=12, leading=15, spaceBefore=12, spaceAfter=4),
        "Body": P("acBody", fontSize=9.5, leading=13, spaceAfter=3),
        "Narr": P("acNarr", fontSize=10, leading=14.5, spaceAfter=6),
        "Foot": P("acFoot", fontSize=8, leading=10.5, textColor=colors.HexColor(MUTED)),
        "SecTitle": P("acSecTitle", fontName=_FONT_B, fontSize=11.5, leading=14, textColor=colors.white),
        "Band": P("acBand", fontSize=9.5, leading=12.5),
        "Warn": P("acWarn", fontName=_FONT_B, fontSize=9, leading=12, textColor=colors.white,
                  backColor=colors.HexColor(ACTIVE_RULE), borderPadding=5, spaceAfter=8),
    }


def section_box(title: str, body: list[Any], styles: dict[str, Any], *,
                content_width: float = CONTENT_W, accent: str = SECTION_ACCENT) -> list[Any]:
    """A bordered section: accent title bar on top, `body` flowables below. Returns
    [Table, Spacer] to extend the story with."""
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

    rows: list[list[Any]] = [[Paragraph(title, styles["SecTitle"])]]
    rows += [[b] for b in body]
    t = Table(rows, colWidths=[content_width])
    t.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 1.0, colors.HexColor(BOX_BORDER)),
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


def signal_box(inner: list[Any], *, content_width: float = CONTENT_W, active: bool) -> Any:
    """A per-item sub-box tinted by active(hot)/quiet(passive) signal state."""
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle

    fill = ACTIVE_FILL if active else QUIET_FILL
    rule = ACTIVE_RULE if active else QUIET_RULE
    t = Table([[inner]], colWidths=[content_width - 20])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(fill)),
        ("LINEBEFORE", (0, 0), (0, -1), 3.0, colors.HexColor(rule)),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(BOX_BORDER)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    return t


def build_pdf(out_path: Path | str, story: list[Any], *, title: str,
              left_margin: float = 44, right_margin: float = 44,
              top_margin: float = 44, bottom_margin: float = 40) -> Path:
    """Build `story` into a PDF at `out_path`. Fail-loud (PdfRenderError) on an
    empty story or a zero-byte result — never a silent empty PDF. Returns the path."""
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate

    if not story:
        raise PdfRenderError("refusing to render an empty story (no flowables)")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        SimpleDocTemplate(
            str(out_path), pagesize=letter, title=title,
            leftMargin=left_margin, rightMargin=right_margin,
            topMargin=top_margin, bottomMargin=bottom_margin,
        ).build(story)
    except Exception as exc:  # noqa: BLE001 — surface any reportlab failure loud
        raise PdfRenderError(f"PDF build failed: {exc}") from exc
    if not out_path.is_file() or out_path.stat().st_size == 0:
        raise PdfRenderError(f"PDF render produced an empty/zero-byte file at {out_path}")
    return out_path


def eastern_stamp(iso_utc: str) -> str:
    """UTC ISO-8601 (Z) -> 'MM-DD-YYYY HH:MM TZ' in US Eastern. Malformed input
    returns raw, never crashes."""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        east = dt.astimezone(ZoneInfo("America/New_York"))
        return f"{east:%m-%d-%Y %H:%M} {east.tzname()}"
    except (ValueError, TypeError):
        return iso_utc


def first_sentences(text: str | None, n: int = 5) -> str:
    """First `n` sentences of `text`, whitespace-collapsed; whole text when it has
    <= n sentences. Common abbreviations shielded so the cut never lands mid-abbrev."""
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


__all__ = [
    "PdfRenderError", "register_unicode_fonts", "default_styles",
    "section_box", "signal_box", "build_pdf", "eastern_stamp", "first_sentences",
    "CONTENT_W", "MUTED", "SECTION_ACCENT", "ACTIVE_RULE",
]
