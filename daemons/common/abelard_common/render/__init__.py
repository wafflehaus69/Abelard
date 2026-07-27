"""abelard_common.render — shared PDF render toolkit for the OpenClaw daemons.

Generic ReportLab Platypus primitives, hoisted from the news_watch full-brief PDF
(pattern source ``news_watch_daemon/fullbrief/pdf.py``) so a third consumer
(smart_money) builds its brief on one implementation rather than a fourth copy.

This package provides the MECHANICS only — Unicode-safe font registration,
a default style set, boxed-section and signal-box layout, a fail-loud document
builder, and small text helpers. Each consuming daemon composes its OWN sections
(its envelope shape is its own); nothing here is envelope-specific.

Fail-loud contract: ``PdfRenderError`` (a ``DaemonError`` subclass) is raised on a
missing bundled font, a structurally empty story, or a zero-byte result — never a
silent empty PDF.
"""

from __future__ import annotations

from .pdf import (
    PdfRenderError,
    build_pdf,
    default_styles,
    eastern_stamp,
    first_sentences,
    register_unicode_fonts,
    section_box,
    signal_box,
)

__all__ = [
    "PdfRenderError",
    "build_pdf",
    "default_styles",
    "eastern_stamp",
    "first_sentences",
    "register_unicode_fonts",
    "section_box",
    "signal_box",
]
