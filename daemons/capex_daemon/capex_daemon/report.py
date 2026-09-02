"""The whole dashboard as one PDF — all seven views, paginated, portable.

The dashboard is read-only, bound to Tailscale, and lives on BASILIC. That is
the right home for something you interrogate, and the wrong one for something
you hand to someone, read on a plane, or keep beside a quarter's filings. This
renders the same seven views into a single document.

**It is a second renderer over the same model, not a second computation.** Every
number here is read from the persisted snapshot, and the arithmetic that turns a
number into a string is `dashboard`'s own — `_money`, `_pct`, `_spark`,
`_provenance`, `STATE_COLORS` are imported rather than reimplemented, so the PDF
cannot round, threshold, or withhold differently from the page it mirrors. Chart
geometry is `svgcharts.Frame` with the y-axis flipped into ReportLab's
bottom-left origin, which is the same trick `brief.composite_drawing` already
uses: one frame, two coordinate systems, points that land in the same place.

**Landscape, and not as a preference.** The dashboard's charts are 1160–1240pt
wide and its issuer table has ten columns. On portrait Letter (524pt of content)
both get cut in half or shrunk past reading. Landscape gives 720pt, which is
what these views were laid out for.

**What print cannot carry, this carries differently.** Three things on the HTML
page live in `title=` hover attributes — matched membership, per-issuer
provenance, supplier restatement detail — and hover does not survive paper. They
are not dropped: they become a Provenance appendix and per-table footnotes. The
unicode sparkline blocks (▁▂▃) are absent from the bundled Vera face and would
render as tofu, so sparklines are drawn as real micro bar charts instead.

**Known drift risk, stated rather than hidden.** The explanatory prose in each
section is duplicated from `dashboard.py`, where it lives inline in the view
functions. Numbers cannot drift — they share the formatters — but wording can.
The honest fix is to lift the narrative into a module both renderers read; it is
not done here because it would rewrite seven deployed view functions for a
change that is not what was asked for. `tests/test_report.py` pins the numeric
parity that matters; the wording is a follow-up.
"""
import socket
import time

from . import brief, dashboard, phases, snapshot, svgcharts, trend

# The box the dashboard actually serves from. A render produced anywhere else is
# a copy of a copy: the snapshot was pulled at some moment and cannot know what
# happened after. Saying so on the face of the document is cheaper than a reader
# discovering it from a figure that will not reconcile.
LIVE_HOST = "basilic"

# Landscape Letter, house margins tightened a little because the content is wide.
PAGE_SIZE = (792.0, 612.0)
MARGIN_L = MARGIN_R = 36.0
MARGIN_T, MARGIN_B = 40.0, 34.0
CONTENT_W = PAGE_SIZE[0] - MARGIN_L - MARGIN_R          # 720
CHART_W = CONTENT_W
FRAME_H = PAGE_SIZE[1] - MARGIN_T - MARGIN_B            # 538

# Tables use the dashboard's formatter; chart axes use the chart layer's, which
# carries one decimal on billions rather than two. That split is the dashboard's
# own — matching it is the point, so an axis tick reads identically in both.
_money = dashboard._money
_axis_money = svgcharts._money
_pct = dashboard._pct
STATE_COLORS = dashboard.STATE_COLORS

MUTED = "#666666"
RULE = "#c9ced6"
HEAD_BG = "#e8eaf0"
WARN_FILL = "#fff4f4"
WARN_RULE = "#9b1c1c"
COV_FILL = "#fdf6e3"
COV_RULE = "#8a6d1a"
MAP_FILL = "#eef3fb"
MAP_RULE = "#1f4e9c"


def _hex(c):
    from reportlab.lib import colors
    return colors.HexColor(c)


def _x(s):
    """XML-escape for Paragraph markup. Paragraphs accept a tiny HTML subset, so
    only the three structural characters are escaped and <b>/<i> stay usable."""
    return (str("" if s is None else s)
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ---------------- geometry: svgcharts.Frame, flipped ----------------

class PdfFrame:
    """`svgcharts.Frame` in ReportLab coordinates.

    SVG's origin is top-left and ReportLab's is bottom-left, so the only
    difference between the two renderers is one subtraction. Everything that
    decides WHERE a point goes — the log transform, the tick choice, the
    categorical x placement — stays in Frame, shared. A second implementation of
    any of that is a second chance to disagree with the dashboard about where a
    line is, which is the same class of defect as disagreeing about its value.
    """

    def __init__(self, quarters, lo, hi, width, height, left, right, top, bottom,
                 log=False):
        self.f = svgcharts.Frame(quarters, lo, hi, width=width, height=height,
                                 left=left, right=right, top=top, bottom=bottom,
                                 log=log)
        self.w, self.h = width, height

    @property
    def quarters(self):
        return self.f.quarters

    @property
    def l(self):
        return self.f.l

    @property
    def pw(self):
        return self.f.pw

    @property
    def ph(self):
        return self.f.ph

    @property
    def lo(self):
        return self.f.lo

    @property
    def hi(self):
        return self.f.hi

    def base_y(self):
        """PDF y of the plot floor."""
        return self.h - self.f.t - self.f.ph

    def top_y(self):
        return self.h - self.f.t

    def x(self, q):
        return self.f.x(q)

    def y(self, v):
        return self.h - self.f.y(v)

    def step(self):
        return self.f.step()

    def ticks(self, n=5):
        return self.f.ticks(n)

    def has(self, q):
        return q in self.f.index


# ---------------- chart primitives ----------------

def _bands(d, pf, states, alpha=svgcharts.BAND_ALPHA):
    from reportlab.graphics.shapes import Rect
    half = pf.step() / 2.0
    for q in pf.quarters:
        fill = svgcharts.BAND_FILLS.get(states.get(q))
        if not fill:
            continue
        x0 = max(pf.l, pf.x(q) - half)
        x1 = min(pf.l + pf.pw, pf.x(q) + half)
        d.add(Rect(x0, pf.base_y(), max(0.0, x1 - x0), pf.ph, fillColor=_hex(fill),
                   fillOpacity=alpha, strokeColor=None))


def _gridlines(d, pf, fmt=_axis_money, ticks=5, size=6.0, font="Helvetica"):
    from reportlab.graphics.shapes import Line, String
    for v in pf.ticks(ticks):
        y = pf.y(v)
        if not (pf.base_y() - 1 <= y <= pf.top_y() + 1):
            continue
        d.add(Line(pf.l, y, pf.l + pf.pw, y, strokeColor=_hex("#dcdcdc"),
                   strokeWidth=0.4))
        d.add(String(pf.l - 5, y - size * 0.34, fmt(v), fontSize=size, fontName=font,
                     fillColor=_hex("#777777"), textAnchor="end"))


def _quarter_axis(d, pf, size=6.0, font="Helvetica", every=None):
    """Quarter labels, with the last one guaranteed and never overprinted.

    `svgcharts.quarter_axis` draws every Nth quarter AND the last one
    unconditionally, so whenever the series length is not a multiple of N the
    final two labels land on top of each other — "2026Q1" and "2026Q2" render as
    "20226Q2" on the live dashboard's Leg 1 and on its YoY charts. The last
    quarter is the one a reader most wants, so it wins and the neighbour that
    would collide with it is dropped.
    """
    from reportlab.graphics.shapes import Line, String
    qs = pf.quarters
    n = len(qs)
    every = every or max(1, n // 14)
    d.add(Line(pf.l, pf.base_y(), pf.l + pf.pw, pf.base_y(),
               strokeColor=_hex("#bbbbbb"), strokeWidth=0.5))
    # ~"2026Q2" at this size, plus air. Shared pick logic with the SVG axis and
    # the phase grid, all three of which had the same overprint.
    picks = svgcharts.label_picks(n, every, lambda i: pf.x(qs[i]), size * 4.2)
    for i in picks:
        d.add(String(pf.x(qs[i]), pf.base_y() - size - 3, qs[i], fontSize=size,
                     fontName=font, fillColor=_hex("#666666"), textAnchor="middle"))


def _polyline(d, pf, rows, color, width=1.4, dash=None, opacity=1.0):
    from reportlab.graphics.shapes import PolyLine
    pts = []
    for r in rows:
        if r[1] is None or not pf.has(r[0]):
            continue
        pts += [pf.x(r[0]), pf.y(r[1])]
    if len(pts) < 4:
        return
    pl = PolyLine(pts, strokeColor=_hex(color), strokeWidth=width,
                  strokeOpacity=opacity, strokeLineJoin=1)
    if dash:
        pl.strokeDashArray = dash
    d.add(pl)


def _area(d, pf, rows, color, opacity=0.10):
    from reportlab.graphics.shapes import Polygon
    pairs = [(r[0], r[1]) for r in rows if r[1] is not None and pf.has(r[0])]
    if len(pairs) < 2:
        return
    base = pf.y(max(pf.lo, 0.0))
    pts = [pf.x(pairs[0][0]), base]
    for q, v in pairs:
        pts += [pf.x(q), pf.y(v)]
    pts += [pf.x(pairs[-1][0]), base]
    d.add(Polygon(pts, fillColor=_hex(color), fillOpacity=opacity, strokeColor=None))


def _dots(d, pf, rows, color, r=1.5):
    from reportlab.graphics.shapes import Circle
    for row in rows:
        if row[1] is None or not pf.has(row[0]):
            continue
        d.add(Circle(pf.x(row[0]), pf.y(row[1]), r, fillColor=_hex(color),
                     strokeColor=None))


def _zero_line(d, pf):
    from reportlab.graphics.shapes import Line
    if not (pf.lo < 0 < pf.hi):
        return
    y = pf.y(0.0)
    ln = Line(pf.l, y, pf.l + pf.pw, y, strokeColor=_hex("#999999"), strokeWidth=0.6)
    ln.strokeDashArray = [3, 2]
    d.add(ln)


def _chart_title(d, pf, text, note=None, size=8.0, font="Helvetica-Bold"):
    from reportlab.graphics.shapes import String
    d.add(String(pf.l, pf.h - 13, text, fontSize=size, fontName=font,
                 fillColor=_hex("#333333")))
    if note:
        d.add(String(pf.l + 300, pf.h - 13, note, fontSize=size - 1.2,
                     fontName="Helvetica", fillColor=_hex(COV_RULE)))


def _place_labels(d, items, min_gap=8.0, top=None, bottom=None, size=7.0):
    """Right-edge series labels, nudged apart. Mirrors `svgcharts.place_labels`
    with the sign flipped for PDF coordinates: 'push down' is decreasing y."""
    from reportlab.graphics.shapes import Line, String
    items = sorted(items, reverse=True)                 # topmost first
    ys = [y for y, _x, _t, _c in items]
    for i in range(1, len(ys)):
        ys[i] = min(ys[i], ys[i - 1] - min_gap)
    if bottom is not None and ys and ys[-1] < bottom:
        shift = bottom - ys[-1]
        ys = [y + shift for y in ys]
        for i in range(len(ys) - 2, -1, -1):
            ys[i] = max(ys[i], ys[i + 1] + min_gap)
    if top is not None and ys:
        ys[0] = min(ys[0], top)
        for i in range(1, len(ys)):
            ys[i] = min(ys[i], ys[i - 1] - min_gap)
    for (orig_y, x, text, color), y in zip(items, ys):
        if abs(y - orig_y) > 2.0:
            d.add(Line(x - 5, orig_y, x - 1, y + size * 0.3, strokeColor=_hex(color),
                       strokeWidth=0.4, strokeOpacity=0.45))
        d.add(String(x, y, text, fontSize=size, fontName="Helvetica-Bold",
                     fillColor=_hex(color)))


def _refusal_in_chart(d, pf, text, size=6.6):
    """A refused leg, printed where the leg would have been. A missing series and
    a refused series look identical unless the chart says otherwise."""
    from reportlab.graphics.shapes import Rect, String
    y = pf.base_y() + 8
    d.add(Rect(pf.l + 2, y, pf.pw - 6, 17, fillColor=_hex(COV_FILL),
               strokeColor=_hex("#e6d9a8"), strokeWidth=0.5))
    d.add(String(pf.l + 9, y + 5.5, "! " + text, fontSize=size, fontName="Helvetica",
                 fillColor=_hex(COV_RULE)))


def empty_drawing(msg, width=CHART_W, height=44):
    from reportlab.graphics.shapes import Drawing, String
    d = Drawing(width, height)
    d.add(String(width / 2.0, height / 2.0 - 3, msg, fontSize=7.5,
                 fontName="Helvetica", fillColor=_hex("#999999"), textAnchor="middle"))
    return d


# ---------------- composed charts ----------------

def level_drawing(rows, title, color, observations=None, fmt=_axis_money,
                  width=CHART_W, height=150):
    """A dollar-level line with optional phase shading. Mirrors
    `svgcharts.level_chart`, including its axis bounds and 10% headroom."""
    from reportlab.graphics.shapes import Drawing
    rows = [r for r in rows if r[1] is not None]
    if len(rows) < 2:
        return empty_drawing("{}: fewer than two observations".format(title), width)
    vals = [r[1] for r in rows]
    d = Drawing(width, height)
    pf = PdfFrame([r[0] for r in rows], min(0.0, min(vals)), max(vals) * 1.10,
                  width, height, left=62, right=26, top=22, bottom=26)
    _chart_title(d, pf, title)
    if observations:
        _bands(d, pf, svgcharts._states_of(observations))
    _gridlines(d, pf, fmt=fmt)
    _quarter_axis(d, pf)
    _area(d, pf, rows, color)
    _polyline(d, pf, rows, color, width=1.5)
    _dots(d, pf, rows, color, r=1.4)
    return d


def yoy_drawing(snap_series, observations, title, color, band=None,
                width=CHART_W, height=150):
    """A TTM-YoY line on its phase shading. Mirrors `svgcharts.yoy_chart`."""
    from reportlab.graphics.shapes import Drawing
    rows = [(r["q"], r["yoy"]) for r in (snap_series or []) if r.get("yoy") is not None]
    if len(rows) < 2:
        return empty_drawing("{}: fewer than two observations".format(title), width)
    vals = [v for _, v in rows]
    lo, hi = min(vals + [0.0]), max(vals)
    pad = (hi - lo) * 0.12 or 0.05
    d = Drawing(width, height)
    pf = PdfFrame([q for q, _ in rows], lo - pad, hi + pad, width, height,
                  left=62, right=26, top=22, bottom=26)
    _chart_title(d, pf, title,
                 note=("dead-band {}pp".format(band) if band else None))
    _bands(d, pf, svgcharts._states_of(observations))
    _gridlines(d, pf, fmt=lambda v: "{:+.0f}%".format(100 * v))
    _zero_line(d, pf)
    _quarter_axis(d, pf)
    _polyline(d, pf, rows, color, width=1.6)
    _dots(d, pf, rows, color, r=1.4)
    return d


def multi_line_drawing(series_by_name, title, note=None, fmt=_axis_money,
                       width=CHART_W, height=210):
    """One line per issuer, no aggregation. Mirrors `svgcharts.multi_line_chart`,
    dashed gaps included — a dashed segment spans quarters with no disclosure and
    is not a flat stretch."""
    from reportlab.graphics.shapes import Drawing
    series_by_name = {k: v for k, v in series_by_name.items() if len(v) >= 2}
    if not series_by_name:
        return empty_drawing("{}: nothing with two or more observations".format(title),
                             width)
    quarters = sorted({q for v in series_by_name.values() for q, _ in v},
                      key=trend._cq_sort)
    vals = [x for v in series_by_name.values() for _, x in v]
    d = Drawing(width, height)
    pf = PdfFrame(quarters, min(0.0, min(vals)), max(vals) * 1.10, width, height,
                  left=64, right=74, top=22, bottom=26)
    _chart_title(d, pf, title, note=note)
    _gridlines(d, pf, fmt=fmt)
    _quarter_axis(d, pf)
    labels = []
    ordered = sorted(series_by_name.items(), key=lambda kv: -kv[1][-1][1])
    for i, (name, pts) in enumerate(ordered):
        c = svgcharts.MULTI_PALETTE[i % len(svgcharts.MULTI_PALETTE)]
        _polyline(d, pf, pts, c, width=1.2,
                  dash=[4, 2.5] if svgcharts._has_gap(pts) else None)
        _dots(d, pf, pts, c, r=1.1)
        labels.append((pf.y(pts[-1][1]), pf.x(pts[-1][0]) + 5, name, c))
    _place_labels(d, labels, min_gap=7.5, top=pf.top_y() - 4,
                  bottom=pf.base_y() + 2, size=6.4)
    return d


def state_grid_drawing(rows, quarters, width=CHART_W, label_w=96, cell=None):
    """The phase board as a coloured grid — one row per series, one cell per
    quarter. A blank cell is INSUFFICIENT-HISTORY, deliberately uncoloured so
    that absence of a state does not read as a fifth state."""
    from reportlab.graphics.shapes import Drawing, Rect, String
    if not rows or not quarters:
        return empty_drawing("no classified history", width)
    cell = cell or min(11.0, (width - label_w - 4) / max(1, len(quarters)))
    idx = {q: i for i, q in enumerate(quarters)}
    body_h = len(rows) * cell
    height = 20 + body_h + 20
    d = Drawing(width, height)
    every = max(1, len(quarters) // 12)

    def _gx(i):
        return label_w + i * cell + cell / 2.0

    for i in svgcharts.label_picks(len(quarters), every, _gx, cell * 2.6):
        d.add(String(_gx(i), height - 12, quarters[i], fontSize=5.4,
                     fontName="Helvetica", fillColor=_hex("#777777"),
                     textAnchor="middle"))
    for r, (key, states, _sub) in enumerate(rows):
        y = height - 20 - (r + 1) * cell
        d.add(String(label_w - 5, y + cell * 0.28, key, fontSize=5.8,
                     fontName="Helvetica", fillColor=_hex("#333333"), textAnchor="end"))
        for q, i in idx.items():
            st = states.get(q)
            if not st:
                continue
            d.add(Rect(label_w + i * cell, y, max(0.6, cell - 0.8), cell - 1.0,
                       fillColor=_hex(svgcharts.BAND_FILLS.get(st, "#d5d5d5")),
                       fillOpacity=0.82, strokeColor=_hex("#ffffff"), strokeWidth=0.3))
    for i, st in enumerate((phases.STATE_ACCELERATING, phases.STATE_PLATEAU,
                            phases.STATE_DECELERATING, phases.STATE_CONTRACTING)):
        cx = label_w + i * 116
        d.add(Rect(cx, 4, 11, 8, fillColor=_hex(svgcharts.BAND_FILLS[st]),
                   fillOpacity=svgcharts.BAND_ALPHA * 3, strokeColor=None))
        d.add(String(cx + 15, 5.5, st, fontSize=6.0, fontName="Helvetica",
                     fillColor=_hex("#555555")))
    return d


def spark_drawing(vals, width=52, height=9, color="#444444"):
    """A sparkline as real bars.

    `dashboard._spark` draws with the unicode block characters. Those are absent
    from the bundled Vera face and would render as tofu on paper, so the same
    window (last 24) and the same min-max normalisation are drawn as rectangles.
    The only difference is that bar height is continuous where the block glyphs
    quantise to eight levels — the shape is the same, at more resolution.
    """
    from reportlab.graphics.shapes import Drawing, Rect
    vals = [v for v in (vals or []) if v is not None][-24:]
    d = Drawing(width, height)
    if not vals:
        return d
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1.0
    bw = width / float(len(vals))
    for i, v in enumerate(vals):
        h = 0.8 + (height - 0.8) * (v - lo) / rng
        d.add(Rect(i * bw, 0, max(0.5, bw - 0.35), h, fillColor=_hex(color),
                   strokeColor=None))
    return d


# ---------------- text and table helpers ----------------

def _P(text, style):
    from reportlab.platypus import Paragraph
    return Paragraph(text, style)


def _state_text(state, flags=None):
    """A state as coloured words. The colour is redundant, never load-bearing —
    the house palette's own rule is that words carry meaning so it survives
    grayscale, and paper is where that gets tested."""
    c = STATE_COLORS.get(state, "#777777")
    out = "<font color='{}'><b>{}</b></font>".format(c, _x(state))
    for f in (flags or []):
        out += " <font size='5.6' color='#666666'>{}</font>".format(_x(f))
    return out


def _table(header, rows, col_widths, styles, right_cols=(), font_size=6.6,
           repeat=True, head_bg=HEAD_BG):
    """A dense table. Long ones go into the story directly with the header
    repeated — never nested inside `section_box`, which cannot split and which
    is how the 30-row issuer table overflowed the frame the first time
    (`brief._state_table`)."""
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle
    hs, bs, bsr = styles["_th"], styles["_td"], styles["_tdr"]
    hsr = styles["_thr"]
    right = set(right_cols)
    data = [[_P("<b>{}</b>".format(_x(h)), hsr if i in right else hs)
             for i, h in enumerate(header)]]
    for r in rows:
        # A right-aligned TableStyle ALIGN does nothing to a Paragraph — the
        # paragraph fills the cell and aligns inside itself. The alignment has to
        # live on the style, which is why the numeric columns get their own.
        data.append([c if hasattr(c, "wrapOn") else _P(c, bsr if i in right else bs)
                     for i, c in enumerate(r)])
    t = Table(data, colWidths=col_widths, repeatRows=1 if repeat else 0)
    st = [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(head_bg)),
          ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.HexColor("#aab0b8")),
          ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#dfe3e8")),
          ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
          ("FONTSIZE", (0, 0), (-1, -1), font_size),
          ("LEFTPADDING", (0, 0), (-1, -1), 3.5),
          ("RIGHTPADDING", (0, 0), (-1, -1), 3.5),
          ("TOPPADDING", (0, 0), (-1, -1), 1.8),
          ("BOTTOMPADDING", (0, 0), (-1, -1), 1.8),
          ("ROWBACKGROUNDS", (0, 1), (-1, -1),
           [colors.white, colors.HexColor("#f7f8fa")])]
    for c in right_cols:
        st.append(("ALIGN", (c, 1), (c, -1), "RIGHT"))
    t.setStyle(TableStyle(st))
    return t


def _tinted(text, styles, fill, rule):
    """A warn / mapped / coverage block — the HTML's tinted callouts."""
    from reportlab.lib import colors
    from reportlab.platypus import Table, TableStyle
    t = Table([[_P(text, styles["_note"])]], colWidths=[CONTENT_W])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(fill)),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, colors.HexColor(rule)),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return t


def _warn(text, styles):
    return _tinted(text, styles, WARN_FILL, WARN_RULE)


def _styles():
    from abelard_common.render import pdf as housepdf
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_RIGHT
    from reportlab.lib.styles import ParagraphStyle
    reg, bold = housepdf.register_unicode_fonts()
    st = housepdf.default_styles()
    st["_font"], st["_font_b"] = reg, bold
    st["_h1"] = ParagraphStyle("cxH1", fontName=bold, fontSize=15, leading=18,
                               spaceAfter=2)
    st["_h2"] = ParagraphStyle("cxH2", fontName=bold, fontSize=11, leading=14,
                               spaceBefore=10, spaceAfter=4)
    st["_h3"] = ParagraphStyle("cxH3", fontName=bold, fontSize=9, leading=12,
                               spaceBefore=8, spaceAfter=3)
    st["_note"] = ParagraphStyle("cxNote", fontName=reg, fontSize=7.6, leading=10.4,
                                 spaceAfter=5, textColor=colors.HexColor("#333333"))
    st["_chartnote"] = ParagraphStyle("cxCN", fontName=reg, fontSize=6.8, leading=9.2,
                                      spaceBefore=1, spaceAfter=7,
                                      textColor=colors.HexColor("#777777"))
    st["_th"] = ParagraphStyle("cxTh", fontName=bold, fontSize=6.4, leading=8.2,
                               textColor=colors.HexColor("#333333"))
    st["_thr"] = ParagraphStyle("cxThR", parent=st["_th"], alignment=TA_RIGHT)
    st["_td"] = ParagraphStyle("cxTd", fontName=reg, fontSize=6.6, leading=8.4)
    st["_tdr"] = ParagraphStyle("cxTdR", parent=st["_td"], alignment=TA_RIGHT)
    st["_foot"] = ParagraphStyle("cxFoot", fontName=reg, fontSize=6.4, leading=8.6,
                                 textColor=colors.HexColor(MUTED), spaceBefore=2,
                                 spaceAfter=6)
    return st


def _spacer(h=6):
    from reportlab.platypus import Spacer
    return Spacer(1, h)


def _cov_tags(coverage, drop_ok=True):
    tags = [c for c in (coverage or []) if not (drop_ok and c == "OK")]
    if not tags:
        return ""
    return " ".join("<font size='5.4' color='{}'>{}</font>".format(COV_RULE, _x(c))
                    for c in tags)


def _band(v):
    """A dead-band, or a dash. `"{}pp".format(None)` prints "Nonepp", which reads
    as a value and is not one — every name with no band is a name the classifier
    never reached."""
    return "—" if v is None else "{}pp".format(v)


def _bucket_cell(bk, key, fmt):
    """A bucket figure, or an explicit withholding. Mirrors
    `dashboard._bucket_num`: a figure that is not a bucket figure does not get
    shown as one, and on paper the reason has to be printed rather than hovered."""
    if bk.get("state") == trend.STATE_INSUFFICIENT_MEMBERSHIP:
        return "<font color='{}'>— withheld</font>".format(COV_RULE)
    return _x(fmt(bk.get(key)))


# ---------------- sections ----------------

def _heading(story, styles, title, subtitle=None):
    story.append(_P(_x(title), styles["_h1"]))
    if subtitle:
        story.append(_P(subtitle, styles["_chartnote"]))


def sec_aggregate(snap, styles):
    t, panel = snap["total"], (snap.get("panel") or {})
    br = panel.get("breadth_series") or []
    latest_br = br[-1] if br else {}
    net = latest_br.get("net_direction", 0)
    cap = ((panel.get("constant") or {}).get("capex") or {})
    ic = ((panel.get("constant") or {}).get("issuance") or {})
    out = []
    _heading(out, styles, "The aggregate — one chart")
    out.append(brief.composite_drawing(
        snap, width=CHART_W, height=300, margins=(66, 132, 42, 40),
        tick_size=6.2, label_size=6.8))
    out.append(_spacer(4))
    out.append(_P(
        "<b>Reading it.</b> The heavy black line is panel trailing-twelve-month capex, "
        "drawn on the phase state the classifier assigned it (shaded behind). The thin "
        "lines are the bucket sums. The two orange lines are the <b>jaws</b>: capex and "
        "credit issuance for the <i>same</i> names over the <i>same</i> window, solid and "
        "dashed. Everything is in <b>true dollars on a log axis</b> — nothing is rescaled "
        "to fit, so a steeper line is genuinely growing faster and the jaws open by slope "
        "rather than by any factor a reader has to divide back out. The strip along the "
        "bottom is breadth: how many member names are turning, which the dollar-weighted "
        "line cannot show.", styles["_note"]))
    out.append(_P(
        "<b>Why these names.</b> Every line here is a <b>level</b>, and a level summed over "
        "changing membership shows arrivals as growth — the panel's matched membership runs "
        "1 to 12 names across its 66 quarters. So each line holds its membership <b>constant"
        "</b> across the whole window, and the window chosen is the longest one whose members "
        "still cover {}% of the dollars reported at its end. Capex: <b>{}</b> ({}). Credit, "
        "and the jaws: <b>{}</b> ({}). The YoY charts on the other views are immune to this "
        "and use the full matched membership.".format(
            int(100 * trend.COVERAGE_FLOOR),
            _x(", ".join(cap.get("members") or []) or "—"),
            "{:.1f}% of reported dollars".format(100 * cap.get("coverage", 0)),
            _x(", ".join(ic.get("members") or []) or "—"),
            "{:.1f}%".format(100 * ic.get("coverage", 0))), styles["_note"]))

    lag = cap.get("lagging") or []
    if lag:
        out.append(_warn(
            "<b>Behind on filing, so absent from the last point:</b> {}. These are not "
            "contractions — the issuer has not filed the quarter yet. {} alone is {} of "
            "last-reported capex.".format(
                ", ".join("{} (last {}, {})".format(
                    _x(x["ticker"]), _x(x["last_quarter"]), _x(_money(x["last_value"])))
                    for x in sorted(lag, key=lambda z: -z["last_value"])[:6]),
                _x(max(lag, key=lambda z: z["last_value"])["ticker"]),
                _x(_money(max(lag, key=lambda z: z["last_value"])["last_value"]))),
            styles))
        out.append(_spacer(4))

    if net <= 0 and t["state"] in (phases.STATE_ACCELERATING, phases.STATE_PLATEAU):
        out.append(_warn(
            "<b>Breadth disagrees with the level.</b> The panel is {} while breadth runs "
            "net {:+d} — the aggregate is being carried by its largest members while the "
            "majority of names turn. Both readings are published because neither is the "
            "whole answer.".format(_x(t["state"]), net), styles))
        out.append(_spacer(4))

    rows = [["<b>TOTAL PANEL</b>", _state_text(t["state"]), _x(_money(t.get("ttm"))),
             _x(_pct(t.get("latest_yoy"))), str(t.get("member_count")),
             _band(t.get("band"))]]
    for b, bk in sorted(snap["buckets"].items()):
        rows.append([_x(b), _state_text(bk["state"]),
                     _bucket_cell(bk, "ttm", _money),
                     _bucket_cell(bk, "latest_yoy", _pct),
                     str(bk.get("member_count")), _band(bk.get("band"))])
    iss = panel.get("issuance_ttm") or []
    comm = panel.get("commitments") or []
    if iss:
        rows.append(["credit issuance", "—", _x(_money(iss[-1]["value"])), "—",
                     str(iss[-1]["members"]), "—"])
    if comm:
        rows.append(["forward commitments", "—", _x(_money(comm[-1]["value"])), "—",
                     str(comm[-1]["members"]), "—"])
    out.append(_table(("Series", "State", "TTM", "TTM YoY", "Members", "Band"), rows,
                      [150, 132, 96, 84, 62, 56], styles, right_cols=(2, 3, 4, 5)))

    memb = ["<b>TOTAL PANEL</b> matched: {}".format(
        _x(", ".join(t.get("membership") or []) or "—"))]
    for b, bk in sorted(snap["buckets"].items()):
        memb.append("<b>{}</b> matched: {}".format(
            _x(b), _x(", ".join(bk.get("membership") or []) or "—")))
    if iss:
        memb.append("<b>credit issuance</b> contributing: {}".format(
            _x(", ".join(panel.get("issuance_membership_latest") or []) or "—")))
    if comm:
        memb.append("<b>forward commitments</b> disclosing: {}".format(
            _x(", ".join(panel.get("commitments_membership_latest") or []) or "—")))
    out.append(_P("Matched membership behind each row, which the dashboard carries on "
                  "hover: " + " &nbsp;·&nbsp; ".join(memb), styles["_foot"]))

    for b, bk in sorted(snap["buckets"].items()):
        if bk["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP:
            out.append(_warn(
                "<b>{}</b>: matched membership fell to {} member(s), below the {}-member "
                "floor — no state published.".format(
                    _x(b), bk.get("member_count"), bk.get("min_members")), styles))
    return out


def sec_hayes(snap, styles):
    t, panel = snap["total"], (snap.get("panel") or {})
    out = []
    _heading(out, styles, "Hayes panel — capex, credit, forward commitments")
    out.append(_P(
        "The falsifier as three co-plotted legs. The claim under test is about their "
        "<b>divergence</b>, so they are shown together and never collapsed into one number. "
        "Dead-bands stamped {}. <b>Unlike the front page, nothing here is rebased</b> — "
        "these are true dollar magnitudes, one axis each, so the legs can be sized against "
        "each other rather than only shaped against each other.".format(
            _x(snap.get("bands_measured_on"))), styles["_note"]))
    out.append(level_drawing(
        [(r["q"], r["ttm"]) for r in (t.get("ttm_series") or [])],
        "Leg 1 — panel TTM capex (phase-shaded)", svgcharts.SERIES_COLORS["total"],
        observations=t.get("observations"), height=126))
    out.append(level_drawing(
        [(r["q"], r["value"]) for r in (panel.get("issuance_ttm") or [])],
        "Leg 2 — panel TTM credit issuance", svgcharts.SERIES_COLORS["issuance"],
        height=126))
    # Sized so all three legs sit on one page: the claim is about their
    # divergence, and a leg on the next sheet is a leg you cannot compare.
    out.append(_commitments_drawing(
        snap, "Leg 3 — forward commitment stock (contracted, unspent), per issuer",
        top=8, height=166))
    out.append(_P(
        "Shading on leg 1 only: the classifier runs on the capex series. Credit and "
        "commitments carry no phase state and are not given the appearance of one. Leg 3 is "
        "drawn per issuer because the panel sum is refused — see below.", styles["_chartnote"]))
    ref = _commitments_refusal_text(snap)
    if ref:
        out.append(_warn(ref, styles))
        out.append(_spacer(4))

    rows = [["<b>TOTAL PANEL</b>", _state_text(t["state"]), _x(_money(t.get("ttm"))),
             _x(_pct(t.get("latest_yoy"))), str(t.get("member_count")), "—"]]
    for b, bk in sorted(snap["buckets"].items()):
        br = bk.get("breadth") or {}
        rows.append([
            _x(b), _state_text(bk["state"]),
            _bucket_cell(bk, "ttm", _money), _bucket_cell(bk, "latest_yoy", _pct),
            str(bk.get("member_count")),
            "ACC {} · PLAT {} · DEC {} · CONTR {} · <b>net {:+d}</b>".format(
                br.get("ACCELERATING", 0), br.get("PLATEAU", 0),
                br.get("DECELERATING", 0), br.get("CONTRACTING", 0),
                br.get("net_direction", 0))])
    out.append(_table(("Series", "State", "TTM", "TTM YoY", "Members", "Breadth"), rows,
                      [110, 120, 92, 80, 56, 262], styles, right_cols=(2, 3, 4)))

    for b, bk in sorted(snap["buckets"].items()):
        if bk["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP:
            out.append(_warn(
                "<b>{}</b>: matched membership fell to {} member(s), below the {}-member "
                "floor. A one-name bucket sum is that member's own number wearing a bucket "
                "label, so no state is published.".format(
                    _x(b), bk.get("member_count"), bk.get("min_members")), styles))

    out.append(_P("Per-issuer legs", styles["_h2"]))
    irows = []
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: -(kv[1]["ttm_capex"] or 0)):
        comm = iss["commitments"]
        cov = _cov_tags(iss["coverage"])
        irows.append([
            "<b>{}</b>{}".format(_x(tick), " " + cov if cov else ""),
            _x(iss["bucket"]), _state_text(iss["state"], iss["flags"]),
            _x(_money(iss["ttm_capex"])), _x(_pct(iss["latest_yoy"])),
            _x(_pct(iss["credit_ratio"], 0)), _x(_money(comm["latest"])),
            spark_drawing([q["value"] for q in iss["quarters"]])])
    out.append(_table(
        ("Issuer", "Bucket", "State", "TTM capex", "TTM YoY", "credit/capex",
         "commitments", "capex trend"),
        irows, [104, 68, 122, 76, 66, 70, 76, 60], styles, right_cols=(3, 4, 5, 6)))
    out.append(_P("Per-issuer provenance — band, state entry, coverage — is carried in the "
                  "Provenance appendix at the end of this report, where the dashboard "
                  "carries it on hover.", styles["_foot"]))
    return out


def sec_phases(snap, styles):
    out = []
    _heading(out, styles, "Phase board")
    out.append(_P(
        "State is set by the ladder on TTM YoY. <b>N=2</b> consecutive same-direction moves "
        "to enter a state, so a single move against the trend does not flip it — read "
        "<i>direction</i> beside <i>state</i>. SOFTENING is a flag on the first out-of-band "
        "decline, never a state. CONTRACTING is the only level-based state (TTM YoY &lt; 0).",
        styles["_note"]))
    rows, quarters = svgcharts.issuer_rows_for_grid(snap)
    out.append(state_grid_drawing(rows, quarters))
    out.append(_P(
        "Every classified series, every quarter it classifies. A blank cell is "
        "INSUFFICIENT-HISTORY, deliberately left uncoloured so that absence of a state does "
        "not read as a fifth state.", styles["_chartnote"]))
    out.append(yoy_drawing(
        snap["total"].get("yoy_series"), snap["total"].get("observations"),
        "TOTAL PANEL — TTM YoY on its phase state", svgcharts.SERIES_COLORS["total"],
        band=snap["total"].get("band"), height=150))

    t = snap["total"]
    tobs = t.get("observations") or []
    tl = tobs[-1] if tobs else {}
    arows = [["<b>TOTAL PANEL</b>", _state_text(t["state"]),
              str(tl.get("quarters_in_state", "—")), _x(tl.get("entered") or "—"),
              _x(_pct(t.get("latest_yoy"))),
              "{:+.1f}pp".format(tl["delta"]) if tl.get("delta") is not None else "—",
              _x(tl.get("direction") or "—"), _band(t.get("band"))]]
    for b, bk in sorted(snap["buckets"].items()):
        obs = bk.get("observations") or []
        last = obs[-1] if obs else {}
        arows.append([
            "bucket:" + _x(b), _state_text(bk["state"]),
            str(last.get("quarters_in_state", "—")), _x(last.get("entered") or "—"),
            _bucket_cell(bk, "latest_yoy", _pct),
            "{:+.1f}pp".format(last["delta"]) if last.get("delta") is not None else "—",
            _x(last.get("direction") or "—"), _band(bk.get("band"))])
    out.append(_table(
        ("Series", "State", "Qtrs in state", "Entered", "TTM YoY", "Delta",
         "Last move", "Band"),
        arows, [120, 126, 62, 74, 78, 62, 118, 50], styles, right_cols=(2, 4, 5, 7)))

    out.append(_P("Issuers", styles["_h2"]))
    order = {phases.STATE_CONTRACTING: 0, phases.STATE_DECELERATING: 1,
             phases.STATE_PLATEAU: 2, phases.STATE_ACCELERATING: 3,
             phases.STATE_INSUFFICIENT: 4}
    irows = []
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: (order.get(kv[1]["state"], 9), kv[0])):
        mirror = (" <font size='5.4' color='#666666'>MIRROR — no alerts</font>"
                  if iss["bucket"] == "mirror" else "")
        irows.append([
            "<b>{}</b>{}".format(_x(tick), mirror), _x(iss["bucket"]),
            _state_text(iss["state"], iss["flags"]),
            str(iss["quarters_in_state"] or "—"), _x(iss["entered"] or "—"),
            _x(_pct(iss["latest_yoy"])),
            "{:+.1f}pp".format(iss["latest_delta"]) if iss["latest_delta"] is not None else "—",
            _x(iss["direction"] or "—"), _band(iss.get("band")),
            spark_drawing([p["yoy"] for p in iss["yoy_series"]])])
    out.append(_table(
        ("Issuer", "Bucket", "State", "Qtrs", "Entered", "TTM YoY", "Delta",
         "Last move", "Band", "YoY history"),
        irows, [96, 60, 118, 34, 54, 60, 48, 90, 44, 56], styles,
        right_cols=(3, 5, 6, 8)))

    trans = snap.get("transitions", [])[-25:]
    if trans:
        out.append(_P("Recent transitions", styles["_h2"]))
        trows = [[_x(tr["series_key"]), _x(tr["quarter"]), _state_text(tr["from_state"]),
                  _state_text(tr["to_state"]), _x(_pct(tr.get("yoy"))),
                  "{:+.1f}pp".format(tr["delta"]) if tr.get("delta") is not None else "—"]
                 for tr in reversed(trans)]
        out.append(_table(("Series", "Quarter", "From", "To", "TTM YoY", "Delta"),
                          trows, [190, 64, 138, 138, 84, 66], styles,
                          right_cols=(4, 5)))
    return out


def sec_divergence(snap, styles):
    out = []
    _heading(out, styles, "Credit-to-capex divergence")
    out.append(_P(
        "TTM issuance over TTM capex. <b>TTM only</b> — measured quarterly ratios swing 0% "
        "to 148% on issuance lumpiness. A withheld ratio shows its coverage status and never "
        "a zero.", styles["_note"]))
    ratio = (snap.get("panel") or {}).get("credit_ratio_series") or []
    out.append(level_drawing(
        [(r["q"], r["ratio"]) for r in ratio],
        "Panel credit-to-capex ratio, TTM over TTM", svgcharts.SERIES_COLORS["issuance"],
        observations=snap["total"].get("observations"),
        fmt=lambda v: "{:.0f}%".format(100 * v), height=170))
    out.append(_P(
        "Shaded on the panel's <i>capex</i> phase state: the question this chart answers is "
        "whether credit is doing more of the work as the capex phase turns, so the phase "
        "belongs behind it. The ratio itself is published in the snapshot, not divided here.",
        styles["_chartnote"]))
    rows = []
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: -(kv[1]["credit_ratio"] or -1)):
        if iss["ttm_capex"] is None:
            continue
        rows.append(["<b>{}</b>".format(_x(tick)), _x(iss["bucket"]),
                     _x(_money(iss["ttm_capex"])), _x(_money(iss["ttm_issuance"])),
                     _x(_pct(iss["credit_ratio"], 0)),
                     _cov_tags(iss["coverage"], drop_ok=False) or "OK"])
    out.append(_table(
        ("Issuer", "Bucket", "TTM capex", "TTM issuance", "credit/capex", "Coverage"),
        rows, [80, 72, 92, 92, 84, 300], styles, right_cols=(2, 3, 4)))
    return out


def sec_buckets(snap, styles):
    from reportlab.platypus import PageBreak
    out = []
    _heading(out, styles, "Bucket drilldowns")
    out.append(_P("Composition travels with every subtotal. Membership changes are published "
                  "beside the trend, never blended into it.", styles["_note"]))
    for n, (b, bk) in enumerate(sorted(snap["buckets"].items())):
        if n:
            out.append(PageBreak())
        br = bk.get("breadth") or {}
        out.append(_P("{} &nbsp; {}".format(_x(b), _state_text(bk["state"])),
                      styles["_h2"]))
        out.append(yoy_drawing(
            bk.get("yoy_series"), bk.get("observations"),
            "{} — bucket-sum TTM YoY, matched membership".format(b),
            svgcharts.SERIES_COLORS.get(b, "#555555"), band=bk.get("band"), height=140))
        out.append(level_drawing(
            [(r["q"], r["ttm"]) for r in (bk.get("ttm_series") or [])],
            "{} — bucket-sum TTM level".format(b),
            svgcharts.SERIES_COLORS.get(b, "#555555"),
            observations=bk.get("observations"), height=130))
        out.append(_P(
            "TTM <b>{}</b> · YoY <b>{}</b> · members {} · top-2 concentration <b>{}</b> · "
            "band {}pp<br/>Breadth: ACC {} · PLAT {} · DEC {} · CONTR {} · net {:+d}".format(
                _bucket_cell(bk, "ttm", _money), _bucket_cell(bk, "latest_yoy", _pct),
                bk.get("member_count"),
                "{:.0f}%".format(100 * bk["top2_share"]) if bk.get("top2_share") else "—",
                bk.get("band"), br.get("ACCELERATING", 0), br.get("PLATEAU", 0),
                br.get("DECELERATING", 0), br.get("CONTRACTING", 0),
                br.get("net_direction", 0)), styles["_note"]))
        members = [snap["issuers"][m] for m in bk.get("membership", [])
                   if m in snap["issuers"]]
        tot = sum(m["ttm_capex"] or 0 for m in members) or 1.0
        rows = [[_x(m["ticker"]), _state_text(m["state"], m["flags"]),
                 _x(_money(m["ttm_capex"])),
                 "{:.0f}%".format(100 * (m["ttm_capex"] or 0) / tot),
                 _x(_pct(m["latest_yoy"])), _x(_pct(m["credit_ratio"], 0))]
                for m in sorted(members, key=lambda x: -(x["ttm_capex"] or 0))]
        if rows:
            out.append(_table(
                ("Member", "State", "TTM capex", "share", "TTM YoY", "credit/capex"),
                rows, [76, 132, 88, 54, 76, 80], styles, right_cols=(2, 3, 4, 5)))
        ce = bk.get("composition_events") or []
        if ce:
            out.append(_P("<b>Composition events:</b> " + " · ".join(
                "{} {} {}".format(_x(e["quarter"]), _x(e["ticker"]), _x(e["change"]))
                for e in ce[-8:]), styles["_foot"]))
    return out


def _commitments_series(snap, top):
    ser = {}
    for tick, iss in (snap.get("issuers") or {}).items():
        pts = [(p["q"], p["value"]) for p in (iss["commitments"].get("points_cq") or [])]
        if len(pts) >= 2:
            ser[tick] = pts
    return dict(sorted(ser.items(), key=lambda kv: -kv[1][-1][1])[:top])


def _commitments_drawing(snap, title, top=8, height=210):
    return multi_line_drawing(
        _commitments_series(snap, top), title,
        note="dashed = gaps in disclosure, not flat quarters", height=height)


def _commitments_refusal_text(snap):
    cp = (snap.get("panel") or {}).get("commitments_panel") or {}
    if not cp.get("status", "").startswith("REFUSED"):
        return ""
    return ("<b>Panel commitment total: {}.</b> {}. The per-issuer series above are "
            "unaffected — what fails is <i>adding</i> them, not observing them.".format(
                _x(cp["status"]), _x(cp["detail"])))


def sec_commitments(snap, styles):
    out = []
    _heading(out, styles, "Forward commitment stock")
    out.append(_P(
        "Contracted but unspent. Leads reported capex. Issuers that disclose a figure "
        "without XBRL-tagging it publish <b>UNCOVERED-UNTAGGED</b> rather than a zero.",
        styles["_note"]))
    out.append(_commitments_drawing(snap, "Forward commitment stock, per issuer",
                                    top=10, height=210))
    out.append(_P(
        "A <b>stock</b>, not a flow, and disclosed on the issuer's own schedule rather than "
        "every quarter — so these are plotted separately and never summed. A dashed segment "
        "spans quarters with no disclosure; it is not a flat stretch.", styles["_chartnote"]))
    ref = _commitments_refusal_text(snap)
    if ref:
        out.append(_warn(ref, styles))
        out.append(_spacer(4))

    deltas = [d for d in snapshot.commitment_deltas(snap) if d["multiple"] is not None]
    if deltas:
        deltas.sort(key=lambda d: -(d["multiple"] or 0))
        out.append(_P("Change since the previous observation", styles["_h2"]))
        out.append(_P(
            "Each issuer against <b>its own</b> previous disclosure on <b>its own</b> "
            "concept — never across issuers, so the basis problem that makes cross-issuer "
            "commitment totals incomparable does not arise. A stock is disclosed on the "
            "issuer's own schedule, so the <b>gap</b> travels beside the move: 3x over one "
            "quarter and 3x over eight are different facts.", styles["_note"]))
        if (snapshot.COMMITMENT_JUMP_MULTIPLE is None
                or snapshot.COMMITMENT_JUMP_MIN_DELTA is None):
            out.append(_warn(
                "<b>These do not alert yet.</b> The threshold is UNSET pending "
                "ratification (E8). Measured over 308 observation pairs: p50 1.00x, "
                "p90 2.00x, p95 3.20x — but the tail is near-zero bases, so a bare "
                "multiple is a bad gate, and a multiple ALONE misses META's "
                "+$111.64B at 1.47x — the largest move on the panel. Proposed and "
                "held: <b>(2.0x AND &ge;$1B) OR &ge;$20B</b>.",
                styles))
            out.append(_spacer(4))
        out.append(_table(
            ("Issuer", "Concept", "From", "To", "gap", "was", "now", "change", "multiple"),
            [["<b>{}</b>".format(_x(d["ticker"])), _x(d["concept"]), _x(d["from_q"]),
              _x(d["to_q"]), "{}q".format(d["quarters_between"]),
              _x(_money(d["from_value"])), _x(_money(d["to_value"])),
              _x(_money(d["delta"])), "{:.2f}x".format(d["multiple"])]
             for d in deltas],
            [52, 176, 52, 52, 34, 74, 74, 78, 56], styles,
            right_cols=(4, 5, 6, 7, 8)))

    rows = []
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: -((kv[1]["commitments"] or {}).get("latest") or -1)):
        c = iss["commitments"]
        if c["status"] == "ABSENT":
            continue
        rows.append(["<b>{}</b>".format(_x(tick)), _x(iss["bucket"]),
                     "<font color='{}'>{}</font>".format(COV_RULE, _x(c["status"])),
                     _x(_money(c["latest"])), _x(c["concept"] or "—"),
                     _x(c["detail"][:150])])
    out.append(_table(("Issuer", "Bucket", "Status", "Latest", "Concept", "Detail"),
                      rows, [56, 62, 106, 66, 214, 216], styles, right_cols=(3,)))
    return out


def sec_suppliers(snap, styles):
    sup = snap.get("suppliers") or {}
    legs, cc = sup.get("legs") or {}, sup.get("crosscheck") or {}
    out = []
    _heading(out, styles, "Supplier cross-check — the same dollar, from the other side")
    out.append(_P(
        "A hyperscaler's capex and NVIDIA's datacenter revenue are largely the <b>same money "
        "seen from opposite sides of the invoice</b>. That makes this an independent read on "
        "the buildout — different filers, different fiscal calendars, different incentives — "
        "and it makes adding the two a category error. Suppliers are <b>never summed into the "
        "spending aggregate</b>; they are related to it by a ratio, which is a corroboration "
        "and not a reconciliation. It is not expected to reach 100%.", styles["_note"]))
    out.append(_P(
        "This leg is <b>parser-only</b>. Segment revenue is dimension-qualified, so the "
        "companyfacts API drops it entirely — NVDA's API record carries total Revenues and a "
        "segment <i>count</i>, and nothing else. Every figure below was read out of the "
        "filing itself.", styles["_note"]))

    fr = sup.get("frontier") or {}
    if fr.get("rows"):
        out.append(_P("One quarter ahead of the demand panel", styles["_h2"]))
        out.append(_P(
            "The hyperscalers' newest reported quarter is <b>{}</b>. These suppliers close "
            "off-calendar and have already filed beyond it. There is <b>no ratio here and "
            "there cannot be</b> — the denominator does not exist yet — so each row is the "
            "supplier's own discrete quarter against its own prior quarter and its own "
            "year-ago quarter. <b>Not a TTM, not a phase state, and in no aggregate.</b> It "
            "is the earliest signal the panel carries, and it is one name at a time.".format(
                _x(fr.get("demand_frontier"))), styles["_note"]))
        out.append(_table(
            ("Supplier", "Quarter", "DC revenue", "QoQ", "YoY", "compared against"),
            [["<b>{}</b>".format(_x(r["ticker"])), _x(r["q"]), _x(_money(r["value"])),
              _x(_pct(r["qoq"])), _x(_pct(r["yoy"])),
              "{} and {}".format(_x(r["prior_q"]), _x(r["year_ago_q"]))]
             for r in fr["rows"]],
            [70, 62, 92, 72, 72, 160], styles, right_cols=(2, 3, 4)))

    series = cc.get("series") or []
    if series:
        out.append(level_drawing(
            [(r["q"], r["ratio"]) for r in series],
            "Supplier datacenter revenue as a share of hyperscaler capex (TTM/TTM)",
            svgcharts.SERIES_COLORS["issuance"],
            fmt=lambda v: "{:.0f}%".format(100 * v), height=170))
    if cc.get("warning"):
        out.append(_warn("<b>Read the last point with care.</b> {}".format(
            _x(cc["warning"])), styles))
        out.append(_spacer(4))

    out.append(_P("Legs", styles["_h2"]))
    rows = []
    for tick, leg in sorted(legs.items(), key=lambda kv: -((kv[1].get("ttm")) or -1)):
        st = leg["status"]
        colour = {"COVERED": "#1d6f42", "MAPPED-BUSINESS-UNITS": "#1f4e9c"}.get(st, COV_RULE)
        dcs = leg.get("dc_state") or phases.STATE_INSUFFICIENT
        rows.append([
            "<b>{}</b>".format(_x(tick)),
            "<font color='{}'><b>{}</b></font>".format(colour, _x(st)),
            _state_text(dcs, leg.get("dc_flags")),
            _x(_money(leg.get("ttm"))), _x(_pct(leg.get("dc_latest_yoy"))),
            str(len(leg.get("quarters") or [])),
            str(leg.get("restatement_count") or 0),
            _x(", ".join(leg.get("axes") or []) or (leg.get("detail") or "")[:70])])
    out.append(_table(
        ("Supplier", "Status", "DC revenue phase", "DC revenue TTM", "TTM YoY",
         "Quarters", "Restated", "Resolved"),
        rows, [58, 124, 118, 78, 62, 50, 50, 180], styles, right_cols=(3, 4, 5, 6)))

    band_notes = ["<b>{}</b> banded on dcrev:supplier at {}pp, measured {} — NOT the "
                  "issuer:supplier band, which applies to the supplier's own capex".format(
                      _x(t), l.get("dc_band"), _x(l.get("dc_band_measured_on")))
                  for t, l in sorted(legs.items()) if l.get("dc_band")]
    if band_notes:
        out.append(_P(" &nbsp;·&nbsp; ".join(band_notes), styles["_foot"]))
    restated = [(t, l) for t, l in sorted(legs.items()) if l.get("restatements")]
    if restated:
        out.append(_P("<b>Restatements</b> (carried on hover in the dashboard): " +
                      " &nbsp;·&nbsp; ".join(
                          "<b>{}</b> {}".format(_x(t), _x("; ".join(
                              "{} {:,.0f} -> {:,.0f} (superseded by {})".format(
                                  r["period_end"], r["was"], r["now"], r["superseded_by"])
                              for r in l["restatements"])))
                          for t, l in restated), styles["_foot"]))

    if series:
        out.append(_P("Cross-check history", styles["_h2"]))
        hrows = [[_x(r["q"]), _x(_money(r["dc"])), _x(_money(r["capex"])),
                  "<b>{:.1f}%</b>".format(100 * r["ratio"]),
                  str(r["dc_members"]), str(r["capex_members"])]
                 for r in reversed(series[-16:])]
        out.append(_table(
            ("Quarter", "Supplier DC revenue TTM", "Hyperscaler capex TTM", "Ratio",
             "DC members", "Capex members"),
            hrows, [64, 132, 132, 66, 68, 74], styles, right_cols=(1, 2, 3, 4, 5)))

    mapped = [l for l in legs.values() if l.get("mapping")]
    if mapped:
        out.append(_P("Ruled mappings — a semantic judgement, disclosed as one",
                      styles["_h2"]))
        for l in sorted(mapped, key=lambda x: x["ticker"]):
            m = l["mapping"]
            out.append(_tinted(
                "<b>{}</b> reports no datacenter member. Ruled by <b>{}</b> on <b>{}</b>: "
                "the units below are treated as datacenter revenue.<br/>"
                "<b>Summed:</b> {}<br/><b>Excluded:</b> {}<br/>{}<br/>"
                "<b>This figure is mapped, not measured.</b> It is published as "
                "MAPPED-BUSINESS-UNITS everywhere it appears, and it carries that label "
                "precisely because reasonable people could draw the boundary differently."
                .format(_x(l["ticker"]), _x(m.get("ruled_by", "ruling")), _x(m.get("ruled")),
                        _x(" + ".join("{} ({})".format(m["labels"].get(x, x), x)
                                      for x in m["members"])),
                        _x(", ".join("{} ({})".format(m["excluded_labels"].get(x, x), x)
                                     for x in m["excluded"])),
                        _x(m.get("rationale", ""))),
                styles, MAP_FILL, MAP_RULE))
            out.append(_spacer(4))

    refused = [l for l in legs.values()
               if l["status"] not in ("COVERED", "MAPPED-BUSINESS-UNITS")]
    if refused:
        out.append(_P("Refused, and why", styles["_h2"]))
        for l in sorted(refused, key=lambda x: x["ticker"]):
            out.append(_warn("<b>{}</b> — {}. {}".format(
                _x(l["ticker"]), _x(l["status"]), _x(l["detail"])), styles))
            out.append(_spacer(3))
        out.append(_P(
            "Each of these reports revenue by segment, but none of those segments is a "
            "datacenter line and none has a ruled mapping. They stay in the bucket because "
            "their inventory and purchase obligations still bear on the buildout, and "
            "because a named refusal is worth more than a silent omission.", styles["_note"]))
    return out


def sec_provenance(snap, styles):
    """What the dashboard carries on hover, which paper cannot.

    Not an extra: the module's own rule is that provenance is not optional, and
    a printed number whose derivation has been dropped is a number a reader has
    to take on trust. Every `title=` attribute the seven views attach to an
    issuer lands here.
    """
    out = []
    _heading(out, styles, "Appendix — provenance",
             "Every published figure's derivation. On the dashboard these are hover "
             "tooltips; on paper they are a table, because a number without its "
             "provenance is a number taken on trust.")
    rows = []
    for tick, iss in sorted(snap["issuers"].items()):
        comm = iss["commitments"] or {}
        rows.append([
            "<b>{}</b>".format(_x(tick)), _x(iss["bucket"]),
            _band(iss.get("band")), _x(iss.get("entered") or "—"),
            str(iss.get("quarters_in_state") or 0),
            str(len(iss.get("quarters") or [])),
            _x(", ".join(iss.get("coverage") or []) or "OK"),
            _x("{} — {}".format(comm.get("status", "—"), (comm.get("detail") or "")[:110]))])
    out.append(_table(
        ("Issuer", "Bucket", "Band", "State entered", "Qtrs in state",
         "Quarters observed", "Coverage", "Commitments provenance"),
        rows, [54, 60, 40, 62, 52, 62, 150, 240], styles,
        right_cols=(2, 4, 5)))
    return out


SECTIONS = (
    ("The aggregate", "/", sec_aggregate),
    ("Hayes panel", "/hayes", sec_hayes),
    ("Phase board", "/phases", sec_phases),
    ("Divergence", "/divergence", sec_divergence),
    ("Bucket drilldowns", "/buckets", sec_buckets),
    ("Forward commitments", "/commitments", sec_commitments),
    ("Suppliers", "/suppliers", sec_suppliers),
    ("Provenance appendix", None, sec_provenance),
)


def _banner_text(snap, last_scan_unix=None):
    """The dashboard's own staleness banner, verbatim in substance.

    Computed by `dashboard._staleness`, not re-derived — the banner is the
    difference between a reader trusting a number as current and knowing when it
    was true, and two implementations of that is one too many.
    """
    stale, age = dashboard._staleness(snap, last_scan_unix=last_scan_unix)
    gen = (snap or {}).get("generated_unix")
    stamp = (time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(int(gen)))
             if gen else "unknown")
    if not stale:
        return False, "panel last changed {} · scan is current".format(stamp)
    return True, (
        "<b>SCAN HAS NOT RUN IN {:.0f}h</b> — the nightly is not completing, so nothing "
        "below can have picked up a new filing. The panel itself last changed {}, which may "
        "legitimately be older still, since most nights are no-ops by design. Served rather "
        "than withheld because stale history is still history.".format(age, stamp))


def _stamp(unix):
    return (time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(int(unix)))
            if unix else "unknown")


def render_provenance(snap, last_scan_unix=None):
    """Where this render came from — host, snapshot stamp, last-scan stamp.

    Three different clocks, routinely three different answers: the panel last
    CHANGED when an issuer filed, the scan last RAN last night, and this file was
    produced on whichever machine happened to run the renderer. A document that
    prints only one of them invites a reader to assume the other two agree.
    """
    host = socket.gethostname().split(".")[0]
    return {
        "host": host,
        "is_live_host": host.lower() == LIVE_HOST,
        "snapshot": _stamp((snap or {}).get("generated_unix")),
        "last_scan": _stamp(last_scan_unix),
    }


def _footer_line(prov):
    base = "Capex Daemon — dashboard report · rendered on {} · snapshot {} · last scan {}"\
        .format(prov["host"], prov["snapshot"], prov["last_scan"])
    if prov["is_live_host"]:
        return base
    return "LOCAL COPY — NOT THE LIVE DASHBOARD · " + base


def _footer_factory(prov):
    line = _footer_line(prov)
    live = prov["is_live_host"]

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 6.5)
        if live:
            canvas.setFillColorRGB(0.45, 0.45, 0.45)
        else:
            canvas.setFillColorRGB(0.61, 0.11, 0.11)
        canvas.drawString(MARGIN_L, 18, line)
        canvas.setFillColorRGB(0.45, 0.45, 0.45)
        canvas.drawRightString(PAGE_SIZE[0] - MARGIN_R, 18, "page {}".format(doc.page))
        canvas.restoreState()

    return _footer


def build(snap, out_path, last_scan_unix=None,
          title="Capex Daemon — dashboard report"):
    """Render every dashboard view into one PDF. Returns the written path."""
    from abelard_common.render import pdf as housepdf
    from reportlab.platypus import PageBreak

    if not snap:
        raise housepdf.PdfRenderError(
            "no snapshot to render — run `capex-daemon scan` first")

    styles = _styles()
    stale, banner = _banner_text(snap, last_scan_unix)
    total = snap["total"]

    story = [_P(_x(title), styles["_h1"])]
    story.append(_P(
        "All seven views of the read-only dashboard, rendered from the same persisted "
        "snapshot the server renders. Panel state <b>{}</b> · TTM {} · TTM YoY {} · "
        "{} issuers · dead-bands stamped {}.".format(
            _x(total["state"]), _x(_money(total.get("ttm"))),
            _x(_pct(total.get("latest_yoy"))), len(snap.get("issuers") or {}),
            _x(snap.get("bands_measured_on"))), styles["_note"]))
    story.append(_warn(banner, styles) if stale
                 else _P(banner, styles["_chartnote"]))

    prov = render_provenance(snap, last_scan_unix)
    if prov["is_live_host"]:
        story.append(_P(
            "Rendered on <b>{}</b> — the host that serves the dashboard. Snapshot {} · "
            "last scan {}.".format(_x(prov["host"]), _x(prov["snapshot"]),
                                   _x(prov["last_scan"])), styles["_chartnote"]))
    else:
        story.append(_warn(
            "<b>LOCAL COPY — NOT THE LIVE DASHBOARD.</b> Rendered on <b>{}</b>, which is not "
            "<b>{}</b>. The figures below came from a copy of the snapshot taken at the "
            "moment of rendering; anything the daemon has done since is not in this "
            "document. Snapshot {} · last scan {}.".format(
                _x(prov["host"]), _x(LIVE_HOST), _x(prov["snapshot"]),
                _x(prov["last_scan"])), styles))

    contents = " &nbsp;·&nbsp; ".join(
        "{}. {}{}".format(i, _x(name), " ({})".format(_x(route)) if route else "")
        for i, (name, route, _fn) in enumerate(SECTIONS, start=1))
    story.append(_P("<b>Contents.</b> " + contents, styles["_foot"]))
    story.append(_spacer(10))

    # Three things differ from the screen, and a reader who does not know that
    # will read a difference as a disagreement.
    story.append(_P("How this differs from the screen", styles["_h2"]))
    story.append(_P(
        "<b>Provenance moved, it did not go away.</b> On the dashboard, matched membership, "
        "per-issuer band and state entry, and supplier restatement history are hover "
        "tooltips. Paper has no hover, so membership is printed under the aggregate table, "
        "restatements under the supplier legs, and everything else is the <b>Provenance "
        "appendix</b> at the end. Nothing that was published on screen is unpublished here.",
        styles["_note"]))
    story.append(_P(
        "<b>Sparklines are drawn, not typed.</b> The dashboard's trend columns use the "
        "unicode block characters. Those are not in the bundled font and would print as "
        "empty boxes, so they are drawn as bars over the same 24-quarter window and the same "
        "min-max scaling — the same shape, at more resolution than eight block heights allow.",
        styles["_note"]))
    story.append(_P(
        "<b>Landscape, and one view per page.</b> The charts are laid out at 1160pt and the "
        "issuer table has ten columns; portrait would shrink both past reading. Each of the "
        "seven views starts on its own page, in the order the dashboard's own navigation "
        "lists them.", styles["_note"]))
    story.append(_P(
        "Every figure is read from the persisted snapshot — this renderer computes nothing. "
        "Where the dashboard withholds a number it withholds it here for the same reason and "
        "says so; where a leg is refused, the refusal is printed where the leg would have "
        "been.", styles["_foot"]))

    for name, _route, fn in SECTIONS:
        story.append(PageBreak())
        story.extend(fn(snap, styles))

    return housepdf.build_pdf(
        out_path, story, title=title, pagesize=PAGE_SIZE,
        left_margin=MARGIN_L, right_margin=MARGIN_R,
        top_margin=MARGIN_T, bottom_margin=MARGIN_B,
        on_page=_footer_factory(prov))


def build_from_db(db_path=None, out_path=None, title="Capex Daemon — dashboard report"):
    """Load the snapshot read-only and render it. No write path exists here."""
    import os

    from . import config
    db_path = db_path or config.DB_PATH_DEFAULT
    snap, last_scan = dashboard._read_only_snapshot(db_path)
    out_path = out_path or os.path.join(config.STATE_HOME, "charts",
                                        "capex_dashboard.pdf")
    return build(snap, out_path, last_scan_unix=last_scan, title=title)
