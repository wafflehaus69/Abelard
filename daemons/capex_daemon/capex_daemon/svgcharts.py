"""Native SVG charts. No matplotlib, no JavaScript, no CDN.

**Why hand-rolled SVG.** CD-PH1 shipped matplotlib PNGs, and a PNG cannot carry
provenance: a reader who wants to know which concept produced a point, over how
many matched members, has to leave the page. Every mark here carries a `<title>`,
which browsers render as a native tooltip with no script at all. The dashboard
stays a stdlib `ThreadingHTTPServer` returning one self-contained document.

**Renderers never compute.** Everything in this module is a pure function from
already-published snapshot series to a string. If a number is not in the
snapshot it does not appear on a chart — that is the "one computation, many
renderers" rule, enforced by the module having no imports from the data layer
except the shared phase colours.

**Phase shading is the point.** The order asks for the phase state behind every
series, so `phase_bands` is the substrate every chart is drawn on top of: the
classifier's verdict as the background, the series as the foreground, and the
reader able to see immediately whether the two agree.
"""
import html

from . import phases, trend

# Shading is the same hue family as the state pills, at low alpha so a line
# drawn over it stays legible. INSUFFICIENT deliberately gets no fill: absence
# of a state must look like absence, not like a fifth state.
BAND_FILLS = {
    phases.STATE_ACCELERATING: "#1d6f42",
    phases.STATE_PLATEAU: "#8a8a8a",
    phases.STATE_DECELERATING: "#a8600f",
    phases.STATE_CONTRACTING: "#9b1c1c",
}
BAND_ALPHA = 0.13

SERIES_COLORS = {
    "total": "#111111",
    "hyperscaler": "#1f4e9c",
    "builder": "#8a3fa0",
    "reit": "#0f7f7f",
    "issuance": "#c2410c",
    "commitments": "#6b7280",
}


def _e(s):
    return html.escape(str(s if s is not None else ""))


def _money(v):
    if v is None:
        return "—"
    a = abs(v)
    if a >= 1e12:
        return "${:,.2f}T".format(v / 1e12)
    if a >= 1e9:
        return "${:,.1f}B".format(v / 1e9)
    if a >= 1e6:
        return "${:,.0f}M".format(v / 1e6)
    return "${:,.0f}".format(v)


import math

LOG_FLOOR = 1e6         # $1M; below this a capex series is not a signal


def norm(v, lo, hi, log=False):
    """Value -> 0..1 within [lo, hi]. Shared by the SVG and PDF renderers.

    Both renderers must place a point identically or the dashboard and the
    brief disagree about where a line is, which is the same class of bug as
    them disagreeing about its value.
    """
    if log:
        v, lo, hi = (math.log10(max(x, LOG_FLOOR)) for x in (v, lo, hi))
    return 0.0 if hi <= lo else (v - lo) / (hi - lo)


def log_ticks(lo, hi):
    """Decade ticks, with a 3x mid-decade tick when the span is short."""
    a, b = int(math.floor(math.log10(max(lo, LOG_FLOOR)))), \
        int(math.ceil(math.log10(max(hi, LOG_FLOOR * 10))))
    out = []
    for e in range(a, b + 1):
        out.append(10.0 ** e)
        if b - a <= 4:
            out.append(3.0 * 10.0 ** e)
    return [t for t in sorted(out) if lo <= t <= hi]


class Frame:
    """A quarter-indexed plot box. x is categorical, y is linear or log10.

    Log is not decoration. The composite carries $91M alongside $477B — four
    orders of magnitude — and on a linear axis every series but the largest is
    pinned to the floor. It also removes the need to rescale the credit leg at
    all: on a log axis a rebase is a vertical shift, so the two legs can be
    drawn in TRUE DOLLARS and their divergence read directly off the slopes.
    """

    def __init__(self, quarters, lo, hi, width=1160, height=380,
                 left=64, right=118, top=16, bottom=44, log=False):
        self.quarters = list(quarters)
        self.index = {q: i for i, q in enumerate(self.quarters)}
        self.w, self.h = width, height
        self.l, self.r, self.t, self.b = left, right, top, bottom
        self.pw = max(1, width - left - right)
        self.ph = max(1, height - top - bottom)
        self.log = log
        if log:
            lo = max(lo, LOG_FLOOR)
            hi = max(hi, lo * 10)
        elif hi <= lo:
            hi = lo + 1.0
        self.lo, self.hi = lo, hi

    def x(self, q):
        n = max(1, len(self.quarters) - 1)
        return self.l + self.pw * (self.index[q] / n)

    def step(self):
        return self.pw / max(1, len(self.quarters) - 1)

    def y(self, v):
        return self.t + self.ph * (1.0 - norm(v, self.lo, self.hi, self.log))

    def ticks(self, n=5):
        return log_ticks(self.lo, self.hi) if self.log else _nice_ticks(self.lo, self.hi, n)

    def points(self, pairs):
        return " ".join("{:.1f},{:.1f}".format(self.x(q), self.y(v))
                        for q, v in pairs if q in self.index)


def _nice_ticks(lo, hi, n=5):
    """Round tick values covering [lo, hi]. Plain 1/2/5 stepping.

    **The magnitude must be able to go below 1.** The first cut computed it from
    the digit count of `int(raw)` and floored it at 1 for any raw < 1, so every
    fractional axis got a step of at least 1.0. Every YoY and ratio chart on the
    dashboard is fractional — a TTM YoY of +84.7% is 0.847 — so a panel spanning
    -0.2 to +0.9 produced ticks at 0 and 1, of which only one was in range, and
    the chart rendered with a single gridline reading "+0%". Measured on the live
    dashboard: every yoy_chart and every ratio chart, unreadable for that reason.
    """
    span = hi - lo
    if span <= 0:
        return [lo]
    raw = span / max(1, n)
    if raw <= 0:
        return [lo, hi]
    mag = 10.0 ** math.floor(math.log10(raw))
    step = next((m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw), raw)
    out, v = [], (math.floor(lo / step) * step)
    while v <= hi + step * 0.001:
        if v >= lo - step * 0.001:
            out.append(v)
        v += step
    return out or [lo, hi]


# ---------------- marks ----------------

def phase_bands(frame, states_by_quarter, alpha=BAND_ALPHA):
    """The classifier's verdict as the background of the plot.

    One rect per quarter rather than per run, so a hovering reader gets the
    state at the quarter under the cursor and not the state of some run whose
    boundaries they cannot see.
    """
    half = frame.step() / 2.0
    out = []
    for q in frame.quarters:
        st = states_by_quarter.get(q)
        fill = BAND_FILLS.get(st)
        if not fill:
            continue
        x0 = max(frame.l, frame.x(q) - half)
        x1 = min(frame.l + frame.pw, frame.x(q) + half)
        out.append(
            "<rect x='{:.1f}' y='{:.1f}' width='{:.1f}' height='{:.1f}' fill='{}' "
            "fill-opacity='{}'><title>{} — {}</title></rect>".format(
                x0, frame.t, max(0.0, x1 - x0), frame.ph, fill, alpha, _e(q), _e(st)))
    return "".join(out)


def grid(frame, fmt=_money, ticks=5):
    out = []
    for v in frame.ticks(ticks):
        y = frame.y(v)
        if not (frame.t - 1 <= y <= frame.t + frame.ph + 1):
            continue
        out.append("<line x1='{l}' y1='{y:.1f}' x2='{r}' y2='{y:.1f}' stroke='#dcdcdc' "
                   "stroke-width='1'/><text x='{tx}' y='{ty:.1f}' font-size='10' "
                   "fill='#777' text-anchor='end'>{lab}</text>".format(
                       l=frame.l, r=frame.l + frame.pw, y=y, tx=frame.l - 6,
                       ty=y + 3, lab=_e(fmt(v))))
    return "".join(out)


MIN_LABEL_GAP = 38.0        # px; "2026Q2" at font-size 10 plus air


def label_picks(n, every, x_of, min_gap):
    """Indices to label: every Nth, the last one always, none overprinted.

    Shared by the time axis and the phase grid because both had the same
    defect — drawing every Nth AND the last unconditionally, so the final two
    collide whenever the length is not a multiple of N. The last quarter is the
    one a reader most wants, so it wins and its neighbour is dropped.
    """
    picks = [i for i in range(n) if i % every == 0]
    if n and n - 1 not in picks:
        picks.append(n - 1)
    while len(picks) >= 2 and x_of(picks[-1]) - x_of(picks[-2]) < min_gap:
        picks.pop(-2)
    return picks


def quarter_axis(frame, every=None, min_gap=MIN_LABEL_GAP):
    """Quarter labels, with the last one guaranteed and never overprinted.

    Every Nth label AND the last one were both drawn unconditionally, so
    whenever the series length was not a multiple of N the final two landed on
    top of each other — Leg 1 rendered "2026Q1" and "2026Q2" as "20226Q2" on the
    live dashboard. The last quarter is the one a reader most wants, so it wins
    and the neighbour that would collide with it is dropped.
    """
    qs = frame.quarters
    n = len(qs)
    every = every or max(1, n // 14)
    out = ["<line x1='{}' y1='{:.1f}' x2='{}' y2='{:.1f}' stroke='#bbb'/>".format(
        frame.l, frame.t + frame.ph, frame.l + frame.pw, frame.t + frame.ph)]
    picks = label_picks(n, every, lambda i: frame.x(qs[i]), min_gap)
    for i in picks:
        out.append("<text x='{:.1f}' y='{:.1f}' font-size='10' fill='#666' "
                   "text-anchor='middle'>{}</text>".format(
                       frame.x(qs[i]), frame.t + frame.ph + 14, _e(qs[i])))
    return "".join(out)


def line(frame, series, color, width=2.0, dash=None, label=None, title_fmt=None,
         opacity=1.0, dots=False):
    """`series` is [(quarter, value, title_extra)] or [(quarter, value)]."""
    pairs = [(s[0], s[1]) for s in series if s[0] in frame.index and s[1] is not None]
    if not pairs:
        return ""
    d = " stroke-dasharray='{}'".format(dash) if dash else ""
    out = ["<polyline points='{}' fill='none' stroke='{}' stroke-width='{}' "
           "stroke-opacity='{}' stroke-linejoin='round'{}/>".format(
               frame.points(pairs), color, width, opacity, d)]
    if dots or title_fmt:
        for s in series:
            if s[0] not in frame.index or s[1] is None:
                continue
            extra = s[2] if len(s) > 2 else ""
            tip = title_fmt(s) if title_fmt else "{} {}{}".format(
                s[0], _money(s[1]), " — " + str(extra) if extra else "")
            out.append("<circle cx='{:.1f}' cy='{:.1f}' r='{}' fill='{}' "
                       "fill-opacity='{}'><title>{}</title></circle>".format(
                           frame.x(s[0]), frame.y(s[1]), 3.2 if dots else 5,
                           color, 1.0 if dots else 0.001, _e(tip)))
    if label:
        lq, lv = pairs[-1]
        out.append("<text x='{:.1f}' y='{:.1f}' font-size='11' font-weight='600' "
                   "fill='{}'>{}</text>".format(
                       frame.x(lq) + 7, frame.y(lv) + 3.5, color, _e(label)))
    return "".join(out)


def place_labels(items, min_gap=12.0, top=None, bottom=None):
    """Nudge right-edge series labels apart so none is hidden under another.

    Measured live: TOTAL PANEL and the hyperscaler sum ended 0.4px apart and one
    label was unreadable. A chart whose lines are correct and whose labels are
    illegible is not a correct chart.

    `items` is [(y, x, text, color)]; returns SVG text elements.
    """
    items = sorted(items)
    ys = [y for y, _x, _t, _c in items]
    for i in range(1, len(ys)):                       # push down
        ys[i] = max(ys[i], ys[i - 1] + min_gap)
    if bottom is not None and ys and ys[-1] > bottom:  # then pull the block up
        shift = ys[-1] - bottom
        ys = [y - shift for y in ys]
        for i in range(len(ys) - 2, -1, -1):
            ys[i] = min(ys[i], ys[i + 1] - min_gap)
    if top is not None and ys:
        ys[0] = max(ys[0], top)
        for i in range(1, len(ys)):
            ys[i] = max(ys[i], ys[i - 1] + min_gap)
    out = []
    for (orig_y, x, text, color), y in zip(items, ys):
        if abs(y - orig_y) > 2.5:      # leader line, so a nudged label still points
            out.append("<line x1='{:.1f}' y1='{:.1f}' x2='{:.1f}' y2='{:.1f}' stroke='{}' "
                       "stroke-width='.8' stroke-opacity='.45'/>".format(
                           x - 5, orig_y, x - 1, y - 3.5, color))
        out.append("<text x='{:.1f}' y='{:.1f}' font-size='11' font-weight='600' "
                   "fill='{}'>{}</text>".format(x, y, color, _e(text)))
    return "".join(out)


def area(frame, series, color, opacity=0.16, label=None):
    pairs = [(s[0], s[1]) for s in series if s[0] in frame.index and s[1] is not None]
    if not pairs:
        return ""
    base = frame.y(max(frame.lo, 0.0))
    pts = "{:.1f},{:.1f} ".format(frame.x(pairs[0][0]), base) + frame.points(pairs) \
        + " {:.1f},{:.1f}".format(frame.x(pairs[-1][0]), base)
    out = ["<polygon points='{}' fill='{}' fill-opacity='{}' stroke='none'/>".format(
        pts, color, opacity)]
    for s in series:
        if s[0] not in frame.index or s[1] is None:
            continue
        extra = s[2] if len(s) > 2 else ""
        out.append("<circle cx='{:.1f}' cy='{:.1f}' r='5' fill='{}' fill-opacity='0.001'>"
                   "<title>{} {}{}</title></circle>".format(
                       frame.x(s[0]), frame.y(s[1]), color, _e(s[0]), _e(_money(s[1])),
                       _e(" — " + str(extra) if extra else "")))
    if label:
        lq, lv = pairs[-1]
        out.append("<text x='{:.1f}' y='{:.1f}' font-size='11' fill='{}' "
                   "fill-opacity='.85'>{}</text>".format(
                       frame.x(lq) + 7, frame.y(lv) + 3.5, color, _e(label)))
    return "".join(out)


def zero_line(frame):
    if not (frame.lo < 0 < frame.hi):
        return ""
    y = frame.y(0.0)
    return ("<line x1='{}' y1='{:.1f}' x2='{}' y2='{:.1f}' stroke='#999' "
            "stroke-width='1' stroke-dasharray='4 3'/>".format(
                frame.l, y, frame.l + frame.pw, y))


BREADTH_COLORS = {1: "#1d6f42", 0: "#8a8a8a", -1: "#9b1c1c"}


def breadth_strip(frame, breadth_rows, y, height=16):
    """Net direction per quarter as a colour, with the census on hover.

    Breadth is the count of names turning, which the dollar-weighted line
    cannot show — the builder bucket has been ACCELERATING at +269% while its
    breadth ran net −1. Both are published, side by side, deliberately.
    """
    half = frame.step() / 2.0
    out = []
    for row in breadth_rows:
        q = row["q"]
        if q not in frame.index:
            continue
        net = row.get("net_direction", 0)
        c = BREADTH_COLORS.get((net > 0) - (net < 0), "#8a8a8a")
        x0 = max(frame.l, frame.x(q) - half)
        w = min(frame.l + frame.pw, frame.x(q) + half) - x0
        tip = "{} — net {:+d} · ACC {} · PLAT {} · DEC {} · CONTR {}".format(
            q, net, row.get(phases.STATE_ACCELERATING, 0), row.get(phases.STATE_PLATEAU, 0),
            row.get(phases.STATE_DECELERATING, 0), row.get(phases.STATE_CONTRACTING, 0))
        out.append("<rect x='{:.1f}' y='{}' width='{:.1f}' height='{}' fill='{}' "
                   "fill-opacity='{:.2f}'><title>{}</title></rect>".format(
                       x0, y, max(0.0, w - 0.6), height, c,
                       0.30 + 0.10 * min(4, abs(net)), _e(tip)))
    out.append("<text x='{}' y='{}' font-size='10' fill='#666'>breadth (net direction "
               "of member states)</text>".format(frame.l + frame.pw + 7, y + height - 4))
    return "".join(out)


def refusal(x, y, text, width_hint=520):
    """A leg that is refused, printed where the leg would have been.

    A missing series and a refused series look identical on a chart unless the
    chart says otherwise, and they mean opposite things: one is "we have not
    got to it", the other is "we looked, and drawing this would mislead you".
    """
    return ("<rect x='{x}' y='{y}' width='{w}' height='30' fill='#fdf6e3' "
            "stroke='#e6d9a8' rx='3'/>"
            "<text x='{tx}' y='{ty}' font-size='10.5' fill='#8a6d1a'>⚠ {t}</text>").format(
                x=x, y=y, w=width_hint, tx=x + 9, ty=y + 19, t=_e(text))


MULTI_PALETTE = ["#1f4e9c", "#c2410c", "#1d6f42", "#8a3fa0", "#0f7f7f",
                 "#9b1c1c", "#a8600f", "#4b5563"]


def multi_line_chart(series_by_name, title, width=1160, height=300, fmt=_money,
                     note=None):
    """One line per issuer, no aggregation.

    Where a SUM is refused, the underlying series are still perfectly readable
    on their own — what fails is adding them, not observing them. Plotting them
    separately is the honest remainder of a refused aggregate.
    """
    series_by_name = {k: v for k, v in series_by_name.items() if len(v) >= 2}
    if not series_by_name:
        return empty("{}: nothing with two or more observations".format(title), width, 110)
    quarters = sorted({q for v in series_by_name.values() for q, _ in v}, key=trend._cq_sort)
    vals = [x for v in series_by_name.values() for _, x in v]
    f = Frame(quarters, min(0.0, min(vals)), max(vals) * 1.10, width=width,
              height=height, left=70, right=126, top=34, bottom=40)
    body = ["<text x='{}' y='20' font-size='12' font-weight='600' fill='#333'>{}</text>"
            .format(f.l, _e(title))]
    if note:
        body.append("<text x='{}' y='20' font-size='10' fill='#8a6d1a'>{}</text>"
                    .format(f.l + 330, _e(note)))
    body.append(grid(f, fmt=fmt))
    body.append(quarter_axis(f))
    ordered = sorted(series_by_name.items(), key=lambda kv: -kv[1][-1][1])
    for i, (name, pts) in enumerate(ordered):
        c = MULTI_PALETTE[i % len(MULTI_PALETTE)]
        # Gaps are real here — a dashed join says "not observed", not "flat".
        body.append(line(f, [(q, v, name) for q, v in pts], c, width=1.8,
                         dash="5 3" if _has_gap(pts) else None, label=name, dots=True,
                         title_fmt=lambda s, n=name: "{} {} {}".format(n, s[0], fmt(s[1]))))
    return svg(width, height, "".join(body), title=title)


def _has_gap(pts):
    idx = [trend._cq_index(q) for q, _ in pts]
    return any(b - a != 1 for a, b in zip(idx, idx[1:]))


def legend(items, x, y, gap=150):
    """items: [(label, color, dash|None)]"""
    out = []
    for i, (lab, color, dash) in enumerate(items):
        cx = x + i * gap
        d = " stroke-dasharray='{}'".format(dash) if dash else ""
        out.append("<line x1='{}' y1='{}' x2='{}' y2='{}' stroke='{}' stroke-width='2.5'{}/>"
                   "<text x='{}' y='{}' font-size='11' fill='#333'>{}</text>".format(
                       cx, y, cx + 18, y, color, d, cx + 24, y + 4, _e(lab)))
    return "".join(out)


def state_legend(x, y, gap=126):
    out = []
    for i, st in enumerate((phases.STATE_ACCELERATING, phases.STATE_PLATEAU,
                            phases.STATE_DECELERATING, phases.STATE_CONTRACTING)):
        cx = x + i * gap
        out.append("<rect x='{}' y='{}' width='14' height='11' fill='{}' fill-opacity='{}'/>"
                   "<text x='{}' y='{}' font-size='10' fill='#555'>{}</text>".format(
                       cx, y - 9, BAND_FILLS[st], BAND_ALPHA * 3, cx + 19, y, _e(st)))
    return "".join(out)


def svg(width, height, body, title=""):
    return ("<svg viewBox='0 0 {w} {h}' width='100%' height='{h}' "
            "xmlns='http://www.w3.org/2000/svg' role='img' aria-label='{t}' "
            "style='background:#fff;border:1px solid #e6e6e6;border-radius:3px'>"
            "{b}</svg>").format(w=width, h=height, b=body, t=_e(title))


def empty(msg, width=1160, height=120):
    return svg(width, height,
               "<text x='{}' y='{}' font-size='12' fill='#999' text-anchor='middle'>{}</text>"
               .format(width // 2, height // 2, _e(msg)), title=msg)


# ---------------- composed charts ----------------

def _states_of(observations):
    return {o["quarter"]: o["state"] for o in (observations or [])}


def _rebase(series, target_first):
    """Scale a series so its first point equals `target_first`.

    The credit leg is plotted axis-free against capex: what the Hayes claim is
    about is the GAP that opens between them, and a second dollar axis would
    let that gap be manufactured by choosing axis bounds. Rebasing to a common
    start makes the divergence a property of the data. True dollars stay on
    hover, and the scale factor is printed under the chart.
    """
    pts = [(q, v) for q, v in series if v is not None and v > 0]
    if not pts or not target_first:
        return None, None
    k = target_first / pts[0][1]
    return [(q, v * k) for q, v in pts], k


def composite_model(snap):
    """The data behind View 0, independent of how it is drawn.

    Split out because the same chart is rendered twice — as SVG on the
    dashboard and as vector art in the PDF phase page. Two renderers reading
    one model is fine; two renderers each deciding for themselves what the
    series are is how a dashboard and a brief start disagreeing.
    """
    total = snap.get("total") or {}
    panel = snap.get("panel") or {}
    const = panel.get("constant") or {}
    cap = const.get("capex") or {}
    tser = [(r["q"], r["value"]) for r in (cap.get("series") or [])]
    if len(tser) < 2:
        return None
    qset = {q for q, _ in tser}

    buckets = {}
    for b, bd in (const.get("buckets") or {}).items():
        rows = [(r["q"], r["value"]) for r in (bd.get("series") or []) if r["q"] in qset]
        if len(rows) >= 2:
            buckets[b] = {"rows": rows, "members": bd.get("members") or [],
                          "member_count": bd.get("member_count", 0)}

    # THE JAWS, in true dollars. No rebasing: the chart is drawn on a log axis,
    # where a rebase is only a vertical shift and the divergence is the
    # difference in SLOPE. Rebasing onto a linear axis was tried and rescaled
    # credit by 21x, which pushed the axis to $4T and pinned every real capex
    # line to the floor. True dollars, log axis, slopes compared — nothing to
    # explain away and nothing a reader has to divide back out.
    jc, ji = const.get("jaws_capex") or {}, const.get("jaws_issuance") or {}
    jaws_cap = [(r["q"], r["value"]) for r in (jc.get("series") or []) if r["q"] in qset]
    iss = [(r["q"], r["value"]) for r in (ji.get("series") or [])
           if r["q"] in qset and r["value"] > 0] or None
    growth = None
    if iss and jaws_cap:
        first = next((q for q, v in iss), None)
        jaws_cap = [(q, v) for q, v in jaws_cap
                    if trend._cq_sort(q) >= trend._cq_sort(first)]
        if jaws_cap and jaws_cap[0][1] > 0 and iss[0][1] > 0:
            growth = {"capex_x": jaws_cap[-1][1] / jaws_cap[0][1],
                      "credit_x": iss[-1][1] / iss[0][1],
                      "from": first, "to": iss[-1][0]}

    vals = [v for _, v in tser] + [v for d in buckets.values() for _, v in d["rows"]]
    if iss:
        vals += [v for _, v in iss] + [v for _, v in jaws_cap]
    vals = [v for v in vals if v > 0]
    return {
        "quarters": [q for q, _ in tser],
        "total": tser,
        "capex_panel": cap,
        "buckets": buckets,
        "jaws_capex": jaws_cap, "jaws_capex_panel": jc,
        "issuance": iss, "growth": growth, "issuance_panel": ji,
        "commitments_panel": panel.get("commitments_panel") or {},
        "breadth": panel.get("breadth_series") or [],
        "states": _states_of(total.get("observations")),
        "state": total.get("state"),
        "bands_measured_on": snap.get("bands_measured_on"),
        "log": True,
        "lo": (min(vals) / 1.6 if vals else LOG_FLOOR),
        "hi": (max(vals) * 1.6 if vals else 1.0),
    }


def composite(snap, width=1240, height=560):
    """VIEW 0 — the aggregate, one chart.

    Five legs on one time axis: panel TTM capex with the phase state shaded
    behind it, the bucket sums beneath at reduced weight, the credit-issuance
    TTM rebased onto capex for the SAME names (the jaws), the forward-commitment
    leg (or its refusal), and the breadth census as a strip along the bottom.
    Everything a reader needs to say what the panel is doing, and nothing that
    has to be taken on trust — every point names its members.
    """
    m = composite_model(snap)
    if m is None:
        return empty("no constant-membership panel of at least {} quarters"
                     .format(trend.MIN_PANEL_QUARTERS), width, 160)

    cap, jc = m["capex_panel"], m["jaws_capex_panel"]
    iss, jaws_cap, g = m["issuance"], m["jaws_capex"], m["growth"]
    tser = [(q, v, "{} names, constant".format(cap.get("member_count")))
            for q, v in m["total"]]

    strip_h, strip_gap = 16, 26
    f = Frame(m["quarters"], m["lo"], m["hi"], width=width,
              height=height - strip_h - strip_gap,
              left=72, right=146, top=52, bottom=46, log=m["log"])

    body = ["<text x='{}' y='26' font-size='14' font-weight='700' fill='#1a1a1a'>"
            "AI capex — the panel, one chart</text>".format(f.l)]
    body.append("<text x='{}' y='42' font-size='11' fill='#777'>TTM levels, true dollars, "
                "<tspan font-weight='600'>log scale</tspan> · "
                "<tspan font-weight='600'>constant membership</tspan>: {} names, {}–{} · "
                "bands stamped {}{}</text>".format(
                    f.l, cap.get("member_count"), _e(cap.get("first_quarter")),
                    _e(cap.get("last_quarter")), _e(m["bands_measured_on"]),
                    "" if not g else
                    " · jaws {}–{}: capex ×{:.1f} vs credit ×{:.1f} for the same {} names"
                    .format(_e(g["from"]), _e(g["to"]), g["capex_x"], g["credit_x"],
                            jc.get("member_count", 0))))

    body.append(phase_bands(f, m["states"]))
    body.append(grid(f))
    body.append(quarter_axis(f))

    cpanel = m["commitments_panel"]
    if cpanel.get("status", "").startswith("REFUSED"):
        body.append(refusal(f.l, f.t + f.ph - 34,
                            "forward commitments: no panel line — {}"
                            .format(cpanel.get("detail", "")[:150]), f.pw - 8))
    labels = []

    def _lab(rows, text, color):
        labels.append((f.y(rows[-1][1]), f.x(rows[-1][0]) + 7, text, color))

    for b in ("hyperscaler", "builder", "reit"):
        if b in m["buckets"]:
            rows, c = m["buckets"][b]["rows"], SERIES_COLORS.get(b, "#666")
            n = m["buckets"][b]["member_count"]
            body.append(line(f, [(q, v, "{} names".format(n)) for q, v in rows], c,
                             width=1.4, opacity=0.75,
                             title_fmt=lambda s, bb=b, nn=n: "{} {} {} ({} names)".format(
                                 bb, s[0], _money(s[1]), nn)))
            _lab(rows, b, c)
    if iss:
        oc = SERIES_COLORS["issuance"]
        n = jc.get("member_count", 0)
        # Both jaws legs in true dollars on the same log axis: the gap that
        # opens is the finding, and it is a gap between the same companies'
        # two legs, closing by slope rather than by any rescaling.
        body.append(line(f, [(q, v, "jaws capex") for q, v in jaws_cap], oc,
                         width=1.6, opacity=0.55,
                         title_fmt=lambda s: "{} capex TTM {} ({} names)".format(
                             s[0], _money(s[1]), n)))
        body.append(line(f, [(q, v) for q, v in iss], oc, width=2.0, dash="6 4",
                         title_fmt=lambda s: "{} credit TTM {} ({} names)".format(
                             s[0], _money(s[1]), n)))
        _lab(jaws_cap, "capex ({} names)".format(n), oc)
        _lab(iss, "credit (same {})".format(n), oc)
    body.append(line(f, tser, SERIES_COLORS["total"], width=3.0, dots=True))
    _lab(m["total"], "TOTAL PANEL", SERIES_COLORS["total"])

    body.append(place_labels(labels, min_gap=12.5, top=f.t + 6,
                             bottom=f.t + f.ph - 2))
    body.append(state_legend(f.l, f.t + f.ph + 34))
    body.append(breadth_strip(f, m["breadth"], height - strip_h - 4, strip_h))

    return svg(width, height, "".join(body), title="AI capex composite")


def yoy_chart(snap_series, observations, title, color, width=1160, height=250,
              band=None):
    """A YoY line with phase shading — the shape views 1–5 reuse."""
    rows = [(r["q"], r["yoy"]) for r in (snap_series or []) if r.get("yoy") is not None]
    if len(rows) < 2:
        return empty("{}: fewer than two observations".format(title), width, 110)
    vals = [v for _, v in rows]
    lo, hi = min(vals + [0.0]), max(vals)
    pad = (hi - lo) * 0.12 or 0.05
    f = Frame([q for q, _ in rows], lo - pad, hi + pad, width=width, height=height,
              left=64, right=118, top=34, bottom=40)
    body = ["<text x='{}' y='20' font-size='12' font-weight='600' fill='#333'>{}</text>"
            .format(f.l, _e(title))]
    if band:
        body.append("<text x='{}' y='20' font-size='10' fill='#888'>dead-band {}pp</text>"
                    .format(f.l + 340, band))
    body.append(phase_bands(f, _states_of(observations)))
    body.append(grid(f, fmt=lambda v: "{:+.0f}%".format(100 * v)))
    body.append(zero_line(f))
    body.append(quarter_axis(f))
    body.append(line(f, rows, color, width=2.4, dots=True,
                     title_fmt=lambda s: "{} TTM YoY {:+.1f}%".format(s[0], 100 * s[1])))
    return svg(width, height, "".join(body), title=title)


def level_chart(rows, title, color, width=1160, height=230, observations=None,
                fmt=_money):
    """A dollar-level line with optional phase shading. `rows` = [(q, v, extra)]."""
    rows = [r for r in rows if r[1] is not None]
    if len(rows) < 2:
        return empty("{}: fewer than two observations".format(title), width, 110)
    vals = [r[1] for r in rows]
    f = Frame([r[0] for r in rows], min(0.0, min(vals)), max(vals) * 1.10,
              width=width, height=height, left=66, right=118, top=32, bottom=40)
    body = ["<text x='{}' y='19' font-size='12' font-weight='600' fill='#333'>{}</text>"
            .format(f.l, _e(title))]
    if observations:
        body.append(phase_bands(f, _states_of(observations)))
    body.append(grid(f, fmt=fmt))
    body.append(quarter_axis(f))
    body.append(area(f, rows, color, opacity=0.10))
    body.append(line(f, rows, color, width=2.2, dots=True))
    return svg(width, height, "".join(body), title=title)


def state_grid(rows, quarters, width=1160, cell=15, label_w=104):
    """The phase board as a coloured grid — one row per series, one cell per quarter.

    A table of latest states answers "what is it now". This answers "how did it
    get here", which is the question the ladder exists to make answerable, and
    it does it for every series at once.
    """
    if not rows or not quarters:
        return empty("no classified history", width, 110)
    idx = {q: i for i, q in enumerate(quarters)}
    height = 34 + len(rows) * cell + 26
    body = []
    every = max(1, len(quarters) // 16)

    def _gx(i):
        return label_w + i * cell + cell / 2.0

    # Same overprint the time axis had: the grid drew every Nth label AND the
    # last, so "2026Q2" and "2026Q3" landed on top of each other.
    for i in label_picks(len(quarters), every, _gx, cell * 2.6):
        x, q = _gx(i), quarters[i]
        body.append("<text x='{:.1f}' y='24' font-size='9' fill='#777' text-anchor='middle' "
                    "transform='rotate(-40 {:.1f} 24)'>{}</text>".format(x, x, _e(q)))
    for r, (key, states, sub) in enumerate(rows):
        y = 34 + r * cell
        body.append("<text x='{}' y='{}' font-size='10' fill='#333' text-anchor='end'>{}</text>"
                    .format(label_w - 6, y + cell - 4, _e(key)))
        for q, i in idx.items():
            st = states.get(q)
            if not st:
                continue
            body.append("<rect x='{}' y='{}' width='{}' height='{}' fill='{}' "
                        "fill-opacity='0.82' stroke='#fff' stroke-width='0.6'>"
                        "<title>{} {} — {}{}</title></rect>".format(
                            label_w + i * cell, y, cell - 1, cell - 2,
                            BAND_FILLS.get(st, "#d5d5d5"), _e(key), _e(q), _e(st),
                            _e(" · " + sub if sub else "")))
    body.append(state_legend(label_w, height - 8))
    return svg(width, height, "".join(body), title="phase grid")


SUPPLIER_CAPEX_SUFFIX = " · own capex"


def issuer_rows_for_grid(snap):
    """Rows for `state_grid`, ordered aggregate-first then worst-state-first.

    **A supplier's primary row is its DATACENTER REVENUE phase, not its capex.**
    The board previously gave a supplier's own capex the same weight and the
    same look as a hyperscaler's, and they do not mean the same thing. NVDA's
    capex is a ~$7B series covering its offices and test equipment; its
    datacenter revenue is $277.8B and is the other side of the hyperscalers'
    invoice. Reading a DECELERATING on the first as a bend in the buildout is a
    category error, and the board's layout invited it — the dcrev phase lived
    on a different page.

    So suppliers show `dcrev` first, and their capex follows as a clearly
    labelled secondary row rather than being hidden: it is still a real series,
    it is just not a buildout signal.
    """
    order = {phases.STATE_CONTRACTING: 0, phases.STATE_DECELERATING: 1,
             phases.STATE_PLATEAU: 2, phases.STATE_ACCELERATING: 3}
    rows, quarters = [], set()

    def add(key, obs, sub):
        st = _states_of(obs)
        if st:
            rows.append((key, st, sub))
            quarters.update(st)

    add("TOTAL PANEL", (snap.get("total") or {}).get("observations"), "matched membership")
    for b, bk in sorted((snap.get("buckets") or {}).items()):
        add("bucket:" + b, bk.get("observations"), "{} members".format(bk.get("member_count")))

    legs = ((snap.get("suppliers") or {}).get("legs") or {})
    issuers = sorted((snap.get("issuers") or {}).items(),
                     key=lambda kv: (order.get(kv[1]["state"], 9), kv[0]))
    for tick, iss in issuers:
        leg = legs.get(tick)
        if leg and leg.get("dc_observations"):
            add(tick, leg["dc_observations"],
                "supplier · DATACENTER REVENUE — the buildout series")
            add(tick + SUPPLIER_CAPEX_SUFFIX, iss.get("observations"),
                "supplier's OWN capex — not a buildout signal")
            continue
        add(tick, iss.get("observations"),
            "{}{}".format(iss["bucket"], " · MIRROR, never alerts"
                          if iss["bucket"] == "mirror" else ""))
    return rows, sorted(quarters, key=trend._cq_sort)
