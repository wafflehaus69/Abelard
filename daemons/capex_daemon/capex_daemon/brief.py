"""The phase page — the snapshot board as a PDF, house pattern.

`charts.build_pdf` already assembles the CD-2 thesis layer from matplotlib PNGs
and keeps doing so. This adds the PHASE PAGE the order asks for, which the PNG
pipeline cannot carry: vector art built from the same model the dashboard draws,
and the state tables beside it, using the `abelard_common.render.pdf` helpers
every other daemon's brief uses.

**The composite is drawn from `svgcharts.composite_model`, not redrawn.** The
dashboard renders that model to SVG and this renders it to ReportLab vector
art. One model, two renderers — which is the same rule the snapshot enforces one
level up, and the reason the PDF cannot quietly disagree with the front page.
"""
from . import phases, svgcharts, trend

PAGE_W = 612 - 44 - 44          # letter minus the house margins


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


def _pct(v, d=1):
    return "—" if v is None else "{:+.{d}f}%".format(100 * v, d=d)


def _hex(c):
    from reportlab.lib import colors
    return colors.HexColor(c)


def composite_drawing(snap, width=PAGE_W, height=210):
    """The View 0 composite as vector art, from the shared model."""
    from reportlab.graphics.shapes import Drawing, Line, PolyLine, Rect, String

    m = svgcharts.composite_model(snap)
    d = Drawing(width, height)
    if m is None:
        d.add(String(4, height / 2, "no constant-membership panel", fontSize=8,
                     fillColor=_hex("#999999")))
        return d

    L, R, T, B = 46, 92, 16, 26
    pw, ph = width - L - R, height - T - B
    qs = m["quarters"]
    xi = {q: i for i, q in enumerate(qs)}
    n = max(1, len(qs) - 1)
    lo, hi = m["lo"], m["hi"] or 1.0

    def X(q):
        return L + pw * (xi[q] / n)

    log = m.get("log", False)

    def Y(v):                                         # PDF origin is bottom-left
        return B + ph * svgcharts.norm(v, lo, hi, log)

    step = pw / n
    for q in qs:                                      # phase shading
        fill = svgcharts.BAND_FILLS.get(m["states"].get(q))
        if fill:
            d.add(Rect(max(L, X(q) - step / 2), B, step, ph, fillColor=_hex(fill),
                       fillOpacity=svgcharts.BAND_ALPHA, strokeColor=None))
    gticks = svgcharts.log_ticks(lo, hi) if log else svgcharts._nice_ticks(lo, hi, 4)
    for gv in gticks:                                 # gridlines + y labels
        if not lo <= gv <= hi:
            continue
        d.add(Line(L, Y(gv), L + pw, Y(gv), strokeColor=_hex("#dcdcdc"), strokeWidth=0.4))
        d.add(String(L - 4, Y(gv) - 2.5, _money(gv), fontSize=5.5,
                     fillColor=_hex("#777777"), textAnchor="end"))
    every = max(1, len(qs) // 9)
    for i, q in enumerate(qs):
        if i % every and i != len(qs) - 1:
            continue
        d.add(String(X(q), B - 9, q, fontSize=5.5, fillColor=_hex("#666666"),
                     textAnchor="middle"))

    def poly(rows, color, w, dash=None, opacity=1.0):
        pts = []
        for q, v in rows:
            pts += [X(q), Y(v)]
        pl = PolyLine(pts, strokeColor=_hex(color), strokeWidth=w,
                      strokeOpacity=opacity)
        if dash:
            pl.strokeDashArray = dash
        d.add(pl)

    labels = []
    for b in ("hyperscaler", "builder", "reit"):
        if b in m["buckets"]:
            rows, c = m["buckets"][b]["rows"], svgcharts.SERIES_COLORS.get(b, "#666666")
            poly(rows, c, 0.7, opacity=0.75)
            labels.append((Y(rows[-1][1]), b, c))
    if m["issuance"]:
        oc, jn = svgcharts.SERIES_COLORS["issuance"], m["jaws_capex_panel"].get("member_count", 0)
        poly(m["jaws_capex"], oc, 0.8, opacity=0.55)
        poly(m["issuance"], oc, 1.1, dash=[3, 2])
        labels.append((Y(m["jaws_capex"][-1][1]), "capex ({})".format(jn), oc))
        labels.append((Y(m["issuance"][-1][1]), "credit (same {})".format(jn), oc))
    poly(m["total"], svgcharts.SERIES_COLORS["total"], 1.6)
    labels.append((Y(m["total"][-1][1]), "TOTAL PANEL", svgcharts.SERIES_COLORS["total"]))

    labels.sort(reverse=True)                          # de-collide downward
    ys = [y for y, _t, _c in labels]
    for i in range(1, len(ys)):
        ys[i] = min(ys[i], ys[i - 1] - 7.0)
    for (_orig, text, color), y in zip(labels, ys):
        d.add(String(L + pw + 4, y - 2, text, fontSize=5.8, fillColor=_hex(color)))

    for row in m["breadth"]:                           # breadth strip
        q = row["q"]
        if q not in xi:
            continue
        net = row.get("net_direction", 0)
        c = svgcharts.BREADTH_COLORS.get((net > 0) - (net < 0), "#8a8a8a")
        d.add(Rect(max(L, X(q) - step / 2), 2, max(0.4, step - 0.4), 5,
                   fillColor=_hex(c), fillOpacity=0.30 + 0.10 * min(4, abs(net)),
                   strokeColor=None))
    return d


def _state_table(rows, styles, col_w, repeat_header=False):
    """A state table. `repeat_header` makes it splittable across pages.

    A Table nested inside `section_box` cannot split, so a long one overflows
    the frame and ReportLab refuses the build — which is how the issuer table
    (30 rows, 811pt) failed the first time. Long tables therefore go into the
    story directly, with the header row repeated on each page.
    """
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph, Table, TableStyle

    data = [[Paragraph("<b>{}</b>".format(h), styles["Body"]) for h in
             ("Series", "State", "TTM", "TTM YoY", "Qtrs", "Members", "Band")]]
    style = [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eaf0")),
             ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#c9ced6")),
             ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
             ("FONTSIZE", (0, 0), (-1, -1), 7.5),
             ("LEFTPADDING", (0, 0), (-1, -1), 4),
             ("RIGHTPADDING", (0, 0), (-1, -1), 4),
             ("TOPPADDING", (0, 0), (-1, -1), 2.5),
             ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5),
             ("ALIGN", (2, 1), (-1, -1), "RIGHT")]
    for i, r in enumerate(rows, start=1):
        data.append([Paragraph(c, styles["Body"]) for c in r[:-1]])
        style.append(("TEXTCOLOR", (1, i), (1, i), colors.HexColor(r[-1])))
        style.append(("FONTNAME", (1, i), (1, i), styles["Body"].fontName))
    t = Table(data, colWidths=col_w, repeatRows=1 if repeat_header else 0)
    t.setStyle(TableStyle(style))
    return t


def phase_page(snap, out_path, title="Capex Daemon — phase page"):
    """Build the one-page phase board. Returns the written path."""
    from abelard_common.render import pdf as housepdf
    from reportlab.platypus import Paragraph, Spacer

    st = housepdf.default_styles()
    story = [Paragraph("AI capex — phase page", st["Title"])]

    total, panel = snap["total"], (snap.get("panel") or {})
    cap = ((panel.get("constant") or {}).get("capex") or {})
    m = svgcharts.composite_model(snap)
    g = (m or {}).get("growth")
    story.append(Paragraph(
        "Panel state <b>{}</b> · TTM {} · TTM YoY {} · dead-bands stamped {} · "
        "levels in true dollars on a <b>log</b> axis over constant membership "
        "({} names, {}–{}, {:.1f}% of reported dollars){}"
        .format(total["state"], _money(total.get("ttm")), _pct(total.get("latest_yoy")),
                snap.get("bands_measured_on"), cap.get("member_count"),
                cap.get("first_quarter"), cap.get("last_quarter"),
                100 * cap.get("coverage", 0),
                "" if not g else
                " · <b>jaws {}–{}: capex ×{:.1f} against credit ×{:.1f}</b> for the same "
                "{} names".format(g["from"], g["to"], g["capex_x"], g["credit_x"],
                                  (m["jaws_capex_panel"] or {}).get("member_count", 0))),
        st["Sub"]))
    story.append(composite_drawing(snap))
    story.append(Spacer(1, 8))

    # --- aggregates -------------------------------------------------------
    rows = []
    tobs = (total.get("observations") or [])
    tl = tobs[-1] if tobs else {}
    rows.append(("<b>TOTAL PANEL</b>", total["state"], _money(total.get("ttm")),
                 _pct(total.get("latest_yoy")), str(tl.get("quarters_in_state", "—")),
                 str(total.get("member_count", "—")), "{}pp".format(total.get("band")),
                 svgcharts.BAND_FILLS.get(total["state"], "#333333")))
    for b, bk in sorted(snap["buckets"].items()):
        obs = bk.get("observations") or []
        last = obs[-1] if obs else {}
        rows.append(("bucket:" + b, bk["state"], _money(bk.get("ttm")),
                     _pct(bk.get("latest_yoy")), str(last.get("quarters_in_state", "—")),
                     str(bk.get("member_count", "—")), "{}pp".format(bk.get("band")),
                     svgcharts.BAND_FILLS.get(bk["state"], "#333333")))
    story += housepdf.section_box(
        "Aggregates", [_state_table(rows, st, [96, 96, 66, 58, 30, 52, 44])], st)

    # --- issuers, worst state first --------------------------------------
    order = {phases.STATE_CONTRACTING: 0, phases.STATE_DECELERATING: 1,
             phases.STATE_PLATEAU: 2, phases.STATE_ACCELERATING: 3,
             phases.STATE_INSUFFICIENT: 4}
    irows = []
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: (order.get(kv[1]["state"], 9), kv[0])):
        flags = " ".join(iss["flags"] or [])
        name = "<b>{}</b>{}".format(tick, " · MIRROR" if iss["bucket"] == "mirror" else "")
        irows.append((name, iss["state"] + (" " + flags if flags else ""),
                      _money(iss["ttm_capex"]), _pct(iss["latest_yoy"]),
                      str(iss["quarters_in_state"] or "—"), iss["bucket"],
                      "{}pp".format(iss.get("band") or "—"),
                      svgcharts.BAND_FILLS.get(iss["state"], "#666666")))
    story.append(Paragraph("Issuers — worst state first", st["H2"]))
    story.append(_state_table(irows, st, [64, 126, 66, 58, 28, 58, 44],
                              repeat_header=True))
    story.append(Spacer(1, 9))

    # --- what is refused, stated rather than omitted ----------------------
    notes = []
    cp = panel.get("commitments_panel") or {}
    if cp.get("status", "").startswith("REFUSED"):
        notes.append("<b>Forward commitments, panel total: {}.</b> {}."
                     .format(cp["status"], cp["detail"]))
    lag = cap.get("lagging") or []
    if lag:
        notes.append("<b>Behind on filing, absent from the last point:</b> " + ", ".join(
            "{} (last {}, {})".format(x["ticker"], x["last_quarter"],
                                      _money(x["last_value"]))
            for x in sorted(lag, key=lambda z: -z["last_value"])[:6]) + ".")
    for b, bk in sorted(snap["buckets"].items()):
        if bk["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP:
            notes.append("<b>{}</b>: matched membership fell to {} member(s), below the "
                         "{}-member floor — no state published.".format(
                             b, bk.get("member_count"), bk.get("min_members")))
    if notes:
        story += housepdf.section_box(
            "Refused and withheld", [Paragraph(n, st["Body"]) for n in notes], st,
            accent="#8a6d1a")

    story.append(Paragraph(
        "States are set by the ladder on TTM YoY: N=2 same-direction out-of-band moves "
        "to enter a state, N=3 to confirm; SOFTENING is a flag on the first out-of-band "
        "decline and never a state; CONTRACTING is the only level-based state. "
        "Aggregates key on calendar quarters and use matched membership; plotted levels "
        "additionally hold membership constant. MIRROR names classify and are shown but "
        "never alert.", st["Foot"]))
    return housepdf.build_pdf(out_path, story, title=title)
