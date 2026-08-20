"""C6 — chart artifacts. matplotlib PNGs plus a ReportLab PDF section.

Every chart carries its composition and its coverage caption. A bucket subtotal
without its concentration share, or a divergence bar without the names excluded
from it, is the kind of headline E14 exists to prevent — so the captions are
generated from the same objects that produce the numbers, not written by hand.

No dashboard integration in this order.
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from . import config, divergence, phases  # noqa: E402

BUCKET_COLORS = {"hyperscaler": "#3b6ea5", "builder": "#c2703d", "reit": "#5b8c5a"}
_GRID = {"color": "#dddddd", "linewidth": 0.6}


def _finish(fig, ax, title, caption, path):
    ax.set_title(title, fontsize=12, pad=12)
    ax.grid(axis="y", **_GRID)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    if caption:
        fig.text(0.01, 0.015, caption, fontsize=7.2, color="#555555", wrap=True)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def chart_bucket_capex(comp, outdir):
    """Per-bucket TTM capex with concentration disclosed in the caption (E14)."""
    fig, ax = plt.subplots(figsize=(7.2, 4.0))
    buckets = [b for b in divergence.BUCKET_ORDER if comp["buckets"][b]["n"]]
    vals = [comp["buckets"][b]["subtotal"] / 1e9 for b in buckets]
    ax.bar(buckets, vals, color=[BUCKET_COLORS[b] for b in buckets])
    for i, v in enumerate(vals):
        ax.text(i, v, "${:,.0f}B".format(v), ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("TTM capex (USD bn)")
    parts = []
    for b in buckets:
        d = comp["buckets"][b]
        share = d["top2_share"]
        parts.append("{}: n={}, top-2 {}".format(
            b, d["n"], "{:.0f}%".format(100 * share) if share else "n/a"))
    caption = "Total ${:,.0f}B. {}. {}. Excluded: {}".format(
        comp["total"] / 1e9, " | ".join(parts), comp["coverage"],
        ", ".join(t for t, _ in comp["excluded"]) or "none")
    return _finish(fig, ax, "AI capex by bucket — TTM", caption,
                   os.path.join(outdir, "bucket_capex_ttm.png"))


def chart_divergence(views, outdir):
    """Credit-to-capex per issuer, with withheld ratios shown as gaps, not zeros."""
    rows = [v for v in views if v.ttm_capex and v.ratio is not None]
    rows.sort(key=lambda v: -(v.ratio or 0))
    withheld = [v for v in views if v.ttm_capex and v.ratio is None]
    fig, ax = plt.subplots(figsize=(7.6, 4.4))
    ax.bar([v.ticker for v in rows], [100 * v.ratio for v in rows],
           color=[BUCKET_COLORS.get(v.bucket, "#888888") for v in rows])
    ax.axhline(100, color="#b03030", linewidth=1.0, linestyle="--")
    ax.text(len(rows) - 0.5, 103, "debt = capex", fontsize=7.5, color="#b03030", ha="right")
    ax.set_ylabel("TTM debt issuance / TTM capex (%)")
    ax.tick_params(axis="x", labelsize=8, rotation=45)
    caption = ("Ratio withheld (shown as absent, never zero) for: {}. "
               "Colour = bucket. TTM only: quarterly ratios swing 0% to 148% "
               "on issuance lumpiness.").format(
        ", ".join("{} [{}]".format(v.ticker, ",".join(v.statuses)) for v in withheld) or "none")
    return _finish(fig, ax, "Credit-to-capex divergence — TTM", caption,
                   os.path.join(outdir, "divergence_ttm.png"))


def chart_commitments(views, outdir):
    """Forward-commitment stock, with uncovered issuers named in the caption."""
    covered, uncovered = [], []
    for v in views:
        c = v.commitments
        if c and c.status == "COVERED" and c.latest:
            covered.append((v.ticker, c.latest.value, v.bucket))
        elif c and c.status != "ABSENT":
            uncovered.append("{} [{}]".format(v.ticker, c.status))
    covered.sort(key=lambda r: -r[1])
    covered = covered[:12]
    fig, ax = plt.subplots(figsize=(7.6, 4.2))
    ax.barh([r[0] for r in covered][::-1], [r[1] / 1e9 for r in covered][::-1],
            color=[BUCKET_COLORS.get(r[2], "#888888") for r in covered][::-1])
    ax.set_xlabel("Forward purchase obligations (USD bn)")
    caption = "Contracted, unspent. Uncovered: {}.".format(", ".join(uncovered) or "none")
    return _finish(fig, ax, "Forward commitment stock — latest", caption,
                   os.path.join(outdir, "forward_commitments.png"))


def build_pdf(paths, outpath, title="Capex Daemon — CD-2 thesis layer"):
    """Assemble the PNGs into a PDF section (ReportLab, matching the NW pattern)."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer

    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(outpath, pagesize=letter,
                            leftMargin=0.7 * inch, rightMargin=0.7 * inch,
                            topMargin=0.7 * inch, bottomMargin=0.7 * inch)
    story = [Paragraph(title, styles["Title"]), Spacer(1, 6),
             Paragraph("Sums and composition. No weighted indices. Coverage status "
                       "travels with every figure.", styles["Italic"]), Spacer(1, 14)]
    for p in paths:
        if not p or not os.path.exists(p):
            continue
        story.append(Image(p, width=6.9 * inch, height=6.9 * inch * 0.55))
        story.append(Spacer(1, 16))
    doc.build(story)
    return outpath


STATE_FILL = {
    "ACCELERATING": "#1d6f42", "PLATEAU": "#6b6b6b",
    "DECELERATING": "#a8600f", "CONTRACTING": "#9b1c1c",
}


def chart_hayes_panel(snap, outdir):
    """P5's brief chart: per-bucket TTM YoY with phase-state shading."""
    fig, ax = plt.subplots(figsize=(7.8, 4.4))
    for b, bk in sorted(snap["buckets"].items()):
        ser = bk.get("yoy_series") or []
        if len(ser) < 2:
            continue
        xs = [p["q"] for p in ser][-16:]
        ys = [100 * p["yoy"] for p in ser][-16:]
        ax.plot(range(len(xs)), ys, marker="o", markersize=3,
                color=BUCKET_COLORS.get(b, "#888"), label="{} ({})".format(b, bk["state"]))
    ax.axhline(0, color="#9b1c1c", linewidth=1.0, linestyle="--")
    ax.set_ylabel("TTM YoY capex growth (%)")
    ax.legend(fontsize=8, frameon=False)
    ticks = None
    for b, bk in sorted(snap["buckets"].items()):
        ser = bk.get("yoy_series") or []
        if len(ser) >= 2:
            ticks = [p["q"] for p in ser][-16:]
            break
    if ticks:
        ax.set_xticks(range(len(ticks)))
        ax.set_xticklabels(ticks, rotation=60, fontsize=7)
    t = snap["total"]
    caption = ("Total panel: {} at {:+.1f}% TTM YoY over {} matched members. "
               "Dashed line is zero growth — below it is CONTRACTING, the only "
               "level-based state. Bands stamped {}.").format(
        t["state"], 100 * (t.get("latest_yoy") or 0), t.get("member_count"),
        snap.get("bands_measured_on"))
    return _finish(fig, ax, "Capex phase — per-bucket TTM YoY", caption,
                   os.path.join(outdir, "hayes_panel.png"))


def render_all(views, comp, outdir=None, snap=None):
    outdir = outdir or config.artifact_path("", sub="charts")
    os.makedirs(outdir, exist_ok=True)
    paths = [chart_bucket_capex(comp, outdir),
             chart_divergence(views, outdir),
             chart_commitments(views, outdir)]
    if snap:
        paths.insert(0, chart_hayes_panel(snap, outdir))
    pdf = build_pdf(paths, os.path.join(outdir, "cd2_thesis_layer.pdf"))
    return paths, pdf
