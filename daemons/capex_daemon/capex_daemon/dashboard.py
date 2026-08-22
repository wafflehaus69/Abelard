"""P4 — the capex dashboard. Read-only, port 8788.

**Stack note, flagged rather than silently resolved (E3).** The order specified
"Flask on :8788, SM dashboard pattern (same stack)". Those conflict: SM's
dashboard is stdlib `ThreadingHTTPServer`, and Flask appears NOWHERE in the
monorepo — no pyproject declares it, no module imports it. Following "Flask"
would add a dependency the house has never used, so this follows "same stack"
and the disk. Rendering is kept pure (every view is a function returning HTML
from the snapshot), so swapping in a Flask shim later touches only `serve()`.

**One computation, many renderers.** Every view reads the persisted snapshot and
recomputes nothing. The DB is opened `mode=ro` per request, so there are zero
write endpoints by construction.

**Provenance is not optional.** Every published number carries its resolved
concept, derivation and coverage status in a `title` attribute — hover or click
and the figure explains where it came from.
"""
import html
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import config, phases, snapshot, svgcharts, trend

PORT = 8788

STATE_COLORS = {
    phases.STATE_ACCELERATING: "#1d6f42",
    phases.STATE_PLATEAU: "#6b6b6b",
    phases.STATE_DECELERATING: "#a8600f",
    phases.STATE_CONTRACTING: "#9b1c1c",
    phases.STATE_INSUFFICIENT: "#b9b9b9",
    trend.STATE_INSUFFICIENT_MEMBERSHIP: "#b9b9b9",
}

CSS = """
body{font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#fafafa;color:#1a1a1a}
header{background:#1a1a1a;color:#fff;padding:10px 18px}
header a{color:#9ecbff;margin-right:16px;text-decoration:none;font-size:13px}
header a.on{color:#fff;font-weight:600;text-decoration:underline}
main{padding:18px;max-width:1500px}
h2{font-size:16px;margin:22px 0 8px}
table{border-collapse:collapse;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.08);margin-bottom:16px}
th,td{padding:6px 10px;border-bottom:1px solid #eee;text-align:left;font-variant-numeric:tabular-nums}
th{background:#f2f2f2;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
td.num{text-align:right}
.pill{display:inline-block;padding:2px 8px;border-radius:10px;color:#fff;font-size:11px;font-weight:600}
.flag{font-size:10px;background:#eee;border-radius:3px;padding:1px 5px;margin-left:4px;color:#555}
.cov{font-size:11px;color:#8a6d1a;background:#fdf6e3;padding:1px 5px;border-radius:3px}
.note{color:#666;font-size:12px;margin:6px 0 14px;max-width:900px}
.warn{background:#fff4f4;border-left:3px solid #9b1c1c;padding:8px 12px;margin:10px 0;font-size:13px}
.spark{font-family:ui-monospace,monospace;letter-spacing:-1px;color:#444}
.hero{margin:4px 0 14px}
svg{display:block;max-width:100%;margin:0 0 14px}
.chartnote{color:#888;font-size:11px;margin:-8px 0 16px}
.mapped{background:#eef3fb;border-left:3px solid #1f4e9c;padding:9px 12px;margin:10px 0;font-size:13px}
[title]{cursor:help;border-bottom:1px dotted #bbb}
"""

VIEWS = [("/", "The aggregate"), ("/hayes", "Hayes panel"), ("/phases", "Phase board"),
         ("/divergence", "Divergence"), ("/buckets", "Bucket drilldowns"),
         ("/commitments", "Forward commitments"), ("/suppliers", "Suppliers")]


def _esc(s):
    return html.escape(str(s if s is not None else ""))


def _money(v):
    if v is None:
        return "—"
    a = abs(v)
    if a >= 1e9:
        return "${:,.2f}B".format(v / 1e9)
    if a >= 1e6:
        return "${:,.0f}M".format(v / 1e6)
    return "${:,.0f}".format(v)


def _pct(v, digits=1):
    return "—" if v is None else "{:+.{d}f}%".format(100 * v, d=digits)


def _pill(state):
    c = STATE_COLORS.get(state, "#777")
    return "<span class='pill' style='background:{}'>{}</span>".format(c, _esc(state))


def _flags(fl):
    return "".join("<span class='flag'>{}</span>".format(_esc(f)) for f in (fl or []))


def _spark(vals, width=24):
    if not vals:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    v = vals[-width:]
    lo, hi = min(v), max(v)
    rng = (hi - lo) or 1.0
    return "".join(blocks[min(7, int((x - lo) / rng * 7))] for x in v)


def _page(title, active, body):
    nav = "".join("<a href='{}' class='{}'>{}</a>".format(
        p, "on" if p == active else "", _esc(n)) for p, n in VIEWS)
    return ("<!doctype html><meta charset='utf-8'><title>Capex — {t}</title>"
            "<style>{css}</style><header><b>Capex Daemon</b> &nbsp; {nav}</header>"
            "<main>{body}</main>").format(t=_esc(title), css=CSS, nav=nav, body=body)


def _bucket_num(bk, key, fmt):
    """A bucket figure, or a withheld marker when the sum is not a sum.

    CD-PH1 caught the REIT bucket publishing +56.7% off ONE member — EQIX
    wearing a bucket label — and added the membership floor. The floor set the
    STATE, but the TTM and YoY cells kept printing the number anyway, which is
    the same defect one column to the right. A figure that is not a bucket
    figure does not get shown as one.
    """
    if bk.get("state") == trend.STATE_INSUFFICIENT_MEMBERSHIP:
        return ("<span title='withheld: {} of {} members required — this would be "
                "{} own number wearing a bucket label'>—</span>".format(
                    bk.get("member_count"), bk.get("min_members"),
                    _esc(", ".join(bk.get("membership") or []) + "&#39;s")))
    return fmt(bk.get(key))


def _provenance(iss):
    cov = ", ".join(iss.get("coverage") or []) or "OK"
    return ("bucket={} | band={}pp | state entered {} ({} quarters) | "
            "coverage: {} | quarters observed: {}").format(
        iss["bucket"], iss.get("band"), iss.get("entered") or "—",
        iss.get("quarters_in_state") or 0, cov, len(iss.get("quarters") or []))


def _commitments_chart(snap, title, top=8):
    """Per-issuer commitment stocks. The sum is refused; the series are not."""
    ser = {}
    for tick, iss in (snap.get("issuers") or {}).items():
        pts = [(p["q"], p["value"]) for p in (iss["commitments"].get("points_cq") or [])]
        if len(pts) >= 2:
            ser[tick] = pts
    ser = dict(sorted(ser.items(), key=lambda kv: -kv[1][-1][1])[:top])
    return svgcharts.multi_line_chart(
        ser, title, height=320,
        note="dashed = gaps in disclosure, not flat quarters")


def _commitments_refusal(snap):
    cp = (snap.get("panel") or {}).get("commitments_panel") or {}
    if not cp.get("status", "").startswith("REFUSED"):
        return ""
    return ("<div class='warn'><b>Panel commitment total: {}.</b> {}. The per-issuer "
            "series above are unaffected — what fails is <i>adding</i> them, not "
            "observing them.</div>".format(_esc(cp["status"]), _esc(cp["detail"])))


# ---------------- view 0: the aggregate ----------------

def view_aggregate(snap):
    """One chart, the front page. Everything else is a drilldown from here."""
    t = snap["total"]
    panel = snap.get("panel") or {}
    out = ["<div class='hero'>", svgcharts.composite(snap), "</div>"]

    br = (panel.get("breadth_series") or [])
    latest_br = br[-1] if br else {}
    net = latest_br.get("net_direction", 0)
    cap = ((panel.get("constant") or {}).get("capex") or {})
    ic = ((panel.get("constant") or {}).get("issuance") or {})
    out.append(
        "<p class='note'><b>Reading it.</b> The heavy black line is panel "
        "trailing-twelve-month capex, drawn on the phase state the classifier assigned it "
        "(shaded behind). The thin lines are the bucket sums. The two orange lines are the "
        "<b>jaws</b>: capex and credit issuance for the <i>same</i> names over the "
        "<i>same</i> window, solid and dashed. Everything is in <b>true dollars on a log "
        "axis</b> — nothing is rescaled to fit, so a steeper line is genuinely growing "
        "faster and the jaws open by slope rather than by any factor a reader has to divide "
        "back out. The strip along the bottom is breadth: how many member names are "
        "turning, which the dollar-weighted line cannot show.</p>")
    out.append(
        "<p class='note'><b>Why these names.</b> Every line here is a <b>level</b>, and a "
        "level summed over changing membership shows arrivals as growth — the panel's "
        "matched membership runs 1 to 12 names across its 66 quarters. So each line holds "
        "its membership <b>constant</b> across the whole window, and the window chosen is "
        "the longest one whose members still cover {}% of the dollars reported at its end. "
        "Capex: <b>{}</b> ({}). Credit, and the jaws: <b>{}</b> ({}). "
        "The YoY charts on the other views are immune to this and use the full matched "
        "membership.</p>".format(
            int(100 * trend.COVERAGE_FLOOR),
            ", ".join(cap.get("members") or []) or "—",
            "{:.1f}% of reported dollars".format(100 * cap.get("coverage", 0)),
            ", ".join(ic.get("members") or []) or "—",
            "{:.1f}%".format(100 * ic.get("coverage", 0))))

    lag = cap.get("lagging") or []
    if lag:
        out.append(
            "<div class='warn'><b>Behind on filing, so absent from the last point:</b> "
            "{}. These are not contractions — the issuer has not filed the quarter yet. "
            "{} alone is {} of last-reported capex.</div>".format(
                ", ".join("{} (last {}, {})".format(
                    _esc(x["ticker"]), _esc(x["last_quarter"]), _money(x["last_value"]))
                    for x in sorted(lag, key=lambda z: -z["last_value"])[:6]),
                _esc(max(lag, key=lambda z: z["last_value"])["ticker"]),
                _money(max(lag, key=lambda z: z["last_value"])["last_value"])))

    if net <= 0 and t["state"] in (phases.STATE_ACCELERATING, phases.STATE_PLATEAU):
        out.append("<div class='warn'><b>Breadth disagrees with the level.</b> The panel is "
                   "{} while breadth runs net {:+d} — the aggregate is being carried by its "
                   "largest members while the majority of names turn. Both readings are "
                   "published because neither is the whole answer.</div>".format(
                       _esc(t["state"]), net))

    out.append("<table><tr><th>Series</th><th>State</th><th class='num'>TTM</th>"
               "<th class='num'>TTM YoY</th><th class='num'>Members</th>"
               "<th class='num'>Band</th></tr>")
    out.append("<tr><td><b>TOTAL PANEL</b></td><td>{}</td><td class='num'>{}</td>"
               "<td class='num'>{}</td><td class='num' title='{}'>{}</td>"
               "<td class='num'>{}pp</td></tr>".format(
                   _pill(t["state"]), _money(t.get("ttm")), _pct(t.get("latest_yoy")),
                   _esc("matched: " + ", ".join(t.get("membership") or [])),
                   t.get("member_count"), t.get("band")))
    for b, bk in sorted(snap["buckets"].items()):
        out.append("<tr><td>{}</td><td>{}</td><td class='num'>{}</td><td class='num'>{}</td>"
                   "<td class='num' title='{}'>{}</td><td class='num'>{}pp</td></tr>".format(
                       _esc(b), _pill(bk["state"]),
                       _bucket_num(bk, "ttm", _money),
                       _bucket_num(bk, "latest_yoy", _pct),
                       _esc("matched: " + ", ".join(bk.get("membership") or [])),
                       bk.get("member_count"), bk.get("band")))
    iss = panel.get("issuance_ttm") or []
    comm = panel.get("commitments") or []
    if iss:
        out.append("<tr><td>credit issuance</td><td>—</td><td class='num'>{}</td>"
                   "<td class='num'>—</td><td class='num' title='{}'>{}</td>"
                   "<td class='num'>—</td></tr>".format(
                       _money(iss[-1]["value"]),
                       _esc("contributing: " + ", ".join(
                           panel.get("issuance_membership_latest") or [])),
                       iss[-1]["members"]))
    if comm:
        out.append("<tr><td>forward commitments</td><td>—</td><td class='num'>{}</td>"
                   "<td class='num'>—</td><td class='num' title='{}'>{}</td>"
                   "<td class='num'>—</td></tr>".format(
                       _money(comm[-1]["value"]),
                       _esc("disclosing: " + ", ".join(
                           panel.get("commitments_membership_latest") or [])),
                       comm[-1]["members"]))
    out.append("</table>")

    for b, bk in sorted(snap["buckets"].items()):
        if bk["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP:
            out.append("<div class='warn'><b>{}</b>: matched membership fell to {} member(s), "
                       "below the {}-member floor — no state published.</div>".format(
                           _esc(b), bk.get("member_count"), bk.get("min_members")))
    return _page("The aggregate", "/", "".join(out))


# ---------------- view 1: Hayes panel ----------------

def view_hayes(snap):
    out = ["<h2>Hayes panel — capex, credit, forward commitments</h2>",
           "<p class='note'>The falsifier as three co-plotted legs. The claim under test is about "
           "their <b>divergence</b>, so they are shown together and never collapsed into one number. "
           "Dead-bands stamped {}. <b>Unlike the front page, nothing here is rebased</b> — these "
           "are true dollar magnitudes, one axis each, so the legs can be sized against each "
           "other rather than only shaped against each other.</p>".format(
               _esc(snap.get("bands_measured_on")))]

    t = snap["total"]
    panel = snap.get("panel") or {}
    out.append(svgcharts.level_chart(
        [(r["q"], r["ttm"], "{} matched members".format(r["members"]))
         for r in (t.get("ttm_series") or [])],
        "Leg 1 — panel TTM capex (phase-shaded)", svgcharts.SERIES_COLORS["total"],
        observations=t.get("observations")))
    out.append(svgcharts.level_chart(
        [(r["q"], r["value"], "{} contributing issuers".format(r["members"]))
         for r in (panel.get("issuance_ttm") or [])],
        "Leg 2 — panel TTM credit issuance", svgcharts.SERIES_COLORS["issuance"]))
    out.append(_commitments_chart(snap, "Leg 3 — forward commitment stock "
                                        "(contracted, unspent), per issuer"))
    out.append("<p class='chartnote'>Shading on leg 1 only: the classifier runs on the capex "
               "series. Credit and commitments carry no phase state and are not given the "
               "appearance of one. Leg 3 is drawn per issuer because the panel sum is "
               "refused — see below.</p>")
    out.append(_commitments_refusal(snap))
    out.append("<table><tr><th>Series</th><th>State</th><th class='num'>TTM</th>"
               "<th class='num'>TTM YoY</th><th class='num'>Members</th><th>Breadth</th></tr>")
    out.append("<tr><td><b>TOTAL PANEL</b></td><td>{}</td><td class='num'>{}</td>"
               "<td class='num'>{}</td><td class='num'>{}</td><td>—</td></tr>".format(
                   _pill(t["state"]), _money(t.get("ttm")), _pct(t.get("latest_yoy")),
                   t.get("member_count")))
    for b, bk in sorted(snap["buckets"].items()):
        br = bk.get("breadth") or {}
        breadth = "ACC {} · PLAT {} · DEC {} · CONTR {} · <b>net {:+d}</b>".format(
            br.get("ACCELERATING", 0), br.get("PLATEAU", 0), br.get("DECELERATING", 0),
            br.get("CONTRACTING", 0), br.get("net_direction", 0))
        out.append("<tr><td>{}</td><td>{}</td><td class='num'>{}</td><td class='num'>{}</td>"
                   "<td class='num' title='{}'>{}</td><td>{}</td></tr>".format(
                       _esc(b), _pill(bk["state"]),
                       _bucket_num(bk, "ttm", _money),
                       _bucket_num(bk, "latest_yoy", _pct),
                       _esc("matched membership: " + ", ".join(bk.get("membership") or [])),
                       bk.get("member_count"), breadth))
    out.append("</table>")

    for b, bk in sorted(snap["buckets"].items()):
        if bk["state"] == trend.STATE_INSUFFICIENT_MEMBERSHIP:
            out.append("<div class='warn'><b>{}</b>: matched membership fell to {} member(s), "
                       "below the {}-member floor. A one-name bucket sum is that member's own "
                       "number wearing a bucket label, so no state is published.</div>".format(
                           _esc(b), bk.get("member_count"), bk.get("min_members")))

    out.append("<h2>Per-issuer legs</h2><table><tr><th>Issuer</th><th>Bucket</th>"
               "<th>State</th><th class='num'>TTM capex</th><th class='num'>TTM YoY</th>"
               "<th class='num'>credit/capex</th><th class='num'>commitments</th>"
               "<th>capex trend</th></tr>")
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: -(kv[1]["ttm_capex"] or 0)):
        comm = iss["commitments"]
        cov = "".join("<span class='cov'>{}</span> ".format(_esc(c))
                      for c in iss["coverage"] if c != "OK")
        out.append(
            "<tr><td><b>{}</b> {}</td><td>{}</td><td title='{}'>{}{}</td>"
            "<td class='num'>{}</td><td class='num'>{}</td><td class='num'>{}</td>"
            "<td class='num' title='{}'>{}</td><td class='spark'>{}</td></tr>".format(
                _esc(tick), cov, _esc(iss["bucket"]), _esc(_provenance(iss)),
                _pill(iss["state"]), _flags(iss["flags"]),
                _money(iss["ttm_capex"]), _pct(iss["latest_yoy"]),
                _pct(iss["credit_ratio"], 0),
                _esc("{} — {}".format(comm["status"], comm["detail"])),
                _money(comm["latest"]),
                _spark([q["value"] for q in iss["quarters"]])))
    out.append("</table>")
    return _page("Hayes panel", "/hayes", "".join(out))


# ---------------- view 2: phase board ----------------

def view_phases(snap):
    out = ["<h2>Phase board</h2>",
           "<p class='note'>State is set by the ladder on TTM YoY. <b>N=2</b> consecutive "
           "same-direction moves to enter a state, so a single move against the trend does not "
           "flip it — read <i>direction</i> beside <i>state</i>. SOFTENING is a flag on the first "
           "out-of-band decline, never a state. CONTRACTING is the only level-based state "
           "(TTM YoY &lt; 0).</p>"]

    rows, quarters = svgcharts.issuer_rows_for_grid(snap)
    out.append(svgcharts.state_grid(rows, quarters))
    out.append("<p class='chartnote'>Every classified series, every quarter it classifies. "
               "A blank cell is INSUFFICIENT-HISTORY, deliberately left uncoloured so that "
               "absence of a state does not read as a fifth state. Hover any cell for the "
               "series, quarter and state.</p>")

    out.append(svgcharts.yoy_chart(
        snap["total"].get("yoy_series"), snap["total"].get("observations"),
        "TOTAL PANEL — TTM YoY on its phase state", svgcharts.SERIES_COLORS["total"],
        band=snap["total"].get("band")))

    out.append("<table><tr><th>Series</th><th>State</th><th class='num'>Qtrs in state</th>"
               "<th>Entered</th><th class='num'>TTM YoY</th><th class='num'>Δ</th>"
               "<th>Last move</th><th class='num'>Band</th></tr>")
    t = snap["total"]
    tobs = (t.get("observations") or [])
    tl = tobs[-1] if tobs else {}
    out.append("<tr><td><b>TOTAL PANEL</b></td><td>{}</td><td class='num'>{}</td><td>{}</td>"
               "<td class='num'>{}</td><td class='num'>{}</td><td>{}</td>"
               "<td class='num'>{}pp</td></tr>".format(
                   _pill(t["state"]), tl.get("quarters_in_state", "—"),
                   _esc(tl.get("entered") or "—"), _pct(t.get("latest_yoy")),
                   "{:+.1f}pp".format(tl["delta"]) if tl.get("delta") is not None else "—",
                   _esc(tl.get("direction") or "—"), t.get("band")))
    for b, bk in sorted(snap["buckets"].items()):
        obs = bk.get("observations") or []
        last = obs[-1] if obs else {}
        out.append("<tr><td>bucket:{}</td><td>{}</td><td class='num'>{}</td><td>{}</td>"
                   "<td class='num'>{}</td><td class='num'>{}</td><td>{}</td>"
                   "<td class='num'>{}pp</td></tr>".format(
                       _esc(b), _pill(bk["state"]), last.get("quarters_in_state", "—"),
                       _esc(last.get("entered") or "—"),
                       _bucket_num(bk, "latest_yoy", _pct),
                       "{:+.1f}pp".format(last["delta"]) if last.get("delta") is not None else "—",
                       _esc(last.get("direction") or "—"), bk.get("band")))
    out.append("</table>")

    out.append("<h2>Issuers</h2><table><tr><th>Issuer</th><th>Bucket</th><th>State</th>"
               "<th class='num'>Qtrs</th><th>Entered</th><th class='num'>TTM YoY</th>"
               "<th class='num'>Δ</th><th>Last move</th><th class='num'>Band</th>"
               "<th>YoY history</th></tr>")
    order = {phases.STATE_CONTRACTING: 0, phases.STATE_DECELERATING: 1,
             phases.STATE_PLATEAU: 2, phases.STATE_ACCELERATING: 3,
             phases.STATE_INSUFFICIENT: 4}
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: (order.get(kv[1]["state"], 9), kv[0])):
        mirror = " <span class='flag'>MIRROR — no alerts</span>" if iss["bucket"] == "mirror" else ""
        out.append(
            "<tr><td><b>{}</b>{}</td><td>{}</td><td>{}{}</td><td class='num'>{}</td>"
            "<td>{}</td><td class='num'>{}</td><td class='num'>{}</td><td>{}</td>"
            "<td class='num'>{}pp</td><td class='spark'>{}</td></tr>".format(
                _esc(tick), mirror, _esc(iss["bucket"]),
                _pill(iss["state"]), _flags(iss["flags"]),
                iss["quarters_in_state"] or "—", _esc(iss["entered"] or "—"),
                _pct(iss["latest_yoy"]),
                "{:+.1f}pp".format(iss["latest_delta"]) if iss["latest_delta"] is not None else "—",
                _esc(iss["direction"] or "—"), iss.get("band"),
                _spark([p["yoy"] for p in iss["yoy_series"]])))
    out.append("</table>")

    trans = snap.get("transitions", [])[-25:]
    out.append("<h2>Recent transitions</h2><table><tr><th>Series</th><th>Quarter</th>"
               "<th>From</th><th>To</th><th class='num'>TTM YoY</th><th class='num'>Δ</th></tr>")
    for tr in reversed(trans):
        out.append("<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class='num'>{}</td>"
                   "<td class='num'>{}</td></tr>".format(
                       _esc(tr["series_key"]), _esc(tr["quarter"]),
                       _pill(tr["from_state"]), _pill(tr["to_state"]),
                       _pct(tr.get("yoy")),
                       "{:+.1f}pp".format(tr["delta"]) if tr.get("delta") is not None else "—"))
    out.append("</table>")
    return _page("Phase board", "/phases", "".join(out))


# ---------------- view 3: divergence ----------------

def view_divergence(snap):
    out = ["<h2>Credit-to-capex divergence</h2>",
           "<p class='note'>TTM issuance over TTM capex. <b>TTM only</b> — measured quarterly "
           "ratios swing 0% to 148% on issuance lumpiness. A withheld ratio shows its coverage "
           "status and never a zero.</p>"]
    ratio = (snap.get("panel") or {}).get("credit_ratio_series") or []
    out.append(svgcharts.level_chart(
        [(r["q"], r["ratio"], "{} contributing issuers".format(r["members"]))
         for r in ratio],
        "Panel credit-to-capex ratio, TTM over TTM", svgcharts.SERIES_COLORS["issuance"],
        observations=snap["total"].get("observations"),
        fmt=lambda v: "{:.0f}%".format(100 * v)))
    out.append("<p class='chartnote'>Shaded on the panel's <i>capex</i> phase state: the "
               "question this chart answers is whether credit is doing more of the work as "
               "the capex phase turns, so the phase belongs behind it. The ratio itself is "
               "published in the snapshot, not divided here.</p>")
    out += ["<table><tr><th>Issuer</th><th>Bucket</th><th class='num'>TTM capex</th>"
           "<th class='num'>TTM issuance</th><th class='num'>credit/capex</th>"
           "<th>Coverage</th></tr>"]
    for tick, iss in sorted(snap["issuers"].items(),
                            key=lambda kv: -(kv[1]["credit_ratio"] or -1)):
        if iss["ttm_capex"] is None:
            continue
        cov = "".join("<span class='cov'>{}</span> ".format(_esc(c))
                      for c in iss["coverage"]) or "OK"
        out.append("<tr><td><b>{}</b></td><td>{}</td><td class='num'>{}</td>"
                   "<td class='num'>{}</td><td class='num'>{}</td><td>{}</td></tr>".format(
                       _esc(tick), _esc(iss["bucket"]), _money(iss["ttm_capex"]),
                       _money(iss["ttm_issuance"]), _pct(iss["credit_ratio"], 0), cov))
    out.append("</table>")
    return _page("Divergence", "/divergence", "".join(out))


# ---------------- view 4: bucket drilldowns ----------------

def view_buckets(snap):
    out = ["<h2>Bucket drilldowns</h2>",
           "<p class='note'>Composition travels with every subtotal. Membership changes are "
           "published beside the trend, never blended into it.</p>"]
    for b, bk in sorted(snap["buckets"].items()):
        br = bk.get("breadth") or {}
        out.append("<h2>{} &nbsp; {}</h2>".format(_esc(b), _pill(bk["state"])))
        out.append(svgcharts.yoy_chart(
            bk.get("yoy_series"), bk.get("observations"),
            "{} — bucket-sum TTM YoY, matched membership".format(b),
            svgcharts.SERIES_COLORS.get(b, "#555"), height=210, band=bk.get("band")))
        out.append(svgcharts.level_chart(
            [(r["q"], r["ttm"], "{} matched members".format(r["members"]))
             for r in (bk.get("ttm_series") or [])],
            "{} — bucket-sum TTM level".format(b),
            svgcharts.SERIES_COLORS.get(b, "#555"), height=190,
            observations=bk.get("observations")))
        out.append("<p class='note'>TTM <b>{}</b> · YoY <b>{}</b> · members {} · "
                   "top-2 concentration <b>{}</b> · band {}pp<br>Breadth: ACC {} · PLAT {} · "
                   "DEC {} · CONTR {} · net {:+d}</p>".format(
                       _bucket_num(bk, "ttm", _money),
                       _bucket_num(bk, "latest_yoy", _pct),
                       bk.get("member_count"),
                       "{:.0f}%".format(100 * bk["top2_share"]) if bk.get("top2_share") else "—",
                       bk.get("band"), br.get("ACCELERATING", 0), br.get("PLATEAU", 0),
                       br.get("DECELERATING", 0), br.get("CONTRACTING", 0),
                       br.get("net_direction", 0)))
        out.append("<table><tr><th>Member</th><th>State</th><th class='num'>TTM capex</th>"
                   "<th class='num'>share</th><th class='num'>TTM YoY</th>"
                   "<th class='num'>credit/capex</th></tr>")
        members = [snap["issuers"][m] for m in bk.get("membership", [])
                   if m in snap["issuers"]]
        tot = sum(m["ttm_capex"] or 0 for m in members) or 1.0
        for m in sorted(members, key=lambda x: -(x["ttm_capex"] or 0)):
            out.append("<tr><td>{}</td><td>{}{}</td><td class='num'>{}</td>"
                       "<td class='num'>{:.0f}%</td><td class='num'>{}</td>"
                       "<td class='num'>{}</td></tr>".format(
                           _esc(m["ticker"]), _pill(m["state"]), _flags(m["flags"]),
                           _money(m["ttm_capex"]), 100 * (m["ttm_capex"] or 0) / tot,
                           _pct(m["latest_yoy"]), _pct(m["credit_ratio"], 0)))
        out.append("</table>")
        ce = bk.get("composition_events") or []
        if ce:
            out.append("<p class='note'><b>Composition events:</b> " + " · ".join(
                "{} {} {}".format(_esc(e["quarter"]), _esc(e["ticker"]), _esc(e["change"]))
                for e in ce[-8:]) + "</p>")
    return _page("Buckets", "/buckets", "".join(out))


# ---------------- view 5: forward commitments ----------------

def view_commitments(snap):
    out = ["<h2>Forward commitment stock</h2>",
           "<p class='note'>Contracted but unspent. Leads reported capex. Issuers that disclose "
           "a figure without XBRL-tagging it publish <b>UNCOVERED-UNTAGGED</b> rather than a "
           "zero.</p>"]
    out.append(_commitments_chart(snap, "Forward commitment stock, per issuer", top=10))
    out.append("<p class='chartnote'>A <b>stock</b>, not a flow, and disclosed on the "
               "issuer's own schedule rather than every quarter — so these are plotted "
               "separately and never summed. A dashed segment spans quarters with no "
               "disclosure; it is not a flat stretch.</p>")
    out.append(_commitments_refusal(snap))
    out += ["<table><tr><th>Issuer</th><th>Bucket</th><th>Status</th>"
           "<th class='num'>Latest</th><th>Concept</th><th>Detail</th></tr>"]
    rows = sorted(snap["issuers"].items(),
                  key=lambda kv: -((kv[1]["commitments"] or {}).get("latest") or -1))
    for tick, iss in rows:
        c = iss["commitments"]
        if c["status"] == "ABSENT":
            continue
        out.append("<tr><td><b>{}</b></td><td>{}</td><td><span class='cov'>{}</span></td>"
                   "<td class='num'>{}</td><td>{}</td><td class='note'>{}</td></tr>".format(
                       _esc(tick), _esc(iss["bucket"]), _esc(c["status"]),
                       _money(c["latest"]), _esc(c["concept"] or "—"), _esc(c["detail"][:90])))
    out.append("</table>")
    return _page("Forward commitments", "/commitments", "".join(out))


# ---------------- view 6: the supplier cross-check ----------------

def view_suppliers(snap):
    sup = snap.get("suppliers") or {}
    legs, cc = sup.get("legs") or {}, sup.get("crosscheck") or {}
    out = ["<h2>Supplier cross-check — the same dollar, from the other side</h2>",
           "<p class='note'>A hyperscaler's capex and NVIDIA's datacenter revenue are largely "
           "the <b>same money seen from opposite sides of the invoice</b>. That makes this an "
           "independent read on the buildout — different filers, different fiscal calendars, "
           "different incentives — and it makes adding the two a category error. Suppliers are "
           "<b>never summed into the spending aggregate</b>; they are related to it by a ratio, "
           "which is a corroboration and not a reconciliation. It is not expected to reach "
           "100%.</p>",
           "<p class='note'>This leg is <b>parser-only</b>. Segment revenue is dimension-"
           "qualified, so the companyfacts API drops it entirely — NVDA's API record carries "
           "total <code>Revenues</code> and a segment <i>count</i>, and nothing else. Every "
           "figure below was read out of the filing itself.</p>"]

    series = cc.get("series") or []
    if series:
        out.append(charts_ratio_svg(series))
    if cc.get("warning"):
        out.append("<div class='warn'><b>Read the last point with care.</b> {}</div>".format(
            _esc(cc["warning"])))

    out.append("<h2>Legs</h2><table><tr><th>Supplier</th><th>Status</th>"
               "<th>DC revenue phase</th><th class='num'>DC revenue TTM</th>"
               "<th class='num'>TTM YoY</th><th class='num'>Quarters</th>"
               "<th class='num'>Restated</th><th>Resolved</th></tr>")
    for tick, leg in sorted(legs.items(), key=lambda kv: -((kv[1].get("ttm")) or -1)):
        st = leg["status"]
        # Three states, three colours. A MAPPED figure is usable and is NOT a
        # measurement, so it must not wear the same green as one.
        colour = {"COVERED": "#1d6f42", "MAPPED-BUSINESS-UNITS": "#1f4e9c"}.get(st, "#8a6d1a")
        dcs = leg.get("dc_state") or phases.STATE_INSUFFICIENT
        out.append("<tr><td><b>{}</b></td><td>{}</td>"
                   "<td title='{}'>{}{}</td><td class='num'>{}</td>"
                   "<td class='num'>{}</td>"
                   "<td class='num'>{}</td><td class='num' title='{}'>{}</td>"
                   "<td class='note' title='{}'>{}</td></tr>".format(
                       _esc(tick),
                       "<span class='pill' style='background:{}'>{}</span>".format(
                           colour, _esc(st)),
                       _esc("banded on dcrev:supplier at {}pp, measured {} — NOT the "
                            "issuer:supplier band, which applies to the supplier's own "
                            "capex".format(leg.get("dc_band"),
                                           leg.get("dc_band_measured_on"))),
                       _pill(dcs), _flags(leg.get("dc_flags")),
                       _money(leg.get("ttm")),
                       _pct(leg.get("dc_latest_yoy")),
                       len(leg.get("quarters") or []),
                       _esc("; ".join("{} {:,.0f} -> {:,.0f} (superseded by {})".format(
                           r["period_end"], r["was"], r["now"], r["superseded_by"])
                           for r in (leg.get("restatements") or [])) or "none"),
                       leg.get("restatement_count") or 0,
                       _esc(leg.get("detail") or ""),
                       _esc(", ".join(leg.get("axes") or []) or leg.get("detail", "")[:60])))
    out.append("</table>")

    if series:
        out.append("<h2>Cross-check history</h2><table><tr><th>Quarter</th>"
                   "<th class='num'>Supplier DC revenue TTM</th>"
                   "<th class='num'>Hyperscaler capex TTM</th><th class='num'>Ratio</th>"
                   "<th class='num'>DC members</th><th class='num'>Capex members</th></tr>")
        for r in reversed(series[-16:]):
            out.append("<tr><td>{}</td><td class='num'>{}</td><td class='num'>{}</td>"
                       "<td class='num'><b>{:.1f}%</b></td><td class='num'>{}</td>"
                       "<td class='num'>{}</td></tr>".format(
                           _esc(r["q"]), _money(r["dc"]), _money(r["capex"]),
                           100 * r["ratio"], r["dc_members"], r["capex_members"]))
        out.append("</table>")

    mapped = [l for l in legs.values() if l.get("mapping")]
    if mapped:
        out.append("<h2>Ruled mappings — a semantic judgement, disclosed as one</h2>")
        for l in sorted(mapped, key=lambda x: x["ticker"]):
            m = l["mapping"]
            out.append(
                "<div class='mapped'><b>{}</b> reports no datacenter member. Ruled by <b>{}</b> "
                "on <b>{}</b>: the units below are treated as datacenter revenue.<br>"
                "<b>Summed:</b> {}<br><b>Excluded:</b> {}<br>"
                "<span class='note'>{}</span><br>"
                "<span class='note'><b>This figure is mapped, not measured.</b> It is published "
                "as <code>MAPPED-BUSINESS-UNITS</code> everywhere it appears, and it carries "
                "that label precisely because reasonable people could draw the boundary "
                "differently.</span></div>".format(
                    _esc(l["ticker"]), _esc(m.get("ruled_by", "ruling")), _esc(m.get("ruled")),
                    _esc(" + ".join("{} ({})".format(m["labels"].get(x, x), x)
                                    for x in m["members"])),
                    _esc(", ".join("{} ({})".format(m["excluded_labels"].get(x, x), x)
                                   for x in m["excluded"])),
                    _esc(m.get("rationale", ""))))

    refused = [l for l in legs.values() if l["status"] not in ("COVERED", "MAPPED-BUSINESS-UNITS")]
    if refused:
        out.append("<h2>Refused, and why</h2>")
        for l in sorted(refused, key=lambda x: x["ticker"]):
            out.append("<div class='warn'><b>{}</b> — {}. {}</div>".format(
                _esc(l["ticker"]), _esc(l["status"]), _esc(l["detail"])))
        out.append("<p class='note'>Each of these reports revenue by segment, but none of "
                   "those segments is a datacenter line and none has a ruled mapping. They stay "
                   "in the bucket because their inventory and purchase obligations still bear "
                   "on the buildout, and because a named refusal is worth more than a silent "
                   "omission.</p>")
    return _page("Suppliers", "/suppliers", "".join(out))


def charts_ratio_svg(series):
    """The cross-check ratio over time, on the shared chart primitives."""
    rows = [(r["q"], r["ratio"], "{} DC / {} capex members".format(
        r["dc_members"], r["capex_members"])) for r in series]
    return svgcharts.level_chart(
        rows, "Supplier datacenter revenue as a share of hyperscaler capex (TTM/TTM)",
        svgcharts.SERIES_COLORS["issuance"], height=250,
        fmt=lambda v: "{:.0f}%".format(100 * v))


ROUTES = {"/": view_aggregate, "/hayes": view_hayes, "/phases": view_phases,
          "/divergence": view_divergence, "/buckets": view_buckets,
          "/commitments": view_commitments, "/suppliers": view_suppliers}


def render(path, snap):
    fn = ROUTES.get(path)
    if fn is None:
        return None
    return fn(snap)


def _read_only_snapshot(db_path):
    """Open the DB read-only and load the persisted snapshot. Zero write paths."""
    uri = "file:{}?mode=ro".format(str(db_path).replace("?", "%3F"))
    con = sqlite3.connect(uri, uri=True)
    try:
        return snapshot.load(con)
    finally:
        con.close()


def serve(db_path=None, port=PORT):
    db_path = db_path or config.DB_PATH_DEFAULT

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            try:
                snap = _read_only_snapshot(db_path)
            except Exception as exc:
                return self._send(500, "<pre>snapshot unavailable: {}</pre>".format(_esc(exc)))
            if snap is None:
                return self._send(503, "<pre>no snapshot yet — run `capex-daemon scan`</pre>")
            if path == "/health":
                return self._send(200, json.dumps({"ok": True,
                                                   "generated_unix": snap["generated_unix"]}),
                                  ctype="application/json")
            body = render(path, snap)
            if body is None:
                return self._send(404, "<pre>no such view</pre>")
            self._send(200, body)

        def _send(self, code, body, ctype="text/html; charset=utf-8"):
            raw = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("[capex-dashboard] http://127.0.0.1:{}  (read-only, db={})".format(port, db_path))
    srv.serve_forever()
