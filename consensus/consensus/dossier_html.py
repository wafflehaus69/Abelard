"""Static HTML renderer for the dossier dashboard — M10-Dash / addendum v1.18.

WHY THIS IS PYTHON AND NOT JAVASCRIPT. The first dashboard rendered its panels in the
browser from an inlined payload. It worked in a browser, and showed the owner four empty
panels, because the surface the file is actually VIEWED through renders HTML without
executing scripts. Static markup (headers, prose) appeared; every JS-populated panel was
blank. The page therefore looked calm and authoritative while showing nothing, and a
JS-based fail-loud guard could not help — a guard that needs JS cannot fire when JS is
what is missing.

So the page is now built here, fully rendered, before it ever reaches a browser. It needs
no JavaScript to show its data. That also makes the §3 fail-loud contract enforceable: a
panel that cannot be built writes its error INTO the markup at generation time.

Three states, always visually distinct (§3):
  * data present    -> render it
  * legitimately empty -> an explicit "none yet" that reads as a true zero
  * missing/failed  -> an explicit error state naming the reason, never silence
"""

from __future__ import annotations

import datetime as dt
import html
from typing import Any, Callable

from . import resolution as _res

_CSS = """
:root{--bg:#0f1115;--panel:#171a21;--line:#262b36;--ink:#e6e9ef;--dim:#8b93a3;
      --accent:#6ea8fe;--warn:#e0b341;--err:#e06c75}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
     font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:28px 20px 64px}
h1{font-size:22px;margin:0 0 6px}h2{font-size:15px;margin:0 0 4px}
.sub{color:var(--dim);font-size:13px}
.framing,.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px;margin:0 0 18px}
.framing{border-left:3px solid var(--accent)}
.grid{display:grid;gap:18px}@media(min-width:900px){.two{grid-template-columns:1fr 1fr}}
.kpi{display:flex;gap:26px;flex-wrap:wrap;margin:12px 0 4px}
.kpi div span{display:block}.kpi .n{font-size:26px;font-weight:600}.kpi .l{color:var(--dim);font-size:12px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--dim);font-weight:500;font-size:12px}
.tag{display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;border:1px solid var(--line);color:var(--dim)}
.unres{color:var(--warn);border-color:var(--warn)}
.bar{height:7px;background:#20242e;border-radius:4px;overflow:hidden;display:inline-block;width:52px;vertical-align:middle}
.bar i{display:block;height:100%;background:var(--accent)}
.bar.nm{background:repeating-linear-gradient(90deg,#2a2f3a 0 4px,transparent 4px 8px)}
.note{color:var(--dim);font-size:12px;margin-top:10px}
.foot{color:var(--dim);font-size:12px;border-top:1px solid var(--line);margin-top:26px;padding-top:14px}
.err{background:#2a1b1d;border:1px solid var(--err);color:#f3c0c4;border-radius:6px;padding:12px 14px;margin:10px 0}
.err b{color:var(--err)}
.none{color:var(--dim);font-style:italic;padding:10px 0}
.strike{margin:6px 0 0;padding-left:18px;color:var(--dim);font-size:12.5px}
.caveat{border-left:2px solid var(--warn);padding-left:10px}
"""

FOOTER = ("Anomaly detection over public on-chain data. Not a validated trade signal, not "
          "an allegation about any person, and no expected value is implied or computed "
          "anywhere on this page. Read-only: this page makes no network call, requires no "
          "JavaScript, and never touches a venue.")


def _e(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _money(v: Any) -> str:
    try:
        return "$" + f"{round(float(v)):,}"
    except (TypeError, ValueError):
        return "—"


def _day(ts: Any) -> str:
    try:
        return dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return "—"


def panel(title: str, sub: str, body: Callable[[], str]) -> str:
    """Render one panel, converting any failure into a VISIBLE error state rather than
    an empty div. A blank panel is a bug, not a blank (§3)."""
    head = f"<div class='panel'><h2>{title}</h2><div class='sub'>{sub}</div>"
    try:
        inner = body()
    except Exception as exc:  # noqa: BLE001 - the whole point is to surface it
        inner = (f"<div class='err'><b>panel failed to build</b> — "
                 f"{_e(type(exc).__name__)}: {_e(exc)}<br>"
                 "This is an error, not an empty result. The data may exist; this panel "
                 "could not render it.</div>")
    return head + inner + "</div>"


def _kpi(items: list[tuple[Any, str]]) -> str:
    return "<div class='kpi'>" + "".join(
        f"<div><span class='n'>{_e(n)}</span><span class='l'>{_e(l)}</span></div>"
        for n, l in items) + "</div>"


def _bars(f: dict[str, Any]) -> str:
    out = []
    for k in ("F", "S", "D", "C"):
        v = f.get(k)
        if v is None:
            # not measured != zero; an empty bar is indistinguishable from a measured 0
            out.append(f"<span title='{k} not measured — wallet not enriched'>"
                       f"<span class='sub'>{k}</span> <span class='bar nm'></span></span>")
        else:
            w = max(0.0, min(1.0, float(v))) * 100
            out.append(f"<span><span class='sub'>{k}</span> <span class='bar'>"
                       f"<i style='width:{w:.0f}%'></i></span></span>")
    return " ".join(out)


def _series_svg(series: list[dict[str, Any]], goal: int) -> str:
    if not series:
        return ("<div class='none'>No resolved blocks yet — the curve starts when the "
                "first market resolves. This is a true empty state, not a failure.</div>")
    W, H, P = 900, 190, 26
    ymax = max([goal] + [p["blocks"] for p in series]) or 1
    n = len(series)
    xs = [P + (0 if n < 2 else i * (W - 2 * P) / (n - 1)) for i in range(n)]
    ys = [H - P - (p["blocks"] / ymax) * (H - 2 * P) for p in series]
    gy = H - P - (goal / ymax) * (H - 2 * P)
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    return (f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:190px'>"
            f"<line x1='{P}' y1='{H-P}' x2='{W-P}' y2='{H-P}' stroke='#262b36'/>"
            f"<line x1='{P}' y1='{gy:.1f}' x2='{W-P}' y2='{gy:.1f}' stroke='#e0b341' "
            f"stroke-dasharray='4 4'/>"
            f"<text x='{P+4}' y='{gy-5:.1f}' fill='#e0b341' font-size='10'>"
            f"{goal} blocks — powered at MDE 0.10</text>"
            f"<polyline fill='none' stroke='#6ea8fe' stroke-width='2' points='{pts}'/>"
            f"<text x='{P}' y='{H-8}' fill='#8b93a3' font-size='10'>{_e(series[0]['date'])}</text>"
            f"<text x='{W-P-60}' y='{H-8}' fill='#8b93a3' font-size='10'>{_e(series[-1]['date'])}</text>"
            "</svg>")


def _dist(title: str, obj: dict[str, Any], note: str = "") -> str:
    ent = sorted((obj or {}).items(), key=lambda kv: -kv[1])
    if not ent:
        return f"<div class='sub'>{_e(title)}</div><div class='none'>none recorded yet</div>"
    tot = sum(v for _, v in ent) or 1
    rows = "".join(
        f"<div style='display:flex;gap:8px;align-items:center;margin:3px 0'>"
        f"<span style='width:130px;font-size:12px'>{_e(k)}</span>"
        f"<span class='bar' style='flex:1;width:auto'><i style='width:{v/tot*100:.1f}%'></i></span>"
        f"<span style='width:52px;text-align:right;font-size:12px;color:#8b93a3'>{v}</span></div>"
        for k, v in ent)
    return (f"<div style='margin-bottom:14px'><div class='sub'>{_e(title)}</div>{rows}"
            + (f"<div class='note'>{_e(note)}</div>" if note else "") + "</div>")


def render_page(data: dict[str, Any] | None) -> str:
    """Build the whole page as static markup. Never returns a silently empty page."""
    if not isinstance(data, dict) or not data:
        return _shell("<div class='err'><b>dashboard data failed to load</b> — the export "
                      "payload is missing or unparseable. No panel below can be trusted; "
                      "regenerate with <code>consensus dossier export</code>.</div>")

    T = data.get("totals") or {}
    A = data.get("accumulation") or {}
    M = A.get("milestones") or {}
    ceil_m = M.get("ceiling_0.10") or {}
    trade_m = M.get("tradeable_0.05") or {}
    CM = A.get("contested_milestones") or {}
    c_ceil = CM.get("ceiling_0.10") or {}
    c_trade = CM.get("tradeable_0.05") or {}

    def eta(m: dict[str, Any]) -> str:
        if not m:
            return "unknown"
        if m.get("blocks_remaining") == 0:
            return "reached"
        return (f"about {m['days_remaining']} days away" if m.get("reachable")
                else "not reachable at the current rate")

    stamp = _day(data.get("generated_ts"))
    head = (f"<h1>CONSENSUS — dossier record</h1><div class='sub'>generated {stamp}</div>"
            "<div class='framing'><strong>This is an intelligence record, not a trading "
            "signal.</strong><p>It surfaces verifiable on-chain facts as an input to your "
            "own judgement, and it compounds a labelled dataset that is the only honest "
            "route to a decisive answer on whether any edge exists. On the current rate a "
            f"powered re-test at the plausible-edge ceiling is <b>{_e(eta(c_ceil or ceil_m))}</b> "
            "— measured on CONTESTED blocks, the only ones that could carry information. If "
            "that line goes flat or the date runs years out, the correct decision is to "
            "switch it off — this page is built to tell you that too.</p>"
            "<p class='sub'>The trade question was asked four times and answered no. That "
            "record is the context that makes the rest legible:</p><ul class='strike'>"
            "<li><b>Detector A (consensus copy)</b> — edge in spring 2025, zero by 2026. NO-GO.</li>"
            "<li><b>Detector B (footprint, ≤30d)</b> — +2.15pp, inside market-block noise, "
            "below the materiality floor. NO-GO.</li>"
            "<li><b>Detector B tighter cell (CRITICAL ∩ ≤7d)</b> — significant in aggregate "
            "but −9.45pp in the live-adjacent regime. Declined.</li>"
            "<li><b>Cross-venue lead-lag (M0-X)</b> — structurally thin overlap. Stopped at "
            "the gate.</li></ul></div>")

    def panel_b() -> str:
        rows = "".join(
            f"<tr><td>{_e(lbl)}</td><td>{_e(cm.get('target_blocks'))}</td>"
            f"<td><b>{_e(eta(cm))}</b></td><td>{_e(eta(am))}</td></tr>"
            for lbl, cm, am in (
                ("detect a 10pp effect (plausible ceiling)", c_ceil, ceil_m),
                ("detect a 5pp effect (tradeable floor)", c_trade, trade_m)) if cm or am)
        n_carry = T.get("resolved_carry", 0)
        n_cont = T.get("resolved_contested", 0)
        n_res = T.get("resolved_dossiers", 0) or 1
        mech = T.get("resolved_mechanical", 0)
        # v1.19 §2.3 — one unmissable figure. If most of what resolves is carry, the
        # countdown is counting toward a STARVED test, and that must be impossible to miss.
        agg = (f"<div class='err' style='background:#241f14;border-color:#e0b341'>"
               f"<b>{n_carry} of {n_res} resolved footprints are carry-band</b> "
               f"({n_carry/n_res*100:.0f}%) — entered at odds where the outcome is "
               f"near-automatic and winning carries no information. Only <b>{n_cont}</b> "
               f"are contested, spanning <b>{_e(T.get('contested_blocks'))}</b> of the "
               f"{_e(T.get('resolved_blocks'))} resolved blocks. "
               + (f"{mech} are mechanical-count markets by title (a lower bound). "
                  if mech else "")
               + "A block is not an informative block: the powered date below is driven by "
               "the CONTESTED rate, because carry blocks cannot inform a test of "
               "informed-money edge. Counting them would inflate progress toward a test "
               "they can never answer.</div>")
        return (agg
                + _kpi([(T.get("contested_blocks", "—"), "CONTESTED blocks (what the test can actually use)"),
                      (T.get("resolved_blocks", "—"), "all resolved blocks (optimistic bound)"),
                      (A.get("contested_blocks_per_day", "—"), "contested blocks/day"),
                      (T.get("dossiers", "—"), "footprints captured")])
                + _series_svg(A.get("contested_series") or [],
                              (c_ceil.get("target_blocks") or 155))
                + (f"<table><tr><th>target</th><th>blocks needed</th>"
                   f"<th>on CONTESTED blocks (headline)</th>"
                   f"<th>on all blocks (optimistic bound)</th></tr>{rows}</table>" if rows else
                   "<div class='none'>no milestones computed</div>")
                + f"<div class='note'>{_e(A.get('assumption', ''))} This is an estimate, "
                  "not a promise: it assumes the observed rate holds.</div>")

    def panel_c() -> str:
        R = data.get("restraint") or {}
        n_alert = T.get("alerted", 0)
        zero_note = ("" if n_alert else
                     "<div class='none'>No alert has ever fired. That is a true zero and "
                     "the expected state — the bar is set so a normal week is quiet.</div>")
        return (_kpi([(n_alert, "alerts ever raised"),
                      (R.get("calibrated_rate_per_week", "—"), "calibrated alerts/week"),
                      (T.get("false_positives_refused", 0), "inflated scores refused"),
                      (T.get("untierable", 0), "footprints left untiered")])
                + zero_note
                + f"<div class='note'>Alert bar <b>{_e(R.get('alert_bar'))}</b> on the "
                  "fill-factor composite, deliberately higher than the CRITICAL tier so a "
                  "quiet week stays quiet. Footprints whose freshness could not be verified "
                  "are refused a tier and never page — that refusal is counted above rather "
                  "than hidden.</div>"
                  f"<div class='note'><span class='tag unres'>coordination alerting: "
                  f"{_e(R.get('cluster_arm', 'closed'))}</span> {_e(R.get('cluster_arm_reason', ''))}</div>")

    def panel_d() -> str:
        tx = data.get("texture") or {}
        share = tx.get("favorite_harvest_share")
        note = (f"Favourite-harvesting (>0.90) is {share*100:.0f}% of captured footprints. "
                "It is yield carry, not information, and is shown separately so it never "
                "drowns the contested slice.") if isinstance(share, (int, float)) else ""
        return (_dist("by price band", tx.get("by_price_band"), note)
                + _dist("by category", tx.get("by_category"))
                + _dist("by freshness at detection", tx.get("by_freshness"))
                + _dist("by notional (headline)", tx.get("by_notional")))

    def panel_a() -> str:
        recent = data.get("recent") or []
        if not recent:
            return ("<div class='none'>No footprints captured yet. This is a true empty "
                    "state — the scan has run and found nothing above the floor.</div>")
        n_res = sum(1 for c in recent if c.get("resolved"))
        n_mkt = len({c.get("market") for c in recent if c.get("resolved")})
        caveat = (f"<div class='note caveat'><b>The outcome column is a per-row fact, not a "
                  f"hit rate.</b> These are the newest {len(recent)} footprints by detection "
                  f"time, of which {n_res} have resolved — out of "
                  f"{_e(T.get('resolved_dossiers'))} resolved footprints in the store, so "
                  f"this is a recency sample, not a scorecard. Those {n_res} span only "
                  f"{n_mkt} distinct market(s): rows from one event are correlated and count "
                  "once, which is why the panel above measures in blocks.</div>")
        rows = []
        for c in recent:
            st = c.get("collapse_state")
            raw = c.get("raw_wallets") or 1
            if st == "unresolved":
                cluster = (f"<span class='tag unres'>{raw} wallets → actor count "
                           f"UNRESOLVED</span><div class='note'>may be one actor in "
                           f"{raw} masks</div>")
            elif st == "collapsed":
                cluster = (f"<span class='tag unres'>{raw} wallets → {c.get('actor_count')} "
                           f"actor(s)</span><div class='note'>n={c.get('actor_count')} "
                           f"evidence, not {raw}</div>")
            else:
                cluster = (f"{c.get('actor_count')} independent actors" if raw > 1 else "solo")
            vw = c.get("entry_vwap")
            try:
                vwf = float(vw)
                band = (f"<div class='note'>entry {vwf:.3f} — carry band; winning here is "
                        "near-automatic and information-free</div>" if vwf > 0.90
                        else f"<div class='note'>entry {vwf:.3f}</div>")
            except (TypeError, ValueError):
                band = "<div class='note'>entry price not measured</div>"
            if not c.get("resolved"):
                outcome = "<span class='tag'>unresolved</span>"
            elif c.get("outcome_for_side") is None:
                outcome = "—"
            else:
                outcome = ("flagged side won" if c["outcome_for_side"] == 1
                           else "flagged side lost") + band
            tier = (_e(c.get("tier") or "—") if c.get("score_complete")
                    else "<span class='tag unres'>untiered</span>")
            peak = (f"<div class='note'>peak {_e(c.get('tier_peak'))}</div>"
                    if c.get("tier_peak") and c.get("tier_peak") != c.get("tier") else "")
            contested = (_money(c.get("contested_notional"))
                         if c.get("contested_notional") is not None
                         else "<span class='tag unres'>not measured</span>")
            rows.append(
                f"<tr><td>{_e((c.get('market') or '—')[:68])}"
                f"<div class='note'>{_e(c.get('category') or '')} · {_day(c.get('detection_ts'))}</div></td>"
                f"<td>{tier}{peak}</td><td>{_bars(c.get('factors') or {})}</td>"
                f"<td>{contested}<div class='note'>informed slice</div></td>"
                f"<td>{_money(c.get('headline_notional'))}<div class='note'>carry confound</div></td>"
                f"<td>{cluster}</td><td>{outcome}</td></tr>")
        return (caveat + "<table><tr><th>market</th><th>tier</th><th>factors F/S/D/C</th>"
                "<th>contested</th><th>headline</th><th>cluster</th><th>outcome</th></tr>"
                + "".join(rows) + "</table>")

    body = (head
            + panel("The dataset compounding <span class='sub'>— why this keeps running</span>",
                    "Resolved, outcome-stamped footprints accumulating. Counted in independent "
                    "market-blocks, not rows — and then only the CONTESTED blocks, because a "
                    "block entered at carry odds has no information for the test to find. "
                    "The curve below plots contested blocks only.", panel_b)
            + "<div class='grid two'>"
            + panel("What it stayed quiet about",
                    "A product whose value is partly restraint has to show the restraint.", panel_c)
            + panel("Detection texture <span class='sub'>— the browse surface</span>",
                    "Descriptive only. Nothing here is ranked for following.", panel_d)
            + "</div>"
            + panel("What fired, and how sure we are",
                    "Headline notional is the full position and carries the carry-trade "
                    "confound. The contested slice — the informed part bought inside the "
                    "0.10–0.90 band — is not measured by the live scan and is shown as such "
                    "rather than inferred. A dashed factor bar likewise means not measured, "
                    "never zero.", panel_a))
    return _shell(body)


def _shell(body: str) -> str:
    return ("<!DOCTYPE html><meta charset='utf-8'>"
            "<title>CONSENSUS — dossier record</title>"
            f"<style>{_CSS}</style><div class='wrap'>{body}"
            f"<div class='foot'>{FOOTER}</div></div>")
