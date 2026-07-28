"""SM-R1 L3 PDF brief. Consumes the L1 query layer + abelard_common.render.

EDITORIAL LINE (binding, from the SM-U1/SM-2 verdicts): the front matter is
sentinels, named-principal convergence, ownership pressure, and overlay-flagged
positioning events. Clusters (buy AND sell) are CONTEXT sections, never
headlines, never alerts. Quiet sections collapse to one line; every section
carries its own as_of; a data-quality footer (mojibake, price coverage, corpus
windows) is always present; scheduled briefs close with the SM-A1 standing
warnings verbatim.

Read-only: the brief only reads. It opens the same mode=ro connection the query
layer uses and never touches a write path.
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import os
import sys

from . import db as dbmod
from . import queries as q

# abelard_common.render is the hoisted toolkit (lazy reportlab inside it).
from abelard_common.render import (build_pdf, default_styles, section_box,
                                    eastern_stamp)
# SM-A1 standing warnings, carried verbatim in scheduled briefs.
from .phase4_joins import STANDING_WARNINGS


def _p(text, style):
    from reportlab.platypus import Paragraph
    return Paragraph(html.escape(str(text)), style)


def _quiet(styles, label):
    return [_p("{} — nothing to report this window.".format(label), styles["Foot"])]


def _money(v):
    try:
        return "${:,.0f}".format(float(v))
    except (TypeError, ValueError):
        return "-"


def _corpus_windows(con):
    def mm(sql):
        r = con.execute(sql).fetchone()
        return (r[0], r[1]) if r else (None, None)
    return {
        "form4": mm("SELECT MIN(substr(tx_date,1,10)), MAX(substr(tx_date,1,10)) "
                    "FROM form4_transactions"),
        "congress": mm("SELECT MIN(tx_date), MAX(tx_date) FROM congress_trades "
                       "WHERE asset_type='Stock'"),
        "thirteenf": mm("SELECT MIN(period), MAX(period) FROM thirteenf_holdings"),
    }


# ---------------------------------------------------------------- sections
def _sec_sentinels(con, styles, window, anchor):
    res = q.q_sentinel_log(con, window=window, anchor=anchor)
    if not res["rows"]:
        return _quiet(styles, "No registry-seed activity")
    body = [_p("Registry as-of {}. {} events in the trailing {}d, newest first."
               .format(res["registry_as_of"], res["count"], window), styles["Foot"])]
    for r in res["rows"][:20]:
        body.append(_p("{} [{}] {} {} {} {}".format(
            r.get("event_date") or "?", r["src"], r["seed"], r.get("ticker") or "-",
            r.get("action") or "-",
            _money(r.get("value")) if r.get("value") is not None else
            ("{}-{}".format(r.get("amt_low"), r.get("amt_high")) if r.get("amt_low") else "")),
            styles["Body"]))
    return body


def _sec_convergence(con, styles, period):
    res = q.q_principal_convergence(con, period=period)
    conv, intra, qoq = (res["convergences"], res["intra_quarter_disagreements"],
                        res["qoq_accumulate_distribute_disagreements"])
    if not (conv or intra or qoq):
        return _quiet(styles, "No principal convergence")
    body = []
    body.append(_p("Convergences (2+ filers same side, intra-quarter):", styles["Body"]))
    for r in conv[:15]:
        body.append(_p("  {} {} {} long={} put-heavy={}".format(
            r["ticker"], r["period"], r["converge_dir"], r["long_filers"],
            r.get("short_filers_put_heavy", 0)), styles["Foot"]))
    if intra:
        body.append(_p("Intra-quarter disagreements (long vs put-heavy, same quarter):",
                       styles["Body"]))
        for r in intra:
            body.append(_p("  {} {} 1 net-long vs 1 net-put-heavy".format(
                r["ticker"], r["period"]), styles["Foot"]))
    if qoq:
        body.append(_p("QoQ accumulate vs distribute (cross-manager, over time):",
                       styles["Body"]))
        for r in qoq[:15]:
            body.append(_p("  {} {} accumulating {} vs distributing {}".format(
                r["ticker"], r["period"], ",".join(r["accumulating_ciks"]),
                ",".join(r["distributing_ciks"])), styles["Foot"]))
    return body


def _sec_pressure(con, styles, window, anchor):
    res = q.q_ownership_pressure(con, target="all", window=window, anchor=anchor)
    if not res["rows"]:
        return _quiet(styles, "No ownership-pressure flow")
    body = [_p("Flow = discretionary open-market P/S transaction rows (no holding "
               "level). Reporting-population Section-16 filers. Top by |net shares|."
               , styles["Foot"])]
    for r in res["rows"][:15]:
        body.append(_p("{} net {:+,.0f} sh  buyers={} sellers={}  {}".format(
            r["ticker"] or r["issuer_cik"], r["net_shares"], r["distinct_buyers"],
            r["distinct_sellers"], r["direction"]), styles["Body"]))
    return body


def _sec_overlay_events(con, styles, since):
    res = q.q_positioning_events(since=since, overlay_only=True)
    if not res["rows"]:
        return _quiet(styles, "No overlay-flagged positioning events")
    body = []
    for e in res["rows"][:20]:
        f = e.get("flags") or {}
        tags = [k for k in ("conviction_overlay", "watchlist_overlay", "sentinel",
                            "cluster") if f.get(k)]
        body.append(_p("{} [{}] {} {} {}  <{}>".format(
            e.get("disclosure_date") or e.get("tx_date") or "?", e.get("leg"),
            e.get("person") or e.get("entity") or "-", e.get("ticker") or "-",
            e.get("side") or "-", ",".join(tags)), styles["Body"]))
    return body


def _sec_cluster_context(con, styles, window, anchor, floor):
    res = q.q_cluster_context(con, window=window, floor=floor, anchor=anchor)
    if not res["rows"]:
        return _quiet(styles, "No buy clusters")
    body = [_p("CONTEXT, not an alert. Cluster = >={} distinct discretionary buyers "
               "per issuer in a 30d window. `capitulation` = all buys in one calendar "
               "month (coordinated read vs slow accumulation).".format(floor),
               styles["Foot"])]
    for c in res["rows"][:15]:
        body.append(_p("{} buyers={} buys={} span={}d months={} {}".format(
            c["ticker"], c["n_buyers"], c["n_buys"], c["span_days"],
            ",".join(c["calendar_months"]),
            "CAPITULATION" if c["capitulation"] else ""), styles["Body"]))
    return body


def _sec_sell_context(con, styles, window, anchor):
    res = q.q_sell_anomaly(con, window=window, anchor=anchor)
    elevated = [r for r in res["rows"] if r.get("elevated")]
    if not elevated:
        return _quiet(styles, "No elevated sell clusters")
    body = [_p("CONTEXT ranked feed, not a verdict. `elevated` tint = rate ratio "
               ">= {} with a >=3-seller baseline. Seasonality caveat applies "
               "(grant-season vesting inflates the ratio).".format(res["elevated_ratio"]),
               styles["Foot"])]
    for r in sorted(elevated, key=lambda x: -x["rate_ratio"])[:15]:
        body.append(_p("{} ratio={:.1f} sellers {}/{}yr  {}".format(
            r["ticker"] or r["issuer_cik"], r["rate_ratio"],
            r["distinct_sellers_window"], r["distinct_sellers_12mo"],
            _money(r["window_sell_value"])), styles["Body"]))
    return body


def _sec_footer(con, styles):
    from .mojibake import scan_mojibake
    mj = scan_mojibake(con)
    cw = _corpus_windows(con)
    n_prices = con.execute("SELECT COUNT(DISTINCT ticker) FROM prices "
                           "WHERE price_type='eod'").fetchone()[0]
    n_delisted = con.execute("SELECT COUNT(*) FROM ticker_status WHERE verdict="
                             "'delisted_presumed'").fetchone()[0]
    lines = [
        "Data quality. Suspected mojibake {} across name columns, {} legitimate "
        "non-ASCII, detected not fixed.".format(mj["total_suspected_mojibake"],
                                                mj["total_non_ascii"]),
        "Corpus windows. Form 4 {}..{}. Congress {}..{}. 13F {}..{}.".format(
            cw["form4"][0], cw["form4"][1], cw["congress"][0], cw["congress"][1],
            cw["thirteenf"][0], cw["thirteenf"][1]),
        "Price coverage. {} tickers with cached EOD series, {} presumed delisted. "
        "Returns never imputed for a missing series.".format(n_prices, n_delisted),
    ]
    return [_p(x, styles["Foot"]) for x in lines]


# ---------------------------------------------------------------- assembly
def render_brief(con, out_path, *, window=90, anchor=None, since=None,
                 floor=3, scheduled=False, title="Smart Money Brief"):
    """Assemble the brief in editorial order and render it. Returns the path."""
    anchor = anchor or dt.date.today().isoformat()
    since = since or q._win(anchor, window)
    styles = default_styles()
    story = []
    story.append(_p(title, styles["Title"]))
    story.append(_p("Generated {}  window {}d  anchor {}".format(
        eastern_stamp(q._as_of()), window, anchor), styles["Sub"]))

    # FRONT MATTER (editorial order) — never clusters.
    for label, body in (
        ("Sentinels — registry-seed activity", _sec_sentinels(con, styles, window * 2, anchor)),
        ("Principal convergence and disagreement", _sec_convergence(con, styles, None)),
        ("Ownership pressure — flow", _sec_pressure(con, styles, window, anchor)),
        ("Overlay-flagged positioning events", _sec_overlay_events(con, styles, since)),
    ):
        story += section_box(label, body, styles)

    # CONTEXT sections — clusters, buy then sell. Never headlines.
    for label, body in (
        ("Buy-cluster context", _sec_cluster_context(con, styles, window * 2, anchor, floor)),
        ("Sell context — ranked, elevated tint", _sec_sell_context(con, styles, window, anchor)),
    ):
        story += section_box(label, body, styles)

    story += section_box("Data quality", _sec_footer(con, styles), styles)
    if scheduled:
        story += section_box("Standing warnings",
                             [_p(w, styles["Foot"]) for w in STANDING_WARNINGS], styles)
    return build_pdf(out_path, story, title=title)


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-R1 PDF brief")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--out", default=None)
    ap.add_argument("--window", type=int, default=90)
    ap.add_argument("--anchor", default=dt.date.today().isoformat())
    ap.add_argument("--scheduled", action="store_true")
    args = ap.parse_args(argv)
    con = q.connect_ro(args.db)
    out = args.out or os.path.join(dbmod.SCANS_DIR,
                                   "SMART_MONEY_BRIEF_{}.pdf".format(args.anchor.replace("-", "")))
    path = render_brief(con, out, window=args.window, anchor=args.anchor,
                        scheduled=args.scheduled)
    print("[brief] -> {}".format(path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
