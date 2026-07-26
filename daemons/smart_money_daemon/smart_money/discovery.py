"""SM-U1 PH3 (discovery counter) + PH4 (validation harness).

PH3: g1 semantics over the universal corpus — per issuer, distinct reporting
persons with code-P non-plan buys in 30/90/180d. Reports the buyer-count
DISTRIBUTION first (how many issuers hit 2/3/4/5+ distinct buyers) so the
threshold is set from data, not guessed. Provisional floor >=3. Enriched with
total buy value, buyer roles, cluster span, overlay + pedigree-set membership.

PH4 (anti-graphify): each historical cluster (>=floor distinct P-buyers within
30d) is ONE dated event (cluster-collapse — not one per buyer). Forward excess
vs SPY at 21/63/126d from the FILING date the cluster became observable (the
copyable clock). Reports n, hit rate, mean/median excess, dispersion, t-stat.
Small-cap price coverage is worse than the congress universe — coverage is
reported, missing series excluded-and-counted, NEVER imputed. HONESTY: if the
harness shows no measurable edge, that is the finding and ships as the finding.
No threshold tuning until something looks positive.
"""
import argparse
import bisect
import datetime as dt
import json
import math
import statistics as st
import sys
from collections import defaultdict

from . import db as dbmod
from . import prices
from .grade_case import forward_excess
from .overlay import load_overlay

WINDOWS = (30, 90, 180)
# Floor >=3 distinct P-buyers is the ratified EXPLORATORY floor (PH5 gate,
# 2026-07-26): a bookkeeping/analysis threshold only, NOT a live-alert trigger.
# One blind PH4 pass showed no independent edge, so discovery never enters the
# scheduled scan's alert path (scan.py ingests the corpus but emits no cluster
# events); this floor governs manual exploration and the accruing joins prior.
DEFAULT_FLOOR = 3
HZ = (21, 63, 126)

# Data-quality counters populated by _p_buys (surfaced in the honesty section):
# some EDGAR transactionDate values carry a trailing TZ offset
# ('2024-12-27-05:00') that date.fromisoformat rejects. We normalize to the
# YYYY-MM-DD head and drop-and-count anything still unparseable — never coerce.
_DQ = {"rows_seen": 0, "rows_dropped_bad_date": 0}


def _iso10(s):
    """Normalize an EDGAR date value to a valid YYYY-MM-DD, tolerating trailing
    timezone offsets. Returns None if the 10-char head is not a real date."""
    s = (s or "")[:10]
    try:
        dt.date.fromisoformat(s)
        return s
    except ValueError:
        return None


def _p_buys(con, regime):
    """All discretionary open-market buys (code P, plan_flag 0) in the regime,
    with the identity fields the counter and harness need."""
    q = ("SELECT issuer_cik, ticker, reporting_cik, reporting_person, role, "
         "tx_date, filed_date, value FROM form4_transactions "
         "WHERE code='P' AND plan_flag=0 AND ticker IS NOT NULL")
    if regime == "universal":
        # Reclaim the small set of filings the universal walk actually fetched
        # but that kept ingest_regime='watchlist' because of a PK collision with
        # the earlier watchlist leg (SM-U1 form.idx double-fetch note). Those
        # filings belong to the universal population; watchlist rows the
        # universal walk never touched (accession not in the backfill ledger)
        # correctly stay OUT.
        q += (" AND (ingest_regime='universal' OR (ingest_regime='watchlist' AND "
              "accession IN (SELECT accession FROM form4_backfill_seen)))")
    elif regime != "all":
        q += " AND ingest_regime='{}'".format(regime)
    out = []
    for icik, tk, rcik, person, role, txd, fld, val in con.execute(q).fetchall():
        _DQ["rows_seen"] += 1
        ntx = _iso10(txd)
        if ntx is None:  # cannot place in time — drop and count, never coerce
            _DQ["rows_dropped_bad_date"] += 1
            continue
        # filed_date is the index day we assigned at ingest (clean); normalize
        # defensively and fall back to tx_date if a filing ever lacks one.
        out.append((icik, tk, rcik, person, role, ntx, _iso10(fld) or ntx, val))
    return out


def _issuer_key(issuer_cik, ticker):
    return issuer_cik or ("TK:" + (ticker or "?"))


# ---------------------------------------------------------------- PH3
def distribution(buys, anchor):
    """Per window: how many issuers have >=2/3/4/5 distinct P-buyers in the
    trailing window from anchor."""
    out = {}
    for w in WINDOWS:
        start = (dt.date.fromisoformat(anchor) - dt.timedelta(days=w)).isoformat()
        per_issuer = defaultdict(set)
        for icik, tk, rcik, _, _, txd, _, _ in buys:
            if start <= txd <= anchor:
                per_issuer[_issuer_key(icik, tk)].add(rcik)
        counts = [len(s) for s in per_issuer.values()]
        out[w] = {k: sum(1 for c in counts if c >= k) for k in (2, 3, 4, 5)}
        out[w]["issuers_with_any_buy"] = len(per_issuer)
    return out


def clusters(buys, floor, window_days, overlay, pedigree):
    """One cluster per issuer per non-overlapping `window_days` tx-date window
    with >=floor distinct P-buyers. Event date = the filed_date at which the
    floor-th distinct buyer became observable (the copyable clock)."""
    by_issuer = defaultdict(list)
    for icik, tk, rcik, person, role, txd, fld, val in buys:
        by_issuer[_issuer_key(icik, tk)].append(
            {"rcik": rcik, "person": person, "role": role, "tx": txd,
             "filed": fld, "value": val or 0, "ticker": tk})
    out = []
    for key, rows in by_issuer.items():
        rows.sort(key=lambda r: r["tx"])
        used = 0
        while used < len(rows):
            window_start = rows[used]["tx"]
            wend = (dt.date.fromisoformat(window_start)
                    + dt.timedelta(days=window_days)).isoformat()
            seg = [r for r in rows[used:] if r["tx"] <= wend]
            distinct = {}
            for r in seg:
                distinct.setdefault(r["rcik"], r)
            if len(distinct) >= floor:
                # observability = filed_date of the floor-th distinct buyer
                order = sorted(distinct.values(), key=lambda r: r["filed"])
                event_filed = order[floor - 1]["filed"]
                out.append({
                    "issuer_key": key, "ticker": seg[0]["ticker"],
                    "n_buyers": len(distinct), "n_buys": len(seg),
                    "window_start": window_start, "event_filed": event_filed,
                    "total_value": sum(r["value"] for r in seg),
                    "roles": sorted({r["role"] for r in distinct.values() if r["role"]}),
                    "span_days": (dt.date.fromisoformat(seg[-1]["tx"])
                                  - dt.date.fromisoformat(window_start)).days,
                    "overlay": _tag(overlay, seg[0]["ticker"]),
                    "pedigree": _ped(pedigree, seg[0]["ticker"]),
                })
                used += len(seg)  # collapse: consume the window
            else:
                used += 1
    return out


def _tag(overlay, ticker):
    c, w = overlay.match(ticker or "")
    return ("conviction" if c else "") + ("+watchlist" if w else "") or "-"


def _ped(pedigree, ticker):
    return (ticker or "").upper() in pedigree


# ---------------------------------------------------------------- PH4
def validate(con, cl):
    """Forward excess vs SPY at 21/63/126d from each cluster's observable filing
    date. Missing price series excluded-and-counted, never imputed."""
    graded, no_series = [], []
    for c in cl:
        tk = c["ticker"]
        if not tk:
            no_series.append(c)
            continue
        try:
            fe = forward_excess(con, tk, c["event_filed"])
        except prices.PriceError:
            no_series.append(c)
            continue
        if not fe:
            no_series.append(c)
            continue
        row = {"issuer_key": c["issuer_key"], "ticker": tk,
               "event_filed": c["event_filed"], "n_buyers": c["n_buyers"]}
        ok = False
        for h in HZ:
            hv = fe["horizons"][h]
            row["x{}".format(h)] = hv["excess"] if hv else None
            if hv:
                ok = True
        (graded if ok else no_series).append(row)
    return graded, no_series


def _stats(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return {"n": len(vals), "mean": None, "median": None, "std": None, "t": None}
    m, sd = st.mean(vals), st.pstdev(vals)
    return {"n": len(vals), "mean": m, "median": st.median(vals), "std": sd,
            "hit_rate": sum(1 for v in vals if v > 0) / len(vals),
            "t": (m / (sd / math.sqrt(len(vals)))) if sd > 0 else None}


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-U1 PH3/PH4 discovery + validation")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--regime", default="universal")
    ap.add_argument("--floor", type=int, default=DEFAULT_FLOOR)
    ap.add_argument("--anchor", default=dt.date.today().isoformat())
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    overlay = load_overlay()
    # Pedigree set = the tracked/known universe: overlay book + watchlist + the
    # SM-A1 trump_network discovered issuers. Membership is an enrichment flag,
    # NOT a filter — discovery spans the whole regime.
    import os
    pedigree = set(overlay.conviction) | set(overlay.watchlist)
    tnp = dbmod.find_artifact("trump_network_issuers.json", "scans")
    if os.path.exists(tnp):
        pedigree |= {t.upper() for t in json.loads(open(tnp).read()).get("tickers", [])}
    buys = _p_buys(con, args.regime)  # single pass; DQ counters tallied here
    dist = distribution(buys, args.anchor)
    cl = clusters(buys, args.floor, 30, overlay, pedigree)
    graded, no_series = validate(con, cl)
    res = {h: _stats([g.get("x{}".format(h)) for g in graded]) for h in HZ}
    out = args.out or dbmod.artifact_path(
        "U1_DISCOVERY_{}.md".format(args.anchor.replace("-", "")), "scans")
    _render(out, args.regime, args.floor, dist, cl, graded, no_series, res, dict(_DQ))
    print("[discovery] regime={} clusters>={}={} graded={} no_series={} -> {}".format(
        args.regime, args.floor, len(cl), len(graded), len(no_series), out))
    return 0


def _render(out, regime, floor, dist, cl, graded, no_series, res, dq=None):
    m = ["# U1_DISCOVERY — SM-U1 PH3 counter + PH4 validation", "",
         "Regime: {}. Discretionary open-market P buys (plan_flag=0). NO threshold "
         "tuning, NO ranking — the distribution sets the threshold.".format(regime), ""]
    m.append("## PH3 buyer-count distribution (set the floor from THIS)")
    m.append("")
    m.append("| window | issuers w/ any buy | >=2 | >=3 | >=4 | >=5 |")
    m.append("|---|---|---|---|---|---|")
    for w in WINDOWS:
        d = dist[w]
        m.append("| {}d | {} | {} | {} | {} | {} |".format(
            w, d["issuers_with_any_buy"], d[2], d[3], d[4], d[5]))
    m.append("")
    m.append("## PH3 discovery table — clusters at provisional floor >= {} (30d window)".format(floor))
    m.append("")
    m.append("{} clusters (one per issuer per non-overlapping window).".format(len(cl)))
    m.append("")
    for c in sorted(cl, key=lambda x: -x["n_buyers"])[:40]:
        m.append("- {} ({}) buyers={} buys={} value=${:,} span={}d roles={} "
                 "overlay={} pedigree={} observable={}".format(
                     c["ticker"], c["issuer_key"], c["n_buyers"], c["n_buys"],
                     int(c["total_value"]), c["span_days"], ",".join(c["roles"]) or "-",
                     c["overlay"], c["pedigree"], c["event_filed"]))
    m.append("")
    m.append("## PH4 validation harness (blind, one pass) — forward excess vs SPY from observable filing date")
    m.append("")
    m.append("Cluster-collapsed events graded: {}. Excluded (no usable price "
             "series, NEVER imputed): {}.".format(len(graded), len(no_series)))
    m.append("")
    m.append("| horizon | n | hit rate | mean excess | median | std | t-stat |")
    m.append("|---|---|---|---|---|---|---|")
    for h in HZ:
        s = res[h]
        if s["n"] >= 2:
            m.append("| {}d | {} | {:.0%} | {:+.1%} | {:+.1%} | {:.1%} | {} |".format(
                h, s["n"], s["hit_rate"], s["mean"], s["median"], s["std"],
                "{:.2f}".format(s["t"]) if s["t"] is not None else "-"))
        else:
            m.append("| {}d | {} | insufficient n | | | | |".format(h, s["n"]))
    m.append("")
    m.append("## Honesty + coverage")
    m.append("")
    m.append("- One blind pass at the provisional floor. NO threshold tuning was "
             "done to find a positive — if the numbers above are flat or negative, "
             "THAT is the finding.")
    m.append("- {} of {} clusters had no usable price series (small-cap coverage "
             "gap) and were excluded and counted, never imputed.".format(
                 len(no_series), len(cl)))
    m.append("- Selection/survivorship caveats from SM-2 apply: this is a "
             "funnel-narrowing prior, not demonstrated edge, not a sizing input.")
    if dq:
        m.append("- Data quality: {} P-buy rows scanned; {} dropped for an "
                 "unparseable transaction date (EDGAR TZ-suffixed values), "
                 "dropped-and-counted, never coerced.".format(
                     dq.get("rows_seen", 0), dq.get("rows_dropped_bad_date", 0)))
    open(out, "w").write("\n".join(m) + "\n")


if __name__ == "__main__":
    sys.exit(main())
