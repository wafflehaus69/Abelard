"""SM-R1 L1 query layer — the SINGLE SOURCE OF TRUTH for L2/L3.

Every dashboard view and every PDF section reads THESE functions; no SQL lives
anywhere else in the reporting layer. Each function returns a dict with an
``as_of`` stamp plus ``rows`` so a caller always knows the corpus recency.

READ-ONLY BY CONSTRUCTION (Mando hard rule). This module opens the DB with the
sqlite3 URI ``mode=ro`` flag, so a write raises at the driver level — the surface
*cannot* mutate the corpus. It also imports NO module that carries a write path:
never prices/marketcap/discovery/form4/thirteenf_ingest, and never
``db.connect`` (which runs migrations). It reuses only the pure-read join engines
from ``phase4_joins``, handed the read-only connection.

Cross-surface CIK joins ALWAYS cast both sides to INTEGER: the registry stores
zero-padded CIKs, holdings/issuer_cik are zero-stripped, and reporting_cik is
verbatim from EDGAR — a naive string join silently returns zero rows.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3
import sys
from collections import defaultdict

from . import db as dbmod  # path constants + find_artifact ONLY — never connect()

# Trailing baseline for the sell-anomaly rate norm, the ratified baseline floor
# (>=3 distinct 12mo sellers), and the "elevated" tint threshold (Gate 1, Mando
# 2026-07-27). The threshold is a DISPLAY TINT on a ranked context feed, never a
# binary anomaly filter.
SELL_NORM_DAYS = 365
SELL_MIN_BASELINE = 3
SELL_ELEVATED_RATIO = 3.0

# The form4 columns the flow queries pull, in order.
_F4_COLS = ("accession", "reporting_cik", "reporting_person", "issuer_cik",
            "ticker", "tx_date", "code", "plan_flag", "shares", "value",
            "filed_date", "ingest_regime")


# ---------------------------------------------------------------- infra
def connect_ro(db_path=None):
    """Open the canonical DB strictly read-only. ``mode=ro`` makes any write
    raise at the SQLite layer — this is the structural read-only guarantee, not a
    convention. Never routes through db.connect (which migrates = writes)."""
    path = os.path.expanduser(db_path or dbmod.DB_PATH_DEFAULT)
    return sqlite3.connect("file:{}?mode=ro".format(path), uri=True)


def _as_of():
    """Synthesized UTC-ISO run stamp — the daemon stores no run-metadata table,
    so the read layer stamps its own as-of."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _iso10(s):
    """YYYY-MM-DD head, tolerant of EDGAR TZ suffixes ('2024-12-27-05:00');
    None if the head is not a real date (dropped, never coerced)."""
    s = (s or "")[:10]
    try:
        dt.date.fromisoformat(s)
        return s
    except ValueError:
        return None


def _win(anchor, days):
    return (dt.date.fromisoformat(anchor) - dt.timedelta(days=days)).isoformat()


def cik_int(cik):
    """Normalize any CIK spelling (zero-padded, zero-stripped, verbatim) to an
    int for cross-surface joins. None if unparseable. Guards the silent
    zero-row-join class — always compare cik_int(a) == cik_int(b)."""
    try:
        return int(str(cik).strip())
    except (TypeError, ValueError):
        return None


def _dedup_amendments(rows):
    """Read-side Form 4 amendment/supersede dedup (SM-R1 gap 3d). A 4/A carries a
    DIFFERENT accession than its original 4 and both persist (no form_type marker
    exists), so collapse rows sharing an economic key to the latest filing.
    Scoped to ingest_regime='watchlist' — the universal daily-index walk cannot
    ingest 4/A at all (its '^4\\s+' filter excludes it), so universal rows pass
    through untouched. Residual (accepted): an amendment that CHANGES shares will
    NOT collapse because shares is in the key — the safe under-dedup direction for
    a count/flow metric (dropping shares would risk merging two genuine same-day
    same-code trades). Keeps MAX(filed_date) then MAX(accession)."""
    best, passthrough = {}, []
    for r in rows:
        if r["ingest_regime"] != "watchlist":
            passthrough.append(r)
            continue
        key = (r["reporting_cik"] or r["reporting_person"],
               r["issuer_cik"] or r["ticker"], r["tx_date"], r["code"], r["shares"])
        rank = (r["filed_date"] or "", r["accession"] or "")
        cur = best.get(key)
        if cur is None or rank > (cur["filed_date"] or "", cur["accession"] or ""):
            best[key] = r
    return passthrough + list(best.values())


def _fetch_f4(con, codes, start, anchor, ticker=None):
    """Discretionary (plan_flag=0) open-market rows of the given codes with a
    tx_date in [start, anchor], amendment-deduped. Returns list of dicts."""
    ph = ",".join("?" for _ in codes)
    q = ("SELECT accession, reporting_cik, reporting_person, issuer_cik, ticker, "
         "tx_date, code, plan_flag, shares, value, filed_date, ingest_regime "
         "FROM form4_transactions WHERE code IN ({}) AND plan_flag=0 "
         "AND ticker IS NOT NULL AND substr(tx_date,1,10)>=? "
         "AND substr(tx_date,1,10)<=?".format(ph))
    params = list(codes) + [start, anchor]
    if ticker and ticker != "all":
        q += " AND UPPER(ticker)=?"
        params.append(ticker.upper())
    rows = [dict(zip(_F4_COLS, r)) for r in con.execute(q, params).fetchall()]
    return _dedup_amendments(rows)


def _issuer_key(r):
    return r["issuer_cik"] or ("TK:" + (r["ticker"] or "?"))


# ---------------------------------------------------------------- q_ownership_pressure
def q_ownership_pressure(con, target="all", window=90, anchor=None):
    """FLOW-based ownership pressure (SM-R1 gap 3c, v1 — uses NO ownership_after).

    Per issuer over the trailing window, computed from TRANSACTION ROWS only:
    distinct discretionary open-market buyers (code P) vs sellers (code S),
    buy/sell share volume, and net_shares = bought - sold. This is a FLOW
    (accumulating vs distributing), NOT a holding level. Levels ('how much they
    hold') wait on the direct/indirect re-ingest order — they are deliberately
    absent here so the front page carries no shaky running-total number.

    HONESTY: reporting-population = Section-16 insiders only (not float);
    discretionary = plan_flag=0; amendment double-counts removed via
    _dedup_amendments; the corpus is coverage-limited to backfilled issuers on the
    watchlist path plus the universal recency slice."""
    anchor = anchor or dt.date.today().isoformat()
    start = _win(anchor, window)
    rows = _fetch_f4(con, ("P", "S"), start, anchor, target)
    agg = defaultdict(lambda: {"buyers": set(), "sellers": set(),
                               "buy_shares": 0.0, "sell_shares": 0.0,
                               "n_buys": 0, "n_sells": 0, "ticker": None})
    for r in rows:
        a = agg[_issuer_key(r)]
        a["ticker"] = a["ticker"] or r["ticker"]
        who = r["reporting_cik"] or r["reporting_person"]
        sh = r["shares"] or 0.0
        if r["code"] == "P":
            a["buyers"].add(who); a["buy_shares"] += sh; a["n_buys"] += 1
        else:
            a["sellers"].add(who); a["sell_shares"] += sh; a["n_sells"] += 1
    out = []
    for key, a in agg.items():
        net = a["buy_shares"] - a["sell_shares"]
        out.append({
            "issuer_cik": key, "ticker": a["ticker"],
            "distinct_buyers": len(a["buyers"]), "distinct_sellers": len(a["sellers"]),
            "buy_shares": a["buy_shares"], "sell_shares": a["sell_shares"],
            "net_shares": net,
            "direction": "accumulating" if net > 0 else "distributing" if net < 0 else "flat",
            "n_buys": a["n_buys"], "n_sells": a["n_sells"],
        })
    out.sort(key=lambda x: -abs(x["net_shares"]))
    return {"as_of": _as_of(), "window_days": window, "anchor": anchor,
            "target": target,
            "basis": "FLOW from discretionary open-market P/S transaction rows; "
                     "reporting-population Section-16 filers; plan_flag=0; "
                     "amendment-deduped; levels deferred to re-ingest order",
            "rows": out}


# ---------------------------------------------------------------- q_sell_anomaly
def q_sell_anomaly(con, window=90, anchor=None, min_baseline=SELL_MIN_BASELINE):
    """g1-SELL RANKED CONTEXT FEED (SM-R1, Gate-1 thresholds ratified 2026-07-27).

    Per issuer: distinct non-plan (plan_flag=0) open-market sellers (code S) in the
    window vs that issuer's own trailing-12mo seller rate, plus window sell dollar
    volume. This is a RANKED CONTEXT FEED, NEVER a binary anomaly list — every row
    is returned, ranked descending by rate_ratio, and `elevated` (ratio >= 3.0 with
    a sufficient >=3-seller baseline) is a DISPLAY TINT only. `n_yr`
    (distinct_sellers_12mo) rides every row so thin baselines are visible, not
    hidden. The ratio is None (never infinity) when the baseline is empty.
    Amendment-deduped.

    KNOWN LIMITATION (filed, Gate 1): insider selling is seasonal — vesting and
    post-earnings windows cluster sells in grant season, so a 90d window against an
    annualized rate mechanically inflates then. The honest future refinement is a
    same-window-last-year comparison; not built now. In isolation this is CONTEXT;
    it earns eyes at the extreme tail and in tension joins (a heavy sell cluster on
    a name a tracked principal is long, or an overlay name)."""
    anchor = anchor or dt.date.today().isoformat()
    wstart = _win(anchor, window)
    ystart = _win(anchor, SELL_NORM_DAYS)
    rows = _fetch_f4(con, ("S",), ystart, anchor)  # 12mo pull; window is a subset
    win_sellers, yr_sellers, tick = defaultdict(set), defaultdict(set), {}
    win_value = defaultdict(float)
    for r in rows:
        txd = _iso10(r["tx_date"])
        if txd is None:
            continue
        key = _issuer_key(r)
        tick[key] = tick.get(key) or r["ticker"]
        who = r["reporting_cik"] or r["reporting_person"]
        yr_sellers[key].add(who)
        if wstart <= txd <= anchor:
            win_sellers[key].add(who)
            win_value[key] += r["value"] or 0.0
    frac = window / SELL_NORM_DAYS
    out = []
    for key, yr in yr_sellers.items():
        n_win, n_yr = len(win_sellers.get(key, ())), len(yr)
        expected = n_yr * frac
        ratio = (n_win / expected) if expected > 0 else None
        sufficient = n_yr >= min_baseline
        out.append({
            "issuer_cik": key, "ticker": tick.get(key),
            "distinct_sellers_window": n_win, "distinct_sellers_12mo": n_yr,
            "window_sell_value": round(win_value.get(key, 0.0), 2),
            "expected_window_rate": round(expected, 3),
            "rate_ratio": round(ratio, 3) if ratio is not None else None,
            "baseline_sufficient": sufficient,
            "elevated": bool(sufficient and ratio is not None
                             and ratio >= SELL_ELEVATED_RATIO),
        })
    out.sort(key=lambda x: (x["rate_ratio"] is None, -(x["rate_ratio"] or 0)))
    return {"as_of": _as_of(), "window_days": window, "anchor": anchor,
            "norm_days": SELL_NORM_DAYS, "min_baseline": min_baseline,
            "elevated_ratio": SELL_ELEVATED_RATIO,
            "note": "RANKED CONTEXT FEED — `elevated` (ratio>={} at >=3 sellers/yr) "
                    "is a tint, not an anomaly verdict. Confounders carried: "
                    "plan_flag is filing-level and 10b5-1 coverage is materially "
                    "incomplete so non-plan sellers are inflated; insider selling "
                    "is SEASONAL (grant-season vesting inflates a 90d-vs-annual "
                    "ratio) — same-window-last-year is the honest future upgrade; "
                    "the coverage-limited corpus leaves the baseline thin for "
                    "out-of-scope issuers.".format(SELL_ELEVATED_RATIO),
            "rows": out}


def _hist(rows):
    edges = [(0, 0.5), (0.5, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 1e9)]
    h = {}
    for lo, hi in edges:
        label = ">=3.0" if hi > 1e8 else "{:.1f}-{:.1f}".format(lo, hi)
        h[label] = sum(1 for r in rows if r["rate_ratio"] is not None and lo <= r["rate_ratio"] < hi)
    return h


def _distribution_report(res):
    """Base-rate distribution of the sell rate_ratio across issuers — the Gate-1
    artifact Mando reads to set a threshold.

    The ratio is dominated by a SMALL-COUNT ARTIFACT: an issuer with a single
    12mo seller who sold inside the window has expected = 1*(w/365) ~ 0.25, so its
    ratio pins near 4.0 with no real signal. So the report ALSO breaks issuers
    down by baseline size (distinct 12mo sellers) and gives a second histogram
    over the meaningful-baseline subset (>=3 sellers/yr). No threshold is chosen —
    that is Mando's call — but the artifact must not be read past."""
    defined = [r for r in res["rows"] if r["rate_ratio"] is not None]
    def bcount(lo, hi):
        return sum(1 for r in defined if lo <= r["distinct_sellers_12mo"] < hi)
    baseline_breakdown = {"n_yr=1": bcount(1, 2), "n_yr=2": bcount(2, 3),
                          "n_yr=3-4": bcount(3, 5), "n_yr>=5": bcount(5, 1e9)}
    meaningful = [r for r in defined if r["distinct_sellers_12mo"] >= 3]
    return {"issuers_with_sells": len(res["rows"]),
            "issuers_scored": len(defined),
            "baseline_breakdown": baseline_breakdown,
            "histogram_all": _hist(defined),
            "meaningful_baseline_issuers": len(meaningful),
            "histogram_meaningful_ge3sellers": _hist(meaningful),
            "top_meaningful": sorted(
                meaningful, key=lambda r: -(r["rate_ratio"] or 0))[:15]}


# ---------------------------------------------------------------- CLI
def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-R1 L1 query layer")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--anchor", default=dt.date.today().isoformat())
    ap.add_argument("--window", type=int, default=90)
    ap.add_argument("--query", choices=["pressure", "sell_dist"], default="sell_dist")
    ap.add_argument("--target", default="all")
    args = ap.parse_args(argv)
    con = connect_ro(args.db)
    if args.query == "pressure":
        res = q_ownership_pressure(con, args.target, args.window, args.anchor)
        print("[pressure] as_of={} window={} issuers={}".format(
            res["as_of"], res["window_days"], len(res["rows"])))
        for r in res["rows"][:25]:
            print("  {} ({}) net={:+.0f} buyers={} sellers={} dir={}".format(
                r["ticker"], r["issuer_cik"], r["net_shares"],
                r["distinct_buyers"], r["distinct_sellers"], r["direction"]))
    else:
        res = q_sell_anomaly(con, args.window, args.anchor)
        d = _distribution_report(res)
        print("[sell_dist] as_of={} window={}d norm={}d".format(
            res["as_of"], res["window_days"], res["norm_days"]))
        print("  issuers_with_sells={} scored={}".format(
            d["issuers_with_sells"], d["issuers_scored"]))
        print("  baseline size (distinct 12mo sellers):")
        for k, v in d["baseline_breakdown"].items():
            print("    {:>9}: {}".format(k, v))
        print("  rate_ratio histogram ALL scored (dominated by the n_yr=1 artifact):")
        for k, v in d["histogram_all"].items():
            print("    {:>8}: {}".format(k, v))
        print("  rate_ratio histogram MEANINGFUL baseline (>=3 sellers/yr, n={}):".format(
            d["meaningful_baseline_issuers"]))
        for k, v in d["histogram_meaningful_ge3sellers"].items():
            print("    {:>8}: {}".format(k, v))
    return 0


if __name__ == "__main__":
    sys.exit(main())
