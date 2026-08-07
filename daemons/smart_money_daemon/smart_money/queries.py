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
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict

from . import db as dbmod  # path constants + find_artifact ONLY — never connect()

# phase4_joins (join engines) and overlay are LAZY-imported inside the two
# functions that use them: both are read-only (they only SELECT / read yaml), but
# lazy-importing keeps `import queries` light and avoids pulling yaml for the five
# functions that never touch them. Registry loading is re-implemented below rather
# than importing events, which chains events -> scorecard -> prices (a write path).


class QueryError(RuntimeError):
    """A read-layer invariant failed loud (e.g. a registry seed orphaned by a
    person merge). Never silently skipped."""

# Trailing baseline for the sell-anomaly rate norm, the ratified baseline floor
# (>=3 distinct 12mo sellers), and the "elevated" tint threshold (Gate 1, Mando
# 2026-07-27). The threshold is a DISPLAY TINT on a ranked context feed, never a
# binary anomaly filter.
SELL_NORM_DAYS = 365
SELL_MIN_BASELINE = 3
SELL_ELEVATED_RATIO = 3.0

# The form4 columns the flow queries pull, in order.
_F4_COLS = ("accession", "reporting_cik", "reporting_person", "issuer_cik",
            "ticker", "tx_date", "code", "plan_flag", "shares", "value", "price",
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
    """Collapse rows reporting the SAME economic trade under different accessions
    to the latest filing. REGIME-AGNOSTIC: this catches both 4/A amendments (a
    different accession than the original 4, no form_type marker to key on) AND
    the recurring-re-report artifact seen on the universal path — e.g. a
    closed-end-fund director filing the same distribution across multiple
    accessions, and identical repeated rows within one filing. Keys on (reporting
    person/cik, issuer cik/ticker, tx_date, code, shares); keeps MAX(filed_date)
    then MAX(accession). Order-preserving. Residual (accepted): an amendment that
    CHANGES shares will not collapse — the safe under-dedup direction."""
    best, order = {}, []
    for r in rows:
        key = (r["reporting_cik"] or r["reporting_person"],
               r["issuer_cik"] or r["ticker"], r["tx_date"], r["code"], r["shares"])
        rank = (r["filed_date"] or "", r["accession"] or "")
        cur = best.get(key)
        if cur is None:
            best[key] = r
            order.append(key)
        elif rank > (cur["filed_date"] or "", cur["accession"] or ""):
            best[key] = r
    return [best[k] for k in order]


def _fetch_f4(con, codes, start, anchor, ticker=None, plan="discretionary"):
    """Open-market rows of the given codes with a tx_date in [start, anchor],
    amendment-deduped. plan: 'discretionary' (plan_flag=0, the default used by the
    flow/cluster/sell aggregates), 'planned' (plan_flag=1, 10b5-1), or 'all'."""
    ph = ",".join("?" for _ in codes)
    q = ("SELECT accession, reporting_cik, reporting_person, issuer_cik, ticker, "
         "tx_date, code, plan_flag, shares, value, price, filed_date, ingest_regime "
         "FROM form4_transactions WHERE code IN ({}) "
         "AND ticker IS NOT NULL AND substr(tx_date,1,10)>=? "
         "AND substr(tx_date,1,10)<=?".format(ph))
    params = list(codes) + [start, anchor]
    if plan == "discretionary":
        q += " AND plan_flag=0"
    elif plan == "planned":
        q += " AND plan_flag=1"
    if ticker and ticker != "all":
        q += " AND UPPER(ticker)=?"
        params.append(ticker.upper())
    rows = []
    for raw in con.execute(q, params).fetchall():
        r = dict(zip(_F4_COLS, raw))
        # Normalize once here so every consumer gets clean YYYY-MM-DD dates —
        # EDGAR transactionDate values can carry a trailing TZ offset. A row whose
        # tx_date will not parse cannot be placed in time and is dropped.
        r["tx_date"] = _iso10(r["tx_date"])
        if r["tx_date"] is None:
            continue
        r["filed_date"] = _iso10(r["filed_date"]) or r["tx_date"]
        rows.append(r)
    return _dedup_amendments(rows)


def _issuer_key(r):
    return r["issuer_cik"] or ("TK:" + (r["ticker"] or "?"))


def _close_on(con, ticker, on_date):
    """Split/dividend-adjusted close on or before `on_date` (direct read-only
    SELECT, never the write-through prices.eod cache). (close, date) or (None,None)."""
    r = con.execute(
        "SELECT adj_close, date FROM prices WHERE ticker=? AND price_type='eod' "
        "AND date<=? ORDER BY date DESC LIMIT 1", (ticker, on_date)).fetchone()
    return (r[0], r[1]) if r else (None, None)


def _latest_close(con, ticker):
    r = con.execute(
        "SELECT adj_close, date FROM prices WHERE ticker=? AND price_type='eod' "
        "ORDER BY date DESC LIMIT 1", (ticker,)).fetchone()
    return (r[0], r[1]) if r else (None, None)


_NONTICKER = {"NONE", "N/A", "N.A.", "NA", ""}


def _disp_ticker(tk):
    """Display normalization (SM-R1 ruling): upper-case, and render an unmapped or
    non-security placeholder (NONE / N/A / empty) as '-', never the raw string."""
    tk = (tk or "").upper().strip()
    return "-" if tk in _NONTICKER else tk


def _scoped_tickers():
    """The watchlist scope for the trades feed: every ticker in any overlay set
    (conviction book + watchlist + trump_network + thiel_network), from the
    Mando-owned overlay.yaml. Read-only."""
    from .overlay import load_overlay
    return {t for t in load_overlay().scoped() if t and t not in _NONTICKER}


# ---------------------------------------------------------------- q_insider_trades
_TRADE_SORT_FIELDS = {"person": "reporting_person", "ticker": "ticker",
                      "side": "code", "trade_date": "tx_date",
                      "reported_date": "filed_date", "value": "value",
                      "shares": "shares"}


def q_insider_trades(con, side="all", window=90, anchor=None, plan="all",
                     smid_only=False, per_page=100, page=1, full=False,
                     scope="scoped", sort=None, direction="desc"):
    """SM-R1 insider-trades feed. A flat, amendment-deduped list of Form 4
    open-market transactions with per-trade enrichment: the Trade Date (with a
    red Reported-Date fallback when a trade date is absent), the entry close on
    the trade date vs the latest close (+ % return), the 10b5-1 plan flag, and the
    SMID band, and the provenance tag (book | watch | trump | thiel) saying why
    the issuer is in scope. Ranked newest-first; PAGINATED — only the requested
    page's rows are price-enriched (or every row when full=True, for a whole-dataset
    CSV export). side: buy | sell | all. plan: all | discretionary | planned.
    scope: 'scoped' (every overlay set, the default) or 'all' (the full corpus)."""
    anchor = anchor or dt.date.today().isoformat()
    start = _win(anchor, window)
    codes = {"buy": ("P",), "sell": ("S",)}.get(side, ("P", "S"))
    rows = _fetch_f4(con, codes, start, anchor, plan=plan)
    from .overlay import load_overlay
    ov = load_overlay()  # scope filter + per-row provenance from one load
    if scope != "all":
        scoped = {t for t in ov.scoped() if t and t not in _NONTICKER}
        rows = [r for r in rows if (r["ticker"] or "").upper() in scoped]
    tks = sorted({(r["ticker"] or "").upper() for r in rows if r["ticker"]})
    bands = {}
    if tks:
        ph = ",".join("?" for _ in tks)
        for tk, band in con.execute(
            "SELECT UPPER(ticker), band FROM market_cap WHERE UPPER(ticker) IN "
            "({})".format(ph), tks):
            bands[tk] = band
    if smid_only:
        rows = [r for r in rows
                if bands.get((r["ticker"] or "").upper()) in ("micro", "small", "mid")]
    sf = _TRADE_SORT_FIELDS.get(sort)
    if sf:                                         # sort by a raw column, None last
        present = [r for r in rows if r.get(sf) is not None and r.get(sf) != ""]
        missing = [r for r in rows if r.get(sf) is None or r.get(sf) == ""]
        present.sort(key=lambda r: r[sf], reverse=(direction != "asc"))
        rows = present + missing
    else:                                          # default: newest trade first
        rows.sort(key=lambda r: (r["tx_date"], r["filed_date"]), reverse=True)
    total = len(rows)
    per_page = max(1, per_page)
    pages = max(1, (total + per_page - 1) // per_page)
    page = min(max(1, page), pages)               # clamp to range, matching _page_slice
    if full:
        page_rows = rows                          # every row (whole-dataset export)
    else:
        off = (page - 1) * per_page
        page_rows = rows[off:off + per_page]
    out = []
    for r in page_rows:
        tk = (r["ticker"] or "").upper()
        disp = _disp_ticker(tk)                   # '-' for NONE / N/A / empty
        real = disp != "-"
        trade_date = r["tx_date"]                 # Form 4 tx_date is the trade date
        date_is_reported = trade_date is None
        if date_is_reported:                      # fall back to the reported date
            trade_date = r["filed_date"]
        entry, entry_d = _close_on(con, tk, trade_date) if real else (None, None)
        latest, latest_d = _latest_close(con, tk) if real else (None, None)
        pct = ((latest - entry) / entry) if (entry and latest) else None
        try:
            lag = (dt.date.fromisoformat(r["filed_date"])
                   - dt.date.fromisoformat(trade_date)).days
        except (ValueError, TypeError):
            lag = None
        out.append({
            "person": r["reporting_person"], "ticker": disp,
            "side": "buy" if r["code"] == "P" else "sell",
            "trade_date": trade_date, "date_is_reported": date_is_reported,
            "reported_date": r["filed_date"], "lag_days": lag,
            "shares": r["shares"], "value": r["value"],
            "plan_10b5_1": bool(r["plan_flag"]),
            "entry_close": entry, "latest_close": latest,
            "pct_since_trade": round(pct, 4) if pct is not None else None,
            "smid_band": bands.get(tk),
            "provenance": ov.provenance(tk),      # book | watch | trump | thiel | None
        })
    return {"as_of": _as_of(), "side": side, "window_days": window, "anchor": anchor,
            "plan": plan, "smid_only": smid_only, "scope": scope,
            "sort": sort if sf else None, "direction": direction,
            "per_page": per_page, "page": page, "pages": pages,
            "returned": len(out), "total_matching": total, "rows": out}


# ---------------------------------------------------------------- q_net_flows
_FLOW_WINDOWS = (("7", 7), ("30", 30), ("90", 90), ("180", 180), ("365", 365),
                 ("all", None))
# Data-quality guard for the MAGNITUDE metrics. value = shares*price by construction,
# so a corrupt Form 4 row shows up as an implausible per-share price (no US equity but
# BRK.A ~$600k trades above ~$1M/share), an astronomical dollar value, or an impossible
# share count. Any of those condemns BOTH the dollars and the shares for that row, which
# are dropped together; the insider-count (persons) metric is identity-based and stays
# incorruptible. A handful of rows in the corpus, but astronomically large.
_PRICE_SANITY_MAX = 1_000_000.0
_VALUE_SANITY_MAX = 1e11
_SHARES_SANITY_MAX = 1e10


def q_net_flows(con, anchor=None, scope="all"):
    """SM-R1 net buy/sell board. For every SCRAPED security (any ticker carrying an
    open-market Form 4 P/S row), the net insider flow over nested lookbacks — 7 / 30 /
    90 / 180 / 365 days and all-time — as three metrics:
      value   net dollars  = sum(buy value)  - sum(sell value)   (value NULL -> 0)
      shares  net shares   = sum(buy shares) - sum(sell shares)
      persons net insiders = distinct buyers - distinct sellers
    Buys are code P, sells code S; both planned and discretionary are counted (a
    complete net-bought/sold accounting). Amendment-deduped. scope 'all' (every
    scraped ticker, the default) or 'scoped' (overlay sets only). Rows are sorted by
    the all-time value of the requested `metric`, most net-bought first. Non-security
    placeholders (NONE / N/A / empty) are excluded — securities only."""
    anchor = anchor or dt.date.today().isoformat()
    a = dt.date.fromisoformat(anchor)
    cutoffs = [(lbl, (a - dt.timedelta(days=d)).isoformat() if d else None)
               for lbl, d in _FLOW_WINDOWS]
    rows = _fetch_f4(con, ("P", "S"), "0001-01-01", anchor, plan="all")
    scoped = _scoped_tickers() if scope != "all" else None
    agg = defaultdict(lambda: {lbl: {"val": 0.0, "sh": 0, "buyers": set(),
                                     "sellers": set()} for lbl, _ in _FLOW_WINDOWS})
    rows_excluded = 0
    for r in rows:
        tk = _disp_ticker(r["ticker"])
        if tk == "-":
            continue
        if scoped is not None and tk not in scoped:
            continue
        who = r["reporting_cik"] or r["reporting_person"]
        val, sh = (r["value"] or 0.0), (r["shares"] or 0)
        price = r["price"]
        # A row is magnitude-trustworthy only when its per-share price, dollar value,
        # and share count are all sane. If not, drop BOTH its dollars and its shares
        # (value = shares*price, so one bad field poisons the other) — persons still
        # counts, since the insider's identity is not corrupt.
        row_ok = ((price is None or abs(price) <= _PRICE_SANITY_MAX)
                  and abs(val) <= _VALUE_SANITY_MAX
                  and abs(sh) <= _SHARES_SANITY_MAX)
        if (val or sh) and not row_ok:
            rows_excluded += 1
            val, sh = 0.0, 0
        sign = 1 if r["code"] == "P" else -1
        d = r["tx_date"]
        for lbl, cut in cutoffs:
            if cut is None or d >= cut:
                b = agg[tk][lbl]
                b["val"] += sign * val
                b["sh"] += sign * sh
                (b["buyers"] if sign > 0 else b["sellers"]).add(who)
    out = []
    for tk, bl in agg.items():
        row = {"ticker": tk}
        for lbl, _ in _FLOW_WINDOWS:
            b = bl[lbl]
            row["value_" + lbl] = round(b["val"], 2)
            row["shares_" + lbl] = b["sh"]
            row["persons_" + lbl] = len(b["buyers"]) - len(b["sellers"])
        out.append(row)
    out.sort(key=lambda r: r["persons_all"], reverse=True)  # stable default; view re-sorts
    return {"as_of": _as_of(), "anchor": anchor, "scope": scope,
            "windows": [lbl for lbl, _ in _FLOW_WINDOWS],
            "rows_excluded": rows_excluded,
            "count": len(out), "rows": out}


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


# ---------------------------------------------------------------- q_positioning_events
def q_positioning_events(since=None, overlay_only=False, scans_dir=None):
    """SM-R1 gap 3a: positioning events read from the scan ENVELOPE JSONs — the
    only record carrying the rich event (person/role/amount/plan_flag/filing_ref +
    the four computed flags conviction_overlay/watchlist_overlay/cluster/sentinel).
    scan_events is a non-rejoinable dedup ledger, so it is deliberately NOT used.
    Overlay/sentinel-flagged events sort first, then newest. The 'overlay-flagged
    13F' predicate is intentionally absent — 13F events carry ticker=None and can
    never be flagged (query thirteenf_holdings directly for that intent)."""
    import glob
    sdir = scans_dir or dbmod.SCANS_DIR
    seen, events = set(), []
    for path in sorted(glob.glob(os.path.join(sdir, "scan_*.json"))):
        try:
            with open(path, encoding="utf-8") as fh:
                env = json.load(fh)
        except (OSError, ValueError):
            continue  # a torn/partial envelope is skipped, never fabricated
        for ev in env.get("events", []):
            eid = ev.get("event_id")
            if eid and eid in seen:
                continue
            if eid:
                seen.add(eid)
            events.append(ev)

    def _edate(ev):
        return ev.get("disclosure_date") or ev.get("tx_date") or ""

    def _flagged(ev):
        f = ev.get("flags") or {}
        return bool(f.get("conviction_overlay") or f.get("watchlist_overlay")
                    or f.get("sentinel") or f.get("cluster"))

    if since:
        events = [e for e in events if _edate(e) >= since]
    if overlay_only:
        events = [e for e in events if _flagged(e)]
    events.sort(key=_edate, reverse=True)     # newest first ...
    events.sort(key=lambda e: not _flagged(e))  # ... then flagged first (stable)
    return {"as_of": _as_of(), "since": since, "count": len(events),
            "source": "scan envelope JSONs (rich event lives only here)",
            "rows": events}


# ---------------------------------------------------------------- q_sentinel_log
def _load_registry():
    """Load the Mando-ratified registry, state-home first (where scorecard writes)
    then the repo snapshot. Returns (entries, as_of). Fail-loud if absent."""
    path = dbmod.find_artifact("registry.json", "analysis")
    if not path or not os.path.exists(path):
        raise QueryError("registry.json not found (looked via find_artifact)")
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    return data.get("entries", []), data.get("as_of")


def q_sentinel_log(con, window=180, anchor=None, entries=None):
    """SM-R1: activity by every registry seed person/entity, newest first. Shape-A
    (congress person_id) -> congress_trades; Shape-B (SEC entity cik) ->
    thirteenf_holdings (manager_13f) or form4_transactions (trump/thiel_network). All
    CIK joins CAST both sides to INTEGER (registry is zero-padded, holdings/form4
    are not). A Shape-A person_id orphaned by a person-merge FAILS LOUD."""
    anchor = anchor or dt.date.today().isoformat()
    start = _win(anchor, window)
    reg_asof = None
    if entries is None:
        entries, reg_asof = _load_registry()
    rows = []
    for e in entries:
        name, role, pid = e.get("name"), e.get("role"), e.get("person_id")
        if pid is not None:
            if not con.execute("SELECT 1 FROM persons WHERE person_id=?", (pid,)).fetchone():
                raise QueryError("registry person_id {} ({}) orphaned in persons "
                                 "table — a merge ran after the registry froze".format(pid, name))
            for tk, side, lo, hi, txd, disc, lag, owner, aname in con.execute(
                "SELECT ticker, side, amt_low, amt_high, tx_date, disclosure_date, "
                "lag_days, owner, asset_name FROM congress_trades WHERE person_id=? "
                "AND superseded=0 AND disclosure_date>=? ORDER BY disclosure_date DESC",
                (pid, start)):
                # asset_name distinguishes the many non-equity (ticker NULL) trades
                # a single PTR can list — otherwise they render identically.
                rows.append({"seed": name, "role": role, "src": "congress",
                             "event_date": disc, "tx_date": txd,
                             "ticker": tk or (aname or "")[:40],
                             "action": side, "amt_low": lo, "amt_high": hi,
                             "lag_days": lag, "owner": owner})
        elif role == "manager_13f":
            for per, filed, tk, pc, val, sh in con.execute(
                "SELECT period, filed_date, ticker, put_call, value, shares FROM "
                "thirteenf_holdings WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) "
                "AND filed_date>=? ORDER BY filed_date DESC, period DESC",
                (e.get("cik"), start)):
                rows.append({"seed": name, "role": role, "src": "13f",
                             "event_date": filed, "period": per, "ticker": tk,
                             "action": pc, "value": val, "shares": sh})
        else:  # trump_network / thiel_network -> form4
            for txd, filed, tk, code, val, plan, r_role in con.execute(
                "SELECT tx_date, filed_date, ticker, code, value, plan_flag, role "
                "FROM form4_transactions WHERE "
                "CAST(reporting_cik AS INTEGER)=CAST(? AS INTEGER) AND filed_date>=? "
                "ORDER BY filed_date DESC", (e.get("cik"), start)):
                rows.append({"seed": name, "role": role, "src": "form4",
                             "event_date": filed, "tx_date": txd, "ticker": tk,
                             "action": code, "value": val, "plan_flag": plan})
    rows.sort(key=lambda r: r.get("event_date") or "", reverse=True)
    return {"as_of": _as_of(), "registry_as_of": reg_asof, "window_days": window,
            "anchor": anchor, "count": len(rows), "rows": rows}


# ---------------------------------------------------------------- q_principal_convergence
def _qoq_disagreements(con, period=None):
    """NEW cross-manager QoQ pairing (SM-R1 gap 3b): for a quarter transition, one
    filer ADDING or sizing up a name while ANOTHER exits or sizes it down. Built on
    join_d (long-only), so option positioning is out of this view. This is a
    DIFFERENT notion from the intra-quarter directional disagreement and is never
    pooled with it."""
    from .phase4_joins import join_d_new_positions
    d = join_d_new_positions(con)
    acc, dist = defaultdict(set), defaultdict(set)
    for r in d["adds"]:
        acc[(r["ticker"], r["period"])].add(r["cik"])
    for r in d["exits"]:
        dist[(r["ticker"], r["period"])].add(r["cik"])
    for r in d["size_changes"]:
        (acc if r["dir"] == "up_2x" else dist)[(r["ticker"], r["period"])].add(r["cik"])
    out = []
    for key in set(acc) | set(dist):
        a, di = acc.get(key, set()), dist.get(key, set())
        if a and di and len(a | di) >= 2:  # >=2 distinct managers, opposite sides
            out.append({"ticker": key[0], "period": key[1],
                        "accumulating_ciks": sorted(a), "distributing_ciks": sorted(di)})
    if period:
        out = [r for r in out if r["period"] == period]
    return sorted(out, key=lambda r: r["period"], reverse=True)


def q_principal_convergence(con, period=None):
    """SM-R1 gap 3b: TWO disagreement notions, LABELED SEPARATELY, never pooled.
    (1) intra-quarter directional convergence + disagreement from the fixed join_a
    (direction = net long+call minus put; labelled 'put-heavy', NOT 'short', since
    13F reports no real shorts). (2) QoQ accumulate-vs-distribute disagreement — a
    NEW cross-manager pairing on join_d. Universe = the 6 confirmed 13F CIKs."""
    from .phase4_joins import join_a_multi_principal, convergence_accounting
    convergences = join_a_multi_principal(con)
    if period:
        convergences = [r for r in convergences if r["period"] == period]
    for r in convergences:  # rename in the human-facing layer
        if r["converge_dir"] == "short":
            r["converge_dir"] = "put-heavy"
        r["short_filers_put_heavy"] = r.pop("short_filers")
    acc = convergence_accounting(con)
    intra = [r for r in acc["opposed"] if not period or r["period"] == period]
    return {"as_of": _as_of(), "period": period,
            "convergences": convergences,
            "intra_quarter_disagreements": intra,
            "qoq_accumulate_distribute_disagreements": _qoq_disagreements(con, period),
            "note": "13F universe = 6 confirmed CIKs. 'put-heavy' = put value > "
                    "long+call value, NOT a real short. The intra-quarter and QoQ "
                    "disagreement notions answer different questions and are never "
                    "pooled. QoQ pairing is long-only, so options are out of it."}


# ---------------------------------------------------------------- q_cluster_context
def q_cluster_context(con, window=180, floor=3, window_days=30, anchor=None):
    """SM-R1: g1 buy-cluster CONTEXT with the capitulation-timeline read. A cluster
    is a per-issuer non-overlapping `window_days` window with >=floor distinct
    discretionary (plan_flag=0) P-buyers; `calendar_months` and the `capitulation`
    flag (all buys in ONE month = a coordinated bottom-fishing read vs slow
    accumulation) make the SM-verdict lesson visible. CONTEXT, never an alert."""
    anchor = anchor or dt.date.today().isoformat()
    start = _win(anchor, window)
    by_issuer = defaultdict(list)
    for r in _fetch_f4(con, ("P",), start, anchor):
        by_issuer[_issuer_key(r)].append(r)
    out = []
    for key, rs in by_issuer.items():
        rs.sort(key=lambda r: r["tx_date"])
        used = 0
        while used < len(rs):
            wstart = rs[used]["tx_date"]
            wend = (dt.date.fromisoformat(wstart) + dt.timedelta(days=window_days)).isoformat()
            seg = [r for r in rs[used:] if r["tx_date"] <= wend]
            distinct = {}
            for r in seg:
                distinct.setdefault(r["reporting_cik"] or r["reporting_person"], r)
            if len(distinct) >= floor:
                order = sorted(distinct.values(), key=lambda r: r["filed_date"])
                months = sorted({r["tx_date"][:7] for r in seg})
                out.append({
                    "issuer_cik": key, "ticker": seg[0]["ticker"],
                    "n_buyers": len(distinct), "n_buys": len(seg),
                    "window_start": wstart, "event_filed": order[floor - 1]["filed_date"],
                    "span_days": (dt.date.fromisoformat(seg[-1]["tx_date"])
                                  - dt.date.fromisoformat(wstart)).days,
                    "calendar_months": months, "capitulation": len(months) == 1,
                    "total_value": sum((r["value"] or 0.0) for r in seg),
                })
                used += len(seg)  # collapse: consume the window
            else:
                used += 1
    out.sort(key=lambda c: -c["n_buyers"])
    return {"as_of": _as_of(), "window_days": window, "floor": floor,
            "anchor": anchor, "count": len(out), "rows": out}


# ---------------------------------------------------------------- q_ticker_panel
def q_ticker_panel(con, ticker, pressure_window=180, sparkline_days=180, anchor=None):
    """SM-R1: three-surface drill-down for one ticker — insider (Form 4 + the flow
    pressure) | congressional | 13F principal positions (direction-netted) — plus a
    price sparkline from a DIRECT read-only SELECT (never prices.eod(), a
    write-through cache). Overlay membership stated."""
    from .overlay import load_overlay
    tk = ticker.upper()
    conv, watch = load_overlay().match(tk)
    insider = [{"code": c, "plan_flag": pf, "n": n, "shares": sh, "value": val,
                "distinct_filers": nb} for c, pf, n, sh, val, nb in con.execute(
        "SELECT code, plan_flag, COUNT(*), SUM(shares), SUM(value), "
        "COUNT(DISTINCT reporting_cik) FROM form4_transactions WHERE UPPER(ticker)=? "
        "GROUP BY code, plan_flag ORDER BY COUNT(*) DESC", (tk,))]
    congress = [{"name": nm, "side": sd, "amt_low": lo, "amt_high": hi,
                 "tx_date": txd, "disclosure_date": disc, "owner": ow}
                for nm, sd, lo, hi, txd, disc, ow in con.execute(
        "SELECT p.name, ct.side, ct.amt_low, ct.amt_high, ct.tx_date, "
        "ct.disclosure_date, ct.owner FROM congress_trades ct JOIN persons p "
        "USING(person_id) WHERE UPPER(ct.ticker)=? AND ct.superseded=0 "
        "ORDER BY ct.tx_date DESC", (tk,))]
    net13f = defaultdict(float)
    for cik, per, pc, val in con.execute(
        "SELECT cik, period, put_call, value FROM thirteenf_holdings WHERE UPPER(ticker)=?",
        (tk,)):
        net13f[(cik, per)] += (val or 0) * (-1 if pc == "put" else 1)
    holdings = [{"cik": c, "period": p, "net_value": v}
                for (c, p), v in sorted(net13f.items(),
                                        key=lambda x: (x[0][1], x[0][0]), reverse=True)]
    sstart = _win(anchor or dt.date.today().isoformat(), sparkline_days)
    spark = [{"date": d, "adj_close": ac} for d, ac in con.execute(
        "SELECT date, adj_close FROM prices WHERE ticker=? AND price_type='eod' "
        "AND date>=? ORDER BY date", (tk, sstart))]
    return {"as_of": _as_of(), "ticker": tk,
            "overlay": {"conviction": conv, "watchlist": watch},
            "insider_by_code": insider,
            "ownership_pressure": q_ownership_pressure(con, tk, pressure_window, anchor)["rows"],
            "congress": congress, "thirteenf_net": holdings, "price_sparkline": spark,
            "note": "prices via direct read-only SELECT; 13F net = long+call-put "
                    "per (cik, period); congress amounts are bands not points."}


# ---------------------------------------------------------------- q_portfolio (SM-P1)
_INSTRUMENT = {"long": "SH", "put": "PUT", "call": "CALL"}


def _tracked_filers():
    """The Mando-confirmed 13F filers from the registry (role manager_13f), in
    registry order. (cik, name) pairs. Read-only (registry JSON) — queries.py must
    not import thirteenf_ingest (a write-path module), so the set comes from the
    registry the scorecard writes, not the ingest constant."""
    entries, _ = _load_registry()
    return [(e.get("cik"), e.get("name")) for e in entries
            if e.get("role") == "manager_13f" and e.get("cik")]


def _filer_thesis():
    """{cik: thesis} for tracked 13F filers. A GROUPING LABEL for the shelf
    ({ai_tmt, biotech, macro, activist, value, contrarian}), never a performance claim."""
    entries, _ = _load_registry()
    return {e.get("cik"): e.get("thesis") for e in entries
            if e.get("role") == "manager_13f" and e.get("cik")}


def _filer_periods(con, cik):
    """Distinct reported periods for a filer, newest first."""
    return [r[0] for r in con.execute(
        "SELECT DISTINCT period FROM thirteenf_holdings "
        "WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND period IS NOT NULL "
        "ORDER BY period DESC", (cik,))]


def _period_holdings(con, cik, period):
    """{(cusip, put_call): holding-dict} for one filer/period. Carries the position's own
    reporting dates: `period` = the quarter-end the position is reported AS OF, and
    `filed_date` = when that filing reached EDGAR (they differ by up to 45 days, and an
    amendment can file a later date against the same period)."""
    out = {}
    for cusip, ticker, issuer, pc, val, sh, per, filed in con.execute(
        "SELECT cusip, ticker, issuer, put_call, value, shares, period, filed_date "
        "FROM thirteenf_holdings "
        "WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND period=?", (cik, period)):
        out[(cusip, pc)] = {"cusip": cusip, "ticker": ticker, "issuer": issuer,
                            "put_call": pc, "value": val or 0, "shares": sh or 0,
                            "period": per, "filed_date": filed}
    return out


def _filer_unit_scale(con, cik, periods):
    """13F `value` units are consistent WITHIN a filer but differ ACROSS filers — some
    file in thousands, some in whole dollars (verified: Duquesne thousands,
    Thiel/Situational/Affinity dollars). Detect the unit ONCE per filer, from the newest
    period that has price coverage (the latest period is reliably priced), and apply it
    to ALL the filer's periods — so an older period without a pre-period price row is
    never silently misread as dollars, and every period (incl. the QoQ prior) compares
    in one unit. Anchors implied price (value/shares) to the EOD close: dollars -> ~1,
    thousands -> ~0.001.

    SM-P2 G1 GATE — UNIT SCALE OR FAIL LOUD. Returns (scale, basis) where basis is
    "price_anchored" when a real price anchor decided it, or "undetermined" when NO period
    had price coverage to anchor on. The old behaviour silently returned 1 (dollars) in the
    undetermined case, which reads a thousands-filer 1000x TOO SMALL with no signal to the
    reader. The scale is still 1 so arithmetic proceeds, but callers MUST surface
    "undetermined" rather than present the numbers as trustworthy."""
    for period in periods:                          # newest first
        ratios = []
        for ticker, val, sh in con.execute(
                "SELECT ticker, value, shares FROM thirteenf_holdings "
                "WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND period=? AND "
                "put_call='long' AND ticker IS NOT NULL AND shares>0 AND value>0 "
                "ORDER BY value DESC LIMIT 20", (cik, period)):
            close, _ = _close_on(con, ticker.upper(), period)
            if close and close > 0:
                ratios.append((val / sh) / close)
        if ratios:
            ratios.sort()
            return (1000 if ratios[len(ratios) // 2] < 0.01 else 1), "price_anchored"
    return 1, "undetermined"


# A 13F filer manages at least $100M by law (17 CFR 240.13f-1), so a reported book far
# under that is evidence the unit scale is wrong (classic 1000x miss), not a small fund.
# Upper bound catches the opposite error. Both are REPORTED, never silently corrected.
_BOOK_FLOOR = 50_000_000
_BOOK_CEIL = 10_000_000_000_000


def _magnitude_warning(book_value, basis):
    """A human-readable warning when a filer's book is implausible for a 13F filer, or
    when the unit scale could not be anchored. None when the book looks sane."""
    if basis == "undetermined":
        return ("unit scale UNDETERMINED - no period had price coverage to anchor "
                "dollars-vs-thousands, so these values may be 1000x off")
    if book_value and book_value < _BOOK_FLOOR:
        return ("reported book {:,} is under the {:,} sanity floor for a 13F filer (the "
                "filing threshold is $100M) - either the unit scale is wrong or this "
                "filer's holdings are only partly ingested".format(
                    int(book_value), _BOOK_FLOOR))
    if book_value and book_value > _BOOK_CEIL:
        return "reported book {} is implausibly large - check the unit scale".format(
            book_value)
    return None


def _scaled_holdings(con, cik, period, scale):
    """Per-holding dict for a filer/period with `value` scaled to dollars by the
    filer-level `scale` (from _filer_unit_scale) — the SAME scale for every period."""
    h = _period_holdings(con, cik, period)
    if scale != 1:
        for x in h.values():
            x["value"] = int(round(x["value"] * scale))
    return h


def q_portfolio_deltas(con, cik, prior, scale=1):
    """Prior-period holdings {(cusip, put_call): holding} scaled to dollars by the
    filer-level `scale` — the QoQ baseline. Empty when no prior period."""
    return _scaled_holdings(con, cik, prior, scale) if prior else {}


def _next_quarter_end(period_iso):
    d = dt.date.fromisoformat(period_iso)
    for m, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
        qe = dt.date(d.year, m, day)
        if qe > d:
            return qe
    return dt.date(d.year + 1, 3, 31)


def _days_to_next_13f(latest_period, anchor):
    """Days until the next 13F is due — 45 days after the quarter end following the
    latest reported period. Negative = the window has passed (a staleness signal)."""
    try:
        due = _next_quarter_end(latest_period) + dt.timedelta(days=45)
        return (due - dt.date.fromisoformat(anchor)).days
    except (ValueError, TypeError):
        return None


def q_portfolio(con, filer_cik=None, period=None):
    """SM-P1 REPORTED PORTFOLIO for one tracked 13F filer. Per-holding rows from
    thirteenf_holdings for the chosen filer/period: ticker (unmapped CUSIPs kept as
    unmapped rows, counted, NEVER dropped), instrument SH/PUT/CALL, reported value,
    shares, pct of reported book, and a QoQ badge new/added/trimmed/exited vs the
    prior period. Long adds/trims are judged on SHARES (price-independent); option
    adds/trims on notional value. Direction-netted header: long value vs put-notional
    vs call-notional. put-heavy language, never 'short'.

    CAVEATS (standing, surfaced by the view): reported book only — long US-listed, no
    shorts/cash/privates; 45d stale; quarter-end marks; unmapped % shown; single-filing
    filers get no deltas. This reads thirteenf_holdings, which is populated by
    thirteenf_ingest — NOT the nightly Leg C (which only refreshes thirteenf_baseline),
    so freshness depends on that ingest running."""
    filers = _tracked_filers()
    valid = {str(cik_int(c)): (c, n) for c, n in filers}
    key = str(cik_int(filer_cik)) if filer_cik else None
    if key and key in valid:
        cik, name = valid[key]
    elif filers:
        cik, name = filers[0]
    else:
        return {"as_of": _as_of(), "filers": [], "period": None, "periods": [],
                "rows": [], "count": 0, "has_deltas": False}
    periods = _filer_periods(con, cik)
    base = {"as_of": _as_of(), "filer_cik": cik, "filer_name": name, "filers": filers,
            "periods": periods}
    if not periods:
        base.update({"period": None, "prior_period": None, "rows": [], "count": 0,
                     "has_deltas": False, "book_value": 0, "long_value": 0,
                     "put_notional": 0, "call_notional": 0, "unmapped_count": 0,
                     "unmapped_value": 0})
        return base
    period = period if period in periods else periods[0]
    idx = periods.index(period)
    prior = periods[idx + 1] if idx + 1 < len(periods) else None
    scale, unit_basis = _filer_unit_scale(con, cik, periods)  # one unit for the filer
    cur = _scaled_holdings(con, cik, period, scale)
    prior_h = q_portfolio_deltas(con, cik, prior, scale)
    book = sum(h["value"] for h in cur.values()) or 0

    def _row(h, badge, prior_val):
        # reported_period / filed_date are the position's OWN reporting dates. For an
        # "exited" row these are the PRIOR period's — that row's evidence is the last
        # filing the position appeared in, so its dates are the honest provenance.
        return {"cusip": h["cusip"], "ticker": h["ticker"],
                "unmapped": h["ticker"] is None, "issuer": h["issuer"],
                "instrument": _INSTRUMENT.get(h["put_call"], h["put_call"]),
                "value": h["value"], "shares": h["shares"],
                "pct_of_book": round(100.0 * h["value"] / book, 2) if book else None,
                "reported_period": h.get("period"), "filed_date": h.get("filed_date"),
                "badge": badge, "prior_value": prior_val}
    rows = []
    for k, h in cur.items():
        pv = prior_h.get(k)
        if not prior:
            badge = None
        elif pv is None:
            badge = "new"
        else:
            if h["put_call"] == "long":
                cm, pm = h["shares"], pv["shares"]
            else:
                cm, pm = h["value"], pv["value"]
            badge = "added" if cm > pm else "trimmed" if cm < pm else None
        rows.append(_row(h, badge, pv["value"] if pv else None))
    # Exited: held last period, gone this period -> synthetic zero-value rows.
    if prior:
        for k, h in prior_h.items():
            if k not in cur:
                rows.append(_row({**h, "value": 0, "shares": 0}, "exited", h["value"]))
    unmapped = [r for r in rows if r["unmapped"]]
    base.update({
        "period": period, "prior_period": prior, "rows": rows, "count": len(rows),
        "has_deltas": prior is not None, "book_value": book,
        "long_value": sum(h["value"] for h in cur.values() if h["put_call"] == "long"),
        "put_notional": sum(h["value"] for h in cur.values() if h["put_call"] == "put"),
        "call_notional": sum(h["value"] for h in cur.values() if h["put_call"] == "call"),
        "unmapped_count": len(unmapped),
        "unmapped_value": sum(r["value"] for r in unmapped),
        # SM-P2 G1: the unit basis and any magnitude implausibility travel with the
        # result so the view can refuse to present suspect numbers as trustworthy.
        "unit_scale": scale, "unit_basis": unit_basis,
        "magnitude_warning": _magnitude_warning(book, unit_basis)})
    return base


_DIR_ACC = "accumulating"
_DIR_DIS = "distributing"


def _manager_flow(con, cik, thesis, name):
    """{(ticker, instrument): direction} for one filer's NEWEST period vs its prior.
    Direction is QoQ FLOW, not position sign: new/added -> accumulating, trimmed/exited
    -> distributing. Longs are judged on SHARES (price-independent); options on notional
    value. Returns (period, flows) or (None, {}) when the filer has under two periods —
    a single-filing filer cannot express a direction and is never guessed at."""
    periods = _filer_periods(con, cik)
    if len(periods) < 2:
        return None, {}
    scale, _basis = _filer_unit_scale(con, cik, periods)
    cur = _scaled_holdings(con, cik, periods[0], scale)
    prior = _scaled_holdings(con, cik, periods[1], scale)
    flows = {}
    for k, h in cur.items():
        pv = prior.get(k)
        inst = "OP" if h["put_call"] != "long" else "SH"
        key = ((h["ticker"] or "").upper(), inst)
        if not key[0]:
            continue
        if pv is None:
            flows[key] = (_DIR_ACC, h["value"], "new")
        else:
            if h["put_call"] == "long":
                cm, pm = h["shares"], pv["shares"]
            else:
                cm, pm = h["value"], pv["value"]
            if cm > pm:
                flows[key] = (_DIR_ACC, h["value"], "added")
            elif cm < pm:
                flows[key] = (_DIR_DIS, h["value"], "trimmed")
    for k, h in prior.items():                      # exited entirely
        inst = "OP" if h["put_call"] != "long" else "SH"
        key = ((h["ticker"] or "").upper(), inst)
        if key[0] and key not in cur and key not in flows:
            flows[key] = (_DIR_DIS, h["value"], "exited")
    return periods[0], flows


def q_opposed_pairs(con, min_side=1):
    """ORDER SM-P2 FLAGSHIP — cross-manager DISAGREEMENTS. Same ticker, same instrument,
    opposite QoQ direction, in each manager's newest reported period: one tracked filer
    accumulating while another distributes.

    This is the point of expanding the shelf — with 6 filers a prior pass found 16
    convergences and ZERO disagreements, because disagreement needs combinatorial breadth.

    HONEST LIMITS, stated not buried: (1) periods are aligned by each filer's OWN newest
    filing, so a filer one quarter behind is compared slightly off-phase — the period is
    reported per side so that is visible; (2) 13F is long-only US-listed and 45 days
    stale, so an "exit" may be a sale that already reversed; (3) direction is QoQ flow,
    not conviction — trimming a large position is still holding it. No verdict is
    attached: this ranks BY DISAGREEMENT BREADTH, never by who is judged right."""
    thesis = _filer_thesis()
    flows = {}
    for cik, name in _tracked_filers():
        per, fl = _manager_flow(con, cik, thesis.get(cik), name)
        if fl:
            flows[(cik, name, per)] = fl
    agg = defaultdict(lambda: {"acc": [], "dis": []})
    for (cik, name, per), fl in flows.items():
        for key, (direction, value, kind) in fl.items():
            side = "acc" if direction == _DIR_ACC else "dis"
            agg[key][side].append({"filer": name, "cik": cik, "period": per,
                                   "thesis": thesis.get(cik), "value": value,
                                   "action": kind})
    rows = []
    for (ticker, instrument), a in agg.items():
        if len(a["acc"]) < min_side or len(a["dis"]) < min_side:
            continue                                 # not a disagreement
        for side in ("acc", "dis"):
            a[side].sort(key=lambda x: -(x["value"] or 0))
        rows.append({
            "ticker": ticker, "instrument": instrument,
            "n_accumulating": len(a["acc"]), "n_distributing": len(a["dis"]),
            "n_managers": len(a["acc"]) + len(a["dis"]),
            "accumulating": a["acc"], "distributing": a["dis"],
            "acc_value": sum(x["value"] or 0 for x in a["acc"]),
            "dis_value": sum(x["value"] or 0 for x in a["dis"]),
            "acc_names": ", ".join(x["filer"] for x in a["acc"]),
            "dis_names": ", ".join(x["filer"] for x in a["dis"]),
            # a disagreement ACROSS thesis groups is more interesting than one within
            "cross_thesis": len({x["thesis"] for x in a["acc"]}
                                & {x["thesis"] for x in a["dis"]}) == 0})
    rows.sort(key=lambda r: (-min(r["n_accumulating"], r["n_distributing"]),
                             -r["n_managers"], -(r["acc_value"] + r["dis_value"])))
    return {"as_of": _as_of(), "count": len(rows), "rows": rows,
            "filers_compared": len(flows),
            "note": "QoQ flow direction per filer's newest period; 13F is long-only US "
                    "listed and 45d stale; ranked by disagreement breadth, no verdict"}


def q_tracked_books(con, anchor=None):
    """SM-P1 front-page strip: per tracked filer, the latest reported period + book
    value + top-3 long weights + days until the next 13F filing window."""
    anchor = anchor or dt.date.today().isoformat()
    out = []
    for cik, name in _tracked_filers():
        periods = _filer_periods(con, cik)
        if not periods:
            out.append({"cik": cik, "name": name, "period": None, "book_value": 0,
                        "top3": [], "days_to_filing": None})
            continue
        cur = _scaled_holdings(con, cik, periods[0],
                               _filer_unit_scale(con, cik, periods)[0])
        book = sum(h["value"] for h in cur.values()) or 0
        longs = sorted((h for h in cur.values() if h["put_call"] == "long"),
                       key=lambda h: -(h["value"] or 0))
        top3 = [{"ticker": h["ticker"] or ("cusip:" + h["cusip"]),
                 "pct": round(100.0 * (h["value"] or 0) / book, 1) if book else None}
                for h in longs[:3]]
        out.append({"cik": cik, "name": name, "period": periods[0], "book_value": book,
                    "top3": top3, "days_to_filing": _days_to_next_13f(periods[0], anchor)})
    return {"as_of": _as_of(), "filers": out}


# ---------------------------------------------------------------- q_congress_breadth (SM-C1)
_OWNER_LABEL = {"SP": "spouse", "DC": "dependent", "JT": "joint", "Self": "self"}


def _member_latest_years(con):
    """{(chamber, last, first, state_dist): newest_filing_year} — each member's most recent
    annual FD, so breadth counts one snapshot per member. Chamber is part of the key so a
    House and a Senate member who share a name never collapse into one (Senate state_dist
    is NULL, so it cannot disambiguate across chambers on its own)."""
    return {(c, l, f, s): y for c, l, f, s, y in con.execute(
        "SELECT chamber, member_last, member_first, state_dist, MAX(filing_year) "
        "FROM congress_holdings GROUP BY chamber, member_last, member_first, state_dist")}


_PARTY_BUCKET = {"Democrat": "dem", "Republican": "rep", "Independent": "ind"}


def _member_parties(con):
    """{(chamber, last, first, state_dist): 'dem'|'rep'|'ind'} for identities the roster
    resolved DETERMINISTICALLY. Absent = party unknown (never guessed) — the caller counts
    those as 'unknown' so the split always reconciles to the holder count."""
    try:
        rows = con.execute("SELECT chamber, member_last, member_first, state_dist, party "
                           "FROM congress_member_roster WHERE party IS NOT NULL")
    except Exception:                     # roster not synced yet — degrade to all-unknown
        return {}
    return {(c, l, f, s): _PARTY_BUCKET.get(p, "ind") for c, l, f, s, p in rows}


def _confirmed_members(con):
    """{(chamber,last,first,state_dist)} that the roster confirmed as an ACTUAL member.
    Candidates who filed a disclosure but never served resolve to nothing, so this set is
    how they are excluded — a candidate is not an insider. Empty (=> no filtering) when
    the roster has never been synced, so the surface never silently empties."""
    try:
        return {(c, l, f, s) for c, l, f, s in con.execute(
            "SELECT chamber, member_last, member_first, state_dist "
            "FROM congress_member_roster WHERE party IS NOT NULL")}
    except Exception:
        return set()


def q_member_book(con, member_key=None, year=None):
    """The REPORTED BOOK of one political insider — same shape as the 13F portfolio view,
    for a member of Congress. Per-holding rows for the chosen member/filing year, with the
    band midpoint as a coarse value and each position's OWNER.

    OWNER IS FIRST-CLASS, NOT A FOOTNOTE. Several members do little or no trading in their
    own name — the positions sit with a spouse (the Pelosi pattern: the trades were her
    husband's). A book that is mostly spouse-owned is the same disclosure surface, so the
    header reports the owner split and the spouse share outright rather than burying it.

    Band-valued: every figure is a COARSE band midpoint, never a mark. Members are the
    roster-CONFIRMED ones only, so candidates who filed but never served are excluded."""
    confirmed = _confirmed_members(con)
    roster = {}
    try:
        for c, l, f, s, p, st in con.execute(
                "SELECT chamber, member_last, member_first, state_dist, party, state "
                "FROM congress_member_roster WHERE party IS NOT NULL"):
            roster[(c, l, f, s)] = {"party": p, "state": st}
    except Exception:
        pass
    # candidate member list = confirmed identities that actually have holdings
    members = []
    for c, l, f, s, n in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, count(*) "
            "FROM congress_holdings GROUP BY chamber, member_last, member_first, state_dist"):
        k = (c, l, f, s)
        if confirmed and k not in confirmed:
            continue
        members.append({"key": "{}|{}|{}|{}".format(c, l or "", f or "", s or ""),
                        "label": "{}, {} ({})".format(l or "?", f or "", c[:3].upper()),
                        "rows": n})
    members.sort(key=lambda m: m["label"])
    base = {"as_of": _as_of(), "members": members}
    if not members:
        base.update({"member": None, "rows": [], "count": 0, "years": [], "year": None,
                     "book_value": 0, "owner_split": {}, "spouse_share": None,
                     "party": None, "state": None, "chamber": None})
        return base
    key = member_key if any(m["key"] == member_key for m in members) else members[0]["key"]
    cham, last, first, sd = (key.split("|") + ["", "", ""])[:4]
    last, first, sd = last or None, first or None, (sd or None)
    years = [y for (y,) in con.execute(
        "SELECT DISTINCT filing_year FROM congress_holdings WHERE chamber=? AND "
        "member_last IS ? AND member_first IS ? AND state_dist IS ? "
        "ORDER BY filing_year DESC", (cham, last, first, sd)) if y is not None]
    year = year if year in years else (years[0] if years else None)
    rows = []
    for name, ticker, atype, owner, lo, hi, inc in con.execute(
            "SELECT asset_name, ticker, asset_type, owner, value_lo, value_hi, income_type "
            "FROM congress_holdings WHERE chamber=? AND member_last IS ? AND "
            "member_first IS ? AND state_dist IS ? AND filing_year IS ?",
            (cham, last, first, sd, year)):
        mid = ((lo + hi) / 2 if (lo is not None and hi is not None) else (lo or 0))
        rows.append({"asset_name": name, "ticker": ticker,
                     "instrument": "OP" if atype == "OP" else "SH",
                     "asset_type": atype, "owner": _OWNER_LABEL.get(owner, "self"),
                     "value_lo": lo, "value_hi": hi, "midpoint": round(mid),
                     "income_type": inc})
    book = sum(r["midpoint"] for r in rows) or 0
    for r in rows:
        r["pct_of_book"] = round(100.0 * r["midpoint"] / book, 2) if book else None
    rows.sort(key=lambda r: -r["midpoint"])
    split = defaultdict(float)
    for r in rows:
        split[r["owner"]] += r["midpoint"]
    not_self = sum(v for k, v in split.items() if k != "self")
    meta = roster.get((cham, last, first, sd)) or {}
    base.update({
        "member": "{}, {}".format(last or "?", first or ""), "member_key": key,
        "chamber": cham, "party": meta.get("party"), "state": meta.get("state") or sd,
        "years": years, "year": year, "rows": rows, "count": len(rows),
        "book_value": round(book),
        "owner_split": {k: round(v) for k, v in split.items()},
        "proxy_share": round(100.0 * not_self / book, 1) if book else None,
        "spouse_share": round(100.0 * split.get("spouse", 0) / book, 1) if book else None,
        "note": "band-midpoint proxy, never a mark; owner shows whose position it is"})
    return base


def q_oge_holdings(con, filer=None):
    """OGE Form 278e disclosure rows for an executive-branch filer.

    RESTRICTED SOURCE. Every row carries its own `use_restriction` straight out of the DB
    (the column is NOT NULL), so the restriction travels with each row into every view and
    export rather than living only in a page banner. See
    recon/OGE_278E_SOURCE_VERDICT.md — Ethics in Government Act 5 U.S.C. app. Sec 105(c)
    forbids commercial use. This table is intentionally NOT read by the scan/alert path."""
    try:
        filers = [r[0] for r in con.execute(
            "SELECT DISTINCT filer FROM oge_holdings ORDER BY filer")]
    except Exception:
        return {"as_of": _as_of(), "filers": [], "filer": None, "rows": [], "count": 0,
                "restriction": None, "banded": 0}
    if not filers:
        return {"as_of": _as_of(), "filers": [], "filer": None, "rows": [], "count": 0,
                "restriction": None, "banded": 0}
    who = filer if filer in filers else filers[0]
    rows = []
    for ln, desc, tk, eif, lo, hi, itype, restriction, rtype, fdate in con.execute(
            "SELECT line_no, description, ticker, eif, value_lo, value_hi, income_type, "
            "use_restriction, report_type, filed_date FROM oge_holdings WHERE filer=?",
            (who,)):
        mid = ((lo + hi) / 2 if (lo is not None and hi is not None) else lo)
        # "FILER - x" / "SPOUSE - x" is the report's own owner marking; surface it as
        # owner the way congressional FD owner codes are surfaced.
        owner = ("spouse" if desc.upper().startswith("SPOUSE")
                 else "filer" if desc.upper().startswith("FILER") else "-")
        rows.append({"line_no": ln, "description": desc, "ticker": tk, "eif": eif,
                     "owner": owner, "value_lo": lo, "value_hi": hi,
                     "midpoint": round(mid) if mid is not None else None,
                     "income_type": itype, "use_restriction": restriction,
                     "report_type": rtype, "filed_date": fdate})
    rows.sort(key=lambda r: (-(r["midpoint"] or 0), r["line_no"]))
    banded = [r for r in rows if r["value_lo"] is not None]
    return {"as_of": _as_of(), "filers": filers, "filer": who, "rows": rows,
            "count": len(rows), "banded": len(banded),
            "restriction": rows[0]["use_restriction"] if rows else None,
            "note": "band-valued, coarse; restricted source, non-commercial use only"}


_TIER_ANCHORED = "anchored"
_TIER_ANCHORED_FLOWS = "anchored+flows"
_TIER_FLOWS_ONLY = "flows-only"
_TIER_FLOWS_OVER = "flows>anchor"
# A person row whose name is a leaked eFD JSON blob (24 exist, one 26KB). Excluded from
# identity matching — it can never match and would only slow the index.
_MAX_PERSON_NAME = 60


def _person_index(con):
    """{(canon_last, first_token): [(person_id, name)]} over PTR persons."""
    from . import names as _names
    idx = defaultdict(list)
    try:
        rows = con.execute(
            "SELECT person_id, name FROM persons WHERE type='congress' "
            "AND length(name) <= ?", (_MAX_PERSON_NAME,))
    except Exception:
        return {}
    for pid, nm in rows:
        parts = [p.strip() for p in (nm or "").split(",")]
        if len(parts) < 2:
            continue
        last = _names.canonical_key(parts[0])
        first = _names.canonical_key(parts[1]).split()
        idx[(last, first[0] if first else "")].append((pid, nm))
    return idx


def resolve_person(idx, last, first):
    """(person_id, matched_name) for a holdings identity, or (None, None).

    DETERMINISTIC ONLY, same doctrine as the party roster: an exact canonical
    (last, first-token) match, then a PREFIX tie-break for the nickname case the corpus
    actually contains (holdings 'Tillis, Thomas R' vs PTR 'Tillis, Thom'). A member with
    no match is NOT an error — Hyde-Smith, Slotkin and Hirono hold annual-FD positions and
    file no PTRs we hold, which is exactly the [anchored] tier."""
    from . import names as _names
    l = _names.canonical_key(last or "")
    f = _names.canonical_key(first or "").split()
    ft = f[0] if f else ""
    hit = idx.get((l, ft), [])
    if len(hit) == 1:
        return hit[0]
    if not hit:
        for (kl, kf), v in idx.items():
            if kl != l or not kf or not ft or len(v) != 1:
                continue
            if kf.startswith(ft) or ft.startswith(kf):
                if min(len(kf), len(ft)) >= 3:      # Thom/Thomas, never T/Theodore
                    return v[0]
    return (None, None)


def _mid(lo, hi):
    """Band midpoint. An OPEN top band has no midpoint that means anything, so it
    contributes its FLOOR — never an invented ceiling (corpus-wide convention)."""
    if lo is None and hi is None:
        return 0
    if hi is None:
        return lo or 0
    if lo is None:
        return hi
    return (lo + hi) / 2.0


def q_member_fusion(con, member_key=None):
    """SM-C3 Phase F — one member's book, anchor fused with subsequent PTR flows.

    KEY IS (ticker, owner). Owner is NEVER merged: a member's own position and their
    spouse's are different disclosures and combining them would invent a holding neither
    reported.

    anchor  = the member's most recent annual FD position (band + coverage_year)
    flows   = PTR transactions dated AFTER the anchor's coverage year ended
    estimate= anchor band +/- flow midpoints, rendered as a RANGE, never a point

    CONFIDENCE TIERS (per row, always shown):
      anchored       anchor, no flows since — as-reported and STALE, not current
      anchored+flows anchor plus later PTRs — the estimate rows
      flows-only     PTRs with no anchor — new since the annual
      flows>anchor   sells exceed what the anchor could hold. FLAGGED, never rendered
                     as negative dollars. Causes include band coarseness, an owner
                     mismatch between the two sources, or a paper/unparsed anchor year.
                     Counted and visible, deliberately UNINTERPRETED.

    NEVER infers full-vs-partial: a sale is only what the PTR states. A ticker-less
    holding is UNFUSABLE (it can never join a flow) and is marked, so "no flows matched"
    can never be read as "no flows occurred" — Mando's Phase H display ruling."""
    confirmed = _confirmed_members(con)
    members = []
    for c, l, f, s, n in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, count(*) "
            "FROM congress_holdings GROUP BY chamber, member_last, member_first, "
            "state_dist"):
        k = (c, l, f, s)
        if confirmed and k not in confirmed:
            continue
        members.append({"key": "{}|{}|{}|{}".format(c, l or "", f or "", s or ""),
                        "label": "{}, {} ({})".format(l or "?", f or "", c[:3].upper()),
                        "rows": n})
    members.sort(key=lambda m: m["label"])
    base = {"as_of": _as_of(), "members": members}
    if not members:
        base.update({"member": None, "rows": [], "count": 0, "tiers": {},
                     "anchor_year": None, "anchor_filed": None, "unfusable": 0})
        return base
    key = member_key if any(m["key"] == member_key for m in members) else members[0]["key"]
    cham, last, first, sd = (key.split("|") + ["", "", ""])[:4]
    last, first, sd = last or None, first or None, (sd or None)

    yrs = [y for (y,) in con.execute(
        "SELECT DISTINCT coverage_year FROM congress_holdings WHERE chamber=? AND "
        "member_last IS ? AND member_first IS ? AND state_dist IS ? AND "
        "coverage_year IS NOT NULL ORDER BY coverage_year DESC",
        (cham, last, first, sd))]
    anchor_year = yrs[0] if yrs else None
    anchor_filed = None
    if anchor_year is not None:
        r = con.execute(
            "SELECT max(filing_date) FROM congress_holdings WHERE chamber=? AND "
            "member_last IS ? AND member_first IS ? AND state_dist IS ? AND "
            "coverage_year=?", (cham, last, first, sd, anchor_year)).fetchone()
        anchor_filed = r[0] if r else None
    # Flows count only AFTER the anchor's coverage period ends. An annual covering CY2025
    # already reflects everything through 2025-12-31, so double-counting a trade inside
    # that window would inflate the estimate.
    flow_after = "{}-12-31".format(anchor_year) if anchor_year else "0000-00-00"

    anchors = {}
    unfusable = 0
    for name, tk, atype, owner, lo, hi in con.execute(
            "SELECT asset_name, ticker, asset_type, owner, value_lo, value_hi "
            "FROM congress_holdings WHERE chamber=? AND member_last IS ? AND "
            "member_first IS ? AND state_dist IS ? AND coverage_year IS ?",
            (cham, last, first, sd, anchor_year)):
        ol = _OWNER_LABEL.get(owner, "self")
        if not tk:
            unfusable += 1
            anchors[("\x00" + (name or "?"), ol)] = {
                "asset_name": name, "ticker": None, "owner": ol, "lo": lo, "hi": hi,
                "unfusable": True, "instrument": "OP" if atype == "OP" else "SH"}
            continue
        k = (tk.upper(), ol)
        a = anchors.setdefault(k, {"asset_name": name, "ticker": tk.upper(), "owner": ol,
                                   "lo": None, "hi": None, "valued": False,
                                   "unfusable": False,
                                   "instrument": "OP" if atype == "OP" else "SH"})
        # An asset disclosed with NO value band must stay (None, None). Seeding the
        # accumulator at 0 turned "reported, unvalued" into "at least $0, open-ended" —
        # a floor the filer never stated — and then made any later sale look like it
        # exceeded a known holding.
        if lo is None and hi is None:
            continue
        a["valued"] = True
        a["lo"] = (a["lo"] or 0) + (lo or 0)
        a["hi"] = None if hi is None else ((a["hi"] or 0) + hi)

    pid, matched_name = resolve_person(_person_index(con), last, first)
    flows = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "n_buy": 0, "n_sell": 0,
                                 "last_tx": None})
    if pid:
        for tk, side, alo, ahi, tx, owner in con.execute(
                "SELECT ticker, side, amt_low, amt_high, tx_date, owner "
                "FROM congress_trades WHERE person_id=? AND superseded=0 AND "
                "ticker IS NOT NULL AND tx_date > ? AND tx_date <= ?",
                (pid, flow_after, _as_of()[:10])):
            k = (tk.upper(), _OWNER_LABEL.get(owner, "self"))
            fl = flows[k]
            m = _mid(alo, ahi)
            if str(side).startswith("purchase"):
                fl["buy"] += m
                fl["n_buy"] += 1
            else:
                fl["sell"] += m
                fl["n_sell"] += 1
            if fl["last_tx"] is None or (tx or "") > fl["last_tx"]:
                fl["last_tx"] = tx

    rows = []
    tiers = defaultdict(int)
    for k in set(anchors) | set(flows):
        a = anchors.get(k)
        fl = flows.get(k)
        tk, ol = (a["ticker"], a["owner"]) if a else k
        if a and a["unfusable"]:
            tier = _TIER_ANCHORED
        elif a and fl:
            tier = _TIER_ANCHORED_FLOWS
        elif a:
            tier = _TIER_ANCHORED
        else:
            tier = _TIER_FLOWS_ONLY
        alo = a["lo"] if a else None
        ahi = a["hi"] if a else None
        buy = fl["buy"] if fl else 0.0
        sell = fl["sell"] if fl else 0.0
        est_lo = est_hi = None
        # An anchor with no reported band cannot be combined with flows arithmetically.
        # It is still ANCHORED (the asset was disclosed) but carries no estimate, and a
        # later sale must NOT be read as exceeding it.
        if a and not a["unfusable"] and not a.get("valued"):
            pass
        elif a and not a["unfusable"]:
            est_lo = (alo or 0) + buy - sell
            est_hi = None if ahi is None else ahi + buy - sell
            # Sells exceeding what the anchor band could hold. Flag it; NEVER show a
            # negative dollar figure, and never silently clamp without saying so.
            if (est_hi is not None and est_hi < 0) or (ahi is None and est_lo < 0):
                tier = _TIER_FLOWS_OVER
            est_lo = max(0.0, est_lo)
            if est_hi is not None:
                est_hi = max(0.0, est_hi)
        elif fl:
            est_lo, est_hi = max(0.0, buy - sell), None
            if buy - sell < 0:
                tier = _TIER_FLOWS_OVER
        tiers[tier] += 1
        rows.append({
            "ticker": tk if not (a and a["unfusable"]) else None,
            "asset_name": (a or {}).get("asset_name"),
            "owner": ol, "instrument": (a or {}).get("instrument", "SH"),
            "anchor_lo": alo, "anchor_hi": ahi,
            "buy_flow": round(buy), "sell_flow": round(sell),
            "n_buy": (fl or {}).get("n_buy", 0), "n_sell": (fl or {}).get("n_sell", 0),
            "last_tx": (fl or {}).get("last_tx"),
            "estimate_lo": None if est_lo is None else round(est_lo),
            "estimate_hi": None if est_hi is None else round(est_hi),
            "tier": tier, "unfusable": bool(a and a["unfusable"])})
    rows.sort(key=lambda r: (-(r["estimate_lo"] or r["anchor_lo"] or 0), r["ticker"] or ""))
    meta = {}
    try:
        for p, st in con.execute(
                "SELECT party, state FROM congress_member_roster WHERE chamber=? AND "
                "member_last IS ? AND member_first IS ? AND state_dist IS ?",
                (cham, last, first, sd)):
            meta = {"party": p, "state": st}
    except Exception:
        pass
    base.update({
        "member": "{}, {}".format(last or "?", first or ""), "member_key": key,
        "chamber": cham, "party": meta.get("party"), "state": meta.get("state") or sd,
        "anchor_year": anchor_year, "anchor_filed": anchor_filed,
        "coverage_years": yrs, "ptr_person": matched_name, "ptr_linked": bool(pid),
        "rows": rows, "count": len(rows), "tiers": dict(tiers), "unfusable": unfusable,
        "note": "estimates are RANGES from coarse bands, never marks; flows counted only "
                "after the anchor coverage year ended; full-vs-partial sale is never "
                "inferred"})
    return base


def q_coverage_matrix(con):
    """SM-C3 Phase H: per-member, per-coverage-year parse state across the harvest.

    This is the DATA_QUALITY deliverable and the thing that makes every downstream number
    a FLOOR rather than a total. A member-year is one of:
      parsed   — an annual was ingested and yielded rows
      empty    — an annual was ingested and yielded NO rows (a real reported state)
      missing  — no annual for that member-year in the corpus at all
    'missing' is not necessarily a parse failure: the member may not have served that
    year, or the filing may be paper/unparsed/never-fetched. The distinction is NOT
    guessed here — congress_fd_seen carries the per-document status, so the failure modes
    are reported separately rather than folded into one bucket."""
    years = sorted({y for (y,) in con.execute(
        "SELECT DISTINCT coverage_year FROM congress_holdings "
        "WHERE coverage_year IS NOT NULL")})
    rows_by = defaultdict(dict)
    members = {}
    for c, l, f, s, y, n in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "count(*) FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, member_last, member_first, state_dist, coverage_year"):
        k = (c, l, f, s)
        members[k] = True
        rows_by[k][y] = n
    # document-level status tallies, so paper/unparsed are visible as themselves
    status = defaultdict(int)
    try:
        for st, n in con.execute(
                "SELECT status, count(*) FROM congress_fd_seen GROUP BY status"):
            status[st] = n
    except Exception:
        pass
    out = []
    for k in sorted(members, key=lambda x: (x[0], x[1] or "", x[2] or "")):
        c, l, f, s = k
        per = rows_by[k]
        cells = []
        for y in years:
            n = per.get(y)
            cells.append({"year": y, "rows": n,
                          "state": "parsed" if n else ("empty" if n == 0 else "missing")})
        out.append({"member": "{}, {}".format(l or "?", f or ""), "chamber": c,
                    "state_dist": s, "years_present": len(per),
                    "cells": cells, "total_rows": sum(per.values())})
    covered = sum(r["years_present"] for r in out)
    possible = len(out) * len(years) if years else 0
    return {"as_of": _as_of(), "years": years, "rows": out, "members": len(out),
            "cells_covered": covered, "cells_possible": possible,
            "coverage_pct": round(100.0 * covered / possible, 1) if possible else 0.0,
            "doc_status": dict(status),
            "note": "a missing member-year may mean the member did not serve that year, "
                    "not that a filing was lost; document-level paper/unparsed counts are "
                    "reported separately and never folded in"}


def q_congress_gaps(con):
    """SM-C2 P3: who is BEHIND the breadth counts. Per filer identity: chamber, party,
    how it resolved, filing years present, and holdings rows. The point is that breadth
    counts are FLOORS — a member missing from the corpus (paper-only filing, a failed
    fetch, unparsed layout) depresses every ticker they hold, and an 'unmatched' identity
    is usually a CANDIDATE who filed but never served, not a data error."""
    parties = {}
    kinds = {}
    try:
        for c, l, f, s, p, k in con.execute(
                "SELECT chamber, member_last, member_first, state_dist, party, match_kind "
                "FROM congress_member_roster"):
            parties[(c, l, f, s)] = p
            kinds[(c, l, f, s)] = k
    except Exception:
        pass
    rows = []
    for c, l, f, s, n, yrs in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, count(*), "
            "group_concat(DISTINCT filing_year) FROM congress_holdings "
            "GROUP BY chamber, member_last, member_first, state_dist"):
        k = (c, l, f, s)
        years = sorted(y for y in (yrs or "").split(",") if y)
        rows.append({"member": "{}, {}".format(l or "?", f or ""), "chamber": c,
                     "state": s or None, "party": parties.get(k),
                     "match_kind": kinds.get(k, "unsynced"), "years": ",".join(years),
                     "year_count": len(years), "rows": n})
    rows.sort(key=lambda r: -r["rows"])
    resolved = sum(1 for r in rows if r["party"])
    return {"as_of": _as_of(), "count": len(rows), "rows": rows,
            "resolved": resolved, "unresolved": len(rows) - resolved,
            "note": "breadth counts are FLOORS; unmatched identities are dominated by "
                    "candidates who filed an FD but never served"}


def q_congress_breadth(con, min_holders=1, owner_filter="all", members_only=True):
    """SM-C1/C2 flagship: one row per (ticker, instrument) — how many DISTINCT members hold
    it in their LATEST annual FD, with the chamber split (house/senate), the owner split
    (self/spouse/dependent/joint), a ROUGH summed band-midpoint exposure, YoY holder-count
    change, and first-seen year.
    Option rows are kept DISTINCT from stock rows for the same ticker (a member's GOOGL
    stock and GOOGL option are different positions). DISTRIBUTION-FIRST: mega-caps top
    raw breadth mechanically, so the surface reports the holder-count distribution and
    never labels 'notable'; the signal is SMID names with outsized breadth and YoY
    change. Band-valued -> exposure is a coarse proxy. Non-security assets (ticker NULL)
    are not part of ticker breadth."""
    latest = _member_latest_years(con)
    parties = _member_parties(con)
    # A candidate who filed a disclosure but never served is not an insider — excluded by
    # default. Falls back to no filtering when the roster has never been synced.
    confirmed = _confirmed_members(con) if members_only else set()
    agg = defaultdict(lambda: {"members": set(), "owners": defaultdict(int),
                               "chambers": defaultdict(set), "parties": defaultdict(set),
                               "mid": 0.0, "first_year": None,
                               "by_year": defaultdict(set)})
    for c, l, f, s, yr, ticker, atype, owner, vlo, vhi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, filing_year, ticker, "
            "asset_type, owner, value_lo, value_hi FROM congress_holdings "
            "WHERE ticker IS NOT NULL AND ticker!=''"):
        olabel = _OWNER_LABEL.get(owner, "self")
        if owner_filter != "all" and olabel != owner_filter:   # owner is a first-class filter
            continue
        key = (ticker.upper(), "OP" if atype == "OP" else "SH")
        mkey = (c, l, f, s)
        if confirmed and mkey not in confirmed:                # candidates are not insiders
            continue
        a = agg[key]
        a["by_year"][yr].add(mkey)
        a["first_year"] = yr if a["first_year"] is None else min(a["first_year"], yr)
        if latest.get(mkey) == yr:          # count breadth from each member's latest filing only
            a["members"].add(mkey)
            a["owners"][olabel] += 1
            a["chambers"][c].add(mkey)
            a["parties"][parties.get(mkey, "unknown")].add(mkey)
            a["mid"] += ((vlo + vhi) / 2 if (vlo is not None and vhi is not None)
                         else (vlo or 0))
    out = []
    for (ticker, instrument), a in agg.items():
        hc = len(a["members"])
        if hc < min_holders:
            continue
        yrs = sorted(a["by_year"])
        cur = len(a["by_year"][yrs[-1]]) if yrs else 0
        prev = len(a["by_year"][yrs[-2]]) if len(yrs) >= 2 else None
        out.append({"ticker": ticker, "instrument": instrument, "holder_count": hc,
                    "house": len(a["chambers"]["house"]),
                    "senate": len(a["chambers"]["senate"]),
                    "dem": len(a["parties"]["dem"]), "rep": len(a["parties"]["rep"]),
                    "ind": len(a["parties"]["ind"]),
                    "party_unknown": len(a["parties"]["unknown"]),
                    "self": a["owners"]["self"], "spouse": a["owners"]["spouse"],
                    "dependent": a["owners"]["dependent"], "joint": a["owners"]["joint"],
                    "midpoint_exposure": round(a["mid"]), "first_year": a["first_year"],
                    "yoy_change": (cur - prev) if prev is not None else None})
    out.sort(key=lambda r: (-r["holder_count"], -r["midpoint_exposure"]))
    return {"as_of": _as_of(), "count": len(out), "rows": out,
            "note": "band-midpoint exposure is a COARSE proxy; distribution-first "
                    "(mega-caps top raw breadth mechanically)"}


_YOY_BAR = 95.0          # the Phase H ticker-capture bar, reused as the confidence bar


def _capture_cells(con):
    """{(chamber, coverage_year): {"ticker_pct", "members", "rows"}} — how well a
    chamber-year was actually captured. Phase Y badges are only as trustworthy as the
    year they are measured against, so this travels WITH the deltas."""
    cells = {}
    for cham, yr, den, hit, mem, rows in con.execute(
            "SELECT chamber, coverage_year, "
            "SUM(CASE WHEN asset_type IN ('ST','OP','EF') THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN asset_type IN ('ST','OP','EF') AND ticker IS NOT NULL "
            "         AND ticker!='' THEN 1 ELSE 0 END), "
            "COUNT(DISTINCT member_last||'|'||member_first||'|'||"
            "               COALESCE(state_dist,'-')), COUNT(*) "
            "FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, coverage_year"):
        cells[(cham, yr)] = {
            "ticker_pct": (round(100.0 * hit / den, 1) if den else None),
            "members": mem, "rows": rows}
    return cells


def _yoy_years(con, year=None, prior=None):
    """The two coverage years to compare. Defaults to the newest year that looks like a
    FILED cycle rather than a trickle, and the year before it — CY2026 annuals are not due
    until May 2027, so the handful of early/amended CY2026 rows must not be mistaken for
    a year. `year`/`prior` override explicitly and are never second-guessed."""
    counts = {}
    for y, m in con.execute(
            "SELECT coverage_year, COUNT(DISTINCT chamber||member_last||'|'||"
            "member_first||'|'||COALESCE(state_dist,'-')) FROM congress_holdings "
            "WHERE coverage_year IS NOT NULL GROUP BY coverage_year"):
        counts[y] = m
    if not counts:
        return None, None, counts
    if year is None:
        best = max(counts.values())
        # A year with under a fifth of the best-attested year's filers is a trickle.
        seated = [y for y, m in counts.items() if m >= best * 0.2]
        year = max(seated) if seated else max(counts)
    if prior is None:
        earlier = [y for y in counts if y < year]
        prior = max(earlier) if earlier else None
    return year, prior, counts


def _floor_mid(lo, hi):
    """Band contribution. An unbounded top band ('over $50,000,000') has no midpoint, so
    it contributes its FLOOR — inventing a ceiling would put a number in the filer's
    mouth. Same convention as the Phase F anchor."""
    if lo is None and hi is None:
        return 0.0
    if hi is None:
        return float(lo or 0)
    if lo is None:
        return float(hi)
    return (lo + hi) / 2.0


_TWIN_MAX = 3          # a company key shared by more symbols than this is boilerplate
_CO_NOISE = re.compile(
    r"\b(inc|incorporated|corp|corporation|co|company|plc|ltd|limited|holdings?|"
    r"class\s+[a-c]|common|stock|shares?|ordinary|the)\b\.?", re.I)


_SYM_PREFIX = re.compile(r"^\s*[A-Z]{1,5}(?:\.[A-Z]{1,2})?\s*-\s*")


def _co_key(asset_name):
    """A company name reduced to a comparison key.

    Three things have to come off before the name compares, each learned from a real row:
      * the LEADING SYMBOL PREFIX. eFD writes "FISV - Fiserv, Inc. - Common Stock" and
        "FI - Fiserv Inc"; keeping it keys them 'fisvfiserv' vs 'fifiserv' and the two
        notations for one company never meet - which is the whole thing this detects.
      * the ACCOUNT SUFFIX. The House concatenates the custodian after a double space
        ("... ETF (VEA)  Joint Brokerage Account"), so the key would carry the brokerage.
      * legal-form and share-class boilerplate, and the parenthetical symbol.
    """
    s = (asset_name or "").split("  ")[0]            # account suffix off first
    s = _SYM_PREFIX.sub("", s)
    s = re.sub(r"\([^)]*\)", " ", s)                 # drop the parenthetical symbol
    s = _CO_NOISE.sub(" ", s)
    return re.sub(r"[^a-z0-9]+", "", s.lower())[:40]


def _symbol_twins(con, year, prior):
    """{ticker: [other tickers filed under the SAME company name in the compared years]}.

    An identity discontinuity, stated as a fact and nothing more. Fiserv is filed as both
    FISV and FI in the same corpus: 'FI - Fiserv, Inc. Common Stock' (CY2023/24) and
    'FISV - Fiserv, Inc. - Common Stock' (CY2025/26), with the House running the opposite
    way. FISV's +2 falls to +1 when the two are unioned, so half of that delta is a
    notation change rather than members buying.

    This does NOT union anything and does NOT re-rank. It is the same design as
    `new_to_corpus`: a flag that states what the corpus shows without claiming a cause —
    a rename, a dual listing, and a filer typo would all raise it. Hand-editing the cut,
    or quietly merging on this signal, would be the first silent judgment in the pipeline
    and is not where that starts."""
    by_name = defaultdict(set)
    for tk, name in con.execute(
            "SELECT UPPER(ticker), asset_name FROM congress_holdings "
            "WHERE ticker IS NOT NULL AND ticker!='' AND coverage_year IN (?,?)",
            (year, prior)):
        key = _co_key(name)
        if len(key) >= 4:                            # a 3-char key collides on noise
            by_name[key].add(tk)
    out = defaultdict(set)
    for tks in by_name.values():
        # A key shared by many symbols is a GENERIC STRING, not one company - a truncated
        # or boilerplate asset name, not evidence of an identity. Two is the shape a
        # notation change actually makes (FISV/FI); anything wider is noise and is dropped
        # rather than allowed to fuse unrelated issuers.
        if not 2 <= len(tks) <= _TWIN_MAX:
            continue
        for t in tks:
            out[t] |= (tks - {t})
    return {t: sorted(v) for t, v in out.items()}


def _bucket(d):
    for lo, hi, label in ((None, -10, "<= -10"), (-9, -5, "-9..-5"), (-4, -3, "-4..-3"),
                          (-2, -1, "-2..-1"), (0, 0, "0"), (1, 2, "+1..+2"),
                          (3, 4, "+3..+4"), (5, 9, "+5..+9")):
        if (lo is None or d >= lo) and d <= hi:
            return label
    return ">= +10"


_BUCKETS = ["<= -10", "-9..-5", "-4..-3", "-2..-1", "0", "+1..+2", "+3..+4", "+5..+9",
            ">= +10"]


def q_congress_breadth_yoy(con, year=None, prior=None, owner_filter="all",
                           members_only=True):
    """SM-C3 Phase Y: year-over-year breadth deltas per (ticker, instrument).

    THE WHOLE DIFFICULTY IS THE DENOMINATOR. Members do not all file every year — CY2025
    has 265 House filers against CY2024's 385, because extensions push filings months
    past the May due date. Differencing raw holder counts would therefore report a mass
    EXIT that is a filing-calendar artifact, not a sale. So the population is split:

      * `both`    — filed in BOTH years. Only these members can evidence a NEW or an
                    EXITED position, and only their delta is `delta_comparable`.
      * `entered` — filed `year`, absent from `prior`. Their holdings inflate the raw
                    delta upward and are NOT new positions.
      * `left`    — filed `prior`, absent from `year`. Their holdings depress the raw
                    delta and are NOT exits.

    `delta_total` is reported alongside so the artifact is visible rather than hidden,
    but `delta_comparable` is the one that means anything.

    CAPTURE CONFIDENCE (Mando's binding condition): a NEW badge may only mean the
    position was UNPARSED in the prior year, not that it was bought. Confidence is
    measured PER MEMBER, not per chamber: a chamber-level flag fires on every row in the
    corpus (both CY2024 cells are sub-bar) and so discriminates nothing. What actually
    varies is whether the specific member carrying the badge had a clean prior-year
    filing. `new_low_conf` / `exited_low_conf` count the badges whose own member was
    sub-bar; `confidence` is "low" only when such a badge exists on that row. The
    chamber-year cells stay on the envelope as a standing caveat.

    Bands use the floor convention (`_floor_mid`): an unbounded top band contributes its
    floor, never an invented ceiling.

    DISTRIBUTION-FIRST. This returns the delta DISTRIBUTION and no threshold. There is
    deliberately no watch-cut here — the cut does not exist until the distribution has
    been seen and a bar has been set on it."""
    year, prior, year_members = _yoy_years(con, year, prior)
    cells = _capture_cells(con)
    base = {"as_of": _as_of(), "year": year, "prior_year": prior,
            "year_members": year_members, "capture": cells,
            "bar": _YOY_BAR, "buckets": _BUCKETS,
            "note": "delta_comparable counts ONLY members who filed in both years; "
                    "delta_total includes roster churn and is not a holdings signal"}
    if year is None or prior is None:
        base.update({"rows": [], "count": 0, "population": {}, "distribution": {}})
        return base
    confirmed = _confirmed_members(con) if members_only else set()
    parties = _member_parties(con)

    # Per-member PRIOR-year ticker capture. A member with tickerless equity-like rows in
    # `prior` may hold a position we simply could not read, so anything that looks NEW
    # for them this year is not evidence of a purchase.
    mcap = defaultdict(lambda: [0, 0])
    for c, l, f, s, tk in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, ticker "
            "FROM congress_holdings WHERE coverage_year=? AND asset_type IN "
            "('ST','OP','EF')", (prior,)):
        cell = mcap[(c, l, f, s)]
        cell[0] += 1
        if tk:
            cell[1] += 1
    dirty = {m for m, (den, hit) in mcap.items()
             if den and 100.0 * hit / den < _YOY_BAR}

    # First coverage year each ticker appears ANYWHERE in the corpus. A ticker whose first
    # appearance IS `year` was held by nobody in any prior filing, so a 0 -> N delta on it
    # deserves a second look before it is read as accumulation.
    #
    # WHAT THIS DOES AND DOES NOT SAY. It says the ticker is new to OUR corpus. It does
    # NOT say why, and must not be rendered as though it did — we hold no corporate-actions
    # feed. Chasing ticker "Q" showed both readings live under one flag: Q (Qnity
    # Electronics) is new because the company did not exist before CY2025, while HOOD and
    # SMCI are new only because no member we track had held them. Same flag, different
    # causes. The flag is a prompt to look, never a claim about what happened.
    first_seen = {t: y for t, y in con.execute(
        "SELECT UPPER(ticker), MIN(coverage_year) FROM congress_holdings "
        "WHERE ticker IS NOT NULL AND ticker!='' AND coverage_year IS NOT NULL "
        "GROUP BY UPPER(ticker)")}
    twins = _symbol_twins(con, year, prior)

    filed = {year: set(), prior: set()}
    held = {year: defaultdict(set), prior: defaultdict(set)}
    expo = defaultdict(float)
    kinds = defaultdict(lambda: defaultdict(int))
    for c, l, f, s, yr, ticker, atype, owner, vlo, vhi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "ticker, asset_type, owner, value_lo, value_hi FROM congress_holdings "
            "WHERE ticker IS NOT NULL AND ticker!='' AND coverage_year IN (?,?)",
            (year, prior)):
        if owner_filter != "all" and _OWNER_LABEL.get(owner, "self") != owner_filter:
            continue
        mkey = (c, l, f, s)
        if confirmed and mkey not in confirmed:      # candidates are not insiders
            continue
        filed[yr].add(mkey)
        key = (ticker.upper(), "OP" if atype == "OP" else "SH")
        held[yr][key].add(mkey)
        if yr == year:
            expo[key] += _floor_mid(vlo, vhi)
            # A row whose type code the filer left blank is "none", not dropped and not
            # guessed at — and a None dict key would not survive JSON anyway.
            kinds[key][atype or "none"] += 1

    both = filed[year] & filed[prior]
    entered, left = filed[year] - filed[prior], filed[prior] - filed[year]
    pop = {"both": len(both), "entered": len(entered), "left": len(left),
           "filed_year": len(filed[year]), "filed_prior": len(filed[prior])}
    for cham in ("house", "senate"):
        pop[cham] = {"both": sum(1 for m in both if m[0] == cham),
                     "entered": sum(1 for m in entered if m[0] == cham),
                     "left": sum(1 for m in left if m[0] == cham)}

    rows = []
    for key in set(held[year]) | set(held[prior]):
        cur_all, pri_all = held[year][key], held[prior][key]
        cur_b, pri_b = cur_all & both, pri_all & both
        newm, exitm = cur_b - pri_b, pri_b - cur_b
        # A badge is soft when THAT member's prior-year filing was itself sub-bar.
        n_low = len(newm & dirty)
        e_low = len(exitm & dirty)
        atypes = dict(kinds[key])
        rows.append({
            "ticker": key[0], "instrument": key[1],
            "holders_year": len(cur_all), "holders_prior": len(pri_all),
            "delta_total": len(cur_all) - len(pri_all),
            "holders_both_year": len(cur_b), "holders_both_prior": len(pri_b),
            "delta_comparable": len(cur_b) - len(pri_b),
            "new_members": len(newm), "exited_members": len(exitm),
            "new_low_conf": n_low, "exited_low_conf": e_low,
            "first_seen_year": first_seen.get(key[0]),
            "new_to_corpus": first_seen.get(key[0]) == year,
            # Identity discontinuity: another symbol carries the same company name in the
            # compared years. States the fact, claims no cause, changes no number.
            "symbol_twins": twins.get(key[0], []),
            "identity_discontinuity": bool(twins.get(key[0])),
            "dem": sum(1 for m in cur_b if parties.get(m) == "dem"),
            "rep": sum(1 for m in cur_b if parties.get(m) == "rep"),
            "floor_exposure": round(expo[key]),
            # Filer-stated asset type, reported not interpreted: EF is the filer's own
            # "exchange traded fund" code. Kept so the head of the delta distribution can
            # be read for what it is (broad-market products vs single names) WITHOUT this
            # query taking a position on which of them matter.
            "asset_types": atypes,
            "confidence": "low" if (n_low or e_low) else "ok",
            "confidence_why": (
                ["{} of {} new / {} of {} exited badges come from members whose CY{} "
                 "filing was under the {}% ticker bar".format(
                     n_low, len(newm), e_low, len(exitm), prior, _YOY_BAR)]
                if (n_low or e_low) else [])})
    rows.sort(key=lambda r: (-r["delta_comparable"], -r["holders_both_year"],
                             r["ticker"]))

    dist = {"delta_comparable": dict.fromkeys(_BUCKETS, 0),
            "delta_total": dict.fromkeys(_BUCKETS, 0)}
    for r in rows:
        dist["delta_comparable"][_bucket(r["delta_comparable"])] += 1
        dist["delta_total"][_bucket(r["delta_total"])] += 1
    low = sum(1 for r in rows if r["confidence"] == "low")
    base.update({"rows": rows, "count": len(rows), "population": pop,
                 "distribution": dist, "low_confidence_rows": low,
                 "sub_bar_members": len(dirty & both),
                 # Standing caveat: BOTH CY2024 chamber cells are under the bar, so the
                 # whole comparison rests on a prior year we did not fully read. That is
                 # a property of the comparison, not of any one row.
                 "sub_bar_cells": ["{} CY{} ticker capture {}%".format(
                     ch, yr, (cells.get((ch, yr)) or {}).get("ticker_pct"))
                     for ch in ("house", "senate") for yr in (prior, year)
                     if (cells.get((ch, yr)) or {}).get("ticker_pct") is not None
                     and cells[(ch, yr)]["ticker_pct"] < _YOY_BAR]})
    return base


_CUT_MIN_DELTA = 2         # Mando's Phase Y ruling, set on the shown distribution


def q_congress_breadth_watch(con, **kw):
    """SM-C3 Phase Y watch-cut, ratified by Mando on the shown delta distribution:
    ST-only, delta_comparable >= +2, comparable cohort, capture-confidence on, from->to
    counts visible.

    ST-only because the raw delta head is 71% index products — at delta >= +3, 17 of 24
    rows carried an EF or MF code. A cut that surfaces IVV every cycle is not a cut.
    'ST-only' means the filer's OWN type codes for that ticker are exactly {ST}: a ticker
    reported as ST by one member and EF by another is NOT single-name-clean and is left
    out rather than half-counted.

    THIS IS A CONTEXT SECTION, NEVER AN ALERT. It emits no event, sets no watermark, and
    pages nobody. Breadth is a LEVEL, not a trade — a member's annual FD says what they
    held on Dec 31, not that they bought it, and the disclosure lands months late. Wiring
    this to an alert would page on a year-old position.

    Rows carry TWO cause-free markers, both stating a fact and neither re-ranking
    anything. `identity_discontinuity`: another symbol carries the same company name in
    the compared years, so this symbol's cohort may span a notation change. Fiserv is
    filed as both FISV and FI, and FISV's +2 falls to +1 when the two are unioned. The
    cut is left EXACTLY as computed - hand-editing it or quietly re-ranking on this
    signal would be the first silent judgment in the pipeline. The systematic fix is a
    rename-union pass, which is real work and is filed, not improvised here.

    `new_to_corpus`: the ticker appears in no prior year's filings at all, so
    its 0 -> N warrants a look before being read as accumulation. The flag does NOT say
    why — Q (Qnity, a company that did not exist before CY2025) and SMCI (listed since
    2007, simply never held by anyone we track) both raise it. Such rows are RETURNED and
    MARKED, never dropped: the position genuinely is newly held, and dropping it would
    hide a real change in what members hold."""
    res = q_congress_breadth_yoy(con, **kw)
    rows = [r for r in res["rows"]
            if r["delta_comparable"] >= _CUT_MIN_DELTA
            and set(r["asset_types"]) == {"ST"}]
    res = dict(res)
    res.update({
        "rows": rows, "count": len(rows), "min_delta": _CUT_MIN_DELTA,
        "asset_filter": "ST",
        "corporate_action_rows": sum(1 for r in rows if r["new_to_corpus"]),
        "identity_discontinuity_rows": sum(1 for r in rows
                                           if r["identity_discontinuity"]),
        "cut": "ST-only, delta_comparable >= +{} on the both-years cohort".format(
            _CUT_MIN_DELTA),
        "kind": "context",
        "never_alert": "breadth is a LEVEL from an annual filed months late - it is "
                       "context, and this surface emits no events"})
    return res


def q_committee_holdings(con, committee_id=None, min_holders=1):
    """SM-C3 Phase R: COMMITTEE x HOLDINGS. Which tickers the members of one committee
    hold, counted on each member's latest annual FD.

    THREE LIMITS, all printed by the surface rather than left to the reader:

    1. PARTIAL BY CONSTRUCTION. A committee attaches only where a filer identity resolved
       to one person who holds a seat in the CURRENT Congress. That is 55.7% of anchor
       rows (house 57.9%, senate 52.4%) — worse than the roster-join rate, because a
       member who resolved cleanly but has since left Congress has no current seat.
       `coverage` travels with every result.
    2. PRESENT-TENSE MEMBERSHIP, DATED HOLDINGS. congress-legislators publishes no
       historical membership, so this pairs a member's CURRENT seat with holdings from
       whatever year their latest annual covers. It says "this member, who today sits on
       X, disclosed Y" — never "they sat on X when they held Y". `anchor_years` shows
       the spread so the mismatch is visible.
    3. NO CAUSAL READING. Holding a ticker in a committee's jurisdiction is not evidence
       of anything on its own; committee assignment and portfolio both correlate with a
       member's background. This returns counts, never an inference.

    Without `committee_id`, returns the committee roll (seats and filers per committee)
    so the reader picks from what is actually joinable."""
    cov = {}
    try:
        from . import committees as cmod
        cov = cmod.coverage(con)
    except Exception:                       # committees never synced — say so, don't fake
        cov = {}
    total = sum(c["rows"] for c in cov.values()) or 0
    seen = sum(c["with_committee"] for c in cov.values()) or 0
    base = {"as_of": _as_of(), "coverage": cov,
            "coverage_pct": (round(100.0 * seen / total, 1) if total else None),
            "note": "committee membership is a CURRENT-Congress snapshot joined to "
                    "DATED holdings; a member who left Congress carries no seat",
            "causal_note": "co-holding within a committee's jurisdiction is not evidence "
                           "of anything by itself and is not presented as such"}
    latest = {}
    for c, l, f, s, y in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, MAX(coverage_year) "
            "FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, member_last, member_first, state_dist"):
        latest[(c, l, f, s)] = y
    bio = {(c, l, f, s): b for c, l, f, s, b in con.execute(
        "SELECT chamber, member_last, member_first, state_dist, bioguide "
        "FROM congress_member_roster WHERE bioguide IS NOT NULL")}
    seats = defaultdict(list)               # bioguide -> [(cid, name, title)]
    names = {}
    for bg, cid, nm, title in con.execute(
            "SELECT bioguide, committee_id, committee_name, title "
            "FROM congress_committees"):
        seats[bg].append((cid, nm, title))
        names[cid] = nm
    if not committee_id:
        roll = defaultdict(lambda: {"seats": 0, "filers": 0})
        filers = {bio[k] for k in latest if k in bio}
        for bg, ss in seats.items():
            for cid, _nm, _t in ss:
                roll[cid]["seats"] += 1
                if bg in filers:
                    roll[cid]["filers"] += 1
        rows = [{"committee_id": cid, "committee_name": names.get(cid),
                 "seats": v["seats"], "filers_we_hold": v["filers"]}
                for cid, v in roll.items()]
        rows.sort(key=lambda r: (-r["filers_we_hold"], -r["seats"]))
        base.update({"committee_id": None, "rows": rows, "count": len(rows),
                     "committees": len(rows)})
        return base
    members = {bg for bg, ss in seats.items()
               if any(cid == committee_id for cid, _n, _t in ss)}
    agg = defaultdict(lambda: {"members": set(), "mid": 0.0, "years": set()})
    for c, l, f, s, y, tk, atype, vlo, vhi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "ticker, asset_type, value_lo, value_hi FROM congress_holdings "
            "WHERE ticker IS NOT NULL AND ticker!='' AND coverage_year IS NOT NULL"):
        k = (c, l, f, s)
        if latest.get(k) != y or bio.get(k) not in members:
            continue
        a = agg[(tk.upper(), "OP" if atype == "OP" else "SH")]
        a["members"].add(k)
        a["mid"] += _floor_mid(vlo, vhi)
        a["years"].add(y)
    rows = [{"ticker": t, "instrument": i, "holder_count": len(a["members"]),
             "floor_exposure": round(a["mid"]),
             "anchor_years": ",".join(str(y) for y in sorted(a["years"]))}
            for (t, i), a in agg.items() if len(a["members"]) >= min_holders]
    rows.sort(key=lambda r: (-r["holder_count"], -r["floor_exposure"], r["ticker"]))
    joined = {bg for bg in members if bg in set(bio.values())}
    base.update({
        "committee_id": committee_id, "committee_name": names.get(committee_id),
        "rows": rows, "count": len(rows), "seats": len(members),
        "filers_we_hold": len(joined),
        "anchor_years": sorted({y for a in agg.values() for y in a["years"]})})
    return base


def q_congress_holders(con, ticker, instrument="SH"):
    """Drill-down: the members holding one (ticker, instrument) in their latest FD, with
    owner and value band. This expandable holder list IS the who-holds-what matrix."""
    want = "OP" if str(instrument).upper() == "OP" else "SH"
    latest = _member_latest_years(con)
    rows = []
    for c, l, f, s, yr, atype, owner, vlo, vhi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, filing_year, asset_type, "
            "owner, value_lo, value_hi FROM congress_holdings WHERE UPPER(ticker)=?",
            (ticker.upper(),)):
        if ("OP" if atype == "OP" else "SH") != want or latest.get((c, l, f, s)) != yr:
            continue
        rows.append({"member": "{}, {}".format(l or "?", f or ""), "chamber": c, "state": s,
                     "owner": _OWNER_LABEL.get(owner, "self"),
                     "value_lo": vlo, "value_hi": vhi})
    rows.sort(key=lambda r: -(r["value_lo"] or 0))
    return {"as_of": _as_of(), "ticker": ticker.upper(), "instrument": want,
            "count": len(rows), "rows": rows}


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
