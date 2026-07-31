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
    thousands -> ~0.001. Returns the multiplier to dollars (1 or 1000); defaults to 1
    (dollars, the majority) only when NO period has price coverage to anchor on."""
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
            return 1000 if ratios[len(ratios) // 2] < 0.01 else 1
    return 1


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
    scale = _filer_unit_scale(con, cik, periods)  # one unit for the whole filer
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
        "unmapped_value": sum(r["value"] for r in unmapped)})
    return base


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
        cur = _scaled_holdings(con, cik, periods[0], _filer_unit_scale(con, cik, periods))
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


def q_congress_gaps(con):
    """SM-C2 P3: who is BEHIND the breadth counts. Per filer identity: chamber, party,
    how it resolved, filing years present, and holdings rows. The point is that breadth
    counts are FLOORS — a member missing from the corpus (paper-only filing, WAF-blocked
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
