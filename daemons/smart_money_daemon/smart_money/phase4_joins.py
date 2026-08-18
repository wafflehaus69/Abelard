"""SM-A1 Phase 4: cross-surface overlap analysis (the deliverable).

Computes joins (a)-(f) over the populated corpus (13F holdings, Form 4
transactions, congressional trades) and the Mando overlay. Reports RAW counts
and row-level backing. NO composite score, NO ranking, NO verdict — a weighted
conviction score is Mando's modeling decision, not this order's.

SMID banding (needed by (a) and (d)) is BLOCKED-ON-METHOD until Mando picks a
market-cap source; (a) and (d) are reported FULL-UNIVERSE ONLY, the SMID cut is
marked blocked, and no proxy is substituted.

Ticker is the cross-surface join key (13F via OpenFIGI, Form 4 via issuer
trading symbol, congress via normalized symbol). Cross-source ticker mismatch
is a known coverage limit, stated in the report.
"""
import argparse
import datetime as dt
import os
import sys
from collections import defaultdict

from . import db as dbmod
from .mdfmt import md_table
from .overlay import load_overlay

try:
    import pandas as pd
except ImportError:
    pd = None

WINDOWS_B = (90, 180)
NAMED_CASES = ("WULF", "XE", "CCXI")

STANDING_WARNINGS = [
    "13F is stale by construction — roughly a 45-day filing lag; a holding shown "
    "here may already be closed.",
    "13F is a PARTIAL view — it omits shorts, most derivatives, non-US listings, "
    "cash, and private positions.",
    "Confidential treatment is granted for some positions — absence from a 13F is "
    "NOT evidence of absence of a position.",
    "Survivorship governs this whole exercise — convergence is a funnel-narrowing "
    "PRIOR, NOT a demonstrated edge, and NOT a sizing input.",
    "Thiel power-law mismatch — copying selection without the sizing and holding "
    "period reproduces the losses and discards the compensating mechanism.",
    "Compliance — everything here is public filings analyzed behind a standard "
    "information wall. No recommendations, rankings, or verdicts are made.",
]


def _13f_ticker_periods(con):
    """{ticker: {(cik, period)}} for long holdings with a resolved ticker."""
    out = defaultdict(set)
    for tk, cik, per in con.execute(
        "SELECT ticker, cik, period FROM thirteenf_holdings "
        "WHERE ticker IS NOT NULL AND put_call='long'"
    ):
        out[tk.upper()].add((cik, per))
    return out


# ---------------------------------------------------------------- (a)
def join_a_multi_principal(con, bands=None, floors=None):
    """Direction-aware: per (ticker, period, filer) compute NET direction
    (long+call value minus put value). Convergence requires 2+ filers on the
    SAME side — a long-vs-short pair is a DISAGREEMENT, not convergence, and is
    flagged rather than counted as agreement.

    `floors` is {cik: position_floor_pct}. This function previously ignored the
    per-filer floor that _manager_flow applies, so the two code paths disagreed
    about what counts as a position: a long-tailed filer contributed its ENTIRE
    sub-floor tail here. Measured on the 28-filer shelf, that inflated the
    convergence count from 1,115 to 1,818 — 703 of the 1,030 apparent new
    convergences (68%) were floor-tail artefacts, and the headline delta read
    +131% when the honest figure is +41.5%.

    Uses value_usd, not value. The net SIGN is invariant to a uniform per-filer
    scale so the classification was never wrong, but the raw column mixes units
    across filers and must not be the one a reader sees.
    """
    floors = floors or {}
    fl = {}
    for cik, pct in floors.items():
        try:
            fl[str(int(cik))] = pct
        except (TypeError, ValueError):
            fl[str(cik)] = pct
    net = defaultdict(lambda: defaultdict(float))  # (ticker,period) -> cik -> net
    for tk, per, cik, pc, val, pct in con.execute(
        "SELECT h.ticker, h.period, h.cik, h.put_call, h.value_usd, "
        "  CASE WHEN b.book > 0 THEN 100.0 * h.value_usd / b.book ELSE NULL END "
        "FROM thirteenf_holdings h JOIN ("
        "  SELECT cik, period, SUM(value_usd) AS book FROM thirteenf_holdings "
        "  GROUP BY cik, period) b "
        "  ON b.cik = h.cik AND b.period = h.period "
        "WHERE h.ticker IS NOT NULL"
    ):
        try:
            key = str(int(cik))
        except (TypeError, ValueError):
            key = str(cik)
        floor = fl.get(key)
        if floor and pct is not None and pct < floor:
            continue                      # marked below floor: expresses no view
        sign = -1 if pc == "put" else 1
        net[(tk.upper(), per)][cik] += (val or 0) * sign
    out = []
    for (tk, per), ciks in net.items():
        longs = sorted(c for c, v in ciks.items() if v > 0)
        shorts = sorted(c for c, v in ciks.items() if v < 0)
        if len(longs) < 2 and len(shorts) < 2:
            continue
        out.append({
            "ticker": tk, "period": per,
            "long_filers": len(longs), "short_filers": len(shorts),
            # FIX: a two-sided cluster (>=2 each) was silently labelled "long".
            # Emit "both" so a genuine two-sided convergence is never hidden.
            "converge_dir": ("both" if len(longs) >= 2 and len(shorts) >= 2
                             else "long" if len(longs) >= 2 else "short"),
            "disagreement": bool(longs) and bool(shorts),
            "long_ciks": ",".join(longs), "short_ciks": ",".join(shorts) or "-",
            "band": (bands or {}).get(tk, "?"),
        })
    return sorted(out, key=lambda r: -max(r["long_filers"], r["short_filers"]))


def convergence_accounting(con):
    """Close the 19->16 debt. Reconcile the NAIVE co-holding baseline (2+ filers
    holding a LONG position in the same (ticker, period) — what (a) counted before
    the direction-aware rework) against direction-aware netting, classifying EVERY
    naive pair so the total is conserved:
      same-direction : >=2 filers net the SAME way (these are the surviving 16).
      opposed        : one filer net long AND one net put-heavy on the same
                       name/quarter — a 1-vs-1 opposition the >=2-same-side gate
                       (join_a line ~77) silently discards without flagging.
      excluded       : a put position netted a filer's long to <=0, leaving <2
                       filers net-long — the pair drops with a stated reason.
    Asserts same-direction + opposed + excluded == naive_total (nothing lost)."""
    holdings = defaultdict(lambda: defaultdict(
        lambda: {"long": 0.0, "call": 0.0, "put": 0.0}))
    for tk, per, cik, pc, val in con.execute(
        "SELECT ticker, period, cik, put_call, value FROM thirteenf_holdings "
        "WHERE ticker IS NOT NULL"
    ):
        bucket = pc if pc in ("long", "call", "put") else "long"
        holdings[(tk.upper(), per)][cik][bucket] += (val or 0)
    naive, same_dir, opposed, excluded = 0, [], [], []
    for (tk, per), ciks in holdings.items():
        long_filers = [c for c, b in ciks.items() if b["long"] > 0]
        if len(long_filers) < 2:
            continue  # never a naive convergence
        naive += 1
        nets = {c: b["long"] + b["call"] - b["put"] for c, b in ciks.items()}
        pos = sorted(c for c, n in nets.items() if n > 0)
        neg = sorted(c for c, n in nets.items() if n < 0)
        has_put = any(b["put"] > 0 for b in ciks.values())
        rec = {"ticker": tk, "period": per, "long_filers": len(long_filers),
               "net_long": len(pos), "net_put_heavy": len(neg), "put_present": has_put}
        if len(pos) >= 2 or len(neg) >= 2:
            rec["class"] = "same-direction"
            rec["direction"] = "long" if len(pos) >= 2 else "put-heavy"
            same_dir.append(rec)
        elif pos and neg:
            rec["class"] = "opposed"
            rec["reason"] = ("1 filer net long, 1 net put-heavy on the same "
                             "name/quarter — a 1v1 opposition the >=2-same-side "
                             "gate drops without flagging")
            opposed.append(rec)
        else:
            rec["class"] = "excluded"
            rec["reason"] = ("a put position netted a long filer to <=0, leaving "
                             "<2 net-long (net_long={} put_heavy={})".format(
                                 len(pos), len(neg)))
            excluded.append(rec)
    assert len(same_dir) + len(opposed) + len(excluded) == naive, (
        "accounting lost pairs: {} != {}".format(
            len(same_dir) + len(opposed) + len(excluded), naive))
    return {"naive_total": naive, "same_direction": same_dir,
            "opposed": opposed, "excluded": excluded}


# ---------------------------------------------------------------- (b)
def join_b_inst_x_insider(con, anchor):
    held = _13f_ticker_periods(con)
    out = {}
    for w in WINDOWS_B:
        start = (dt.date.fromisoformat(anchor) - dt.timedelta(days=w)).isoformat()
        buys = con.execute(
            "SELECT ticker, COUNT(*) n, COUNT(DISTINCT reporting_cik) nb, "
            "GROUP_CONCAT(DISTINCT reporting_person) "
            "FROM form4_transactions WHERE code='P' AND plan_flag=0 "
            "AND ticker IS NOT NULL AND tx_date>=? AND tx_date<=? "
            "GROUP BY ticker", (start, anchor)).fetchall()
        rows = []
        for tk, n, nb, who in buys:
            u = tk.upper()
            if u in held:
                rows.append({"ticker": u, "insider_buys": n, "distinct_buyers": nb,
                             "n_13f_filers": len({c for c, _ in held[u]}),
                             "buyers": who})
        out[w] = sorted(rows, key=lambda r: -r["distinct_buyers"])
    return out


# ---------------------------------------------------------------- (c)
def join_c_inst_x_congress(con):
    held = _13f_ticker_periods(con)
    rows = con.execute(
        "SELECT ticker, COUNT(DISTINCT person_id) nm, "
        "SUM(CASE WHEN side='purchase' THEN 1 ELSE 0 END) buys "
        "FROM congress_trades WHERE ticker IS NOT NULL AND asset_type='Stock' "
        "GROUP BY ticker").fetchall()
    out = []
    for tk, nm, buys in rows:
        u = (tk or "").upper()
        if u in held:
            out.append({"ticker": u, "congress_members": nm, "congress_buys": buys or 0,
                        "n_13f_filers": len({c for c, _ in held[u]})})
    return sorted(out, key=lambda r: -r["congress_members"])


# ---------------------------------------------------------------- (d)
def join_d_new_positions(con, bands=None):
    """Per filer, quarter-over-quarter adds / exits / material (>2x) size changes.
    Full-universe; SMID band annotated per row when available."""
    by_filer = defaultdict(lambda: defaultdict(dict))  # cik -> period -> {ticker: value}
    for cik, per, tk, val in con.execute(
        "SELECT cik, period, ticker, value FROM thirteenf_holdings "
        "WHERE put_call='long' AND ticker IS NOT NULL"
    ):
        by_filer[cik][per][tk.upper()] = (val or 0)
    adds, exits, sizes = [], [], []
    for cik, periods in by_filer.items():
        ordered = sorted(periods)
        for i in range(1, len(ordered)):
            prev, cur = ordered[i - 1], ordered[i]
            pv, cv = periods[prev], periods[cur]
            for tk in cv.keys() - pv.keys():
                adds.append({"cik": cik, "period": cur, "ticker": tk, "value": cv[tk],
                             "band": (bands or {}).get(tk, "?")})
            for tk in pv.keys() - cv.keys():
                exits.append({"cik": cik, "period": cur, "ticker": tk, "was_value": pv[tk],
                              "band": (bands or {}).get(tk, "?")})
            for tk in cv.keys() & pv.keys():
                b = (bands or {}).get(tk, "?")
                if pv[tk] > 0 and cv[tk] >= 2 * pv[tk]:
                    sizes.append({"cik": cik, "period": cur, "ticker": tk,
                                  "from": pv[tk], "to": cv[tk], "dir": "up_2x", "band": b})
                elif cv[tk] > 0 and pv[tk] >= 2 * cv[tk]:
                    sizes.append({"cik": cik, "period": cur, "ticker": tk,
                                  "from": pv[tk], "to": cv[tk], "dir": "down_2x", "band": b})
    return {"adds": adds, "exits": exits, "size_changes": sizes}


# ---------------------------------------------------------------- (f)
def join_f_named_cases(con):
    out = {}
    for name in NAMED_CASES:
        surfaces = {}
        h = con.execute(
            "SELECT cik, MIN(period), COUNT(*) FROM thirteenf_holdings "
            "WHERE UPPER(ticker)=? GROUP BY cik", (name,)).fetchall()
        surfaces["13f"] = [{"cik": c, "first_period": p, "rows": n} for c, p, n in h]
        f4 = con.execute(
            "SELECT COUNT(*), MIN(tx_date) FROM form4_transactions WHERE UPPER(ticker)=?",
            (name,)).fetchone()
        surfaces["form4"] = {"rows": f4[0], "first_tx": f4[1]} if f4[0] else None
        cg = con.execute(
            "SELECT COUNT(*), MIN(tx_date) FROM congress_trades WHERE UPPER(ticker)=?",
            (name,)).fetchone()
        surfaces["congress"] = {"rows": cg[0], "first_tx": cg[1]} if cg[0] else None
        found = bool(h) or (f4[0] > 0) or (cg[0] > 0)
        out[name] = {"found": found, "surfaces": surfaces}
    return out


def _overlay_tag(overlay, ticker):
    conv, watch = overlay.match(ticker)
    t = []
    if conv:
        t.append("conviction")
    if watch:
        t.append("watchlist")
    return ",".join(t)


def _ticker_period_detail(con, ticker):
    return con.execute(
        "SELECT period, cik, put_call, value FROM thirteenf_holdings "
        "WHERE UPPER(ticker)=? ORDER BY period, cik", (ticker.upper(),)).fetchall()


def accounting_md(con, acc):
    """Render the convergence accounting as a tracked markdown artifact — the
    19->16 debt closed in writing. Includes the AVGO/INTC put-period detail."""
    m = ["# CONVERGENCE ACCOUNTING — smart_money_daemon SM-A1 (a) rework", "",
         "Closes the 19->16 debt from the direction-aware multi-principal join. "
         "Reconciles every NAIVE co-holding (2+ filers holding a LONG position in "
         "the same ticker and quarter) against direction-aware netting "
         "(long+call value minus put value), classifying each so the total is "
         "conserved. Regenerated by phase4_joins.convergence_accounting.", "",
         "**NAIVE {} = same-direction {} + opposed {} + excluded {}** (assert-"
         "conserved).".format(acc["naive_total"], len(acc["same_direction"]),
                              len(acc["opposed"]), len(acc["excluded"])), ""]
    m.append("## Opposed — 1 net-long vs 1 net-put-heavy, same name and quarter")
    m.append("")
    m.append("The pairs the >=2-same-side gate silently dropped in the 19->16 "
             "rework. A put position flips one filer net-negative, leaving a 1v1 "
             "opposition that never reached the convergence table. Now surfaced.")
    m.append("")
    m.append("| ticker | period | net_long | net_put_heavy |")
    m.append("|---|---|---|---|")
    for x in acc["opposed"]:
        m.append("| {} | {} | {} | {} |".format(
            x["ticker"], x["period"], x["net_long"], x["net_put_heavy"]))
    m.append("")
    m.append("## Excluded — a put netted a long filer to <=0")
    m.append("")
    if acc["excluded"]:
        m.append("| ticker | period | reason |")
        m.append("|---|---|---|")
        for x in acc["excluded"]:
            m.append("| {} | {} | {} |".format(x["ticker"], x["period"], x["reason"]))
    else:
        m.append("None.")
    m.append("")
    m.append("## Same-direction — the {} surviving convergences".format(
        len(acc["same_direction"])))
    m.append("")
    m.append("| ticker | period | direction | net_long | net_put_heavy |")
    m.append("|---|---|---|---|---|")
    for x in acc["same_direction"]:
        m.append("| {} | {} | {} | {} | {} |".format(
            x["ticker"], x["period"], x["direction"], x["net_long"], x["net_put_heavy"]))
    m.append("")
    m.append("## AVGO / INTC put-period detail (explicit confirmation)")
    m.append("")
    m.append("Situational Awareness (2045724) writes large index-name puts; where "
             "that overlaps a Duquesne (1536411) long in the same quarter it "
             "creates the 1v1 opposition. INTC 2026-03-31 is one of the dropped "
             "three; AVGO's put quarters had only one long filer so AVGO was never "
             "a naive co-hold (its surviving convergence is 2025-06-30, both long).")
    m.append("")
    for tkr in ("AVGO", "INTC"):
        m.append("- **{}**".format(tkr))
        for per, cik, pc, val in _ticker_period_detail(con, tkr):
            m.append("  - {} cik {} {} value {}".format(per, cik, pc, val))
    m.append("")
    return "\n".join(m) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-A1 Phase 4 overlap joins")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--now", default=dt.date.today().isoformat())
    ap.add_argument("--out", default=None)
    ap.add_argument("--accounting", action="store_true",
                    help="write the convergence accounting artifact and exit")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    if args.accounting:
        import pathlib
        acc = convergence_accounting(con)
        out = args.out or os.path.join(
            os.path.dirname(__file__), "..", "scans", "CONVERGENCE_ACCOUNTING.md")
        pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(out).write_text(accounting_md(con, acc), encoding="utf-8")
        print("[accounting] naive={} same={} opposed={} excluded={} -> {}".format(
            acc["naive_total"], len(acc["same_direction"]), len(acc["opposed"]),
            len(acc["excluded"]), out))
        return 0
    overlay = load_overlay()
    out = args.out or os.path.join(
        dbmod.SCANS_DIR, "PHASE4_OVERLAP_{}.md".format(args.now.replace("-", "")))
    from . import marketcap
    bands = marketcap.bands_for(con, [
        r[0] for r in con.execute(
            "SELECT DISTINCT ticker FROM thirteenf_holdings WHERE ticker IS NOT NULL")])

    # Local import: queries imports this module lazily, so a module-level import
    # here would be circular. The floor must apply on this path too, or the
    # markdown report disagrees with the dashboard about the same number.
    from .queries import _filer_floor
    a = join_a_multi_principal(con, bands, floors=_filer_floor())
    b = join_b_inst_x_insider(con, args.now)
    c = join_c_inst_x_congress(con)
    d = join_d_new_positions(con, bands)
    f = join_f_named_cases(con)

    import pathlib
    pathlib.Path(out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(out).write_text(_render(con, args.now, overlay, a, b, c, d, f))
    print("[phase4] a={} b90={} b180={} c={} d_adds={} named_found={} -> {}".format(
        len(a), len(b[90]), len(b[180]), len(c), len(d["adds"]),
        [n for n, v in f.items() if v["found"]], out))
    return 0


def _render(con, anchor, overlay, a, b, c, d, f):
    m = ["# PHASE4_OVERLAP — smart_money_daemon SM-A1 Phase 4", "",
         "Generated {}. Raw counts and row-level backing only. NO composite "
         "score, NO ranking, NO verdict — those are Mando's.".format(anchor), ""]

    # method + data with as-of
    m.append("## Method + data (as-of)")
    m.append("")
    per = con.execute("SELECT MIN(period), MAX(period) FROM thirteenf_holdings").fetchone()
    f4d = con.execute("SELECT MIN(tx_date), MAX(tx_date) FROM form4_transactions").fetchone()
    cgd = con.execute("SELECT MIN(tx_date), MAX(tx_date) FROM congress_trades WHERE asset_type='Stock'").fetchone()
    m.append("- **13F holdings**: {} rows, periods {}..{} (as-of = filing period end; "
             "STALE ~45d by construction). Confirmed filer set only.".format(
                 con.execute("SELECT COUNT(*) FROM thirteenf_holdings").fetchone()[0],
                 per[0], per[1]))
    m.append("- **Form 4 corpus**: {} rows, tx {}..{}. Backfilled issuer set "
             "(overlay + registry + trump_network), 36-month depth.".format(
                 con.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0],
                 f4d[0], f4d[1]))
    m.append("- **Congress**: {} stock rows, tx {}..{}.".format(
        con.execute("SELECT COUNT(*) FROM congress_trades WHERE asset_type='Stock'").fetchone()[0],
        cgd[0], cgd[1]))
    m.append("- Join key = uppercased ticker across surfaces (13F OpenFIGI / Form 4 "
             "issuer symbol / congress normalized). Cross-source symbol mismatch is "
             "a coverage limit — see gaps.")
    m.append("")
    m.append("## SMID banding — SEC companyfacts (Mando-ratified method)")
    m.append("")
    m.append("Market cap = shares outstanding (SEC companyconcept dei/us-gaap, "
             "keyless, same EDGAR client) x latest price. Bands: micro <$300M, "
             "small $300M-$2B, mid $2B-$10B, large >=$10B. Multi-class names whose "
             "shares are not in a single concept (e.g. META, MSTR) resolve to "
             "UNBANDABLE and are reported, never guessed.")
    m.append("")
    bd = dict(con.execute("SELECT band, COUNT(*) FROM market_cap GROUP BY band").fetchall())
    m.append("- Band distribution (all banded tickers): {}".format(bd or "none computed"))
    m.append("- **AS-OF CAVEAT:** shares as-of the latest cover-page filing, price "
             "as-of the latest quote. A stale or wrong price on a volatile small "
             "cap is a labeled error source — bands near a boundary are soft.")
    m.append("- SMID subset below = micro + small + mid (large and unbandable excluded).")
    m.append("")

    # per-principal holdings summary
    m.append("## Per-principal 13F holdings summary")
    m.append("")
    sm = con.execute(
        "SELECT cik, COUNT(DISTINCT period) q, COUNT(*) rows, "
        "COUNT(DISTINCT ticker) tickers, MAX(period) latest "
        "FROM thirteenf_holdings GROUP BY cik ORDER BY rows DESC").fetchall()
    m.append(md_table(pd.DataFrame(
        [{"cik": r[0], "quarters": r[1], "holding_rows": r[2],
          "distinct_tickers": r[3], "latest_period": r[4]} for r in sm])))
    m.append("")

    # (a)
    m.append("## (a) Multi-principal convergence — direction-aware, 2+ filers same side")
    m.append("")
    disagreements = [r for r in a if r["disagreement"]]
    m.append("{} (ticker, period) convergences (>=2 filers on the SAME side). "
             "{} of them ALSO have a filer on the opposite side (disagreement — "
             "flagged, not counted as agreement).".format(len(a), len(disagreements)))
    m.append("")
    cols = ["ticker", "period", "converge_dir", "long_filers", "short_filers",
            "disagreement", "band", "overlay"]
    if a:
        m.append(md_table(pd.DataFrame(
            [{**r, "overlay": _overlay_tag(overlay, r["ticker"])} for r in a[:60]])[cols]))
        if len(a) > 60:
            m.append("\n(showing 60 of {})".format(len(a)))
    m.append("")
    smid_a = [r for r in a if r["band"] in ("micro", "small", "mid")]
    m.append("### (a) SMID subset — {} convergences on micro/small/mid names".format(len(smid_a)))
    m.append("")
    if smid_a:
        m.append(md_table(pd.DataFrame(
            [{**r, "overlay": _overlay_tag(overlay, r["ticker"])} for r in smid_a])[cols]))
    else:
        m.append("None in micro/small/mid (the confirmed filers' convergences are "
                 "large-cap or unbandable — see coverage).")
    m.append("")

    # (b)
    m.append("## (b) Institutional x insider — 13F holding + discretionary open-market Form 4 buy")
    m.append("")
    m.append("Excludes 10b5-1 planned transactions (plan_flag=0 only). The "
             "highest-interest join — pairs a position with a decision.")
    for w in WINDOWS_B:
        m.append("")
        m.append("### {}d window — {} tickers".format(w, len(b[w])))
        if b[w]:
            m.append(md_table(pd.DataFrame(
                [{**r, "overlay": _overlay_tag(overlay, r["ticker"])} for r in b[w]])))
        else:
            m.append("None.")
    m.append("")
    m.append("> **Selection-effect note (ABCL / GUTS).** ABCL and GUTS surface "
             "here and in the g1 insider-buy counter, but that is largely a "
             "SELECTION ARTIFACT: both are Thiel-network issuers we deliberately "
             "backfilled, so Thiel-adjacent insider buying was always going to "
             "appear on them. Their presence is NOT independent corroboration — "
             "we looked precisely where we expected to find it. Treat as "
             "coverage-shaped, not as a discovered convergence.")
    m.append("")

    # (c)
    m.append("## (c) Institutional x congressional — 13F holding intersects a congressional disclosure")
    m.append("")
    m.append("{} tickers held by a confirmed 13F filer AND traded by Congress.".format(len(c)))
    m.append("")
    if c:
        m.append(md_table(pd.DataFrame(
            [{**r, "overlay": _overlay_tag(overlay, r["ticker"])} for r in c[:60]])))
    m.append("")

    # (d)
    m.append("## (d) New positions — QoQ adds / exits / material size changes (full-universe)")
    m.append("")
    m.append("Adds, exits, and >=2x size changes reported SEPARATELY.")
    for label, key in (("Adds", "adds"), ("Exits", "exits"), ("Size changes (>=2x)", "size_changes")):
        rows = d[key]
        m.append("")
        m.append("### {} — {}".format(label, len(rows)))
        if rows:
            m.append(md_table(pd.DataFrame(rows[:50])))
            if len(rows) > 50:
                m.append("\n(showing 50 of {})".format(len(rows)))
    m.append("")

    # (e)
    m.append("## (e) Mando-book intersection (read-only)")
    m.append("")
    all_tickers = ({r["ticker"] for r in a} | {r["ticker"] for w in WINDOWS_B for r in b[w]}
                   | {r["ticker"] for r in c}
                   | {r["ticker"] for r in d["adds"]})
    conv = sorted(t for t in all_tickers if overlay.match(t)[0])
    watch = sorted(t for t in all_tickers if overlay.match(t)[1])
    m.append("- Tickers surfaced in (a)-(d) that are in conviction_book: {}".format(
        " ".join(conv) or "none"))
    m.append("- ... in watchlist: {}".format(" ".join(watch) or "none"))
    m.append("")

    # (f)
    m.append("## (f) Named-case sanity check — WULF / XE / CCXI")
    m.append("")
    for name, v in f.items():
        if not v["found"]:
            m.append("- **{}**: NOT PRESENT on any surface in any period. "
                     "NEGATIVE COVERAGE FINDING — the name is absent from the "
                     "assembled dataset (13F confirmed-filer set + backfilled Form 4 "
                     "issuers + congress). Absence here reflects ingest scope, not "
                     "market reality.".format(name))
        else:
            s = v["surfaces"]
            bits = []
            if s["13f"]:
                first = min(x["first_period"] for x in s["13f"])
                dqe = [x for x in s["13f"] if x["cik"] == "1536411"]
                dq = " DUQUESNE first-period {}".format(dqe[0]["first_period"]) if dqe else ""
                bits.append("13F: filers {} first {}{}".format(
                    [x["cik"] for x in s["13f"]], first, dq))
            if s["form4"]:
                bits.append("Form4: {} rows first {}".format(s["form4"]["rows"], s["form4"]["first_tx"]))
            if s["congress"]:
                bits.append("Congress: {} rows first {}".format(s["congress"]["rows"], s["congress"]["first_tx"]))
            m.append("- **{}**: PRESENT — {}".format(name, "; ".join(bits)))
    m.append("")

    # coverage gaps (mandatory)
    m.append("## Coverage gaps (MANDATORY)")
    m.append("")
    unmapped_cusip = con.execute("SELECT COUNT(*) FROM cusip_ticker WHERE ticker IS NULL").fetchone()[0]
    total_cusip = con.execute("SELECT COUNT(*) FROM cusip_ticker").fetchone()[0]
    nullticker_13f = con.execute("SELECT COUNT(*) FROM thirteenf_holdings WHERE ticker IS NULL").fetchone()[0]
    m.append("- 13F holdings with UNMAPPED cusip (no ticker, excluded from ticker "
             "joins, never dropped): {} rows; CUSIP map failure {}/{} ({:.1f}%).".format(
                 nullticker_13f, unmapped_cusip, total_cusip,
                 100.0 * unmapped_cusip / total_cusip if total_cusip else 0))
    m.append("- 13F filer set is the 6 Mando-confirmed CIKs ONLY — not all managers.")
    m.append("- Form 4 corpus covers ONLY backfilled issuers (overlay + registry + "
             "trump_network); an insider buy on any other issuer is invisible here.")
    m.append("- Cross-surface ticker mismatch (renames, foreign/OTC suffixes, share "
             "classes) can hide a real overlap. Joins are by ticker string, not CUSIP/CIK.")
    m.append("- SMID banding BLOCKED-ON-METHOD (above).")
    m.append("")

    m.append("## Standing warnings (verbatim)")
    m.append("")
    for w in STANDING_WARNINGS:
        m.append("- {}".format(w))
    m.append("")
    return "\n".join(m) + "\n"


if __name__ == "__main__":
    sys.exit(main())
