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
def join_a_multi_principal(con):
    rows = con.execute(
        "SELECT ticker, period, COUNT(DISTINCT cik) AS n, GROUP_CONCAT(DISTINCT cik) "
        "FROM thirteenf_holdings WHERE ticker IS NOT NULL AND put_call='long' "
        "GROUP BY ticker, period HAVING n>=2 ORDER BY n DESC, ticker"
    ).fetchall()
    return [{"ticker": t, "period": p, "n_filers": n, "filer_ciks": c}
            for t, p, n, c in rows]


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
def join_d_new_positions(con):
    """Per filer, quarter-over-quarter adds / exits / material (>2x) size changes.
    Full-universe (SMID blocked)."""
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
                adds.append({"cik": cik, "period": cur, "ticker": tk, "value": cv[tk]})
            for tk in pv.keys() - cv.keys():
                exits.append({"cik": cik, "period": cur, "ticker": tk, "was_value": pv[tk]})
            for tk in cv.keys() & pv.keys():
                if pv[tk] > 0 and cv[tk] >= 2 * pv[tk]:
                    sizes.append({"cik": cik, "period": cur, "ticker": tk,
                                  "from": pv[tk], "to": cv[tk], "dir": "up_2x"})
                elif cv[tk] > 0 and pv[tk] >= 2 * cv[tk]:
                    sizes.append({"cik": cik, "period": cur, "ticker": tk,
                                  "from": pv[tk], "to": cv[tk], "dir": "down_2x"})
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


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-A1 Phase 4 overlap joins")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--now", default=dt.date.today().isoformat())
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    overlay = load_overlay()
    out = args.out or "scans/PHASE4_OVERLAP_{}.md".format(args.now.replace("-", ""))

    a = join_a_multi_principal(con)
    b = join_b_inst_x_insider(con, args.now)
    c = join_c_inst_x_congress(con)
    d = join_d_new_positions(con)
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
    m.append("## SMID banding — BLOCKED-ON-METHOD")
    m.append("")
    m.append("Market-cap method is Mando's decision (recon candidate: SEC "
             "companyfacts keyless). Not chosen at run time, so **(a) and (d) are "
             "reported FULL-UNIVERSE ONLY** and the SMID cut is marked blocked. No "
             "market-cap proxy is substituted. Proposed bands (pending): micro "
             "<$300M, small $300M-$2B, mid $2B-$10B.")
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
    m.append("## (a) Multi-principal convergence — 2+ tracked 13F filers, same period (full-universe)")
    m.append("")
    m.append("{} (ticker, period) pairs held by >=2 confirmed filers.".format(len(a)))
    m.append("")
    if a:
        m.append(md_table(pd.DataFrame([{**r, "overlay": _overlay_tag(overlay, r["ticker"])}
                                        for r in a[:60]])))
        if len(a) > 60:
            m.append("\n(showing 60 of {})".format(len(a)))
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
