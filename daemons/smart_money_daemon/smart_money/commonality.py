"""SM-A1 Phase 4(g) commonality counters. Scripts-only, zero LLM. Reports RAW
counts plus backing rows, NO composite scores (section 7 rule).

Three observable forms of cross-surface commonality:
  g1  ticker-level insider-buy convergence  (Form 4 surface, per ticker)
  g2  cross-issuer person counter           (Form 4 surface, per person)
  g3  congressional co-holding counter      (congress surface, per ticker)

STRUCTURAL NOTE (carried verbatim into the report): Form 4 insiders file only
on their own issuers, so cross-company common positions between corporate
insiders are structurally unobservable on that surface except via shared
persons (g2). Cross-person co-holding lives on the congressional and 13F
surfaces. These three counters are the three observable forms.

Windows are trailing calendar days on tx_date (the economic date, matching the
scan cluster-flag semantics), measured from the run-date anchor.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import sys

from . import db as dbmod
from .mdfmt import md_table
from .overlay import load_overlay

try:
    import pandas as pd
except ImportError:  # pandas is a hard dep, but keep the import explicit
    pd = None

G1_WINDOWS = (30, 90, 180)
G3_WINDOWS = (90, 180, 365)
G1_MIN_BUYERS = 2
G3_MIN_MEMBERS = 3


def _minus(anchor_iso, days):
    return (dt.date.fromisoformat(anchor_iso) - dt.timedelta(days=days)).isoformat()


def _overlay_flags(overlay, ticker):
    conv, watch = overlay.match(ticker)
    tags = []
    if conv:
        tags.append("conviction")
    if watch:
        tags.append("watchlist")
    return ",".join(tags) or "-"


# ---------------------------------------------------------------- g1
def g1_insider_convergence(con, anchor, overlay):
    src_rows = con.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0]
    out = {"source_rows": src_rows, "windows": {}}
    for w in G1_WINDOWS:
        start = _minus(anchor, w)
        rows = con.execute(
            "SELECT ticker, COUNT(DISTINCT reporting_cik) AS buyers, "
            "SUM(COALESCE(value,0)) AS tot_value "
            "FROM form4_transactions "
            "WHERE code='P' AND plan_flag=0 AND ticker IS NOT NULL "
            "AND tx_date>=? AND tx_date<=? "
            "GROUP BY ticker HAVING buyers>=? ORDER BY buyers DESC",
            (start, anchor, G1_MIN_BUYERS),
        ).fetchall()
        out["windows"][w] = [
            {"ticker": t, "distinct_buyers": b, "total_value": v,
             "overlay": _overlay_flags(overlay, t)}
            for t, b, v in rows
        ]
    return out


# ---------------------------------------------------------------- g2
def g2_cross_issuer_persons(con):
    src_rows = con.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0]
    # Count distinct issuer ENTITIES by issuer CIK — the stable identity. Ticker
    # or name inflates the count on renames (MicroStrategy->Strategy, FB->META
    # which failed to resolve in our own data) or case variants. Ticker is
    # display-only in the concatenated list.
    rows = con.execute(
        "SELECT reporting_cik, MAX(reporting_person), "
        "COUNT(DISTINCT issuer_cik) AS issuers, "
        "GROUP_CONCAT(DISTINCT COALESCE(ticker, issuer)), "
        "MAX(role), MIN(tx_date), MAX(tx_date) "
        "FROM form4_transactions WHERE reporting_cik IS NOT NULL "
        "AND issuer_cik IS NOT NULL "
        "GROUP BY reporting_cik HAVING issuers>=2 ORDER BY issuers DESC"
    ).fetchall()
    people = [
        {"cik": c, "person": p, "issuer_count": n, "issuers": iss,
         "role": role, "first_filing": lo, "last_filing": hi}
        for c, p, n, iss, role, lo, hi in rows
    ]
    return {"source_rows": src_rows, "people": people}


# ---------------------------------------------------------------- g3
def _registry_person_ids(path="analysis/registry.json"):
    p = pathlib.Path(path)
    if not p.exists():
        return set(), []
    d = json.loads(p.read_text())
    ids, names = set(), []
    for e in d.get("entries", []):
        if e.get("person_id") is not None:
            ids.add(int(e["person_id"]))
            names.append(e["name"])
    return ids, names


def g3_congress_coholding(con, anchor, overlay, person_filter=None):
    results = {}
    filt = ""
    params_extra = ()
    if person_filter:
        placeholders = ",".join("?" for _ in person_filter)
        filt = " AND ct.person_id IN ({})".format(placeholders)
        params_extra = tuple(person_filter)
    for w in G3_WINDOWS:
        start = _minus(anchor, w)
        rows = con.execute(
            "SELECT ticker, COUNT(DISTINCT person_id) AS members "
            "FROM congress_trades ct "
            "WHERE side='purchase' AND asset_type='Stock' AND ticker IS NOT NULL "
            "AND tx_date>=? AND tx_date<=?" + filt +
            " GROUP BY ticker HAVING members>=? ORDER BY members DESC",
            (start, anchor) + params_extra + (G3_MIN_MEMBERS,),
        ).fetchall()
        window_rows = []
        for ticker, members in rows:
            detail = con.execute(
                "SELECT p.name, ct.chamber, ct.amt_low, ct.amt_high "
                "FROM congress_trades ct JOIN persons p USING(person_id) "
                "WHERE ct.ticker=? AND ct.side='purchase' AND ct.asset_type='Stock' "
                "AND ct.tx_date>=? AND ct.tx_date<=?" + filt +
                " ORDER BY p.name",
                (ticker, start, anchor) + params_extra,
            ).fetchall()
            # Group per distinct member so the list length equals member_count;
            # collect each member's distinct amount bands (all sides are purchase).
            by_person = {}
            for nm, ch, lo, hi in detail:
                band = "{}-{}".format(lo, hi if hi else "open")
                by_person.setdefault((nm, ch), set()).add(band)
            members_list = "; ".join(
                "{} ({}, {})".format(nm, ch, "/".join(sorted(bands)))
                for (nm, ch), bands in sorted(by_person.items())
            )
            window_rows.append({
                "ticker": ticker, "member_count": members,
                "overlay": _overlay_flags(overlay, ticker),
                "members": members_list,
            })
        results[w] = window_rows
    return results


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-A1 commonality counters")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--out", default="analysis/COMMONALITY_COUNTERS.md")
    ap.add_argument("--anchor", default=dt.date.today().isoformat())
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    overlay = load_overlay()

    anchor = args.anchor
    g1 = g1_insider_convergence(con, anchor, overlay)
    g2 = g2_cross_issuer_persons(con)
    reg_ids, reg_names = _registry_person_ids()
    g3_all = g3_congress_coholding(con, anchor, overlay)
    g3_reg = g3_congress_coholding(con, anchor, overlay, person_filter=reg_ids) \
        if reg_ids else {w: [] for w in G3_WINDOWS}

    # third cut: intersection of g3 (any window) with g1 (any window)
    g1_tickers = {r["ticker"] for w in G1_WINDOWS for r in g1["windows"][w]}
    g3_tickers = {r["ticker"] for w in G3_WINDOWS for r in g3_all[w]}
    intersection = sorted(g1_tickers & g3_tickers)

    md = _render(anchor, g1, g2, g3_all, g3_reg, reg_names, intersection)
    pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(args.out).write_text(md)
    print("[commonality] anchor={} f4_rows={} congress_g3_tickers={} "
          "registry_persons={} intersection={} -> {}".format(
              anchor, g1["source_rows"],
              sum(len(g3_all[w]) for w in G3_WINDOWS), len(reg_ids),
              len(intersection), args.out))
    return 0


def _render(anchor, g1, g2, g3_all, g3_reg, reg_names, intersection):
    m = []
    m.append("# COMMONALITY_COUNTERS — smart_money_daemon SM-A1 Phase 4(g)")
    m.append("")
    m.append("Anchor date {}. Windows are trailing calendar days on tx_date. "
             "Raw counts and backing rows only, no composite scores.".format(anchor))
    m.append("")
    m.append("## STRUCTURAL NOTE (verbatim)")
    m.append("")
    m.append("Form 4 insiders file only on their own issuers, so cross-company "
             "common positions between corporate insiders is structurally "
             "unobservable on that surface except via shared persons (g2). "
             "Cross-person co-holding lives on the congressional and 13F "
             "surfaces. The three counters here are the three observable forms.")
    m.append("")
    m.append("## SCOPE FINDING — Form 4 surface (READ THIS FIRST)")
    m.append("")
    f4 = g1["source_rows"]
    if f4 == 0:
        m.append("**form4_transactions holds 0 rows.** No Form 4 corpus yet — g1 "
                 "and g2 are code-complete but return zero because the corpus is "
                 "empty, NOT because convergence was tested and found rare. "
                 "Populate via the SM-F4 backfill.")
    else:
        m.append("**form4_transactions holds {} rows** (SM-F4 backfill: overlay + "
                 "registry + trump_network issuers, 36-month depth). g1/g2 below "
                 "are computed over this real corpus. COVERAGE-LIMITED: the corpus "
                 "spans only the backfilled issuer set, not all of EDGAR — a "
                 "counter is only as universal as its ingest. Convergence not seen "
                 "for an out-of-scope issuer means that issuer was not ingested, "
                 "not that no insiders bought it.".format(f4))
    m.append("")

    # g1
    m.append("## g1 — ticker-level insider-buy convergence (discretionary open-market P)")
    m.append("")
    m.append("Distinct reporting persons with code P, plan_flag false, per ticker, "
             "trailing 30/90/180d. Threshold >= {} buyers.".format(G1_MIN_BUYERS))
    for w in G1_WINDOWS:
        rows = g1["windows"][w]
        m.append("")
        m.append("### {}d window".format(w))
        if not rows:
            m.append("0 tickers with >= {} discretionary open-market buyers in "
                     "this window.".format(G1_MIN_BUYERS))
        else:
            m.append(md_table(pd.DataFrame(rows)))

    # g2
    m.append("")
    m.append("## g2 — cross-issuer person counter (multi-board operators)")
    m.append("")
    m.append("COVERAGE-LIMITED VIEW. Per reporting-person CIK, distinct issuer "
             "entities (by ticker) filed against, issuer_count >= 2. Only as "
             "universal as the backfilled issuer set. NOTE: corporate 10pct "
             "holders (e.g. a fund filing on its stakes) appear here as "
             "'persons' — that is a valid strategic-stake signal, not a "
             "mislabel to ignore.")
    m.append("")
    if not g2["people"]:
        m.append("0 multi-board operators at >= 2 distinct issuers.")
    else:
        m.append(md_table(pd.DataFrame(g2["people"])))

    # g3
    m.append("")
    m.append("## g3 — congressional co-holding counter (the surface with data)")
    m.append("")
    m.append("Distinct members with stock purchases per ticker, trailing "
             "90/180/365d, threshold >= {} members.".format(G3_MIN_MEMBERS))
    for w in G3_WINDOWS:
        rows = g3_all[w]
        m.append("")
        m.append("### {}d window — {} tickers".format(w, len(rows)))
        if not rows:
            m.append("0 tickers at >= {} members.".format(G3_MIN_MEMBERS))
        else:
            m.append(md_table(pd.DataFrame(
                [{k: r[k] for k in ("ticker", "member_count", "overlay", "members")}
                 for r in rows])))

    # g3 registry cut
    m.append("")
    m.append("## g3 second cut — REGISTRY MEMBERS ONLY (not pooled with universe)")
    m.append("")
    m.append("Same counter restricted to the {} registry members. Registry "
             "co-holding is a different-strength signal than universe "
             "co-holding.".format(len(reg_names)))
    any_reg = False
    for w in G3_WINDOWS:
        rows = g3_reg[w]
        m.append("")
        m.append("### {}d window — {} tickers".format(w, len(rows)))
        if not rows:
            m.append("0 tickers at >= {} registry members.".format(G3_MIN_MEMBERS))
        else:
            any_reg = True
            m.append(md_table(pd.DataFrame(
                [{k: r[k] for k in ("ticker", "member_count", "overlay", "members")}
                 for r in rows])))
    if not any_reg:
        m.append("")
        m.append("(No ticker reaches {} distinct registry buyers in any window — "
                 "expected given the registry is only {} people.)".format(
                     G3_MIN_MEMBERS, len(reg_names)))

    # third cut
    m.append("")
    m.append("## g3 x g1 intersection — congress co-buying AND multi-insider buying")
    m.append("")
    m.append("The rarest join in the dataset. Tickers appearing in both g1 and g3 "
             "in overlapping windows.")
    m.append("")
    g1_tk = sorted({r["ticker"] for w in G1_WINDOWS for r in g1["windows"][w]})
    if not intersection:
        if g1["source_rows"] == 0:
            m.append("**0 rows** — g1 is empty (no Form 4 corpus). Not a "
                     "convergence finding; populate the corpus first.")
        else:
            m.append("**0 rows — a REAL finding.** g1 fired on {} (multi-insider "
                     "buying) and g3 fired on {} congressional co-held tickers, but "
                     "the two sets do not overlap in any window. True cross-surface "
                     "convergence (congress co-buying AND multi-insider buying the "
                     "same name at once) did not occur in the ingested scope — a "
                     "genuine measure of how rare it is, subject to the "
                     "coverage-limit above (g1 only sees backfilled issuers).".format(
                         ", ".join(g1_tk) or "no tickers",
                         sum(len(g3_all[w]) for w in G3_WINDOWS)))
    else:
        m.append("Tickers: {}".format(" ".join(intersection)))
    m.append("")
    return "\n".join(m) + "\n"


if __name__ == "__main__":
    sys.exit(main())
