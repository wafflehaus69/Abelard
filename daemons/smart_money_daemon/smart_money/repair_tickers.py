"""Re-derive congress_holdings.ticker for rows written before the account-label fix.

The House extractor took the LAST symbol-shaped parenthetical in an asset name, so an
account suffix overwrote the real symbol: "Vanguard Mid-Cap Index Fund Admiral Shares
(VIMAX) ... Vanguard - 401(K)" was stored as K. Measured before the fix: 884 of 900 "K"
rows were 401(k) labels and only 16 were genuinely Kellogg/Kellanova, and the phantom
carried 45-49 holders a year into every breadth count.

The parser is fixed, but a re-ingest of 127k rows is not the cheapest correction: the
stored asset_name still carries the evidence, so the ticker can be re-derived in place.

WHAT THIS DOES AND DOES NOT DO. It only ever CLEARS or CORRECTS a ticker whose value the
current extractor would not produce from the same asset_name. It never invents one for a
row that has none — a row we could not read stays unreadable, which is the honest state
and the one the capture report already counts. Every change is reported by class, and
--dry-run is the default so the diff is inspected before it is applied.

  python -m smart_money.repair_tickers              # report only
  python -m smart_money.repair_tickers --apply      # write
"""
import argparse
import collections
import sys

from . import db as dbmod
from .house_fd_ingest import (_TICKER_RE, _BRACKET_TICKER_RE, _FD_TYPES,
                             NOT_TICKER, AMBIGUOUS_LABELS, PLAN_CODE)
import re


def rederive(asset_name):
    """The current extractor's verdict for a stored asset_name, or None."""
    name = PLAN_CODE.sub(" ", asset_name or "")
    ticker = None
    for cand in _TICKER_RE.findall(name):
        if (re.fullmatch(r"[A-Za-z]{1,5}[A-Za-z.\-]{0,3}", cand)
                and cand.upper() not in NOT_TICKER):
            ticker = cand.upper()
    if not ticker:
        for cand in _BRACKET_TICKER_RE.findall(name):
            if cand.upper() not in _FD_TYPES and cand.upper() not in NOT_TICKER:
                ticker = cand.upper()
    return ticker


def is_account_label(old, name):
    """Positive evidence that the STORED ticker is an account label rather than a symbol:
    either it is a denylisted word that is never a symbol, or it appears in the name glued
    to a digit ("401(K)", "403(b)", "457(b)", "401(a)"). Anything else is left alone."""
    if not old:
        return False
    if old.upper() in AMBIGUOUS_LABELS:      # decided by a human, not by this tool
        return False
    if old.upper() in NOT_TICKER:
        return True
    return bool(PLAN_CODE.search(name or "")
                and re.search(r"\d\s*\(\s*" + re.escape(old) + r"\s*\)",
                              name or "", re.I))


def ambiguous_rows(con):
    """Rows whose stored ticker is one of the words that is both an account label and a
    real symbol. Reported, never auto-changed."""
    q = ("SELECT ticker, COUNT(*) FROM congress_holdings WHERE chamber='house' AND "
         "UPPER(ticker) IN ({}) GROUP BY ticker ORDER BY 2 DESC".format(
             ",".join("?" * len(AMBIGUOUS_LABELS))))
    return list(con.execute(q, tuple(sorted(AMBIGUOUS_LABELS))))


def scan(con):
    """[(rowid, chamber, old, new, asset_name)] for rows whose stored ticker is
    DEMONSTRABLY an account label.

    A repair must require positive evidence of error, never absence of evidence of
    correctness. The first cut of this tool re-derived every House row from asset_name
    and cleared any mismatch — which would have destroyed VXF, VWO, VEA and VB, whose
    stored name ("VANGUARD FTSE DEVELOPED MARKETS  CHARLES SCHWAB BROKERAGE ACCOUNT [1]")
    simply does not carry the symbol. asset_name is stored CLEANED and truncated to 200
    chars, so it is not a faithful record of what the parser saw.

    HOUSE ONLY: a Senate ticker comes from the filing's own yahoo link, evidence the
    stored name never carried, so the same reasoning excludes that chamber entirely."""
    out = []
    for rid, cham, name, old in con.execute(
            "SELECT rowid, chamber, asset_name, ticker FROM congress_holdings "
            "WHERE chamber='house' AND ticker IS NOT NULL AND ticker!=''"):
        if not is_account_label(old, name):
            continue
        new = rederive(name)                 # may recover the real symbol, else NULL
        if (new or None) != (old or None):
            out.append((rid, cham, old, new, name))
    return out


def classify(old, new, name):
    if new is None:
        return "cleared_plan_label" if PLAN_CODE.search(name or "") else "cleared_word"
    return "recovered_real_symbol"


def report(rows, amb=(), limit=12):
    by = collections.Counter(classify(o, n, nm) for _r, _c, o, n, nm in rows)
    old_hist = collections.Counter(o for _r, _c, o, _n, _nm in rows)
    lines = ["SM-C3 TICKER REPAIR - House rows the fixed extractor disagrees with",
             "=" * 72,
             "rows to change: {}".format(len(rows)), ""]
    for k, v in by.most_common():
        lines.append("  %-22s %d" % (k, v))
    lines += ["", "most-affected stored tickers:"]
    for tk, n in old_hist.most_common(10):
        lines.append("  %-8s %d" % (tk, n))
    lines += ["", "sample:"]
    for _r, _c, o, n, nm in rows[:limit]:
        lines.append("  %-6s -> %-8s %s" % (o, n or "(cleared)", (nm or "")[:72]))
    if amb:
        lines += ["", "AMBIGUOUS - both an account label and a real symbol, NOT changed:"]
        for tk, n in amb:
            lines.append("  %-8s %d rows   (ETN=Eaton, CASH=Pathward, FUND=Sprott "
                         "Focus, NA=VineBrook, ASX=ASE)" % (tk, n))
    lines += ["",
              "Only rows with POSITIVE evidence of an account label are touched - the",
              "stored ticker is a denylisted account word, or appears glued to a digit",
              "as in 401(K). A row whose name merely fails to contain its ticker is left",
              "alone: asset_name is stored cleaned and truncated, so its silence is not",
              "evidence. Senate rows are excluded entirely - their ticker comes from the",
              "filing's own yahoo link, which the stored name never carried."]
    return "\n".join(lines)


def apply(con, rows):
    for rid, _c, _o, new, _nm in rows:
        con.execute("UPDATE congress_holdings SET ticker=? WHERE rowid=?", (new, rid))
    con.commit()
    return len(rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Re-derive congress tickers (account-label fix)")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--apply", action="store_true", help="write the changes")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    con.execute("PRAGMA busy_timeout=30000")
    try:
        rows = scan(con)
        print(report(rows, ambiguous_rows(con)))
        if args.apply:
            print("\napplied: {} rows".format(apply(con, rows)))
        else:
            print("\nDRY RUN - re-run with --apply to write")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
