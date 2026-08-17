"""Populate thirteenf_holdings.instrument_class from stored evidence.

Runs offline — no network. Reads put_call, cusip, ticker, shares_type,
title_of_class from the holdings row and security_type from the cusip_ticker
cache, and asks smart_money.instrument for the class.

Writes nothing without --apply.
"""
import argparse
import sys

from . import db as dbmod
from . import instrument


def plan(con):
    """[(cik, accession, cusip, put_call, old, new)] for rows whose class changes."""
    sec = {r[0]: r[1] for r in con.execute(
        "SELECT cusip, security_type FROM cusip_ticker")}
    out = []
    for cik, acc, cusip, pc, tk, stype, title, cur in con.execute(
            "SELECT cik, accession, cusip, put_call, ticker, shares_type, "
            "title_of_class, instrument_class FROM thirteenf_holdings"):
        new = instrument.classify(
            put_call=pc, cusip=cusip, ticker=tk,
            security_type=sec.get(cusip), title_of_class=title, shares_type=stype)
        if new != cur:
            out.append((cik, acc, cusip, pc, cur, new))
    return out


def apply(con, rows):
    for cik, acc, cusip, pc, _old, new in rows:
        con.execute(
            "UPDATE thirteenf_holdings SET instrument_class=? WHERE cik=? AND "
            "accession=? AND cusip=? AND put_call=?", (new, cik, acc, cusip, pc))
    con.commit()
    return len(rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Classify 13F holdings by instrument")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    rows = plan(con)
    dist = {}
    for _c, _a, _cu, _p, _o, new in rows:
        dist[new] = dist.get(new, 0) + 1
    print("[classify] rows changing class: {}".format(len(rows)))
    for k in sorted(dist):
        print("    {:<24} {}".format(k, dist[k]))
    if not args.apply:
        print("[classify] DRY RUN, nothing written. Re-run with --apply.")
        return 0
    n = apply(con, rows)
    print("[classify] rows updated: {}".format(n))
    tot = {}
    for cls, c, v in con.execute(
            "SELECT COALESCE(instrument_class,'(null)'), COUNT(*), "
            "COALESCE(SUM(value_usd),0) FROM thirteenf_holdings GROUP BY 1 "
            "ORDER BY 3 DESC"):
        tot[cls] = (c, v)
    print("[classify] corpus distribution:")
    for k, (c, v) in tot.items():
        print("    {:<24} rows={:<6} ${:,}".format(k, c, int(v)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
