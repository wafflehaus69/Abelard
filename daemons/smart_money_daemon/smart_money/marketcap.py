"""SM-A1-fix SMID scan. Market-cap via SEC companyconcept (keyless, same EDGAR
client) x our price layer, banded micro/small/mid/large. Mando-ratified method.

shares = dei:EntityCommonStockSharesOutstanding, falling back to us-gaap common
shares concepts. Multi-class names (META, MSTR) that report shares per-class
resolve to UNBANDABLE — reported in coverage, never guessed. cap = shares x
latest price; both as-of dates recorded (stale cap on a volatile small cap is a
labeled error source).

Bands (Mando): micro < $300M, small $300M-$2B, mid $2B-$10B, large >= $10B.
"""
import argparse
import sys
import time

import requests

from . import db as dbmod
from . import form4
from . import prices
from .efd_ingest import load_env

CONCEPTS = [
    "dei/EntityCommonStockSharesOutstanding",
    "us-gaap/CommonStockSharesOutstanding",
    "us-gaap/CommonStockSharesIssued",
]
CONCEPT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/{concept}.json"
PACE = 0.15
MICRO, SMALL, MID = 300e6, 2e9, 10e9


def band_cap(cap):
    if cap is None:
        return "unbandable"
    if cap < MICRO:
        return "micro"
    if cap < SMALL:
        return "small"
    if cap < MID:
        return "mid"
    return "large"


def resolve_shares(cik10, contact):
    """(shares, concept, asof) from the first concept that resolves, else None."""
    ua = {"User-Agent": form4.UA_TMPL.format(contact)}
    for concept in CONCEPTS:
        time.sleep(PACE)
        r = requests.get(CONCEPT_URL.format(cik10=cik10, concept=concept),
                         headers=ua, timeout=30)
        if r.status_code != 200:
            continue
        try:
            units = r.json()["units"]["shares"]
        except (ValueError, KeyError):
            continue
        if not units:
            continue
        last = sorted(units, key=lambda x: x["end"])[-1]
        return last["val"], concept.split("/")[1], last["end"]
    return None


def compute(con, tickers, contact):
    """Band each ticker. Cache in market_cap. Returns coverage stats."""
    have = {r[0] for r in con.execute("SELECT ticker FROM market_cap")}
    todo = sorted({t.upper() for t in tickers if t and t.upper() not in have})
    tk_cik = form4.ticker_to_cik(contact, todo) if todo else {}
    stats = {"requested": len(todo), "banded": 0, "unbandable_no_cik": 0,
             "unbandable_no_shares": 0, "unbandable_no_price": 0}
    for t in todo:
        cik = tk_cik.get(t)
        if not cik:
            _store(con, t, None, None, None, None, None, None, "unbandable")
            stats["unbandable_no_cik"] += 1
            continue
        sh = resolve_shares(cik, contact)
        if not sh:
            _store(con, t, cik, None, None, None, None, None, "unbandable")
            stats["unbandable_no_shares"] += 1
            continue
        shares, concept, asof = sh
        try:
            price, price_asof = prices.latest(con, t)
        except prices.PriceError:
            _store(con, t, cik, shares, asof, concept, None, None, "unbandable")
            stats["unbandable_no_price"] += 1
            continue
        cap = shares * price
        band = band_cap(cap)
        _store(con, t, cik, shares, asof, concept, price, price_asof, band, cap)
        stats["banded"] += 1
    return stats


def _store(con, ticker, cik, shares, asof, concept, price, price_asof, band, cap=None):
    con.execute(
        "INSERT OR REPLACE INTO market_cap(ticker, cik, shares, shares_asof, "
        "concept, price, price_asof_unix, cap, band, computed_at_unix) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ticker, cik, shares, asof, concept, price, price_asof, cap, band,
         int(time.time())))
    con.commit()


def bands_for(con, tickers):
    """{ticker: band} from cache (uppercased)."""
    out = {}
    for t in {x.upper() for x in tickers if x}:
        r = con.execute("SELECT band FROM market_cap WHERE ticker=?", (t,)).fetchone()
        if r:
            out[t] = r[0]
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="SMID market-cap banding")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--tickers", help="comma-separated; default = all 13F+form4 tickers")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",")]
    else:
        tickers = [r[0] for r in con.execute(
            "SELECT DISTINCT ticker FROM thirteenf_holdings WHERE ticker IS NOT NULL "
            "UNION SELECT DISTINCT ticker FROM form4_transactions WHERE ticker IS NOT NULL")]
    stats = compute(con, tickers, contact)
    print("[marketcap] {}".format(stats))
    dist = con.execute("SELECT band, COUNT(*) FROM market_cap GROUP BY band").fetchall()
    print("[marketcap] band distribution: {}".format(dict(dist)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
