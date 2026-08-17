"""Re-resolve CUSIP -> ticker mappings that the old data[0] pick got wrong.

map_cusips only ever queried CUSIPs it had never seen, so every mapping written
before the US-composite fix is permanent, right or wrong. Three known classes:

  * NON-US LISTINGS. OpenFIGI returns every listing worldwide, unordered, and the
    old code took data[0]. Insmed's CUSIP 457669307 stored as IM8N (Frankfurt)
    when exactly one of its 109 records is exchCode=US, INSM. Somnigroup stored as
    TPD, Baidu as BIDUN. Those three alone are $1.59B of 13F positioning that
    joins to no price row and no Form 4 — silently, returning nothing rather than
    erroring.
  * ERROR-POISONED NULLS. On a failed request the old code wrote a NULL-ticker row
    tagged 'openfigi', identical to a genuine miss, and never retried it.
  * LEGACY ROWS generally: written before exch_code was recorded, so their venue
    is unknown without asking again.

Repairing cusip_ticker is not enough on its own — thirteenf_holdings.ticker was
stamped from the cache at ingest, so the holdings carry the wrong symbol too. This
propagates to both, and reports the dollar value moving before it moves it.

Writes nothing without --apply.
"""
import argparse
import sys
import time

import requests

from . import db as dbmod
from . import thirteenf
from .thirteenf_ingest import FIGI_BATCH, FIGI_PACE, OPENFIGI, pick_listing


def targets(con, mode="suspect"):
    """CUSIPs worth re-asking about.

    'suspect'  — legacy rows, known-foreign picks, and NULL tickers.
    'all'      — every cached CUSIP.
    """
    if mode == "all":
        return [r[0] for r in con.execute(
            "SELECT cusip FROM cusip_ticker ORDER BY cusip")]
    return [r[0] for r in con.execute(
        "SELECT cusip FROM cusip_ticker WHERE "
        # legacy: written before provenance was recorded
        "mapped_via = 'openfigi' "
        # a pick we already know came from a non-US venue
        "OR mapped_via = 'openfigi_foreign' "
        # NULL could be a real miss or a swallowed network error; indistinguishable,
        # so re-ask. A genuine miss simply comes back a miss.
        "OR ticker IS NULL "
        "ORDER BY cusip")]


def _query(cusips, contact):
    """{cusip: (ticker, name, exch, sector, sectype, raw, how)}; absent on failure."""
    out = {}
    for i in range(0, len(cusips), FIGI_BATCH):
        batch = cusips[i:i + FIGI_BATCH]
        jobs = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]
        time.sleep(FIGI_PACE)
        try:
            r = requests.post(OPENFIGI, json=jobs,
                              headers={"Content-Type": "application/json",
                                       "User-Agent": thirteenf.UA_TMPL.format(contact)},
                              timeout=30)
        except requests.RequestException:
            r = None
        if r is None or r.status_code != 200:
            continue                       # leave absent; a later run retries
        for cusip, res in zip(batch, r.json()):
            data = res.get("data") if isinstance(res, dict) else None
            rec, how = pick_listing(data)
            out[cusip] = (
                rec.get("ticker") if rec else None,
                rec.get("name") if rec else None,
                rec.get("exchCode") if rec else None,
                rec.get("marketSector") if rec else None,
                rec.get("securityType") if rec else None,
                data[0].get("ticker") if data else None,
                how)
    return out


def plan(con, contact, mode="suspect", limit=None):
    """[(cusip, old, new, how, rows, value_usd)] for mappings that would change."""
    cus = targets(con, mode)
    if limit:
        cus = cus[:limit]
    old = {r[0]: r[1] for r in con.execute(
        "SELECT cusip, ticker FROM cusip_ticker")}
    got = _query(cus, contact)
    changes = []
    for cusip, vals in sorted(got.items()):
        new = vals[0]
        if new == old.get(cusip):
            continue
        n, v = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(value_usd), 0) FROM thirteenf_holdings "
            "WHERE cusip=?", (cusip,)).fetchone()
        changes.append((cusip, old.get(cusip), new, vals[6], n, v))
    return changes, got, len(cus)


def is_safe(old_ticker, new_ticker, how):
    """Only a US-composite pick may overwrite an existing symbol.

    The dry run proved why. A non-US pick wanted to replace Electronic Arts' EA
    with EA*, a Bloomberg delisted-line marker. A miss wanted to replace AIR LEASE
    CORP's AL with NULL — and AL is already the correct ticker, so the "repair"
    would have destroyed a right answer on a $78M position. Neither is an
    improvement, and mark-never-drop forbids trading a real symbol for a blank.

    Anything else is REPORTED for a human ruling, never auto-acted.
    """
    if how != "openfigi_us" or not new_ticker:
        return False
    return new_ticker != old_ticker


def apply(con, got):
    """Write repaired mappings, then propagate to the holdings that used them.

    Returns (cache_written, holdings_retickered, flagged) where flagged are the
    changes withheld for review.
    """
    old = {r[0]: r[1] for r in con.execute("SELECT cusip, ticker FROM cusip_ticker")}
    cache = holdings = 0
    flagged = []
    for cusip, (tk, name, exch, sector, sectype, raw, how) in got.items():
        prev = old.get(cusip)
        if is_safe(prev, tk, how):
            con.execute(
                "INSERT OR REPLACE INTO cusip_ticker(cusip, ticker, name, "
                "mapped_via, mapped_at_unix, exch_code, market_sector, "
                "security_type, ticker_raw) VALUES (?,?,?,?,?,?,?,?,?)",
                (cusip, tk, name, how, int(time.time()), exch, sector, sectype, raw))
            cache += 1
            # thirteenf_holdings.ticker was stamped from the cache at ingest, so
            # the holdings keep the wrong symbol until this runs too.
            cur = con.execute(
                "UPDATE thirteenf_holdings SET ticker=? WHERE cusip=? AND "
                "(ticker IS NOT ?)", (tk, cusip, tk))
            holdings += cur.rowcount
            continue
        # Keep the existing symbol, but record that we looked and what we saw, so
        # it is not re-queried forever and a reviewer has the evidence.
        if tk != prev:
            flagged.append((cusip, prev, tk, how))
        con.execute(
            "UPDATE cusip_ticker SET mapped_via=?, mapped_at_unix=?, exch_code=?, "
            "market_sector=?, security_type=?, ticker_raw=? WHERE cusip=?",
            ("openfigi_checked_no_us", int(time.time()), exch, sector, sectype,
             raw, cusip))
    con.commit()
    return cache, holdings, flagged


def main(argv=None):
    ap = argparse.ArgumentParser(description="Re-resolve suspect CUSIP tickers")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--contact", default=None,
                    help="contact string for the SEC/OpenFIGI User-Agent")
    ap.add_argument("--mode", choices=("suspect", "all"), default="suspect")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(argv)

    contact = args.contact or "smart_money research"
    con = dbmod.connect(args.db)
    changes, got, n_asked = plan(con, contact, args.mode, args.limit)
    print("[cusip-repair] asked about {} cusips, answered {}, changed {}".format(
        n_asked, len(got), len(changes)))
    tot_rows = sum(c[4] for c in changes)
    tot_val = sum(c[5] for c in changes)
    print("[cusip-repair] holdings rows affected {}  value_usd ${:,}".format(
        tot_rows, int(tot_val)))
    safe = [c for c in changes if is_safe(c[1], c[2], c[3])]
    held = [c for c in changes if not is_safe(c[1], c[2], c[3])]
    print("[cusip-repair] would APPLY {} (US composite), WITHHOLD {} for review"
          .format(len(safe), len(held)))
    print("[cusip-repair] applied value_usd ${:,}".format(
        int(sum(c[5] for c in safe))))
    for cusip, old, new, how, rows, val in sorted(safe, key=lambda c: -c[5])[:40]:
        print("    APPLY    {:<11} {:<22} -> {:<12} rows={:<4} ${:,}".format(
            cusip, str(old), str(new), rows, int(val)))
    for cusip, old, new, how, rows, val in sorted(held, key=lambda c: -c[5]):
        print("    WITHHELD {:<11} keeps {:<10} (offered {:<12} {:<18}) "
              "rows={:<4} ${:,}".format(
                  cusip, str(old), str(new), how, rows, int(val)))
    if not args.apply:
        print("[cusip-repair] DRY RUN, nothing written. Re-run with --apply.")
        return 0
    cache, holdings, flagged = apply(con, got)
    print("[cusip-repair] cache rows written {}  holdings rows retickered {}".format(
        cache, holdings))
    print("[cusip-repair] withheld for review: {}".format(len(flagged)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
