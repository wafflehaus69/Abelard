"""SM-R1 price coverage backfill.

Fills EOD price series for every traded (Form 4 P/S) ticker so the Ticker page and
the trades feed have entry/latest closes across the WHOLE scraped universe, not just
the ~400 the nightly enrichment (scan.leg_enrich) covers. A thin, paced orchestration
over prices.eod, which is cache-first and span-cached: re-running is idempotent and
skips already-covered spans, so an interrupted run resumes cheaply. Ingest-only —
writes the prices / price_spans tables, never events.

Historical closes are immutable, so once a (ticker, date) close is fetched it is
cached permanently; the latest close is what the nightly refresh keeps current.

  python -m smart_money.price_backfill                 # whole universe, full history
  python -m smart_money.price_backfill --only-missing  # just zero-coverage tickers
  python -m smart_money.price_backfill --since 2025-01-01 --floor-days 800
"""
import argparse
import datetime as dt
import sys
import time

from . import db as dbmod
from . import prices

_NON = {"NONE", "N/A", "N.A.", "NA", ""}


def _ok_symbol(t):
    """Yahoo-safe symbol filter — a ticker with a slash or space is not fetchable."""
    t = (t or "").strip()
    return t if t and t not in _NON and "/" not in t and " " not in t else None


def targets(con, since=None, only_missing=False):
    """[(ticker, earliest_date)] for tickers we must be able to price, earliest first.

    TWO populations, because two different readers depend on prices:
      * Form 4 P/S tickers — the trades feed and Ticker page entry/latest closes.
      * 13F holdings tickers — the G1 unit-scale anchor. `_filer_unit_scale` decides
        dollars-vs-thousands by comparing implied price (value/shares) to the EOD close,
        so a filer whose holdings carry NO price rows cannot be anchored at all. The
        13F population was formerly absent here, which left the anchor dependent on a
        13F name coinciding with a Form 4 name: Affinity (one position, QXO, never
        insider-traded) came back `undetermined`, and 314 of 1,075 13F tickers had zero
        coverage. A filer's unit is not allowed to rest on that coincidence.

    `since` bounds to tickers active on/after that ISO date; `only_missing` keeps just
    tickers that have zero EOD price rows today."""
    since = since or "0001-01-01"
    priced = set()
    if only_missing:
        priced = {r[0] for r in con.execute(
            "SELECT DISTINCT ticker FROM prices WHERE price_type='eod'")}
    rows = list(con.execute(
        "SELECT UPPER(ticker), MIN(substr(tx_date,1,10)) FROM form4_transactions "
        "WHERE code IN ('P','S') AND ticker IS NOT NULL "
        "AND substr(tx_date,1,10)>=? GROUP BY UPPER(ticker)", (since,)))
    rows += list(con.execute(
        "SELECT UPPER(ticker), MIN(period) FROM thirteenf_holdings "
        "WHERE ticker IS NOT NULL AND period IS NOT NULL AND period>=? "
        "GROUP BY UPPER(ticker)", (since,)))
    earliest = {}
    for t, e in rows:
        t = _ok_symbol(t)
        if not t or not e:
            continue
        if only_missing and t in priced:
            continue
        # A ticker in BOTH populations takes the EARLIER date: fetching from the later
        # one would leave the older 13F period without the close its anchor needs.
        if t not in earliest or e < earliest[t]:
            earliest[t] = e
    return sorted(earliest.items(), key=lambda x: x[1])


def run(con, since=None, floor_days=None, only_missing=False, limit=None,
        progress_every=25, out=sys.stdout):
    """Fetch each target's EOD series from its earliest trade (or the floor) to today.
    A per-ticker failure of ANY kind is counted and skipped, never fatal — delisted /
    renamed / invalid-symbol tickers are expected across a multi-thousand-ticker run.
    Returns {total, ok, fail}."""
    end = dt.date.today().isoformat()
    floor = ((dt.date.today() - dt.timedelta(days=floor_days)).isoformat()
             if floor_days else None)
    tgts = targets(con, since=since, only_missing=only_missing)
    if limit:
        tgts = tgts[:limit]
    total, ok, fail, fails = len(tgts), 0, 0, []
    t0 = time.time()
    for i, (tk, earliest) in enumerate(tgts, 1):
        start = max(earliest, floor) if floor else earliest
        if start > end:
            start = end
        try:
            prices.eod(con, tk, start, end)
            ok += 1
        except Exception as exc:   # noqa: BLE001 - one weird ticker must not kill a
            fail += 1              # multi-thousand-ticker bulk run; count and move on
            if len(fails) < 50:
                fails.append("{} {}: {}".format(tk, type(exc).__name__, str(exc)[:100]))
        if i % progress_every == 0 or i == total:
            out.write("[price_backfill] {}/{} ok={} fail={} elapsed={}s\n".format(
                i, total, ok, fail, int(time.time() - t0)))
            out.flush()
    out.write("[price_backfill] DONE total={} ok={} fail={} in {}s\n".format(
        total, ok, fail, int(time.time() - t0)))
    if fails:
        out.write("[price_backfill] sample failures:\n  " + "\n  ".join(fails) + "\n")
    out.flush()
    return {"total": total, "ok": ok, "fail": fail}


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM price coverage backfill")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--since", help="only tickers with a P/S trade on/after this ISO date")
    ap.add_argument("--floor-days", type=int,
                    help="do not fetch earlier than N days before today (bounds history)")
    ap.add_argument("--only-missing", action="store_true",
                    help="only tickers with zero existing EOD price rows")
    ap.add_argument("--limit", type=int, help="cap ticker count (for chunked runs)")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    # The nightly scan is the other writer; WAL allows concurrent reads but only one
    # writer, so wait out a lock collision (per-ticker commit) rather than erroring.
    con.execute("PRAGMA busy_timeout=30000")
    try:
        res = run(con, since=args.since, floor_days=args.floor_days,
                  only_missing=args.only_missing, limit=args.limit)
    finally:
        con.close()
    # A total wipeout (nothing succeeded despite targets) signals a systemic failure;
    # per-ticker failures are expected and non-fatal.
    return 1 if res["total"] and not res["ok"] else 0


if __name__ == "__main__":
    sys.exit(main())
