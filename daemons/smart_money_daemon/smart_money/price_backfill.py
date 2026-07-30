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


def targets(con, since=None, only_missing=False):
    """[(ticker, earliest_trade_date)] for P/S tickers, earliest-trade ascending.
    `since` bounds to tickers with a trade on/after that ISO date; `only_missing`
    keeps just tickers that have zero EOD price rows today."""
    since = since or "0001-01-01"
    priced = set()
    if only_missing:
        priced = {r[0] for r in con.execute(
            "SELECT DISTINCT ticker FROM prices WHERE price_type='eod'")}
    out = []
    for t, e in con.execute(
            "SELECT UPPER(ticker), MIN(substr(tx_date,1,10)) FROM form4_transactions "
            "WHERE code IN ('P','S') AND ticker IS NOT NULL "
            "AND substr(tx_date,1,10)>=? GROUP BY UPPER(ticker) ORDER BY 2", (since,)):
        t = (t or "").strip()
        if not t or t in _NON or not e or "/" in t or " " in t:  # invalid Yahoo symbols
            continue
        if only_missing and t in priced:
            continue
        out.append((t, e))
    return out


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
