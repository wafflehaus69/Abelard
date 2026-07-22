"""F3 survivorship annotation (ORDER SM-2). For every ticker that appears in a
purchase but has no usable Yahoo series, classify delisted_presumed vs data_gap
and cache the verdict so the scorecard stops slow-retrying dead tickers.

Heuristic (marked as heuristic in output): probe Yahoo v8 once for any
metadata. No data at all AND the ticker's last observed trade row is older than
24 months => delisted_presumed; otherwise data_gap. Probes are cached in
ticker_status; no retry storms. NEVER imputes returns for a missing series.
"""
import argparse
import datetime as dt
import sys
import time

from . import db as dbmod
from . import prices

HEURISTIC = "no_yahoo_data_and_last_trade_gt_24mo => delisted_presumed else data_gap"
RECENCY_ONLY = "recency_only_no_probe_PROVISIONAL"
STALE_MONTHS = 24


def tickers_without_series(con):
    """Tickers used in stock purchases that have no eod rows cached."""
    rows = con.execute(
        "SELECT DISTINCT ticker FROM congress_trades "
        "WHERE side='purchase' AND asset_type='Stock' AND ticker IS NOT NULL "
        "AND ticker NOT IN (SELECT DISTINCT ticker FROM prices WHERE price_type='eod')"
    ).fetchall()
    return [r[0] for r in rows]


def _last_trade(con, ticker):
    r = con.execute(
        "SELECT MAX(tx_date) FROM congress_trades WHERE ticker=?", (ticker,)
    ).fetchone()
    return r[0] if r and r[0] else None


def classify(con, today=None, probe=True) -> dict:
    today = today or dt.date.today().isoformat()
    stale_before = (
        dt.date.fromisoformat(today) - dt.timedelta(days=STALE_MONTHS * 30)
    ).isoformat()
    todo = tickers_without_series(con)
    # A real probe refines PROVISIONAL recency-only rows; a full probe result is
    # never re-probed.
    done = {
        r[0]
        for r in con.execute(
            "SELECT ticker FROM ticker_status WHERE heuristic != ?", (RECENCY_ONLY,)
        )
    }
    counts = {"delisted_presumed": 0, "data_gap": 0, "skipped_cached": 0}
    mark = RECENCY_ONLY if not probe else HEURISTIC
    for t in todo:
        if t in done:
            counts["skipped_cached"] += 1
            continue
        has_data = False
        if probe:
            try:
                # A quote probe is the cheapest metadata check.
                prices.latest(con, t)
                has_data = True
            except prices.PriceError:
                has_data = False
        last = _last_trade(con, t)
        stale = last is not None and last < stale_before
        verdict = "delisted_presumed" if (not has_data and stale) else "data_gap"
        con.execute(
            "INSERT OR REPLACE INTO ticker_status VALUES (?,?,?,?,?)",
            (t, verdict, last, int(time.time()), mark),
        )
        counts[verdict] += 1
    con.commit()
    return counts


def main(argv=None):
    ap = argparse.ArgumentParser(description="F3 survivorship classification")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--no-probe", action="store_true",
                    help="classify from trade recency only, no network")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    counts = classify(con, probe=not args.no_probe)
    print("[survivorship] {}".format(counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
