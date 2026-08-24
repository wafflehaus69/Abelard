"""CD-3b — supplier dead-band measurement. Report-only; sets no constant.

Ordered by Mando 2026-08-21: *"Same P1 methodology exactly — distribution of
|Δ TTM YoY| per supplier series, recent window, p25 proposal, hold for my
ratification."* Suppliers carry no phase state until ratified bands exist, and
that refusal is correct behaviour meanwhile.

Everything measured here is imported from `tools/measure_deadband.py` — the same
`yoy_from`, `deltas`, `pct` and `line`. This file supplies only the SERIES, not
the statistics, so a supplier band and an issuer band cannot drift apart by
being computed two different ways.

The one thing that differs is the input. P1 reads capex out of companyfacts;
the supplier series is DATACENTER REVENUE, which is dimension-qualified and
therefore exists only inside the filing (E6). It is read from the parsed-fact
cache in SQLite, which the nightly scan already maintains.

This measures `dcrev:supplier` ONLY. The companion class `issuer:supplier` bands
a supplier's own CAPITAL SPENDING and is measured by `tools/measure_deadband.py`
with the rest of the per-issuer classes — the two are different series and must
never be measured here and applied there (CD-3-VERIFY 9.3).

Re-runnable. Run it again when the panel gains filed quarters.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from capex_daemon import config, storage, suppliers, universe  # noqa: E402
from measure_deadband import RECENT_FROM, deltas, line, pct, yoy_from  # noqa: E402


def supplier_series(con, roster):
    """{ticker: {calendar_quarter: (value, period_end)}} from the fact cache."""
    out, status = {}, {}
    for _cik, e in sorted(roster.items(), key=lambda kv: kv[1].ticker_display):
        if e.bucket != suppliers.SUPPLIER_BUCKET:
            continue
        leg = suppliers.leg_from_db(e, con)
        status[e.ticker_display] = leg
        if not leg.is_covered:
            continue
        # yoy_from wants (value, period_end); the calendar quarter's own label
        # is the period marker the recent-window filter needs.
        out[e.ticker_display] = {q: (v, _end_of(q)) for q, v in leg.quarters.items()}
    return out, status


def _end_of(cq):
    y, n = cq.split("Q")
    return "{}-{:02d}-{}".format(y, int(n) * 3, {1: "31", 2: "30", 3: "30", 4: "31"}[int(n)])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None)
    ap.add_argument("--since", default=RECENT_FROM,
                    help="recent-window start (default matches P1)")
    args = ap.parse_args(argv)

    con = storage.connect(args.db or config.DB_PATH_DEFAULT)
    roster = universe.load()
    series, status = supplier_series(con, roster)

    print("=" * 100)
    print("CD-3b — SUPPLIER DEAD-BAND MEASUREMENT (report-only; nothing applied)")
    print("Methodology: identical to P1. |Δ TTM YoY| per series, recent window "
          "from {}, band = p25.".format(args.since))
    print("=" * 100)
    print()

    print("SERIES AVAILABLE")
    for tick, leg in sorted(status.items()):
        n_q = len(leg.quarters)
        y = yoy_from(series[tick]) if tick in series else {}
        note = ""
        if leg.is_mapped:
            note = "  [MAPPED-BUSINESS-UNITS, ruled {}]".format(
                (leg.mapping or {}).get("ruled"))
        print("  {:<6} {:<24} quarters={:<3} TTM-YoY points={}{}".format(
            tick, leg.status, n_q, len(y), note))
    print()

    pool, per_issuer = [], {}
    for tick, qmap in sorted(series.items()):
        y = yoy_from(qmap)
        d_all = [v for _q, v in deltas(y)]
        d_recent = [v for _q, v in deltas(y, args.since)]
        per_issuer[tick] = d_recent
        pool += d_recent
        print(line("{} full".format(tick), d_all))
        print(line("{} recent".format(tick), d_recent))
    print()
    print(line("SUPPLIER POOL recent", pool))
    print()

    print("-" * 100)
    print("PROPOSED DEAD-BAND (percentile logic stated; NOT applied)")
    a = sorted(abs(v) for v in pool)
    if not a:
        print("  dcrev:supplier       n=0 — INSUFFICIENT DATA TO PROPOSE")
    else:
        print("  {:<20} n={:<4} p25={:>7.1f}pp  ->  proposed band {:>6.1f}pp".format(
            "dcrev:supplier", len(a), pct(a, .25), round(pct(a, .25))))
    for tick, d in sorted(per_issuer.items()):
        b = sorted(abs(v) for v in d)
        print("    {:<8} n={:<4} p25={}".format(
            tick, len(b), "{:>7.1f}pp".format(pct(b, .25)) if b else "n/a"))
    print()
    print("  Logic: band = p25 of |Δ| in the recent window — the smallest quarter of")
    print("  observed moves is treated as noise and cannot trigger a direction change,")
    print("  while three quarters of real moves still register. Identical to P1.")
    print()
    print("  HELD FOR RATIFICATION. Suppliers publish no phase state until a band is")
    print("  ruled and stamped in config.DEAD_BANDS with its measurement date.")
    con.close()


if __name__ == "__main__":
    main()
