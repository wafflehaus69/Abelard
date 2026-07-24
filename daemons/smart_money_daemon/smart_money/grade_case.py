"""SM-W1: grade a single named case — forward excess vs SPY at 21/63/126
trading days from an entry date, using the existing price layer. One number,
real stakes: the first time the system is graded against a case brought TO it
rather than one it found. SM-U1 generalizes this harness over many cases.

Honest framing is mandatory: a single case is a data point, not evidence; and a
case brought by a human who already knows it worked is selection-biased.
"""
import argparse
import bisect
import datetime as dt
import sys

from . import db as dbmod
from . import prices

HORIZONS = (21, 63, 126)


def forward_excess(con, ticker, entry_iso, pad_end_days=200):
    end = (dt.date.fromisoformat(entry_iso) + dt.timedelta(days=pad_end_days)).isoformat()
    start = (dt.date.fromisoformat(entry_iso) - dt.timedelta(days=10)).isoformat()
    tk = {d: a for d, _, a, _ in prices.eod(con, ticker, start, end)}
    spy = {d: a for d, _, a, _ in prices.eod(con, "SPY", start, end)}
    days = sorted(set(tk) & set(spy))
    i = bisect.bisect_left(days, entry_iso)
    if i >= len(days):
        return None
    ed = days[i]
    out = {"entry_date": ed, "entry_px": tk[ed], "entry_spy": spy[ed], "horizons": {}}
    for h in HORIZONS:
        xi = i + h
        if xi >= len(days):
            out["horizons"][h] = None
            continue
        xd = days[xi]
        tr = tk[xd] / tk[ed] - 1
        sr = spy[xd] / spy[ed] - 1
        out["horizons"][h] = {"exit_date": xd, "ticker_ret": tr, "spy_ret": sr,
                              "excess": tr - sr}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-W1 grade a single case")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--ticker", default="WULF")
    ap.add_argument("--entry", default="2024-08-14",
                    help="entry date (default = Duquesne Q2-2024 13F filing date)")
    ap.add_argument("--source", default="Duquesne Family Office 13F, period 2024-06-30",
                    help="how the case surfaced, for the report header")
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    r = forward_excess(con, args.ticker, args.entry)
    if not r:
        print("FATAL insufficient price history", file=sys.stderr)
        return 2
    out = args.out or dbmod.artifact_path(
        "GRADE_{}_{}.md".format(args.ticker, args.entry.replace("-", "")), "scans")
    m = ["# GRADE — {} (SM-W1 single-case grading)".format(args.ticker), "",
         "Case source: {}. Entry {} (position public). Forward excess vs SPY, "
         "adjusted close, trading-day horizons.".format(args.source, r["entry_date"]),
         "", "| horizon | {} | SPY | excess |".format(args.ticker),
         "|---|---|---|---|"]
    for h in HORIZONS:
        hv = r["horizons"][h]
        if hv:
            m.append("| {}d ({}) | {:+.1%} | {:+.1%} | **{:+.1%}** |".format(
                h, hv["exit_date"], hv["ticker_ret"], hv["spy_ret"], hv["excess"]))
        else:
            m.append("| {}d | horizon past data | | |".format(h))
    m += ["", "## Standing caveats (mandatory)", "",
          "- **N=1.** A single case is a data point, not evidence of edge.",
          "- **Selection bias.** This case was brought by a human who already knew "
          "it worked; that is not the same as the system discovering it blind. "
          "SM-U1 generalizes this grading over ALL flagged cases to remove that bias.",
          "- Excess is price-only, no sizing/holding-period — copying selection "
          "without the sizing mechanism does not reproduce the result.",
          "- Public filings, standard information wall. No recommendation.", ""]
    import pathlib
    pathlib.Path(out).write_text("\n".join(m) + "\n")
    print("[grade] {} entry {} -> {}".format(args.ticker, r["entry_date"], out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
