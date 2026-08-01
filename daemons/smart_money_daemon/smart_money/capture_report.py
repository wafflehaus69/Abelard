"""SM-C3 Phase H gate: capture-rate report over the harvested congressional corpus.

Measures the same two bars SM-C1/SM-C2 were held to, now across every harvested coverage
year and both chambers:
  * ticker  >= 95% of TICKER-BEARING rows (equity-like asset types)
  * band    >= 90% of those same equity-like rows

DENOMINATORS ARE THE WHOLE POINT. A congressional FD lists real property, bank accounts,
LPs and funds that have no ticker by nature — counting those as ticker misses would
manufacture a failure.

The band denominator is equity-like rows for a specific, admitted reason: the RAW VALUE
TEXT IS NOT RETAINED in congress_holdings, so from the DB alone a legitimately value-less
row ("--" / "Unascertainable") is indistinguishable from a band that failed to parse.
Equity holdings essentially always report a band, so a miss there really is a parse miss.
The corpus-wide rate over ALL rows is printed alongside as a FLOOR and explicitly is not
the gate — quoting it as the capture rate would understate parsing, and quoting it as a
pass would overstate it. Rows outside the denominators are reported, never silently
dropped.

Read-only. No network. Distribution-first: per-chamber-per-year rates are printed before
any pass/fail so a single bad year cannot hide inside a healthy average.
"""
import argparse
import collections
import sys

from . import db as dbmod

# Calibration note: SM-C1/SM-C2 set the 95% bar against ST/OP/EF. Adding MF here was MY
# error and it silently changed what the bar means — House MF is 55.9% ticker-bearing
# because it is dominated by TIAA-CREF / 401(k) retirement SUB-ACCOUNTS ("TIAA
# Traditional", "Vanguard Target 2055") that have no ticker by nature, exactly like real
# property. MF is reported separately below, never folded into the gate.
TICKER_BEARING = ("ST", "OP", "EF")
REPORTED_NOT_GATED = ("MF",)
TICKER_BAR = 95.0
BAND_BAR = 90.0


def measure(con):
    """Per (chamber, coverage_year) capture stats plus corpus totals."""
    cells = collections.defaultdict(
        lambda: {"rows": 0, "tick_den": 0, "tick_hit": 0, "band_den": 0,
                 "band_hit": 0, "all_den": 0, "all_hit": 0, "no_type": 0})
    for cham, yr, atype, ticker, lo, hi in con.execute(
            "SELECT chamber, coverage_year, asset_type, ticker, value_lo, value_hi "
            "FROM congress_holdings"):
        c = cells[(cham, yr)]
        c["rows"] += 1
        if atype is None:
            c["no_type"] += 1
        has_band = lo is not None or hi is not None
        if atype in TICKER_BEARING:
            c["tick_den"] += 1
            if ticker:
                c["tick_hit"] += 1
            # Equity-like rows essentially always report a value band, so this is the
            # denominator where a miss actually means a PARSE miss.
            c["band_den"] += 1
            if has_band:
                c["band_hit"] += 1
        # Corpus-wide band rate, reported alongside as a FLOOR. It is not the gate: the
        # raw value text is not retained, so from the DB alone a legitimately value-less
        # row ("--" / "Unascertainable") is indistinguishable from a parse failure, and
        # non-equity rows carry those disproportionately. Quoting this as the capture
        # rate would understate parsing; quoting it as a pass would overstate it.
        c["all_den"] += 1
        if has_band:
            c["all_hit"] += 1
    return cells


def measure_anchor(con):
    """Capture over the ANCHOR POPULATION only: the rows in each member's most recent
    coverage year.

    Mando's ruling on the Phase H gate — the 95% bar applies to anchor years, with
    historical years documented as coverage floors. The anchor set is defined
    STRUCTURALLY rather than as a hardcoded year cutoff: Phase F's fusion anchors on each
    member's latest annual, so that is precisely the population whose capture governs a
    derived holding claim. It also stays correct on its own as new years land, instead of
    silently going stale the way a ">= 2025" rule would."""
    latest = {}
    for c, l, f, s, y in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, MAX(coverage_year) "
            "FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, member_last, member_first, state_dist"):
        latest[(c, l, f, s)] = y
    st = {"rows": 0, "tick_den": 0, "tick_hit": 0, "band_den": 0, "band_hit": 0,
          "members": len(latest), "years": set()}
    for c, l, f, s, y, atype, ticker, lo, hi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "asset_type, ticker, value_lo, value_hi FROM congress_holdings"):
        if latest.get((c, l, f, s)) != y:
            continue
        st["rows"] += 1
        st["years"].add(y)
        if atype in TICKER_BEARING:
            st["tick_den"] += 1
            if ticker:
                st["tick_hit"] += 1
            st["band_den"] += 1
            if lo is not None or hi is not None:
                st["band_hit"] += 1
    return st


def _pct(hit, den):
    return round(100.0 * hit / den, 1) if den else None


def render(cells, anchor=None):
    lines = ["CONGRESSIONAL CORPUS CAPTURE REPORT (SM-C3 Phase H gate)",
             "=" * 78,
             "%-7s %-6s %8s %10s %8s %10s %8s" % (
                 "chamber", "cov_yr", "rows", "tick_den", "tick%", "band_den", "band%")]
    tot = {"rows": 0, "tick_den": 0, "tick_hit": 0, "band_den": 0, "band_hit": 0,
           "all_den": 0, "all_hit": 0}
    for key in sorted(cells, key=lambda k: (k[0], k[1] or 0)):
        cham, yr = key
        c = cells[key]
        for k in tot:
            tot[k] += c[k]
        lines.append("%-7s %-6s %8d %10d %7s%% %10d %7s%%" % (
            cham, yr, c["rows"], c["tick_den"], _pct(c["tick_hit"], c["tick_den"]),
            c["band_den"], _pct(c["band_hit"], c["band_den"])))
    lines.append("-" * 78)
    tp = _pct(tot["tick_hit"], tot["tick_den"])
    bp = _pct(tot["band_hit"], tot["band_den"])
    lines.append("%-7s %-6s %8d %10d %7s%% %10d %7s%%" % (
        "ALL", "", tot["rows"], tot["tick_den"], tp, tot["band_den"], bp))
    lines.append("")
    lines.append("CORPUS-WIDE (all coverage years): ticker {}%  band {}% - reported as "
                 "COVERAGE FLOORS, not the gate. Historical years contain more filer-side "
                 "symbol omissions, which parsing cannot recover without guessing."
                 .format(tp, bp))
    if anchor is not None:
        atp = _pct(anchor["tick_hit"], anchor["tick_den"])
        abp = _pct(anchor["band_hit"], anchor["band_den"])
        yrs = sorted(anchor["years"])
        lines.append("")
        lines.append("=" * 78)
        lines.append("GATE POPULATION - ANCHOR ROWS ONLY (each member's LATEST coverage "
                     "year; {} members, years {}-{})".format(
                         anchor["members"], yrs[0] if yrs else "-", yrs[-1] if yrs else "-"))
        lines.append("  rows {}   ticker {}/{} = {}%   band {}/{} = {}%".format(
            anchor["rows"], anchor["tick_hit"], anchor["tick_den"], atp,
            anchor["band_hit"], anchor["band_den"], abp))
        lines.append("GATE ticker >= {}%: {}".format(
            TICKER_BAR, "PASS" if (atp or 0) >= TICKER_BAR else "FAIL"))
        lines.append("GATE band   >= {}%: {}".format(
            BAND_BAR, "PASS" if (abp or 0) >= BAND_BAR else "FAIL"))
        lines.append("Anchor rows are what Phase F derives holding claims FROM, so this is "
                     "the population the bar governs (Mando ruling, SM-C3 Phase H).")
        lines.append("=" * 78)
    lines.append("")
    lines.append("Corpus-wide band rate (ALL rows incl. non-equity): {}% - reported as a "
                 "FLOOR, NOT the gate. The raw value text is not retained, so from the DB "
                 "a legitimately value-less row (\"--\" / \"Unascertainable\") cannot be "
                 "told apart from a parse failure; non-equity rows carry those "
                 "disproportionately.".format(_pct(tot["all_hit"], tot["all_den"])))
    lines.append("")
    lines.append("Denominators: both gates count only equity-like rows ({}); a "
                 "congressional FD's real property, bank accounts and LPs have no ticker "
                 "by nature and are excluded rather than counted as misses.".format(
                     "/".join(TICKER_BEARING)))
    lines.append("Reported but NOT gated: {} - mutual-fund rows are dominated by "
                 "retirement sub-accounts with no ticker by nature; including them in the "
                 "denominator was a calibration error, not a parser finding.".format(
                     "/".join(REPORTED_NOT_GATED)))
    worst = [(k, _pct(c["tick_hit"], c["tick_den"])) for k, c in cells.items()
             if c["tick_den"] >= 50]
    worst = sorted([w for w in worst if w[1] is not None], key=lambda w: w[1])[:3]
    if worst:
        lines.append("Weakest cells (>=50 ticker-bearing rows): " + ", ".join(
            "{} {} {}%".format(k[0], k[1], v) for k, v in worst))
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="SM-C3 Phase H capture report")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    try:
        print(render(measure(con), measure_anchor(con)))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
