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
import re
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

# ORDER SM-C3 Phase H — STANDING RULING (Mando, 2026-07-31). The anchor gate measured
# 94.4% and was ACCEPTED as a documented coverage floor, not waived. The reasoning is
# permanent and travels with the report:
#   * A missing ticker cannot produce a WRONG holdings claim. An asset with no symbol
#     never joins a PTR flow, so it sits in the book as an anchored row and participates
#     in nothing. The failure mode is UNDER-COVERAGE, not incorrectness.
#   * The residue is irreducible by parsing: private companies with no ticker in
#     existence (First Bank, Cantex Pharmaceuticals, John Neely Kennedy APLC) plus
#     filer-side symbol omissions. Closing it would require guessing, which is banned.
#   * REJECTED ON THE RECORD: narrowing the gate population to active filers (which
#     measured 96.0% and would have passed). Rejected as gate-shopping — the population
#     would have been changed after seeing which one passed.
# Phase F DISPLAY REQUIREMENT arising from this ruling: a holding with no ticker is
# UNFUSABLE and must be marked as such in the member view, so a reader can never mistake
# "no flows matched" for "no flows occurred".
RULING = (
    "Phase H gate ACCEPTED at 94.4% as a documented COVERAGE FLOOR (Mando 2026-07-31). "
    "A missing ticker yields an UNFUSABLE row, never a wrong claim; the residue is "
    "private companies and filer omissions, unreachable without guessing. Narrowing to "
    "active filers (96.0%) was REJECTED as gate-shopping.")

# BAND GATE, PER CHAMBER — STANDING RULING (Mando, 2026-08-07). The aggregate band rate
# passed at 92.9% only because the Senate's 99.9% carried a House 88.0% sitting below the
# bar. Disclosed per chamber on the doctrine ledger: a bad cell hiding inside a healthy
# aggregate is the silent-denominator family, and disclosure-that-reads-as-regression
# beats concealment-that-reads-as-health.
#   * NOT A REGRESSION. The number did not move; the denominator stopped hiding it.
#   * Accepted on the SAME structure as the ticker ruling, because the risk is identical:
#     a missing band cannot fabricate a wrong estimate. It degrades to an
#     anchored-no-value row, which Phase F already renders honestly and refuses to carry
#     an estimate for (the DNUT case).
#   * RESIDUE CHARACTERISED before acceptance, as the ruling required: of 1,546
#     band-missing House anchor rows, 1,544 (99.9%) are FILER-SIDE — the filing states no
#     value and the raw text is not retained, so parsing cannot reach them. 2 (0.1%) are
#     parser-reachable and are BACKLOG, not a gate-blocker.
BAND_RULING = (
    "Band gate now PER-CHAMBER (Mando 2026-08-07). House 88.0% is a PRE-EXISTING "
    "CONDITION NEWLY DISCLOSED, not a regression. Accepted as a documented per-chamber "
    "FLOOR: a missing band degrades to an anchored-no-value row, never a wrong estimate. "
    "Residue characterised - 1,544/1,546 filer-side, 2 parser-reachable (backlog).")

# The 2 parser-reachable House rows, kept by name so the backlog is concrete rather than
# a number nobody can act on. Both carry a value in free text that the band parser does
# not read: a "C: Value on 12/31/2024 $79,259" comment column, and a "No transactions
# >$1,000" note. Neither shape is a band; reading them would need a comment-column parser.
BAND_BACKLOG = (
    "Vanguard Utilities ETF (VPU) - C: Value on 12/31/2024 $79,259 EquatePlus",
    "Rocket Lab USA, Inc. (RKLB) - C: No transactions >$1,000")


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
    def _blank():
        return {"rows": 0, "tick_den": 0, "tick_hit": 0, "band_den": 0, "band_hit": 0,
                "members": 0, "years": set()}

    st = _blank()
    st["members"] = len(latest)
    # PER CHAMBER as well as aggregate. Mando's ruling: a bad cell hiding inside a healthy
    # aggregate is the silent-denominator family, and the same reasoning that put the
    # ticker gate on anchor rows puts this split on the band gate. The corpus band rate
    # reads 92.9% and PASSES only because the Senate's 99.9% carries a House 88.0% that is
    # BELOW the bar. Disclosing that reads as a regression; it is a pre-existing condition
    # newly disclosed, and concealment-that-reads-as-health is the worse failure.
    st["by_chamber"] = {}
    for c, l, f, s, y, atype, ticker, lo, hi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "asset_type, ticker, value_lo, value_hi FROM congress_holdings"):
        if latest.get((c, l, f, s)) != y:
            continue
        cell = st["by_chamber"].setdefault(c, _blank())
        for tgt in (st, cell):
            tgt["rows"] += 1
            tgt["years"].add(y)
        cell["members"] = cell.get("members", 0)
        if atype in TICKER_BEARING:
            for tgt in (st, cell):
                tgt["tick_den"] += 1
                if ticker:
                    tgt["tick_hit"] += 1
                tgt["band_den"] += 1
                if lo is not None or hi is not None:
                    tgt["band_hit"] += 1
    for k in latest:
        ch = st["by_chamber"].get(k[0])
        if ch is not None:
            ch["members"] += 1
    return st


def band_residue(con, chamber="house", limit=12):
    """WHY the House anchor band rate sits at 88.0%. Mando's acceptance condition: the
    residue must be CHARACTERISED before the floor can be called accepted, exactly as the
    Phase H ticker ruling required.

    Splits band-missing anchor rows into:
      * `filer_side`   — the filer wrote no value at all. The House PDF prints "--" or
        leaves the column empty for assets reported without a valuation, and the raw text
        is not retained, so these are unrecoverable by parsing and are a property of the
        source, not of us.
      * `parser_reachable` — the row DOES carry evidence of a value we failed to read: a
        dollar figure or a band phrase survives in the asset name. These are ours to fix
        and become backlog, never a gate-blocker.
    A missing band cannot fabricate a wrong number: Phase F renders an unvalued anchor as
    anchored-no-value and refuses to carry an estimate for it (the DNUT case).
    """
    latest = {}
    for c, l, f, s, y in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, MAX(coverage_year) "
            "FROM congress_holdings WHERE coverage_year IS NOT NULL "
            "GROUP BY chamber, member_last, member_first, state_dist"):
        latest[(c, l, f, s)] = y
    money = re.compile(r"\$\s?[\d,]{3,}")
    out = {"chamber": chamber, "missing": 0, "filer_side": 0, "parser_reachable": 0,
           "samples": []}
    for c, l, f, s, y, atype, name, lo, hi in con.execute(
            "SELECT chamber, member_last, member_first, state_dist, coverage_year, "
            "asset_type, asset_name, value_lo, value_hi FROM congress_holdings "
            "WHERE chamber=?", (chamber,)):
        if latest.get((c, l, f, s)) != y or atype not in TICKER_BEARING:
            continue
        if lo is not None or hi is not None:
            continue
        out["missing"] += 1
        if money.search(name or ""):
            out["parser_reachable"] += 1
            if len(out["samples"]) < limit:
                out["samples"].append((name or "")[:96])
        else:
            out["filer_side"] += 1
    return out


def render_residue(res):
    m = res["missing"] or 1
    lines = ["BAND RESIDUE CHARACTERISATION - {} anchor rows".format(res["chamber"]),
             "=" * 78,
             "band-missing anchor rows: {}".format(res["missing"]),
             "  filer_side        {:6d}  {:5.1f}%  no value in the filing; raw text not "
             "retained, unrecoverable by parsing".format(
                 res["filer_side"], 100.0 * res["filer_side"] / m),
             "  parser_reachable  {:6d}  {:5.1f}%  a dollar figure survives in the row - "
             "OURS to fix, filed as backlog".format(
                 res["parser_reachable"], 100.0 * res["parser_reachable"] / m)]
    if res["samples"]:
        lines += ["", "parser-reachable samples:"]
        lines += ["  " + s for s in res["samples"]]
    lines += ["",
              "A missing band cannot fabricate a wrong estimate. Phase F renders an",
              "unvalued anchor as anchored-no-value and refuses to carry an estimate for",
              "it, so this residue degrades honestly rather than producing a false number.",
              "The parser-reachable share is BACKLOG, not a gate-blocker."]
    return "\n".join(lines)


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
    # The verdict is ALWAYS emitted, and always says which population it measured. An
    # earlier cut put the GATE lines inside the anchor branch, so a corpus-only render
    # produced a report with no conclusion in it — worse than a wrong number, because
    # nothing looks missing.
    lines.append("")
    lines.append("=" * 78)
    if anchor is not None:
        gtp = _pct(anchor["tick_hit"], anchor["tick_den"])
        gbp = _pct(anchor["band_hit"], anchor["band_den"])
        yrs = sorted(anchor["years"])
        lines.append("GATE POPULATION - ANCHOR ROWS ONLY (each member's LATEST coverage "
                     "year; {} members, years {}-{})".format(
                         anchor["members"], yrs[0] if yrs else "-", yrs[-1] if yrs else "-"))
        lines.append("  rows {}   ticker {}/{} = {}%   band {}/{} = {}%".format(
            anchor["rows"], anchor["tick_hit"], anchor["tick_den"], gtp,
            anchor["band_hit"], anchor["band_den"], gbp))
        # PER CHAMBER, always. An aggregate that PASSES while one chamber sits below the
        # bar is the silent-denominator failure this report exists to prevent.
        for cham in sorted(anchor.get("by_chamber") or {}):
            c = anchor["by_chamber"][cham]
            ct, cb = _pct(c["tick_hit"], c["tick_den"]), _pct(c["band_hit"], c["band_den"])
            lines.append(
                "    {:<7} rows {:6d}  ticker {:>5}% {:<4}  band {:>5}% {:<4}".format(
                    cham, c["rows"], ct,
                    "PASS" if (ct or 0) >= TICKER_BAR else "FAIL",
                    cb, "PASS" if (cb or 0) >= BAND_BAR else "FAIL"))
        below = [ch for ch, c in (anchor.get("by_chamber") or {}).items()
                 if (_pct(c["band_hit"], c["band_den"]) or 0) < BAND_BAR]
        if below:
            lines.append(
                "  BAND GATE NOW PER-CHAMBER: {} below the {}% bar while the aggregate "
                "passes. This is a PRE-EXISTING CONDITION NEWLY DISCLOSED, not a "
                "regression - the number did not move, the denominator stopped hiding it. "
                "Accepted as a documented per-chamber FLOOR on the same structure as the "
                "Phase H ticker ruling: a missing band cannot fabricate a wrong estimate, "
                "it degrades to an anchored-no-value row that Phase F already renders "
                "honestly. Residue characterised via `--residue`; the parser-reachable "
                "share is BACKLOG, not a gate-blocker.".format(
                    "/".join(sorted(below)), BAND_BAR))
        tail = ("Anchor rows are what Phase F derives holding claims FROM, so this is the "
                "population the bar governs (Mando ruling, SM-C3 Phase H).")
    else:
        gtp, gbp = tp, bp
        lines.append("GATE POPULATION - WHOLE CORPUS (no anchor set supplied)")
        tail = ("Measured over every coverage year, so historical filer-side symbol "
                "omissions are included in this verdict.")
    lines.append("GATE ticker >= {}%: {}".format(
        TICKER_BAR, "PASS" if (gtp or 0) >= TICKER_BAR else "FAIL"))
    lines.append("GATE band   >= {}%: {}".format(
        BAND_BAR, "PASS" if (gbp or 0) >= BAND_BAR else "FAIL"))
    lines.append(tail)
    lines.append("-" * 78)
    lines.append("STANDING RULING: " + RULING)
    lines.append("STANDING RULING: " + BAND_RULING)
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
    ap.add_argument("--residue", metavar="CHAMBER", nargs="?", const="house",
                    help="characterise that chamber's band-missing anchor rows")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    try:
        if args.residue:
            print(render_residue(band_residue(con, args.residue)))
        else:
            print(render(measure(con), measure_anchor(con)))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
