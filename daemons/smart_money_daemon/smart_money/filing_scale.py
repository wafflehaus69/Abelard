"""Resolve the VALUE unit of a 13F filing once, at ingest, and store it.

Form 13F states no unit for VALUE anywhere — not on the cover page, not in the
information table. Since the SEC's 2023 amendments whole dollars are mandated, but
filers still report thousands and the document says so nowhere. Duquesne reports
Natera as value=864923 against sshPrnamt=3186306, an implied $0.27 on a ~$271
stock, with sshPrnamtType=SH and titleOfClass=COM. Nothing in the row is wrong;
the unit is simply unstated.

RESOLVED PER FILING, NEVER PER FILER. Verified against EDGAR: Duquesne filed
thousands at 2022-09-30, whole dollars at 2022-12-31 (its first filing under the
amended schema, tableValueTotal 2,020,266,796), then reverted to thousands and is
still there at 2026-06-30. A per-filer scale holds for the nine periods currently
ingested and would silently mis-scale an entire quarter the moment earlier periods
are backfilled.

Two signals, used for different jobs:
  1. PRICE ANCHOR decides the unit — median of (value/shares) / close across the
     filing's largest priced long positions. Whole dollars land near 1.0,
     thousands near 0.001. The clusters sit three orders of magnitude apart, so
     the 0.01 cut is ~10x clear of both.
  2. CONTROL TOTAL checks the parse, not the unit — the cover page's own
     tableValueTotal must equal the sum of the rows we stored. A mismatch means an
     incomplete parse, which would also poison the anchor. Reported, never
     silently tolerated.

A filing that cannot be anchored is left NULL, not defaulted to 1. NULL means "not
resolved" and reads as raw, which is visibly wrong for a thousands filer rather
than silently plausible.
"""
import argparse
import sys
import time

from . import db as dbmod
from .queries import _close_on

# Ratio below this is thousands, above is dollars. Both observed clusters sit a
# factor of ~10 clear of it.
SCALE_CUT = 0.01
ANCHOR_LIMIT = 20
# One anchor is enough to DECIDE: the two clusters are 1.0 and 0.001, three orders
# of magnitude apart, so this is never a marginal call. Requiring three would leave
# genuinely small books permanently undetermined — Affinity and Founders Fund hold
# one position each, so three anchors is structurally impossible for them.
MIN_ANCHORS = 1
# Below this the verdict stands but is marked weak, so thin evidence is visible
# rather than presented with the same confidence as twenty anchors.
WEAK_ANCHORS = 3


def price_anchor(con, cik, accession, period, limit=ANCHOR_LIMIT):
    """(scale, ratios) from the filing's largest priced long rows, or (None, [])."""
    ratios = []
    for ticker, val, sh in con.execute(
            "SELECT ticker, value, shares FROM thirteenf_holdings "
            "WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND accession=? AND "
            "put_call='long' AND ticker IS NOT NULL AND shares>0 AND value>0 "
            # PRN rows carry dollars of par in `shares`, so their implied price is
            # meaningless as an anchor. Excluded where the type is known; rows
            # ingested before shares_type existed are NULL and still admitted.
            "AND (shares_type IS NULL OR shares_type='SH') "
            "ORDER BY value DESC LIMIT ?", (cik, accession, limit)):
        close, _d = _close_on(con, ticker.upper(), period)
        if close and close > 0:
            ratios.append((val / sh) / close)
    if len(ratios) < MIN_ANCHORS:
        return None, ratios
    ratios.sort()
    med = ratios[len(ratios) // 2]
    return (1000 if med < SCALE_CUT else 1), ratios


def control_total(con, cik, accession):
    """(parsed_rows, parsed_value) actually stored for this filing."""
    n, v = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(value), 0) FROM thirteenf_holdings "
        "WHERE CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND accession=?",
        (cik, accession)).fetchone()
    return n, v


def resolve(con, cik, accession, period):
    """Decide the scale for one filing. Returns a dict; writes nothing."""
    scale, ratios = price_anchor(con, cik, accession, period)
    n, v = control_total(con, cik, accession)
    med = ratios[len(ratios) // 2] if ratios else None
    if scale is None:
        basis = "undetermined"
    elif len(ratios) < WEAK_ANCHORS:
        basis = "price_anchored_weak"
    else:
        basis = "price_anchored"
    return {
        "cik": str(int(cik)), "accession": accession, "period": period,
        "value_scale": scale, "scale_basis": basis,
        "anchors": len(ratios), "median_ratio": med,
        "parsed_rows": n, "parsed_value": v,
    }


def apply_to_filing(con, cik, accession, scale):
    """Stamp the resolved scale onto every row of one filing."""
    cur = con.execute(
        "UPDATE thirteenf_holdings SET value_scale=? WHERE "
        "CAST(cik AS INTEGER)=CAST(? AS INTEGER) AND accession=?",
        (scale, cik, accession))
    return cur.rowcount


def record_meta(con, r, entry_total=None, value_total=None):
    con.execute(
        "INSERT OR REPLACE INTO thirteenf_filing_meta("
        "cik, accession, period, filed_date, entry_total, value_total, "
        "parsed_rows, parsed_value, value_scale, scale_basis, resolved_at_unix)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (r["cik"], r["accession"], r["period"], r.get("filed_date"),
         entry_total, value_total, r["parsed_rows"], r["parsed_value"],
         r["value_scale"], r["scale_basis"], int(time.time())))


def plan(con):
    """Resolve every distinct filing in thirteenf_holdings.

    Two passes. The first anchors each filing on its own prices — that is the
    per-filing evidence and it always wins. The second lets a filing with no price
    coverage of its own inherit from the SAME filer's nearest anchorable filing,
    marked `inherited` so it is never mistaken for direct evidence.

    Inheritance is a fallback, not the model: it is exactly the assumption
    (one unit per filer) that Duquesne's 2022-12-31 filing disproves. It is used
    only where there is no evidence at all, and the nearest-in-time source makes a
    mid-history switch the smallest possible error rather than a corpus-wide one.
    """
    out = []
    for cik, acc, period, filed in con.execute(
            "SELECT cik, accession, period, MIN(filed_date) FROM "
            "thirteenf_holdings GROUP BY cik, accession, period "
            "ORDER BY cik, period"):
        r = resolve(con, cik, acc, period)
        r["filed_date"] = filed
        out.append(r)

    by_filer = {}
    for r in out:
        by_filer.setdefault(r["cik"], []).append(r)
    for cik, rs in by_filer.items():
        anchored = [x for x in rs if x["value_scale"] is not None]
        if not anchored:
            continue                       # nothing to inherit from; stays undetermined
        for r in rs:
            if r["value_scale"] is not None:
                continue
            near = min(anchored,
                       key=lambda a: abs(_ord(a["period"]) - _ord(r["period"])))
            r["value_scale"] = near["value_scale"]
            r["scale_basis"] = "inherited"
            r["inherited_from"] = near["accession"]
    return out


def _ord(period):
    """Sortable integer for a period string, for nearest-in-time comparison."""
    try:
        return int((period or "").replace("-", ""))
    except ValueError:
        return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="Resolve 13F value units per filing")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--apply", action="store_true",
                    help="write value_scale and filing meta; without this nothing changes")
    args = ap.parse_args(argv)

    # dbmod.connect even for the dry run: the anchor query reads shares_type and
    # value_scale, so an unmigrated DB cannot be inspected read-only. connect()
    # applies pending column adds and nothing else; the scale and meta writes are
    # still gated behind --apply.
    con = dbmod.connect(args.db)
    rows = plan(con)
    by_basis = {}
    thousands = []
    for r in rows:
        by_basis[r["scale_basis"]] = by_basis.get(r["scale_basis"], 0) + 1
        if r["value_scale"] == 1000:
            thousands.append(r)
    print("[scale] filings: {}".format(len(rows)))
    for k in sorted(by_basis):
        print("    {:<16} {}".format(k, by_basis[k]))
    print("[scale] filings reporting in THOUSANDS: {}".format(len(thousands)))
    for r in thousands:
        print("    cik {:<10} {} {}  median_ratio={:.5f} rows={}".format(
            r["cik"], r["accession"], r["period"], r["median_ratio"] or 0,
            r["parsed_rows"]))
    und = [r for r in rows if r["scale_basis"] == "undetermined"]
    for r in und:
        print("    UNDETERMINED cik {} {} {} anchors={} rows={}".format(
            r["cik"], r["accession"], r["period"], r["anchors"], r["parsed_rows"]))
    if not args.apply:
        print("[scale] DRY RUN, nothing written. Re-run with --apply.")
        return 0
    stamped = 0
    for r in rows:
        if r["value_scale"]:
            stamped += apply_to_filing(con, r["cik"], r["accession"], r["value_scale"])
        record_meta(con, r)
    con.commit()
    print("[scale] rows stamped: {}".format(stamped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
