"""Backfill congress_trades.filing_status / clerk_line_id from cached House PDFs.

Why this exists. The Clerk stamps every PTR line with New/Amended/Deleted. Two
PDF vintages reach us and the original parser lost the marker on both, in
opposite ways: pre-2022 captions ("FIlINg STATuS:") were swept into the asset
name, and 2022+ captions decode to NULs that collapse to "F S:", which the
layout-noise skip discarded outright. From 2023 on that is 100% of filings, so
every recent amendment is invisible and its superseded original still reads as
live. house_ingest now captures both; this walks the cached PDFs and repairs
rows already on disk.

Safe by construction: writes nothing without --apply, and skips any filing whose
re-parse does not reproduce exactly the row count already stored, since raw_ref
is a positional "{doc}#{i}" index and a shifted parse would misattribute every
status in the document.
"""
import argparse
import collections
import os
import sys

from . import db as dbmod
from .house_ingest import normalize_row, parse_ptr_pdf

RAW_DIR_DEFAULT = "data/raw/house/pdfs"


def _pdf_path(raw_dir, filing_id, disclosure_date):
    """Cached PDFs are filed under the year they were published."""
    cands = []
    if disclosure_date:
        cands.append(disclosure_date[:4])
    for yr in cands + [str(y) for y in range(2012, 2027)]:
        p = os.path.join(raw_dir, yr, "{}.pdf".format(filing_id))
        if os.path.exists(p):
            return p
    return None


def plan(con, raw_dir=RAW_DIR_DEFAULT, limit=None):
    """Compute the repair without touching the DB. Returns (updates, stats)."""
    stats = collections.Counter()
    updates = []
    filings = con.execute(
        "SELECT filing_id, MIN(disclosure_date), COUNT(*) FROM congress_trades "
        "WHERE source='house_clerk' GROUP BY filing_id ORDER BY filing_id"
    ).fetchall()
    if limit:
        filings = filings[:limit]
    for filing_id, disclosure, n_rows in filings:
        stats["filings_seen"] += 1
        path = _pdf_path(raw_dir, filing_id, disclosure)
        if path is None:
            stats["filings_no_pdf"] += 1
            continue
        try:
            rows, status = parse_ptr_pdf(path)
        except Exception:
            stats["filings_parse_error"] += 1
            continue
        if status != "ok":
            stats["filings_not_ok"] += 1
            continue
        if len(rows) != n_rows:
            # Positional raw_ref means a shifted parse would attach every
            # status to the wrong transaction. Refuse the whole document.
            stats["filings_rowcount_mismatch"] += 1
            continue
        stats["filings_usable"] += 1
        for i, row in enumerate(rows, 1):
            try:
                n = normalize_row(row, filing_id)
            except Exception:
                stats["rows_normalize_error"] += 1
                continue
            updates.append(
                (n["filing_status"], n["clerk_line_id"], n["asset_name"],
                 n["comment"], filing_id, "{}#{}".format(filing_id, i))
            )
            if n["filing_status"]:
                stats["rows_status_" + n["filing_status"].lower()] += 1
            if n["clerk_line_id"]:
                stats["rows_with_clerk_id"] += 1
    return updates, stats


def apply(con, updates):
    """Write the planned repair. Only ever touches the four repaired columns."""
    changed = 0
    for fs, cid, asset, comment, filing_id, raw_ref in updates:
        cur = con.execute(
            "UPDATE congress_trades SET filing_status=?, clerk_line_id=?, "
            "asset_name=?, comment=? WHERE filing_id=? AND raw_ref=?",
            (fs, cid, asset, comment, filing_id, raw_ref),
        )
        changed += cur.rowcount
    con.commit()
    return changed


def main(argv=None):
    ap = argparse.ArgumentParser(description="Backfill House PTR filing_status")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--raw-dir", default=RAW_DIR_DEFAULT)
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N filings, for a fast dry run")
    ap.add_argument("--apply", action="store_true",
                    help="write the repair; without this nothing is modified")
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    updates, stats = plan(con, args.raw_dir, args.limit)
    print("[reparse-status] planned updates: {}".format(len(updates)))
    for k in sorted(stats):
        print("    {:<32} {}".format(k, stats[k]))
    if not args.apply:
        print("[reparse-status] DRY RUN, nothing written. Re-run with --apply.")
        return 0
    changed = apply(con, updates)
    print("[reparse-status] rows updated: {}".format(changed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
