"""Backfill thirteenf_holdings.shares / shares_type for option rows from EDGAR.

Why this exists. Form 13F reports sshPrnamt and sshPrnamtType on option rows
exactly as it does on long rows — Duquesne's Q2 Tesla call states sshPrnamt
126000, sshPrnamtType SH, value 52996 (thousands), an implied $420.60 on 1,260
contracts. The ingest emitted a hardcoded 0 for every put and call, so all 450
option rows in the corpus carry shares=0 and the option share count was
unrecoverable from the DB. The parser is fixed; this repairs rows already stored
without re-ingesting anything else.

There is still NO strike and NO expiry. The complete Form 13F tag vocabulary is
eleven tags and contains neither, so contract terms remain out of reach — this
recovers size, not terms.

Safe by construction:
  * writes nothing without --apply;
  * only ever touches `shares` and `shares_type`, and only on option rows;
  * refuses any filing whose re-parse does not reproduce the stored option
    (cusip, put_call) key set AND the stored `value` on every one of those rows.
    Value is the untouched control: if it still matches, the re-parse is the same
    document we ingested and the share counts belong to these rows. If it does
    not, the filing was amended or the parser changed, and a silent update would
    attach one filing's sizes to another's rows.
"""
import argparse
import collections
import sys

from . import db as dbmod
from . import thirteenf
from .efd_ingest import load_env

OPTS = ("call", "put")


def _stored_options(con, cik, accession):
    """{(cusip, put_call): (value, shares)} for one filing's option rows."""
    return {
        (c, pc): (v, sh)
        for c, pc, v, sh in con.execute(
            "SELECT cusip, put_call, value, shares FROM thirteenf_holdings "
            "WHERE cik=? AND accession=? AND put_call IN ('call','put')",
            (cik, accession))
    }


def _parsed_options(holdings):
    """{(cusip, put_call): (value, shares, shares_type)} from a fresh parse."""
    out = {}
    for cusip, h in holdings.items():
        for b in OPTS:
            if h.get(b + "_val") or h.get(b + "_sh"):
                out[(cusip, b)] = (h[b + "_val"], h.get(b + "_sh", 0),
                                   h.get(b + "_type"))
    return out


def plan(con, contact, limit=None):
    """Compute the repair without touching the DB. Returns (updates, stats)."""
    stats = collections.Counter()
    updates = []
    filings = con.execute(
        "SELECT cik, accession, COUNT(*) FROM thirteenf_holdings "
        "WHERE put_call IN ('call','put') GROUP BY cik, accession "
        "ORDER BY cik, accession").fetchall()
    if limit:
        filings = filings[:limit]
    for cik, accession, n_opt in filings:
        stats["filings_seen"] += 1
        stats["option_rows_seen"] += n_opt
        try:
            holdings = thirteenf.fetch_info_table(cik, accession, contact)
        except Exception as e:                       # network / EDGAR / XML
            stats["filings_fetch_error"] += 1
            print("    FETCH-FAIL cik={} acc={} {}".format(cik, accession, e),
                  file=sys.stderr)
            continue
        stored = _stored_options(con, cik, accession)
        fresh = _parsed_options(holdings)
        if set(stored) != set(fresh):
            stats["filings_key_mismatch"] += 1
            continue
        if any(stored[k][0] != fresh[k][0] for k in stored):
            # `value` was never touched by the defect, so it is the control that
            # proves this is the same document. A mismatch means amended-or-drifted.
            stats["filings_value_mismatch"] += 1
            continue
        stats["filings_usable"] += 1
        for (cusip, pc), (_v, sh, st) in fresh.items():
            was = stored[(cusip, pc)][1]
            if not sh:
                # The filing really does state zero. Counted BEFORE the
                # already-correct check: a stored 0 matching a filed 0 is not a
                # row we got right, it is a row that stays sizeless, and pooling
                # the two would report the corpus as repaired when it is not.
                stats["rows_zero_in_filing"] += 1
                continue
            if was == sh:
                stats["rows_already_correct"] += 1
                continue
            updates.append((sh, st, cik, accession, cusip, pc))
            stats["rows_to_update"] += 1
            stats["shares_recovered"] += sh
    return updates, stats


def apply(con, updates):
    """Write the planned repair. Only ever touches shares and shares_type, and
    only on option rows — the put_call predicate is repeated here so that a bad
    caller cannot reach a long row through this function."""
    changed = 0
    for sh, st, cik, accession, cusip, pc in updates:
        cur = con.execute(
            "UPDATE thirteenf_holdings SET shares=?, shares_type=? "
            "WHERE cik=? AND accession=? AND cusip=? AND put_call=? "
            "AND put_call IN ('call','put')",
            (sh, st, cik, accession, cusip, pc))
        changed += cur.rowcount
    con.commit()
    return changed


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Backfill 13F option-row sshPrnamt from EDGAR")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N filings, for a fast dry run")
    ap.add_argument("--apply", action="store_true",
                    help="write the repair; without this nothing is modified")
    args = ap.parse_args(argv)
    contact = load_env().get("EDGAR_CONTACT")
    if not contact:
        print("FATAL missing EDGAR_CONTACT", file=sys.stderr)
        return 2
    con = dbmod.connect(args.db)
    updates, stats = plan(con, contact, args.limit)
    print("[reparse-opt-shares] planned updates: {}".format(len(updates)))
    for k in sorted(stats):
        print("    {:<28} {:,}".format(k, stats[k]))
    if stats["filings_fetch_error"] or stats["filings_key_mismatch"] or \
            stats["filings_value_mismatch"]:
        print("[reparse-opt-shares] NOTE: {} filing(s) refused or unfetched; "
              "their option rows keep shares=0.".format(
                  stats["filings_fetch_error"] + stats["filings_key_mismatch"]
                  + stats["filings_value_mismatch"]))
    if not args.apply:
        print("[reparse-opt-shares] DRY RUN, nothing written. Re-run with --apply.")
        return 0
    changed = apply(con, updates)
    print("[reparse-opt-shares] rows updated: {}".format(changed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
