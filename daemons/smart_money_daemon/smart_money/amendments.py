"""Amendment dedup policy for congressional PTRs (ORDER SM-2, revised).

Policy: an amendment supersedes its original, matched on
(person_id, tx_date, ticker, side, amt_low, amt_high). Keep the rows of the
latest filing in the group; mark the rest superseded=1. Scoring reads
superseded=0 only. An amendment with no earlier filing to supersede scores as a
new event and stays live.

Evidence for "this is an amendment" comes from two places, in order of quality:

  1. congress_trades.filing_status - the Clerk's own per-line New/Amended/
     Deleted marker, backfilled by reparse_status. This is the real signal.
  2. ingested_filings.report_label containing 'amend' - the original gate.
     Retained because it catches a handful of filings, but it is nearly useless
     on its own: the Clerk files amended PTRs under an ordinary 'P' type, so
     the label almost never says 'amend' even when every line in the document
     is marked Deleted.

Supersession is decided per (group, filing), not per row. A single filing
legitimately lists the same asset on several lines - verified against the
source PDFs - so superseding individual rows would silently drop real
disclosures. Whole filings win or lose together.

A Deleted line is the filer retracting a disclosure. It is never itself a live
transaction, whether or not we hold the original it retracts; treating one as
live would invent a transaction the filer explicitly withdrew.

'Latest' is decided by filed_date then filing_id, deterministic.
"""
import argparse
import sys

from . import db as dbmod

MATCH_COLS = ("person_id", "tx_date", "ticker", "side", "amt_low", "amt_high")
AMEND_STATUSES = ("Amended", "Deleted")


def apply_supersedes(con) -> dict:
    """Mark originals superseded by a later amendment. Idempotent: recomputes
    superseded from scratch each call."""
    con.execute("UPDATE congress_trades SET superseded=0")

    label_filings = {
        row[0]
        for row in con.execute(
            "SELECT filing_id FROM ingested_filings "
            "WHERE LOWER(COALESCE(report_label,'')) LIKE '%amend%'"
        )
    }

    order = {
        fid: (fd or "", fid)
        for fid, fd in con.execute(
            "SELECT filing_id, filed_date FROM ingested_filings"
        )
    }

    rows = con.execute(
        "SELECT trade_id, person_id, tx_date, ticker, side, amt_low, amt_high, "
        "filing_id, filing_status FROM congress_trades"
    ).fetchall()

    groups = {}
    for tid, pid, tx, tk, side, lo, hi, fid, fstat in rows:
        groups.setdefault((pid, tx, tk, side, lo, hi), []).append(
            (tid, fid, (fstat or ""))
        )

    dead = set()
    status_filings = set()
    unmatched = 0

    for members in groups.values():
        filings = {fid for _, fid, _ in members}
        has_status = False
        for tid, fid, fstat in members:
            if fstat in AMEND_STATUSES:
                has_status = True
                status_filings.add(fid)
            if fstat == "Deleted":
                # Retracted by the filer. Dies regardless of what else is in
                # the group, including when it is the only thing we hold.
                dead.add(tid)
        if not has_status and not (filings & label_filings):
            continue
        if len(filings) == 1:
            unmatched += 1  # lone amendment, no earlier filing to supersede
            continue
        latest = max(filings, key=lambda f: order.get(f, ("", f)))
        for tid, fid, _ in members:
            if fid != latest:
                dead.add(tid)

    for tid in dead:
        con.execute(
            "UPDATE congress_trades SET superseded=1 WHERE trade_id=?", (tid,)
        )
    con.commit()
    return {
        "amendment_filings": len(label_filings | status_filings),
        "label_filings": len(label_filings),
        "status_filings": len(status_filings),
        "superseded": len(dead),
        "unmatched": unmatched,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Apply amendment supersede policy")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    stats = apply_supersedes(con)
    print(
        "[amend] amendment_filings={} (label={} status={}) superseded_rows={} "
        "lone_amendments={}".format(
            stats["amendment_filings"], stats["label_filings"],
            stats["status_filings"], stats["superseded"], stats["unmatched"]
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
