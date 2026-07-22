"""F5 amendment dedup policy (ORDER SM-2). Binding for the future delta-scan;
moot on today's backfill which has no matched amendments, but implemented and
unit-tested now so the delta-scan inherits it.

Policy: an amendment PTR supersedes its original, matched on
(person_id, tx_date, ticker, side, amt_low, amt_high). Keep the latest
filing_id's row; mark the superseded original row superseded=1. Scoring reads
superseded=0 only. An amendment row with no matching original scores as a new
event (stays superseded=0).

Amendment filings are identified by 'amend' in ingested_filings.report_label.
'Latest' is decided by filed_date then filing_id, deterministic.
"""
import argparse
import sys

from . import db as dbmod

MATCH_COLS = ("person_id", "tx_date", "ticker", "side", "amt_low", "amt_high")


def apply_supersedes(con) -> dict:
    """Mark originals superseded by a later amendment. Idempotent: recomputes
    superseded from scratch each call."""
    con.execute("UPDATE congress_trades SET superseded=0")

    amend_filings = {
        row[0]
        for row in con.execute(
            "SELECT filing_id FROM ingested_filings "
            "WHERE LOWER(COALESCE(report_label,'')) LIKE '%amend%'"
        )
    }
    if not amend_filings:
        con.commit()
        return {"amendment_filings": 0, "superseded": 0, "unmatched": 0}

    # filing order key for 'latest'
    order = {
        fid: (fd or "", fid)
        for fid, fd in con.execute(
            "SELECT filing_id, filed_date FROM ingested_filings"
        )
    }

    rows = con.execute(
        "SELECT trade_id, person_id, tx_date, ticker, side, amt_low, amt_high, "
        "filing_id FROM congress_trades"
    ).fetchall()

    groups = {}
    for tid, pid, tx, tk, side, lo, hi, fid in rows:
        key = (pid, tx, tk, side, lo, hi)
        groups.setdefault(key, []).append((tid, fid))

    superseded = 0
    unmatched = 0
    for key, members in groups.items():
        has_amend = any(fid in amend_filings for _, fid in members)
        if not has_amend:
            continue
        if len(members) == 1:
            unmatched += 1  # lone amendment, no original to supersede
            continue
        # keep the latest filing, supersede the rest
        winner = max(members, key=lambda m: order.get(m[1], ("", m[1])))
        for tid, fid in members:
            if tid != winner[0]:
                con.execute(
                    "UPDATE congress_trades SET superseded=1 WHERE trade_id=?", (tid,)
                )
                superseded += 1
    con.commit()
    return {
        "amendment_filings": len(amend_filings),
        "superseded": superseded,
        "unmatched": unmatched,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Apply amendment supersede policy")
    ap.add_argument("--db", default=dbmod.DB_PATH_DEFAULT)
    args = ap.parse_args(argv)
    con = dbmod.connect(args.db)
    stats = apply_supersedes(con)
    print(
        "[amend] amendment_filings={} superseded_rows={} lone_amendments={}".format(
            stats["amendment_filings"], stats["superseded"], stats["unmatched"]
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
