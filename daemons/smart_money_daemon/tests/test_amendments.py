"""F5 amendment supersede policy, synthetic fixture (live backfill has none)."""
import os
import tempfile

from smart_money import db as dbmod
from smart_money.amendments import apply_supersedes


def _mk(con, person_id, name):
    con.execute(
        "INSERT OR IGNORE INTO persons(person_id, name, type, cik_or_chamber) "
        "VALUES (?,?,'congress','house')",
        (person_id, name),
    )


def _trade(con, pid, filing_id, raw_ref, tx="2025-01-10", ticker="AAPL",
           side="purchase", lo=1001, hi=15000):
    con.execute(
        "INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
        "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, "
        "asset_type, filing_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pid, ticker, side, lo, hi, tx, "2025-02-01", 22, "house", "test",
         raw_ref, "Stock", filing_id),
    )


def _filing(con, filing_id, label, filed):
    con.execute(
        "INSERT INTO ingested_filings(filing_id, chamber, status, report_label, "
        "filed_date, ingested_at_unix) VALUES (?,?,?,?,?,0)",
        (filing_id, "house", "electronic", label, filed),
    )


def _fresh_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def test_amendment_supersedes_matching_original():
    path = _fresh_db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "F1", "Periodic Transaction Report", "2025-02-01")
        _filing(con, "F2", "Periodic Transaction Report Amendment", "2025-03-01")
        _trade(con, 1, "F1", "F1#1")   # original
        _trade(con, 1, "F2", "F2#1")   # amendment, same signature
        con.commit()

        stats = apply_supersedes(con)
        assert stats["superseded"] == 1, stats

        rows = dict(
            con.execute(
                "SELECT filing_id, superseded FROM congress_trades"
            ).fetchall()
        )
        assert rows["F1"] == 1, "original must be superseded"
        assert rows["F2"] == 0, "amendment must survive"
    finally:
        os.remove(path)


def test_unmatched_amendment_scores_as_new():
    path = _fresh_db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "F9", "Periodic Transaction Report Amendment", "2025-03-01")
        _trade(con, 1, "F9", "F9#1", ticker="NVDA")  # no original to match
        con.commit()

        stats = apply_supersedes(con)
        assert stats["superseded"] == 0
        assert stats["unmatched"] == 1
        row = con.execute(
            "SELECT superseded FROM congress_trades WHERE filing_id='F9'"
        ).fetchone()
        assert row[0] == 0, "lone amendment scores as a new event"
    finally:
        os.remove(path)


def test_idempotent():
    path = _fresh_db()
    try:
        con = dbmod.connect(path)
        _mk(con, 1, "Rep A")
        _filing(con, "F1", "PTR", "2025-02-01")
        _filing(con, "F2", "PTR Amendment", "2025-03-01")
        _trade(con, 1, "F1", "F1#1")
        _trade(con, 1, "F2", "F2#1")
        con.commit()
        a = apply_supersedes(con)
        b = apply_supersedes(con)
        assert a == b, (a, b)
    finally:
        os.remove(path)
