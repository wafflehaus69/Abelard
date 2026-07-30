"""Targeted re-parse of corrupt Form 4 accessions: delete + re-persist through
the sanity guard, carrying filed_date and ingest_regime. EDGAR fetch is
monkeypatched so the test is hermetic (no network)."""
import os
import tempfile

from smart_money import db as dbmod
from smart_money import form4
from smart_money import reparse_corrupt as R


def _fresh():
    fd, p = tempfile.mkstemp(suffix=".db"); os.close(fd); return p


# What EDGAR would parse for the re-fetched accession: one clean equity buy and
# one value-denominated debt note (the FINS/KYN mechanism).
FETCHED = {
    "owner": "Fund Manager LLC", "owner_cik": "0009999999",
    "issuer": "Acme Corp", "issuer_cik": "12345", "symbol": "ACME",
    "plan_flag": False, "role": "10pct",
    "txns": [
        {"code": "P", "security_title": "Common Stock", "shares": "1000",
         "price": "12.5", "date": "2026-06-10", "ad": "A",
         "owned_after": "5000", "value_denominated": False},
        {"code": "P", "security_title": "5.0% Senior Notes due 2030",
         "shares": "40000000", "price": "40000000", "date": "2026-06-10",
         "ad": "A", "owned_after": "", "value_denominated": True},
    ],
    "deriv_txns": [],
}


def _swap_fetch(fn):
    """Save/restore form4.fetch_form4_from_txt so the test runs under both the
    bare Orban runner and pytest (no fixture dependency). Returns the original."""
    orig = form4.fetch_form4_from_txt
    form4.fetch_form4_from_txt = fn
    return orig


def test_reparse_replaces_corrupt_rows_and_carries_regime():
    path = _fresh()
    orig = _swap_fetch(lambda contact, path, pace=None: FETCHED)
    try:
        con = dbmod.connect(path)
        # Seed a single pre-guard corrupt row (bad value, NULL value_flag), tagged
        # 'universal' with a known filed_date to prove carry-over.
        con.execute(
            "INSERT INTO form4_transactions(accession,tx_index,issuer_cik,ticker,"
            "code,shares,price,value,filed_date,ingest_regime) "
            "VALUES ('ACC-1',0,'0000012345','ACME','P',40000000,40000000,1.6e15,"
            "'2026-06-12','universal')")
        con.commit()

        res = R.reparse(con, "contact", [("ACC-1", "0000012345")])
        assert res == [("ACC-1", "reparsed", 2, 1)]  # 2 persisted, 1 flagged

        rows = con.execute(
            "SELECT tx_index, code, value, value_flag, filed_date, ingest_regime "
            "FROM form4_transactions WHERE accession='ACC-1' ORDER BY tx_index"
        ).fetchall()
        assert len(rows) == 2                      # old single row replaced by two
        # clean equity row: value computed, no flag
        assert rows[0][2] == 12500.0 and rows[0][3] is None
        # debt row: value withheld, flagged
        assert rows[1][2] is None and rows[1][3] == "value_denominated"
        # filed_date + ingest_regime carried over from the deleted row
        assert rows[0][4] == "2026-06-12" and rows[0][5] == "universal"
    finally:
        form4.fetch_form4_from_txt = orig
        con.close(); os.remove(path)


def test_reparse_fetch_failure_leaves_rows_untouched():
    def _boom(contact, path, pace=None):
        raise RuntimeError("edgar 500")
    path = _fresh()
    orig = _swap_fetch(_boom)
    try:
        con = dbmod.connect(path)
        con.execute(
            "INSERT INTO form4_transactions(accession,tx_index,issuer_cik,ticker,"
            "code,shares,price,value) "
            "VALUES ('ACC-2',0,'0000012345','ACME','P',1,2000000,2000000)")
        con.commit()

        res = R.reparse(con, "contact", [("ACC-2", "0000012345")])
        assert res[0][1].startswith("fetch_error")
        # delete only happens after a successful fetch -> original row survives
        assert con.execute("SELECT count(*) FROM form4_transactions "
                           "WHERE accession='ACC-2'").fetchone()[0] == 1
    finally:
        form4.fetch_form4_from_txt = orig
        con.close(); os.remove(path)
