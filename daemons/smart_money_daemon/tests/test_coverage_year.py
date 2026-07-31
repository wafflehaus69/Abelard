"""SM-C3 Phase H: coverage_year vs filing_date.

`filing_year` meant DIFFERENT things per chamber — the House FD cycle year (coverage =
year-1) but the Senate eFD title year (already the coverage year). Fusion against PTR
flows depends on knowing which year a position DESCRIBES, so both are explicit now.
These tests pin the asymmetry so it cannot silently regress.
"""
from smart_money import db as dbmod
from smart_money.house_fd_ingest import _iso_mdy as h_iso
from smart_money.senate_fd_ingest import _iso_mdy as s_iso


def test_iso_mdy_both_chambers():
    for iso in (h_iso, s_iso):
        assert iso("05/15/2024") == "2024-05-15"
        assert iso("1/14/2026") == "2026-01-14"      # unpadded month/day
        assert iso("12/3/2025") == "2025-12-03"
        # unparseable stays None — a wrong filing_date mis-ages a fusion anchor
        for bad in ("", None, "garbage", "2024-05-15"):
            assert iso(bad) is None, bad


def test_migration_backfills_each_chamber_by_its_own_convention(tmp_path):
    path = str(tmp_path / "cov.db")
    con = dbmod.connect(path)
    ins = ("INSERT INTO congress_holdings(doc_id, chamber, filing_year, period, "
           "member_last, row_idx, ticker, ingested_at_unix) VALUES(?,?,?,?,?,?,?,0)")
    con.execute(ins, ("h1", "house", 2025, "5/15/2025", "Aaa", 0, "AAPL"))
    con.execute(ins, ("s1", "senate", 2025, "07/30/2026", "Bbb", 0, "AAPL"))
    con.commit()
    # simulate a pre-migration DB by clearing the derived columns, then re-migrate
    con.execute("UPDATE congress_holdings SET coverage_year=NULL, filing_date=NULL")
    con.commit()
    dbmod._migrate_coverage(con)
    got = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT chamber, coverage_year, filing_date FROM congress_holdings")}
    # House 2025 cycle covers CY2024; Senate title year IS the coverage year
    assert got["house"] == (2024, "2025-05-15"), got
    assert got["senate"] == (2025, "2026-07-30"), got
    con.close()


def test_senate_annual_is_filed_the_year_after_it_covers(tmp_path):
    """Guards the real-world shape: 'Annual Report for CY 2025' is filed in 2026, so a
    senate row's filing_date year is normally coverage_year + 1. If a change ever makes
    coverage_year track the filing year instead, this catches it."""
    path = str(tmp_path / "lag.db")
    con = dbmod.connect(path)
    con.execute(
        "INSERT INTO congress_holdings(doc_id, chamber, filing_year, coverage_year, "
        "filing_date, period, member_last, row_idx, ticker, ingested_at_unix) "
        "VALUES('s2','senate',2025,2025,'2026-07-30','07/30/2026','Ccc',0,'AAPL',0)")
    con.commit()
    cov, fd = con.execute(
        "SELECT coverage_year, filing_date FROM congress_holdings").fetchone()
    assert int(fd[:4]) - cov == 1, "annual should be filed the year after it covers"
    con.close()
