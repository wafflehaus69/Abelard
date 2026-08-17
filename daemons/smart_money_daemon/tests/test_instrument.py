"""Instrument classification from stored evidence, never from a ticker suffix.

thirteenf_holdings had no instrument vocabulary beyond put_call, so 264 convertible
rows worth $17,242,546,817 counted as equity conviction and a filer holding $899,412
of CORZ common beside $42,018,000 of CORZW warrants read as one equity stake.
"""
import os
import tempfile

from smart_money import db as dbmod, instrument as ins


# --------------------------------------------------------- the suffix trap ---

# Measured across Q2: endswith('W') would misclassify $1,578,772,294 of common to
# catch $47,466,880 of warrants — 33x more false than true.
W_COMMON = [("GLW", "219350105"), ("NOW", "81762P102"), ("SNOW", "833445109"),
            ("BW", "05614L100"), ("CDW", "12514G108"), ("PANW", "697435105")]


def test_common_tickers_ending_in_w_are_not_warrants():
    for tk, cu in W_COMMON:
        assert ins.classify(put_call="long", cusip=cu, ticker=tk) == ins.COMMON, tk


def test_a_warrant_is_recognised_from_stated_type_not_from_its_symbol():
    assert ins.classify(put_call="long", cusip="21873S119", ticker="CORZW",
                        security_type="Warrant") == ins.WARRANT
    # and the same symbol with no stated type is NOT guessed at
    assert ins.classify(put_call="long", cusip="21873S119",
                        ticker="CORZW") == ins.COMMON


def test_warrants_that_do_not_end_in_w_are_still_caught_by_type():
    """FLYX/WS and OPENL are real warrants whose symbols a suffix rule misses."""
    assert ins.classify(put_call="long", cusip="26884L109", ticker="FLYX/WS",
                        security_type="Warrant") == ins.WARRANT
    assert ins.classify(put_call="long", cusip="683712137", ticker="OPENL",
                        security_type="Warrant") == ins.WARRANT


def test_title_of_class_also_states_it():
    assert ins.classify(put_call="long", cusip="21873S119", ticker="X",
                        title_of_class="*W EXP 01/15/27") == ins.WARRANT
    assert ins.classify(put_call="long", cusip="123456789", ticker="X",
                        title_of_class="UNIT 99/99/99") == ins.UNIT


# ------------------------------------------------------------ convertibles ---

def test_options_are_decided_by_the_filings_own_tag():
    assert ins.classify(put_call="put", cusip="67066G104") == ins.OPTION_PUT
    assert ins.classify(put_call="call", cusip="595112103") == ins.OPTION_CALL


def test_a_debt_issue_cusip_is_a_convertible_note():
    """CUSIP chars 7-8 are the issue number and are alphabetic for debt. This is
    the CUSIP standard, not a heuristic, and needs no backfill."""
    for cu, tk in (("37940XAU6", "GPN 1.5 03/01/31"),
                   ("55024UAF6", "LITE 0.5 06/15/28"),
                   ("090043AF7", "BILL 0 04/01/30")):
        assert ins.classify(put_call="long", cusip=cu,
                            ticker=tk) == ins.CONVERTIBLE_NOTE, cu


def test_a_coupon_on_an_equity_issue_cusip_is_preferred_not_a_note():
    """GOOGL 6.25 05/15/29 A is a depositary share at ~$50 par. Its CUSIP issue
    number is numeric, so it is not debt; the implied unit prices cluster at
    $44.88-$68.28 against notes at 0.82-8.61 per dollar of par."""
    for cu, tk in (("02079K404", "GOOGL 6.25 05/15/29 A"),
                   ("02079K602", "GOOGL 6.25 05/15/29 B"),
                   ("65339F655", "NEE 7.375 02/15/29"),
                   ("68389X204", "ORCL 6.5 01/15/29 D")):
        assert ins.classify(put_call="long", cusip=cu,
                            ticker=tk) == ins.CONVERTIBLE_PREFERRED, cu


def test_par_denominated_shares_corroborate_debt():
    assert ins.classify(put_call="long", cusip="123456109", ticker="X",
                        shares_type="PRN") == ins.CONVERTIBLE_NOTE


def test_plain_common_stays_common():
    for cu, tk in (("02079K305", "GOOGL"), ("02079K107", "GOOG"),
                   ("632307104", "NTRA"), ("457669307", "INSM")):
        assert ins.classify(put_call="long", cusip=cu, ticker=tk) == ins.COMMON


# ------------------------------------------------------- issuer rollup (D8) ---

def test_share_classes_and_converts_share_one_issuer_id():
    """Alphabet's real exposure is $4.49bn across 4 distinct ticker strings, and
    no page joined them because the join was attempted on ticker."""
    ids = {ins.issuer_id(c) for c in
           ("02079K305", "02079K107", "02079K404", "02079K602")}
    assert ids == {"02079K"}


def test_berkshire_classes_share_an_issuer_id():
    assert ins.issuer_id("084670702") == ins.issuer_id("084670108") == "084670"


def test_issuer_id_is_defensive():
    assert ins.issuer_id(None) is None
    assert ins.issuer_id("abc") is None


def test_issuer_ticker_strips_a_bond_descriptor():
    assert ins.issuer_ticker("GOOGL 6.25 05/15/29 A") == "GOOGL"
    assert ins.issuer_ticker("BILL 0 04/01/30") == "BILL"
    assert ins.issuer_ticker("GOOGL") == "GOOGL"
    assert ins.issuer_ticker(None) is None


# ---------------------------------------------------------------- storage ---

def test_issuer_id_is_generated_by_the_database():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = None
    try:
        con = dbmod.connect(path)
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, "
            "cusip, ticker, issuer, put_call, value, shares, ingested_at_unix) "
            "VALUES ('1','a','2026-06-30','2026-08-14','02079K404',"
            "'GOOGL 6.25 05/15/29 A','ALPHABET INC','long',66157000,1300000,0)")
        con.commit()
        assert con.execute(
            "SELECT issuer_id FROM thirteenf_holdings").fetchone()[0] == "02079K"
    finally:
        if con:
            con.close()
        try:
            os.remove(path)
        except OSError:
            pass


def test_figi_spells_warrants_equity_wrt():
    """FIGI's securityType for a warrant is 'Equity WRT', which does not contain
    the word 'warrant'. Matching only the full word missed all 9 warrant CUSIPs in
    the corpus."""
    assert ins.classify(put_call="long", cusip="21874A114", ticker="CORZW",
                        security_type="Equity WRT") == ins.WARRANT
    assert ins.classify(put_call="long", cusip="165167172", ticker="EXEEZ",
                        security_type="Equity WRT") == ins.WARRANT


def test_wrt_must_be_a_whole_word():
    """Guard the guard: a type merely containing those letters is not a warrant."""
    assert ins.classify(put_call="long", cusip="123456109", ticker="X",
                        security_type="WRTX Holdings Common") == ins.COMMON


def test_the_issuer_id_index_survives_a_migration_from_the_old_schema():
    """Regression: the index was declared in the DDL, which executescript runs on
    EVERY connect — including against a pre-existing table where CREATE TABLE IF
    NOT EXISTS is a no-op, so it failed with 'no such column: issuer_id' before the
    migration could add it."""
    import sqlite3
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = None
    try:
        old = sqlite3.connect(path)
        old.executescript("""
        CREATE TABLE thirteenf_holdings(
          cik TEXT NOT NULL, accession TEXT NOT NULL, period TEXT, filed_date TEXT,
          cusip TEXT NOT NULL, ticker TEXT, issuer TEXT,
          put_call TEXT NOT NULL DEFAULT 'long', value INTEGER, shares INTEGER,
          ingested_at_unix INTEGER NOT NULL,
          PRIMARY KEY(cik, accession, cusip, put_call));
        """)
        old.commit()
        old.close()
        con = dbmod.connect(path)          # must not raise
        names = {r[1] for r in con.execute("PRAGMA table_xinfo(thirteenf_holdings)")}
        assert "issuer_id" in names and "instrument_class" in names
        idx = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND "
            "name='idx_13fh_issuer'")]
        assert idx == ["idx_13fh_issuer"]
        con.close()
        con = dbmod.connect(path)          # idempotent
    finally:
        if con:
            con.close()
        try:
            os.remove(path)
        except OSError:
            pass
