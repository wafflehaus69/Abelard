"""Insider-feed data quality: execution price, return basis, issuer class, co-filing.

The governing rule is MARK, NEVER DROP. Every one of these markers separates "we cannot
trust this number" from "this number is small" — and a marker that deleted rows would
destroy the one thing that makes the residue reviewable.
"""
import os
import tempfile

from smart_money import db as dbmod, queries as q

_F4 = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
       "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
       "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
       "VALUES(?,0,?,?,?,'9',?,'P',0,?,?,?,NULL,?,?,NULL,'watchlist')")
_PX = "INSERT INTO prices VALUES(?,?,?,?,'eod',0,0,'y')"


def _db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return p, dbmod.connect(p)


def _buy(con, tk, shares, price, person="Insider A", cik="1", issuer="Acme Inc",
         date="2026-06-01", i=[0]):
    i[0] += 1
    con.execute(_F4, ("a%d" % i[0], person, cik, issuer, tk, shares, price,
                      round(shares * price, 2), date, date))


def _feed(con, **kw):
    con.commit()
    return q.q_insider_trades(con, side="buy", window=400, anchor="2026-08-07",
                              scope="all", per_page=500, **kw)["rows"]


def _row(rows, tk):
    for r in rows:
        if r["ticker"] == tk:
            return r
    raise AssertionError("no %s in %s" % (tk, [r["ticker"] for r in rows]))


# ---------------------------------------------------------------- execution price

def test_the_feed_carries_the_execution_price_not_only_the_close():
    """BRVE: bought at $18 while the stock closed at $30. The feed used to load `price`
    and discard it, leaving entry_close as the only per-share number on the row - so a
    reader had no way to see what the insider actually paid."""
    p, con = _db()
    try:
        _buy(con, "BRVE", 1111, 18.0)
        con.execute(_PX, ("BRVE", "2026-06-01", 30.0, 30.0))
        r = _row(_feed(con), "BRVE")
        assert r["exec_price"] == 18.0
        assert r["entry_close"] == 30.0
        assert r["implied_price"] == 18.0, "value/shares reconstructs the same price"
    finally:
        con.close()
        os.unlink(p)


def test_the_two_returns_are_reported_separately():
    """The stock's return from the trade-date close and the insider's return from what
    they paid are different numbers. Collapsing them into one 'pct_since_trade' made the
    insider look flat when they were up 67%."""
    p, con = _db()
    try:
        _buy(con, "BRVE", 1111, 18.0)
        con.execute(_PX, ("BRVE", "2026-06-01", 30.0, 30.0))
        con.execute(_PX, ("BRVE", "2026-08-07", 30.0, 30.0))
        r = _row(_feed(con), "BRVE")
        assert r["market_return_since_trade"] == 0.0, "the STOCK went nowhere"
        assert r["insider_return"] == 0.6667, "the INSIDER is up 67%"
        assert "pct_since_trade" not in r, "the ambiguous name is gone"
    finally:
        con.close()
        os.unlink(p)


def test_a_below_market_purchase_stays_rankable():
    """BRVE at $18 against a $30 close is a real below-market allocation, not an error.
    The guards must not swallow it - that is exactly the signal worth seeing."""
    p, con = _db()
    try:
        _buy(con, "BRVE", 1111, 18.0)
        con.execute(_PX, ("BRVE", "2026-06-01", 30.0, 30.0))
        con.execute(_PX, ("BRVE", "2026-08-07", 30.0, 30.0))
        r = _row(_feed(con), "BRVE")
        assert r["return_rankable"] is True and r["return_basis_warning"] is None
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- return basis

def test_a_share_basis_mismatch_withdraws_the_return_from_ranking():
    """LEGO reports a $0.003 execution against a $9.98 close - a 3,325x 'return' that is
    a reverse split. The number is kept and MARKED, never nulled."""
    p, con = _db()
    try:
        _buy(con, "LEGO", 1000, 0.003)
        con.execute(_PX, ("LEGO", "2026-06-01", 9.98, 9.98))
        con.execute(_PX, ("LEGO", "2026-08-07", 9.98, 9.98))
        r = _row(_feed(con), "LEGO")
        assert r["insider_return"] is not None, "the number is still visible"
        assert r["return_rankable"] is False
        assert "price_vs_close" in r["return_basis_warning"]
    finally:
        con.close()
        os.unlink(p)


def test_a_post_trade_corporate_action_is_caught_by_the_return_guard():
    """DBGI's execution price and trade-date close AGREE, so the basis test is silent -
    the distortion arrives later, in the latest close. A second guard is required."""
    p, con = _db()
    try:
        _buy(con, "DBGI", 1000, 0.70)
        con.execute(_PX, ("DBGI", "2026-06-01", 0.70, 0.70))
        con.execute(_PX, ("DBGI", "2026-08-07", 15.35, 15.35))   # post reverse split
        r = _row(_feed(con), "DBGI")
        assert r["return_basis_warning"] is not None
        assert "corporate_action" in r["return_basis_warning"]
        assert r["return_rankable"] is False
    finally:
        con.close()
        os.unlink(p)


def test_an_ordinary_gain_under_the_bar_stays_rankable():
    p, con = _db()
    try:
        _buy(con, "OK1", 1000, 10.0)
        con.execute(_PX, ("OK1", "2026-06-01", 10.0, 10.0))
        con.execute(_PX, ("OK1", "2026-08-07", 17.0, 17.0))      # +70%, plausible
        r = _row(_feed(con), "OK1")
        assert r["return_rankable"] is True and r["insider_return"] == 0.7
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- value quality

def test_price_equal_to_the_share_count_is_flagged():
    """KRMD: 1,191 shares at a 'price' of 1,191 -> $1.4m for a ~$3.90 stock. The filer put
    the quantity in the price field. Deterministic, needs no market data."""
    p, con = _db()
    try:
        _buy(con, "KRMD", 1191, 1191.0)
        assert _row(_feed(con), "KRMD")["value_quality"] == "price_equals_share_count"
    finally:
        con.close()
        os.unlink(p)


def test_a_price_far_from_its_ticker_peers_is_flagged():
    """CNTM: $4,576 against sibling filings near $8.90. Compared to other filers on the
    SAME ticker - the only reference that survives when there is no market data."""
    p, con = _db()
    try:
        for j, px in enumerate((8.83, 9.06, 8.93)):
            _buy(con, "CNTM", 500, px, person="Peer %d" % j, cik="p%d" % j)
        _buy(con, "CNTM", 600, 4576.0, person="Odd One", cik="odd")
        rows = [r for r in _feed(con) if r["ticker"] == "CNTM"]
        bad = [r for r in rows if r["value_quality"]]
        assert len(bad) == 1 and bad[0]["exec_price"] == 4576.0
        assert bad[0]["value_quality"] == "price_vs_ticker_peers"
    finally:
        con.close()
        os.unlink(p)


def test_the_peer_test_has_a_known_false_positive_and_says_review_not_error():
    """BGDE reports $1,000/share against ~$7 neighbours and is CORRECT - a Series D
    Convertible Preferred. We do not store security_title, so a different share class and
    a unit error are indistinguishable here. The marker must therefore be a REVIEW flag,
    and the row must survive."""
    p, con = _db()
    try:
        for j, px in enumerate((7.62, 7.33, 7.50)):
            _buy(con, "BGDE", 100, px, person="Common %d" % j, cik="c%d" % j)
        _buy(con, "BGDE", 16700, 1000.0, person="Preferred Buyer", cik="pref")
        rows = [r for r in _feed(con) if r["ticker"] == "BGDE"]
        assert len(rows) == 4, "the row is kept, not dropped"
        flagged = [r for r in rows if r["value_quality"] == "price_vs_ticker_peers"]
        assert len(flagged) == 1 and flagged[0]["value"] == 16700000.0


    finally:
        con.close()
        os.unlink(p)


def test_a_ticker_whose_every_row_is_distorted_is_caught_by_the_value_bar():
    """SVRE: 2.5bn shares in one purchase. Its own median price looks unremarkable, so a
    within-ticker peer test is structurally blind to it - and it is 46.8% of the corpus's
    gross buy value."""
    p, con = _db()
    try:
        _buy(con, "SVRE", 2501582400, 3.45)
        r = _row(_feed(con), "SVRE")
        assert r["value_quality"] == "value_above_1b_review"
    finally:
        con.close()
        os.unlink(p)


def test_an_ordinary_large_purchase_is_not_flagged():
    """p99 of the corpus is $25m. A $50m buy is big, not suspect."""
    p, con = _db()
    try:
        _buy(con, "BIG", 1000000, 50.0)
        assert _row(_feed(con), "BIG")["value_quality"] is None
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- issuer class

def test_a_fund_ticker_is_labelled_not_dropped():
    """A '40-Act fund sponsor subscribing into its own fund is not a CEO buying the
    company they run. Measured 62/62 precision on the X-suffix convention."""
    p, con = _db()
    try:
        _buy(con, "SCISX", 1000, 25.0, issuer="MA Eagle II Holdings Fund")
        r = _row(_feed(con), "SCISX")
        assert r["issuer_class"] == "fund_certain"
        assert r["value"] == 25000.0, "labelled, still fully present"
    finally:
        con.close()
        os.unlink(p)


def test_a_fund_by_issuer_name_is_labelled():
    p, con = _db()
    try:
        _buy(con, "GF", 1000, 10.0, issuer="Saba Capital Income & Opportunities Fund")
        assert _row(_feed(con), "GF")["issuer_class"] == "fund_named"
    finally:
        con.close()
        os.unlink(p)


def test_an_operating_company_is_not_mislabelled():
    """TRUST was tested and REJECTED as a signal - roughly half its matches are REITs and
    royalty trusts, i.e. real operating companies with real insider buying."""
    p, con = _db()
    try:
        _buy(con, "O", 1000, 55.0, issuer="Realty Income Trust")
        _buy(con, "AAPL", 100, 200.0, issuer="Apple Inc")
        rows = _feed(con)
        assert _row(rows, "O")["issuer_class"] == "operating"
        assert _row(rows, "AAPL")["issuer_class"] == "operating"
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- co-filing

def test_affiliated_filers_reporting_one_block_are_marked_not_merged():
    """SMMT: Duggan and Zanganeh each report the same 3.81m shares. No accession in the
    corpus carries two reporting owners, so this is never one filing read twice - it is
    separate filings of one economic block. Summing value over both double-counts."""
    p, con = _db()
    try:
        _buy(con, "SMMT", 3810000, 13.12, person="DUGGAN ROBERT W", cik="11")
        _buy(con, "SMMT", 3810000, 13.12, person="Zanganeh Mahkam", cik="22")
        rows = [r for r in _feed(con) if r["ticker"] == "SMMT"]
        assert len(rows) == 2, "both filings survive - neither is deleted"
        assert all(r["cofiling_suspected"] for r in rows)
        assert all(r["cofiler_count"] == 2 for r in rows)
    finally:
        con.close()
        os.unlink(p)


def test_one_filer_buying_alone_is_not_marked():
    p, con = _db()
    try:
        _buy(con, "SOLO", 1000, 10.0, person="Only One", cik="99")
        r = _row(_feed(con), "SOLO")
        assert r["cofiling_suspected"] is False and r["cofiler_count"] == 1
    finally:
        con.close()
        os.unlink(p)


def test_the_same_person_buying_twice_is_not_a_cofiling():
    """Grouping is by distinct reporting CIK, so one insider's repeated identical lots do
    not masquerade as several owners of one block."""
    p, con = _db()
    try:
        _buy(con, "REP", 1000, 10.0, person="Same Person", cik="7", date="2026-06-01")
        _buy(con, "REP", 1000, 10.0, person="Same Person", cik="7", date="2026-06-02")
        rows = [r for r in _feed(con) if r["ticker"] == "REP"]
        assert all(r["cofiler_count"] == 1 for r in rows)
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- security class

_F4T = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
        "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
        "value, ownership_after, tx_date, filed_date, role, ingest_regime, "
        "security_title) VALUES(?,0,?,?,'Acme Inc','9',?,'P',0,?,?,?,NULL,?,?,NULL,"
        "'watchlist',?)")


def _buy_titled(con, tk, shares, price, title, person="P", cik="1",
                date="2026-06-01", i=[0]):
    i[0] += 1
    con.execute(_F4T, ("t%d" % i[0], person, cik, tk, shares, price,
                       round(shares * price, 2), date, date, title))


def test_a_stated_preferred_title_explains_the_price_and_clears_the_flag():
    """BGDE's $1,000 against ~$7 common is a Series D Convertible Preferred - correct,
    and previously flagged as suspect only because securityTitle was parsed and then
    dropped at the INSERT. With the field present the row is explained, not reviewed."""
    p, con = _db()
    try:
        for j, px in enumerate((7.62, 7.33, 7.50)):
            _buy_titled(con, "BGDE", 100, px, "Common Stock",
                        person="C%d" % j, cik="c%d" % j)
        _buy_titled(con, "BGDE", 16700, 1000.0,
                    "Series D Convertible Preferred Stock",
                    person="Pref", cik="pref")
        rows = [r for r in _feed(con) if r["ticker"] == "BGDE"]
        odd = [r for r in rows if r["exec_price"] == 1000.0][0]
        assert odd["security_class"] == "non_common"
        assert odd["value_quality"] is None, "explained by the filer's own title"
    finally:
        con.close()
        os.unlink(p)


def test_an_unexplained_outlier_is_still_flagged_when_the_title_says_common():
    """The escape hatch must not swallow real errors: a COMMON-titled row far off its
    peers stays flagged."""
    p, con = _db()
    try:
        for j, px in enumerate((8.83, 9.06, 8.93)):
            _buy_titled(con, "CNTM", 500, px, "Common Stock",
                        person="P%d" % j, cik="p%d" % j)
        _buy_titled(con, "CNTM", 600, 4576.0, "Common Stock",
                    person="Odd", cik="odd")
        rows = [r for r in _feed(con) if r["ticker"] == "CNTM"]
        odd = [r for r in rows if r["exec_price"] == 4576.0][0]
        assert odd["security_class"] == "common"
        assert odd["value_quality"] == "price_vs_ticker_peers"
    finally:
        con.close()
        os.unlink(p)


def test_a_missing_title_leaves_the_row_reviewable_rather_than_excused():
    """Existing rows predate the column. unknown must NOT be treated as non_common, or
    the whole back corpus would silently excuse itself."""
    p, con = _db()
    try:
        for j, px in enumerate((8.83, 9.06, 8.93)):
            _buy_titled(con, "OLD", 500, px, None, person="P%d" % j, cik="p%d" % j)
        _buy_titled(con, "OLD", 600, 4576.0, None, person="Odd", cik="odd")
        rows = [r for r in _feed(con) if r["ticker"] == "OLD"]
        odd = [r for r in rows if r["exec_price"] == 4576.0][0]
        assert odd["security_class"] == "unknown"
        assert odd["value_quality"] == "price_vs_ticker_peers"
    finally:
        con.close()
        os.unlink(p)


def test_the_parser_output_reaches_the_column():
    """form4.py parsed security_title from the day it was written and dropped it at the
    Table I INSERT while writing it faithfully for derivatives."""
    from smart_money import form4
    p, con = _db()
    try:
        parsed = {"owner": "X", "owner_cik": "1", "issuer": "Acme", "issuer_cik": "9",
                  "plan_flag": False, "role": None,
                  "txns": [{"code": "P", "shares": "10", "price": "5",
                                    "date": "2026-06-01", "owned_after": "0",
                                    "security_title": "Common Stock"}]}
        form4.persist_transactions(con, "acc-1", parsed, "ZZZ", "2026-06-02")
        con.commit()
        assert con.execute("SELECT security_title FROM form4_transactions"
                           ).fetchone()[0] == "Common Stock"
    finally:
        con.close()
        os.unlink(p)
