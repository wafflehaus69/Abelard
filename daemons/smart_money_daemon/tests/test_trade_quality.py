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
        for j, px in enumerate((3.96, 3.93, 3.91)):
            _buy(con, "KRMD", 5000, px, person="Peer %d" % j, cik="k%d" % j)
        _buy(con, "KRMD", 1191, 1191.0, person="Tharby Linda M", cik="th")
        rows = [r for r in _feed(con) if r["ticker"] == "KRMD"]
        bad = [r for r in rows if r["value_quality"]]
        assert len(bad) == 1 and bad[0]["value_quality"] == "price_equals_share_count"
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
        # a genuine co-filing: both report the SAME beneficial block, so the holding
        # after the transaction is identical
        _buy_titled(con, "SMMT", 3810000, 13.12, "Common Stock",
                    person="DUGGAN ROBERT W", cik="11", owned_after=9000000)
        _buy_titled(con, "SMMT", 3810000, 13.12, "Common Stock",
                    person="Zanganeh Mahkam", cik="22", owned_after=9000000)
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
        "security_title) VALUES(?,0,?,?,'Acme Inc','9',?,'P',0,?,?,?,?,?,?,NULL,"
        "'watchlist',?)")


def _buy_titled(con, tk, shares, price, title, person="P", cik="1",
                date="2026-06-01", owned_after=None, i=[0]):
    i[0] += 1
    con.execute(_F4T, ("t%d" % i[0], person, cik, tk, shares, price,
                       round(shares * price, 2), owned_after, date, date, title))


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


# ---------------------------------------------------------------- the parse guard

def test_the_close_check_does_not_null_a_legitimate_preferred():
    """The close cross-check compares against the COMMON stock's close, so it is only
    meaningful for common stock. BGDE's Series D Preferred at $1,000 is 112x its common
    close and entirely correct - flagging it would NULL a real $16.7m purchase, which is
    exactly what a re-parse would have done before the title was persisted."""
    from smart_money import form4
    assert form4.value_sanity_flag(
        16700, 1000.0, 16700000.0, close=8.92,
        security_title="Series D Convertible Preferred Stock") is None
    assert form4.value_sanity_flag(
        16700, 1000.0, 16700000.0, close=8.92,
        security_title="American Depositary Shares") is None


def test_the_close_check_still_fires_on_common_stock():
    """The exemption must not become a blanket amnesty."""
    from smart_money import form4
    assert form4.value_sanity_flag(
        100, 1000.0, 100000.0, close=8.92,
        security_title="Common Stock") == "price_vs_close"
    # and an unknown title stays checked - the back corpus must not excuse itself
    assert form4.value_sanity_flag(
        100, 1000.0, 100000.0, close=8.92, security_title=None) == "price_vs_close"


def test_the_absolute_ceilings_are_not_exempted_by_a_title():
    """A preferred title explains a price ABOVE the common close; it does not explain a
    price above every US equity ever traded."""
    from smart_money import form4
    assert form4.value_sanity_flag(
        1, 50000.0, 50000.0, close=8.92,
        security_title="Series A Preferred Stock") == "price_over_max"


# ---------------------------------------------------------------- clean subset

def test_the_aggregate_subset_drops_untrustworthy_values():
    p, con = _db()
    try:
        _buy_titled(con, "GOOD", 1000, 10.0, "Common Stock", person="A", cik="1")
        for j, px in enumerate((3.96, 3.93, 3.91)):
            _buy_titled(con, "KRMD", 5000, px, "Common Stock",
                        person="Peer %d" % j, cik="k%d" % j)
        _buy_titled(con, "KRMD", 1191, 1191.0, "Common Stock", person="T", cik="th")
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        clean = q.clean_subset(raw)
        assert not [r for r in clean if r["price"] == 1191.0], "the bad row is gone"
        assert [r for r in clean if r["ticker"] == "GOOD"], "the good row survives"
    finally:
        con.close()
        os.unlink(p)


def test_the_aggregate_subset_drops_fund_issuers():
    """LNBIX alone is $250,000,006 of sponsor subscription - not insider conviction."""
    p, con = _db()
    try:
        _buy_titled(con, "GOOD", 1000, 10.0, "Common Stock", person="A", cik="1")
        con.execute(_F4T, ("f1", "Sponsor", "9", "SCISX", 1000, 25.0, 25000.0,
                           None, "2026-06-01", "2026-06-01", "Common Stock"))
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert [r["ticker"] for r in q.clean_subset(raw)] == ["GOOD"]
    finally:
        con.close()
        os.unlink(p)


def test_a_cofiled_block_is_collapsed_to_one_not_removed_entirely():
    """Removing every co-filed row would delete the economic event. Exactly one survives,
    deterministically, so the block is counted once rather than twice or zero times."""
    p, con = _db()
    try:
        _buy_titled(con, "SMMT", 3810000, 13.12, "Common Stock",
                    person="DUGGAN ROBERT W", cik="22", owned_after=9000000)
        _buy_titled(con, "SMMT", 3810000, 13.12, "Common Stock",
                    person="Zanganeh Mahkam", cik="11", owned_after=9000000)
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert len(raw) == 2, "both filings exist in the raw view"
        clean = q.clean_subset(raw)
        assert len(clean) == 1, "counted once"
        assert clean[0]["reporting_cik"] == "11", "lowest CIK wins, deterministically"
        assert q.clean_subset(raw)[0]["reporting_cik"] == "11", "stable across runs"
    finally:
        con.close()
        os.unlink(p)


def test_two_genuinely_different_lots_are_both_kept():
    """Collapsing keys on (ticker, date, shares, price) - a different size or price is a
    different event and must survive."""
    p, con = _db()
    try:
        _buy_titled(con, "X", 100, 10.0, "Common Stock", person="A", cik="1")
        _buy_titled(con, "X", 200, 10.0, "Common Stock", person="B", cik="2")
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert len(q.clean_subset(raw)) == 2
    finally:
        con.close()
        os.unlink(p)


def test_the_trades_feed_still_shows_everything_the_aggregate_excludes():
    """The residue must stay inspectable - the feed is the place it stays visible."""
    p, con = _db()
    try:
        for j, px in enumerate((3.96, 3.93, 3.91)):
            _buy_titled(con, "KRMD", 5000, px, "Common Stock",
                        person="Peer %d" % j, cik="k%d" % j)
        _buy_titled(con, "KRMD", 1191, 1191.0, "Common Stock", person="T", cik="th")
        rows = [r for r in _feed(con) if r["ticker"] == "KRMD"]
        assert len(rows) == 4, "the feed keeps every row"
        assert [r for r in rows if r["value_quality"]], "and marks the bad one"
    finally:
        con.close()
        os.unlink(p)


def test_contamination_propagates_to_the_FILER_for_aggregation():
    """SVRE reports 18 rows on one distorted basis and only 7 clear the $1bn bar
    individually. Excluding just those left $4.19bn still topping the board - a partial
    clean that reads as a clean one. Unit corruption is a property of the filing
    convention, so it propagates to the ticker for TOTALS."""
    p, con = _db()
    try:
        _buy_titled(con, "SVRE", 2501582400, 3.45, "Common Stock",
                    person="VisionWave", cik="1")           # >$1bn, trips the bar
        _buy_titled(con, "SVRE", 100000000, 3.45, "Common Stock",
                    person="VisionWave", cik="1", date="2026-06-02")   # under the bar
        _buy_titled(con, "OK", 1000, 10.0, "Common Stock", person="B", cik="2")
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert ("SVRE", "1") in q.contaminated_filers(raw)
        assert [r["ticker"] for r in q.clean_subset(raw)] == ["OK"], (
            "both SVRE rows go, not just the one over the bar")
    finally:
        con.close()
        os.unlink(p)


def test_a_clean_filer_on_a_dirty_ticker_keeps_its_dollars():
    p, con = _db()
    try:
        _buy_titled(con, "SVRE", 2501582400, 3.45, "Common Stock", person="V", cik="1")
        _buy_titled(con, "AAPL", 1000, 200.0, "Common Stock", person="B", cik="2")
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert q.contaminated_filers(raw) == {("SVRE", "1")}
    finally:
        con.close()
        os.unlink(p)


def test_the_feed_still_shows_every_row_of_a_contaminated_ticker():
    """Propagation governs TOTALS only. The rows stay individually visible and only the
    ones that actually tripped a test carry a marker."""
    p, con = _db()
    try:
        _buy_titled(con, "SVRE", 2501582400, 3.45, "Common Stock", person="V", cik="1")
        _buy_titled(con, "SVRE", 100000, 3.45, "Common Stock", person="W", cik="2",
                    date="2026-06-02")
        rows = [r for r in _feed(con) if r["ticker"] == "SVRE"]
        assert len(rows) == 2, "both rows visible in the feed"
        assert sum(1 for r in rows if r["value_quality"]) == 1, (
            "only the row that tripped a test is marked")
    finally:
        con.close()
        os.unlink(p)


def test_an_innocent_insider_on_a_contaminated_ticker_is_not_punished():
    """Propagating across the whole ticker would zero the dollars of everyone who
    happened to buy the same stock, for someone else's filing convention."""
    p, con = _db()
    try:
        _buy_titled(con, "CCC", 40000000, 40000000.0, "Common Stock",
                    person="Bad Filer", cik="bad")
        _buy_titled(con, "CCC", 100, 10.0, "Common Stock",
                    person="Ordinary Insider", cik="good", date="2026-06-02")
        con.commit()
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        kept = q.clean_subset(raw)
        assert [r["reporting_cik"] for r in kept] == ["good"]
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- the rendered table

def test_the_trades_table_shows_what_the_insider_paid():
    """The row dict and the CSV carried exec_price, but the visible table did not - it
    still showed only the trade-date close, which is what a reader takes for the price.
    BRVE read back $30 when the insiders paid $18."""
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        _buy_titled(con, "BRVE", 1111, 18.0, "Common Stock")
        con.execute(_PX, ("BRVE", "2026-06-01", 30.0, 30.0))
        con.execute(_PX, ("BRVE", "2026-08-07", 30.0, 30.0))
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            out = dash.view_trades(ro, dash._params(
                {"scope": ["all"], "window": ["400"], "anchor": ["2026-08-07"]}))
            assert ">paid<" in out, "the execution price needs its own column"
            assert ">close<" in out, "and the close must say it is the close"
            assert "18" in out and "30" in out, "both numbers on the row"
            assert "execution price from the filing" in out
        finally:
            ro.close()
    finally:
        os.unlink(p)


def test_a_below_market_purchase_is_emphasised_in_the_table():
    """$18 into a $30 close is a 40% discount - the signal the old table hid."""
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        _buy_titled(con, "BRVE", 1111, 18.0, "Common Stock")
        con.execute(_PX, ("BRVE", "2026-06-01", 30.0, 30.0))
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            out = dash.view_trades(ro, dash._params(
                {"scope": ["all"], "window": ["400"], "anchor": ["2026-08-07"]}))
            assert "vs the trade-date close" in out
        finally:
            ro.close()
    finally:
        os.unlink(p)


def test_two_people_taking_identical_tranches_are_not_a_cofiling():
    """BRVE: Murdoch and Viehbacher each bought 83,333 shares at $18 in the same
    placement and are plainly separate buyers - their post-transaction holdings differ
    (2,940,670 vs 609,519). Keying on (ticker, date, shares, price) alone called them one
    block and would have deleted a real $1.5m purchase from every total."""
    p, con = _db()
    try:
        _buy_titled(con, "BRVE", 83333, 18.0, "Common Stock",
                    person="Murdoch Travis", cik="m", owned_after=2940670)
        _buy_titled(con, "BRVE", 83333, 18.0, "Common Stock",
                    person="Viehbacher Christopher", cik="v", owned_after=609519)
        rows = [r for r in _feed(con) if r["ticker"] == "BRVE"]
        assert len(rows) == 2
        assert not any(r["cofiling_suspected"] for r in rows), "separate buyers"
        raw = q._fetch_f4(con, ("P",), "2020-01-01", "2026-08-07", plan="all")
        assert len(q.clean_subset(raw)) == 2, "both purchases counted"
    finally:
        con.close()
        os.unlink(p)


# ---------------------------------------------------------------- code labels

def test_every_code_the_corpus_actually_uses_has_a_plain_english_label():
    """A bare letter is unreadable without the SEC table memorised, and the panel showed
    nothing else. These 18 are the codes present in the live corpus."""
    for code in ("S", "A", "F", "M", "P", "D", "G", "J", "C", "L", "U", "X", "I", "W",
                 "Z", "O", "E", "H", "K", "V"):
        assert q.tx_code_label(code), "no label for %s" % code


def test_the_labels_say_the_right_thing():
    assert q.tx_code_label("P") == "Open-market buy"
    assert q.tx_code_label("S") == "Open-market sale"
    assert q.tx_code_label("M") == "Option exercise"
    assert q.tx_code_label("F") == "Tax withholding"
    assert q.tx_code_label("A") == "Grant / award"
    assert q.tx_code_label("C") == "Conversion"
    assert q.tx_code_label("G") == "Gift"


def test_an_unknown_code_is_not_guessed_at():
    assert q.tx_code_label("QQ") is None
    assert q.tx_code_label(None) is None
    assert q.tx_code_label("") is None


def test_codes_are_matched_case_and_space_insensitively():
    assert q.tx_code_label(" p ") == "Open-market buy"


def test_non_cash_codes_are_marked_so_a_zero_value_reads_as_expected():
    """19,800,000 shares at value 0.00 looks like broken data until you know a conversion
    has no market price."""
    p, con = _db()
    try:
        con.execute(_F4T, ("m1", "X", "1", "AAA", 157213, None, None, None,
                           "2026-06-01", "2026-06-01", "Common Stock"))
        con.execute("UPDATE form4_transactions SET code='M' WHERE accession='m1'")
        con.commit()
        panel = q.q_ticker_panel(con, "AAA")
        row = panel["insider_by_code"][0]
        assert row["what"] == "Option exercise"
        assert row["cash"] == "no", "an exercise is not a cash purchase"
    finally:
        con.close()
        os.unlink(p)


def test_an_open_market_buy_is_marked_as_cash():
    p, con = _db()
    try:
        _buy_titled(con, "AAA", 2000, 51.175, "Common Stock")
        con.commit()
        row = q.q_ticker_panel(con, "AAA")["insider_by_code"][0]
        assert row["what"] == "Open-market buy" and row["cash"] == "yes"
    finally:
        con.close()
        os.unlink(p)


def test_the_panel_page_explains_the_codes():
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        _buy_titled(con, "AAA", 2000, 51.175, "Common Stock")
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            out = dash.view_ticker(ro, dash._params({"symbol": ["AAA"]}))
            assert "Insider activity by transaction type" in out
            assert "Open-market buy" in out
            assert "value of 0.00 on those" in out, "explains the zero-value rows"
        finally:
            ro.close()
    finally:
        os.unlink(p)


def test_the_panel_says_which_period_each_block_covers():
    """TSLA shows 26 open-market buys worth 1bn dollars in the all-time table and '0 buyers,
    distributing' in the 180-day pressure block directly beneath it. Unlabelled the two
    read as a contradiction; they are a difference of window."""
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        _buy_titled(con, "AAA", 2000, 51.175, "Common Stock", date="2020-01-02")
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            out = dash.view_ticker(ro, dash._params({"symbol": ["AAA"]}))
            assert "transaction type &mdash; all time" in out
            assert "WHOLE filing history" in out
            assert "Ownership pressure &mdash; last 180d" in out
            assert "difference of period, not a" in out
        finally:
            ro.close()
    finally:
        os.unlink(p)


def test_the_code_table_uses_readable_column_names():
    p, con = _db()
    try:
        _buy_titled(con, "AAA", 2000, 51.175, "Common Stock")
        con.commit()
        row = q.q_ticker_panel(con, "AAA")["insider_by_code"][0]
        for k in ("what", "cash", "10b5-1", "filings", "filers"):
            assert k in row, k
        for gone in ("plan_flag", "n", "distinct_filers"):
            assert gone not in row, gone
    finally:
        con.close()
        os.unlink(p)
