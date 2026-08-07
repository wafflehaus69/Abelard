"""SM-C3 Phase X: three-surface tension.

The load-bearing rule is that only a surface expressing a DIRECTION may vote. Breadth is
a level: a holder count falls identically on a sale and on a lapsed filing, so putting it
on a direction axis would manufacture disagreements out of a filing calendar.
"""
import os
import tempfile

from smart_money import db as dbmod, queries as q

_F4 = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
       "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
       "value, ownership_after, tx_date, filed_date, role, ingest_regime) "
       "VALUES(?,?,'P',?,'Co','9',?,?,?,?,1.0,1,NULL,?,?,NULL,'watchlist')")
_CT = ("INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, tx_date, "
       "disclosure_date, lag_days, chamber, source, raw_ref, owner, asset_type, "
       "filing_id, superseded) VALUES(1,?,?,1001,15000,?,?,0,'house','x',?,'Self',"
       "'Stock',?,0)")
_CH = ("INSERT INTO congress_holdings(doc_id, chamber, coverage_year, member_last, "
       "member_first, state_dist, row_idx, asset_name, ticker, asset_type, value_lo, "
       "value_hi, ingested_at_unix) VALUES(?,?,?,?,'A',?,?,'Asset',?,'ST',?,?,0)")


def _db():
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = dbmod.connect(p)
    con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, cik_or_chamber) "
                "VALUES(1,'M, A','congress','house')")
    return p, con


def _f4(con, tk, code, shares, date, i=[0], cik="1"):
    i[0] += 1
    con.execute(_F4, ("a%d" % i[0], i[0], cik, tk, code, 0, shares, date, date))


def _ptr(con, tk, side, date, i=[0]):
    i[0] += 1
    con.execute(_CT, (tk, side, date, date, "r%d" % i[0], "f%d" % i[0]))


def _tension(con, tk, **kw):
    con.commit()
    return q.q_surface_tension(con, tk, anchor="2026-06-30", **kw)


def test_insider_selling_against_congress_buying_is_a_tension():
    p, con = _db()
    try:
        _f4(con, "TSLA", "S", 5000, "2026-06-01")
        for d in ("2026-05-01", "2026-05-10", "2026-05-20"):
            _ptr(con, "TSLA", "purchase", d)
        t = _tension(con, "TSLA")
        assert t["tension"] is True and t["agreement"] is False
        assert t["legs"]["insider"]["direction"] == "distributing"
        assert t["legs"]["congress"]["direction"] == "accumulating"
        assert sorted(t["surfaces_with_direction"]) == ["congress", "insider"]
    finally:
        con.close()
        os.unlink(p)


def test_both_surfaces_agreeing_is_not_a_tension():
    p, con = _db()
    try:
        _f4(con, "NVDA", "S", 5000, "2026-06-01")
        _ptr(con, "NVDA", "sale", "2026-05-01")
        t = _tension(con, "NVDA")
        assert t["tension"] is False and t["agreement"] is True
        assert t["consensus"] == "distributing"
    finally:
        con.close()
        os.unlink(p)


def test_one_surface_alone_can_neither_agree_nor_disagree():
    """A tension needs two voters. One surface talking to itself is not a disagreement."""
    p, con = _db()
    try:
        _f4(con, "AAPL", "S", 5000, "2026-06-01")
        t = _tension(con, "AAPL")
        assert t["surfaces_with_direction"] == ["insider"]
        assert t["tension"] is False and t["agreement"] is False
        assert t["consensus"] is None
    finally:
        con.close()
        os.unlink(p)


def test_a_flat_surface_does_not_vote():
    p, con = _db()
    try:
        _f4(con, "AAPL", "P", 100, "2026-06-01")
        _f4(con, "AAPL", "S", 100, "2026-06-02")      # nets to zero
        _ptr(con, "AAPL", "purchase", "2026-05-01")
        t = _tension(con, "AAPL")
        assert t["legs"]["insider"]["direction"] == "flat"
        assert t["surfaces_with_direction"] == ["congress"]
    finally:
        con.close()
        os.unlink(p)


def test_breadth_is_a_level_and_never_votes():
    """THE Phase X rule. 40 members holding a ticker is not a direction, and a count that
    drops because a member stopped filing must not read as selling."""
    p, con = _db()
    try:
        for i in range(40):
            con.execute(_CH, ("d%d" % i, "house", 2025, "M%d" % i, "NC%02d" % i, i,
                              "AAPL", 1001, 15000))
        _f4(con, "AAPL", "S", 5000, "2026-06-01")
        t = _tension(con, "AAPL")
        assert t["congress_level"]["holder_count"] == 40
        assert t["congress_level"]["is_level"] is True
        assert "congress" not in t["surfaces_with_direction"], "breadth must not vote"
        assert t["tension"] is False, "one voter plus a level is not a disagreement"
        assert "does NOT vote" in t["note"]
    finally:
        con.close()
        os.unlink(p)


def test_planned_insider_sales_are_excluded_from_direction():
    """A 10b5-1 sale is a calendar, not a decision."""
    p, con = _db()
    try:
        con.execute(_F4, ("z", 1, "1", "AAPL", "S", 1, 9999, "2026-06-01", "2026-06-01"))
        t = _tension(con, "AAPL")
        assert t["legs"]["insider"]["direction"] == "flat"
    finally:
        con.close()
        os.unlink(p)


def test_trades_outside_the_window_do_not_count():
    p, con = _db()
    try:
        _f4(con, "AAPL", "S", 5000, "2020-01-01")
        t = _tension(con, "AAPL", window=90)
        assert t["legs"]["insider"]["direction"] == "flat"
        assert t["legs"]["insider"]["as_of"] is None
    finally:
        con.close()
        os.unlink(p)


def test_the_as_of_spread_is_reported_so_the_clock_is_visible():
    """A fresh insider sale against a quarter-old 13F may be the calendar, not the
    parties. The spread has to be on the result."""
    p, con = _db()
    try:
        _f4(con, "AAPL", "S", 5000, "2026-06-20")
        _ptr(con, "AAPL", "purchase", "2026-04-01")
        t = _tension(con, "AAPL")
        assert t["as_of_spread_days"] == 80, t["as_of_spread_days"]
        assert "may be the calendar" in t["staleness"]
    finally:
        con.close()
        os.unlink(p)


def test_congress_level_counts_each_member_once_on_their_latest_year():
    p, con = _db()
    try:
        con.execute(_CH, ("a", "house", 2024, "Smith", "NC01", 0, "AAPL", 1001, 15000))
        con.execute(_CH, ("b", "house", 2025, "Smith", "NC01", 1, "AAPL", 1001, 15000))
        con.execute(_CH, ("c", "senate", 2025, "Jones", None, 2, "AAPL", 1001, 15000))
        lvl = _tension(con, "AAPL")["congress_level"]
        assert lvl["holder_count"] == 2 and lvl["house"] == 1 and lvl["senate"] == 1
        assert lvl["anchor_years"] == [2025]
    finally:
        con.close()
        os.unlink(p)


def test_congress_level_uses_the_band_floor_never_an_invented_ceiling():
    p, con = _db()
    try:
        con.execute(_CH, ("a", "house", 2025, "Smith", "NC01", 0, "AAPL",
                          50000001, None))
        assert _tension(con, "AAPL")["congress_level"]["floor_exposure"] == 50000001
    finally:
        con.close()
        os.unlink(p)


def test_panel_carries_the_congress_level_and_the_tension():
    p, con = _db()
    try:
        con.execute(_CH, ("a", "house", 2025, "Smith", "NC01", 0, "AAPL", 1001, 15000))
        _f4(con, "AAPL", "S", 5000, "2026-06-01")
        con.commit()
        panel = q.q_ticker_panel(con, "AAPL", anchor="2026-06-30")
        assert panel["congress_holdings"]["holder_count"] == 1
        assert panel["tension"]["ticker"] == "AAPL"
        assert "congress_level" in panel["tension"]
    finally:
        con.close()
        os.unlink(p)


# ---- the ticker panel renders it ----

def test_panel_page_states_the_verdict_and_that_breadth_does_not_vote():
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        _f4(con, "TSLA", "S", 5000, "2026-06-01")
        for d in ("2026-05-01", "2026-05-10", "2026-05-20"):
            _ptr(con, "TSLA", "purchase", d)
        for i in range(7):
            con.execute(_CH, ("d%d" % i, "house", 2025, "M%d" % i, "NC%02d" % i, i,
                              "TSLA", 1001, 15000))
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            html_out = dash.view_ticker(ro, dash._params({"symbol": ["TSLA"]}))
            assert "Three-surface tension" in html_out
            assert "TENSION" in html_out and "the surfaces disagree" in html_out
            assert "breadth does not vote here" in html_out
            assert "7 members" in html_out
            assert "may be the calendar rather than the" in html_out
            assert "votes" in html_out
        finally:
            ro.close()
    finally:
        os.unlink(p)


def test_panel_page_says_when_there_is_no_read():
    from smart_money import dashboard as dash
    p, con = _db()
    try:
        con.commit()
        con.close()
        ro = q.connect_ro(p)
        try:
            out = dash.view_ticker(ro, dash._params({"symbol": ["ZZZZ"]}))
            assert "No read" in out and "fewer than two surfaces" in out
        finally:
            ro.close()
    finally:
        os.unlink(p)
