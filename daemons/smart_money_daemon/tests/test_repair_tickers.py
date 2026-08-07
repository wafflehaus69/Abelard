"""SM-C3: repairing the account-label ticker contamination in place.

The load-bearing property is CONSERVATISM. A repair must require positive evidence that
a stored value is wrong; absence of evidence that it is right is not the same thing.
"""
import os
import tempfile

from smart_money import db as dbmod, repair_tickers as rt

_H = ("INSERT INTO congress_holdings(doc_id, chamber, coverage_year, member_last, "
      "member_first, state_dist, row_idx, asset_name, ticker, asset_type, "
      "ingested_at_unix) VALUES(?,?,2025,'M','A','NC01',?,?,?,'ST',0)")


def _db(rows):
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    con = dbmod.connect(p)
    for i, (cham, name, tk) in enumerate(rows):
        con.execute(_H, ("d%d" % i, cham, i, name, tk))
    con.commit()
    return p, con


def test_a_401k_label_is_cleared_and_the_real_symbol_recovered():
    p, con = _db([("house",
                   "Vanguard Mid-Cap Index Fund Admiral Shares (VIMAX)  Vanguard - "
                   "401(K)", "K")])
    try:
        rows = rt.scan(con)
        assert len(rows) == 1 and rows[0][3] == "VIMAX", rows
        assert rt.classify(*rows[0][2:]) == "recovered_real_symbol"
    finally:
        con.close()
        os.unlink(p)


def test_a_bare_plan_row_is_cleared_to_null_not_guessed():
    p, con = _db([("house", "NFL 401(k) Savings Plan Target Date 2045 Fund", "K")])
    try:
        rows = rt.scan(con)
        assert len(rows) == 1 and rows[0][3] is None
    finally:
        con.close()
        os.unlink(p)


def test_a_correct_ticker_absent_from_the_stored_name_is_left_alone():
    """THE conservatism test. asset_name is stored cleaned and truncated, so a name that
    does not contain its ticker is not evidence the ticker is wrong. An earlier cut of
    this tool would have destroyed VXF, VWO, VEA and VB on exactly this shape."""
    p, con = _db([
        ("house", "VANGUARD FTSE DEVELOPED MARKETS   CHARLES SCHWAB BROKERAGE ACCOUNT [1]",
         "VEA"),
        ("house", "VANGUARD SMALL CAP   CHARLES SCHWAB BROKERAGE ACCOUNT [2]", "VB"),
    ])
    try:
        assert rt.scan(con) == [], "no positive evidence of error -> no change"
    finally:
        con.close()
        os.unlink(p)


def test_kellogg_is_not_swept_up_with_the_401k_rows():
    p, con = _db([("house", "Kellanova Common Stock (K)", "K")])
    try:
        assert rt.scan(con) == []
    finally:
        con.close()
        os.unlink(p)


def test_account_words_are_cleared():
    p, con = _db([("house", "Schwab Brokerage (IRA)", "IRA"),
                  ("house", "Fidelity (ROLLOVER)", "ROLLOVER")])
    try:
        rows = rt.scan(con)
        assert {r[2] for r in rows} == {"IRA", "ROLLOVER"}
        assert all(r[3] is None for r in rows)
    finally:
        con.close()
        os.unlink(p)


def test_senate_rows_are_never_touched():
    """A Senate ticker comes from the filing's yahoo link, which the stored name never
    carried. Re-deriving it from the name would destroy good data."""
    p, con = _db([("senate", "Some Plan 401(k)", "K"),
                  ("senate", "Schwab (IRA)", "IRA")])
    try:
        assert rt.scan(con) == []
    finally:
        con.close()
        os.unlink(p)


def test_apply_writes_exactly_the_scanned_rows_and_is_idempotent():
    p, con = _db([("house", "NFL 401(k) Savings Plan", "K"),
                  ("house", "Apple Inc (AAPL)", "AAPL")])
    try:
        n = rt.apply(con, rt.scan(con))
        assert n == 1
        left = con.execute("SELECT ticker FROM congress_holdings ORDER BY row_idx"
                           ).fetchall()
        assert left == [(None,), ("AAPL",)], left
        assert rt.scan(con) == [], "a second run has nothing left to do"
    finally:
        con.close()
        os.unlink(p)


def test_report_names_the_conservatism_rule():
    p, con = _db([("house", "NFL 401(k) Savings Plan", "K")])
    try:
        txt = rt.report(rt.scan(con))
        assert "POSITIVE evidence" in txt
        assert "its silence is not" in txt
        assert "Senate rows are excluded" in txt
    finally:
        con.close()
        os.unlink(p)
