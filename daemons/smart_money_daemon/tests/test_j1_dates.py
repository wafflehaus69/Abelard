"""ORDER SM-JAN2 J1 -- dates that cannot be true.

The nightly brief's footer printed 'Form 4 0025-07-25..2028-03-19. Congress
2012-09-13..3031-04-30', and PHASE4_OVERLAP printed the same from its own copy of the
query. Beneath that: ~800 Form 4 dates carried an EDGAR timezone suffix, 362 OGE filed
dates were M/D/YYYY and never once comparable, and one House typo (2220-04-07) left MMP
-- acquired in 2023 -- cached as a data gap instead of presumed delisted.
"""
import collections
import os
import sqlite3

import pytest

from smart_money import dates
from smart_money import db as dbmod
from smart_money import form4
from smart_money import oge_ingest as oge
from smart_money import queries as q
from smart_money import repair_dates as rd
from smart_money import survivorship

TODAY = "2026-09-28"


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "t.db")


# ------------------------------------------------------------------ the module --

@pytest.mark.parametrize("raw,want", [
    ("2026-07-03", "2026-07-03"),
    ("2026-07-03-05:00", "2026-07-03"),     # EDGAR xs:date with an offset
    ("2026-07-03+05:30", "2026-07-03"),
    ("2026-07-03Z", "2026-07-03"),
    (" 2026-07-03 ", "2026-07-03"),
    ("2026-13-45", None),                    # the shape, not a date
    ("07/03/2026", None),
    # Python 3.11+ fromisoformat accepts these; a date column must not.
    ("20200101", None), ("2020-W01-1", None),
    # Any tail that is not a timezone makes the value unparseable, never its head.
    ("2026-07-03garbage", None), ("2026-07-031", None), ("2026-07-03T10:00", None),
    ("", None), (None, None), (20260703, None),
])
def test_iso10_is_strict_about_the_whole_value(raw, want):
    assert dates.iso10(raw) == want


@pytest.mark.parametrize("raw,want", [
    ("04/10/2026", "2026-04-10"), ("1/4/2026", "2026-01-04"),
    ("13/45/2026", None),   # house_fd_ingest._iso_mdy would write '2026-13-45'
    ("01/14/20261", None),  # and '2026-01-14' for this
    ("2026-04-10", None), ("", None), (None, None),
])
def test_iso_mdy_refuses_rather_than_guesses(raw, want):
    assert dates.iso_mdy(raw) == want


@pytest.mark.parametrize("raw,direction,want", [
    ("2026-09-28", dates.BACKWARD, None),
    ("2026-09-29", dates.BACKWARD, None),                  # tomorrow, some timezone
    ("2026-09-30", dates.BACKWARD, dates.AFTER_CEILING),
    ("3031-04-30", dates.BACKWARD, dates.AFTER_CEILING),
    ("0025-07-25", dates.BACKWARD, dates.BEFORE_FLOOR),
    ("1989-12-31", dates.BACKWARD, dates.BEFORE_FLOOR),
    ("1990-01-01", dates.BACKWARD, None),
    ("2050-12-16", dates.FORWARD, None),                   # a real option expiry
    ("0025-07-25", dates.FORWARD, dates.BEFORE_FLOOR),
    ("garbage", dates.BACKWARD, dates.UNPARSEABLE),
    ("2026-07-03garbage", dates.BACKWARD, dates.UNPARSEABLE),
    ("20260703", dates.BACKWARD, dates.UNPARSEABLE),     # not 'after_ceiling'
    ("3031-04-30-05:00", dates.BACKWARD, dates.AFTER_CEILING),
])
def test_date_flag_judges_by_direction(raw, direction, want):
    assert dates.date_flag(raw, direction, today=TODAY) == want


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_an_absent_date_is_not_an_impossible_one(raw):
    """'No data' and 'impossible data' are different states."""
    assert dates.date_flag(raw, today=TODAY) is None
    assert dates.row_flag(raw, "2026-01-01", today=TODAY) is None
    assert dates.normalise(raw, today=TODAY) == (None, None)


@pytest.mark.parametrize("tx,filed,want", [
    ("2026-03-01", "2026-03-02", None),
    ("2026-03-03", "2026-03-02", None),                 # one day: a timezone apart
    ("2026-03-27", "2026-03-02", dates.AFTER_FILING),   # LRCX, live
    ("2019-12-21", "2019-01-11", dates.AFTER_FILING),   # IBKR, live: lag -344
    ("2026-01-01", "3031-01-01", "filed_after_ceiling"),
    ("2026-01-01", None, None),                          # no anchor, no order check
    ("3031-04-30", "2026-05-15", dates.AFTER_FILING),   # judged against its own filing
    ("3031-04-30", None, dates.AFTER_CEILING),          # no filing: only the calendar
    ("0025-07-25", "2025-07-25", dates.BEFORE_FLOOR),
])
def test_a_trade_cannot_follow_its_own_filing(tx, filed, want):
    """The today+1 ceiling weakens with time: BSET's 2026-11-12 on a 2026-01-12 filing
    passes every range check run after mid-November. The order rule does not decay."""
    assert dates.row_flag(tx, filed, today=TODAY) == want


@pytest.mark.parametrize("tx,filed,on_time,years_later,want", [
    # A year slip arriving ON TIME. With the ceiling tested first this was after_ceiling
    # with no sub-class and no suggestion: the label was reachable only years late.
    ("2027-12-18", "2027-01-20", "2027-01-21", "2028-01-05",
     (dates.AFTER_FILING, dates.YEAR_SLIP_PATTERN, "2026-12-18")),
    # CBDS, judged the day after its filing and long after.
    ("2026-09-17", "2026-09-14", "2026-09-15", "2030-01-01",
     (dates.AFTER_FILING, dates.FUTURE_DATED_OTHER, None)),
])
def test_a_rows_verdict_does_not_depend_on_the_day_it_is_judged(
        tx, filed, on_time, years_later, want):
    """Against its own filing, the verdict is a property of the row. It cannot be one
    thing at ingest and another at repair, and it cannot go stale as the date arrives."""
    cal = dates.Calendar(sessions={"2026-09-17"})
    assert dates.judge(tx, filed, cal, today=on_time) == want
    assert dates.judge(tx, filed, cal, today=years_later) == want


def test_normalise_keeps_the_evidence_of_a_bad_date():
    assert dates.normalise("2026-07-03-05:00", today=TODAY) == ("2026-07-03", None)
    assert dates.normalise("3031-04-30", today=TODAY) == (
        "3031-04-30", dates.AFTER_CEILING), "the filer's date stays; the row is marked"
    assert dates.normalise("garbage", today=TODAY) == ("garbage", dates.UNPARSEABLE)


def test_lag_is_arithmetic_and_the_flag_is_the_verdict():
    """congress_trades.lag_days is NOT NULL. A first cut wrote None for a quarantined
    date -- which would have failed the INSERT and taken the whole filing down on the
    first typo row. The lag stays the arithmetic of what was filed; date_flag voids it."""
    assert dates.lag_days("2026-05-15", "2026-05-01") == 14
    assert dates.lag_days("2026-05-15", "3031-04-30") < 0, "kept, not nulled"
    assert dates.lag_days("2026-05-15", None) is None


# ------------------------------------------------------------------ sub-classes --

SESSIONS = dates.Calendar(sessions={"2025-08-29", "2025-09-02", "2026-03-27",
                                    "2018-12-21", "2019-12-20"})


def test_the_calendar_knows_closed_from_unknown():
    assert SESSIONS.non_trading("2019-12-21") is True, "a Saturday needs no calendar"
    assert SESSIONS.non_trading("2025-09-01") is True, "Labor Day: a weekday, no session"
    assert SESSIONS.non_trading("2025-09-02") is False
    assert SESSIONS.non_trading("2030-01-02") is None, "beyond coverage is UNKNOWN"
    assert dates.Calendar(sessions=()).non_trading("2025-09-01") is None
    assert dates.Calendar(sessions=()).non_trading("2025-08-30") is True


@pytest.mark.parametrize("tx,filed,want_sub,want_suggested", [
    # December trade on a January filing of the same year -- the ruled pattern.
    ("2019-12-21", "2019-01-11", dates.YEAR_SLIP_PATTERN, "2018-12-21"),   # IBKR
    ("2023-12-07", "2023-01-05", dates.YEAR_SLIP_PATTERN, "2022-12-07"),   # MA
    # Dated after the filing, on a day the market was closed.
    ("2025-09-01", "2025-08-26", dates.NON_TRADING_DAY, None),             # GOOGL
    # Dated after the filing, on an ordinary session.
    ("2026-03-27", "2026-03-02", dates.FUTURE_DATED_OTHER, None),          # LRCX
    # A November trade on a January filing looks like a slip and is NOT the pattern.
    ("2016-11-23", "2016-01-12", dates.FUTURE_DATED_OTHER, None),
])
def test_an_after_filing_row_is_named_for_its_string(tx, filed, want_sub, want_suggested):
    flag, sub, sugg = dates.judge(tx, filed, SESSIONS, today=TODAY)
    assert flag == dates.AFTER_FILING
    assert (sub, sugg) == (want_sub, want_suggested)


def test_the_year_slip_pattern_outranks_the_weekend():
    """IBKR's 2019-12-21 is also a Saturday. The pattern is the more specific
    description, and the suggestion it carries lands on a session."""
    _, sub, sugg = dates.judge("2019-12-21", "2019-01-11", SESSIONS, today=TODAY)
    assert sub == dates.YEAR_SLIP_PATTERN
    assert SESSIONS.non_trading(sugg) is False


def test_a_suggestion_is_never_the_date():
    """tx_date_suggested is a labelled hypothesis. Nothing writes it into tx_date."""
    assert dates.normalise("2019-12-21", today=TODAY)[0] == "2019-12-21"
    assert "hypothesis" in dates.SUGGESTION_RULE.lower()


@pytest.mark.parametrize("market,want", [(True, dates.NON_TRADING_DAY), (False, None)])
def test_a_closed_market_is_a_mark_only_for_a_market_execution(market, want):
    """A general non_trading_day QUARANTINE would have caught 25,755 Form 4 rows -- a
    fifth of all tax withholdings -- because vestings land on calendar dates. Only an
    open-market trade is judged, and even then it is marked, not quarantined."""
    flag, sub, sugg = dates.judge("2025-08-30", "2025-09-02", SESSIONS,
                                  market_execution=market, today=TODAY)
    assert flag is None, "implausible is not impossible: the row stays on every board"
    assert (sub, sugg) == (want, None)


@pytest.mark.parametrize("raw,want", [("0025-07-25", dates.CENTURY_SLIP),
                                      ("0202-05-01", dates.SHORT_YEAR)])
def test_a_short_year_is_named_for_its_digits(raw, want):
    """0202 is a dropped digit, not a century. The name says what is on the page."""
    assert dates.judge(raw, "2022-05-03", today=TODAY)[:2] == (dates.BEFORE_FLOOR, want)


def test_quarter_ends():
    assert all(dates.is_quarter_end(d) for d in
               ("2026-03-31", "2026-06-30", "2026-09-30", "2026-12-31"))
    assert not dates.is_quarter_end("2026-06-29")
    assert not dates.is_quarter_end(None)


# ------------------------------------------------------------------ the writers --

def _parsed(txn_date, deriv=None):
    return {"owner": "Insider", "owner_cik": "1", "issuer": "Co", "issuer_cik": "9",
            "plan_flag": False, "role": None,
            "txns": [{"code": "P", "shares": "10", "price": "5", "date": txn_date}],
            "deriv_txns": deriv or []}


def test_form4_writer_strips_the_offset_and_marks_the_impossible(path):
    con = dbmod.connect(path)
    try:
        form4.persist_transactions(con, "TZ", _parsed("2026-07-03-05:00"), "AAA",
                                   "2026-07-05")
        form4.persist_transactions(con, "BAD", _parsed("3031-04-30"), "AAA", "2026-05-01")
        form4.persist_transactions(con, "LATE", _parsed("2026-03-27"), "LRCX", "2026-03-02")
        got = dict(con.execute("SELECT accession, tx_date || '|' || "
                               "COALESCE(date_flag,'') FROM form4_transactions"))
        assert got["TZ"] == "2026-07-03|"
        assert got["BAD"] == "3031-04-30|after_filing", "kept as filed, and marked"
        assert got["LATE"] == "2026-03-27|after_filing"
        assert con.execute("SELECT date_subclass FROM form4_transactions WHERE "
                           "accession='LATE'").fetchone()[0] == dates.FUTURE_DATED_OTHER
    finally:
        con.close()


def test_form4_writer_marks_a_weekend_purchase_and_leaves_a_weekend_vesting(path):
    con = dbmod.connect(path)
    try:
        sat = "2026-06-06"
        form4.persist_transactions(con, "BUY", _parsed(sat), "AAA", "2026-06-08")
        vest = _parsed(sat)
        vest["txns"][0]["code"] = "F"
        form4.persist_transactions(con, "VEST", vest, "AAA", "2026-06-08")
        got = {a: (f, s) for a, f, s in con.execute(
            "SELECT accession, date_flag, date_subclass FROM form4_transactions")}
        assert got["BUY"] == (None, dates.NON_TRADING_DAY), "marked, not quarantined"
        assert got["VEST"] == (None, None), "a withholding lands when the vesting does"
    finally:
        con.close()


def test_derivative_writer_judges_the_trade_date_only(path):
    """An expiry decades out is a promise, not an error. A flat today+1 ceiling across
    every date column would have quarantined 165,854 legal expiries."""
    con = dbmod.connect(path)
    try:
        leg = {"code": "A", "shares": "1", "price": "0", "date": "2026-01-02",
               "exercise_date": "2027-01-02", "expiration_date": "2050-12-16"}
        form4.persist_derivatives(con, "D1", _parsed("2026-01-02", [leg]), "AAA",
                                  "2026-01-03")
        form4.persist_derivatives(con, "D2", _parsed("2026-01-02",
                                                     [dict(leg, date="2036-07-30")]),
                                  "AAA", "2026-01-03")
        got = dict(con.execute("SELECT accession, COALESCE(date_flag,'') "
                               "FROM form4_derivatives"))
        assert got == {"D1": "", "D2": "after_filing"}
        assert con.execute("SELECT expiration_date FROM form4_derivatives "
                           "WHERE accession='D1'").fetchone()[0] == "2050-12-16"
    finally:
        con.close()


def _ptr_row(tx_date):
    return {"asset": ["International Business Machines", "(IBM)", "[ST]"],
            "amount": ["$1,001 - $15,000"], "tx_type": "P", "tx_date": tx_date,
            "owner": "SP"}


def test_a_house_filing_with_a_typo_row_still_ingests(monkeypatch, tmp_path, path):
    """THE regression pin for the NOT NULL lag_days bug a first cut would have shipped."""
    from smart_money import house_ingest as hi
    monkeypatch.setattr(hi, "fetch_pdf", lambda *a, **k: tmp_path / "x.pdf")
    monkeypatch.setattr(hi, "parse_ptr_pdf", lambda p: (
        [_ptr_row("04/30/3031"), _ptr_row("04/01/2026"), _ptr_row("06/01/2026")],
        "electronic"))
    con = dbmod.connect(path)
    try:
        filing = {"DocID": "20020914", "FilingDate": "05/15/2026", "Last": "Doe",
                  "First": "J", "StateDst": "XX01", "Year": "2026"}
        assert hi.ingest_filing(con, filing, 2026, tmp_path, "ua") == "electronic"
        got = {tx: (flag, lag) for tx, flag, lag in con.execute(
            "SELECT tx_date, COALESCE(date_flag,''), lag_days FROM congress_trades")}
        assert got["2026-04-01"] == ("", 44)
        assert got["3031-04-30"][0] == "after_filing"
        assert isinstance(got["3031-04-30"][1], int)
        assert got["2026-06-01"][0] == "after_filing", "traded after its own disclosure"
    finally:
        con.close()


def test_a_house_year_slip_carries_its_suggestion_and_keeps_its_date(
        monkeypatch, tmp_path, path):
    """IBKR, live: a trade dated 2019-12-21 on a disclosure of 2019-01-11."""
    from smart_money import house_ingest as hi
    monkeypatch.setattr(hi, "fetch_pdf", lambda *a, **k: tmp_path / "x.pdf")
    monkeypatch.setattr(hi, "parse_ptr_pdf",
                        lambda p: ([_ptr_row("12/21/2019")], "electronic"))
    con = dbmod.connect(path)
    try:
        filing = {"DocID": "20010975", "FilingDate": "01/11/2019", "Last": "Doe",
                  "First": "J", "StateDst": "XX01", "Year": "2019"}
        hi.ingest_filing(con, filing, 2019, tmp_path, "ua")
        assert con.execute("SELECT tx_date, date_flag, date_subclass, tx_date_suggested "
                           "FROM congress_trades").fetchone() == (
            "2019-12-21", "after_filing", "year_slip_pattern", "2018-12-21")
    finally:
        con.close()


def test_a_senate_filing_with_a_typo_row_still_ingests(monkeypatch, tmp_path, path):
    from smart_money import efd_ingest as ei
    (tmp_path / "ptr_U1.html").write_text("cached")
    cells = ["Self", "IBM", "IBM Corp", "Stock", "Purchase", "$1,001 - $15,000", "--"]
    monkeypatch.setattr(ei, "parse_ptr_table", lambda raw, uuid: [
        ["1", "04/30/3031"] + cells, ["2", "04/01/2026"] + cells])
    con = dbmod.connect(path)
    try:
        filing = {"uuid": "U1", "kind": "ptr", "office": "Doe, Jane (Senator)",
                  "first": "Jane", "last": "Doe", "label": "PTR", "filed": "2026-05-15"}
        assert ei.ingest_filing(con, None, filing, tmp_path) == "electronic"
        assert dict(con.execute("SELECT tx_date, COALESCE(date_flag,'') "
                                "FROM congress_trades")) == {
            "3031-04-30": "after_filing", "2026-04-01": ""}
    finally:
        con.close()


def test_oge_dates_are_iso_and_the_label_survives(path):
    con = dbmod.connect(path)
    try:
        rows = [{"line_no": "1", "description": "X", "ticker": None, "eif": None,
                 "value_lo": None, "value_hi": None, "income_type": None,
                 "income_lo": None, "income_hi": None}]
        for doc, raw in (("A", "04/10/2026"), ("B", "2026-04-10"), ("C", "sometime"),
                         ("D", "1/14/0025")):
            oge.ingest(con, doc, "F", "Nominee 278", raw, rows, "u")
        got = {d: (f, raw) for d, f, raw in con.execute(
            "SELECT doc_id, filed_date, filed_date_raw FROM oge_holdings")}
        assert got["A"] == ("2026-04-10", "04/10/2026")
        assert got["B"] == ("2026-04-10", "2026-04-10"), "ISO in is not refused"
        assert got["C"] == (None, "sometime"), "refused, never guessed, bytes kept"
        assert got["D"] == (None, "1/14/0025"), "well-formed but impossible: refused"
    finally:
        con.close()


def test_a_13f_period_that_is_not_a_quarter_end_fails_the_filer(monkeypatch):
    """The fallback swaps FILING dates in for every period when reportDate is absent,
    and none of them is a quarter-end."""
    from smart_money import thirteenf, thirteenf_ingest as ti

    class R:
        status_code = 200

        def __init__(self, recent):
            self._r = recent

        def json(self):
            return {"filings": {"recent": self._r}}

    monkeypatch.setattr(ti.time, "sleep", lambda s: None)
    good = {"form": ["13F-HR"], "accessionNumber": ["A1"], "filingDate": ["2026-08-14"],
            "reportDate": ["2026-06-30"]}
    monkeypatch.setattr(ti.requests, "get", lambda *a, **k: R(good))
    assert ti.list_13f_filings("1", "c")[0]["period"] == "2026-06-30"
    bad = {k: v for k, v in good.items() if k != "reportDate"}
    monkeypatch.setattr(ti.requests, "get", lambda *a, **k: R(bad))
    with pytest.raises(ValueError, match="not a quarter-end"):
        ti.list_13f_filings("1", "c")
    assert thirteenf  # imported for its PACE/_ua, used by the module under test


# ------------------------------------------------------------------ the readers --

def _registry_persons(con):
    # q_sentinel_log fails loud on registry seeds missing from persons; the brief
    # renders it, so the fixture carries them (as test_brief.py's does).
    entries, _ = q._load_registry()
    for e in entries:
        if e.get("person_id") is not None:
            con.execute("INSERT OR IGNORE INTO persons(person_id, name, type, "
                        "cik_or_chamber) VALUES(?,?,?,?)",
                        (e["person_id"], e["name"], "congress", e.get("chamber") or "house"))


def _holding(con, accession, period):
    con.execute("INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
                "put_call, value, shares, ingested_at_unix) "
                "VALUES('1',?,?,?,'C','long',1,1,0)", (accession, period, period))


_F4 = ("INSERT INTO form4_transactions(accession, tx_index, reporting_person, "
       "reporting_cik, issuer, issuer_cik, ticker, code, plan_flag, shares, price, "
       "value, tx_date, filed_date, ingest_regime, date_flag) "
       "VALUES(?,0,'P','1','Co','9',?,?,0,10,5,50,?,?,?,?)")
_CT = ("INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
       "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, asset_type, "
       "filing_id, date_flag) VALUES(1,?,'purchase',1001,15000,?,?,?,'house',"
       "'house_clerk',?,'Stock','F',?)")


def _corpus(path, marked=True):
    """The live defects. marked=False stores them the way production holds them BEFORE
    repair_dates runs: impossible dates, no flags."""
    con = dbmod.connect(path)
    f = (lambda flag: flag) if marked else (lambda flag: None)
    for acc, tk, code, tx, filed, reg, flag in (
            ("A1", "AAA", "P", "2024-03-01", "2024-03-02", "watchlist", None),
            ("A2", "AAA", "P", "2026-06-01", "2026-06-02", "universal", None),
            ("A3", "BBB", "P", "2002-02-24", "2026-02-25", "universal", None),
            ("U1", "CCC", "P", "2025-07-20", "2025-07-23", "universal", None),
            ("GS", "GS", "S", "0025-07-25", "2025-07-25", "universal", "before_floor"),
            ("PR", "PRCH", "A", "2028-03-19", "2026-03-20", "universal", "after_filing")):
        con.execute(_F4, (acc, tk, code, tx, filed, reg, f(flag)))
    for tk, tx, disc, lag, ref, flag in (
            ("MMP", "2023-09-26", "2023-10-20", 24, "r1", None),
            ("MMP", "2220-04-07", "2022-05-01", -72000, "r2", "after_filing"),
            ("IBM", "3031-04-30", "2026-05-15", -368891, "r3", "after_filing"),
            ("AAA", "2025-01-10", "2025-02-01", 22, "r4", None)):
        con.execute(_CT, (tk, tx, disc, lag, ref, f(flag)))
    # A quarantined congressional BOND row: outside the stock window, and still counted.
    con.execute("INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
                "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, asset_type, "
                "filing_id, date_flag) VALUES(1,NULL,'purchase',1001,15000,'2022-12-31',"
                "'2022-05-27',-218,'house','house_clerk','r5','Corporate Bond','F',?)",
                (f("after_filing"),))
    con.execute("INSERT INTO form4_derivatives(accession, tx_index, ticker, code, tx_date, "
                "filed_date, ingest_regime, date_flag) VALUES('D',0,'APMD','A',"
                "'2036-07-30','2026-08-03','universal',?)", (f("after_filing"),))
    for period in ("2023-12-31", "2026-06-30"):
        _holding(con, "X" + period, period)
    _registry_persons(con)
    con.commit()
    con.close()
    return path


def test_the_windows_exclude_what_cannot_be_true(path):
    con = q.connect_ro(_corpus(path))
    try:
        cw = q.q_corpus_windows(con)
        assert (cw["form4"]["lo"], cw["form4"]["hi"]) == ("2002-02-24", "2026-06-01")
        assert cw["form4"]["quarantined"] == 2
        assert cw["form4"]["derivatives_quarantined"] == 1
        assert (cw["congress"]["lo"], cw["congress"]["hi"]) == ("2023-09-26", "2025-01-10")
        assert cw["congress"]["quarantined"] == 3, "every asset type, the bond included"
        assert cw["thirteenf"]["off_quarter"] == []
    finally:
        con.close()


def test_the_congress_line_says_what_it_windows_and_what_it_counts(path):
    """The window is over STOCK trades and always was. Scoped the same way, the
    quarantine count reported 19 of 27 live rows and left eight bonds, options and
    untyped rows counted nowhere."""
    con = q.connect_ro(_corpus(path))
    try:
        line = q.corpus_window_lines(q.q_corpus_windows(con))[1]
        assert line.startswith("Congress stock trades 2023-09-26..2025-01-10")
        assert ("3 congressional rows of every asset type quarantined on the trade clock "
                "(3 dated after their own filing and still valid on the disclosure "
                "clock)") in line, line
    finally:
        con.close()


def test_before_the_repair_the_footer_says_so(path):
    """THE blocker. A first cut trusted date_flag alone, so on a migrated but unrepaired
    database it printed the impossible endpoints with 'none quarantined' under a heading
    claiming every row could be true -- worse than the footer it replaced. The range is
    now enforced on read, and an unmarked impossible date is its own stated state."""
    con = q.connect_ro(_corpus(path, marked=False))
    try:
        cw = q.q_corpus_windows(con)
        assert (cw["form4"]["lo"], cw["form4"]["hi"]) == ("2002-02-24", "2026-06-01")
        assert (cw["congress"]["lo"], cw["congress"]["hi"]) == ("2023-09-26", "2025-01-10")
        assert cw["form4"]["quarantined"] == 0
        assert cw["form4"]["not_yet_marked"] == 2
        assert cw["form4"]["derivatives_not_yet_marked"] == 1
        assert cw["congress"]["not_yet_marked"] == 2
        text = " ".join(q.corpus_window_lines(cw))
        assert "NOT YET MARKED" in text and "repair has not been applied" in text
        assert "none quarantined" not in text, "the lie the first cut told"
        for impossible in ("0025", "2028-03-19", "3031", "2220"):
            assert impossible not in text, impossible
    finally:
        con.close()


def test_an_endpoint_set_by_the_tail_is_named_as_the_tail(path):
    """Over sane rows the window opens 2002-02-24 for a corpus whose first filing is
    years later; printing a quarantine count beside that alone would assert the rest
    are clean."""
    con = q.connect_ro(_corpus(path))
    try:
        cw = q.q_corpus_windows(con)
        assert cw["form4"]["collection_starts"] == "2024-03-02"
        assert cw["form4"]["dated_before_collection"] == 1, "A3 (2002) only, not A1"
        text = " ".join(q.corpus_window_lines(cw))
        assert "filings collected from 2024-03-02" in text
        assert "late reports, amendments of old filings, or date errors" in text
        assert ("2 transactions and 1 derivative rows quarantined on the trade clock "
                "(2 dated after their own filing and still valid on the disclosure "
                "clock, 1 dated before 1990)") in text, text
    finally:
        con.close()


# ------------------------------------------------------------------ two clocks --

def _two_clock_corpus(path):
    """A year-slip row and a clean one for the same member and ticker."""
    con = dbmod.connect(path)
    con.execute("INSERT INTO persons(person_id, name, type, cik_or_chamber) "
                "VALUES(1,'Doe, Jane','congress','house')")
    ins = ("INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
           "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, asset_type, "
           "filing_id, date_flag, date_subclass, tx_date_suggested) VALUES(1,'IBKR',"
           "'purchase',1001,15000,?,?,?,'house','house_clerk',?,'Stock',?,?,?,?)")
    con.execute(ins, ("2019-12-21", "2019-01-11", -344, "a#1", "A", "after_filing",
                      "year_slip_pattern", "2018-12-21"))
    con.execute(ins, ("2019-01-02", "2019-01-20", 18, "b#1", "B", None, None, None))
    con.commit()
    con.close()
    return path


def test_a_quarantined_trade_is_still_a_whole_disclosure(path):
    """THE ruling. A trade dated after its own filing is impossible on the trade clock
    and perfectly valid on the disclosure clock. The sentinel feed runs on the
    disclosure clock, so the row stays in it -- and loses only its lag, a number
    computed from a trade date that cannot be true."""
    con = q.connect_ro(_two_clock_corpus(path))
    try:
        res = q.q_sentinel_log(con, window=3650, anchor="2019-06-30", entries=[
            {"name": "Doe, Jane", "role": "member", "person_id": 1}])
        by = {r["tx_date"]: r for r in res["rows"]}
        assert set(by) == {"2019-12-21", "2019-01-02"}, "nothing is hidden"
        slip = by["2019-12-21"]
        assert slip["event_date"] == "2019-01-11", "placed by its filing"
        assert slip["lag_days"] is None, "-344 is not a lag"
        assert "year_slip_pattern" in slip["date_note"]
        assert "suggested 2018-12-21" in slip["date_note"]
        assert "hypothesis, not a correction" in slip["date_note"]
        assert slip["tx_date"] == "2019-12-21", "the filed date is never rewritten"
        assert by["2019-01-02"]["lag_days"] == 18 and by["2019-01-02"]["date_note"] is None
    finally:
        con.close()


def test_the_tension_block_does_not_let_a_quarantined_trade_vote(path):
    """A window on the trade clock. PINS showed three congressional sells where two
    could be placed; the third voted from wherever its mistyped date landed."""
    con = q.connect_ro(_two_clock_corpus(path))
    try:
        leg = q.q_surface_tension(con, "IBKR", window=180, anchor="2019-12-31")["legs"][
            "congress"]
        assert leg["buy"] == 0, "the only buy in this window is the quarantined one"
        assert leg["as_of"] is None, "and it does not set the leg's as-of date"
        wide = q.q_surface_tension(con, "IBKR", window=400, anchor="2019-12-31")["legs"][
            "congress"]
        assert wide["buy"] == 1 and wide["as_of"] == "2019-01-02", "the clean row counts"
    finally:
        con.close()


def test_a_mark_note_says_what_the_string_shows_and_no_more():
    """'The market was closed' would be an assumed cause. Among the live marks are
    Taipei trades on US Labor Day and a merger cash-out that closed on a Sunday."""
    note = q.date_note(None, dates.NON_TRADING_DAY, None)
    assert "no US session" in note
    assert "closed" not in note and "off the trade clock" not in note


def test_the_sentinel_export_carries_the_note(path):
    """A blank lag with no reason would be a number silently removed. The CSV
    completeness contract makes the note leave with the data."""
    from smart_money import dashboard as dash
    assert "date_note" in dash._SENTINEL_CSV_COLS
    con = q.connect_ro(_two_clock_corpus(path))
    try:
        rows = q.q_sentinel_log(con, window=3650, anchor="2019-06-30", entries=[
            {"name": "Doe, Jane", "role": "member", "person_id": 1}])["rows"]
        csv_text = dash._csv_bytes(dash._SENTINEL_CSV_COLS, rows)
        assert "year_slip_pattern" in csv_text
    finally:
        con.close()


class _Overlay:
    min_persons, window_days = 2, 30

    def match(self, ticker):
        return None, None


def _cluster_corpus(path):
    """Two members buy PINS on real dates a week apart; a third 'buys' it on a date
    that follows its own disclosure."""
    con = dbmod.connect(path)
    ins = ("INSERT INTO congress_trades(person_id, ticker, side, amt_low, amt_high, "
           "tx_date, disclosure_date, lag_days, chamber, source, raw_ref, asset_type, "
           "filing_id, date_flag) VALUES(?,'PINS','purchase',1001,15000,?,?,0,'house',"
           "'house_clerk',?,'Stock',?,?)")
    con.execute(ins, (1, "2026-06-02", "2026-06-20", "a", "A", None))
    con.execute(ins, (2, "2026-06-09", "2026-06-20", "b", "B", None))
    con.execute(ins, (3, "2026-06-11", "2026-06-01", "c", "C", "after_filing"))
    con.commit()
    return con


def test_a_quarantined_trade_is_in_no_cluster_span(path):
    """A cluster is a span on the trade clock. Counted, the flagged row would have made
    a three-member cluster out of two."""
    from smart_money import events
    con = _cluster_corpus(path)
    try:
        cl = events.cluster_flag(con, "PINS", "buy", "2026-06-12", 2, 30)
        assert cl["n_persons"] == 2
    finally:
        con.close()


def test_a_quarantined_disclosure_is_still_an_event(path):
    """Emitted, because the disclosure is real. Without a lag or a cluster, because
    both are computed from a trade date that cannot be true."""
    from smart_money import events
    con = _cluster_corpus(path)
    try:
        args = (1, "congress", "Doe, Jane", None, None, "PINS", "purchase", "stock",
                (1001, 15000))
        tail = (None, "house_clerk", "C", _Overlay(), con)
        clean = events.make_event(*args, "2026-06-09", "2026-06-20", 11, *tail)
        assert clean["lag_days"] == 11 and clean["flags"]["cluster"]["n_persons"] == 2
        assert clean["flags"]["trade_date"] is None

        ev = events.make_event(*args, "2026-06-11", "2026-06-01", -10, *tail,
                               date_flag="after_filing",
                               date_subclass="future_dated_other")
        assert ev["disclosure_date"] == "2026-06-01", "placed by its filing"
        assert ev["tx_date"] == "2026-06-11", "as filed; part of the event's identity"
        assert ev["lag_days"] is None, "-10 is not a lag"
        assert ev["flags"]["cluster"] is None
        assert ev["flags"]["trade_date"] == {
            "flag": "after_filing", "subclass": "future_dated_other", "suggested": None}
    finally:
        con.close()


def test_the_ticker_panel_lists_disclosures_on_the_disclosure_clock(path):
    """Ordered on tx_date, a panel opened with whichever trade was mistyped furthest
    into the future. Every disclosure is listed; the quarantined one says why."""
    con = q.connect_ro(_two_clock_corpus(path))
    try:
        rows = q.q_ticker_panel(con, "IBKR", anchor="2019-06-30")["congress"]
        assert [r["disclosure_date"] for r in rows] == ["2019-01-20", "2019-01-11"]
        assert "year_slip_pattern" in rows[1]["note"]
        assert not rows[0].get("note")
        assert all("_date_note" not in r for r in rows)
    finally:
        con.close()


def test_the_filing_deadline_is_not_the_tail(path):
    """A1 traded 2024-03-01 and filed 2024-03-02 -- the very first filing. That is the
    Form 4 deadline at work, not a late report."""
    con = q.connect_ro(_corpus(path))
    try:
        cw = q.q_corpus_windows(con)
        assert cw["form4"]["dated_before_collection"] == 1
        assert "more than 5 days before that" in q.corpus_window_lines(cw)[0]
    finally:
        con.close()


def test_each_regime_states_its_own_start(path):
    """93% of live Form 4 rows come from a sweep that began in 2025; one start date for
    both regimes would overstate the depth."""
    con = q.connect_ro(_corpus(path))
    try:
        cw = q.q_corpus_windows(con)
        assert cw["form4"]["collection_by_regime"] == [
            ("watchlist", "2024-03-02"), ("universal", "2025-07-23")]
        assert "(watchlist from 2024-03-02, universal from 2025-07-23)" in \
            q.corpus_window_lines(cw)[0]
    finally:
        con.close()


def test_a_corpus_with_no_usable_date_still_reports_its_quarantine():
    cw = {"lo": None, "hi": None, "collection_starts": None, "dated_before_collection": 0,
          "deadline_days": 5, "quarantined": 7, "not_yet_marked": 0, "absent": 0}
    line = q.corpus_window_lines({"form4": cw, "congress": cw,
                                  "thirteenf": {"lo": None, "off_quarter": []}})[0]
    assert "no trades with a usable date" in line and "7 transactions quarantined" in line


def test_a_malformed_filed_date_cannot_become_the_corpus_start(path):
    """One '12/01/2025' sorting first would have been parsed with fromisoformat and
    taken the whole brief footer down."""
    _corpus(path)
    con = dbmod.connect(path)
    con.execute(_F4, ("M", "MMM", "P", "2026-01-02", "12/01/2025", "universal", None))
    con.commit()
    con.close()
    con = q.connect_ro(path)
    try:
        assert q.q_corpus_windows(con)["form4"]["collection_starts"] == "2024-03-02"
    finally:
        con.close()


def test_an_off_quarter_13f_period_is_named(path):
    _corpus(path)
    con = dbmod.connect(path)
    _holding(con, "Y", "2026-06-29")
    con.commit()
    con.close()
    con = q.connect_ro(path)
    try:
        cw = q.q_corpus_windows(con)
        assert cw["thirteenf"]["off_quarter"] == ["2026-06-29"]
        assert "NOT a quarter-end" in q.corpus_window_lines(cw)[2]
    finally:
        con.close()


def test_the_brief_footer_no_longer_prints_the_typos(path, tmp_path):
    import pdfplumber
    from smart_money import brief
    con = q.connect_ro(_corpus(path, marked=False))
    out = str(tmp_path / "b.pdf")
    try:
        brief.render_brief(con, out, anchor="2026-06-30")
        with pdfplumber.open(out) as doc:
            text = " ".join(" ".join((p.extract_text() or "") for p in doc.pages).split())
        assert "Corpus windows" in text and "NOT YET MARKED" in text
        for impossible in ("0025-07-25", "2028-03-19", "3031-04-30"):
            assert impossible not in text, impossible
    finally:
        con.close()


def test_phase4_overlap_uses_the_same_windows(path):
    """The second printer. It had its own raw MIN/MAX and wrote 3031 to disk."""
    from smart_money import phase4_joins as p4
    con = q.connect_ro(_corpus(path))
    try:
        md = p4._render(con, TODAY, {}, [], collections.defaultdict(list), [],
                        collections.defaultdict(list), {})
        assert "late reports, amendments of old filings" in md
        for impossible in ("0025-07-25", "2028-03-19", "3031-04-30"):
            assert impossible not in md, impossible
    finally:
        con.close()


@pytest.mark.parametrize("marked", [True, False])
def test_a_quarantined_trade_reaches_no_board(path, marked):
    """Marked or not yet marked: the GS sale dated 0025-07-25 was the one live row the
    all-time flows window let through."""
    con = q.connect_ro(_corpus(path, marked=marked))
    try:
        got = {r["accession"] for r in
               q._fetch_f4(con, ("P", "S"), "0001-01-01", "9999-12-31", plan="all")}
        assert "GS" not in got
        assert {"A1", "A2", "A3"} <= got
    finally:
        con.close()


@pytest.mark.parametrize("marked", [True, False])
def test_survivorship_reads_past_the_typo(path, marked):
    con = dbmod.connect(_corpus(path, marked=marked))
    try:
        assert survivorship._last_trade(con, "MMP") == "2023-09-26"
    finally:
        con.close()


def test_a_cached_verdict_can_be_recomputed_deliberately(path):
    """The cache is why a corrected input changes nothing on its own."""
    con = dbmod.connect(_corpus(path))
    try:
        con.execute("INSERT INTO ticker_status VALUES('MMP','data_gap','2220-04-07',0,?)",
                    (survivorship.HEURISTIC,))
        con.commit()
        assert survivorship.classify(con, today=TODAY, probe=False)["skipped_cached"] >= 1
        survivorship.classify(con, today=TODAY, probe=False, tickers=["MMP"], force=True)
        assert con.execute("SELECT verdict, last_trade_date FROM ticker_status WHERE "
                           "ticker='MMP'").fetchone() == ("delisted_presumed", "2023-09-26")
    finally:
        con.close()


# ------------------------------------------------------------------ the repair --

def _unmigrated(path):
    """A database exactly as production holds it before this change: the defects
    stored raw, and none of the new columns."""
    con = dbmod.connect(path)
    for acc, tk, code, tx, filed in (
            ("OK", "AAA", "P", "2026-06-01", "2026-06-02"),
            ("TZ", "AAA", "P", "2026-07-03-05:00", "2026-07-05"),
            ("GS", "GS", "P", "0025-07-25", "2025-07-25"),
            ("PR", "PRCH", "P", "2028-03-19", "2026-03-20"),
            ("LATE", "LRCX", "P", "2026-03-27", "2026-03-02"),
            ("SATBUY", "AAA", "P", "2026-06-06", "2026-06-08"),    # a Saturday purchase
            ("SATVEST", "AAA", "F", "2026-06-06", "2026-06-08")):  # a Saturday vesting
        con.execute(_F4, (acc, tk, code, tx, filed, "universal", None))
    con.execute(_CT, ("IBM", "3031-04-30", "2026-05-15", -368891, "r", None))
    con.execute(_CT, ("IBKR", "2019-12-21", "2019-01-11", -344, "s", None))
    rows = [{"line_no": "1", "description": "X", "ticker": None, "eif": None,
             "value_lo": None, "value_hi": None, "income_type": None,
             "income_lo": None, "income_hi": None}]
    oge.ingest(con, "DOC", "F", "278", "04/10/2026", rows, "u")
    con.execute("UPDATE oge_holdings SET filed_date='04/10/2026'")
    con.commit()
    for table in ("form4_transactions", "form4_derivatives", "congress_trades"):
        for col in ("date_flag", "date_subclass", "tx_date_suggested"):
            con.execute("ALTER TABLE {} DROP COLUMN {}".format(table, col))
    con.execute("ALTER TABLE oge_holdings DROP COLUMN filed_date_raw")
    con.commit()
    con.close()
    return path


def _has(path, table, col):
    c = sqlite3.connect(path)
    try:
        return col in {r[1] for r in c.execute("PRAGMA table_info({})".format(table))}
    finally:
        c.close()


def test_the_dry_run_writes_nothing_not_even_the_schema(path, capsys):
    _unmigrated(path)
    before = open(path, "rb").read()
    assert rd.main(["--db", path]) == 0
    assert open(path, "rb").read() == before, "not one byte"
    assert not _has(path, "form4_transactions", "date_flag")
    out = capsys.readouterr().out
    assert "PASS 2 QUARANTINE on the TRADE clock: 5 rows" in out
    assert "after_filing" in out and "century_slip" in out
    assert "year_slip_pattern" in out and "suggest 2018-12-21" in out
    assert "PASS 2b MARK, NOT quarantine: 1 purchases and sales" in out
    assert "no US session" in out and "market was closed" not in out


def test_the_backup_is_taken_before_the_migration(path, tmp_path, capsys):
    _unmigrated(path)
    assert rd.main(["--db", path, "--apply"]) == 0
    snaps = [p for p in os.listdir(str(tmp_path)) if p.startswith("pre_date_sanity_")]
    assert len(snaps) == 1
    snap = str(tmp_path / snaps[0])
    assert not _has(snap, "form4_transactions", "date_flag"), \
        "the snapshot is the database as it WAS, schema included"
    assert _has(path, "form4_transactions", "date_flag")
    con = sqlite3.connect(path)
    try:
        f4 = {a: (tx, f, s) for a, tx, f, s in con.execute(
            "SELECT accession, tx_date, date_flag, date_subclass FROM form4_transactions")}
        assert f4 == {
            "OK": ("2026-06-01", None, None),
            "TZ": ("2026-07-03", None, None),
            "GS": ("0025-07-25", "before_floor", "century_slip"),
            "PR": ("2028-03-19", "after_filing", "non_trading_day"),    # a Sunday
            "LATE": ("2026-03-27", "after_filing", "future_dated_other"),
            "SATBUY": ("2026-06-06", None, "non_trading_day"),   # marked, on every board
            "SATVEST": ("2026-06-06", None, None)}
        ct = {tk: rest for tk, *rest in con.execute(
            "SELECT ticker, tx_date, date_flag, date_subclass, tx_date_suggested, "
            "lag_days FROM congress_trades")}
        assert ct["IBM"] == ["3031-04-30", "after_filing", "non_trading_day", None,
                             -368891], "lag_days is NOT NULL; the flag voids it"
        assert ct["IBKR"] == ["2019-12-21", "after_filing", "year_slip_pattern",
                              "2018-12-21", -344], "suggested beside it, never applied"
        assert con.execute("SELECT filed_date, filed_date_raw FROM oge_holdings"
                           ).fetchone() == ("2026-04-10", "04/10/2026")
    finally:
        con.close()
    out = capsys.readouterr().out
    assert "re-scan   tz=0 quarantine=0 marks=0 oge=0" in out
    folder = [ln.split()[-1] for ln in out.splitlines() if ln.startswith("artifacts")][0]
    with open(os.path.join(folder, "tz_normalised.csv")) as fh:
        assert "2026-07-03-05:00" in fh.read(), "the original bytes were kept"


def test_nothing_to_do_takes_no_backup(path, tmp_path, capsys):
    """Including with --reclassify, when the only cache row left is one this tool
    reports and never touches. Counting that row wrote a fresh 700 MB backup on every
    later run, to change nothing."""
    _unmigrated(path)
    con = sqlite3.connect(path)
    con.execute("INSERT INTO market_cap(ticker, cik, shares, shares_asof, concept, band, "
                "computed_at_unix) VALUES('AAL','0000006201',1,'2027-07-17','x','mid',0)")
    con.commit()
    con.close()
    rd.main(["--db", path, "--apply"])
    capsys.readouterr()
    for extra in ([], ["--reclassify"]):
        rd.main(["--db", path, "--apply"] + extra)
        assert "Nothing to apply" in capsys.readouterr().out, extra
    assert len([p for p in os.listdir(str(tmp_path))
                if p.startswith("pre_date_sanity_")]) == 1


def test_counts_are_rows_changed_not_rows_attempted(path, tmp_path):
    con = dbmod.connect(_unmigrated(path))
    try:
        found = rd.scan(con, today=TODAY)
        first = rd.apply(con, found, str(tmp_path / "a1"))
        assert first == {"tz": 1, "quarantine": 5, "marks": 1, "oge": 1}
        again = rd.apply(con, found, str(tmp_path / "a2"))   # the same, stale scan
        assert sum(again.values()) == 0
    finally:
        con.close()


def test_the_repair_refuses_to_overwrite_a_backup(path, tmp_path):
    _unmigrated(path)
    open(str(tmp_path / "pre_date_sanity_x.db"), "w").close()
    with pytest.raises(SystemExit):
        rd.backup(path, "x")


def test_the_repair_and_the_writer_cannot_disagree(path):
    """Both go through dates.judge. A row the repair marks is a row a fresh ingest of
    the same filing would have marked the same way."""
    con = dbmod.connect(path)
    try:
        form4.persist_transactions(con, "W", _parsed("2026-03-27"), "LRCX", "2026-03-02")
        written = con.execute("SELECT date_flag, date_subclass, tx_date_suggested "
                              "FROM form4_transactions").fetchone()
        con.execute("UPDATE form4_transactions SET date_flag=NULL, date_subclass=NULL")
        con.commit()
        r = rd.scan(con, today=TODAY)["quarantine"][0]
        assert (r["reason"], r["subclass"], r["suggested"]) == written
    finally:
        con.close()
