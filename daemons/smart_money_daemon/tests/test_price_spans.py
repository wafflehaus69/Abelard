"""Cache spans must record what was RECEIVED, never what was REQUESTED.

prices.eod recorded the requested end date as the covered span. grade_case asks for
entry + 200 days — a future date — so SPY's span was written as ending 2027-02-07.
_covered() then answered True for every later request, eod() skipped the fetch, and
the cache SELECT (which has no completeness check) returned stale rows and raised
nothing. SPY froze at 2026-07-24 while the nightly scan kept incrementing price_ok
against zero new rows. 343 tickers were affected.
"""
import datetime as dt
import os
import tempfile

from smart_money import db as dbmod, prices


def _db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _span(con, ticker, start, end):
    con.execute("INSERT INTO price_spans VALUES (?,?,?,0)", (ticker, start, end))
    con.commit()


def test_span_records_the_last_date_returned_not_the_end_requested(monkeypatch):
    path = _db()
    try:
        con = dbmod.connect(path)
        # Yahoo answers with data only through 2026-08-13.
        days = ["2026-08-11", "2026-08-12", "2026-08-13"]
        ts = [int(dt.datetime.fromisoformat(d).replace(
            tzinfo=dt.timezone.utc).timestamp()) for d in days]
        monkeypatch.setattr(prices, "_fetch", lambda *a, **k: (
            {"timestamp": ts,
             "indicators": {"quote": [{"close": [1.0, 2.0, 3.0]}],
                            "adjclose": [{"adjclose": [1.0, 2.0, 3.0]}]}}, b""))
        # ...but the caller asks for a span reaching far into the future.
        prices.eod(con, "SPY", "2026-08-11", "2027-02-07")

        spans = con.execute(
            "SELECT start_date, end_date FROM price_spans WHERE ticker='SPY'"
        ).fetchall()
        assert spans == [("2026-08-11", "2026-08-13")], spans
        assert not prices._covered(con, "SPY", "2026-08-11", "2026-08-20"), (
            "a future end must not be treated as covered")
    finally:
        os.remove(path)


def test_a_fetch_returning_nothing_claims_no_coverage(monkeypatch):
    """No rows means no coverage. Claiming it would freeze the ticker permanently."""
    path = _db()
    try:
        con = dbmod.connect(path)
        monkeypatch.setattr(prices, "_fetch", lambda *a, **k: (
            {"timestamp": [], "indicators": {"quote": [{"close": []}],
                                             "adjclose": [{"adjclose": []}]}}, b""))
        prices.eod(con, "ZZZZ", "2026-08-11", "2026-08-13")
        spans = con.execute(
            "SELECT * FROM price_spans WHERE ticker='ZZZZ'").fetchall()
        assert spans == [], spans
        assert not prices._covered(con, "ZZZZ", "2026-08-11", "2026-08-13")
    finally:
        os.remove(path)


def test_purge_future_spans_finds_and_clears_them():
    path = _db()
    try:
        con = dbmod.connect(path)
        _span(con, "SPY", "2012-09-03", "2027-02-07")   # the bug's fingerprint
        _span(con, "TSM", "2017-08-29", "2027-01-25")
        _span(con, "INTC", "2016-12-05", "2026-08-13")  # healthy, must survive
        bad, tickers = prices.purge_future_spans(con, today="2026-08-14")
        assert tickers == ["SPY", "TSM"], tickers
        assert len(bad) == 2
        # dry run by default
        assert con.execute("SELECT COUNT(*) FROM price_spans").fetchone()[0] == 3

        prices.purge_future_spans(con, today="2026-08-14", apply=True)
        left = con.execute(
            "SELECT ticker FROM price_spans ORDER BY ticker").fetchall()
        assert left == [("INTC",)], left
    finally:
        os.remove(path)


def test_purge_is_idempotent_and_safe_on_a_clean_table():
    path = _db()
    try:
        con = dbmod.connect(path)
        _span(con, "INTC", "2016-12-05", "2026-08-13")
        bad, tickers = prices.purge_future_spans(con, today="2026-08-14",
                                                 apply=True)
        assert bad == [] and tickers == []
        assert con.execute("SELECT COUNT(*) FROM price_spans").fetchone()[0] == 1
    finally:
        os.remove(path)
