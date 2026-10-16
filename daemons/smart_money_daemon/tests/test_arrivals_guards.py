"""ORDER SM-H1 -- the arrival ledger's guards, pinned after a mutation review.

A mutation pass over the first build (207 single-point mutants across the ledger, the
query, the scan leg and the pages) found 100 that the whole suite survived. None was a
present defect: each was a guard that nothing pinned -- the deadline roll at two of its
consumers, what the strip promotes, every cell of the /arrivals table, the read time
that dates every 'awaited', the scan's wiring of the leg. Each test here holds one in
place and fails on the mutant it was written against.

The fixtures are test_arrivals.py's, loaded by path so this file needs no conftest.
"""
import http.server
import importlib.util
import inspect
import os
import re
import threading
import urllib.request

import pytest

_spec = importlib.util.spec_from_file_location(
    "arrivals_fixtures",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_arrivals.py"))
T = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(T)

A, AI, dash, dbmod, q, scan = T.A, T.AI, T.dash, T.dbmod, T.q, T.scan
edgar, db = T.edgar, T.db                  # the fixtures, re-exported
today_is_the_anchor = T.today_is_the_anchor  # autouse: the pages' today is the fixtures' day
NOW, ANCHOR = T.NOW, T.ANCHOR
cover, acc, hold, registry, rows_by_name = (T.cover, T.acc, T.hold, T.registry,
                                            T.rows_by_name)
DAY = 86400
FEED = "https://data.sec.gov/submissions/CIK{:010d}.json"


def _one(con, edgar, tmp_path, monkeypatch, name="Fund", cik=111):
    registry(tmp_path, monkeypatch, [(cik, name)])
    return cik


def _params(anchor=ANCHOR, path="/arrivals", **qs):
    return dict(dash._params(dict({"anchor": [anchor]}, **qs)), _path=path)


def _page(path, anchor=ANCHOR):
    ro = q.connect_ro(path)
    try:
        return dash.view_arrivals(ro, _params(anchor))
    finally:
        ro.close()


def _cells(page, name):
    """(the <tr ...> opening, [the cell bodies]) of one filer's row in the table.
    Cells: 0 filer, 1 closed, 2 open, 3 last 13F, 4 filed, 5 book held, 6 deadlines
    passed, 7 filed with no book held, 8 amendments not applied, 9 EDGAR read, 10 notes."""
    table = page[page.index("<table>"):page.index("</table>")]
    for tr in table.split("<tr")[1:]:
        if ">{}</a>".format(name) in tr:
            return tr[:tr.index(">")], re.findall(r"<td>(.*?)</td>", tr, re.S)
    raise AssertionError("no table row for " + name)


def _card(strip_html, name):
    """The one strip card (<div class='book'>...) that carries this filer's name."""
    for piece in strip_html.split("<div class='book'>")[1:]:
        if "<b>{}</b>".format(name) in piece:
            return piece
    raise AssertionError("no card for " + name)


# ====================================================== what a filing states ==
def test_a_cover_that_states_13f_notice_is_a_notice_whatever_the_form():
    assert A.filing_kind(T._f(form="13F-HR", value=None, report="13F NOTICE")) == "notice"


def test_a_read_cover_with_no_value_total_is_never_a_book():
    assert A.filing_kind(T._f(value=None, status="read")) == "unstated"


def test_a_13f_nt_is_a_notice_before_its_cover_is_read(
        db, edgar, tmp_path, monkeypatch):
    """Pershing's Q3 notice lands around 2026-11-16. On a night its cover fetch fails
    the form alone must still say NT-notice, and the note must say the cover was not
    read - not that the cover names nobody."""
    con, path = db
    registry(tmp_path, monkeypatch, [(333, "Notice Fund")])
    edgar.filing(333, acc(333, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    a = acc(333, 26, 1)
    edgar.filing(333, a, "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PARENT INC.", "0002026053", "028-25746")]))
    edgar.status[edgar.url(333, a, "primary_doc.xml")] = 503
    AI.read_filer(con, "333", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Notice Fund"]
    assert r["closed_state"] == A.NT_NOTICE
    assert "cover not read, so that manager is not named here" in r["notes"]
    assert "does not name" not in r["notes"]


def test_a_book_outranks_a_notice():
    notice = T._f(form="13F-NT", value=None, report="13F NOTICE")
    assert A.period_state([notice, T._f()]) == A.FILED


def test_a_book_outranks_an_unread_amendment():
    """An original read as a book and an amendment whose cover fetch failed: the period
    is filed. Twelve stored periods carry an original and an amendment."""
    unread = T._f(form="13F-HR/A", status="failed", value=None)
    assert A.period_state([T._f(), unread]) == A.FILED


def test_an_absent_confidential_flag_is_not_a_stated_false():
    assert A.parse_cover(T.PERSHING_NT)["confidential_omitted"] is None


def test_isAmendment_false_is_not_an_amendment():
    assert A.parse_cover(T.PERSHING_NT)["is_amendment"] == 0


# ============================================================ the filer view ==
def test_the_book_held_is_the_newest_period_held(db, edgar, tmp_path, monkeypatch):
    """Every production filer holds eight or more quarters; every test filer held one."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    for n, per, filed in ((1, "2026-03-31", "2026-05-15"), (2, "2026-06-30", "2026-08-14")):
        edgar.filing(111, acc(111, 26, n), "13F-HR", per, filed,
                     cover_xml=cover("13F-HR", per))
        hold(con, 111, acc(111, 26, n), per, filed)
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert by["Fund"]["book_period"] == "2026-06-30"


def test_a_notice_for_the_open_period_is_explained(db, edgar, tmp_path, monkeypatch):
    """From 2026-10-01 to 2026-11-16 the quarter on everybody's mind is the OPEN one."""
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path, anchor="2026-08-14")
    r = by["Notice Fund"]
    assert r["open_state"] == A.NT_NOTICE
    assert ("13F-NT for 2026-06-30 filed 2026-08-14: holdings are reported by PARENT INC."
            in r["notes"])


def test_a_confidential_omission_is_stated(db, edgar, tmp_path, monkeypatch):
    """17 stored covers state it, Berkshire 2025-03-31 and Elliott 2025-06-30 among them."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30", omitted=True))
    hold(con, 111, acc(111, 26, 1), "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert ("13F-HR for 2026-06-30 states that holdings were omitted under a "
            "confidential treatment request") in by["Fund"]["notes"]


def test_a_filing_dated_on_the_deadline_is_not_late(db, edgar, tmp_path, monkeypatch):
    """19 of the 2026-06-30 originals are dated exactly on the deadline."""
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    assert "day(s) after" not in by["Filed Fund"]["notes"]


def test_an_amendment_after_the_deadline_is_not_a_late_13f_hr(
        db, edgar, tmp_path, monkeypatch):
    """205 stored amendments are dated after their period's deadline, First Eagle's
    2026-06-30 one (filed 2026-08-24) among them. That is what an amendment is."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-06-30", "2026-08-24",
                 cover_xml=cover("13F-HR/A", "2026-06-30", amendment=("1", "RESTATEMENT")))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert "day(s) after" not in by["Fund"]["notes"]


def test_a_filing_on_the_rolled_deadline_is_not_late(db, edgar, tmp_path, monkeypatch):
    """2026-09-30 + 45 days is Saturday 2026-11-14 and the deadline is Monday 11-16.
    244 stored originals are on time by the rolled date and late by the unrolled one."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-09-30", "2026-11-16",
                 cover_xml=cover("13F-HR", "2026-09-30"))
    AI.read_filer(con, "111", "c", now=NOW)
    for anchor in ("2026-11-16", "2026-11-17"):         # as the open, then the closed period
        _, by = rows_by_name(path, anchor=anchor)
        assert "day(s) after" not in by["Fund"]["notes"], anchor


def test_days_past_the_deadline_count_from_the_rolled_date(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    AI.read_filer(con, "111", "c", now=T.evening("2026-11-19"))   # read after the deadline
    _, by = rows_by_name(path, anchor="2026-11-20")
    r = by["Fund"]
    assert (r["closed_period"], r["closed_state"]) == ("2026-09-30", A.AWAITED)
    assert r["closed_days_past_deadline"] == 4          # from 11-16, not from 11-14


def test_the_closed_periods_due_date_is_the_rolled_one(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.feeds[111] = []
    AI.read_filer(con, "111", "c", now=NOW)
    res, _ = rows_by_name(path, anchor="2026-11-20")
    assert (res["closed_period"], res["closed_deadline"]) == ("2026-09-30", "2026-11-16")


def test_a_filing_older_than_the_stored_window_is_not_unapplied(
        db, edgar, tmp_path, monkeypatch):
    """1,632 originals are on EDGAR; covers are read from 2023-12-31 only."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 19, 1), "13F-HR", "2019-06-30", "2019-08-14",
                 cover_xml=cover("13F-HR", "2019-06-30"))
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["amendments_not_applied"], r["filed_not_held"], r["notes"]) == (0, "", "")


def test_a_filing_older_than_the_filers_own_book_is_not_unapplied(
        db, edgar, tmp_path, monkeypatch):
    """A book that starts later than the cover floor: the quarters between are read
    covers of filings this system never set out to hold."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 24, 1), "13F-HR", "2024-03-31", "2024-05-15",
                 cover_xml=cover("13F-HR", "2024-03-31"))
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["amendments_not_applied"], r["filed_not_held"], r["notes"]) == (0, "", "")


def test_an_unheld_13f_whose_cover_was_not_read_is_still_reported(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    hold(con, 111, acc(111, 26, 1), "2026-03-31", "2026-05-15")
    a = acc(111, 26, 2)
    edgar.filing(111, a, "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    edgar.status[edgar.url(111, a, "primary_doc.xml")] = 503
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["closed_state"], r["filed_not_held"]) == (A.FILED_UNREAD, "2026-06-30")
    assert ("13F-HR for 2026-06-30 filed 2026-08-14 is on EDGAR and no holdings from it "
            "are held (cover not read: cover: RuntimeError: cover HTTP 503)") in r["notes"]


def test_a_notice_pointing_at_a_tracked_filer_says_tracked(
        db, edgar, tmp_path, monkeypatch):
    """The successor-CIK ruling (CIK 2026053) is open; if it is added this is the branch."""
    con, path = db
    registry(tmp_path, monkeypatch, [(333, "Notice Fund"), (444, "Parent")])
    edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PARENT INC.", "0000000444", "028-1")]))
    edgar.feeds[444] = []
    AI.read_roster(con, "c", {"0000000333": "Notice Fund", "0000000444": "Parent"}, now=NOW)
    _, by = rows_by_name(path)
    assert "holdings are reported by PARENT INC. (CIK 444, tracked)" in by["Notice Fund"]["notes"]


def test_two_filings_on_one_day_are_ordered_by_the_feed_stamp(
        db, edgar, tmp_path, monkeypatch):
    """In 16 stored (filer, day) pairs the accession order is not the acceptance order."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 9), "13F-HR", "2026-06-30", "2026-08-14",
                 feed_accepted="2026-08-14T15:00:00.000Z",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-06-30", "2026-08-14",
                 feed_accepted="2026-08-14T19:00:00.000Z",
                 cover_xml=cover("13F-HR/A", "2026-06-30", amendment=("1", "RESTATEMENT")))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert by["Fund"]["last_form"] == "13F-HR/A"


def test_a_paper_filing_is_not_a_filers_first_13f(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, "9999999997-12-000001", "13FCONP", "2011-12-31", "2012-08-15",
                 primary="doc.paper")
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2025-12-31", "2026-02-17",
                 cover_xml=cover("13F-HR", "2025-12-31"))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2026-02-17")
    assert (by["Fund"]["closed_period"], by["Fund"]["closed_state"]) == (
        "2025-09-30", A.NOT_DUE)


def test_a_period_that_is_not_a_quarter_end_does_not_start_the_clock(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 25, 1), "13F-HR", "2025-08-31", "2025-09-10",
                 cover_xml=cover("13F-HR", "2025-08-31"))
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2025-12-31", "2026-02-17",
                 cover_xml=cover("13F-HR", "2025-12-31"))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2026-02-17")
    assert by["Fund"]["closed_state"] == A.NOT_DUE


# ========================================================== the EDGAR read ==
def test_the_cover_floor_is_2023_12_31_inclusive(db, edgar):
    con, _ = db
    edgar.filing(111, acc(111, 23, 1), "13F-HR", "2023-09-30", "2023-11-14",
                 cover_xml=cover("13F-HR", "2023-09-30"))
    edgar.filing(111, acc(111, 24, 1), "13F-HR", "2023-12-31", "2024-02-14",
                 cover_xml=cover("13F-HR", "2023-12-31"))
    AI.read_filer(con, "111", "c", now=NOW)
    got = dict(con.execute("SELECT period, cover_status FROM thirteenf_arrivals"))
    assert got == {"2023-09-30": "pending", "2023-12-31": "read"}


def test_a_later_good_read_moves_the_read_time(db, edgar, tmp_path, monkeypatch):
    """The read time dates every 'awaited'. Frozen at the backfill it would say the
    states are as of deploy day for ever."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.feeds[111] = []
    AI.read_filer(con, "111", "c", now=NOW)
    AI.read_filer(con, "111", "c", now=NOW + DAY)
    assert con.execute("SELECT read_unix FROM thirteenf_feed_reads").fetchone()[0] == NOW + DAY
    _, by = rows_by_name(path)
    assert by["Fund"]["feed_read"] == A.iso_utc(NOW + DAY)


def test_a_failed_nightly_read_is_recorded_and_said(
        db, edgar, tmp_path, monkeypatch):
    """Through read_roster, the only path production takes, and read back on a separate
    read-only connection as the dashboard reads it."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-06-30", "2026-08-14")
    roster = {"0000000111": "Fund"}
    AI.read_roster(con, "c", roster, now=NOW)
    edgar.status[FEED.format(111)] = 503
    rep = AI.read_roster(con, "c", roster, now=NOW + DAY)
    assert len(rep["failed_reads"]) == 1
    ro = q.connect_ro(path)
    try:
        read, attempted, error = ro.execute(
            "SELECT read_unix, attempted_unix, error FROM thirteenf_feed_reads").fetchone()
    finally:
        ro.close()
    assert (read, attempted) == (NOW, NOW + DAY) and "503" in (error or "")
    _, by = rows_by_name(path)
    assert by["Fund"]["closed_state"] == A.FILED
    assert ("last EDGAR read attempt failed (RuntimeError: submissions HTTP 503); states "
            "are as of filings dated through 2026-10-01") in by["Fund"]["notes"]


def test_a_good_read_clears_the_last_error(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.feeds[111] = []
    AI.read_filer(con, "111", "c", now=NOW)
    edgar.status[FEED.format(111)] = 503
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW + DAY)
    del edgar.status[FEED.format(111)]
    AI.read_filer(con, "111", "c", now=NOW + 2 * DAY)
    assert con.execute("SELECT error FROM thirteenf_feed_reads").fetchone()[0] is None
    _, by = rows_by_name(path)
    assert "read attempt failed" not in by["Fund"]["notes"]


def test_history_read_after_a_failed_first_attempt_is_still_history(db, edgar):
    """28 feeds on backfill night; one 503 and the retry would stamp years-old filings
    as arrived that night."""
    con, _ = db
    edgar.filing(111, acc(111, 24, 1), "13F-HR", "2024-06-30", "2024-08-14",
                 cover_xml=cover("13F-HR", "2024-06-30"))
    edgar.status[FEED.format(111)] = 503
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW)
    del edgar.status[FEED.format(111)]
    AI.read_filer(con, "111", "c", now=NOW + DAY)
    assert con.execute("SELECT first_seen_unix FROM thirteenf_arrivals").fetchone()[0] is None


def test_a_cover_stating_another_form_is_refused(db, edgar):
    con, _ = db
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-06-30", "2026-08-20",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    r = AI.read_filer(con, "111", "c", now=NOW)
    assert "cover states 13F-HR for 2026-06-30, feed says 13F-HR/A" in (
        r["details_failed"][0]["error"])


def test_deep_follows_the_older_pages(monkeypatch):
    def block(form, accession, period, filed):
        return {"form": [form], "accessionNumber": [accession], "filingDate": [filed],
                "reportDate": [period], "primaryDocument": ["x"],
                "acceptanceDateTime": [filed + "T20:00:00.000Z"]}
    recent = block("13F-HR", "a", "2026-06-30", "2026-08-14")
    older = block("13F-NT", "b", "2012-06-30", "2012-08-14")

    def get(url, contact):
        if url.endswith("/CIK0000000111.json"):
            return T.Resp(js={"filings": {"recent": recent, "files": [
                {"name": "CIK0000000111-submissions-001.json"}]}})
        if url.endswith("/CIK0000000111-submissions-001.json"):
            return T.Resp(js=older)
        return T.Resp(404)
    monkeypatch.setattr(AI, "_get", get)
    rows, older_pages = AI.fetch_feed("111", "c")
    assert [r["accession"] for r in rows] == ["a"] and older_pages == 1
    rows, older_pages = AI.fetch_feed("111", "c", deep=True)
    assert [r["accession"] for r in rows] == ["a", "b"] and older_pages == 1


BASE = {"filers": 2, "read": 2, "family": 9, "new": [], "details_read": 0,
        "stamps_read": 0, "failed_reads": [], "failed_details": []}


def _cli(monkeypatch, tmp_path, report, contact="c"):
    from smart_money import efd_ingest
    monkeypatch.setattr(efd_ingest, "load_env",
                        lambda: {"EDGAR_CONTACT": contact} if contact else {})
    monkeypatch.setattr(AI, "read_roster", lambda *a, **k: dict(BASE, **report))
    return AI.main(["--db", str(tmp_path / "cli.db")])


def test_the_cli_exits_nonzero_on_a_failed_feed_read(monkeypatch, tmp_path):
    """Gates are scripts: the backfill's exit status is the only thing a script can gate on."""
    assert _cli(monkeypatch, tmp_path, {}) == 0
    assert _cli(monkeypatch, tmp_path, {"failed_reads": [
        {"cik": "111", "name": "A", "error": "HTTP 503"}]}) == 1


def test_the_cli_exits_nonzero_on_a_failed_cover_read(monkeypatch, tmp_path):
    assert _cli(monkeypatch, tmp_path, {"failed_details": [
        {"cik": "111", "name": "A", "accession": "x", "error": "HTTP 403"}]}) == 1


def test_the_cli_says_when_no_feed_was_read_at_all(monkeypatch, tmp_path, capsys):
    """A run that read nothing is not 'a read with failures': the deploy script stops on
    it, because otherwise the whole first read lands in that night's scan."""
    failed = [{"cik": c, "name": c, "error": "HTTP 503"} for c in ("111", "222")]
    assert _cli(monkeypatch, tmp_path, {"read": 0, "failed_reads": failed}) == 3
    assert "NO FEED WAS READ" in capsys.readouterr().err
    assert _cli(monkeypatch, tmp_path, {"read": 1, "failed_reads": failed[:1]}) == 1


def test_the_cli_refuses_to_run_without_a_contact(monkeypatch, tmp_path):
    assert _cli(monkeypatch, tmp_path, {}, contact=None) == 2


# ================================================================ q_arrivals ==
def test_the_oldest_read_is_the_oldest(db, edgar, tmp_path, monkeypatch):
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "A"), (222, "B")])
    edgar.feeds[111], edgar.feeds[222] = [], []
    AI.read_filer(con, "111", "c", now=NOW)
    AI.read_filer(con, "222", "c", now=NOW + DAY)
    res, _ = rows_by_name(path)
    assert res["oldest_feed_read"] == A.iso_utc(NOW)


def test_a_read_of_an_untracked_cik_does_not_make_the_ledger_read(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Fund")])
    edgar.feeds[999], edgar.feeds[111] = [], []
    AI.read_filer(con, "999", "c", now=NOW)
    res, _ = rows_by_name(path)
    assert res["ledger"] == "empty" and res["oldest_feed_read"] is None
    AI.read_filer(con, "111", "c", now=NOW + DAY)
    res, _ = rows_by_name(path)
    assert (res["ledger"], res["oldest_feed_read"]) == ("ok", A.iso_utc(NOW + DAY))


def test_a_13f_with_no_book_held_is_named_above_the_cards(
        db, edgar, tmp_path, monkeypatch):
    """The line that says a filing is on EDGAR with no holdings held was pinned
    nowhere, per row or on the front page."""
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-03-31", "2026-05-15")
    AI.read_filer(con, "111", "c", now=NOW)
    res, _ = rows_by_name(path)
    assert res["filed_not_held"] == [("Fund", "2026-06-30")]
    assert res["amendments_not_applied"] == 0
    assert ("filed on EDGAR, no book held here: Fund 2026-06-30"
            in dash._arrivals_summary(res))


def test_the_row_carries_the_filers_thesis(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    assert by["Fund"]["thesis"] == "macro"


# ================================================================== the scan ==
def _stub_scan(monkeypatch, arrivals, calls=None):
    calls = [] if calls is None else calls
    S = scan._src
    monkeypatch.setattr(scan, "load_overlay", lambda *a, **k: {})
    monkeypatch.setattr(scan, "load_registry", lambda *a, **k: {"entries": []})
    monkeypatch.setattr(scan, "leg_congress", lambda *a, **k: ([], [S("congress", "OK")], {}))
    monkeypatch.setattr(scan, "leg_form4", lambda *a, **k: ([], [S("edgar_form4", "OK")], {}))
    monkeypatch.setattr(scan, "leg_13f", lambda *a, **k: ([], [S("edgar_13f", "OK")], {}))
    monkeypatch.setattr(scan, "leg_universal_ingest",
                        lambda *a, **k: (S("edgar_form4_universal", "OK"), {}))
    monkeypatch.setattr(scan, "leg_enrich", lambda *a, **k: (S("enrich_scoped", "OK"), {}))

    def holdings(*a, **k):
        calls.append("holdings")
        return S("13f_holdings", "OK"), {}
    monkeypatch.setattr(scan, "leg_13f_holdings", holdings)
    monkeypatch.setattr(scan, "leg_congress_annual",
                        lambda *a, **k: ([S("congress_annual", "OK")], {}))
    monkeypatch.setattr(scan, "leg_options", lambda *a, **k: (S("options_chains", "OK"), {}))
    monkeypatch.setattr(scan, "leg_13f_arrivals", arrivals)
    return calls


def _sources(envelope):
    return {s["source"]: s for s in envelope["sources"]}


def test_the_scan_runs_the_arrivals_leg_and_reports_it(db, monkeypatch):
    """The options leg has this pin (test_the_leg_is_actually_called_by_the_scan). The
    arrivals leg could be deleted from run_scan and 674 tests pass."""
    con, _ = db
    calls = []

    def leg(con_, contact, reg):
        calls.append("arrivals")
        return scan._src("13f_arrivals", "DEGRADED", "feed reads failed=1 (B)", 0), {}
    _stub_scan(monkeypatch, leg, calls)
    env, _ = scan.run_scan(con, "c", None)
    assert calls == ["arrivals", "holdings"]   # called, and first
    s = _sources(env)
    assert s["13f_arrivals"]["status"] == "DEGRADED"   # and it is reported
    assert "feed reads failed=1" in s["13f_arrivals"]["note"]


def test_an_exception_in_the_arrivals_leg_never_stops_the_scan(db, monkeypatch):
    con, _ = db

    def boom(*a, **k):
        raise RuntimeError("no such table: thirteenf_arrivals")
    calls = _stub_scan(monkeypatch, boom)
    env, _ = scan.run_scan(con, "c", None)
    s = _sources(env)
    assert s["13f_arrivals"]["status"] == "DEGRADED"
    assert "no such table" in s["13f_arrivals"]["note"]
    assert calls == ["holdings"] and s["13f_holdings"]["status"] == "OK"


def test_a_skipped_arrivals_leg_says_skipped(db, monkeypatch):
    con, _ = db
    _stub_scan(monkeypatch, lambda *a, **k: 1 / 0)
    env, _ = scan.run_scan(con, "c", None, skip_universal=True)
    assert _sources(env)["13f_arrivals"]["status"] == "SKIPPED"


def test_a_healthy_arrivals_leg_cannot_mask_every_signal_source_down(
        monkeypatch, tmp_path):
    S = scan._src
    env = {"scan_id": "scan_1", "sources": [
        S("house_ptr", "DEGRADED"), S("edgar_form4", "DEGRADED"),
        S("edgar_13f", "DEGRADED"), S("13f_arrivals", "OK")]}
    monkeypatch.setattr(scan, "load_env", lambda: {"EDGAR_CONTACT": "c"})
    monkeypatch.setattr(scan, "run_scan", lambda *a, **k: (dict(env), []))
    monkeypatch.setattr(scan, "_enqueue", lambda e, ev: {"queue": "absent"})
    monkeypatch.setattr(scan.dbmod, "SCANS_DIR", str(tmp_path / "scans"))
    rc = scan.main(["--db", str(tmp_path / "s.db"), "--raw", str(tmp_path / "raw")])
    assert rc == 1


def _leg(monkeypatch, roster, reg_entries, seen=None):
    from smart_money import thirteenf_ingest
    monkeypatch.setattr(thirteenf_ingest, "CONFIRMED", roster)

    def fake(con, contact, r, **k):
        if seen is not None:
            seen["roster"] = r
        return dict(BASE)
    monkeypatch.setattr(AI, "read_roster", fake)
    return scan.leg_13f_arrivals(None, "c", {"entries": reg_entries})[0]


def test_the_leg_reads_the_confirmed_roster(monkeypatch):
    roster, seen = {"0000000111": "A", "0000000222": "B"}, {}
    _leg(monkeypatch, roster, [{"role": "manager_13f", "cik": c} for c in roster], seen)
    assert seen["roster"] is roster


def test_a_registry_filer_the_ingest_roster_lacks_is_drift(monkeypatch):
    """Shown on every page as 'EDGAR feed never read', every night, with the leg OK."""
    src = _leg(monkeypatch, {"0000000111": "A"},
               [{"role": "manager_13f", "cik": "0000000111"},
                {"role": "manager_13f", "cik": "0000000222"}])
    assert src["status"] == "DEGRADED" and "roster drift" in src["note"] and "222" in src["note"]


def test_a_registry_entry_that_is_not_a_13f_manager_is_not_drift(monkeypatch):
    """The production registry carries insiders and members with CIKs."""
    roster = {"0000000111": "A"}
    src = _leg(monkeypatch, roster, [{"role": "manager_13f", "cik": "0000000111"},
                                     {"role": "insider", "cik": "0000000999"}])
    assert src["status"] == "OK"


def test_drift_is_judged_on_integer_ciks(monkeypatch):
    src = _leg(monkeypatch, {"0000000111": "A"}, [{"role": "manager_13f", "cik": "111"}])
    assert src["status"] == "OK"


# ========================================================== the dashboard ==
def test_the_coverage_line_names_every_state_it_counts():
    states = (A.FILED, A.FILED_EMPTY, A.NT_NOTICE, A.AWAITED, A.FILED_UNREAD, A.NOT_DUE,
              A.NOT_READ)
    txt = dash._arr_coverage_text(
        {"closed_period": "2026-06-30", "closed_deadline": "2026-08-14",
         "closed_coverage": {s: 1 for s in states}, "count": len(states)}, "closed")
    assert txt == ("2026-06-30 (due 2026-08-14): 1 filed, 1 filed-empty, 1 NT-notice, "
                   "1 awaited, 1 filed (contents not read), 1 before first 13F, "
                   "1 not read of 7 tracked filers")


def _strip_with(con, edgar, tmp_path, monkeypatch, small, setup, anchor=ANCHOR, read=True):
    """Eight large filed books plus the `small` filers, which `setup` files for."""
    big = [(100 + i, "Big {}".format(i)) for i in range(8)]
    filers = big + small
    registry(tmp_path, monkeypatch, filers)
    for cik, _name in big:
        a, size = acc(cik, 26, 1), 10 ** 9 * (20 - cik % 100)
        edgar.filing(cik, a, "13F-HR", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-HR", "2026-06-30", value=size))
        hold(con, cik, a, "2026-06-30", "2026-08-14", value=size)
    setup()
    if read:
        AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in filers}, now=NOW)


def _strip(path, anchor=ANCHOR):
    ro = q.connect_ro(path)
    try:
        return dash._tracked_books_strip(q.q_tracked_books(ro, anchor=anchor),
                                         q.q_arrivals(ro, anchor=anchor))
    finally:
        ro.close()


def test_the_card_itself_states_what_was_filed(db, edgar, tmp_path, monkeypatch):
    """The existing strip test finds '49d past the deadline' in the summary line above
    the cards. The CARD could still read 'filing window passed' - the words the order
    was written to remove - and it passes."""
    con, path = db

    def setup():
        edgar.filing(444, acc(444, 25, 1), "13F-HR", "2025-09-30", "2025-11-03",
                     cover_xml=cover("13F-HR", "2025-09-30"))
        hold(con, 444, acc(444, 25, 1), "2025-09-30", "2025-11-03")
    _strip_with(con, edgar, tmp_path, monkeypatch, [(444, "Quiet Fund")], setup)
    card = _card(_strip(path), "Quiet Fund")
    assert "2026-06-30 awaited, 49d past the deadline" in card
    assert "2026-09-30 awaited, due 2026-11-16" in card
    assert "filing window passed" not in card
    assert "2026-06-30 filed; 2026-09-30 awaited, due 2026-11-16" in _card(_strip(path), "Big 0")


def test_what_is_promoted_out_of_the_fold(
        db, edgar, tmp_path, monkeypatch):
    """Only 'awaited' was pinned as needing attention. Pershing's state is NT-notice."""
    con, path = db
    small = [(222, "Empty Fund"), (333, "Notice Fund"), (555, "Unread Fund"),
             (666, "New Fund"), (777, "Dark Fund")]

    def setup():
        for cik in (222, 333, 555, 777):
            edgar.filing(cik, acc(cik, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                         cover_xml=cover("13F-HR", "2026-03-31"))
            hold(con, cik, acc(cik, 26, 0), "2026-03-31", "2026-05-15")
        edgar.filing(222, acc(222, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-HR", "2026-06-30", value=0, entries=1))
        edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                     managers=[("PARENT INC.", "0002026053", "028-25746")]))
        a = acc(555, 26, 1)
        edgar.filing(555, a, "13F-HR", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-HR", "2026-06-30"))
        edgar.status[edgar.url(555, a, "primary_doc.xml")] = 503
        # first 13F ever is for the OPEN quarter: the closed one was never owed
        edgar.filing(666, acc(666, 26, 1), "13F-HR", "2026-09-30", "2026-10-01",
                     cover_xml=cover("13F-HR", "2026-09-30"))
        hold(con, 666, acc(666, 26, 1), "2026-09-30", "2026-10-01")
        edgar.status[FEED.format(777)] = 503
    _strip_with(con, edgar, tmp_path, monkeypatch, small, setup)
    res, by = rows_by_name(path)
    assert {n: by[n]["closed_state"] for _c, n in small} == {
        "Empty Fund": A.FILED_EMPTY, "Notice Fund": A.NT_NOTICE,
        "Unread Fund": A.FILED_UNREAD, "New Fund": A.NOT_DUE, "Dark Fund": A.NOT_READ}
    head, _, folded = _strip(path).partition("<details>")
    for name in ("Notice Fund", "Empty Fund", "Unread Fund", "Dark Fund"):
        assert "<b>{}</b>".format(name) in head, name
        assert "{}: 2026-06-30".format(name) in head, name     # and listed above the cards
    assert "<b>New Fund</b>" in folded and "New Fund: " not in head


def test_a_filer_with_no_book_held_keeps_its_state_and_its_place(
        db, edgar, tmp_path, monkeypatch):
    con, path = db

    def setup():
        edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                     managers=[("PARENT INC.", "0002026053", "028-25746")]))
    _strip_with(con, edgar, tmp_path, monkeypatch, [(333, "Notice Fund")], setup)
    html_ = _strip(path)
    head, _, folded = html_.partition("<details>")
    card = _card(html_, "Notice Fund")
    assert "no book held" in card and "2026-06-30 NT-notice" in card
    assert "<b>Notice Fund</b>" in head and "<b>Notice Fund</b>" not in folded


def test_without_a_read_the_cards_fall_back_and_stay_folded(
        db, edgar, tmp_path, monkeypatch):
    """Between a deploy and the first read: no card states an arrival, none is promoted."""
    con, path = db
    _strip_with(con, edgar, tmp_path, monkeypatch, [], lambda: None, read=False)
    html_ = _strip(path)
    assert "no EDGAR read yet" in html_
    assert "<details><summary>2 more tracked book(s)</summary>" in html_
    assert "not read" not in html_
    assert "d to next filing" in _card(html_, "Big 0")


def test_the_front_page_is_given_the_ledger():
    """Rendering view_front needs the whole registry; the call site is pinned the way
    the house pins the options leg."""
    src = inspect.getsource(dash.view_front).replace(" ", "").replace("\n", "")
    assert ('_tracked_books_strip(q.q_tracked_books(con,anchor=p["anchor"]),'
            '*_arrivals_for_strip(con,p))') in src
    feed = inspect.getsource(dash._arrivals_for_strip) + inspect.getsource(
        dash._arrivals_safe)
    assert 'q.q_arrivals(con, anchor=p["anchor"])' in feed


def test_the_table_states_each_period_in_its_own_column(
        db, edgar, tmp_path, monkeypatch):
    """'NT-notice' and '49d past the deadline' are both in the summary paragraph that
    view_arrivals prints above the table, so the existing page assertions pass with
    either column of the table wrong, or the summary gone."""
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    page = _page(path)
    _, quiet = _cells(page, "Quiet Fund")
    assert quiet[1] == "awaited, 49d past the deadline"
    assert quiet[2] == "awaited, due 2026-11-16"
    assert _cells(page, "Notice Fund")[1][1] == "NT-notice"
    assert _cells(page, "Empty Fund")[1][1] == "filed-empty"
    assert _cells(page, "Filed Fund")[1][1] == "filed"


def test_the_table_states_the_last_filing_and_the_book(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    page = _page(path)
    _, quiet = _cells(page, "Quiet Fund")
    assert quiet[3] == "13F-HR 2025-09-30"
    assert quiet[4] == "2025-11-03 (accepted 2025-11-03 16:05:00 ET)"
    assert quiet[5] == "2025-09-30"
    assert quiet[6] == "2025-12-31, 2026-03-31, 2026-06-30"
    _, notice = _cells(page, "Notice Fund")
    assert (notice[3], notice[5]) == ("13F-NT 2026-06-30", "2026-03-31")


def test_the_table_carries_the_notes_and_marks_the_row(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    page = _page(path)
    tr, notice = _cells(page, "Notice Fund")
    assert ("holdings are reported by PARENT INC. (CIK 2026053, not a tracked filer)"
            in notice[10])
    assert "class='hot'" in tr
    assert "class='hot'" not in _cells(page, "Filed Fund")[0]


def test_the_table_names_a_13f_with_no_book_held(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _one(con, edgar, tmp_path, monkeypatch)
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-03-31", "2026-05-15")
    AI.read_filer(con, "111", "c", now=NOW)
    cells = _cells(_page(path), "Fund")[1]
    assert (cells[7], cells[8]) == ("2026-06-30", "-")


def test_the_arrivals_page_carries_the_coverage_and_the_absent_ledger_statement(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    assert ("2026-06-30 (due 2026-08-14): 1 filed, 1 filed-empty, 1 NT-notice, 1 awaited "
            "of 4 tracked filers") in _page(path)
    bare = T.bare_db(tmp_path)
    assert "13F arrival ledger not built on this database" in _page(bare)


def test_the_default_order_puts_what_needs_a_look_first(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        _arr, rows = dash._arr_rows(ro, _params())
    finally:
        ro.close()
    assert [r["filer"] for r in rows] == ["Empty Fund", "Notice Fund", "Quiet Fund",
                                          "Filed Fund"]


EVIL_XML = "&lt;b&gt;X &amp; Y&lt;/b&gt;"       # a cover's manager name: <b>X & Y</b>
EVIL_NAME = "A & <i>B</i> Fund"


def _evil(con, edgar, tmp_path, monkeypatch):
    registry(tmp_path, monkeypatch, [(333, EVIL_NAME)])
    edgar.filing(333, acc(333, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    hold(con, 333, acc(333, 26, 0), "2026-03-31", "2026-05-15")
    edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[(EVIL_XML, "0002026053", "028-25746")]))
    AI.read_filer(con, "333", "c", now=NOW)


def test_text_from_a_cover_or_the_registry_is_escaped_on_the_arrivals_page(
        db, edgar, tmp_path, monkeypatch):
    """A cover is filer-authored XML. Its manager name reaches the notes cell."""
    con, path = db
    _evil(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    assert "<b>X & Y</b>" in by[EVIL_NAME]["notes"]            # the row carries it raw
    page = _page(path)
    assert "<b>X & Y</b>" not in page and "&lt;b&gt;X &amp; Y&lt;/b&gt;" in page
    assert "<i>B</i>" not in page
    assert page.count("A &amp; &lt;i&gt;B&lt;/i&gt; Fund") >= 2   # the summary and the table


def test_the_portfolio_page_states_what_the_filer_filed_escaped(
        db, edgar, tmp_path, monkeypatch):
    """q_portfolio returns the registry's zero-padded CIK and q_arrivals a bare one, so
    this join is through cik_int on every production page."""
    con, path = db
    _evil(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        res = q.q_portfolio(ro, filer_cik="333")
        assert res["filer_cik"] == "0000000333"
        out = T.portfolio_line(ro, _params(path="/portfolios", filer=["333"]), res)
    finally:
        ro.close()
    assert "2026-06-30 NT-notice" in out
    assert "<b>X & Y</b>" not in out and "&lt;b&gt;X &amp; Y&lt;/b&gt;" in out


def test_the_portfolio_line_names_the_last_13f_and_an_absent_ledger(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        out = T.portfolio_line(ro, _params(path="/portfolios", filer=["444"]),
                               q.q_portfolio(ro, filer_cik="444"))
    finally:
        ro.close()
    assert "last 13F: 13F-HR for 2025-09-30, filed 2025-11-03" in out
    bare = T.bare_db(tmp_path)
    ro = q.connect_ro(bare)
    try:
        out = T.portfolio_line(ro, _params(path="/portfolios"),
                               {"filer_cik": "0000000444"})
    finally:
        ro.close()
    assert "13F arrival ledger not built" in out


def test_the_printed_page_says_when_the_ledger_is_absent(tmp_path, monkeypatch):
    registry(tmp_path, monkeypatch, [(111, "Fund")])
    bare = T.bare_db(tmp_path)
    ro = q.connect_ro(bare)
    try:
        spec = dash._page_brief_spec(ro, _params(), "/arrivals")
    finally:
        ro.close()
    assert spec["notes"][0].startswith("ARRIVAL LEDGER ABSENT")


def test_the_printed_page_carries_the_coverage(db, edgar, tmp_path, monkeypatch):
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        spec = dash._page_brief_spec(ro, _params(), "/arrivals")
    finally:
        ro.close()
    assert spec["notes"][0] == ("2026-06-30 (due 2026-08-14): 1 filed, 1 filed-empty, "
                                "1 NT-notice, 1 awaited of 4 tracked filers")
    assert spec["notes"][1] == "2026-09-30 (due 2026-11-16): 4 awaited of 4 tracked filers"


def test_the_ledger_is_in_the_nav():
    assert ("/arrivals", "13F arrivals") in dash.NAV


def test_the_full_export_is_every_filer_not_the_page(tmp_path, monkeypatch):
    registry(tmp_path, monkeypatch, [(100 + i, "F{:02d}".format(i)) for i in range(30)])
    fresh = str(tmp_path / "fresh.db")
    dbmod.connect(fresh).close()
    ro = q.connect_ro(fresh)
    try:
        p = _params(per_page=["25"])
        full = dash._build_arrivals_csv(ro, p, full=True)
        page = dash._build_arrivals_csv(ro, p, full=False)
    finally:
        ro.close()
    assert len(full.strip().splitlines()) == 31 and len(page.strip().splitlines()) == 26


def test_the_csv_link_on_the_page_is_served(db, edgar, tmp_path, monkeypatch):
    """The pager on /arrivals links to /arrivals.csv. Over real HTTP on loopback."""
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)

    class H(dash.Handler):
        db_path = path

        def log_message(self, *a):
            pass
    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        url = "http://127.0.0.1:{}/arrivals.csv?anchor={}&full=1".format(
            srv.server_address[1], ANCHOR)
        with urllib.request.urlopen(url, timeout=10) as r:
            body = r.read().decode("utf-8")
            assert r.status == 200 and "text/csv" in r.headers["Content-Type"]
            assert "13f_arrivals_" + ANCHOR in r.headers["Content-Disposition"]
    finally:
        srv.shutdown()
        srv.server_close()
    assert body.splitlines()[0].startswith("filer,cik,thesis,closed_period")
    assert len(body.strip().splitlines()) == 5


def test_the_csv_is_named_for_the_day_it_answers_for(db, edgar, tmp_path, monkeypatch):
    """An anchor that is not a date is answered for today. The download must not carry
    the refused value in its name: '13f_arrivals_2026-8-1.csv' held today's rows."""
    con, path = db
    T._four_filers(con, edgar, tmp_path, monkeypatch)

    class H(dash.Handler):
        db_path = path

        def log_message(self, *a):
            pass
    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        url = "http://127.0.0.1:{}/arrivals.csv?anchor=2026-8-1&full=1".format(
            srv.server_address[1])
        with urllib.request.urlopen(url, timeout=10) as r:
            name = r.headers["Content-Disposition"]
            assert r.status == 200
    finally:
        srv.shutdown()
        srv.server_close()
    assert "2026-8-1" not in name
    assert "13f_arrivals_" + q.arrival_anchor("2026-8-1")[0] in name

