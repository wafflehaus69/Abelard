"""ORDER SM-H1 -- the 13F arrival ledger. Each test is pinned to the falsehood it removes.

Arrival used to be inferred from rows in thirteenf_holdings, and rows can only say that
a book with holdings landed. Measured on production on 2026-10-01: Pershing Square had
filed a 13F-NT for Q2 pointing at a parent company's report and its card read 'filing
window passed'; Thiel Macro's two placeholder quarters were period gaps; Scion's
year-old book carried the same words as a filer one day late; and ten amendments on
EDGAR, one of them correcting a $16.57bn line, were invisible because both readers
matched form == "13F-HR" exactly.

The covers below are the real ones, trimmed of addresses and signatures.
"""
import datetime as dt
import io
import json
import re
import sqlite3

import pytest

from smart_money import arrivals as A
from smart_money import arrivals_ingest as AI
from smart_money import brief
from smart_money import dashboard as dash
from smart_money import db as dbmod
from smart_money import queries as q
from smart_money import scan

NOW = 1790900000            # 2026-10-02 00:13Z = the evening of 2026-10-01 in New York
DAY = 86400
ANCHOR = "2026-10-02"       # closed = 2026-06-30 (due 08-14), open = 2026-09-30 (due 11-16)
NO_BOOK = {"periods": set(), "accessions": set(), "sums": {}, "floor": None}
NS = 'xmlns="http://www.sec.gov/edgar/thirteenffiler"'

# 0001172661-26-003777. A notice: the holdings are in another manager's report.
PERSHING_NT = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission {ns} xmlns:ns1="http://www.sec.gov/edgar/common">
  <headerData><submissionType>13F-NT</submissionType><filerInfo>
    <filer><credentials><cik>0001336528</cik></credentials></filer>
    <periodOfReport>06-30-2026</periodOfReport></filerInfo></headerData>
  <formData><coverPage>
      <reportCalendarOrQuarter>06-30-2026</reportCalendarOrQuarter>
      <isAmendment>false</isAmendment>
      <filingManager><name>Pershing Square Capital Management, L.P.</name></filingManager>
      <reportType>13F NOTICE</reportType>
      <form13FFileNumber>028-11694</form13FFileNumber>
      <otherManagersInfo><otherManager>
          <cik>0002026053</cik><form13FFileNumber>028-25746</form13FFileNumber>
          <name>PERSHING SQUARE INC.</name></otherManager></otherManagersInfo>
    </coverPage>
    <summaryPage><otherIncludedManagersCount/><tableEntryTotal/><tableValueTotal/>
    </summaryPage></formData></edgarSubmission>""".format(ns=NS)

# 0001325447-26-000033. States NEW HOLDINGS; a combination report; and its SUMMARY page
# lists a manager included IN this report, the opposite statement to the cover's list.
FIRST_EAGLE_AMENDMENT = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission {ns} xmlns:com="http://www.sec.gov/edgar/common">
  <headerData><submissionType>13F-HR/A</submissionType><filerInfo>
    <periodOfReport>06-30-2026</periodOfReport></filerInfo></headerData>
  <formData><coverPage>
      <reportCalendarOrQuarter>06-30-2026</reportCalendarOrQuarter>
      <isAmendment>true</isAmendment><amendmentNo>1</amendmentNo>
      <amendmentInfo><amendmentType>NEW HOLDINGS</amendmentType></amendmentInfo>
      <filingManager><name>First Eagle Investment Management, LLC</name></filingManager>
      <reportType>13F COMBINATION REPORT</reportType>
      <otherManagersInfo><otherManager>
          <cik>0001457320</cik><form13FFileNumber>028-19670</form13FFileNumber>
          <name>EXCHANGE TRADED CONCEPTS, LLC</name></otherManager></otherManagersInfo>
    </coverPage>
    <summaryPage><otherIncludedManagersCount>1</otherIncludedManagersCount>
      <tableEntryTotal>616</tableEntryTotal><tableValueTotal>60750982695</tableValueTotal>
      <otherManagers2Info><otherManager2><sequenceNumber>1</sequenceNumber>
          <otherManager><cik>0001791280</cik>
            <name>FIRST EAGLE SEPARATE ACCOUNT MANAGEMENT, LLC</name></otherManager>
      </otherManager2></otherManagers2Info>
    </summaryPage></formData></edgarSubmission>""".format(ns=NS)

# 0001315863-26-000423. A placeholder: entry total 1, value 0, no isAmendment element.
THIEL_EMPTY = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission {ns} xmlns:com="http://www.sec.gov/edgar/common">
  <headerData><submissionType>13F-HR</submissionType><filerInfo>
    <periodOfReport>03-31-2026</periodOfReport></filerInfo></headerData>
  <formData><coverPage>
      <reportCalendarOrQuarter>03-31-2026</reportCalendarOrQuarter>
      <filingManager><name>Thiel Macro LLC</name></filingManager>
      <reportType>13F HOLDINGS REPORT</reportType>
    </coverPage>
    <summaryPage><otherIncludedManagersCount>0</otherIncludedManagersCount>
      <tableEntryTotal>1</tableEntryTotal><tableValueTotal>0</tableValueTotal>
      <isConfidentialOmitted>false</isConfidentialOmitted>
    </summaryPage></formData></edgarSubmission>""".format(ns=NS)

INDEX_PAGE = """<div class="formGrouping">
  <div class="infoHead">Filing Date</div>
  <div class="info">2026-05-18</div>
  <div class="infoHead">Accepted</div>
  <div class="info">{}</div>
</div>"""


# ------------------------------------------------------------------ fixtures --
def cover(form, period, value=1000, entries=5, report="13F HOLDINGS REPORT",
          amendment=None, managers=(), omitted=None):
    """A cover in the real shape. amendment=None writes NO isAmendment element, as
    517 of 1,183 real covers do."""
    mm, dd, yyyy = period[5:7], period[8:10], period[:4]
    amend = ""
    if amendment:
        amend = ("<isAmendment>true</isAmendment><amendmentNo>{}</amendmentNo>"
                 "<amendmentInfo><amendmentType>{}</amendmentType></amendmentInfo>"
                 .format(*amendment))
    others = "".join(
        "<otherManager>{}<form13FFileNumber>{}</form13FFileNumber><name>{}</name>"
        "</otherManager>".format("<cik>{}</cik>".format(c) if c else "", fn, n)
        for n, c, fn in managers)
    others = "<otherManagersInfo>{}</otherManagersInfo>".format(others) if others else ""
    summary = ("<tableEntryTotal>{}</tableEntryTotal><tableValueTotal>{}</tableValueTotal>"
               .format(entries, value) if value is not None else
               "<tableEntryTotal/><tableValueTotal/>")
    if omitted is not None:
        summary += "<isConfidentialOmitted>{}</isConfidentialOmitted>".format(
            "true" if omitted else "false")
    return ("<edgarSubmission {ns}><headerData><submissionType>{form}</submissionType>"
            "<filerInfo><periodOfReport>{mm}-{dd}-{yyyy}</periodOfReport></filerInfo>"
            "</headerData><formData><coverPage>{amend}<reportType>{report}</reportType>"
            "{others}</coverPage><summaryPage>{summary}</summaryPage></formData>"
            "</edgarSubmission>").format(ns=NS, form=form, mm=mm, dd=dd, yyyy=yyyy,
                                         amend=amend, report=report, others=others,
                                         summary=summary)


class Resp:
    def __init__(self, status=200, body="", js=None):
        self.status_code, self._body, self._js = status, body, js

    def json(self):
        return self._js

    @property
    def text(self):
        return self._body

    @property
    def content(self):
        return self._body.encode("utf-8")


class Edgar:
    """A fake EDGAR keyed the way the real one is, with a log of every request."""

    def __init__(self):
        self.feeds, self.docs, self.status, self.log = {}, {}, {}, []
        self.older = {}             # cik -> {page name: rows}, the filings.files pages

    def page(self, cik, name, rows):
        """An older feed page: [(form, accession, period, filed)], served only when a
        read follows filings.files."""
        self.older.setdefault(int(cik), {})[name] = [{
            "form": form, "accessionNumber": accession, "filingDate": filed,
            "reportDate": period, "primaryDocument": "form.txt",
            "acceptanceDateTime": filed + "T20:00:00.000Z"}
            for form, accession, period, filed in rows]

    @staticmethod
    def _block(rows):
        return {k: [r[k] for r in rows] for k in (
            "form", "accessionNumber", "filingDate", "reportDate",
            "primaryDocument", "acceptanceDateTime")}

    def filing(self, cik, accession, form, period, filed, accepted_et=None,
               cover_xml=None, feed_accepted=None, primary="xslForm13F_X02/primary_doc.xml"):
        self.feeds.setdefault(int(cik), []).insert(0, {
            "form": form, "accessionNumber": accession, "filingDate": filed,
            "reportDate": period, "primaryDocument": primary,
            "acceptanceDateTime": feed_accepted or filed + "T20:00:00.000Z"})
        if cover_xml is not None:
            self.docs[(accession, "primary_doc.xml")] = cover_xml
        self.docs[(accession, accession + "-index.htm")] = INDEX_PAGE.format(
            accepted_et or filed + " 16:05:00")

    def __call__(self, url, contact):
        self.log.append(url)
        if url in self.status:
            return Resp(self.status[url])
        m = re.search(r"/submissions/CIK(\d{10})\.json$", url)
        if m:
            cik = int(m.group(1))
            return Resp(js={"filings": {
                "recent": self._block(self.feeds.get(cik, [])),
                "files": [{"name": n} for n in self.older.get(cik, {})]}})
        m = re.search(r"/submissions/(CIK(\d{10})-submissions-\d+\.json)$", url)
        if m:
            return Resp(js=self._block(self.older[int(m.group(2))][m.group(1)]))
        m = re.search(r"/edgar/data/\d+/(\d{18})/(.+)$", url)
        n = m.group(1)
        acc = "{}-{}-{}".format(n[:10], n[10:12], n[12:])
        body = self.docs.get((acc, m.group(2)))
        return Resp(body=body) if body is not None else Resp(404)

    def url(self, cik, accession, doc):
        return "https://www.sec.gov/Archives/edgar/data/{}/{}/{}".format(
            int(cik), accession.replace("-", ""), doc)


@pytest.fixture
def edgar(monkeypatch):
    e = Edgar()
    monkeypatch.setattr(AI, "_get", e)
    return e


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "arr.db")
    con = dbmod.connect(path)
    yield con, path
    con.close()


@pytest.fixture(autouse=True)
def today_is_the_anchor(monkeypatch):
    """The pages' 'today' is the fixtures' day. Without this every page test here would
    turn over with the calendar: on 2026-11-17 the fixtures' closed quarter stops being
    the closed quarter, and every anchor but today's reads as a replay."""
    monkeypatch.setattr(q, "today", lambda: ANCHOR)


def acc(cik, yy, n):
    return "{:010d}-{:02d}-{:06d}".format(int(cik), yy, n)


def hold(con, cik, accession, period, filed, value=1000):
    """A one-line book. Its value equals the default cover total, as a real book's
    does: a held book that disagrees with its own cover is a finding, not a fixture."""
    con.execute(
        "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, ticker,"
        " issuer, put_call, value, shares, ingested_at_unix, value_scale) "
        "VALUES (?,?,?,?,'037833100','AAPL','APPLE','long',?,10,0,1)",
        (str(int(cik)), accession, period, filed, value))
    con.commit()


def bare_db(tmp_path):
    """A production-shaped database BEFORE the ledger's migration: it has holdings and
    neither ledger table. The dashboard meets exactly this between a deploy and
    post_deploy.sh."""
    path = str(tmp_path / "bare.db")
    raw = sqlite3.connect(path)
    raw.execute("CREATE TABLE thirteenf_holdings(cik TEXT, accession TEXT, period TEXT, "
                "filed_date TEXT, value INTEGER)")
    raw.commit()
    raw.close()
    return path


def registry(tmp_path, monkeypatch, filers):
    """A registry with ZERO-PADDED CIKs, as production's is, against tables that store
    them bare. A raw string join here matches nothing."""
    d = tmp_path / "analysis"
    d.mkdir(exist_ok=True)
    p = d / "registry.json"
    p.write_text(json.dumps({"as_of": "2026-01-01", "entries": [
        {"name": n, "cik": str(c).zfill(10), "role": "manager_13f", "thesis": "macro"}
        for c, n in filers]}), encoding="utf-8")
    monkeypatch.setattr(q.dbmod, "find_artifact", lambda *a, **k: str(p))


def et(day, hhmm):
    """A unix time at hh:mm US Eastern (standard time) on `day`."""
    return int(dt.datetime.fromisoformat("{}T{}:00-05:00".format(day, hhmm)).timestamp())


def portfolio_line(ro, p, res):
    """The /portfolios ledger paragraph, built the way the page builds it."""
    return dash._portfolio_arrival(p, *dash._portfolio_ledger(ro, p, res)[1:])


def cells(page, name):
    """[the cell bodies] of one filer's row in the /arrivals table. 0 filer, 1 closed,
    2 open, 3 last 13F, 4 filed, 5 book held, 6 deadlines passed, 7 filed with no book
    held, 8 amendments not applied, 9 EDGAR read, 10 notes."""
    table = page[page.index("<table>"):page.index("</table>")]
    for tr in table.split("<tr")[1:]:
        if ">{}</a>".format(name) in tr:
            return re.findall(r"<td>(.*?)</td>", tr, re.S)
    raise AssertionError("no table row for " + name)


def rows_by_name(path, anchor=ANCHOR):
    ro = q.connect_ro(path)
    try:
        res = q.q_arrivals(ro, anchor=anchor)
    finally:
        ro.close()
    return res, {r["filer"]: r for r in res["rows"]}


# ------------------------------------------------------------------ the calendar --
@pytest.mark.parametrize("period, due", [
    ("2026-06-30", "2026-08-14"),     # a Friday: no roll
    ("2026-09-30", "2026-11-16"),     # +45 is Saturday 11-14
    ("2026-12-31", "2027-02-16"),     # +45 is Sunday; Monday is Washington's Birthday
    ("2025-12-31", "2026-02-17"),     # the one rolled quarter the corpus holds
    ("2027-03-31", "2027-05-17"),
])
def test_the_deadline_rolls_past_weekends_and_federal_holidays(period, due):
    """Quarter end + 45 calendar days called 15 on-time filers late for Q4 2025 and
    would print 'filing window passed' on every card on deadline day 2026-11-16."""
    assert A.deadline(period) == due


def test_the_filing_calendar_is_the_federal_one_not_the_markets():
    """Good Friday is a market holiday and an EDGAR filing day; Columbus Day and
    Veterans Day are the reverse. The price-session calendar gets all three wrong."""
    import datetime as dt
    assert A.is_filing_day(dt.date(2026, 4, 3))            # Good Friday
    assert not A.is_filing_day(dt.date(2026, 10, 12))      # Columbus Day
    assert not A.is_filing_day(dt.date(2026, 11, 11))      # Veterans Day
    assert not A.is_filing_day(dt.date(2021, 12, 31))      # New Year's Day observed
    assert A.is_filing_day(dt.date(2020, 6, 19))           # Juneteenth, before the statute
    assert not A.is_filing_day(dt.date(2022, 6, 20))       # Juneteenth observed (a Sunday)


@pytest.mark.parametrize("anchor, closed, open_", [
    ("2026-10-02", "2026-06-30", "2026-09-30"),
    ("2026-11-16", "2026-06-30", "2026-09-30"),   # deadline day is still open
    ("2026-11-17", "2026-09-30", None),
    ("2026-09-30", "2026-06-30", None),           # the quarter is not over on its last day
    ("2027-01-01", "2026-09-30", "2026-12-31"),
])
def test_windows_name_the_closed_period_and_the_open_one(anchor, closed, open_):
    assert A.windows(anchor) == (closed, open_)


def test_the_strip_countdown_uses_the_rolled_deadline():
    assert q._days_to_next_13f("2026-06-30", "2026-11-16") == 0
    assert q._days_to_next_13f("2026-06-30", "2026-11-15") == 1
    assert q._days_to_next_13f("2026-09-30", "2027-02-16") == 0


# ------------------------------------------------------------------ what a filing states --
def test_a_notice_cover_names_the_manager_who_reports_the_holdings():
    """A 13F-NT is not a notice of no holdings. The form says the holdings are in
    another manager's report and names it."""
    c = A.parse_cover(PERSHING_NT)
    assert c["report_type"] == "13F NOTICE" and c["submission_type"] == "13F-NT"
    assert c["period"] == "2026-06-30"
    assert c["value_total"] is None and c["entry_total"] is None
    assert c["other_managers"] == [{"name": "PERSHING SQUARE INC.",
                                    "cik": "0002026053", "file_number": "028-25746"}]


def test_the_cover_list_is_never_confused_with_the_summary_pages():
    """Both lists are built from an element called otherManager. The cover's says who
    reports FOR this filer; the summary's says who is included IN this report."""
    c = A.parse_cover(FIRST_EAGLE_AMENDMENT)
    assert [m["name"] for m in c["other_managers"]] == ["EXCHANGE TRADED CONCEPTS, LLC"]
    assert (c["is_amendment"], c["amendment_no"], c["amendment_type"]) == (
        1, "1", "NEW HOLDINGS")
    assert c["report_type"] == "13F COMBINATION REPORT"
    assert (c["entry_total"], c["value_total"]) == (616, 60750982695)


def test_an_absent_isAmendment_reads_as_not_an_amendment():
    c = A.parse_cover(THIEL_EMPTY)
    assert c["is_amendment"] == 0 and c["amendment_type"] is None
    assert (c["entry_total"], c["value_total"], c["confidential_omitted"]) == (1, 0, 0)


def test_the_acceptance_stamp_comes_from_the_index_page_or_not_at_all():
    assert A.parse_accepted(INDEX_PAGE.format("2026-05-15 18:33:35")) == "2026-05-15 18:33:35"
    assert A.parse_accepted(INDEX_PAGE.format("soon")) is None
    assert A.parse_accepted("<html>Rate limit</html>") is None


def test_the_feed_read_keeps_every_13f_form_and_guesses_no_period():
    """The old readers kept form == "13F-HR" only, and one of them once borrowed the
    FILING date for a missing report date."""
    block = {"form": ["13F-HR", "10-K", "13F-NT", "13F-HR/A", "13FCONP"],
             "accessionNumber": ["a", "b", "c", "d", "e"],
             "filingDate": ["2026-08-14"] * 5,
             "reportDate": [""] * 5,                           # stated for none of them
             "acceptanceDateTime": ["2026-08-14T20:00:00.000Z"] * 5,
             "primaryDocument": ["x"] * 5}
    rows = A.family_rows(block)
    assert [r["form"] for r in rows] == ["13F-HR", "13F-NT", "13F-HR/A", "13FCONP"]
    assert all(r["period"] is None for r in rows)


@pytest.mark.parametrize("missing", A.FEED_COLUMNS)
def test_a_feed_block_missing_a_column_is_refused_not_read_as_no_filings(missing):
    """A feed whose shape changed would otherwise be a clean read of zero 13F filings:
    the filer prints 'before first 13F', unpromoted, beside the book it holds."""
    block = {c: ["13F-HR"] if c == "form" else ["x"] for c in A.FEED_COLUMNS}
    del block[missing]
    with pytest.raises(ValueError, match="no usable"):
        A.family_rows(block)
    short = {c: ["13F-HR", "13F-HR"] if c == "form" else ["x", "y"]
             for c in A.FEED_COLUMNS}
    short[missing] = short[missing][:1]
    with pytest.raises(ValueError):
        A.family_rows(short)


def _f(form="13F-HR", value=1000, entries=5, status="read", report="13F HOLDINGS REPORT"):
    return {"form": form, "value_total": value, "entry_total": entries,
            "cover_status": status, "report_type": report}


def test_an_empty_book_is_a_stated_value_of_zero_never_an_entry_count():
    """tableEntryTotal is 0 on two real books (Third Point 2019-09-30, Scion 2023-12-31)
    and 1 on fifteen real one-position books. tableValueTotal 0 marks exactly the seven
    placeholders."""
    assert A.filing_kind(_f(value=0, entries=1)) == "empty"
    assert A.filing_kind(_f(value=0, entries=0)) == "empty"
    assert A.filing_kind(_f(value=94591240, entries=0)) == "book"
    assert A.filing_kind(_f(value=2028956, entries=1)) == "book"


def test_a_filing_whose_cover_was_not_read_is_never_called_empty_or_a_book():
    assert A.filing_kind(_f(status="failed", value=None)) == "unread"
    assert A.filing_kind(_f(status="pending", value=None)) == "unread"
    assert A.period_state([_f(status="failed", value=None)]) == A.FILED_UNREAD
    # a cover that WAS read and states no table value is neither, and is not 'unread'
    assert A.filing_kind(_f(status="read", value=None)) == "unstated"
    assert A.period_state([_f(status="read", value=None)]) == A.FILED_UNREAD


def test_nothing_is_called_empty_or_a_notice_while_a_filing_for_it_is_unread():
    """A placeholder 13F-HR and a 13F-HR/A whose cover fetch failed that night: the
    amendment may be the book. 'filed-empty' is said only when nothing is left unread."""
    unread = _f(form="13F-HR/A", status="failed", value=None)
    notice = _f(form="13F-NT", value=None, report="13F NOTICE")
    assert A.period_state([_f(value=0), unread]) == A.FILED_UNREAD
    assert A.period_state([notice, unread]) == A.FILED_UNREAD
    assert A.period_state([_f(), unread]) == A.FILED          # a read book still wins


def test_the_period_state_follows_what_the_filings_state():
    notice = _f(form="13F-NT", value=None, report="13F NOTICE")
    assert A.period_state([]) == A.AWAITED
    assert A.period_state([_f()]) == A.FILED
    assert A.period_state([_f(value=0)]) == A.FILED_EMPTY
    assert A.period_state([notice]) == A.NT_NOTICE
    # a notice says where the holdings are, so it outranks an empty report
    assert A.period_state([_f(value=0), notice]) == A.NT_NOTICE
    # and a placeholder later restated into a book is a filed book
    assert A.period_state([_f(value=0), _f(form="13F-HR/A")]) == A.FILED
    # a paper confidential filing is not an arrival
    assert A.period_state([_f(form="13FCONP", status="paper", value=None)]) == A.AWAITED


# ------------------------------------------------------------------ the EDGAR read --
def test_one_read_records_every_family_filing_and_reads_recent_covers(db, edgar):
    con, _ = db
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30", value=5000))
    edgar.filing(111, acc(111, 19, 1), "13F-HR", "2019-06-30", "2019-08-14",
                 cover_xml=cover("13F-HR", "2019-06-30"))
    edgar.filing(111, "9999999997-12-000001", "13FCONP", "2011-12-31", "2012-08-15",
                 primary="doc.paper")
    r = AI.read_filer(con, "0000000111", "c", now=NOW)
    assert r["family"] == 3 and len(r["new"]) == 3 and r["details_read"] == 1
    got = {a: (f, s, v) for a, f, s, v in con.execute(
        "SELECT accession, form, cover_status, value_total FROM thirteenf_arrivals")}
    assert got[acc(111, 26, 2)] == ("13F-HR", "read", 5000)
    assert got[acc(111, 19, 1)] == ("13F-HR", "pending", None)     # below the floor
    assert got["9999999997-12-000001"] == ("13FCONP", "paper", None)
    # the paper filing and the pre-floor cover were never requested
    assert not [u for u in edgar.log if "999999999712000001" in u or "19000001" in u]
    assert con.execute("SELECT accepted_et FROM thirteenf_arrivals WHERE accession=?",
                       (acc(111, 26, 2),)).fetchone()[0] == "2026-08-14 16:05:00"


def test_a_backfilled_filing_has_no_arrival_time_and_a_new_one_does(db, edgar):
    """seen_at_unix in the old gate table is a backfill time for 209 of 225 filings,
    2.5 years after some of them were filed. The first read of a feed is history."""
    con, _ = db
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    AI.read_filer(con, "111", "c", now=NOW)
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-09-30", "2026-11-04",
                 cover_xml=cover("13F-HR", "2026-09-30"))
    r = AI.read_filer(con, "111", "c", now=NOW + 86400)
    assert [n["accession"] for n in r["new"]] == [acc(111, 26, 2)]
    seen = dict(con.execute("SELECT accession, first_seen_unix FROM thirteenf_arrivals"))
    assert seen == {acc(111, 26, 1): None, acc(111, 26, 2): NOW + 86400}


def test_a_second_read_of_an_unchanged_feed_writes_and_fetches_nothing_new(db, edgar):
    con, _ = db
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    AI.read_filer(con, "111", "c", now=NOW)
    before = list(con.execute("SELECT * FROM thirteenf_arrivals"))
    edgar.log.clear()
    r = AI.read_filer(con, "111", "c", now=NOW + 60)
    assert r["new"] == [] and r["details_read"] == 0
    assert list(con.execute("SELECT * FROM thirteenf_arrivals")) == before
    assert len(edgar.log) == 1 and "submissions" in edgar.log[0]


def test_a_failed_feed_read_never_moves_the_read_time(db, edgar):
    """Never record the request as the response: 'awaited as of <date>' is dated by the
    last read that happened."""
    con, _ = db
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    AI.read_filer(con, "111", "c", now=NOW)
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 503
    with pytest.raises(RuntimeError, match="HTTP 503"):
        AI.read_filer(con, "111", "c", now=NOW + 86400)
    read, attempted, error = con.execute(
        "SELECT read_unix, attempted_unix, error FROM thirteenf_feed_reads").fetchone()
    assert (read, attempted) == (NOW, NOW + 86400) and "503" in error


def test_a_filer_whose_feed_was_never_read_is_not_read_not_awaited(db, edgar):
    con, _ = db
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 403
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW)
    assert con.execute("SELECT read_unix FROM thirteenf_feed_reads").fetchone()[0] is None
    row = A.filer_view("111", "X", A.load(con)[111], NO_BOOK, ANCHOR, {111})
    assert row["closed_state"] == row["open_state"] == A.NOT_READ
    assert "read failed: RuntimeError: submissions HTTP 403" in row["notes"]
    # nothing about EDGAR is asserted for a feed that was never read: no zero, no ''
    assert (row["amendments_not_applied"], row["filed_not_held"],
            row["missed_periods"]) == (None, None, None)


def test_an_error_response_is_a_failed_read_never_an_empty_filing(db, edgar):
    """The old path had no status check: an HTTP error whose body parsed as XML was
    recorded as a filing with no holdings, marked seen, and never retried."""
    con, _ = db
    a = acc(111, 26, 1)
    edgar.filing(111, a, "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30", value=0))
    edgar.status[edgar.url(111, a, "primary_doc.xml")] = 403
    r = AI.read_filer(con, "111", "c", now=NOW)
    assert r["details_failed"] == [
        {"accession": a, "what": "cover", "error": "cover: RuntimeError: cover HTTP 403"}]
    assert con.execute("SELECT cover_status, value_total, read_error FROM "
                       "thirteenf_arrivals").fetchone() == (
        "failed", None, "cover: RuntimeError: cover HTTP 403")
    assert A.period_state(A.load(con)[111]["filings"]) == A.FILED_UNREAD
    # and it is retried: the next read gets the real cover
    del edgar.status[edgar.url(111, a, "primary_doc.xml")]
    assert AI.read_filer(con, "111", "c", now=NOW + 60)["details_read"] == 1
    assert A.period_state(A.load(con)[111]["filings"]) == A.FILED_EMPTY
    assert con.execute("SELECT read_error FROM thirteenf_arrivals").fetchone()[0] is None


def test_a_404_cover_is_a_fact_only_where_the_feed_names_no_xml_cover(db, edgar):
    con, _ = db
    modern, text_era = acc(111, 26, 1), acc(111, 24, 1)
    edgar.filing(111, modern, "13F-HR", "2026-06-30", "2026-08-14")          # no cover served
    edgar.filing(111, text_era, "13F-HR", "2024-03-31", "2024-05-15", primary="form.txt")
    r = AI.read_filer(con, "111", "c", now=NOW)
    status = dict(con.execute("SELECT accession, cover_status FROM thirteenf_arrivals"))
    assert status == {modern: "failed", text_era: "no_xml_cover"}
    assert [f["accession"] for f in r["details_failed"]] == [modern]


def test_a_cover_for_some_other_filing_is_refused(db, edgar):
    con, _ = db
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    r = AI.read_filer(con, "111", "c", now=NOW)
    assert "cover states 13F-HR for 2026-03-31" in r["details_failed"][0]["error"]
    assert con.execute("SELECT cover_status, report_type FROM thirteenf_arrivals"
                       ).fetchone() == ("failed", None)


def test_one_dark_filer_never_stops_the_roster_and_is_reported(db, edgar):
    con, _ = db
    edgar.filing(222, acc(222, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 503
    rep = AI.read_roster(con, "c", {"0000000111": "Dark", "0000000222": "Light"}, now=NOW)
    assert (rep["filers"], rep["read"]) == (2, 1)
    assert [f["name"] for f in rep["failed_reads"]] == ["Dark"]
    assert [n["name"] for n in rep["new"]] == ["Light"]


# ------------------------------------------------------------------ the four states --
def _four_filers(con, edgar, tmp_path, monkeypatch):
    """One filer per state for the closed period 2026-06-30, read on 2026-10-02."""
    filers = [(111, "Filed Fund"), (222, "Empty Fund"), (333, "Notice Fund"),
              (444, "Quiet Fund")]
    registry(tmp_path, monkeypatch, filers)
    # Each has an earlier quarter on EDGAR, as every real filer does: a filer with no
    # 13F at all is 'before first 13F', which is a different statement from awaited.
    edgar.filing(111, acc(111, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    edgar.filing(222, acc(222, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31", value=0, entries=1))
    edgar.filing(333, acc(333, 26, 0), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-06-30", "2026-08-14")
    edgar.filing(222, acc(222, 26, 1), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30", value=0, entries=1))
    hold(con, 222, acc(222, 25, 9), "2025-09-30", "2025-11-14")
    edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                 accepted_et="2026-08-14 16:23:33",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PARENT INC.", "0002026053", "028-25746")]))
    hold(con, 333, acc(333, 26, 0), "2026-03-31", "2026-05-15")
    edgar.filing(444, acc(444, 25, 1), "13F-HR", "2025-09-30", "2025-11-03",
                 cover_xml=cover("13F-HR", "2025-09-30"))
    hold(con, 444, acc(444, 25, 1), "2025-09-30", "2025-11-03")
    AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in filers}, now=NOW)


def test_the_four_ratified_states_are_told_apart(db, edgar, tmp_path, monkeypatch):
    """Before the ledger all four read the same: a quarter with no holdings rows."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    res, by = rows_by_name(path)
    assert res["ledger"] == "ok"
    assert {n: r["closed_state"] for n, r in by.items()} == {
        "Filed Fund": A.FILED, "Empty Fund": A.FILED_EMPTY,
        "Notice Fund": A.NT_NOTICE, "Quiet Fund": A.AWAITED}
    assert res["closed_coverage"] == {A.FILED: 1, A.FILED_EMPTY: 1, A.NT_NOTICE: 1,
                                      A.AWAITED: 1}
    # the open window: nobody has filed for 2026-09-30 and it is due on the ROLLED date
    assert res["open_coverage"] == {A.AWAITED: 4}
    assert (res["open_period"], res["open_deadline"]) == ("2026-09-30", "2026-11-16")
    assert all(r["open_due"] == "2026-11-16" for r in by.values())


def test_a_notice_says_where_the_holdings_are_and_whether_we_hold_them(
        db, edgar, tmp_path, monkeypatch):
    """Pershing's card read 'filing window passed' the day after its 13F-NT. Its book
    is a $19.5bn public filing under a CIK this system does not track."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    r = by["Notice Fund"]
    assert (r["last_form"], r["last_period"], r["book_period"]) == (
        "13F-NT", "2026-06-30", "2026-03-31")
    assert r["last_accepted_et"] == "2026-08-14 16:23:33"
    assert ("holdings are reported by PARENT INC. (CIK 2026053, not a tracked filer)"
            in r["notes"])
    assert r["missed_periods"] == "" and r["closed_days_past_deadline"] is None


def test_a_stale_filer_shows_its_last_filing_and_every_deadline_it_passed(
        db, edgar, tmp_path, monkeypatch):
    """Scion and a filer one day late carried the same words. The row states the last
    period filed and the quarters since with nothing on EDGAR."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    r = by["Quiet Fund"]
    assert (r["closed_state"], r["closed_days_past_deadline"]) == (A.AWAITED, 49)
    assert (r["last_form"], r["last_period"]) == ("13F-HR", "2025-09-30")
    assert r["missed_periods"] == "2025-12-31, 2026-03-31, 2026-06-30"


def test_filed_empty_is_not_a_stale_book(db, edgar, tmp_path, monkeypatch):
    """Thiel Macro's two placeholder quarters were period gaps: the card would have
    shown the older book as 'filing window passed' across two on-time filings."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    _, by = rows_by_name(path)
    r = by["Empty Fund"]
    assert r["closed_state"] == A.FILED_EMPTY and r["missed_periods"] == ""
    assert (r["last_period"], r["book_period"]) == ("2026-06-30", "2025-09-30")
    # an empty report has nothing to hold
    assert (r["amendments_not_applied"], r["filed_not_held"]) == (0, "")


def test_a_past_anchor_replays_what_edgar_held_that_day(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    res, by = rows_by_name(path, anchor="2026-08-10")     # four days before the deadline
    assert (res["closed_period"], res["open_period"]) == ("2026-03-31", "2026-06-30")
    assert {r["open_state"] for r in by.values()} == {A.AWAITED}
    assert res["closed_coverage"] == {A.FILED: 2, A.FILED_EMPTY: 1, A.AWAITED: 1}
    res, by = rows_by_name(path, anchor="2026-08-14")
    assert by["Filed Fund"]["open_state"] == A.FILED
    assert by["Notice Fund"]["open_state"] == A.NT_NOTICE


def test_a_filer_with_no_13f_at_all_is_not_called_awaited(db, edgar, tmp_path, monkeypatch):
    """With nothing ever filed there is no evidence a filing is owed. Affinity's first
    13F arrived 21 days after a deadline it may not have had."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Unfiled Fund")])
    edgar.feeds[111] = []
    AI.read_filer(con, "111", "c", now=NOW)
    res, by = rows_by_name(path)
    r = by["Unfiled Fund"]
    assert res["ledger"] == "ok"
    assert (r["closed_state"], r["open_state"]) == (A.NOT_DUE, A.NOT_DUE)
    assert "no 13F-family filing on EDGAR" in r["notes"] and r["last_form"] is None


def test_a_quarter_before_a_filers_first_13f_was_never_awaited(
        db, edgar, tmp_path, monkeypatch):
    """Founders Fund Growth II's first 13F was for 2025-12-31. On its filing day the
    quarter before read 'awaited, 95 days past the deadline'."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "New Fund")])
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2025-12-31", "2026-02-17",
                 cover_xml=cover("13F-HR", "2025-12-31"))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2026-02-17")
    r = by["New Fund"]
    assert (r["closed_period"], r["closed_state"]) == ("2025-09-30", A.NOT_DUE)
    assert r["open_state"] == A.FILED and r["closed_days_past_deadline"] is None


def test_last_filed_is_the_newest_period_not_the_newest_filing(
        db, edgar, tmp_path, monkeypatch):
    """Berkshire's Q1 2025 amendment was accepted five minutes AFTER its Q2 original,
    on the same day. Newest-by-date would call Q1 its last filed period."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Berk")])
    edgar.filing(111, acc(111, 25, 1), "13F-HR", "2025-03-31", "2025-05-15",
                 cover_xml=cover("13F-HR", "2025-03-31", value=258701144516, entries=110))
    edgar.filing(111, acc(111, 25, 2), "13F-HR", "2025-06-30", "2025-08-14",
                 feed_accepted="2025-08-14T20:05:02.000Z",
                 cover_xml=cover("13F-HR", "2025-06-30"))
    edgar.filing(111, acc(111, 25, 3), "13F-HR/A", "2025-03-31", "2025-08-14",
                 feed_accepted="2025-08-14T20:10:02.000Z",
                 cover_xml=cover("13F-HR/A", "2025-03-31", value=1106550356, entries=4,
                                 amendment=("1", "NEW HOLDINGS")))
    for a, per, filed in ((acc(111, 25, 1), "2025-03-31", "2025-05-15"),
                          (acc(111, 25, 2), "2025-06-30", "2025-08-14")):
        hold(con, 111, a, per, filed)
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2025-08-20")
    r = by["Berk"]
    assert (r["last_form"], r["last_period"]) == ("13F-HR", "2025-06-30")
    # the amendment is named with both stated totals and applied to nothing
    assert (r["amendments_not_applied"], r["filed_not_held"]) == (1, "")
    assert ("13F-HR/A (NEW HOLDINGS) for 2025-03-31 filed 2025-08-14 is on EDGAR and is "
            "not applied to the book held; its cover states 1,106,550,356 against the "
            "original's 258,701,144,516") in r["notes"]


def test_a_filing_on_edgar_with_no_holdings_held_is_said(db, edgar, tmp_path, monkeypatch):
    """'Filed' comes from the filing and 'held' from the holdings table. The night a
    13F lands and its ingest fails, the two differ and the page must say so."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Fund")])
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31"))
    edgar.filing(111, acc(111, 26, 2), "13F-HR", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    hold(con, 111, acc(111, 26, 1), "2026-03-31", "2026-05-15")
    AI.read_filer(con, "111", "c", now=NOW)
    res, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["closed_state"], r["book_period"], r["filed_not_held"]) == (
        A.FILED, "2026-03-31", "2026-06-30")
    assert "13F-HR for 2026-06-30 filed 2026-08-14 is on EDGAR and no holdings" in r["notes"]
    # it is not an amendment and is never pooled with them: it wants a look tonight
    assert r["amendments_not_applied"] == 0 and r["needs_look"] is True
    summary = dash._arrivals_summary(res)
    assert "filed on EDGAR, no book held here: Fund 2026-06-30" in summary
    assert "amendment(s)" not in summary


def test_a_combination_report_and_a_late_date_are_stated(db, edgar, tmp_path, monkeypatch):
    """Kopernik's book is partial every quarter, and Situational Awareness's Q1 13F is
    dated three days late for a Friday 18:33 ET submission."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Kop"), (222, "Sit")])
    combo = [("Russell Investment Group, Ltd.", None, "028-17598")]
    edgar.filing(111, acc(111, 26, 1), "13F-HR", "2026-03-31", "2026-05-15",
                 cover_xml=cover("13F-HR", "2026-03-31", report="13F COMBINATION REPORT",
                                 managers=combo))
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-03-31", "2026-05-20",
                 cover_xml=cover("13F-HR/A", "2026-03-31", report="13F COMBINATION REPORT",
                                 managers=combo, amendment=("1", "RESTATEMENT")))
    edgar.filing(222, acc(222, 26, 1), "13F-HR", "2026-03-31", "2026-05-18",
                 accepted_et="2026-05-15 18:33:35", cover_xml=cover("13F-HR", "2026-03-31"))
    AI.read_roster(con, "c", {"0000000111": "Kop", "0000000222": "Sit"}, now=NOW)
    _, by = rows_by_name(path, anchor="2026-05-25")
    kop = by["Kop"]["notes"]
    assert kop.count("is a combination report") == 1     # stated once, not per filing
    assert "Russell Investment Group, Ltd. (028-17598)" in kop
    assert ("13F-HR for 2026-03-31 is dated 3 day(s) after the 2026-05-15 deadline "
            "(accepted 2026-05-15 18:33:35 ET)") in by["Sit"]["notes"]


def test_an_absent_or_unread_ledger_is_said_never_rendered_as_awaited(
        tmp_path, monkeypatch):
    """The dashboard opens the database read-only and cannot create a table. Between a
    deploy and the first read, 28 cards must not read 'awaited'."""
    registry(tmp_path, monkeypatch, [(111, "Fund")])
    bare = bare_db(tmp_path)
    res, by = rows_by_name(bare)
    assert res["ledger"] == "absent" and by["Fund"]["closed_state"] == A.NOT_READ
    assert "not built" in dash._arrivals_summary(res)
    fresh = str(tmp_path / "fresh.db")
    dbmod.connect(fresh).close()
    res, by = rows_by_name(fresh)
    assert res["ledger"] == "empty" and by["Fund"]["open_state"] == A.NOT_READ
    assert "no EDGAR read yet" in dash._arrivals_summary(res)
    assert "awaited" not in dash._arr_card_line(by["Fund"])


# ------------------------------------------------------------------ the surfaces --
def test_every_arrival_row_key_is_exported(db, edgar, tmp_path, monkeypatch):
    """The CSV contract raises on an unlisted key, and a key added to a query row
    without its column answered HTTP 500 for nine days last month."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    res, _ = rows_by_name(path)
    assert set(res["rows"][0]) == set(dash._ARR_CSV_COLS)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR]}), _path="/arrivals")
        text = dash._build_arrivals_csv(ro, p, full=True)
        spec = dash._page_brief_spec(ro, p, "/arrivals")
        page = dash.view_arrivals(ro, p)
    finally:
        ro.close()
    lines = text.strip().splitlines()
    assert lines[0].split(",")[:2] == ["filer", "cik"] and len(lines) == 5
    assert "nt_notice" in text and "PARENT INC." in text
    assert spec["tables"][0][3] is dash._ARR_PDF_WIDTHS
    assert len(spec["tables"][0][1]) == len(dash._ARR_PDF_WIDTHS)
    assert any("Notice Fund: 13F-NT for 2026-06-30" in n for n in spec["notes"])
    assert "NT-notice" in page and "49d past the deadline" in page
    assert dash.ROUTES["/arrivals"] is dash.view_arrivals
    assert "/arrivals" in dash.PRINTABLE_VIEWS


def test_the_strip_keeps_a_card_in_view_when_its_period_is_not_a_filed_book(
        db, edgar, tmp_path, monkeypatch):
    """The strip shows six cards by book size. Pershing was card 11, Scion 24 and
    Founders Fund VII 28 -- every filer the ledger exists for was behind the fold."""
    con, path = db
    filers = [(100 + i, "Big {}".format(i)) for i in range(8)] + [(444, "Quiet Fund")]
    registry(tmp_path, monkeypatch, filers)
    for cik, _name in filers[:8]:
        a, size = acc(cik, 26, 1), 10 ** 9 * (20 - cik % 100)
        edgar.filing(cik, a, "13F-HR", "2026-06-30", "2026-08-14",
                     cover_xml=cover("13F-HR", "2026-06-30", value=size))
        hold(con, cik, a, "2026-06-30", "2026-08-14", value=size)
    edgar.filing(444, acc(444, 25, 1), "13F-HR", "2025-09-30", "2025-11-03",
                 cover_xml=cover("13F-HR", "2025-09-30"))
    hold(con, 444, acc(444, 25, 1), "2025-09-30", "2025-11-03")
    AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in filers}, now=NOW)
    ro = q.connect_ro(path)
    try:
        html_ = dash._tracked_books_strip(q.q_tracked_books(ro, anchor=ANCHOR),
                                          q.q_arrivals(ro, anchor=ANCHOR))
    finally:
        ro.close()
    head, _, folded = html_.partition("<details>")
    assert "Quiet Fund" in head and "Quiet Fund" not in folded
    assert "Big 7" in folded                              # an ordinary small card still folds
    assert "2026-06-30 awaited, 49d past the deadline" in head
    # above the cards the line says how old the filer's last word is; on the card it
    # is not repeated, because it IS the book the card shows
    assert head.count("last 13F: 13F-HR for 2025-09-30, filed 2025-11-03") == 1
    assert head.count("last 13F:") == 1
    assert "2026-06-30 (due 2026-08-14): 8 filed, 1 awaited of 9 tracked filers" in head
    assert "2026-09-30 (due 2026-11-16): 9 awaited of 9 tracked filers" in head


def test_the_scan_leg_is_degraded_by_any_dark_filer_or_roster_drift(monkeypatch):
    """The holdings leg reads OK unless NOTHING landed, so 27 good filers hide one dark
    one. And the ledger is written from one roster and read through another."""
    from smart_money import thirteenf_ingest
    roster = {"0000000111": "A", "0000000222": "B"}
    monkeypatch.setattr(thirteenf_ingest, "CONFIRMED", roster)
    base = {"filers": 2, "read": 2, "family": 9, "new": [], "details_read": 0,
            "stamps_read": 0, "failed_reads": [], "failed_details": []}
    reg = {"entries": [{"role": "manager_13f", "cik": c} for c in roster]}

    def leg(report, registry_=reg):
        monkeypatch.setattr(AI, "read_roster", lambda *a, **k: dict(base, **report))
        return scan.leg_13f_arrivals(None, "c", registry_)[0]

    ok = leg({"new": [{"form": "13F-HR", "period": "2026-09-30", "name": "A"}]})
    assert (ok["status"], ok["items"]) == ("OK", 1) and "13F-HR 2026-09-30 A" in ok["note"]
    dark = leg({"read": 1, "failed_reads": [{"name": "B", "error": "HTTP 503"}]})
    assert dark["status"] == "DEGRADED"
    assert "feed reads failed=1 (B: HTTP 503)" in dark["note"]
    # the reason and the accession travel into the scan line: a cover that fails every
    # night otherwise repeats a bare count and only a hand run would say why
    cov = leg({"failed_details": [{"accession": "x", "what": "cover",
                                   "error": "cover: HTTP 403"}]})
    assert cov["status"] == "DEGRADED"
    assert "cover reads failed=1 (x cover: HTTP 403)" in cov["note"]
    # a stamp failure is counted as a stamp failure, never as a cover failure
    stamp = leg({"details_read": 1, "stamps_read": 0, "failed_details": [
        {"accession": "y", "what": "stamp", "error": "stamp: HTTP 429"}]})
    assert stamp["status"] == "DEGRADED"
    assert "covers_read=1 stamps_read=0" in stamp["note"]
    assert "stamp reads failed=1 (y stamp: HTTP 429)" in stamp["note"]
    assert "cover reads failed" not in stamp["note"]
    drift = leg({}, {"entries": [{"role": "manager_13f", "cik": "0000000111"}]})
    assert drift["status"] == "DEGRADED" and "roster drift" in drift["note"]
    assert "222" in drift["note"]


# ================================================================ after the review ==
# An adversarial review of the first build (five lenses, 2026-10-05) found no wrong
# state on today's data and these cases where a rule, or a sentence built on one,
# parted from what the filings and the reads support.

def evening(day):
    """A unix time on the evening of `day` in New York, when the nightly reads."""
    return int(dt.datetime.fromisoformat(day + "T20:00:00-05:00").timestamp())


def _fund(con, edgar, tmp_path, monkeypatch, name="Fund", cik=111):
    registry(tmp_path, monkeypatch, [(cik, name)])
    return cik


def _hr(edgar, cik, n, period, filed, yy=26, **cover_args):
    a = acc(cik, yy, n)
    edgar.filing(cik, a, "13F-HR", period, filed, cover_xml=cover("13F-HR", period,
                                                                 **cover_args))
    return a


def test_a_read_answers_for_the_last_day_whose_filings_it_can_have_seen():
    """The 22:30 nightly's UTC date is already tomorrow, and a read before EDGAR's 17:30
    ET cut-off has not seen the day's filings: 137 of 150 deadline-day originals with a
    stamp were accepted after 11:00."""
    assert AI.read_day(evening("2026-11-16")) == "2026-11-16"
    assert A.iso_utc(evening("2026-11-16")).startswith("2026-11-17")
    assert AI.read_day(NOW) == "2026-10-01"
    assert AI.read_day(et("2026-11-16", "09:00")) == "2026-11-15"
    assert AI.read_day(et("2026-11-16", "17:29")) == "2026-11-15"
    assert AI.read_day(et("2026-11-16", "17:30")) == "2026-11-16"
    # and the day the read happened on is a different fact, kept apart
    assert AI.calendar_day(et("2026-11-16", "09:00")) == "2026-11-16"
    assert AI.calendar_day(evening("2026-11-16")) == "2026-11-16"


def test_a_deadline_nobody_read_after_is_not_read_never_overdue(
        db, edgar, tmp_path, monkeypatch):
    """71% of filings arrive on deadline day. If the nightly does not run that night,
    'awaited, 1d past the deadline' would be printed for every one of them."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)               # last read: evening of 10-01
    res, by = rows_by_name(path, anchor="2026-11-17")
    r = by["Fund"]
    assert (r["closed_period"], r["closed_state"]) == ("2026-09-30", A.NOT_READ)
    assert r["closed_days_past_deadline"] is None and r["missed_periods"] == ""
    assert "no EDGAR read covers the 2026-09-30 deadline day of 2026-11-16" in r["notes"]
    assert ("the last EDGAR read answers for filings dated through 2026-10-01; states "
            "are as of that read") in r["notes"]
    summary = dash._arrivals_summary(res)
    assert "past the deadline" not in summary
    # and it is said ABOVE the cards, not only in a note the front page never prints
    assert ("1 of 1 feeds have no read that answers for the day before 2026-11-17 "
            "(oldest answers for 2026-10-01): their states are as of that read") in summary
    # read on deadline evening, the same row is a fact: awaited, one day past
    AI.read_filer(con, "111", "c", now=evening("2026-11-16"))
    _, by = rows_by_name(path, anchor="2026-11-17")
    r = by["Fund"]
    assert (r["closed_state"], r["closed_days_past_deadline"]) == (A.AWAITED, 1)
    assert "states are as of" not in r["notes"] and r["missed_periods"] == "2026-09-30"


def test_a_first_13f_filed_late_was_awaited_not_before_first(
        db, edgar, tmp_path, monkeypatch):
    """Affinity's first 13F, for 2024-12-31, was filed 2025-03-07. On each of the
    twenty days between the deadline and that filing the quarter was owed."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Late First")
    _hr(edgar, 111, 1, "2024-12-31", "2025-03-07", yy=25)
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2025-02-20")
    r = by["Late First"]
    assert (r["closed_state"], r["closed_days_past_deadline"]) == (A.AWAITED, 6)
    assert "no 13F-family filing on EDGAR for this filer as of 2025-02-20" in r["notes"]
    _, by = rows_by_name(path, anchor="2024-11-20")       # the quarter BEFORE its first
    assert by["Late First"]["closed_state"] == A.NOT_DUE


def test_a_feed_with_unread_older_pages_is_not_read_never_before_first(
        db, edgar, tmp_path, monkeypatch):
    """filings.recent starts in mid-2023 for Alphabet and Horizon Kinetics. Behind it
    the ledger knows nothing, and 'before first 13F' there was false: their 13F history
    starts in 2013 and 1999."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Heavy Filer")
    _hr(edgar, 111, 1, "2023-06-30", "2023-08-14", yy=23)
    edgar.page(111, "CIK0000000111-submissions-001.json",
               [("13F-HR", acc(111, 23, 0), "2023-03-31", "2023-05-12")])
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2023-07-10")
    r = by["Heavy Filer"]
    assert (r["closed_period"], r["closed_state"]) == ("2023-03-31", A.NOT_READ)
    assert ("the feed pages read start at 2023-06-30; older pages exist and were not "
            "read") in r["notes"]
    # nothing in the pages read is not nothing on EDGAR
    assert ("no 13F-family filing in the feed pages read for this filer as of "
            "2023-07-10") in r["notes"]
    assert "on EDGAR for this filer" not in r["notes"]
    AI.read_filer(con, "111", "c", now=NOW + DAY, deep=True)
    _, by = rows_by_name(path, anchor="2023-07-10")
    assert by["Heavy Filer"]["closed_state"] == A.FILED_UNREAD    # below the cover floor
    # an ordinary read afterwards does not make the ledger forget it holds the pages
    AI.read_filer(con, "111", "c", now=NOW + 2 * DAY)
    assert con.execute("SELECT older_pages, deep FROM thirteenf_feed_reads"
                       ).fetchone() == (1, 1)


def test_a_deep_run_after_the_first_read_stamps_no_history_as_arrived(db, edgar):
    """On the saved feeds a --deep run after any ordinary read would have stamped 374
    filings from 1999 to 2023 as arriving that night."""
    con, _ = db
    new, old = _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), acc(111, 5, 1)
    edgar.page(111, "CIK0000000111-submissions-001.json",
               [("13F-HR", old, "2005-03-31", "2005-05-16")])
    AI.read_filer(con, "111", "c", now=NOW)
    r = AI.read_filer(con, "111", "c", now=NOW + DAY, deep=True)
    assert [n["accession"] for n in r["new"]] == [old]
    assert dict(con.execute("SELECT accession, first_seen_unix FROM thirteenf_arrivals")
                ) == {new: None, old: None}


def test_the_first_13f_of_a_filer_already_read_is_an_arrival(db, edgar):
    con, _ = db
    edgar.feeds[111] = []
    AI.read_filer(con, "111", "c", now=NOW)               # a read that saw no 13F at all
    a = _hr(edgar, 111, 1, "2026-09-30", "2026-11-04")
    AI.read_filer(con, "111", "c", now=NOW + DAY)
    assert con.execute("SELECT first_seen_unix FROM thirteenf_arrivals WHERE accession=?",
                       (a,)).fetchone()[0] == NOW + DAY


def test_a_cover_is_kept_when_the_index_page_cannot_be_read(
        db, edgar, tmp_path, monkeypatch):
    """The states need the cover and never the acceptance stamp. One HTML page failing
    used to discard a parsed cover: a notice lost its reporting manager and a
    placeholder read 'cover not read' instead of filed-empty."""
    con, path = db
    registry(tmp_path, monkeypatch, [(222, "Empty Fund"), (333, "Notice Fund")])
    e = _hr(edgar, 222, 1, "2026-06-30", "2026-08-14", value=0, entries=1)
    n = acc(333, 26, 1)
    edgar.filing(333, n, "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PARENT INC.", "0002026053", "028-25746")]))
    for a in (e, n):
        edgar.docs[(a, a + "-index.htm")] = "<html>Request Rate Threshold Exceeded</html>"
    rep = AI.read_roster(con, "c", {"0000000222": "Empty Fund",
                                    "0000000333": "Notice Fund"}, now=NOW)
    assert [(f["what"], f["error"]) for f in rep["failed_details"]] == [
        ("stamp", "stamp: RuntimeError: index page states no Accepted stamp")] * 2
    # two covers read, no stamp: counted apart, so 'covers_read=2' cannot hide them
    assert (rep["details_read"], rep["stamps_read"]) == (2, 0)
    _, by = rows_by_name(path)
    assert by["Empty Fund"]["closed_state"] == A.FILED_EMPTY
    assert "holdings are reported by PARENT INC." in by["Notice Fund"]["notes"]
    # the stamp alone is owed and retried; the cover is not fetched again
    edgar.docs[(e, e + "-index.htm")] = INDEX_PAGE.format("2026-08-14 16:08:07")
    edgar.log.clear()
    AI.read_filer(con, "222", "c", now=NOW + DAY)
    assert [u.rsplit("/", 1)[-1] for u in edgar.log] == [
        "CIK0000000222.json", e + "-index.htm"]
    assert con.execute("SELECT cover_status, accepted_et, read_error FROM "
                       "thirteenf_arrivals WHERE accession=?", (e,)).fetchone() == (
        "read", "2026-08-14 16:08:07", None)


def test_one_bad_filing_never_discards_the_others_of_its_filer(db, edgar):
    """Every cover of a filer was stored in one transaction with the UPDATE outside the
    per-filing guard, so one value SQLite could not bind threw away all of them and
    reported the FEED read as failed, every night."""
    con, _ = db
    good = _hr(edgar, 111, 1, "2026-03-31", "2026-05-15")
    bad = _hr(edgar, 111, 2, "2026-06-30", "2026-08-14", value=10 ** 19)
    rep = AI.read_roster(con, "c", {"0000000111": "Fund"}, now=NOW)
    assert rep["read"] == 1 and rep["failed_reads"] == []
    assert [f["accession"] for f in rep["failed_details"]] == [bad]
    assert rep["failed_details"][0]["error"].startswith("cover: OverflowError")
    assert dict(con.execute("SELECT accession, cover_status FROM thirteenf_arrivals")
                ) == {good: "read", bad: "failed"}
    assert not con.in_transaction                          # nothing left open across a request


def test_a_period_the_feed_left_out_is_filled_in_never_guessed(
        db, edgar, tmp_path, monkeypatch):
    """A 13F the feed lists with no reportDate used to be invisible for good: never
    read, never 'last', and its quarter printed awaited beside it."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    a = acc(111, 26, 1)
    edgar.filing(111, a, "13F-HR", "", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-30"))
    AI.read_filer(con, "111", "c", now=NOW)
    # the cover STATES the period; that is not a guess
    assert con.execute("SELECT period, cover_status FROM thirteenf_arrivals"
                       ).fetchone() == ("2026-06-30", "read")
    _, by = rows_by_name(path)
    assert by["Fund"]["closed_state"] == A.FILED
    # with no cover to read, the filing still blocks 'awaited' and is named
    b = acc(111, 26, 2)
    edgar.filing(111, b, "13F-HR", "", "2026-10-01")
    AI.read_filer(con, "111", "c", now=NOW + DAY)
    _, by = rows_by_name(path, anchor="2026-10-03")
    r = by["Fund"]
    assert r["open_state"] == A.FILED_UNREAD
    assert ("13F-HR filed 2026-10-01: the feed states no quarter-end period for it "
            "(none)") in r["notes"]
    # and when a later feed states it, the row takes it
    edgar.feeds[111][0]["reportDate"] = "2026-09-30"
    AI.read_filer(con, "111", "c", now=NOW + 2 * DAY)
    assert con.execute("SELECT period FROM thirteenf_arrivals WHERE accession=?",
                       (b,)).fetchone()[0] == "2026-09-30"


def test_a_filer_that_resumes_keeps_the_quarters_it_skipped(
        db, edgar, tmp_path, monkeypatch):
    """If Scion files for Q3, its closed state becomes 'filed' and the row goes quiet.
    The three quarters it never filed for must not vanish with that."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Quiet Fund", 444)
    _hr(edgar, 444, 1, "2025-09-30", "2025-11-03", yy=25)
    hold(con, 444, _hr(edgar, 444, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "444", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Quiet Fund"]
    assert r["closed_state"] == A.FILED and r["needs_look"] is False
    assert r["missed_periods"] == "2025-12-31, 2026-03-31"


def test_an_amendment_that_states_zero_against_a_held_book_is_reported(
        db, edgar, tmp_path, monkeypatch):
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-06-30", "2026-08-20",
                 cover_xml=cover("13F-HR/A", "2026-06-30", value=0, entries=0,
                                 amendment=("1", "RESTATEMENT")))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["closed_state"], r["amendments_not_applied"]) == (A.FILED, 1)
    assert ("13F-HR/A (RESTATEMENT) for 2026-06-30 filed 2026-08-20 states a table "
            "value of 0 and is not applied to the book held") in r["notes"]


def test_the_summary_counts_what_is_not_applied_and_claims_nothing_about_holdings(
        db, edgar, tmp_path, monkeypatch):
    """The front page said '10 filing(s) on EDGAR state holdings that are not in the
    books held here'. Elliott's and Scion's restatements are row for row the books
    already held. The ledger reads no holdings row and cannot know what an amendment
    changes; it knows the amendment is not applied."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-06-30", "2026-08-20",
                 cover_xml=cover("13F-HR/A", "2026-06-30",
                                 amendment=("1", "RESTATEMENT")))   # identical total
    AI.read_filer(con, "111", "c", now=NOW)
    res, by = rows_by_name(path)
    summary = dash._arrivals_summary(res)
    assert res["amendments_not_applied"] == 1 and res["filed_not_held"] == []
    assert "1 amendment(s) on EDGAR are not applied to the books held here" in summary
    assert "state holdings" not in summary and "not in the book" not in summary
    assert by["Fund"]["needs_look"] is False             # standing, not tonight's problem
    ro = q.connect_ro(path)
    try:
        page = dash.view_arrivals(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                           _path="/arrivals"))
    finally:
        ro.close()
    assert "amendments not applied" in page and "not in book" not in page


def test_a_held_book_is_checked_against_its_own_cover(db, edgar, tmp_path, monkeypatch):
    """The cover's table value total is the one independent check that a holdings parse
    was complete, and nothing read it. On all 225 held filings the two agree within 4."""
    con, path = db
    registry(tmp_path, monkeypatch, [(111, "Whole"), (222, "Partial")])
    for cik, held_value, cover_value in ((111, 5000, 5003), (222, 5000, 9000)):
        a = _hr(edgar, cik, 1, "2026-06-30", "2026-08-14", value=cover_value)
        hold(con, cik, a, "2026-06-30", "2026-08-14", value=held_value)
    AI.read_roster(con, "c", {"0000000111": "Whole", "0000000222": "Partial"}, now=NOW)
    _, by = rows_by_name(path)
    assert by["Whole"]["book_check"] == "matches cover" and not by["Whole"]["needs_look"]
    assert "comes from" not in by["Whole"]["notes"]
    p = by["Partial"]
    assert p["book_check"] == "differs from cover" and p["needs_look"] is True
    assert ("the book held for 2026-06-30 sums to 5,000 in the filing's own units and "
            "its cover states 9,000") in p["notes"]


def test_a_small_book_that_equals_its_cover_is_not_called_a_scale_error(
        db, edgar, tmp_path, monkeypatch):
    """Founders Fund VII's card says 'either the unit scale is wrong or this filer's
    holdings are only partly ingested'. Its cover states 2,028,956 and the book held
    sums to 2,028,956: the filing rules out both causes. A book that does NOT equal its
    cover keeps the warning."""
    con, path = db
    registry(tmp_path, monkeypatch, [(333, "Tiny"), (222, "Partial"), (444, "Unread")])
    hold(con, 333, _hr(edgar, 333, 1, "2026-06-30", "2026-08-14", value=2028956),
         "2026-06-30", "2026-08-14", value=2028956)
    hold(con, 222, _hr(edgar, 222, 1, "2026-06-30", "2026-08-14", value=9000000),
         "2026-06-30", "2026-08-14", value=2028956)
    unread = _hr(edgar, 444, 1, "2026-06-30", "2026-08-14", value=2028956)
    hold(con, 444, unread, "2026-06-30", "2026-08-14", value=2028956)
    edgar.status[edgar.url(444, unread, "primary_doc.xml")] = 503
    AI.read_roster(con, "c", {"0000000333": "Tiny", "0000000222": "Partial",
                              "0000000444": "Unread"}, now=NOW)
    warn = q._magnitude_warning(2028956, "price_anchored")
    assert "either the unit scale is wrong" in warn
    books = {"filers": [
        {"cik": str(c).zfill(10), "name": n, "period": "2026-06-30",
         "book_value": 2028956, "top3": [], "days_to_filing": 45, "discretionary": True,
         "magnitude_warning": warn} for c, n in ((333, "Tiny"), (222, "Partial"),
                                                 (444, "Unread"))]}
    ro = q.connect_ro(path)
    try:
        html_ = dash._tracked_books_strip(books, q.q_arrivals(ro, anchor=ANCHOR))
    finally:
        ro.close()
    tiny, partial, unread_card = (html_.split("<b>{}</b>".format(n))[1].split("</div>")[0]
                                  for n in ("Tiny", "Partial", "Unread"))
    assert "reported book equals the total on the filer" in tiny
    assert "under the $100M 13F threshold" in tiny
    assert "either the unit scale is wrong" not in tiny
    assert "either the unit scale is wrong" in partial
    # a cover that was not read rules nothing out
    assert "either the unit scale is wrong" in unread_card
    assert "reported book equals" not in unread_card


def test_the_printed_ledger_does_not_print_one_cell_over_another(
        db, edgar, tmp_path, monkeypatch):
    """Seven equal columns printed 'awaited' on top of 'Founders Fund VII Management,
    LLC (Thiel)' and 'nt_notice' on top of Pershing's name: 20 of 28 rows, and all
    three that were not 'filed'. pdfplumber read 'Capital Mnat_nnaogteicmeent'."""
    import pdfplumber
    con, path = db
    pershing = "Pershing Square Capital Management (Ackman)"
    founders = "Founders Fund VII Management, LLC (Thiel)"
    registry(tmp_path, monkeypatch, [(333, pershing), (444, founders)])
    _hr(edgar, 333, 0, "2026-03-31", "2026-05-15")
    edgar.filing(333, acc(333, 26, 1), "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PERSHING SQUARE INC.", "0002026053", "x")]))
    _hr(edgar, 444, 1, "2025-12-31", "2026-02-17")
    AI.read_roster(con, "c", {"0000000333": pershing, "0000000444": founders}, now=NOW)
    ro = q.connect_ro(path)
    try:
        spec = dash._page_brief_spec(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                              _path="/arrivals"), "/arrivals")
    finally:
        ro.close()
    assert abs(sum(dash._ARR_PDF_WIDTHS) - 7.2) < 1e-9
    assert spec["tables"][0][1][1:3] == ["2026-06-30", "2026-09-30"]   # periods, not keys
    # 'amendments not applied' is wider than its 0.6in column; the page notes what the
    # short header means
    assert spec["tables"][0][1][-1] == "not applied"
    assert any(n.startswith("Last column: amendments") for n in spec["notes"])
    out = str(tmp_path / "arrivals.pdf")
    brief.render_page_brief(out, title=spec["title"], subtitle=spec["subtitle"],
                            tables=spec["tables"], notes=spec["notes"])
    with pdfplumber.open(out) as doc:
        text = "\n".join((pg.extract_text() or "") for pg in doc.pages)
    table = text[text.index("filer"):]
    assert pershing in table and founders in table      # each name intact on its line
    assert "NT-notice" in table and "nt_notice" not in table   # in words, as on screen
    assert "13F-NT 2026-06-30" in table


@pytest.mark.parametrize("asked", ["20260810", "2026-W33-1", "2026-8-10", "garbage",
                                   "0001-01-01", "9999-12-31"])
def test_an_anchor_that_is_not_a_plain_date_is_refused_and_said(
        asked, db, edgar, tmp_path, monkeypatch):
    """Python 3.14 parses '20260810', and as a STRING it sorts above every date in
    2026: a replay of 08-10 counted filings from 08-14. A year-1 anchor answered 500
    on a page that never used to care about the anchor."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    res, _ = rows_by_name(path, anchor=asked)
    assert res["anchor_refused"] == asked and res["anchor"] == ANCHOR    # today
    summary = dash._arrivals_summary(res)
    # the sentence names what was asked and what is shown, and the page answers for
    # the day it shows
    import html as html_mod
    assert ("anchor {} is not a date this page can answer for; showing {}".format(
        html_mod.escape(repr(asked)), ANCHOR)) in summary
    assert "<b>13F arrivals</b> as of {} ".format(ANCHOR) in summary
    assert "as of {}".format(html_mod.escape(asked)) not in summary
    assert q.arrival_anchor("2026-08-10") == ("2026-08-10", None)
    assert q.arrival_anchor(None)[1] is None and q.arrival_anchor("")[1] is None


def test_a_ledger_failure_costs_a_page_one_line_not_the_page(
        db, edgar, tmp_path, monkeypatch, capsys):
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    monkeypatch.setattr(q, "q_arrivals", lambda *a, **k: 1 / 0)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR], "filer": ["444"]}), _path="/portfolios")
        page = dash.view_portfolios(ro, p)
        strip = dash._tracked_books_strip(q.q_tracked_books(ro, anchor=ANCHOR),
                                          *dash._arrivals_for_strip(ro, p))
        assert "Reported portfolio" in page and "Quiet Fund" in page
        for html_ in (page, strip):
            assert "13F arrival ledger could not be read: ZeroDivisionError" in html_
        assert "d to next filing" in strip                # the cards fall back
        with pytest.raises(ZeroDivisionError):            # the ledger's own page fails loud
            dash.view_arrivals(ro, p)
    finally:
        ro.close()
    # and the log says so with the traceback: one red line on a 200 page was the only
    # trace a programming error left
    err = capsys.readouterr().err
    assert "[dash] 13F arrival ledger could not be read" in err
    assert "ZeroDivisionError" in err and "Traceback" in err


def test_a_past_anchor_shows_the_book_held_that_day(db, edgar, tmp_path, monkeypatch):
    """A replay of 2026-08-10 printed 'book held 2026-06-30' beside '2026-06-30
    awaited': that day's ledger paired with today's book."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    res, by = rows_by_name(path, anchor="2026-08-10")
    assert by["Filed Fund"]["book_period"] is None        # its book was filed 08-14
    assert by["Notice Fund"]["book_period"] == "2026-03-31"
    # the card still shows today's book, and now says so beside it, and above the cards
    # the page says the ledger is a replay
    ro = q.connect_ro(path)
    try:
        strip = dash._tracked_books_strip(q.q_tracked_books(ro, anchor="2026-08-10"), res)
    finally:
        ro.close()
    filed = strip.split("<b>Filed Fund</b>")[1].split("</div>")[0]
    notice = strip.split("<b>Notice Fund</b>")[1].split("</div>")[0]
    assert "2026-06-30 (book held today)" in filed
    assert "(book held today)" not in notice              # its card IS that day's book
    assert ("the ledger is replayed as of 2026-08-10: the books and the boards on these "
            "pages are today's") in strip
    res, by = rows_by_name(path)
    assert by["Filed Fund"]["book_period"] == "2026-06-30"
    assert not res["replayed"] and "replayed" not in dash._arrivals_summary(res)


def test_a_clicked_sort_breaks_ties_in_the_order_the_page_explains(
        db, edgar, tmp_path, monkeypatch):
    """Sorted on a column, ties fell back to registry order, which is on no column."""
    con, path = db
    filers = [(113, "Zed"), (111, "Alpha"), (112, "Mike")]
    registry(tmp_path, monkeypatch, filers)
    for cik, _n in filers:
        hold(con, cik, _hr(edgar, cik, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
             "2026-08-14")
    AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in filers}, now=NOW)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR], "sort": ["closed_state"],
                               "dir": ["asc"]}), _path="/arrivals")
        _arr, rows = dash._arr_rows(ro, p)
    finally:
        ro.close()
    assert [r["filer"] for r in rows] == ["Alpha", "Mike", "Zed"]


def test_a_notice_filer_keeps_naming_its_reporting_manager(
        db, edgar, tmp_path, monkeypatch):
    """The note that names the manager holding a notice filer's book was written only
    while the notice's quarter was the open or closed one. On 2026-11-17 Pershing's
    last word is still that notice, and the page must still say where the book is."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    AI.read_filer(con, "333", "c", now=evening("2026-11-16"))
    _, by = rows_by_name(path, anchor="2026-11-17")
    r = by["Notice Fund"]
    assert (r["closed_period"], r["closed_state"]) == ("2026-09-30", A.AWAITED)
    assert (r["last_form"], r["last_period"]) == ("13F-NT", "2026-06-30")
    assert ("13F-NT for 2026-06-30 filed 2026-08-14: holdings are reported by "
            "PARENT INC.") in r["notes"]


def test_a_filing_for_the_open_quarter_that_is_not_a_book_wants_a_look_now(
        db, edgar, tmp_path, monkeypatch):
    """Promotion keyed on the closed period only. A third empty report from Thiel Macro
    on 11-09 would sit behind the fold, unnamed, until the deadline had passed."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Empty Again", 222)
    hold(con, 222, _hr(edgar, 222, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    _hr(edgar, 222, 2, "2026-09-30", "2026-10-01", value=0, entries=1)
    AI.read_filer(con, "222", "c", now=NOW)
    res, by = rows_by_name(path)
    r = by["Empty Again"]
    assert (r["closed_state"], r["open_state"]) == (A.FILED, A.FILED_EMPTY)
    assert r["needs_look"] is True
    assert "Empty Again: 2026-06-30 filed; 2026-09-30 filed-empty" in (
        dash._arrivals_summary(res))


def test_a_comparison_across_skipped_quarters_says_what_was_skipped(
        db, edgar, tmp_path, monkeypatch):
    """Thiel Macro's 2026-06-30 book is compared with 2025-09-30. The page says 'QoQ'
    and its badges are measured across two quarters the filer filed empty."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Thiel-like")
    hold(con, 111, _hr(edgar, 111, 1, "2025-09-30", "2025-11-14", yy=25), "2025-09-30",
         "2025-11-14")
    _hr(edgar, 111, 1, "2025-12-31", "2026-02-17", value=0, entries=1)
    _hr(edgar, 111, 2, "2026-03-31", "2026-05-15", value=0, entries=1)
    hold(con, 111, _hr(edgar, 111, 3, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    ro = q.connect_ro(path)
    try:
        res = q.q_portfolio(ro, filer_cik="111")
        assert (res["period"], res["prior_period"]) == ("2026-06-30", "2025-09-30")
        out = portfolio_line(ro, dict(dash._params({"anchor": [ANCHOR], "filer": ["111"]}),
                                      _path="/portfolios"), res)
        assert q.q_arrival_gaps(ro, "111", "2026-03-31", "2026-06-30", ANCHOR) == []
        # On 2026-01-15 the first quarter of 2026 had eleven weeks to run: nothing was
        # awaited for it, and the gaps stop at the quarter open on that day.
        assert q.q_arrival_gaps(ro, "111", "2025-09-30", "2026-06-30", "2026-01-15") == [
            ("2025-12-31", A.AWAITED)]
        # and the page, replayed, compares today's books: it says so and lists no gaps
        replay = portfolio_line(ro, dict(dash._params({"anchor": ["2026-01-15"],
                                                       "filer": ["111"]}),
                                         _path="/portfolios"), res)
    finally:
        ro.close()
    assert ("the QoQ comparison above steps across 2025-12-31 filed-empty; 2026-03-31 "
            "filed-empty") in out
    assert "replayed" not in out
    assert ("the ledger is replayed as of 2026-01-15: the book on this page is "
            "today&#x27;s") in replay
    assert "steps across" not in replay


def pos(con, cik, accession, period, filed, shares, ticker="AAPL"):
    """One long position with a share count, so a pair of books has a direction."""
    con.execute(
        "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, ticker,"
        " issuer, put_call, value, shares, ingested_at_unix, value_scale) "
        "VALUES (?,?,?,?,?,?,?,'long',?,?,0,1)",
        (str(int(cik)), accession, period, filed,
         {"AAPL": "037833100", "MSFT": "594918104"}[ticker], ticker, ticker,
         shares * 100, shares))
    con.commit()


def _board(con, edgar, tmp_path, monkeypatch):
    """Five filers. On the board: one comparing across two empty quarters, one whose
    newest book is a quarter old beside a notice, one at the closed quarter, one already
    at the open quarter. Off it: a stale filer with no opposite side."""
    names = {111: "Thiel-like", 222: "Old Book", 333: "Current", 444: "Early",
             555: "Not on board"}
    registry(tmp_path, monkeypatch, list(names.items()))

    def book(cik, n, period, filed, shares, ticker="AAPL", yy=26):
        a = _hr(edgar, cik, n, period, filed, yy=yy, value=shares * 100, entries=1)
        pos(con, cik, a, period, filed, shares, ticker)

    book(111, 1, "2025-09-30", "2025-11-14", 10, yy=25)
    _hr(edgar, 111, 1, "2025-12-31", "2026-02-17", value=0, entries=1)
    _hr(edgar, 111, 2, "2026-03-31", "2026-05-15", value=0, entries=1)
    book(111, 3, "2026-06-30", "2026-08-14", 20)                      # accumulating
    book(222, 1, "2025-12-31", "2026-02-17", 20)
    book(222, 2, "2026-03-31", "2026-05-15", 10)                      # distributing
    edgar.filing(222, acc(222, 26, 3), "13F-NT", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-NT", "2026-06-30", value=None, report="13F NOTICE",
                                 managers=[("PARENT INC.", "0002026053", "028-25746")]))
    book(333, 1, "2026-03-31", "2026-05-15", 20)
    book(333, 2, "2026-06-30", "2026-08-14", 10)                      # distributing
    book(444, 1, "2026-06-30", "2026-08-14", 10)
    book(444, 2, "2026-09-30", "2026-10-01", 20)                      # accumulating
    book(555, 1, "2025-12-31", "2026-02-17", 10, "MSFT")
    book(555, 2, "2026-03-31", "2026-05-15", 20, "MSFT")              # nobody opposite
    AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in names.items()}, now=NOW)


def test_the_disagreement_board_names_the_entries_that_are_not_a_comparison_into_the_closed_quarter(
        db, edgar, tmp_path, monkeypatch):
    """The board pairs each filer's newest HELD book with its previous held one. The
    first line written for it named Founders Fund VII, which has no entry on the board,
    and left out Thiel Macro, the filer it was written for: it tested each filer's
    newest book against the closed quarter and never looked at the pair compared."""
    con, path = db
    _board(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR]}), _path="/disagreements")
        res = q.q_opposed_pairs(ro)
        page = dash.view_disagreements(ro, p)
        spec = dash._page_brief_spec(ro, p, "/disagreements")
    finally:
        ro.close()
    on_board = {e["filer"] for r in res["rows"] for e in r["accumulating"] + r["distributing"]}
    assert on_board == {"Thiel-like", "Old Book", "Current", "Early"}
    lines = [
        "Early: its book is already 2026-09-30, the open quarter",
        "Old Book: its newest book is 2026-03-31; 2026-06-30 NT-notice",
        "Thiel-like: compares 2026-06-30 with 2025-09-30, stepping across 2025-12-31 "
        "filed-empty; 2026-03-31 filed-empty"]
    assert "Not a quarter-over-quarter comparison into 2026-06-30:" in page
    assert "<br>".join(lines) in page                    # on the page, in name order
    assert "Current:" not in page and "Not on board" not in page
    # the printed board carries the same lines
    assert spec["notes"][1:] == ["Not a quarter-over-quarter comparison into 2026-06-30:"
                                 ] + lines


def test_the_disagreement_board_stands_when_the_ledger_cannot_be_read(
        db, edgar, tmp_path, monkeypatch, capsys):
    con, path = db
    _board(con, edgar, tmp_path, monkeypatch)
    monkeypatch.setattr(q, "q_arrivals", lambda *a, **k: 1 / 0)
    ro = q.connect_ro(path)
    try:
        page = dash.view_disagreements(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                                _path="/disagreements"))
    finally:
        ro.close()
    assert "AAPL" in page
    assert ("the arrival ledger could not be read, so entries that are not a comparison "
            "into 2026-06-30 are not named here") in page
    assert "ZeroDivisionError" in capsys.readouterr().err


def test_a_filer_never_read_exports_no_count_as_zero(db, edgar, tmp_path, monkeypatch):
    """A filer the ledger never read exported 0 filings not in the book: a statement
    about EDGAR that nothing had read. Beside it, a filer that WAS read and is clean."""
    con, path = db
    registry(tmp_path, monkeypatch, [(555, "Unread Fund"), (111, "Clean Fund")])
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR]}), _path="/arrivals")
        text = dash._build_arrivals_csv(ro, p, full=True)
        page = dash.view_arrivals(ro, p)
    finally:
        ro.close()
    import csv as csv_mod
    rows = {r["filer"]: r for r in csv_mod.DictReader(io.StringIO(text))}
    assert rows["Unread Fund"]["closed_state"] == "not_read"
    # the count a clean read exports as 0 is exported blank when nothing was read
    assert (rows["Unread Fund"]["amendments_not_applied"],
            rows["Clean Fund"]["amendments_not_applied"]) == ("", "0")
    unread, clean = cells(page, "Unread Fund"), cells(page, "Clean Fund")
    assert unread[6:10] == ["not read", "not read", "not read", "never"]
    assert clean[6:9] == ["-", "-", "-"] and clean[9] != "never"


def test_the_deploy_script_reads_the_ledger_before_it_restarts_the_dashboard():
    """Gates are scripts. Left to the nightly leg, the first read of every feed (618
    requests, about four minutes) would run inside the 22:30 scan, which already ends
    minutes before the clock-gated 23:15 brief."""
    import pathlib
    text = (pathlib.Path(__file__).resolve().parent.parent / "scripts"
            / "post_deploy.sh").read_text(encoding="utf-8")
    migrate = text.index("db.connect(db.DB_PATH_DEFAULT)")
    read = text.index("-m smart_money.arrivals_ingest")
    restart = text.index("launchctl kickstart")
    assert migrate < read < restart
    # The dashboard restarts whatever the read did; a read that did not happen at all
    # (exit 3: no feed read) ends the script non-zero AFTER the restart, and a partial
    # one (exit 1) is the nightly leg's to retry.
    status = text.index("ledger=$?")
    assert text.index("set +e") < read < status < text.index("set -e", status)
    stop = text.index("THE LEDGER READ DID NOT RUN")
    assert restart < text.index('if [ "$ledger" -eq 1 ]') < stop < text.index(
        "exit 1", stop)


def test_the_list_of_passed_deadlines_starts_at_the_stored_window(
        db, edgar, tmp_path, monkeypatch):
    """Thiel Macro filed nothing for 16 quarters to 2024-09-30. The list names the ones
    since 2023-12-31, the earliest period any book is held for, and says so in its
    column header; it does not reach back to a 2019 filing."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    _hr(edgar, 111, 1, "2019-06-30", "2019-08-14", yy=19)
    hold(con, 111, _hr(edgar, 111, 1, "2024-12-31", "2025-02-14", yy=25), "2024-12-31",
         "2025-02-14")
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2025-03-01")
    assert by["Fund"]["missed_periods"] == "2023-12-31, 2024-03-31, 2024-06-30, 2024-09-30"
    ro = q.connect_ro(path)
    try:
        page = dash.view_arrivals(ro, dict(dash._params({"anchor": ["2025-03-01"]}),
                                           _path="/arrivals"))
    finally:
        ro.close()
    assert "deadlines passed, nothing filed (since 2023-12-31)" in page


def test_the_strip_keeps_in_view_a_card_that_needs_a_look_for_the_open_quarter(
        db, edgar, tmp_path, monkeypatch):
    """The card of a filer that has already filed an empty report for the OPEN quarter
    is small by book and filed for the closed one. Promotion by closed state alone
    leaves it behind the fold until the deadline has passed."""
    con, path = db
    filers = [(100 + i, "Big {}".format(i)) for i in range(8)] + [(222, "Empty Again")]
    registry(tmp_path, monkeypatch, filers)
    for cik, _name in filers:
        size = 10 ** 9 * (20 - cik % 100) if cik < 200 else 1000
        hold(con, cik, _hr(edgar, cik, 1, "2026-06-30", "2026-08-14", value=size),
             "2026-06-30", "2026-08-14", value=size)
    _hr(edgar, 222, 2, "2026-09-30", "2026-10-01", value=0, entries=1)
    AI.read_roster(con, "c", {str(c).zfill(10): n for c, n in filers}, now=NOW)
    ro = q.connect_ro(path)
    try:
        html_ = dash._tracked_books_strip(q.q_tracked_books(ro, anchor=ANCHOR),
                                          q.q_arrivals(ro, anchor=ANCHOR))
    finally:
        ro.close()
    head, _, folded = html_.partition("<details>")
    assert "<b>Empty Again</b>" in head and "<b>Empty Again</b>" not in folded
    assert "<b>Big 7</b>" in folded


# ================================================================ after the second review ==
# A second adversarial review (2026-10-08) confirmed every first-round finding closed
# and found these in the revision: rules a replay or a failed read reaches, and
# sentences built on a narrower fact than they state.

def test_a_morning_read_on_deadline_day_does_not_answer_for_it(
        db, edgar, tmp_path, monkeypatch):
    """post_deploy.sh reads the ledger at whatever hour the deploy happens. Read at
    09:00 on 2026-11-16 with no nightly after it, 2026-11-17 printed every filer
    'awaited, 1d past the deadline', promoted, with no note."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=et("2026-11-16", "09:00"))
    _, by = rows_by_name(path, anchor="2026-11-17")
    r = by["Fund"]
    assert (r["closed_period"], r["closed_state"]) == ("2026-09-30", A.NOT_READ)
    assert r["closed_days_past_deadline"] is None
    assert "no EDGAR read covers the 2026-09-30 deadline day of 2026-11-16" in r["notes"]
    AI.read_filer(con, "111", "c", now=et("2026-11-16", "17:30"))
    _, by = rows_by_name(path, anchor="2026-11-17")
    assert (by["Fund"]["closed_state"], by["Fund"]["closed_days_past_deadline"]) == (
        A.AWAITED, 1)


def test_a_book_held_for_a_quarter_the_ledger_has_not_seen_is_never_awaited(
        db, edgar, tmp_path, monkeypatch):
    """Amazon files on 11-09, that night its ledger read fails and the holdings leg
    lands the book. On 11-10 the card read '2026-09-30 awaited, due 2026-11-16' under
    the 2026-09-30 book, unpromoted, and the failure was in one table cell."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Amazon-like")
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=evening("2026-11-08"))
    hold(con, 111, _hr(edgar, 111, 2, "2026-09-30", "2026-11-09"), "2026-09-30",
         "2026-11-09")
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 503
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=evening("2026-11-09"))
    res, by = rows_by_name(path, anchor="2026-11-10")
    r = by["Amazon-like"]
    assert (r["open_period"], r["open_state"], r["book_period"]) == (
        "2026-09-30", A.FILED_UNREAD, "2026-09-30")
    assert r["needs_look"] is True
    assert ("holdings for 2026-09-30 are held and the ledger has no 13F for it: the "
            "ledger's read is behind the holdings") in r["notes"]
    assert ("last EDGAR read attempt failed (RuntimeError: submissions HTTP 503); states "
            "are as of filings dated through 2026-11-08") in r["notes"]
    summary = dash._arrivals_summary(res)
    assert ("1 of 1 feeds have no read that answers for the day before 2026-11-10 "
            "(oldest answers for 2026-11-08); the last attempt failed for Amazon-like"
            ) in summary
    assert "Amazon-like: 2026-06-30 filed; 2026-09-30 filed (contents not read)" in summary
    assert res["stale_feeds"] == [("Amazon-like", "2026-11-08",
                                   "RuntimeError: submissions HTTP 503")]


def test_a_failed_read_alone_asks_for_a_look(db, edgar, tmp_path, monkeypatch):
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW - DAY)
    _, by = rows_by_name(path)
    assert by["Fund"]["needs_look"] is False
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 503
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert by["Fund"]["closed_state"] == A.FILED and by["Fund"]["needs_look"] is True


def test_a_replay_before_the_first_book_was_filed_names_nothing_as_unheld(
        db, edgar, tmp_path, monkeypatch):
    """Anchoring the held book let the floor fall to the stored window on a replay:
    every 13F since 2023-12-31 was 'filed on EDGAR, no book held here' for the 289
    days before the first held book was filed, and then nothing was."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    _hr(edgar, 111, 1, "2024-03-31", "2024-05-15", yy=24)
    _hr(edgar, 111, 2, "2024-06-30", "2024-08-14", yy=24)
    hold(con, 111, _hr(edgar, 111, 3, "2024-09-30", "2024-11-14", yy=24), "2024-09-30",
         "2024-11-14")
    AI.read_filer(con, "111", "c", now=NOW)
    for anchor in ("2024-11-12", "2024-11-14", ANCHOR):
        _, by = rows_by_name(path, anchor=anchor)
        r = by["Fund"]
        assert (r["filed_not_held"], r["amendments_not_applied"]) == ("", 0), anchor
    _, by = rows_by_name(path, anchor="2024-11-12")
    r = by["Fund"]
    assert r["book_period"] is None and r["needs_look"] is False
    assert (r["closed_state"], r["open_state"]) == (A.FILED, A.AWAITED)


def test_the_cover_check_allows_half_a_unit_per_line(db, edgar):
    """Duquesne rounds each of 95 lines in thousands; a clean book can sit 6 off its
    cover, and a tolerance of 5 called it a partial parse a few percent of quarters."""
    con, _ = db
    a = _hr(edgar, 111, 1, "2026-06-30", "2026-08-14", value=5000)
    AI.read_filer(con, "111", "c", now=NOW)
    entry = A.load(con)[111]

    def check(raw_sum, rows):
        held = {"periods": {"2026-06-30"}, "accessions": {a}, "floor": "2026-06-30",
                "sums": {a: ("2026-06-30", raw_sum, rows)}}
        return A.filer_view("111", "X", entry, held, ANCHOR, {111})["book_check"]

    assert (A.cover_tolerance(None), A.cover_tolerance(4), A.cover_tolerance(95)) == (
        5, 5, 47)
    assert check(5006, 95) == "matches cover"
    assert check(5047, 95) == "matches cover"
    assert check(5048, 95) == "differs from cover"
    assert check(5006, 4) == "differs from cover"
    assert check(4995, 4) == "matches cover"


def test_the_older_pages_note_is_only_for_quarters_before_the_pages_read(
        db, edgar, tmp_path, monkeypatch):
    """Six filers explained a dark-scan 'not read' for 2026-09-30 with 'older pages
    exist and were not read', which has nothing to do with it."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Heavy Filer")
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    edgar.page(111, "CIK0000000111-submissions-001.json",
               [("13F-HR", acc(111, 15, 1), "2015-06-30", "2015-08-14")])
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path, anchor="2026-11-17")
    r = by["Heavy Filer"]
    assert r["closed_state"] == A.NOT_READ
    assert "no EDGAR read covers the 2026-09-30 deadline day of 2026-11-16" in r["notes"]
    assert "older pages exist" not in r["notes"]


def test_a_placeholder_restated_into_a_book_is_a_book_not_held(
        db, edgar, tmp_path, monkeypatch):
    """A value-0 13F-HR restated by a 13F-HR/A that states the book: the quarter read
    'filed' and quiet and the book was counted among the standing amendments, though
    the holdings ingest reads 13F-HR only and will never hold it."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Thiel-like")
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    _hr(edgar, 111, 2, "2026-09-30", "2026-11-16", value=0, entries=1)
    edgar.filing(111, acc(111, 26, 3), "13F-HR/A", "2026-09-30", "2026-11-19",
                 cover_xml=cover("13F-HR/A", "2026-09-30", value=400000000, entries=9,
                                 amendment=("1", "RESTATEMENT")))
    AI.read_filer(con, "111", "c", now=evening("2026-11-20"))
    res, by = rows_by_name(path, anchor="2026-11-21")
    r = by["Thiel-like"]
    assert r["closed_state"] == A.FILED
    assert (r["filed_not_held"], r["amendments_not_applied"]) == ("2026-09-30", 0)
    assert r["needs_look"] is True
    assert ("13F-HR/A (RESTATEMENT) for 2026-09-30 filed 2026-11-19 states holdings for "
            "a period whose 13F-HR states none; no holdings from it are held"
            ) in r["notes"]
    assert res["amendments_not_applied"] == 0
    assert "filed on EDGAR, no book held here: Thiel-like 2026-09-30" in (
        dash._arrivals_summary(res))


def test_two_originals_held_for_one_quarter_are_a_doubled_book(
        db, edgar, tmp_path, monkeypatch):
    """Each original matched its own cover while every card summed the two."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    first = _hr(edgar, 111, 1, "2026-06-30", "2026-08-12")
    second = _hr(edgar, 111, 2, "2026-06-30", "2026-08-14")
    for a, filed in ((first, "2026-08-12"), (second, "2026-08-14")):
        hold(con, 111, a, "2026-06-30", filed)
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["book_check"], r["needs_look"]) == ("differs from cover", True)
    assert ("the book held for 2026-06-30 comes from 2 filings ({}, {}); the period is "
            "summed across them".format(first, second)) in r["notes"]


def test_a_filing_with_an_off_quarter_period_blocks_only_its_own_quarter(
        db, edgar, tmp_path, monkeypatch):
    """One 13F stating 2026-06-29 turned every awaited quarter before its filing date
    into 'filed, contents not read' for good, and a silent filer's passed deadlines
    left the page."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Quiet Fund", 444)
    hold(con, 444, _hr(edgar, 444, 1, "2025-09-30", "2025-11-03", yy=25), "2025-09-30",
         "2025-11-03")
    edgar.filing(444, acc(444, 26, 1), "13F-HR", "2026-06-29", "2026-08-14",
                 cover_xml=cover("13F-HR", "2026-06-29"))
    AI.read_filer(con, "444", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Quiet Fund"]
    assert r["closed_state"] == A.FILED_UNREAD                    # 2026-06-30, its window
    assert r["missed_periods"] == "2025-12-31, 2026-03-31"
    assert ("13F-HR filed 2026-08-14: the feed states no quarter-end period for it "
            "(2026-06-29)") in r["notes"]


def test_the_two_counts_are_never_pooled_when_both_are_present(
        db, edgar, tmp_path, monkeypatch):
    """Each earlier test of the two sentences had one count at zero, so pooling them
    back into the amendment sentence passed."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-03-31", "2026-05-15"), "2026-03-31",
         "2026-05-15")
    edgar.filing(111, acc(111, 26, 2), "13F-HR/A", "2026-03-31", "2026-05-20",
                 cover_xml=cover("13F-HR/A", "2026-03-31", amendment=("1", "RESTATEMENT")))
    _hr(edgar, 111, 3, "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    res, _ = rows_by_name(path)
    assert (res["amendments_not_applied"], res["filed_not_held"]) == (
        1, [("Fund", "2026-06-30")])
    summary = dash._arrivals_summary(res)
    assert "filed on EDGAR, no book held here: Fund 2026-06-30</span>" in summary
    assert "<br>1 amendment(s) on EDGAR are not applied to the books held here" in summary


def test_a_stamp_failure_never_overwrites_the_reason_a_cover_failed(db, edgar):
    con, _ = db
    a = _hr(edgar, 111, 1, "2026-06-30", "2026-08-14")
    edgar.status[edgar.url(111, a, "primary_doc.xml")] = 503
    edgar.docs[(a, a + "-index.htm")] = "<html>Request Rate Threshold Exceeded</html>"
    r = AI.read_filer(con, "111", "c", now=NOW)
    assert [f["what"] for f in r["details_failed"]] == ["cover", "stamp"]
    assert (r["details_read"], r["stamps_read"]) == (0, 0)
    assert con.execute("SELECT read_error FROM thirteenf_arrivals").fetchone()[0] == (
        "cover: RuntimeError: cover HTTP 503")


def test_a_failure_storing_a_read_feed_is_recorded_as_a_failed_attempt(
        db, edgar, monkeypatch):
    """A lock held by another writer: the scan line reported a failed read and the
    ledger row was byte-identical to before."""
    import sqlite3 as sqlite3_mod
    con, _ = db
    _hr(edgar, 111, 1, "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW - DAY)

    def locked(*a, **k):
        raise sqlite3_mod.OperationalError("database is locked")

    monkeypatch.setattr(AI, "record_feed", locked)
    with pytest.raises(sqlite3_mod.OperationalError):
        AI.read_filer(con, "111", "c", now=NOW)
    assert con.execute("SELECT read_unix, attempted_unix, error FROM thirteenf_feed_reads"
                       ).fetchone() == (NOW - DAY, NOW,
                                        "store: OperationalError: database is locked")
    assert not con.in_transaction


def test_a_filing_recorded_before_its_filing_date_degrades_the_leg(db, edgar, monkeypatch):
    """The ledger dates a filing by EDGAR's filing date on the premise that one accepted
    after 17:30 ET is not in the feed until that date. A filing dated today and read
    this afternoon is ordinary; one dated tomorrow and read tonight breaks the premise."""
    from smart_money import thirteenf_ingest
    con, _ = db
    monkeypatch.setattr(thirteenf_ingest, "CONFIRMED", {"0000000111": "A"})
    reg = {"entries": [{"role": "manager_13f", "cik": "0000000111"}]}
    real = AI.read_roster
    when = {}
    monkeypatch.setattr(AI, "read_roster",
                        lambda c, contact, roster: real(c, contact, roster, now=when["t"]))
    _hr(edgar, 111, 1, "2026-09-30", "2026-11-04")
    when["t"] = et("2026-11-04", "14:00")                 # answers for 11-03
    ok = scan.leg_13f_arrivals(con, "c", reg)[0]
    assert ok["status"] == "OK", ok["note"]
    _hr(edgar, 111, 2, "2026-09-30", "2026-11-05")
    when["t"] = et("2026-11-04", "22:30")
    bad = scan.leg_13f_arrivals(con, "c", reg)[0]
    assert bad["status"] == "DEGRADED"
    assert "1 filing(s) recorded before their filing date" in bad["note"]


def test_the_printed_ledger_cuts_no_cell_short(db, edgar, tmp_path, monkeypatch):
    """Wrapped cells were still cut at 44 characters: the longest names lost their
    tail to '...' in a column with room for them."""
    import pdfplumber
    con, path = db
    longest = "Founders Fund Growth Management II, LLC (Thiel group of funds)"
    assert len(longest) > 44
    registry(tmp_path, monkeypatch, [(111, longest)])
    _hr(edgar, 111, 1, "2026-06-30", "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    ro = q.connect_ro(path)
    try:
        spec = dash._page_brief_spec(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                              _path="/arrivals"), "/arrivals")
    finally:
        ro.close()
    out = str(tmp_path / "long.pdf")
    brief.render_page_brief(out, title=spec["title"], subtitle=spec["subtitle"],
                            tables=spec["tables"], notes=spec["notes"])
    with pdfplumber.open(out) as doc:
        text = "\n".join((pg.extract_text() or "") for pg in doc.pages)
    table = text[text.index("filer"):]
    # the name wraps inside its column, so its words are read line by line; cut at 44
    # characters it ended '... LLC (...' and lost the rest
    for word in ("Founders", "Management", "Thiel", "group", "funds)"):
        assert word in table, word
    assert "(..." not in table


def test_a_small_book_page_says_what_its_card_says(db, edgar, tmp_path, monkeypatch):
    """The card said the book equals its cover; the page it links to said, in a red
    banner, that either the unit scale was wrong or the book partly ingested."""
    con, path = db
    registry(tmp_path, monkeypatch, [(333, "Tiny"), (222, "Partial")])
    hold(con, 333, _hr(edgar, 333, 1, "2026-06-30", "2026-08-14", value=2028956),
         "2026-06-30", "2026-08-14", value=2028956)
    hold(con, 222, _hr(edgar, 222, 1, "2026-06-30", "2026-08-14", value=9000000),
         "2026-06-30", "2026-08-14", value=2028956)
    AI.read_roster(con, "c", {"0000000333": "Tiny", "0000000222": "Partial"}, now=NOW)
    # a price-anchored unit, as production's is; there are no prices in this database
    monkeypatch.setattr(q, "_filer_unit_scale", lambda *a, **k: (1, "price_anchored"))
    ro = q.connect_ro(path)
    try:
        out = {}
        for cik in ("333", "222"):
            p = dict(dash._params({"anchor": [ANCHOR], "filer": [cik]}), _path="/portfolios")
            out[cik] = (dash.view_portfolios(ro, p),
                        dash._page_brief_spec(ro, p, "/portfolios")["notes"])
    finally:
        ro.close()
    tiny_page, tiny_notes = out["333"]
    part_page, part_notes = out["222"]
    assert "UNIT SCALE NOT VERIFIED" not in tiny_page
    assert dash._SMALL_BOOK_STATED.replace("'", "") in tiny_page.replace("&#x27;", "")
    assert tiny_notes[0] == dash._SMALL_BOOK_STATED
    assert "UNIT SCALE NOT VERIFIED" in part_page
    assert part_notes[0].startswith("UNIT SCALE NOT VERIFIED - reported book")
    # the printed page carries the ledger line too
    assert any(n.startswith("13F arrivals: 2026-06-30 filed") for n in tiny_notes)


# ================================================================ rev-3 mutation kills ==
# A mutation pass over rev 3 (122 single-point mutants) left 16 guards that no test
# pinned. None was a wrong output; each test below fails on the mutant it names.
def test_the_cover_tolerance_counts_the_rows_of_the_book_held(
        db, edgar, tmp_path, monkeypatch):
    """The half-a-unit-per-line allowance is taken from the rows the holdings table holds
    for the book. Counted as one row, a clean 20-line book 8 off its cover read 'differs
    from cover' and was promoted every night."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    a = _hr(edgar, 111, 1, "2026-06-30", "2026-08-14", value=20008, entries=20)
    for i in range(20):
        con.execute(
            "INSERT INTO thirteenf_holdings(cik, accession, period, filed_date, cusip, "
            "ticker, issuer, put_call, value, shares, ingested_at_unix, value_scale) "
            "VALUES ('111', ?, '2026-06-30', '2026-08-14', ?, NULL, 'X', 'long', 1000, "
            "10, 0, 1)", (a, "{:09d}".format(i)))
    con.commit()
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    assert (by["Fund"]["book_check"], by["Fund"]["needs_look"]) == ("matches cover", False)


def test_an_amendment_beside_a_13f_hr_that_states_a_book_is_an_amendment(
        db, edgar, tmp_path, monkeypatch):
    """Only a 13F-HR/A for a period whose 13F-HR states NO book is the period's book.
    Beside a 13F-HR that states holdings it is an amendment like any other, counted and
    named as one, and never said to restate a period whose 13F-HR states none."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-03-31", "2026-05-15"), "2026-03-31",
         "2026-05-15")
    _hr(edgar, 111, 2, "2026-06-30", "2026-08-12")            # its ingest has not run
    edgar.filing(111, acc(111, 26, 3), "13F-HR/A", "2026-06-30", "2026-08-14",
                 cover_xml=cover("13F-HR/A", "2026-06-30", amendment=("1", "RESTATEMENT")))
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["filed_not_held"], r["amendments_not_applied"]) == ("2026-06-30", 1)
    assert ("13F-HR/A (RESTATEMENT) for 2026-06-30 filed 2026-08-14 is on EDGAR and is "
            "not applied to the book held") in r["notes"]
    assert "whose 13F-HR states none" not in r["notes"]


def test_two_quarters_held_are_a_history_not_a_doubled_book(
        db, edgar, tmp_path, monkeypatch):
    """Two filings held for ONE period are a doubled book. One filing for each of two
    periods is an ordinary history, and the newest book is checked on its own cover."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    for n, (period, filed) in enumerate((("2026-03-31", "2026-05-15"),
                                         ("2026-06-30", "2026-08-14")), 1):
        hold(con, 111, _hr(edgar, 111, n, period, filed), period, filed)
    AI.read_filer(con, "111", "c", now=NOW)
    _, by = rows_by_name(path)
    r = by["Fund"]
    assert (r["book_check"], r["needs_look"]) == ("matches cover", False)
    assert "comes from" not in r["notes"]


def test_a_feed_is_stale_by_its_read_day_or_its_failed_attempt_and_never_otherwise(
        db, edgar, tmp_path, monkeypatch):
    """A nightly read on day D answers for filings dated through D: on D+1 the ledger is
    current and the page must not say otherwise. An attempt after it that failed makes
    the feed stale whatever the date of the last good read."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)                    # answers for 2026-10-01
    res, _ = rows_by_name(path)                                # anchor 2026-10-02
    assert res["stale_feeds"] == []
    assert "feeds have no read" not in dash._arrivals_summary(res)
    edgar.status["https://data.sec.gov/submissions/CIK0000000111.json"] = 503
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW + 3600)
    res, _ = rows_by_name(path)
    assert res["stale_feeds"] == [("Fund", "2026-10-01",
                                   "RuntimeError: submissions HTTP 503")]
    assert "the last attempt failed for Fund" in dash._arrivals_summary(res)


def test_an_empty_ledger_lists_no_stale_feed(tmp_path, monkeypatch):
    """Before the first read the page says the ledger is empty. It does not also list
    every filer as a stale feed, a statement about reads that never happened."""
    registry(tmp_path, monkeypatch, [(111, "Fund")])
    fresh = str(tmp_path / "fresh.db")
    dbmod.connect(fresh).close()
    res, _ = rows_by_name(fresh)
    assert res["ledger"] == "empty" and res["stale_feeds"] == []
    ro = q.connect_ro(fresh)
    try:
        spec = dash._page_brief_spec(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                              _path="/arrivals"), "/arrivals")
    finally:
        ro.close()
    assert spec["notes"][0] == "ARRIVAL LEDGER EMPTY - no state on this page is known"
    assert not any("feeds have no read" in n for n in spec["notes"])


def test_the_printed_ledger_says_its_feeds_are_stale(db, edgar, tmp_path, monkeypatch):
    """The screen says it above the cards; the printed page must say it above the
    table, or a printout of a five-week-old read reads as current."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    hold(con, 111, _hr(edgar, 111, 1, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    ro = q.connect_ro(path)
    try:
        spec = dash._page_brief_spec(ro, dict(dash._params({"anchor": ["2026-11-17"]}),
                                              _path="/arrivals"), "/arrivals")
    finally:
        ro.close()
    assert ("1 of 1 feeds have no read that answers for the day before 2026-11-17: "
            "their states are as of that read") in spec["notes"]


def test_a_replayed_card_names_the_last_13f_when_it_is_not_the_book_on_the_card(
        db, edgar, tmp_path, monkeypatch):
    """On a replay the card shows today's book. The newest 13F as of the anchor is left
    off a card only when it IS the book the card shows: on 2026-08-10 this card shows
    2026-06-30, and the filer's word that day was its 2026-03-31 13F."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch)
    for n, (period, filed) in enumerate((("2026-03-31", "2026-05-15"),
                                         ("2026-06-30", "2026-08-14")), 1):
        hold(con, 111, _hr(edgar, 111, n, period, filed), period, filed)
    AI.read_filer(con, "111", "c", now=NOW)
    ro = q.connect_ro(path)
    try:
        strip = dash._tracked_books_strip(q.q_tracked_books(ro, anchor="2026-08-10"),
                                          q.q_arrivals(ro, anchor="2026-08-10"))
    finally:
        ro.close()
    card = strip.split("<b>Fund</b>")[1].split("</div>")[0]
    assert "2026-06-30 (book held today)" in card
    assert "last 13F: 13F-HR for 2026-03-31" in card


def test_a_cover_match_for_another_period_keeps_the_small_book_warning(
        db, edgar, tmp_path, monkeypatch):
    """Replayed to 2026-08-10, the ledger checked the 2026-03-31 book against its cover;
    the card shows today's 2026-06-30 book, which that check says nothing about."""
    con, path = db
    registry(tmp_path, monkeypatch, [(333, "Tiny")])
    for n, (period, filed) in enumerate((("2026-03-31", "2026-05-15"),
                                         ("2026-06-30", "2026-08-14")), 1):
        hold(con, 333, _hr(edgar, 333, n, period, filed, value=2028956), period, filed,
             value=2028956)
    AI.read_filer(con, "333", "c", now=NOW)
    warn = q._magnitude_warning(2028956, "price_anchored")
    books = {"filers": [{"cik": "0000000333", "name": "Tiny", "period": "2026-06-30",
                         "book_value": 2028956, "top3": [], "days_to_filing": 45,
                         "discretionary": True, "magnitude_warning": warn}]}
    ro = q.connect_ro(path)
    try:
        res = q.q_arrivals(ro, anchor="2026-08-10")
    finally:
        ro.close()
    row = next(r for r in res["rows"] if r["filer"] == "Tiny")
    assert (row["book_period"], row["book_check"]) == ("2026-03-31", "matches cover")
    card = dash._tracked_books_strip(books, res).split("<b>Tiny</b>")[1].split("</div>")[0]
    assert "either the unit scale is wrong" in card
    assert "reported book equals" not in card


def test_the_board_line_answers_for_the_pages_today(db, edgar, tmp_path, monkeypatch):
    """The board is never replayed, so its line is evaluated at today, and today is
    q.today(), the one clock every arrival page asks."""
    con, path = db
    _board(con, edgar, tmp_path, monkeypatch)
    monkeypatch.setattr(q, "today", lambda: "2031-06-01")     # closed = 2031-03-31
    ro = q.connect_ro(path)
    try:
        page = dash.view_disagreements(ro, dict(dash._params({"anchor": [ANCHOR]}),
                                                _path="/disagreements"))
    finally:
        ro.close()
    assert "Not a quarter-over-quarter comparison into 2031-03-31:" in page


def test_the_printed_portfolio_says_when_the_ledger_could_not_be_read(
        db, edgar, tmp_path, monkeypatch):
    """The screen carries a red line; the printed page carried nothing, and read as a
    book with no arrival question about it."""
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    monkeypatch.setattr(q, "q_arrivals", lambda *a, **k: 1 / 0)
    ro = q.connect_ro(path)
    try:
        p = dict(dash._params({"anchor": [ANCHOR], "filer": ["111"]}), _path="/portfolios")
        notes = dash._page_brief_spec(ro, p, "/portfolios")["notes"]
    finally:
        ro.close()
    assert "13F arrival ledger not available for this page" in notes
    assert not any(n.startswith("13F arrivals:") for n in notes)


def test_covers_and_stamps_read_are_counted_apart(db, edgar):
    """'covers_read' counted index pages as covers: one filing read whole reported two
    covers and no stamp, and a night of failed stamps could hide behind it."""
    con, _ = db
    _hr(edgar, 111, 1, "2026-06-30", "2026-08-14")
    _hr(edgar, 222, 1, "2026-06-30", "2026-08-14")
    r = AI.read_filer(con, "111", "c", now=NOW)
    assert (r["details_read"], r["stamps_read"]) == (1, 1)
    rep = AI.read_roster(con, "c", {"0000000222": "B"}, now=NOW)
    assert (rep["details_read"], rep["stamps_read"]) == (1, 1)


def test_a_failure_storing_a_feed_keeps_none_of_its_rows(db, edgar, monkeypatch):
    """The feed's rows and the record of the read are one transaction. Committed with
    the failure record, the rows of a read that was never recorded become history the
    next good read cannot tell from a real backfill."""
    con, _ = db
    _hr(edgar, 111, 1, "2026-06-30", "2026-08-14")

    def broken(unix):
        raise RuntimeError("clock")

    monkeypatch.setattr(AI, "read_day", broken)
    with pytest.raises(RuntimeError):
        AI.read_filer(con, "111", "c", now=NOW)
    assert con.execute("SELECT COUNT(*) FROM thirteenf_arrivals").fetchone()[0] == 0
    assert con.execute("SELECT read_unix, attempted_unix, error FROM thirteenf_feed_reads"
                       ).fetchone() == (None, NOW, "store: RuntimeError: clock")
    assert not con.in_transaction


@pytest.mark.parametrize("ledger_exit, script_exit", [(0, 0), (1, 0), (3, 1)])
def test_the_deploy_script_ends_nonzero_only_when_no_feed_was_read(
        tmp_path, ledger_exit, script_exit):
    """Run with the venv's python and launchctl stubbed. The dashboard restarts whatever
    the read did; a partial read is the nightly leg's to retry; only a read that did not
    happen at all ends the deploy non-zero, and only after the restart."""
    import os
    import pathlib
    import shutil
    import subprocess
    if not shutil.which("bash"):
        pytest.skip("no bash on this host")
    src = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "post_deploy.sh"
    root = tmp_path / "live"
    (root / "scripts").mkdir(parents=True)
    shutil.copy(str(src), str(root / "scripts" / "post_deploy.sh"))
    venv, stubs, log = root / ".venv" / "bin", tmp_path / "bin", tmp_path / "calls.log"
    venv.mkdir(parents=True)
    stubs.mkdir()
    (venv / "python").write_text(
        "#!/bin/bash\n"
        "if [ \"$1\" = \"-\" ]; then cat >/dev/null; exit 0; fi\n"
        "echo \"ingest $*\" >> '{}'\nexit {}\n".format(log, ledger_exit))
    (stubs / "launchctl").write_text(
        "#!/bin/bash\necho \"launchctl $*\" >> '{}'\n".format(log))
    for f in (venv / "python", stubs / "launchctl"):
        f.chmod(0o755)
    r = subprocess.run(["bash", str(root / "scripts" / "post_deploy.sh")],
                       capture_output=True, text=True,
                       env=dict(os.environ, PATH="{}:{}".format(stubs, os.environ["PATH"])))
    calls = log.read_text().splitlines()
    assert calls[0] == "ingest -m smart_money.arrivals_ingest"
    assert calls[1].startswith("launchctl kickstart -k ")
    assert r.returncode == script_exit, r.stderr
    assert ("THE LEDGER READ DID NOT RUN" in r.stderr) == (script_exit == 1)


def test_the_arrivals_table_links_each_filer_to_its_book_on_the_same_day(
        db, edgar, tmp_path, monkeypatch):
    """A replayed ledger's filer link opened /portfolios on today: the reader clicked
    from 2026-08-10 into a page answering a different question."""
    import re
    con, path = db
    _four_filers(con, edgar, tmp_path, monkeypatch)
    ro = q.connect_ro(path)
    try:
        page = dash.view_arrivals(ro, dict(dash._params({"anchor": ["2026-08-10"]}),
                                           _path="/arrivals"))
    finally:
        ro.close()
    assert re.search(r"<a href='/portfolios\?[^']*anchor=2026-08-10[^']*filer=111'>"
                     r"Filed Fund</a>", page)


def test_a_gap_read_failure_is_said_on_the_portfolio_page(
        db, edgar, tmp_path, monkeypatch, capsys):
    """The skipped-quarter line is context, so a failure reading it costs the page that
    line; it must not cost it silently, as if nothing had been skipped."""
    con, path = db
    _fund(con, edgar, tmp_path, monkeypatch, "Thiel-like")
    hold(con, 111, _hr(edgar, 111, 1, "2025-09-30", "2025-11-14", yy=25), "2025-09-30",
         "2025-11-14")
    _hr(edgar, 111, 1, "2025-12-31", "2026-02-17", value=0, entries=1)
    _hr(edgar, 111, 2, "2026-03-31", "2026-05-15", value=0, entries=1)
    hold(con, 111, _hr(edgar, 111, 3, "2026-06-30", "2026-08-14"), "2026-06-30",
         "2026-08-14")
    AI.read_filer(con, "111", "c", now=NOW)
    monkeypatch.setattr(q, "q_arrival_gaps", lambda *a, **k: 1 / 0)
    ro = q.connect_ro(path)
    try:
        res = q.q_portfolio(ro, filer_cik="111")
        out = portfolio_line(ro, dict(dash._params({"anchor": [ANCHOR], "filer": ["111"]}),
                                      _path="/portfolios"), res)
    finally:
        ro.close()
    assert "the quarters between these two books could not be read from the ledger" in out
    err = capsys.readouterr().err
    assert "[dash] 13F arrival gap states could not be read" in err
    assert "ZeroDivisionError" in err
