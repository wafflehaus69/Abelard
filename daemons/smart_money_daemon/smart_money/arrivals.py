"""SM-H1 arrival ledger: what EDGAR says each tracked 13F filer has filed.

Everything the 13F legs knew about arrival was inferred from rows in
thirteenf_holdings, and rows can only say "a book with holdings landed". A quarter
with no rows read the same for four different facts: the filer has not filed yet, it
filed a 13F-HR whose table is a placeholder, it filed a notice that another manager
reports its holdings, or our own fetch failed. This module records the filings
themselves, from an UNFILTERED read of each filer's submissions feed, and derives the
state of a (filer, period) from what those filings state.

Three things are measured facts about the feed and shape the code:

  * A 13F-NT is not a notice of no holdings. The form reads "no holdings reported are
    in this report, and all holdings are reported by other reporting manager(s)", and
    the cover names that manager. All 32 notices on this roster do.
  * An empty book is identified by the cover's tableValueTotal being 0, never by
    tableEntryTotal: the entry total is 0 on two real books and 1 on fifteen real
    one-position books, while value 0 marks exactly the seven placeholder filings.
  * The feed's acceptanceDateTime is labelled Z and is not reliably UTC (Horizon
    Kinetics is shifted twice, Scion and Founders Fund VII are raw Eastern). It is kept
    verbatim, and the acceptance stamp used for anything is the filing's own index page,
    which states Eastern time.

The ledger records and derives. It does not ingest holdings and it applies no
amendment to any book: an amendment on EDGAR that the book does not reflect is
REPORTED here, never silently applied.

This module judges and reads, and carries no write path and no network, so the
read-only query layer may import it. The EDGAR read and every INSERT live in
smart_money.arrivals_ingest.
"""
import datetime as dt
import json
import re
import sqlite3
import xml.etree.ElementTree as ET

from . import dates

FAMILY_PREFIX = "13F"
PAPER = "13FCONP"               # paper confidential-treatment filings, 1999-2012
NOTICE_REPORT = "13F NOTICE"
COMBINATION_REPORT = "13F COMBINATION REPORT"

# The four ratified arrival states.
AWAITED = "awaited"
FILED = "filed"
FILED_EMPTY = "filed_empty"
NT_NOTICE = "nt_notice"
# Three statements that are not arrival states. Two are about OUR knowledge and exist
# so that a gap in what we read is never printed as a fact about what was filed. The
# third keeps 'awaited' honest: a quarter before a filer's first 13F was never owed.
NOT_READ = "not_read"           # no read of this feed that can answer for the period
FILED_UNREAD = "filed_unread"   # a 13F is on EDGAR and what it states has not been read
NOT_DUE = "not_due"             # the period predates the filer's first 13F in the feed

LABEL = {AWAITED: "awaited", FILED: "filed", FILED_EMPTY: "filed-empty",
         NT_NOTICE: "NT-notice", NOT_READ: "not read",
         FILED_UNREAD: "filed (contents not read)", NOT_DUE: "before first 13F"}
# Closed-period states that need nobody's attention.
QUIET = (FILED, NOT_DUE)
# Open-period states worth a look before the deadline: the filer has ALREADY filed
# something for the quarter that is not a book this system can hold.
OPEN_ATTENTION = (FILED_EMPTY, NT_NOTICE, FILED_UNREAD)

# Covers and acceptance stamps are fetched for filings reporting this period or
# later. It is the earliest period thirteenf_holdings carries. Older filings in the
# pages read are recorded with their covers unread.
COVER_FLOOR = "2023-12-31"

# A held book is compared with its own cover's control total, in the same raw units. A
# filer rounds each line and the total separately, so the two differ by at most half a
# unit per line: on the 225 held filings 215 agree exactly and the widest gap is 4, on
# Duquesne's 65- and 95-line books in thousands. The allowance is that bound, never
# less than 5. Anything wider is a partial parse or a wrong filing.
COVER_TOLERANCE = 5


def cover_tolerance(rows):
    return max(COVER_TOLERANCE, (rows or 0) // 2)

# Every column a submissions block must carry. A block missing one is a feed whose
# shape changed, and reading it as "no 13F filings" would print a clean filer.
FEED_COLUMNS = ("form", "accessionNumber", "filingDate", "reportDate",
                "acceptanceDateTime", "primaryDocument")

ACCEPTED_RE = re.compile(
    r'<div class="infoHead">Accepted</div>\s*<div class="info">([^<]*)</div>')
STAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


# ------------------------------------------------------------------ the calendar --
def _nth_weekday(year, month, weekday, n):
    d = dt.date(year, month, 1)
    d += dt.timedelta(days=(weekday - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def _last_weekday(year, month, weekday):
    d = dt.date(year + (month == 12), month % 12 + 1, 1) - dt.timedelta(days=1)
    return d - dt.timedelta(days=(d.weekday() - weekday) % 7)


def federal_holidays(year):
    """Observed dates of the statutory federal holidays (5 U.S.C. 6103) touching `year`.

    This is the calendar that rolls an SEC deadline, and it is not the market's: Good
    Friday is a filing day, Columbus Day and Veterans Day are not. Executive-order
    closures are not statutory and cannot be computed; 2025-12-24 and 2025-12-26 were
    two, and none has ever fallen on a 13F deadline."""
    days = [dt.date(year, 1, 1), _nth_weekday(year, 1, 0, 3), _nth_weekday(year, 2, 0, 3),
            _last_weekday(year, 5, 0), dt.date(year, 7, 4), _nth_weekday(year, 9, 0, 1),
            _nth_weekday(year, 10, 0, 2), dt.date(year, 11, 11),
            _nth_weekday(year, 11, 3, 4), dt.date(year, 12, 25),
            dt.date(year + 1, 1, 1)]
    if year >= 2021:
        days.append(dt.date(year, 6, 19))
    out = set()
    for d in days:
        if d.weekday() == 5:
            d -= dt.timedelta(days=1)
        elif d.weekday() == 6:
            d += dt.timedelta(days=1)
        out.add(d)
    return out


def is_filing_day(d):
    return d.weekday() < 5 and d not in federal_holidays(d.year)


def deadline(period):
    """The 13F due date for a quarter-end: 45 days after it, rolled to the next filing
    day when that lands on a weekend or federal holiday.

    The unrolled date is wrong for 438 of 1,669 originals on this roster. For
    2026-09-30 it is a Saturday and the deadline is Monday 2026-11-16; for 2026-12-31
    it is a Sunday before Washington's Birthday and the deadline is Tuesday 2027-02-16."""
    d = dt.date.fromisoformat(period) + dt.timedelta(days=45)
    while not is_filing_day(d):
        d += dt.timedelta(days=1)
    return d.isoformat()


def quarter_end_before(day):
    """The latest calendar quarter-end strictly before `day` (a date)."""
    for year in (day.year, day.year - 1):
        for month, dom in ((12, 31), (9, 30), (6, 30), (3, 31)):
            qe = dt.date(year, month, dom)
            if qe < day:
                return qe
    raise ValueError("no quarter end before {}".format(day))


def prev_quarter_end(period):
    return quarter_end_before(dt.date.fromisoformat(period)).isoformat()


def next_quarter_end(period):
    d = dt.date.fromisoformat(period)
    for year in (d.year, d.year + 1):
        for month, dom in ((3, 31), (6, 30), (9, 30), (12, 31)):
            qe = dt.date(year, month, dom)
            if qe > d:
                return qe.isoformat()
    raise ValueError("no quarter end after {}".format(period))


def windows(anchor):
    """(closed_period, open_period) on `anchor`.

    closed = the newest quarter whose deadline has passed: every filer owes a filing
    for it, so "awaited" there means overdue. open = the quarter whose filing window is
    open on `anchor` (quarter end passed, deadline not), or None between a deadline and
    the next quarter end. The deadline day itself is still open."""
    day = dt.date.fromisoformat(anchor)
    q = quarter_end_before(day)
    if day <= dt.date.fromisoformat(deadline(q.isoformat())):
        return prev_quarter_end(q.isoformat()), q.isoformat()
    return q.isoformat(), None


# ------------------------------------------------------------------ what a filing states --
def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _find(node, name):
    if node is None:
        return None
    for e in node.iter():
        if _local(e.tag) == name:
            return e
    return None


def _text(node, name):
    e = _find(node, name)
    return (e.text or "").strip() if e is not None else ""


def _int(text):
    text = (text or "").replace(",", "").strip()
    return int(text) if text.isdigit() else None


def _period_iso(mdy):
    """The cover writes MM-DD-YYYY."""
    m = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", mdy or "")
    return "{}-{}-{}".format(m.group(3), m.group(1), m.group(2)) if m else None


def parse_cover(raw_xml):
    """What a 13F primary_doc.xml states. Raises on XML that does not parse.

    isAmendment is ABSENT on 517 of 1,183 covers, as late as 2026-08-14, and absent
    reads as not an amendment. amendmentNo is text ('01' occurs). other_managers is the
    COVER list -- the managers who report holdings FOR this filer -- and never the
    summary page's list of managers included IN this report; the two are opposite
    statements and share an element name."""
    root = ET.fromstring(raw_xml)
    cover = _find(root, "coverPage")
    summary = _find(root, "summaryPage")
    managers = []
    info = _find(cover, "otherManagersInfo")
    for e in (info.iter() if info is not None else ()):
        if _local(e.tag) != "otherManager":
            continue
        managers.append({"name": _text(e, "name") or None,
                         "cik": _text(e, "cik") or None,
                         "file_number": _text(e, "form13FFileNumber") or None})
    omitted = _text(summary, "isConfidentialOmitted").lower()
    return {
        "submission_type": _text(root, "submissionType") or None,
        "period": _period_iso(_text(root, "periodOfReport")),
        "report_type": _text(cover, "reportType") or None,
        "is_amendment": 1 if _text(cover, "isAmendment").lower() == "true" else 0,
        "amendment_no": _text(cover, "amendmentNo") or None,
        "amendment_type": _text(cover, "amendmentType") or None,
        "entry_total": _int(_text(summary, "tableEntryTotal")),
        "value_total": _int(_text(summary, "tableValueTotal")),
        "confidential_omitted": {"true": 1, "false": 0}.get(omitted),
        "other_managers": managers,
    }


def parse_accepted(index_html):
    """The filing index page's own 'Accepted' stamp, Eastern time, or None."""
    m = ACCEPTED_RE.search(index_html or "")
    stamp = (m.group(1).strip() if m else "")
    return stamp if STAMP_RE.fullmatch(stamp) else None


def family_rows(block):
    """Every 13F-family filing in one submissions column block, as dicts.

    Nothing is filtered by form and nothing is guessed: a blank reportDate stays None
    instead of borrowing the filing date, which is what a filtered reader once did for
    a whole feed. A block that lacks one of its columns is refused: the alternative is
    to read a feed whose shape changed as a filer with no 13F at all."""
    n = len(block.get("accessionNumber") or [])
    for name in FEED_COLUMNS:
        col = block.get(name)
        if col is None or len(col) != n:
            raise ValueError("submissions block has no usable {!r} column".format(name))
    out = []
    for form, acc, filed, period, accepted, doc in zip(*(block[c] for c in FEED_COLUMNS)):
        if not (form or "").startswith(FAMILY_PREFIX):
            continue
        out.append({"form": form, "accession": acc, "filed_date": filed or None,
                    "period": period or None, "feed_accepted": accepted or None,
                    "primary_document": doc or None})
    return out


# ------------------------------------------------------------------ reading the ledger --
_COLS = ("cik", "accession", "form", "period", "filed_date", "feed_accepted",
         "accepted_et", "report_type", "is_amendment", "amendment_no", "amendment_type",
         "entry_total", "value_total", "confidential_omitted", "other_managers",
         "cover_status", "read_error", "first_seen_unix", "recorded_unix")
_READ_COLS = ("cik", "read_unix", "read_day", "newest_filed", "older_pages", "deep",
              "attempted_unix", "error")


def load(con):
    """{int(cik): {"read": {...} or None, "filings": [...]}} from the ledger, or None
    when the ledger tables do not exist on this database.

    None matters: the dashboard and the brief open the database read-only and cannot
    create a table, so before a migration has run they must be able to say the ledger
    is absent instead of failing."""
    try:
        rows = con.execute("SELECT {} FROM thirteenf_arrivals".format(
            ", ".join(_COLS))).fetchall()
        reads = con.execute("SELECT {} FROM thirteenf_feed_reads".format(
            ", ".join(_READ_COLS))).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None
        raise
    out = {}
    for r in reads:
        read = dict(zip(_READ_COLS, r))
        out[int(read.pop("cik"))] = {"read": read, "filings": []}
    for r in rows:
        f = dict(zip(_COLS, r))
        f["other_managers"] = json.loads(f["other_managers"] or "[]")
        out.setdefault(int(f["cik"]), {"read": None, "filings": []})["filings"].append(f)
    for entry in out.values():
        entry["filings"].sort(key=_order, reverse=True)
    return out


def _order(f):
    """Filing order within ONE filer. The feed stamp is not comparable across filers
    but is self-consistent inside a feed, and the filing date alone ties on the 16
    amended periods whose two versions share a date."""
    return (f["filed_date"] or "", f["feed_accepted"] or "", f["accession"])


def filing_kind(f):
    """What one filing is, from what it states: paper / notice / unread / unstated /
    empty / book. 'unstated' is a cover that was read and gives no table value: it is
    neither called empty nor called a book."""
    if f["form"] == PAPER:
        return "paper"
    if f["form"].startswith("13F-NT") or f["report_type"] == NOTICE_REPORT:
        return "notice"
    if f["cover_status"] != "read":
        return "unread"
    if f["value_total"] is None:
        return "unstated"
    return "empty" if f["value_total"] == 0 else "book"


def period_state(filings):
    """The arrival state of one (filer, period) from its filings.

    Any filing that states holdings makes the period filed, whatever else was filed for
    it: a placeholder later restated into a book is a filed book. Short of that, a
    filing whose contents have NOT been read outranks everything read, because it may
    be the book: 'filed-empty' and 'NT-notice' are only said when nothing for the
    period is left unread. Then a notice outranks an empty report, since a notice says
    where the holdings ARE."""
    kinds = {filing_kind(f) for f in filings}
    if "book" in kinds:
        return FILED
    if kinds & {"unread", "unstated"}:
        return FILED_UNREAD
    if "notice" in kinds:
        return NT_NOTICE
    if "empty" in kinds:
        return FILED_EMPTY
    return AWAITED


def iso_utc(unix):
    if not unix:
        return None
    return dt.datetime.fromtimestamp(unix, dt.timezone.utc).strftime("%Y-%m-%d %H:%MZ")


class Judge:
    """One filer's ledger as of `anchor`: which filings count, and the state of any
    period.

    Only filings dated on or before the anchor count, so a past anchor replays what
    EDGAR held that day. The date is EDGAR's filing date on purpose: a filing accepted
    after 17:30 ET is dated the next business day and is not in the public feed until
    then (Alphabet, accepted 2026-08-06 17:36 ET, was absent from that night's read
    and present the next)."""

    def __init__(self, entry, held, anchor):
        entry = entry or {}
        self.anchor = anchor
        self.held = held
        self.read = entry.get("read") or {}
        self.has_read = bool(self.read.get("read_unix"))
        self.read_day = self.read.get("read_day") or ""
        every = [f for f in entry.get("filings", []) if filing_kind(f) != "paper"]
        self.filings = [f for f in every if (f["filed_date"] or "") <= anchor]
        self.by_period = {}
        self.unplaced = []          # 13F filings whose period the feed does not state
        for f in self.filings:
            if dates.is_quarter_end(f["period"]):
                self.by_period.setdefault(f["period"], []).append(f)
            else:
                self.unplaced.append(f)
        # The filer's first 13F period, over the WHOLE ledger and not only what was
        # filed by the anchor: Affinity's first 13F, for 2024-12-31, arrived 21 days
        # late, and on each of those days the quarter was owed, not 'before first'.
        self.first = min((f["period"] for f in every
                          if dates.is_quarter_end(f["period"])), default=None)
        # filings.recent holds about the newest 1,000 filings of any form. Where older
        # pages exist and were not read, 'nothing older in the ledger' is a fact about
        # the read, not about the filer.
        self.reach_complete = (not self.read.get("older_pages")
                               or bool(self.read.get("deep")))

    def state(self, period):
        if not self.has_read:
            return NOT_READ
        s = period_state(self.by_period.get(period, []))
        if s != AWAITED:
            return s
        # A 13F whose period the feed does not state belongs, as far as anyone can
        # say, to the quarter that had just ended when it was filed. It blocks
        # 'awaited' for that quarter and no other: blocking every earlier one would
        # erase a silent filer's passed deadlines for good.
        if any(u["filed_date"] and quarter_end_before(
                dt.date.fromisoformat(u["filed_date"])).isoformat() == period
               for u in self.unplaced):
            return FILED_UNREAD
        # Holdings for the period are held and the ledger has no filing for it: the
        # ledger's read is behind the holdings ingest. 'Awaited' beside its own book
        # is the one thing this must never print.
        if period in self.held["periods"]:
            return FILED_UNREAD
        if self.first is None or period < self.first:
            if not self.reach_complete or (self.first is None and self.held["periods"]):
                return NOT_READ
            return NOT_DUE
        due = deadline(period)
        if self.anchor > due and self.read_day < due:
            # The deadline has passed on the anchor and no read answers for deadline
            # day (one made after EDGAR's 17:30 ET cut-off that day, or later). 71% of
            # filings arrive on deadline day; calling this overdue would print a dark
            # scan, or a morning read, as 28 late filers.
            return NOT_READ
        return AWAITED

    def last(self):
        """The newest PERIOD any 13F-family filing reports, and the newest filing for
        it -- not the newest filing by date, which for six Berkshire quarters is an
        amendment to the PRIOR period accepted minutes after the new original."""
        placed = [f for fs in self.by_period.values() for f in fs]
        return max(placed, key=lambda f: (f["period"], _order(f))) if placed else None


def gap_states(entry, held, anchor, after, before):
    """[(period, state)] for every quarter strictly between two periods. What a
    quarter-over-quarter comparison is stepping across when its two books are not
    adjacent quarters."""
    judge = Judge(entry, held, anchor)
    closed, open_ = windows(anchor)
    newest = open_ or closed        # no later quarter had ended on the anchor
    out = []
    p = next_quarter_end(after)
    while p < before and p <= newest:
        out.append((p, judge.state(p)))
        p = next_quarter_end(p)
    return out


def filer_view(cik, name, entry, held, anchor, tracked):
    """One filer's arrival row on `anchor`.

    entry   = this filer's slice of load(), or None when the ledger has nothing on it.
    held    = what thirteenf_holdings carries for the filer as of the anchor:
              {"periods": {...}, "accessions": {...}, "sums": {accession: (period,
              raw value sum, rows)}, "floor": the earliest period EVER held}.
    tracked = the integer CIKs of every tracked filer, to say whether a notice points
              at a book this system holds.
    """
    j = Judge(entry, held, anchor)
    closed, open_ = windows(anchor)
    last = j.last()
    day = dt.date.fromisoformat(anchor)
    closed_state = j.state(closed)
    open_state = j.state(open_) if open_ else None
    row = {
        "cik": str(int(cik)), "filer": name,
        "closed_period": closed, "closed_state": closed_state,
        "closed_days_past_deadline": (
            (day - dt.date.fromisoformat(deadline(closed))).days
            if closed_state == AWAITED else None),
        "open_period": open_, "open_state": open_state,
        "open_due": deadline(open_) if open_ else None,
        "last_form": last["form"] if last else None,
        "last_period": last["period"] if last else None,
        "last_filed": last["filed_date"] if last else None,
        "last_accepted_et": last["accepted_et"] if last else None,
        "book_period": max(held["periods"]) if held["periods"] else None,
        "feed_read": iso_utc(j.read.get("read_unix")),
    }
    notes = []

    def note(text):
        if text not in notes:
            notes.append(text)

    if not j.has_read:
        note("EDGAR feed read failed: {}".format(j.read["error"]) if j.read.get("error")
             else "EDGAR feed never read for this filer")
        row.update(missed_periods=None, amendments_not_applied=None, filed_not_held=None,
                   book_check=None, needs_look=True, notes=" | ".join(notes))
        return row
    if j.read.get("error"):
        note("last EDGAR read attempt failed ({}); states are as of filings dated "
             "through {}".format(j.read["error"], j.read_day))
    elif j.read_day and j.read_day < (day - dt.timedelta(days=1)).isoformat():
        # A nightly read on day D answers for filings dated through D, so on D+1 the
        # ledger is current. Anything older is stale and says so on the row.
        note("the last EDGAR read answers for filings dated through {}; states are as "
             "of that read".format(j.read_day))
    for period, state in ((closed, closed_state), (open_, open_state)):
        if state == NOT_READ and j.first and period >= j.first:
            note("no EDGAR read covers the {} deadline day of {}: whether a 13F was "
                 "filed for it is not known".format(period, deadline(period)))
    behind = [p for p, st in ((closed, closed_state), (open_, open_state))
              if st == NOT_READ and (j.first is None or p < j.first)]
    if behind and not j.reach_complete:
        note("the feed pages read start at {}; older pages exist and were not "
             "read".format(j.first or "nothing"))
    if not j.filings:
        note("no 13F-family filing {} for this filer as of {}".format(
            "on EDGAR" if j.reach_complete else "in the feed pages read", anchor))
    for period, state in ((closed, closed_state), (open_, open_state)):
        if (state == FILED_UNREAD and period in held["periods"]
                and not j.by_period.get(period)):
            note("holdings for {} are held and the ledger has no 13F for it: the "
                 "ledger's read is behind the holdings".format(period))
    if j.first is None and held["periods"]:
        note("the feed read lists no 13F filing although a book is held: the read is "
             "not trusted")
    for f in j.unplaced:
        note("{} filed {}: the feed states no quarter-end period for it ({})".format(
            f["form"], f["filed_date"], f["period"] or "none"))

    # Quarters whose deadline has passed with nothing filed, from the later of the
    # filer's first 13F and the stored window. Not bounded by the newest filing: a
    # filer that resumes must not have its skipped quarters vanish from every surface.
    missed = []
    if j.first:
        p = max(j.first, COVER_FLOOR)
        while p <= closed:
            if j.state(p) == AWAITED:
                missed.append(p)
            p = next_quarter_end(p)
    row["missed_periods"] = ", ".join(missed)

    for period in [p for p in (open_, closed) if p]:
        due = deadline(period)
        for f in j.by_period.get(period, []):
            kind = filing_kind(f)
            if kind == "notice":
                note(_notice_note(f, tracked))
            if kind == "unstated":
                note("the cover of {} for {} was read and states no table value".format(
                    f["form"], period))
            if f["report_type"] == COMBINATION_REPORT and f["other_managers"]:
                # Said once per period: an original and its amendment both state it.
                note("the {} report is a combination report: part of the book is "
                     "reported by {}".format(period, _names(f["other_managers"], tracked)))
            if f["confidential_omitted"]:
                note("{} for {} states that holdings were omitted under a confidential "
                     "treatment request".format(f["form"], period))
            if f["form"] == "13F-HR" and (f["filed_date"] or "") > due:
                # EDGAR dates a filing accepted after 17:30 ET to the next business
                # day, so the acceptance stamp is shown beside the official date: late
                # by the date and submitted on deadline evening are both true.
                late = (dt.date.fromisoformat(f["filed_date"])
                        - dt.date.fromisoformat(due)).days
                note("13F-HR for {} is dated {} day(s) after the {} deadline{}".format(
                    period, late, due,
                    " (accepted {} ET)".format(f["accepted_et"]) if f["accepted_et"]
                    else ""))
    # A filer whose last word is a notice keeps saying where its holdings are after
    # that quarter stops being the closed one.
    if last and filing_kind(last) == "notice":
        note(_notice_note(last, tracked))

    # Filings on EDGAR that the book this system holds does not reflect. Two different
    # things, kept apart: an AMENDMENT is recorded and never applied here, a standing
    # condition until the amendment rule is ruled on; a 13F-HR with no holdings held
    # is an ingest that has not happened or has failed, and wants a look tonight.
    not_applied, unheld = 0, []
    # The floor is the earliest period this system has EVER held for the filer, not
    # the earliest held by the anchor: on a replay before the first book was filed,
    # an anchored floor fell to the stored window and called every 13F since 2023
    # 'filed, no book held'.
    floor = held.get("floor") or (min(held["periods"]) if held["periods"]
                                  else COVER_FLOOR)
    for f in sorted((f for fs in j.by_period.values() for f in fs), key=_order):
        if f["accession"] in held["accessions"] or f["period"] < floor:
            continue
        kind = filing_kind(f)
        unread = ("" if f["cover_status"] == "read" else " (cover not read{})".format(
            ": " + f["read_error"] if f.get("read_error") else ""))
        # A placeholder 13F-HR later restated into a book: the amendment IS the
        # period's book, the holdings ingest reads 13F-HR only and will never hold it,
        # and it is not a standing amendment to anything.
        if (f["form"] == "13F-HR/A" and kind == "book"
                and f["period"] not in held["periods"]
                and not any(g["form"] == "13F-HR" and filing_kind(g) == "book"
                            for g in j.by_period[f["period"]])):
            if f["period"] not in unheld:
                unheld.append(f["period"])
            note("13F-HR/A{} for {} filed {} states holdings for a period whose 13F-HR "
                 "states none; no holdings from it are held".format(
                     _stated(f), f["period"], f["filed_date"]))
            continue
        if f["form"] == "13F-HR/A" and kind == "empty":
            if f["period"] in held["periods"]:
                not_applied += 1
                note("13F-HR/A{} for {} filed {} states a table value of 0 and is not "
                     "applied to the book held".format(
                         _stated(f), f["period"], f["filed_date"]))
        elif f["form"] == "13F-HR/A" and kind in ("book", "unread", "unstated"):
            not_applied += 1
            note("13F-HR/A{} for {} filed {} is on EDGAR and is not applied to the book "
                 "held{}{}".format(_stated(f), f["period"], f["filed_date"],
                                   _delta(f, j.by_period), unread))
        elif f["form"] == "13F-HR" and kind in ("book", "unread", "unstated"):
            unheld.append(f["period"])
            note("13F-HR for {} filed {} is on EDGAR and no holdings from it are "
                 "held{}".format(f["period"], f["filed_date"], unread))
    row["amendments_not_applied"] = not_applied
    row["filed_not_held"] = ", ".join(unheld)

    # The held book against its own cover's control total: the one independent check
    # that the holdings parse was complete.
    check = ""
    by_accession = {f["accession"]: f for f in j.filings}
    book = [(a, v) for a, v in sorted(held["sums"].items())
            if v[0] == row["book_period"]]
    if len(book) > 1:
        # Every card and board sums the period, so two filings held for it are a
        # doubled book whatever each one's own cover says.
        check = "differs from cover"
        note("the book held for {} comes from {} filings ({}); the period is summed "
             "across them".format(row["book_period"], len(book),
                                  ", ".join(a for a, _v in book)))
    for accession, (period, raw_sum, n_rows) in book[:1] if len(book) == 1 else ():
        f = by_accession.get(accession)
        if not f or filing_kind(f) not in ("book", "empty"):
            continue
        if abs((raw_sum or 0) - f["value_total"]) <= cover_tolerance(n_rows):
            check = "matches cover"
        else:
            check = "differs from cover"
            note("the book held for {} sums to {:,} in the filing's own units and its "
                 "cover states {:,}".format(period, raw_sum or 0, f["value_total"]))
    row["book_check"] = check
    # A read that failed is a reason to look in itself: the states on the row are as
    # of an older read, and the row said so only in a note nobody is shown.
    row["needs_look"] = bool(closed_state not in QUIET or open_state in OPEN_ATTENTION
                             or unheld or check == "differs from cover"
                             or j.read.get("error"))
    row["notes"] = " | ".join(notes)
    return row


def _stated(f):
    return " ({})".format(f["amendment_type"]) if f["amendment_type"] else ""


def _names(managers, tracked):
    out = []
    for m in managers:
        cik = m.get("cik")
        if cik and cik.strip().isdigit():
            where = ("tracked" if int(cik) in tracked else "not a tracked filer")
            out.append("{} (CIK {}, {})".format(m.get("name") or "?", int(cik), where))
        else:
            out.append("{} ({})".format(
                m.get("name") or "?", m.get("file_number") or "no CIK stated"))
    return "; ".join(out)


def _notice_note(f, tracked):
    if f["cover_status"] != "read":
        return ("{} for {} filed {}: a notice that another manager reports the "
                "holdings; cover not read, so that manager is not named here".format(
                    f["form"], f["period"], f["filed_date"]))
    return ("{} for {} filed {}: holdings are reported by {}".format(
        f["form"], f["period"], f["filed_date"],
        _names(f["other_managers"], tracked) or "a manager the cover does not name"))


def _delta(amendment, by_period):
    """How far the amendment's stated total sits from its original's, when both covers
    state one. Stated totals only: nothing here reads a holdings row."""
    if amendment["value_total"] is None:
        return ""
    for f in by_period.get(amendment["period"], []):
        if f["form"] == "13F-HR" and f["value_total"] is not None:
            return "; its cover states {:,} against the original's {:,}".format(
                amendment["value_total"], f["value_total"])
    return ""


def read_is_stale(read, anchor):
    """True when a feed's last good read cannot answer for the day before `anchor`, or
    its last attempt failed. A nightly read on day D answers for filings dated through
    D, so on D+1 the ledger is current."""
    if not read or not read.get("read_unix") or read.get("error"):
        return True
    before = (dt.date.fromisoformat(anchor) - dt.timedelta(days=1)).isoformat()
    return (read.get("read_day") or "") < before


def coverage(rows, key):
    """{state: count} over the arrival rows for 'closed' or 'open'."""
    out = {}
    for r in rows:
        s = r[key + "_state"]
        if s is not None:
            out[s] = out.get(s, 0) + 1
    return out
