"""What a date is, decided once.

Before this module the package had three read-side copies of "the first ten
characters, if they are a date" with two different semantics (one truncated without
validating and would have accepted '0025-07-25'), two write-side M/D/YYYY normalisers,
and writers that persisted whatever string arrived. The nightly brief's footer printed
a Form 4 corpus beginning in the year 25 and a congressional one running to 3031.

Two kinds of column, and the ceiling differs between them. A BACKWARD date records
something that has already happened -- a trade, a filing, a disclosure -- and cannot be
after tomorrow. A FORWARD date is a promise about the future -- an option's expiry, an
exercise date -- and legitimately runs years out. A single today+1 ceiling applied to
both would have quarantined 165,854 legal expiries to catch 14 bad trade dates.

A ceiling of today+1 also weakens with time: a trade typed as 2026-12-17 on a filing of
2025-12-19 passes every check run after the 18th of December. So a row that carries its
own filing date is judged against THAT as well -- a report cannot precede what it
reports. That rule needs no threshold and does not decay.

A missing date is NOT a bad one. "No data" and "impossible data" are different states,
so date_flag() returns None for an absent value and a reason only for a present value
that cannot be true.

TWO CLOCKS. Every disclosure carries a trade date and a filing date, and they fail
independently. date_flag quarantines a row on the TRADE clock only -- returns, lags and
cluster spans anchored on when the trade happened. A trade dated after its own filing is
impossible there and perfectly good on the DISCLOSURE clock: the filing was made, on
that day, about that security. So a flagged row is never hidden from anything anchored
on the filing date, and no date is ever corrected.

date_subclass says what the STRING looks like, never what we think happened.
"""
import datetime as dt
import re
import sqlite3

# The order's floor. Nothing this package ingests predates EDGAR's electronic era, so a
# date before it is a slip rather than history. scorecard.py and data_quality.py enforce
# 2004 on their own read paths; the two agree on every row in the corpus today (no
# congress row sits in 1990-2004) but they are not the same rule.
FLOOR = "1990-01-01"

BACKWARD = "backward"
FORWARD = "forward"

BEFORE_FLOOR = "before_floor"
AFTER_CEILING = "after_ceiling"
UNPARSEABLE = "unparseable"
AFTER_FILING = "after_filing"

# A report is filed in Eastern time and a trade dated where the filer sits, so a trade
# can carry the calendar date AFTER its filing's by one day and still be true.
FILING_TOLERANCE_DAYS = 1

# YYYY-MM-DD, optionally followed by an xs:date timezone designator, and NOTHING else.
# date.fromisoformat alone is not this check: from Python 3.11 it also accepts
# '20200101' and '2020-W01-1', which would then be compared as strings against FLOOR.
_ISO = re.compile(r"(\d{4}-\d{2}-\d{2})(?:Z|[+-]\d{2}:\d{2})?")
_MDY = re.compile(r"\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*")


def iso10(value):
    """The YYYY-MM-DD date of a value that is a calendar date and nothing else, or None.

    Tolerates exactly one kind of tail: the timezone designator EDGAR puts on xs:date
    values ('2026-07-03-05:00'), which carries nothing for a date-only field. Any other
    tail -- '2026-07-03garbage', '2026-07-031' -- makes the value unparseable rather
    than silently becoming its head. Never coerces."""
    if not isinstance(value, str):
        return None
    m = _ISO.fullmatch(value.strip())
    if not m:
        return None
    try:
        dt.date.fromisoformat(m.group(1))
    except ValueError:
        return None
    return m.group(1)


def iso_mdy(value):
    """'1/14/2026' -> '2026-01-14'. Unparseable stays None, never guessed.

    Same contract as house_fd_ingest._iso_mdy, plus two checks that one lacks: the
    calendar ('13/45/2026' would have been written '2026-13-45') and the whole string
    ('01/14/20261' would have been written '2026-01-14')."""
    m = _MDY.fullmatch(value) if isinstance(value, str) else None
    if not m:
        return None
    return iso10("{}-{:02d}-{:02d}".format(m.group(3), int(m.group(1)), int(m.group(2))))


def _day(today):
    if isinstance(today, str):
        return dt.date.fromisoformat(today)
    return today or dt.date.today()


def ceiling(today=None):
    """Tomorrow. A filing can carry today's date in any timezone the filer sits in."""
    return (_day(today) + dt.timedelta(days=1)).isoformat()


def is_blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


def date_flag(value, direction=BACKWARD, today=None):
    """Why this one date cannot be true, or None when it can -- or when it is absent."""
    if is_blank(value):
        return None
    head = iso10(value)
    if head is None:
        return UNPARSEABLE
    if head < FLOOR:
        return BEFORE_FLOOR
    if direction == BACKWARD and head > ceiling(today):
        return AFTER_CEILING
    return None


def row_flag(tx_date, filed_date=None, today=None):
    """Why a row's dates cannot all be true, or None.

    The trade date is judged first, then the date of the filing that reported it, then
    their ORDER: a trade dated after its own filing cannot have been reported by it.
    The house already calls that impossible -- scorecard.py drops negative lags -- and it
    is the same defect as a quarantined 2026-12-26 trade on a 2026-02-09 disclosure,
    only one whose mistyped date happens not to be in the future yet."""
    if is_blank(tx_date):
        return None
    t = iso10(tx_date)
    if t is None:
        return UNPARSEABLE
    if t < FLOOR:
        return BEFORE_FLOOR
    filed = date_flag(filed_date, BACKWARD, today)
    if filed:
        return "filed_" + filed
    f = iso10(filed_date)
    if f:
        # The filing ORDER is judged before the today+1 ceiling, and replaces it. Tested
        # the other way round, the answer depended on the day the row was judged: a
        # December trade on a January filing is always "after tomorrow" when that filing
        # arrives, so it was stored as after_ceiling with no sub-class and no suggested
        # date -- the year-slip label was reachable only for rows judged years late --
        # and "dated after tomorrow" then went false the day the date came round.
        # Against its own filing, a row's verdict is a property of the row.
        latest = (dt.date.fromisoformat(f)
                  + dt.timedelta(days=FILING_TOLERANCE_DAYS)).isoformat()
        return AFTER_FILING if t > latest else None
    # No filing date to judge against: only the calendar can bound it.
    return AFTER_CEILING if t > ceiling(today) else None


def normalise(value, direction=BACKWARD, today=None):
    """(value_to_store, flag) for a writer.

    A date is stored as YYYY-MM-DD -- its timezone designator, which carries nothing,
    is cut -- whether or not it can be true. An impossible date is therefore kept AS
    FILED apart from that designator, and marked: the filer's bytes are the evidence,
    and correcting them would destroy the only record of what was filed. A value that
    is not a date at all is stored exactly as received. A blank is stored as NULL:
    absent, not impossible."""
    if is_blank(value):
        return None, None
    flag = date_flag(value, direction, today)
    head = iso10(value)
    return (head if head else value), flag


def lag_days(disclosure, tx_date):
    """Days from trade to disclosure, or None when either end is not a usable date.

    congress_trades.lag_days is NOT NULL, so a row with a quarantined trade date keeps
    the arithmetic of what was filed -- 3031-04-30 against a 2026 disclosure is -368,891
    -- and its date_flag is what says the number means nothing. Writing NULL there would
    not quarantine the row, it would fail the INSERT and take the whole filing's ingest
    down with it. Any reader of lag_days must honour date_flag."""
    a, b = iso10(disclosure), iso10(tx_date)
    if not (a and b):
        return None
    return (dt.date.fromisoformat(a) - dt.date.fromisoformat(b)).days


def is_quarter_end(value):
    """13F periods are calendar quarter-ends by rule."""
    head = iso10(value)
    return bool(head) and head[5:] in ("03-31", "06-30", "09-30", "12-31")


# ------------------------------------------------------------------ sub-classes --
# What the string looks like. Named for the pattern, not for a cause: two different
# filers reporting LRCX on 2026-03-27 in filings of 2026-03-02 is almost certainly a
# coordinated pre-report of a scheduled vesting rather than two identical typos, and
# 'future_dated_other' says only what is on the page.
CENTURY_SLIP = "century_slip"            # 0025-07-25: a year of the form 00YY
SHORT_YEAR = "short_year"                # 0202-05-01: any other year below 1000
YEAR_SLIP_PATTERN = "year_slip_pattern"  # December trade, January filing, same year
NON_TRADING_DAY = "non_trading_day"      # dated on a weekend or market holiday
FUTURE_DATED_OTHER = "future_dated_other"

# The session calendar is whatever dates this ticker has a close for.
CALENDAR_TICKER = "SPY"

SUGGESTION_RULE = ("tx_date_suggested = the same month and day one year earlier, offered "
                   "only where a December trade sits on a January filing of the same "
                   "year. A hypothesis for the reader, never applied to tx_date.")


class Calendar:
    """Which dates the market was open, read once from the prices table.

    non_trading() answers True (closed), False (open) or None (cannot say). A weekend
    needs no calendar. A weekday is a holiday only INSIDE the calendar's coverage: a
    date the prices table has not reached yet is unknown, not closed."""

    def __init__(self, sessions=None, con=None):
        self._con = con
        self._sessions = None if sessions is None and con is not None else set(sessions or ())

    @classmethod
    def load(cls, con):
        """Lazy: the query runs on the first WEEKDAY asked about, so a filing of awards
        and withholdings -- never judged against the market -- costs nothing."""
        return cls(con=con)

    @property
    def sessions(self):
        if self._sessions is None:
            try:
                self._sessions = {r[0] for r in self._con.execute(
                    "SELECT date FROM prices WHERE ticker=? AND price_type='eod'",
                    (CALENDAR_TICKER,))}
            except sqlite3.Error:      # no prices table: weekends only
                self._sessions = set()
        return self._sessions

    @property
    def bounds(self):
        if getattr(self, "_bounds", None) is None:
            s = self.sessions
            self._bounds = (min(s), max(s)) if s else (None, None)
        return self._bounds

    def non_trading(self, value):
        head = iso10(value)
        if head is None:
            return None
        if dt.date.fromisoformat(head).weekday() >= 5:
            return True
        lo, hi = self.bounds
        if lo and lo <= head <= hi:
            return head not in self.sessions
        return None


def subclass(tx_date, filed_date, flag, calendar=None):
    """What a flagged trade date's string looks like, or None when the flag says it all.

    The year-slip pattern is tested first, deliberately: IBKR's 2019-12-21 on a
    2019-01-11 disclosure is also a Saturday, and the pattern is the more specific
    description of the string."""
    head = iso10(tx_date)
    if head is None:
        return None
    if head[:2] == "00":
        return CENTURY_SLIP
    if head[:4] < "1000":
        return SHORT_YEAR
    if flag != AFTER_FILING:
        return None
    filed = iso10(filed_date)
    if filed and head[:4] == filed[:4] and head[5:7] == "12" and filed[5:7] == "01":
        return YEAR_SLIP_PATTERN
    if calendar is not None and calendar.non_trading(head):
        return NON_TRADING_DAY
    return FUTURE_DATED_OTHER


def suggested(tx_date, sub):
    """The labelled hypothesis for a year-slip row, or None. See SUGGESTION_RULE."""
    head = iso10(tx_date)
    if sub != YEAR_SLIP_PATTERN or head is None:
        return None
    return iso10("{:04d}{}".format(int(head[:4]) - 1, head[4:]))


def judge(tx_date, filed_date=None, calendar=None, market_execution=False, today=None):
    """(date_flag, date_subclass, tx_date_suggested) for one row -- the single entry
    point every writer and the repair use, so the two cannot disagree.

    market_execution selects the rows worth judging against the session calendar: a
    Form 4 purchase or sale (code P or S), a congressional stock trade. One of those
    dated on a day with no US session is MARKED (date_subclass) and NOT quarantined
    (date_flag stays NULL). The mark says only what the string shows. It is not evidence
    of an error: codes P and S cover private transactions, and the marked rows include
    merger cash-outs closing on a Sunday, private-fund subscriptions dated the first of
    the month, and TSMC shares bought in Taipei on US Labor Day. Applied to every
    transaction code it would have caught 25,755 Form 4 rows, a fifth of all tax
    withholdings among them: vestings land on calendar dates whatever the market does.

    Today the mark is read by the footer count and the congressional row notes. No
    board filters on it."""
    flag = row_flag(tx_date, filed_date, today)
    sub = subclass(tx_date, filed_date, flag, calendar)
    if flag is None and market_execution and calendar is not None \
            and calendar.non_trading(tx_date):
        sub = NON_TRADING_DAY
    return flag, sub, suggested(tx_date, sub)


def sane_sql(column):
    """The range half of the rule, as SQL, for readers that must not depend on a repair
    having been applied. Pairs with `date_flag IS NULL`, which carries the half no range
    can express (a trade after its own filing). Returns (fragment, params)."""
    return ("substr({c},1,10) BETWEEN ? AND ?".format(c=column), [FLOOR, ceiling()])
