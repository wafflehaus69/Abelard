"""PS-1 — exchange calendar and epoch->session-date conversion.

Two jobs, both of them the sort of thing that fails silently.

**1. Session dating.** Yahoo returns epoch seconds. Converting them in UTC
happens to work for US listings, but only by luck: the daily bar for an equity
is stamped 13:30/14:30 UTC (09:30 America/New_York), while ``CL=F`` is stamped
04:00/05:00 UTC — which is **midnight exchange-local**. That lands on the right
UTC date only because New York is behind UTC. Any venue ahead of UTC would date
midnight-local to the previous day, silently, on every bar. So the conversion
runs in the exchange's own timezone, taken from the response's
``meta.exchangeTimezoneName``.

Note ``meta.gmtoffset`` is NOT usable for this: the response reports the offset
in force *now*, not the one in force for each bar. Fetching November 2021 in
September returns ``gmtoffset=-14400`` (EDT) for bars that traded in EST. The
timezone NAME plus ``zoneinfo`` handles the transition correctly; the offset
does not. Both are pinned in the DST tests.

**2. Trading sessions.** ``freshness`` cannot tell a holiday from a stale name
by counting days. "Lags more than one session" has to mean sessions. The NYSE
holiday list is static and observed rules are stable, so it is computed rather
than fetched — with an explicit horizon, so a year past the end fails loud
instead of quietly treating a holiday as a trading day.
"""

from __future__ import annotations

import datetime as dt
from functools import lru_cache
from zoneinfo import ZoneInfo

from .schema import PriceStoreError

DEFAULT_TZ = "America/New_York"

# Recomputed each year from the observed rules below; refresh the horizon
# annually. Past it, session arithmetic fails loud rather than guessing.
CALENDAR_FIRST_YEAR = 2015
CALENDAR_LAST_YEAR = 2035


class CalendarError(PriceStoreError):
    def __init__(self, message: str) -> None:
        super().__init__(message, stage="calendar")


def session_date(epoch: int, tz_name: str | None = DEFAULT_TZ) -> str:
    """Epoch seconds -> the ISO session date, in the EXCHANGE's timezone."""
    try:
        # ZoneInfo() itself raises on an unknown name, so it must be INSIDE the
        # guard: a vendor inventing a timezone must degrade to UTC, not take the
        # whole run down.
        tz = ZoneInfo(tz_name) if tz_name else dt.timezone.utc
        return dt.datetime.fromtimestamp(epoch, tz).date().isoformat()
    except Exception:
        return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).date().isoformat()


# ------------------------------------------------------------------ holidays --

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> dt.date:
    d = dt.date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + dt.timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> dt.date:
    d = dt.date(year, month, 31) if month == 12 else dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    while d.weekday() != weekday:
        d -= dt.timedelta(days=1)
    return d


def _easter(year: int) -> dt.date:
    """Anonymous Gregorian algorithm. Good Friday is Easter minus two days."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return dt.date(year, month, day + 1)


def _observed(d: dt.date) -> dt.date:
    """NYSE rule: a Saturday holiday is observed the Friday before, a Sunday one
    the Monday after."""
    if d.weekday() == 5:
        return d - dt.timedelta(days=1)
    if d.weekday() == 6:
        return d + dt.timedelta(days=1)
    return d


@lru_cache(maxsize=1)
def holidays() -> frozenset[str]:
    """NYSE full-day closures. Half-days still trade, so they are sessions."""
    out: set[str] = set()
    for y in range(CALENDAR_FIRST_YEAR, CALENDAR_LAST_YEAR + 1):
        days = [
            _observed(dt.date(y, 1, 1)),                 # New Year's Day
            _nth_weekday(y, 1, 0, 3),                    # MLK, 3rd Monday Jan
            _nth_weekday(y, 2, 0, 3),                    # Washington, 3rd Mon Feb
            _easter(y) - dt.timedelta(days=2),           # Good Friday
            _last_weekday(y, 5, 0),                      # Memorial Day
            _observed(dt.date(y, 7, 4)),                 # Independence Day
            _nth_weekday(y, 9, 0, 1),                    # Labor Day
            _nth_weekday(y, 11, 3, 4),                   # Thanksgiving, 4th Thu
            _observed(dt.date(y, 12, 25)),               # Christmas
        ]
        if y >= 2022:                                     # Juneteenth, from 2022
            days.append(_observed(dt.date(y, 6, 19)))
        out.update(d.isoformat() for d in days)
    # One-off closures that no rule generates.
    out.update({
        "2018-12-05",   # National Day of Mourning, G.H.W. Bush
        "2025-01-09",   # National Day of Mourning, Carter
    })
    return frozenset(out)


def is_session(day: str | dt.date) -> bool:
    d = dt.date.fromisoformat(day) if isinstance(day, str) else day
    if d.year > CALENDAR_LAST_YEAR or d.year < CALENDAR_FIRST_YEAR:
        raise CalendarError(
            "no calendar for {}: horizon is {}-{}; extend CALENDAR_LAST_YEAR"
            .format(d.year, CALENDAR_FIRST_YEAR, CALENDAR_LAST_YEAR))
    return d.weekday() < 5 and d.isoformat() not in holidays()


def sessions_between(start: str, end: str) -> list[str]:
    """Trading sessions in ``(start, end]`` — exclusive of start, inclusive of
    end, which is the shape ``freshness`` needs: how many sessions have passed
    since the one we hold."""
    a, b = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    out, d = [], a + dt.timedelta(days=1)
    while d <= b:
        if is_session(d):
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def sessions_behind(last_held: str | None, today: str) -> int:
    """How many trading sessions a name is behind. ``None`` means never fetched.

    This is what turns "lags >1 day" into "lags >1 session" — over Thanksgiving
    a perfectly current name is four calendar days stale and zero sessions
    behind, and a day-counting ledger would page somebody every holiday.
    """
    if last_held is None:
        return 10 ** 6
    if last_held >= today:
        return 0
    return len(sessions_between(last_held, today))


# A session is not a FACT until it has closed and settled. NYSE closes 16:00
# local; this margin lets late prints and the vendor's own consolidation land
# before we commit a number we can never change.
SESSION_FINAL_HOUR = 17


def is_final_session(day: str, now_epoch: float | None = None,
                     tz_name: str = DEFAULT_TZ) -> bool:
    """Has ``day`` closed and settled, in the exchange's timezone?

    This exists because insert-only and an in-progress session are incompatible.
    Writing today's intraday print as an immutable fact guarantees a fact-change
    storm on the very next fetch — observed for real during the build: a run
    interrupted mid-universe stored 2026-09-02 intraday values, and the re-run
    fired fact_change on every name it had already touched. The prices were not
    wrong; the session simply had not finished happening yet.

    So the nightly (21:00 local) commits today, and any run before 17:00 local
    stops at yesterday.
    """
    now = dt.datetime.now(_tz(tz_name)) if now_epoch is None else         dt.datetime.fromtimestamp(now_epoch, _tz(tz_name))
    today = now.date().isoformat()
    if day < today:
        return True
    if day > today:
        return False
    return now.hour >= SESSION_FINAL_HOUR


def _tz(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return dt.timezone.utc


def previous_session(day: str) -> str:
    d = dt.date.fromisoformat(day) - dt.timedelta(days=1)
    while not is_session(d):
        d -= dt.timedelta(days=1)
    return d.isoformat()
