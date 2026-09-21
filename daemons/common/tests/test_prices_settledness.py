"""Never record "not yet" as "nothing".

``prices_raw`` is insert-only and a duplicate insert is swallowed, so the first
write for a session wins forever. The nightly runs at 21:00, when the exchange
clock (``is_final_session``) says today is final, but the vendor has usually not
delivered today's bar yet. It arrives with a null close. Written, that null
became a ``vendor_null`` row, and the next night's good close for the same
session had nowhere to go. On the live store, 5,688 sessions across
2026-09-03..18 were lost that way, while the vendor served every one of them on
request a day later.

The ruling: a null for a session the vendor has not had a full day to settle
writes NOTHING. Absence is the honest representation of "not yet";
``vendor_null`` is reserved for its true meaning, a settled session where the
vendor genuinely had nothing. The reconciler shares the same definition, so it
reports the still-arriving session as UNSETTLED instead of INSUFFICIENT.

Plus the Berkshire identity split: iShares began serving 'BRK B' with a space.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from abelard_common.prices import reconstruct as R
from abelard_common.prices import schema as S
from abelard_common.prices import universe as U
from abelard_common.prices import writer as W
from abelard_common.prices.calendar import (exchange_today, is_final_session,
                                            is_vendor_settled)
from abelard_common.prices.reconcile import default_target
from abelard_common.prices.vendor import VendorSeries

RUN = 1789000000
NY = ZoneInfo("America/New_York")


def at(day: str, hour: int) -> float:
    """Epoch for ``hour``:00 New York time on ``day``."""
    d = dt.date.fromisoformat(day)
    return dt.datetime(d.year, d.month, d.day, hour, 0, tzinfo=NY).timestamp()


MON = "2026-09-21"
FRI = "2026-09-18"
THU = "2026-09-17"
NIGHTLY_MON = at(MON, 21)
NIGHTLY_TUE = at("2026-09-22", 21)


def series(sym, closes: dict[str, float | None]) -> VendorSeries:
    dates = sorted(closes)
    return VendorSeries(
        symbol=sym,
        bars=[R.Bar(d, closes[d], closes[d], closes[d], closes[d], 1_000_000)
              for d in dates],
        splits=[], dividends=[],
        vendor_adjclose={d: c for d, c in closes.items() if c is not None},
        fetched_at=RUN)


@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "prices.db")
    yield c
    c.close()


def register(con, iid, ticker):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source, name,"
        " primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,'0','single',?,?,'test',0,?,?)",
        (iid, iid.split(".")[0], ticker, ticker, "2026-09-01", "2026-09-01"))
    con.execute(
        "INSERT INTO ticker_aliases (instrument_id, ticker, notation, valid_from,"
        " valid_to, source) VALUES (?,?,'vendor',?,NULL,'test')",
        (iid, ticker, "2026-09-01"))
    con.execute("INSERT INTO index_membership VALUES (?,?,?,1,'test')",
                (iid, "SPX", "2026-09-01"))
    con.commit()
    return iid


def rows(con, iid):
    return {r["date"]: (r["close"], r["status"]) for r in con.execute(
        "SELECT date, close, status FROM prices_raw WHERE instrument_id=?", (iid,))}


# ------------------------------------------------------------ the definition --

def test_the_clock_and_the_vendor_disagree_about_tonight():
    """THE distinction. At 21:00 the exchange clock calls today final; the
    vendor has not yet had a full day to deliver it. Both are true, and they
    answer different questions."""
    assert is_final_session(MON, NIGHTLY_MON) is True
    assert is_vendor_settled(MON, NIGHTLY_MON) is False


def test_a_past_session_is_settled_and_a_future_one_is_not():
    assert is_vendor_settled(FRI, NIGHTLY_MON) is True
    assert is_vendor_settled("2026-09-22", NIGHTLY_MON) is False


def test_today_is_computed_in_the_exchange_timezone():
    """00:30 UTC on the 22nd is still the 21st in New York."""
    utc_0030 = dt.datetime(2026, 9, 22, 0, 30, tzinfo=dt.timezone.utc).timestamp()
    assert exchange_today(utc_0030) == MON


# ---------------------------------------------------------------- the writer --

def test_tonights_null_writes_nothing(con):
    """THE regression. Before the ruling this row became vendor_null, forever."""
    iid = register(con, "0000000001.0", "AAA")
    W.ingest_series(con, iid, series("AAA", {FRI: 100.0, MON: None}),
                    RUN, now_epoch=NIGHTLY_MON)
    con.commit()
    held = rows(con, iid)
    assert MON not in held, "a not-yet session must leave no row at all: %r" % held
    assert held[FRI] == (100.0, "ok")


def test_the_next_night_heals_the_session(con):
    """Because the slot stayed empty, the next night's good close lands."""
    iid = register(con, "0000000001.0", "AAA")
    W.ingest_series(con, iid, series("AAA", {FRI: 100.0, MON: None}),
                    RUN, now_epoch=NIGHTLY_MON)
    con.commit()
    W.ingest_series(con, iid, series("AAA", {FRI: 100.0, MON: 101.5,
                                            "2026-09-22": None}),
                    RUN + 86400, now_epoch=NIGHTLY_TUE)
    con.commit()
    held = rows(con, iid)
    assert held[MON] == (101.5, "ok"), (
        "Monday must heal to ok, not stay vendor_null: %r" % held)
    assert "2026-09-22" not in held


def test_last_date_held_does_not_advance_past_a_not_yet_session(con):
    """So the next night's span asks for the session again."""
    iid = register(con, "0000000001.0", "AAA")
    res = W.ingest_series(con, iid, series("AAA", {FRI: 100.0, MON: None}),
                          RUN, now_epoch=NIGHTLY_MON)
    assert res.last_date_held == FRI


def test_a_settled_null_is_still_recorded_as_vendor_null(con):
    """vendor_null keeps its true meaning: settled, and the vendor had nothing.
    The 2026-08-28 vendor outage is this case, and it must stay visible."""
    iid = register(con, "0000000001.0", "AAA")
    W.ingest_series(con, iid, series("AAA", {THU: None, FRI: 100.0}),
                    RUN, now_epoch=NIGHTLY_MON)
    con.commit()
    assert rows(con, iid)[THU] == (None, "vendor_null")


def test_a_real_close_tonight_is_still_written(con):
    """The one name the vendor HAS delivered by 21:00 (HUBB, on every night of
    the outage) keeps being written. Only an absence waits."""
    iid = register(con, "0000048898.0", "HUBB")
    W.ingest_series(con, iid, series("HUBB", {FRI: 440.0, MON: 446.92}),
                    RUN, now_epoch=NIGHTLY_MON)
    con.commit()
    assert rows(con, iid)[MON] == (446.92, "ok")


def test_an_afternoon_run_still_stops_at_yesterday(con):
    """The existing clock gate is untouched: before 17:00 today is not final
    whatever the vendor says."""
    iid = register(con, "0000000001.0", "AAA")
    W.ingest_series(con, iid, series("AAA", {FRI: 100.0, MON: 101.0}),
                    RUN, now_epoch=at(MON, 14))
    con.commit()
    assert MON not in rows(con, iid)


# ------------------------------------------------------------ the reconciler --

def _fresh(con, iid, last):
    con.execute("INSERT OR REPLACE INTO freshness (instrument_id, last_date_held,"
                " last_fetch_at, last_fetch_status) VALUES (?,?,?,'ok')",
                (iid, last, RUN))


def test_a_still_arriving_session_is_unsettled_not_insufficient(con):
    """One fast name holding today used to drag the target onto a session
    nobody else had. Now the settled session is reconciled, and said so."""
    fast = register(con, "0000048898.0", "HUBB")
    slow = register(con, "0000000001.0", "AAA")
    _fresh(con, fast, MON)
    _fresh(con, slow, FRI)
    date, note = default_target(con, NIGHTLY_MON)
    assert date == FRI, "Monday's session of record is Friday, over the weekend"
    assert "UNSETTLED" in note and MON in note


def test_a_settled_session_is_never_excused(con):
    """Saturday's run: Friday is past, so it is reconciled as itself and any
    missing weight is INSUFFICIENT, loudly, as before."""
    iid = register(con, "0000000001.0", "AAA")
    _fresh(con, iid, FRI)
    date, note = default_target(con, at("2026-09-19", 21))
    assert (date, note) == (FRI, "")


def test_nothing_held_means_nothing_to_reconcile(con):
    assert default_target(con, NIGHTLY_MON) == (None, "")


# ------------------------------------------------- lag is for members only --

def test_a_departed_name_does_not_lag_forever(con):
    """The 2026-09-21 rebalance retired BLDR, TAP and TTD. A departed name is no
    longer fetched, so it falls behind by construction; counting it would pin
    the nightly at exit 1 for the life of the store."""
    here = register(con, "0000000001.0", "AAA")
    gone = register(con, "0001316835.0", "BLDR")
    con.execute("INSERT INTO index_membership VALUES (?,?,?,0,'test')",
                (gone, "SPX", MON))
    _fresh(con, here, MON)
    _fresh(con, gone, "2026-09-16")          # three sessions behind
    assert gone not in [x[0] for x in W.status(con).lagging]


def test_a_current_member_that_lags_is_still_reported(con):
    """The scoping must not become a way to hide a genuinely stale member."""
    a = register(con, "0000000001.0", "AAA")
    b = register(con, "0000000002.0", "BBB")
    _fresh(con, a, MON)
    _fresh(con, b, "2026-09-16")
    assert [x[0] for x in W.status(con).lagging] == [b]


def test_a_retired_provisional_does_not_lag(con):
    """The Berkshire split left NOCIK.BRK B with a freshness row and no history.
    Once the resolver fix retired it, it must stop counting."""
    here = register(con, "0001067983.0", "BRK-B")
    stub = register(con, "NOCIK.BRK B", "BRK B")
    con.execute("INSERT INTO index_membership VALUES (?,?,?,0,'test')",
                (stub, "SPX", MON))
    _fresh(con, here, MON)
    con.execute("INSERT OR REPLACE INTO freshness (instrument_id, last_date_held,"
                " last_fetch_at, last_fetch_status) VALUES (?,NULL,?,'vendor_error')",
                (stub, RUN))
    assert W.status(con).lagging == []


# ------------------------------------------------------ the Berkshire split --

SEC_MAP = {
    "BRK-A": ("0001067983", "BERKSHIRE HATHAWAY INC", "NYSE"),
    "BRK-B": ("0001067983", "BERKSHIRE HATHAWAY INC", "NYSE"),
    "BF-B": ("0000014693", "BROWN FORMAN CORP", "NYSE"),
    "HOLX": ("0000859737", "HOLOGIC INC", "Nasdaq"),
}


def _c(ticker, source, **kw):
    return U.Constituent(ticker=ticker, name=kw.pop("name", ticker),
                         index_code="SPX", source=source, **kw)


@pytest.mark.parametrize("ishares, canon, cik", [
    ("BRK B", "BRK-B", "0001067983"),
    ("BF B", "BF-B", "0000014693"),
])
def test_a_space_separated_class_resolves_to_the_cik(ishares, canon, cik):
    """THE split. 2026-09-17's iShares file said 'BRK B'; it fell through to a
    NOCIK provisional and Berkshire's 1.4% weight moved onto an instrument with
    no price history. It must merge with Wikipedia's dotted form instead."""
    rows_in = [_c(canon.replace("-", "."), "wikipedia_spx", cik=cik),
               _c(ishares, "ishares_ivv")]
    merged, raw_to_key = U.assign_instrument_ids(rows_in, SEC_MAP)
    assert len(merged) == 1, "one security, one instrument: %r" % list(merged)
    (key, c), = merged.items()
    assert key == canon and c.cik == cik
    assert raw_to_key[ishares] == canon
    assert not U.instrument_id(c).startswith("NOCIK")


def test_the_earlier_notations_still_resolve():
    for raw in ("BRKB", "BRK.B", "BRK-B", " brk b "):
        merged, _ = U.assign_instrument_ids([_c(raw, "ishares_ivv")], SEC_MAP)
        (key, c), = merged.items()
        assert (key, c.cik) == ("BRK-B", "0001067983"), raw


def test_a_plain_ticker_is_untouched():
    assert U.normalise("HOLX")["dash"] == "HOLX"
