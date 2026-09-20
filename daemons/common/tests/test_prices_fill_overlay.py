"""A filled hole must be VISIBLE to the readers, not just recorded.

``prices_raw`` is insert-only, so a session the primary returned with no price
keeps its ``vendor_null`` row forever. G3 fills that hole from a second sourced
vendor, but the slot is taken, so the fill lands in ``fills`` instead — and the
insert's own comment calls it "an overlay the view honours".

Nothing honoured it. ``fills`` was written and never read anywhere in the
package, so a filled hole stayed invisible and the substrate could not heal.
Measured on the live store 2026-09-19: 5,688 sessions across 2026-09-03..18 read
as permanently empty while the vendor served every one of them on request, and
the nightly SPX reconciliation reported 0.0% weight coverage for sixteen
consecutive nights. These tests are that bug.
"""

from __future__ import annotations

import sqlite3

import pytest

from abelard_common.prices import schema as S
from abelard_common.prices.reconcile import effective_close, _price_return

IID = "0000000001.0"
RUN = 1789000000


@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "p.db")
    # prices_raw and fills both carry an FK to instruments.
    c.execute(
        "INSERT INTO instruments (instrument_id, cik, name, primary_ticker,"
        " source, first_seen, last_seen) VALUES (?,?,?,?,'test',?,?)",
        (IID, "0000000001", "Test Co", "TEST", "2026-09-01", "2026-09-16"))
    return c


def _raw(c, date, close, status="ok"):
    c.execute(
        "INSERT INTO prices_raw (instrument_id, date, close, status, source,"
        " fetched_at, run_asof) VALUES (?,?,?,?,'yahoo_v8',?,?)",
        (IID, date, close, status, RUN, RUN))


def _fill(c, date, close):
    c.execute(
        "INSERT INTO fills (instrument_id, date, filled_close, source, evidence,"
        " filled_at, run_asof) VALUES (?,?,?,'tiingo','{}',?,?)",
        (IID, date, close, RUN, RUN))


def test_an_ok_row_is_the_effective_close(con):
    _raw(con, "2026-09-16", 100.0)
    assert effective_close(con, IID, "2026-09-16") == 100.0


def test_a_filled_hole_is_visible_through_the_overlay(con):
    """THE regression. The vendor_null row stays — it is the record that the
    primary had nothing — and the fill supplies the price."""
    _raw(con, "2026-09-16", None, "vendor_null")
    _fill(con, "2026-09-16", 102.36)
    assert effective_close(con, IID, "2026-09-16") == 102.36
    held = con.execute(
        "SELECT close, status FROM prices_raw WHERE instrument_id=? AND date=?",
        (IID, "2026-09-16")).fetchone()
    assert held["status"] == "vendor_null" and held["close"] is None, (
        "the primary's own record must be untouched by the overlay")


def test_a_filled_status_row_is_honoured(con):
    """The other shape of hole: the primary never returned the session at all,
    so the slot was empty and the fill went straight into prices_raw."""
    _raw(con, "2026-09-16", 99.5, "filled")
    assert effective_close(con, IID, "2026-09-16") == 99.5


def test_an_unfilled_hole_stays_none(con):
    """The fix must not invent data — a hole nobody filled is still a hole."""
    _raw(con, "2026-09-16", None, "vendor_null")
    assert effective_close(con, IID, "2026-09-16") is None


def test_a_quarantined_session_is_never_a_fallback(con):
    """A quarantined row is a session under suspicion. Resolving it is a
    human's job; silently reading it would launder the suspicion away."""
    _raw(con, "2026-09-16", 100.0, "quarantined")
    assert effective_close(con, IID, "2026-09-16") is None


def test_the_primary_outranks_the_overlay(con):
    """If the primary has a price, that is what the session is known by. A stale
    or duplicate fill must not displace a held value."""
    _raw(con, "2026-09-16", 100.0)
    _fill(con, "2026-09-16", 999.0)
    assert effective_close(con, IID, "2026-09-16") == 100.0


def test_a_return_spans_two_filled_sessions(con):
    """Both legs filled: the return is real and the pair is usable. Before the
    fix this was None and the session counted as missing weight."""
    _raw(con, "2026-09-15", None, "vendor_null")
    _raw(con, "2026-09-16", None, "vendor_null")
    _fill(con, "2026-09-15", 100.0)
    _fill(con, "2026-09-16", 101.0)
    r = _price_return(con, IID, "2026-09-16", "2026-09-15")
    assert r == pytest.approx(0.01)


def test_one_missing_leg_still_yields_no_return(con):
    _raw(con, "2026-09-15", None, "vendor_null")
    _raw(con, "2026-09-16", None, "vendor_null")
    _fill(con, "2026-09-16", 101.0)          # only the later leg
    assert _price_return(con, IID, "2026-09-16", "2026-09-15") is None


def test_a_split_inside_the_window_is_still_divided_out(con):
    """The overlay must not bypass the split adjustment the raw path applies."""
    _raw(con, "2026-09-15", None, "vendor_null")
    _raw(con, "2026-09-16", None, "vendor_null")
    _fill(con, "2026-09-15", 400.0)
    _fill(con, "2026-09-16", 101.0)          # post 4:1 split
    con.execute(
        "INSERT INTO corporate_actions (instrument_id, effective_date, kind,"
        " ratio, declared_at, source, run_asof)"
        " VALUES (?,?,'split',4.0,?,'yahoo_v8',?)",
        (IID, "2026-09-16", RUN, RUN))
    r = _price_return(con, IID, "2026-09-16", "2026-09-15")
    assert r == pytest.approx(101.0 / 400.0 * 4.0 - 1.0)
