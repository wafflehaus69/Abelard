"""Four rulings of 2026-09-21, each pinned to the measurement that earned it.

1. A zero-weight holdings line nobody corroborates is residue, not a member (HOLX).
2. The benchmark's own distribution is added back on its ex-date (IVV, 2026-09-15).
3. A session is reconciled against the weights in force at the PRIOR close.
4. The nightly fills holes: only settled ones, and only ones not already filled.
"""

from __future__ import annotations

import pytest

from abelard_common.prices import reconcile as RC
from abelard_common.prices import reconstruct as R
from abelard_common.prices import reference as REF
from abelard_common.prices import schema as S
from abelard_common.prices import universe as U
from abelard_common.prices import verify as V
from abelard_common.prices.vendor import VendorSeries

RUN = 1789000000
PRIOR, DATE = "2026-09-14", "2026-09-15"


@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "prices.db")
    yield c
    c.close()


def inst(con, iid, ticker):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source, name,"
        " primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,'0','single',?,?,'test',0,'2026-09-01','2026-09-01')",
        (iid, iid.split(".")[0], ticker, ticker))
    return iid


def raw(con, iid, date, close, status="ok"):
    con.execute(
        "INSERT INTO prices_raw (instrument_id, date, close, status, source,"
        " fetched_at, run_asof) VALUES (?,?,?,?,'yahoo_v8',?,?)",
        (iid, date, close, status, RUN, RUN))


def ref(con, series, date, value):
    con.execute(
        "INSERT INTO reference_series (series_id, date, value, source, fetched_at)"
        " VALUES (?,?,?,'yahoo_v8',?)", (series, date, value, RUN))


# ------------------------------------------------------- 1. residual lines --

SEC = {
    "AAPL": ("0000320193", "Apple Inc.", "Nasdaq"),
    "ALGN": ("0001097149", "Align Technology", "Nasdaq"),
    "HOLX": ("0000859737", "HOLOGIC INC", "Nasdaq"),
    "XYZ": ("0001512673", "Block Inc", "NYSE"),
}


def _c(ticker, source, weight=None, index_code="SPX"):
    return U.Constituent(ticker=ticker, name=ticker, index_code=index_code,
                         source=source, weight=weight)


def _residual(rows):
    merged, r2k = U.assign_instrument_ids(rows, SEC)
    return U.residual_lines(rows, merged, r2k)


def test_holx_is_residue():
    """THE case. 2,843,388 shares marked at $0.01, weight 0.00, never on
    Wikipedia -- while the smallest real member weighed 0.01%."""
    rows = [_c("AAPL", "wikipedia_spx"), _c("AAPL", "ishares_ivv", 6.5),
            _c("ALGN", "wikipedia_spx"), _c("ALGN", "ishares_ivv", 0.01),
            _c("HOLX", "ishares_ivv", 0.0)]
    assert _residual(rows) == {("0000859737.0", "SPX")}


def test_a_corroborated_zero_weight_member_is_kept():
    """Rounded to 0.00 but Wikipedia lists it: a real member, kept."""
    rows = [_c("AAPL", "wikipedia_spx"), _c("AAPL", "ishares_ivv", 6.5),
            _c("XYZ", "wikipedia_spx"), _c("XYZ", "ishares_ivv", 0.0)]
    assert _residual(rows) == set()


def test_an_index_with_one_source_drops_nothing():
    """IWM is off, but if it is ever on, a 2,000-name file rounds real
    small-caps to 0.00 and there is no second source to corroborate against."""
    rows = [_c("AAPL", "ishares_iwm", 0.5, "RUT"), _c("XYZ", "ishares_iwm", 0.0, "RUT")]
    assert _residual(rows) == set()


def test_a_missing_weight_is_never_zero():
    rows = [_c("AAPL", "wikipedia_spx"), _c("AAPL", "ishares_ivv", 6.5),
            _c("HOLX", "ishares_ivv", None)]
    assert _residual(rows) == set()


def test_the_sync_report_names_the_residue():
    rep = U.SyncReport(as_of="2026-09-21")
    rep.residual.append("NOCIK.HOLX (SPX)")
    assert "residual lines excluded (zero weight, uncorroborated): 1" in rep.render()
    assert "NOCIK.HOLX (SPX)" in rep.render()


# ------------------------------------------ 2. the benchmark's own payout --

def test_the_ivv_ex_date_is_no_longer_a_false_failure(con):
    """2026-09-15 verbatim: IVV 764.48 -> 758.88, $2.203 ex. Price return
    -0.7325%; the S&P 500 itself -0.4495%. With the payout added back the
    benchmark lands within 0.6bp of the index instead of 28bp away."""
    ref(con, "IVV", PRIOR, 764.48)
    ref(con, "IVV", DATE, 758.88)
    con.execute("INSERT INTO reference_dividends (series_id, ex_date, amount,"
                " source, fetched_at) VALUES ('IVV',?,2.203,'yahoo_v8',?)", (DATE, RUN))
    r = RC._benchmark_return(con, "IVV", DATE, PRIOR)
    assert r == pytest.approx((758.88 + 2.203) / 764.48 - 1.0)
    assert abs(r - (-0.004495)) < 0.0001, "within 1bp of the index"


def test_an_ordinary_day_is_untouched(con):
    ref(con, "IVV", PRIOR, 764.48)
    ref(con, "IVV", DATE, 758.88)
    assert RC._benchmark_return(con, "IVV", DATE, PRIOR) == pytest.approx(
        758.88 / 764.48 - 1.0)


def test_a_payout_outside_the_window_is_ignored(con):
    ref(con, "IVV", PRIOR, 764.48)
    ref(con, "IVV", DATE, 758.88)
    con.execute("INSERT INTO reference_dividends (series_id, ex_date, amount,"
                " source, fetched_at) VALUES ('IVV','2026-06-16',1.9,'yahoo_v8',?)",
                (RUN,))
    assert RC._benchmark_return(con, "IVV", DATE, PRIOR) == pytest.approx(
        758.88 / 764.48 - 1.0)


def test_a_held_instrument_benchmark_uses_its_declared_dividend(con):
    iid = inst(con, "IVV.0", "IVV")
    raw(con, iid, PRIOR, 764.48)
    raw(con, iid, DATE, 758.88)
    con.execute("INSERT INTO corporate_actions (instrument_id, effective_date, kind,"
                " amount, declared_at, source, run_asof)"
                " VALUES (?,?,'dividend',2.203,?,'yahoo_v8',?)", (iid, DATE, RUN, RUN))
    assert RC._benchmark_return(con, iid, DATE, PRIOR) == pytest.approx(
        (758.88 + 2.203) / 764.48 - 1.0)


def test_the_reference_leg_keeps_the_distributions_it_was_already_sent(con):
    """Yahoo returned IVV's payouts in every response; they were discarded."""
    class Fake:
        def fetch(self, symbol, start, end):
            return VendorSeries(
                symbol=symbol,
                bars=[R.Bar(PRIOR, 764.48, 764.48, 764.48, 764.48, 1),
                      R.Bar(DATE, 758.88, 758.88, 758.88, 758.88, 1)],
                splits=[], dividends=[R.Dividend(DATE, 2.203)],
                vendor_adjclose={}, fetched_at=RUN)
    rep = REF.sync_yahoo(con, Fake(), PRIOR, DATE, series=(("IVV", "IVV"),))
    assert rep.dividends == 1
    assert [tuple(r) for r in con.execute(
        "SELECT ex_date, amount FROM reference_dividends")] == [(DATE, 2.203)]


# ----------------------------------------------- 3. weights at the PRIOR close --

def test_the_session_is_weighted_by_the_basket_that_earned_it(con):
    """Rebalance day: the prior-close basket is all AAA (+1%), the same-day file
    is all BBB (+5%), and the benchmark made +1%. Only the prior basket is
    right; the same-day file would fail the check by 400bp."""
    a, b = inst(con, "0000000001.0", "AAA"), inst(con, "0000000002.0", "BBB")
    for iid, p0, p1 in ((a, 100.0, 101.0), (b, 100.0, 105.0)):
        raw(con, iid, PRIOR, p0)
        raw(con, iid, DATE, p1)
    for iid, asof in ((a, PRIOR), (b, DATE)):
        con.execute("INSERT INTO index_weights (instrument_id, index_code, as_of,"
                    " weight, source) VALUES (?,'SPX',?,100.0,'ishares_ivv')",
                    (iid, asof))
    ref(con, "IVV", PRIOR, 100.0)
    ref(con, "IVV", DATE, 101.0)
    rec = RC.reconcile_session(con, DATE, PRIOR, benchmark="IVV")
    assert rec.status == "pass", rec.render()
    assert rec.rebuilt_return == pytest.approx(0.01)


def test_weights_published_only_after_the_prior_close_are_not_used(con):
    a = inst(con, "0000000001.0", "AAA")
    raw(con, a, PRIOR, 100.0)
    raw(con, a, DATE, 101.0)
    con.execute("INSERT INTO index_weights (instrument_id, index_code, as_of, weight,"
                " source) VALUES (?,'SPX',?,100.0,'ishares_ivv')", (a, DATE))
    rec = RC.reconcile_session(con, DATE, PRIOR, benchmark="IVV")
    assert rec.status == "insufficient" and PRIOR in rec.detail


# ------------------------------------------------ 4. the nightly's fill set --

def test_only_settled_unfilled_holes_are_selected(con):
    x = inst(con, "0000000009.0", "XXX")
    for d in ("2026-09-10", "2026-09-11", "2026-09-21"):
        raw(con, x, d, None, "vendor_null")
    con.execute("INSERT INTO fills (instrument_id, date, filled_close, source,"
                " evidence, filled_at, run_asof) VALUES (?,?,50.0,'yahoo_v8','{}',?,?)",
                (x, "2026-09-10", RUN, RUN))
    got = V.fillable_holes(con, "2026-09-21")
    assert got == {x: ["2026-09-11"]}, (
        "09-10 is already filled, 09-21 is today and not settled: %r" % got)


def test_a_friday_hole_is_fillable_on_monday(con):
    """The old age gate counted TRADING sessions and read Fri->Mon as one, so a
    three-day-settled Friday waited another night."""
    x = inst(con, "0000000009.0", "XXX")
    raw(con, x, "2026-09-18", None, "vendor_null")
    assert V.fillable_holes(con, "2026-09-21") == {x: ["2026-09-18"]}
