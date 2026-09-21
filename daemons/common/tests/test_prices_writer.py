"""PS-1 Phase 2 — writer tests against a synthetic vendor.

No network. A ``FakeVendor`` serves series built to order, which is what makes
the MNST failure mode reproducible instead of only observable in production.

The four the order names explicitly:

* a name that splits mid-history, and a dividend payer;
* the detector fires with the right ratio;
* a changed raw close fails loud;
* ``last_date_held`` comes from the rows RETURNED — the ``prices.py:194``
  regression, in its new home.
"""

from __future__ import annotations

import datetime as dt
import sqlite3

import pytest

from abelard_common.prices import reconstruct as R
from abelard_common.prices import schema as S
from abelard_common.prices import writer as W
from abelard_common.prices.vendor import VendorError, VendorSeries

RUN = 1788000000


# ------------------------------------------------------------ fake vendor --

class FakeVendor:
    """Serves prepared series. Records the spans it was asked for, so a test can
    assert the writer requests a SPAN rather than crawling one day at a time."""

    def __init__(self, series_by_symbol: dict[str, VendorSeries]):
        self._series = series_by_symbol
        self.calls: list[tuple[str, str, str]] = []
        self.fail: set[str] = set()

    def fetch(self, symbol: str, start: str, end: str) -> VendorSeries:
        self.calls.append((symbol, start, end))
        if symbol in self.fail:
            raise VendorError("{}: synthetic outage".format(symbol))
        s = self._series[symbol]
        bars = [b for b in s.bars if start <= b.date <= end]
        return VendorSeries(
            symbol=s.symbol, bars=bars, splits=s.splits, dividends=s.dividends,
            vendor_adjclose={d: v for d, v in s.vendor_adjclose.items()
                             if start <= d <= end},
            short_name=s.short_name, fetched_at=RUN,
        )


def _sessions(start: str, n: int) -> list[str]:
    """n weekday sessions from start."""
    d, out = dt.date.fromisoformat(start), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def make_series(symbol, dates, closes, splits=(), dividends=(), volumes=None):
    bars = [
        R.Bar(d, c, c, c, c, (volumes[i] if volumes else 1_000_000))
        for i, (d, c) in enumerate(zip(dates, closes))
    ]
    return VendorSeries(
        symbol=symbol, bars=bars, splits=list(splits), dividends=list(dividends),
        vendor_adjclose={d: c for d, c in zip(dates, closes) if c is not None},
        fetched_at=RUN,
    )


# ----------------------------------------------------------------- fixtures --

@pytest.fixture()
def con(tmp_path):
    c = S.connect(tmp_path / "prices.db")
    yield c
    c.close()


def register(con, iid, ticker, index_code="SPX", as_of="2026-09-01"):
    con.execute(
        "INSERT INTO instruments (instrument_id, cik, class_code, class_source, name,"
        " primary_ticker, source, provisional, first_seen, last_seen)"
        " VALUES (?,?,'0','single',?,?,'test',0,?,?)",
        (iid, iid.split(".")[0], ticker, ticker, as_of, as_of))
    con.execute(
        "INSERT INTO ticker_aliases (instrument_id, ticker, notation, valid_from,"
        " valid_to, source) VALUES (?,?,'vendor',?,NULL,'test')",
        (iid, ticker, as_of))
    con.execute("INSERT INTO index_membership VALUES (?,?,?,1,'test')",
                (iid, index_code, as_of))
    con.commit()
    return iid


# ------------------------------------------------- a clean split mid-history --

def test_clean_split_reconstructs_and_does_not_quarantine(con):
    """The vendor adjusts its whole history correctly: closes are smooth, raw
    steps 4:1 at the split (as a true traded price must), nothing is flagged."""
    dates = _sessions("2026-06-01", 10)
    eff = dates[5]
    # Vendor close: smooth ~100 throughout, because it is split-adjusted.
    closes = [100.0, 101.0, 102.0, 101.5, 103.0, 104.0, 104.5, 105.0, 104.0, 106.0]
    s = make_series("SPLT", dates, closes, splits=[R.Split(eff, 4.0)])
    iid = register(con, "0000000001.0", "SPLT")
    res = W.ingest_series(con, iid, s, RUN)
    con.commit()

    assert res.status == "ok" and res.quarantined == 0
    raws = {r["date"]: r["close"] for r in
            con.execute("SELECT date, close FROM prices_raw WHERE instrument_id=?", (iid,))}
    # Pre-split raw is 4x the adjusted close; post-split raw equals it.
    assert raws[dates[0]] == pytest.approx(400.0)
    assert raws[dates[4]] == pytest.approx(412.0)
    assert raws[eff] == pytest.approx(104.0)
    # The adjusted view puts the split adjustment back, so returns are sane.
    adj = {r["date"]: r["adj_close"] for r in
           con.execute("SELECT date, adj_close FROM adjusted_view WHERE instrument_id=?", (iid,))}
    assert adj[dates[4]] == pytest.approx(103.0)
    assert adj[eff] == pytest.approx(104.0)
    assert con.execute(
        "SELECT COUNT(*) FROM corporate_actions WHERE instrument_id=? AND kind='split'",
        (iid,)).fetchone()[0] == 1


def test_dividend_payer_gets_a_crsp_total_return_view(con):
    dates = _sessions("2026-06-01", 6)
    closes = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    ex = dates[3]
    s = make_series("DIVP", dates, closes, dividends=[R.Dividend(ex, 2.0)])
    iid = register(con, "0000000002.0", "DIVP")
    W.ingest_series(con, iid, s, RUN)
    con.commit()
    adj = {r["date"]: r["adj_close"] for r in
           con.execute("SELECT date, adj_close FROM adjusted_view WHERE instrument_id=?", (iid,))}
    # f = 1 - 2/100 = 0.98 applied to every session BEFORE the ex-date.
    assert adj[dates[0]] == pytest.approx(98.0)
    assert adj[dates[2]] == pytest.approx(98.0)
    assert adj[ex] == pytest.approx(100.0)
    assert adj[dates[5]] == pytest.approx(100.0)


# ----------------------------------------- the MNST shape: partial adjustment --

def test_partial_split_adjustment_is_detected_and_quarantined(con):
    """The MNST failure, synthesised: the vendor declares a 2:1 but leaves some
    pre-split sessions on the unadjusted scale. The step it leaves behind is a
    ratio matching the declared one, which is exactly the signal."""
    dates = _sessions("2026-06-01", 10)
    eff = dates[7]
    # Sessions 0-2 and 5-6 unadjusted (~100); 3-4 adjusted (~50); post-split ~50.
    closes = [100.0, 100.0, 100.0, 50.0, 50.0, 100.0, 100.0, 50.0, 50.5, 51.0]
    s = make_series("CRPT", dates, closes, splits=[R.Split(eff, 2.0)])
    iid = register(con, "0000000003.0", "CRPT")
    res = W.ingest_series(con, iid, s, RUN)
    con.commit()

    assert res.status == "quarantined"
    events = list(con.execute(
        "SELECT effective_date, implied_ratio, kind FROM adjustment_events"
        " WHERE instrument_id=? ORDER BY effective_date", (iid,)))
    assert events, "the detector must fire"
    assert all(e["kind"] == "vendor_corruption" for e in events)
    # Three scale flips in this series: d2->d3 (0.5), d4->d5 (2.0), d6->d7 (0.5).
    # Both directions must be caught -- the inverse is as much a signal as the
    # ratio itself, since we cannot know which side of a flip is the correct one.
    ratios = sorted(round(e["implied_ratio"], 3) for e in events)
    assert ratios == [0.5, 0.5, 2.0], ratios
    # Nothing in the corrupt region is stamped as a fact.
    statuses = {r["date"]: r["status"] for r in con.execute(
        "SELECT date, status FROM prices_raw WHERE instrument_id=?", (iid,))}
    assert statuses[dates[0]] == "quarantined"
    assert statuses[dates[6]] == "quarantined"
    # ... and a quarantined session never reaches the analytics view.
    viewed = {r["date"] for r in con.execute(
        "SELECT date FROM adjusted_view WHERE instrument_id=?", (iid,))}
    assert not (viewed & {d for d, st in statuses.items() if st == "quarantined"})


def test_unexplained_forty_percent_move_is_flagged_unknown(con):
    dates = _sessions("2026-06-01", 5)
    closes = [100.0, 101.0, 180.0, 181.0, 182.0]       # +78% with no declared action
    s = make_series("JUMP", dates, closes)
    iid = register(con, "0000000004.0", "JUMP")
    W.ingest_series(con, iid, s, RUN)
    con.commit()
    kinds = [r["kind"] for r in con.execute(
        "SELECT kind FROM adjustment_events WHERE instrument_id=?", (iid,))]
    assert kinds == ["unknown"]


# --------------------------------------------------------- fact integrity --

def test_a_changed_raw_close_fails_loud_and_writes_nothing(con):
    dates = _sessions("2026-06-01", 4)
    iid = register(con, "0000000005.0", "FACT")
    first = make_series("FACT", dates, [10.0, 10.2, 10.1, 10.4])
    assert W.ingest_series(con, iid, first, RUN).status == "ok"
    con.commit()

    revised = make_series("FACT", dates, [10.0, 10.9, 10.1, 10.4])   # 10.2 restated as 10.9
    res = W.ingest_series(con, iid, revised, RUN + 1)
    con.commit()

    assert res.status == "fact_change"
    assert "does not change" in res.detail
    held = {r["date"]: r["close"] for r in con.execute(
        "SELECT date, close FROM prices_raw WHERE instrument_id=?", (iid,))}
    assert held[dates[1]] == 10.2, "the original fact must survive"


def test_reingesting_the_same_series_is_idempotent(con):
    dates = _sessions("2026-06-01", 5)
    iid = register(con, "0000000006.0", "IDEM")
    s = make_series("IDEM", dates, [10.0, 10.1, 10.2, 10.15, 10.3])
    W.ingest_series(con, iid, s, RUN); con.commit()
    n1 = con.execute("SELECT COUNT(*) FROM prices_raw").fetchone()[0]
    r2 = W.ingest_series(con, iid, s, RUN + 1); con.commit()
    assert r2.status == "ok" and r2.rows_inserted == 0
    assert con.execute("SELECT COUNT(*) FROM prices_raw").fetchone()[0] == n1


# ------------------------------------------- the prices.py:194 regression --

def test_last_date_held_comes_from_returned_rows_not_the_request(con):
    """THE regression. The old layer recorded the span it ASKED for, so a caller
    requesting entry+200 days wrote a span reaching into next year; after that
    the cache claimed coverage it did not have and served stale data silently,
    freezing 343 tickers including SPY.

    Here the writer asks through a date far in the future and the vendor returns
    nothing past 2026-06-05. freshness must record what came back."""
    dates = _sessions("2026-06-01", 5)
    iid = register(con, "0000000007.0", "SPAN")
    s = make_series("SPAN", dates, [10.0, 10.1, 10.2, 10.15, 10.3])
    vendor = FakeVendor({"SPAN": s})
    run = W.nightly(con, vendor, run_asof=RUN, today="2027-02-07")

    assert run.names[0].status == "ok"
    held = con.execute(
        "SELECT last_date_held FROM freshness WHERE instrument_id=?", (iid,)
    ).fetchone()[0]
    assert held == dates[-1] == "2026-06-05"
    assert held != "2027-02-07"
    # And the request really did reach for today -- it is the RESPONSE that bounds us.
    assert vendor.calls[0][2] == "2027-02-07"


def test_nightly_requests_a_span_not_the_next_single_session(con):
    """The old cache advanced one session per night, ~13 months in arrears. A
    span request closes the whole gap in one call."""
    dates = _sessions("2026-06-01", 20)
    iid = register(con, "0000000008.0", "GAP")
    full = make_series("GAP", dates, [100.0 + i for i in range(20)])
    vendor = FakeVendor({"GAP": full})

    W.nightly(con, vendor, run_asof=RUN, today=dates[4])
    con.commit()
    assert con.execute("SELECT last_date_held FROM freshness").fetchone()[0] == dates[4]

    W.nightly(con, vendor, run_asof=RUN + 1, today=dates[19])
    con.commit()
    _sym, start, end = vendor.calls[-1]
    assert start == dates[4] and end == dates[19]
    assert con.execute("SELECT last_date_held FROM freshness").fetchone()[0] == dates[19]
    assert con.execute("SELECT COUNT(*) FROM prices_raw").fetchone()[0] == 20


def test_vendor_null_session_does_not_advance_last_date_held(con):
    """A5: a null close is recorded as a row, and freshness must not treat it as
    coverage -- otherwise a name reports current while its newest price is missing."""
    dates = _sessions("2026-06-01", 4)
    iid = register(con, "0000000009.0", "NUL")
    s = make_series("NUL", dates, [10.0, 10.1, 10.2, None])
    W.ingest_series(con, iid, s, RUN); con.commit()
    assert con.execute(
        "SELECT last_date_held FROM freshness WHERE instrument_id=?", (iid,)
    ).fetchone()[0] == dates[2]
    assert con.execute(
        "SELECT status FROM prices_raw WHERE instrument_id=? AND date=?",
        (iid, dates[3])).fetchone()[0] == "vendor_null"


# ----------------------------------------------------------- run mechanics --

def test_vendor_outage_is_counted_never_fatal(con):
    dates = _sessions("2026-06-01", 3)
    a = register(con, "0000000010.0", "GOOD")
    b = register(con, "0000000011.0", "BAD")
    vendor = FakeVendor({
        "GOOD": make_series("GOOD", dates, [50.0, 50.5, 50.2]),
        "BAD": make_series("BAD", dates, [50.0, 50.5, 50.2]),
    })
    vendor.fail.add("BAD")
    run = W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    counts = run.counts()
    assert counts.get("ok") == 1 and counts.get("vendor_error") == 1
    assert con.execute(
        "SELECT last_fetch_status FROM freshness WHERE instrument_id=?", (b,)
    ).fetchone()[0] == "vendor_error"


def test_telemetry_row_is_opened_before_data_and_closed_after(con):
    dates = _sessions("2026-06-01", 3)
    register(con, "0000000012.0", "TLM")
    vendor = FakeVendor({"TLM": make_series("TLM", dates, [50.0, 50.5, 50.2])})
    run = W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    row = con.execute("SELECT * FROM run_telemetry WHERE run_asof=?", (RUN,)).fetchone()
    assert row["status"] == "ok"
    assert row["requests_made"] == 1 and row["rows_inserted"] == 3
    assert row["started_at"] and row["finished_at"]
    assert run.run_asof == RUN


def test_one_run_asof_stamps_every_row(con):
    """E13 window alignment: one as-of per run, so 'what did the store know at
    run R' has a single answer."""
    dates = _sessions("2026-06-01", 3)
    for i, tk in enumerate(("AA", "BB")):
        register(con, "000000002{}.0".format(i), tk)
    vendor = FakeVendor({tk: make_series(tk, dates, [50.0, 50.5, 50.2]) for tk in ("AA", "BB")})
    W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    stamps = {r[0] for r in con.execute("SELECT DISTINCT run_asof FROM prices_raw")}
    assert stamps == {RUN}


def test_status_reports_lagging_names(con):
    dates = _sessions("2026-06-01", 6)
    a = register(con, "0000000030.0", "FRSH")
    b = register(con, "0000000031.0", "STAL")
    vendor = FakeVendor({
        "FRSH": make_series("FRSH", dates, [1.0] * 6),
        "STAL": make_series("STAL", dates[:3], [1.0] * 3),
    })
    W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    rep = W.status(con)
    assert rep.latest_session == dates[-1]
    assert [x[1] for x in rep.lagging] == ["STAL"]
    assert not rep.ok


def test_status_is_ok_when_everything_is_current(con):
    dates = _sessions("2026-06-01", 4)
    register(con, "0000000040.0", "ONE")
    vendor = FakeVendor({"ONE": make_series("ONE", dates, [1.0] * 4)})
    W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    assert W.status(con).ok


def test_refetch_picks_the_least_recently_swept(con):
    dates = _sessions("2026-06-01", 3)
    ids = [register(con, "00000000{}.0".format(50 + i), "R{}".format(i)) for i in range(3)]
    vendor = FakeVendor({"R{}".format(i): make_series("R{}".format(i), dates, [50.0, 50.5, 50.2])
                         for i in range(3)})
    W.nightly(con, vendor, run_asof=RUN, today=dates[-1])
    con.execute("UPDATE freshness SET last_full_refetch_at=100 WHERE instrument_id=?", (ids[0],))
    con.execute("UPDATE freshness SET last_full_refetch_at=200 WHERE instrument_id=?", (ids[1],))
    con.execute("UPDATE freshness SET last_full_refetch_at=300 WHERE instrument_id=?", (ids[2],))
    con.commit()
    run = W.refetch(con, vendor, n=2, since=dates[0], today=dates[-1], run_asof=RUN + 9)
    assert sorted(n.instrument_id for n in run.names) == sorted(ids[:2])
    assert con.execute(
        "SELECT last_full_refetch_at FROM freshness WHERE instrument_id=?", (ids[0],)
    ).fetchone()[0] == RUN + 9
