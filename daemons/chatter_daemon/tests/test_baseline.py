"""Baseline store — CRUD, trailing-window cap, exclude-current invariant, sample
std + sigma=0 guard, keyed isolation, re-run overwrite."""

from __future__ import annotations

from chatter_daemon.baseline import (
    Baseline,
    append_observation,
    connect,
    init_db,
    read_baseline,
)


def _store(tmp_path):
    conn = connect(tmp_path / "baseline.sqlite3")
    init_db(conn)
    return conn


def _append(conn, ts, count, *, ticker="NVDA", source="stocktwits"):
    append_observation(
        conn, watchlist="w", ticker=ticker, source=source, canonical_unix=ts, count=count
    )


def _read(conn, *, window=20, now=10_000, ticker="NVDA", source="stocktwits", max_age_s=None):
    return read_baseline(
        conn,
        watchlist="w",
        ticker=ticker,
        source=source,
        window=window,
        now=now,
        max_age_s=max_age_s,
    )


def test_empty_baseline(tmp_path):
    assert _read(_store(tmp_path)) == Baseline(0, 0.0, 0.0)


def test_append_and_read_excludes_current(tmp_path):
    conn = _store(tmp_path)
    for ts, c in ((100, 10), (200, 12), (300, 14)):
        _append(conn, ts, c)
    full = _read(conn, now=400)
    assert full.n == 3 and full.mean == 12.0
    # read AT ts=300 excludes the obs at 300 (the "current" scan) -> only 100, 200
    prior = _read(conn, now=300)
    assert prior.n == 2 and prior.mean == 11.0


def test_trailing_window_caps(tmp_path):
    conn = _store(tmp_path)
    for i in range(30):  # counts 0..29 at ts 1..30
        _append(conn, i + 1, i)
    b = _read(conn, window=5, now=999)
    assert b.n == 5  # only the last 5 by ts
    assert b.mean == 27.0  # (25+26+27+28+29)/5


def test_sample_std(tmp_path):
    conn = _store(tmp_path)
    for ts, c in ((1, 2), (2, 4), (3, 6)):  # mean 4, sample var (4+0+4)/2 = 4 -> std 2
        _append(conn, ts, c)
    b = _read(conn)
    assert b.mean == 4.0 and b.std == 2.0


def test_std_zero_when_constant(tmp_path):
    conn = _store(tmp_path)
    for ts in range(1, 6):
        _append(conn, ts, 7)
    b = _read(conn)
    assert b.n == 5 and b.std == 0.0  # sigma=0 guard fodder


def test_single_obs_std_zero(tmp_path):
    conn = _store(tmp_path)
    _append(conn, 100, 9)
    b = _read(conn)
    assert b.n == 1 and b.mean == 9.0 and b.std == 0.0


def test_keyed_by_ticker_and_source(tmp_path):
    conn = _store(tmp_path)
    _append(conn, 1, 10, source="stocktwits")
    _append(conn, 1, 99, source="smg")
    _append(conn, 1, 50, ticker="AMD", source="stocktwits")
    b = _read(conn)  # NVDA/stocktwits only
    assert b.n == 1 and b.mean == 10.0


def test_rerun_overwrites(tmp_path):
    conn = _store(tmp_path)
    _append(conn, 100, 10)
    _append(conn, 100, 20)  # same ts -> overwrite, not a second row
    b = _read(conn, now=999)
    assert b.n == 1 and b.mean == 20.0


def test_max_age_bounds_lookback(tmp_path):
    conn = _store(tmp_path)
    _append(conn, 1_000, 5)  # old
    _append(conn, 9_000, 15)  # recent
    # now=10_000, max_age 2_000 -> only ts >= 8_000 (the 9_000 obs)
    b = _read(conn, now=10_000, max_age_s=2_000)
    assert b.n == 1 and b.mean == 15.0
