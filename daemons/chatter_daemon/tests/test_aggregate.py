"""Aggregation — grouping + source_diversity, building on first run, spike against
PRIOR baseline (with append-after-compute), thin suppression, and run-provenance
passthrough (sources/degraded/cost/errors)."""

from __future__ import annotations

from chatter_daemon.aggregate import build_aggregate
from chatter_daemon.baseline import append_observation, connect, init_db, read_baseline
from chatter_daemon.schema import (
    CostTelemetry,
    Metrics,
    NormalizedRecord,
    ScanEnvelope,
    Sentiment,
    SourceStatus,
)
from chatter_daemon.windows import derive_windows, iso_z

FIXED = 1_718_733_600
W = derive_windows(FIXED)
FLOORS = {"finnhub_news": 3, "smg": 3, "stocktwits": 10}


def _rec(ticker, source, *, count=0, i24=None, i7=None, im=None, flags=None, window="24h"):
    return NormalizedRecord(
        watchlist="w",
        scan_mode="watchlist",
        canonical_ts=iso_z(FIXED),
        window=W[window],
        source=source,
        ticker=ticker,
        matched_by=[],
        metrics=Metrics(mention_count=count, interest_24h=i24, interest_7d=i7, interest_monthly=im),
        sentiment=Sentiment(method="none"),
        flags=flags or [],
    )


def _env(records, **kw):
    return ScanEnvelope(
        scan_mode="watchlist",
        canonical_ts=iso_z(FIXED),
        windows=list(W.values()),
        records=records,
        cost=kw.pop("cost", CostTelemetry()),
        **kw,
    )


def _store(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    init_db(conn)
    return conn


def _agg(conn, env, *, now=FIXED):
    return build_aggregate(
        env,
        conn=conn,
        scan_id="cd-test",
        source_floors=FLOORS,
        baseline_window=20,
        baseline_min_obs=5,
        spike_z_threshold=2.0,
        now=now,
    )


def _prime(conn, ticker, source, counts, *, end=FIXED):
    for i, c in enumerate(counts):
        append_observation(
            conn, watchlist="w", ticker=ticker, source=source,
            canonical_unix=end - len(counts) + i, count=c,
        )


def test_groups_by_ticker_and_diversity(tmp_path):
    conn = _store(tmp_path)
    env = _env([
        _rec("NVDA", "stocktwits", count=20),
        _rec("NVDA", "smg", count=4),
        _rec("NVDA", "finnhub_news", count=7),
        _rec("AMD", "stocktwits", count=0),  # zero -> no signal
    ])
    res = _agg(conn, env)
    by = {t.ticker: t for t in res.tickers}
    assert {s.source for s in by["NVDA"].sources} == {"stocktwits", "smg", "finnhub_news"}
    assert by["NVDA"].source_diversity == 3  # all three nonzero
    assert by["AMD"].source_diversity == 0  # stocktwits count 0


def test_building_on_first_run(tmp_path):
    conn = _store(tmp_path)
    res = _agg(conn, _env([_rec("NVDA", "smg", count=99)]))
    assert res.tickers[0].sources[0].anomaly.state == "building"  # no history yet


def test_spike_against_prior_baseline_then_appended(tmp_path):
    conn = _store(tmp_path)
    _prime(conn, "NVDA", "smg", [10, 10, 11, 9, 10, 10])  # mean ~10, small sigma
    res = _agg(conn, _env([_rec("NVDA", "smg", count=40)]), now=FIXED)
    a = res.tickers[0].sources[0].anomaly
    assert a.state == "spike" and a.z is not None and a.z > 2.0
    # the current 40 is appended only AFTER the anomaly was computed
    b = read_baseline(conn, watchlist="w", ticker="NVDA", source="smg", window=20, now=FIXED + 1)
    assert b.n == 7  # 6 prior + the just-appended 40


def test_thin_suppresses(tmp_path):
    conn = _store(tmp_path)
    _prime(conn, "DE", "smg", [0, 1, 0, 1, 0, 1])
    res = _agg(conn, _env([_rec("DE", "smg", count=2)]), now=FIXED)  # 2 < smg floor 3
    assert res.tickers[0].sources[0].anomaly.state == "thin"


def test_run_provenance_passthrough(tmp_path):
    conn = _store(tmp_path)
    env = _env(
        [_rec("NVDA", "stocktwits", count=5)],
        sources=[SourceStatus(source="stocktwits", ok=True, record_count=1)],
        degraded=False,
        cost=CostTelemetry(haiku_calls=2, input_tokens=100),
        errors=["smg: warn"],
    )
    res = build_aggregate(
        env, conn=conn, scan_id="cd-xyz", source_floors=FLOORS,
        baseline_window=20, baseline_min_obs=5, spike_z_threshold=2.0,
        now=FIXED,
    )
    assert res.scan_id == "cd-xyz"
    assert res.cost.haiku_calls == 2 and res.cost.input_tokens == 100
    assert res.degraded is False and res.sources[0].source == "stocktwits"
    assert res.errors == ["smg: warn"]
