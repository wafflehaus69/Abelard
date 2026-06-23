"""Integration — a simulated multi-run sequence drives one (ticker, source) through
building -> ok -> thin -> spike as the baseline fills, and the spike artifact
round-trips through persistence + read-chatter. This is the proof that anomaly only
becomes meaningful AFTER baselines accumulate (>= N_min runs)."""

from __future__ import annotations

from chatter_daemon.aggregate import build_aggregate
from chatter_daemon.baseline import connect, init_db
from chatter_daemon.persist import load_result, make_scan_id, write_result
from chatter_daemon.render import render_chatter
from chatter_daemon.schema import (
    CostTelemetry,
    Metrics,
    NormalizedRecord,
    ScanEnvelope,
    Sentiment,
    WatchlistSummary,
)
from chatter_daemon.windows import derive_windows, iso_z

BASE_TS = 1_700_000_000  # ~2023-11


def _env(count, ts):
    W = derive_windows(ts)
    rec = NormalizedRecord(
        watchlist="w",
        scan_mode="watchlist",
        canonical_ts=iso_z(ts),
        window=W["24h"],
        source="stocktwits",
        ticker="NVDA",
        matched_by=[],
        metrics=Metrics(mention_count=count),
        sentiment=Sentiment(method="none"),
        flags=[],
    )
    return ScanEnvelope(
        scan_mode="watchlist",
        canonical_ts=iso_z(ts),
        windows=list(W.values()),
        watchlists=[WatchlistSummary(name="w", tickers=1, active=1)],
        records=[rec],
        cost=CostTelemetry(),
    )


def _run(conn, count, ts, *, min_obs=5, floor=5, z=2.0):
    return build_aggregate(
        _env(count, ts),
        conn=conn,
        scan_id=make_scan_id(iso_z(ts), ["w"]),
        source_floors={"stocktwits": floor},
        baseline_window=20,
        baseline_min_obs=min_obs,
        spike_z_threshold=z,
        trend_spike_ratio=1.5,
        now=ts,
    )


def _state(res):
    return res.tickers[0].sources[0].anomaly.state


def test_building_ok_thin_spike_as_baseline_fills(tmp_path):
    conn = connect(tmp_path / "b.sqlite3")
    init_db(conn)

    # Runs 1..5: history < N_min (5) -> building, regardless of count.
    for i, c in enumerate([10, 11, 9, 10, 10]):
        assert _state(_run(conn, c, BASE_TS + i * 3600)) == "building"

    # Run 6: 5 prior obs -> baseline ready; count in-range and near mean -> ok.
    assert _state(_run(conn, 10, BASE_TS + 5 * 3600)) == "ok"

    # Run 7: count below the floor -> thin (suppresses a would-be z off noise).
    assert _state(_run(conn, 2, BASE_TS + 6 * 3600)) == "thin"

    # Run 8: a big jump over the trailing baseline -> spike.
    res8 = _run(conn, 40, BASE_TS + 7 * 3600)
    a = res8.tickers[0].sources[0].anomaly
    assert a.state == "spike" and a.z is not None and a.z > 2.0

    # The spike run round-trips through persistence + read-chatter.
    path = write_result(tmp_path / "archive", res8)
    loaded = load_result(path)
    assert loaded.tickers[0].sources[0].anomaly.state == "spike"
    out = render_chatter(loaded)
    assert "SPIKE" in out and "NVDA" in out and "messages" in out  # stocktwits noun
