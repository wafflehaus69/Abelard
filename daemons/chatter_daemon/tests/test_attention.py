"""ATTENTION scan — /smg/ floor=3 gate, cold-start salience (no baseline), velocity
maturation, amplified watchlist intersection, StockTwits velocity + carry-through,
prune-to-cold."""

from __future__ import annotations

import json

from chatter_daemon.attention import prune_cold, run_attention_scan
from chatter_daemon.attention_store import (
    append_observation,
    init_attention_table,
    read_baseline,
)
from chatter_daemon.baseline import connect
from chatter_daemon.discovery import SurfaceCounts

NOW = 1_700_000_000
_DAY = 24 * 60 * 60
FLOORS = {"smg_freq": 3, "stocktwits_trending": 0}  # ST: top-30 self-gates (no floor)


def _conn(tmp_path):
    conn = connect(tmp_path / "attn.sqlite3")
    init_attention_table(conn)
    return conn


def _sc(source, counts, *, semantics="24h", warning=None, meta=None):
    return SurfaceCounts(source, semantics, dict(counts), warning, meta or {})


def _run(conn, surfaces, *, wl=None, now=NOW, min_obs=5):
    return run_attention_scan(
        conn=conn,
        surfaces=surfaces,
        watchlist_symbols=wl or {},
        floors=FLOORS,
        scan_id="cd-2023-11-14T00-00-00Z-deadbeef",
        canonical_ts="2023-11-14T00:00:00Z",
        now=now,
        baseline_window=20,
        baseline_min_obs=min_obs,
        spike_z_threshold=2.0,
    )


def test_smg_floor_3_gate(tmp_path):
    res = _run(_conn(tmp_path), [_sc("smg_freq", {"GME": 2, "AMC": 3, "NVDA": 9})])
    admitted = {t.ticker for t in res.tickers}
    assert "GME" not in admitted  # count 2 < floor 3
    assert "AMC" in admitted and "NVDA" in admitted  # 3 and 9 admitted
    assert res.surfaces[0].floor == 3 and res.surfaces[0].candidates == 2


def test_cold_start_salience_no_baseline(tmp_path):
    res = _run(_conn(tmp_path), [_sc("smg_freq", {"GME": 7})])
    t = res.tickers[0]
    assert t.ticker == "GME" and t.salience == 7  # loud immediately, no history needed
    assert "cold_start" in t.flags  # building -> brand-new meme stock from zero
    assert t.signals[0].anomaly.state == "building"


def test_velocity_matures_to_spike(tmp_path):
    conn = _conn(tmp_path)
    for i, c in enumerate([10, 10, 11, 9, 10, 10]):  # 6 prior obs ~10
        append_observation(conn, ticker="GME", source="smg_freq", scan_ts=NOW - 1000 + i, count=c)
    res = _run(conn, [_sc("smg_freq", {"GME": 40})], now=NOW)
    t = res.tickers[0]
    assert t.signals[0].anomaly.state == "spike" and "spike" in t.flags
    # appended only AFTER the anomaly was computed (exclude-current)
    assert read_baseline(conn, ticker="GME", source="smg_freq", window=20, now=NOW + 1).n == 7


def test_amplified_watchlist_intersection(tmp_path):
    res = _run(
        _conn(tmp_path),
        [_sc("smg_freq", {"GME": 5, "AMC": 5})],
        wl={"barber_growth": {"GME"}},
    )
    by = {t.ticker: t for t in res.tickers}
    assert by["GME"].amplified is True and by["GME"].on_watchlists == ["barber_growth"]
    assert by["AMC"].amplified is False and by["AMC"].on_watchlists == []


def test_stocktwits_velocity_stores_and_carries_meta(tmp_path):
    # Order 9: StockTwits trending joined the count path. The rounded trending_score is
    # the salience/velocity axis; rank/score/watchlist_count/sector/summary carry through.
    conn = _conn(tmp_path)
    sc = _sc(
        "stocktwits_trending", {"GME": 88}, semantics="point-in-time trending (top 30)",
        meta={"GME": {"rank": 1, "trending_score": 88.4, "watchlist_count": 123,
                      "sector": "Consumer", "summary": "up big"}},
    )
    res = _run(conn, [sc])
    t = res.tickers[0]
    assert t.ticker == "GME" and t.salience == 88  # rounded trending_score
    sig = t.signals[0]
    assert sig.anomaly is not None and sig.anomaly.state == "building"  # velocity, first obs
    assert sig.rank == 1 and sig.trending_score == 88.4 and sig.watchlist_count == 123
    assert sig.sector == "Consumer" and sig.summary == "up big"
    # NOW stored as a time series (n == 1 after this scan)
    assert read_baseline(conn, ticker="GME", source="stocktwits_trending", window=20, now=NOW + 1).n == 1


def test_degraded_surface_isolates(tmp_path):
    res = _run(
        _conn(tmp_path),
        [_sc("smg_freq", {"NVDA": 5}), _sc("stocktwits_trending", {}, warning="stocktwits: CF wall or transport")],
    )
    assert res.degraded is True
    assert any("CF wall" in e for e in res.errors)
    statuses = {s.source: s for s in res.surfaces}
    assert statuses["smg_freq"].ok is True and statuses["stocktwits_trending"].ok is False
    assert "NVDA" in {t.ticker for t in res.tickers}  # the good surface still produced


def test_prune_cold_archives_then_deletes(tmp_path):
    conn = _conn(tmp_path)
    append_observation(conn, ticker="GME", source="smg_freq", scan_ts=NOW, count=5)
    append_observation(conn, ticker="GME", source="smg_freq", scan_ts=NOW + 20 * _DAY, count=9)
    now2 = NOW + 20 * _DAY  # the old NOW obs is >14d behind now2

    pruned = prune_cold(conn, now=now2, archive_root=tmp_path / "archive", generated_ts=now2)
    assert pruned == 1
    files = list((tmp_path / "archive").rglob("cd-*-attnroll-*.json"))
    assert len(files) == 1  # rolled to cold archive
    data = json.loads(files[0].read_text(encoding="utf-8"))
    assert data["rollups"][0]["ticker"] == "GME" and data["rollups"][0]["total_count"] == 5
    # hot store keeps only the recent obs (nothing lost — the old one is in cold)
    assert read_baseline(conn, ticker="GME", source="smg_freq", window=20, now=now2 + 1).n == 1


def test_multi_run_building_then_spike_then_prune(tmp_path):
    # The acceptance sequence: velocity matures building -> spike as the rolling store
    # fills, then aged events roll to cold.
    conn = _conn(tmp_path)
    for i, c in enumerate([10, 11, 9, 10, 10]):  # runs 1-5: history < N_min -> building
        res = _run(conn, [_sc("smg_freq", {"GME": c})], now=NOW + i * _DAY)
        assert res.tickers[0].signals[0].anomaly.state == "building"
    res6 = _run(conn, [_sc("smg_freq", {"GME": 40})], now=NOW + 5 * _DAY)  # baseline ready -> spike
    assert "spike" in res6.tickers[0].flags
    pruned = prune_cold(
        conn, now=NOW + 25 * _DAY, archive_root=tmp_path / "archive", generated_ts=NOW + 25 * _DAY
    )
    assert pruned >= 1  # the aged early runs rolled to cold
    assert list((tmp_path / "archive").rglob("cd-*-attnroll-*.json"))
