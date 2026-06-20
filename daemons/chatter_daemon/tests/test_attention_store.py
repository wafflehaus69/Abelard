"""ATTENTION rolling store — CRUD, exclude-current trailing baseline, 14-day hot
cutoff, and prune-to-cold roll-up (group by day/ticker/source, archive then delete,
nothing lost)."""

from __future__ import annotations

from chatter_daemon.attention_store import (
    HOT_WINDOW_DAYS,
    append_observation,
    collect_prunable_rollups,
    delete_pruned,
    hot_cutoff,
    init_attention_table,
    read_baseline,
)
from chatter_daemon.baseline import Baseline, connect

_DAY = 24 * 60 * 60
BASE = 1_700_000_000  # 2023-11-14T22:13:20Z


def _store(tmp_path):
    conn = connect(tmp_path / "attn.sqlite3")
    init_attention_table(conn)
    return conn


def _append(conn, ts, count, *, ticker="GME", source="smg_freq"):
    append_observation(conn, ticker=ticker, source=source, scan_ts=ts, count=count)


def _read(conn, *, now, window=20, ticker="GME", source="smg_freq"):
    return read_baseline(conn, ticker=ticker, source=source, window=window, now=now)


def test_empty_baseline(tmp_path):
    assert _read(_store(tmp_path), now=BASE) == Baseline(0, 0.0, 0.0)


def test_append_read_excludes_current(tmp_path):
    conn = _store(tmp_path)
    for ts, c in ((BASE, 10), (BASE + 100, 12), (BASE + 200, 14)):
        _append(conn, ts, c)
    assert _read(conn, now=BASE + 300).n == 3
    # reading AT a ts excludes the obs at that ts (the "current" scan)
    prior = _read(conn, now=BASE + 200)
    assert prior.n == 2 and prior.mean == 11.0


def test_sample_std_and_window(tmp_path):
    conn = _store(tmp_path)
    for i, c in enumerate([2, 4, 6]):  # mean 4, sample var (4+0+4)/2 = 4 -> std 2
        _append(conn, BASE + i, c)
    b = _read(conn, now=BASE + 100)
    assert b.mean == 4.0 and b.std == 2.0


def test_keyed_by_ticker_and_source(tmp_path):
    conn = _store(tmp_path)
    _append(conn, BASE, 10, ticker="GME", source="smg_freq")
    _append(conn, BASE, 99, ticker="GME", source="reddit_rising")
    _append(conn, BASE, 50, ticker="AMC", source="smg_freq")
    b = _read(conn, now=BASE + 100)  # GME / smg_freq only
    assert b.n == 1 and b.mean == 10.0


def test_hot_cutoff():
    assert hot_cutoff(BASE) == BASE - HOT_WINDOW_DAYS * _DAY


def test_prune_rolls_up_and_deletes(tmp_path):
    conn = _store(tmp_path)
    # Two scans the same UTC day (within 1h) -> one rollup; one recent obs kept.
    _append(conn, BASE, 5)
    _append(conn, BASE + 3600, 7)
    _append(conn, BASE + 20 * _DAY, 9)  # 20 days later -> recent
    _append(conn, BASE + 1, 4, ticker="AMC")  # a different ticker, same old day

    cutoff = BASE + 14 * _DAY
    rollups = collect_prunable_rollups(conn, cutoff=cutoff)
    by = {(r.ticker, r.source): r for r in rollups}
    assert by[("GME", "smg_freq")].scans == 2
    assert by[("GME", "smg_freq")].total_count == 12
    assert by[("GME", "smg_freq")].max_count == 7
    assert by[("AMC", "smg_freq")].scans == 1 and by[("AMC", "smg_freq")].total_count == 4
    assert all(r.day == "2023-11-14" for r in rollups)  # grouped by UTC day

    deleted = delete_pruned(conn, cutoff=cutoff)
    assert deleted == 3  # GME x2 + AMC x1; the recent GME obs survives

    # The recent obs remains and still feeds velocity.
    remaining = _read(conn, now=BASE + 30 * _DAY)
    assert remaining.n == 1 and remaining.mean == 9.0


def test_prune_no_old_events_is_noop(tmp_path):
    conn = _store(tmp_path)
    _append(conn, BASE + 30 * _DAY, 5)
    cutoff = BASE + 14 * _DAY
    assert collect_prunable_rollups(conn, cutoff=cutoff) == []
    assert delete_pruned(conn, cutoff=cutoff) == 0
