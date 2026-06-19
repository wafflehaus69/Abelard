"""Orchestrator spine — single timestamp, no-source emptiness, failure isolation."""

from __future__ import annotations

import time

from chatter_daemon.orchestrator import run_scan
from chatter_daemon.schema import ScanEnvelope
from chatter_daemon.watchlist import WatchlistConfig
from chatter_daemon.windows import iso_z

FIXED = 1_718_733_600


def _wl(name="alpha", symbols=("NVDA", "AMD")):
    return WatchlistConfig(name=name, tickers=[{"symbol": s} for s in symbols])


def test_run_scan_single_timestamp_no_clock_read(monkeypatch):
    # With `now` injected, the run must not read the wall clock anywhere — patch
    # time.time to blow up and prove every window + canonical_ts trace to `now`.
    def _no_clock():
        raise AssertionError("run_scan read the wall clock despite now= being injected")

    monkeypatch.setattr(time, "time", _no_clock)
    env = run_scan([_wl()], now=FIXED)
    assert isinstance(env, ScanEnvelope)
    assert env.canonical_ts == iso_z(FIXED)
    assert {w.end for w in env.windows} == {iso_z(FIXED)}
    assert {w.label for w in env.windows} == {"24h", "7d", "monthly"}


def test_run_scan_no_sources_empty_records():
    env = run_scan([_wl()], now=FIXED)
    assert env.records == []
    assert env.errors == []
    assert env.sources == []
    assert env.degraded is False
    assert env.scan_mode == "watchlist"
    assert env.watchlists[0].name == "alpha"
    assert env.watchlists[0].tickers == 2
    assert env.watchlists[0].active == 2


def test_run_scan_source_failure_isolated():
    class BoomSource:
        name = "stocktwits"

        def fetch(self, watchlist, *, context):
            raise RuntimeError("upstream down")

    env = run_scan([_wl()], sources=[BoomSource()], now=FIXED)
    assert any("upstream down" in e for e in env.errors)
    assert env.records == []  # the run survives a dead source
    assert env.degraded is True
    assert env.sources[0].source == "stocktwits"
    assert env.sources[0].ok is False
    assert "upstream down" in env.sources[0].error


def test_run_scan_active_count_excludes_disabled():
    wl = WatchlistConfig(
        name="x", tickers=[{"symbol": "NVDA"}, {"symbol": "P", "enabled": False}]
    )
    env = run_scan([wl], now=FIXED)
    assert env.watchlists[0].tickers == 2
    assert env.watchlists[0].active == 1
