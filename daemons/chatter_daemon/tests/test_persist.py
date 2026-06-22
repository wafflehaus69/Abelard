"""Persistence — stable scan_id, round-trip, atomic re-run overwrite, every fail-loud
load path, and cost surviving a simulated write failure."""

from __future__ import annotations

import json

import pytest

from chatter_daemon.persist import ArchiveError, load_result, make_scan_id, write_result
from chatter_daemon.schema import (
    AggregatedScanResult,
    AggregatedTicker,
    Anomaly,
    CostTelemetry,
    Metrics,
    Sentiment,
    SourceSignal,
)


def _result(scan_id="cd-2026-06-19T14-32-08Z-abcd1234"):
    sig = SourceSignal(
        source="stocktwits",
        metrics=Metrics(mention_count=20),
        sentiment=Sentiment(method="haiku", bullish=3, bearish=1, neutral=2),
        matched_by=[],
        flags=["sentiment_classified"],
        anomaly=Anomaly(kind="count", state="spike", z=3.1, mean=10.0, std=3.0, observations=8),
    )
    return AggregatedScanResult(
        scan_id=scan_id,
        scan_mode="watchlist",
        canonical_ts="2026-06-19T14:32:08Z",
        windows=[],
        tickers=[AggregatedTicker(watchlist="w", ticker="NVDA", sources=[sig], source_diversity=1)],
        sources=[],
        degraded=False,
        cost=CostTelemetry(haiku_calls=1, input_tokens=50),
    )


def test_make_scan_id_stable_and_order_independent():
    a = make_scan_id("2026-06-19T14:32:08Z", ["barber_growth", "x"])
    b = make_scan_id("2026-06-19T14:32:08Z", ["x", "barber_growth"])
    assert a == b
    assert a.startswith("cd-2026-06-19T14-32-08Z-")


def test_round_trip(tmp_path):
    res = _result()
    path = write_result(tmp_path, res)
    assert path.exists() and path.parent.name == "2026-06"  # YYYY-MM partition
    loaded = load_result(path)
    assert loaded.scan_id == res.scan_id
    assert loaded.tickers[0].sources[0].anomaly.z == 3.1
    assert loaded.cost.haiku_calls == 1


def test_rerun_overwrites_no_stray_tmp(tmp_path):
    res = _result()
    write_result(tmp_path, res)
    write_result(tmp_path, res)  # same scan_id -> overwrite in place
    files = sorted(p.name for p in (tmp_path / "2026-06").iterdir())
    assert files == [f"{res.scan_id}.json"]  # only the final, no .tmp residue


def test_load_missing_raises(tmp_path):
    with pytest.raises(ArchiveError, match="does not exist"):
        load_result(tmp_path / "nope.json")


def test_load_directory_raises(tmp_path):
    d = tmp_path / "adir"
    d.mkdir()
    with pytest.raises(ArchiveError, match="not a regular file"):
        load_result(d)


def test_load_malformed_json_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(ArchiveError, match="malformed JSON"):
        load_result(p)


def test_load_schema_mismatch_raises(tmp_path):
    p = tmp_path / "wrong.json"
    p.write_text(json.dumps({"scan_id": "x"}), encoding="utf-8")  # missing required fields
    with pytest.raises(ArchiveError, match="schema mismatch"):
        load_result(p)


def test_malformed_scan_id_raises(tmp_path):
    with pytest.raises(ArchiveError):
        write_result(tmp_path, _result(scan_id="garbage"))


def test_cost_survives_write_failure(tmp_path, monkeypatch):
    import chatter_daemon.persist as P

    res = _result()

    def boom(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr(P.os, "replace", boom)
    with pytest.raises(ArchiveError, match="failed to write"):
        write_result(tmp_path, res)
    # cost was folded in before the write -> never in jeopardy
    assert res.cost.haiku_calls == 1 and res.cost.input_tokens == 50
    # and no half-written tmp left behind
    assert not any(p.name.endswith(".tmp") for p in (tmp_path / "2026-06").iterdir())
