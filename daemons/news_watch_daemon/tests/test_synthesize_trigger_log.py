"""Trigger log writer/reader tests."""

from __future__ import annotations

import json

import pytest

from news_watch_daemon.synthesize.trigger import TriggerDecision
from news_watch_daemon.synthesize.trigger_log import read_last_n, write_entry


def _decision(fire: bool = True, reason: str = "delta_threshold:t:3") -> TriggerDecision:
    return TriggerDecision(
        fire=fire,
        reason=reason,
        matched_headline_ids=("h1", "h2"),
        themes_in_scope=("t",),
        window_since_unix=100,
        window_until_unix=200,
    )


def test_write_creates_parent_dir(tmp_path):
    log = tmp_path / "deep" / "nested" / "trigger_log.jsonl"
    write_entry(log, _decision())
    assert log.is_file()


def test_write_appends_jsonl_line(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision())
    write_entry(log, _decision(reason="cross_theme:a+b"))
    lines = log.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    e1 = json.loads(lines[0])
    e2 = json.loads(lines[1])
    assert e1["reason"] == "delta_threshold:t:3"
    assert e2["reason"] == "cross_theme:a+b"


def test_write_entry_schema(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision())
    entry = json.loads(log.read_text(encoding="utf-8").splitlines()[0])
    assert entry["decision"] == "fire"
    assert entry["reason"] == "delta_threshold:t:3"
    assert entry["themes_in_scope"] == ["t"]
    assert entry["matched_headline_ids"] == ["h1", "h2"]
    assert entry["window_since_unix"] == 100
    assert entry["window_until_unix"] == 200
    # Timestamp fields present + paired
    assert "timestamp" in entry and "timestamp_unix" in entry
    assert entry["timestamp"].endswith("Z")


def test_write_suppress_entry(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision(fire=False, reason="below_thresholds"))
    entry = json.loads(log.read_text(encoding="utf-8").splitlines()[0])
    assert entry["decision"] == "suppress"
    assert entry["reason"] == "below_thresholds"


def test_read_last_n_returns_recent(tmp_path):
    log = tmp_path / "trigger.jsonl"
    for i in range(5):
        write_entry(log, _decision(reason=f"reason-{i}"))
    last_three = read_last_n(log, 3)
    assert len(last_three) == 3
    assert [e["reason"] for e in last_three] == ["reason-2", "reason-3", "reason-4"]


def test_read_last_n_more_than_present_returns_all(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision(reason="only-one"))
    out = read_last_n(log, 100)
    assert len(out) == 1
    assert out[0]["reason"] == "only-one"


def test_read_missing_log_returns_empty(tmp_path):
    assert read_last_n(tmp_path / "does-not-exist.jsonl", 5) == []


def test_read_zero_or_negative_returns_empty(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision())
    assert read_last_n(log, 0) == []
    assert read_last_n(log, -1) == []


def test_corrupt_line_is_skipped(tmp_path):
    """Defensive: a partial/corrupt JSONL line never crashes the reader."""
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision(reason="good-1"))
    # Inject a corrupt line manually
    with log.open("a", encoding="utf-8") as f:
        f.write("{this is not valid json\n")
    write_entry(log, _decision(reason="good-2"))
    out = read_last_n(log, 10)
    # The corrupt line is skipped; the two valid entries are returned.
    reasons = [e["reason"] for e in out]
    assert "good-1" in reasons
    assert "good-2" in reasons
    assert len(out) == 2


def test_blank_lines_skipped(tmp_path):
    log = tmp_path / "trigger.jsonl"
    write_entry(log, _decision(reason="r1"))
    with log.open("a", encoding="utf-8") as f:
        f.write("\n\n  \n")
    write_entry(log, _decision(reason="r2"))
    out = read_last_n(log, 10)
    assert [e["reason"] for e in out] == ["r1", "r2"]
