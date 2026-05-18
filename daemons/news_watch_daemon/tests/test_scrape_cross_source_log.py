"""Append-only JSONL writer tests for cross-source observations.

Pass C end / Pass D foundation (2026-05-17). Mirrors the
trigger_log.py test shape — pure I/O surface, no network, no DB.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from news_watch_daemon.scrape.cross_source_log import (
    read_observations,
    write_observation,
)


# ---------- write_observation: basic shape ----------


def test_write_creates_file_and_directory(tmp_path):
    log = tmp_path / "deep" / "nested" / "cross_source.jsonl"
    write_observation(
        log,
        dedupe_hash="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        first_source="telegram:real_DonaldJTrump",
        first_observed_at_unix=1779000000,
        second_source="telegram:TrumpTruthSocial_Alert",
        second_observed_at_unix=1779000060,
        headline="MAJOR ANNOUNCEMENT!!",
    )
    assert log.is_file()
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["dedupe_hash"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert record["first_source"] == "telegram:real_DonaldJTrump"
    assert record["second_source"] == "telegram:TrumpTruthSocial_Alert"
    assert record["first_observed_at_unix"] == 1779000000
    assert record["second_observed_at_unix"] == 1779000060
    assert record["latency_seconds"] == 60
    assert record["headline"] == "MAJOR ANNOUNCEMENT!!"


def test_write_appends_rather_than_rewrites(tmp_path):
    log = tmp_path / "cs.jsonl"
    for i in range(3):
        write_observation(
            log,
            dedupe_hash=f"hash-{i}",
            first_source="telegram:a",
            first_observed_at_unix=1779000000 + i,
            second_source="telegram:b",
            second_observed_at_unix=1779000010 + i,
            headline=f"event {i}",
        )
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    hashes = [json.loads(line)["dedupe_hash"] for line in lines]
    assert hashes == ["hash-0", "hash-1", "hash-2"]


def test_write_truncates_long_headlines(tmp_path):
    log = tmp_path / "cs.jsonl"
    long_text = "A" * 1000
    write_observation(
        log,
        dedupe_hash="x", first_source="s1", first_observed_at_unix=1, second_source="s2",
        second_observed_at_unix=2, headline=long_text,
    )
    record = json.loads(log.read_text(encoding="utf-8").strip())
    assert len(record["headline"]) == 280


def test_write_handles_none_headline(tmp_path):
    log = tmp_path / "cs.jsonl"
    write_observation(
        log, dedupe_hash="x", first_source="s1", first_observed_at_unix=1,
        second_source="s2", second_observed_at_unix=2, headline=None,  # type: ignore[arg-type]
    )
    record = json.loads(log.read_text(encoding="utf-8").strip())
    assert record["headline"] == ""


def test_write_latency_seconds_never_negative(tmp_path):
    """Clock skew or out-of-order observations could produce a
    second_observed_at < first_observed_at. The log must clamp at 0
    rather than emit a misleading negative latency."""
    log = tmp_path / "cs.jsonl"
    write_observation(
        log, dedupe_hash="x", first_source="s1",
        first_observed_at_unix=2000,  # first
        second_source="s2",
        second_observed_at_unix=1500,  # earlier (clock skew)
        headline="x",
    )
    record = json.loads(log.read_text(encoding="utf-8").strip())
    assert record["latency_seconds"] == 0


def test_write_observed_at_iso_format(tmp_path):
    log = tmp_path / "cs.jsonl"
    when = datetime(2026, 5, 17, 14, 32, 8, tzinfo=timezone.utc)
    write_observation(
        log, dedupe_hash="x", first_source="s1", first_observed_at_unix=1,
        second_source="s2", second_observed_at_unix=2, headline="x",
        now_unix=int(when.timestamp()),
    )
    record = json.loads(log.read_text(encoding="utf-8").strip())
    assert record["observed_at"] == "2026-05-17T14:32:08Z"
    assert record["observed_at_unix"] == int(when.timestamp())


# ---------- read_observations: full-load semantics ----------


def test_read_missing_file_returns_empty(tmp_path):
    assert read_observations(tmp_path / "no_such_file.jsonl") == []


def test_read_skips_malformed_lines(tmp_path):
    log = tmp_path / "cs.jsonl"
    log.write_text(
        '{"dedupe_hash":"a","first_source":"s1"}\n'
        "{not valid json\n"
        '{"dedupe_hash":"b","first_source":"s2"}\n',
        encoding="utf-8",
    )
    records = read_observations(log)
    assert len(records) == 2
    assert [r["dedupe_hash"] for r in records] == ["a", "b"]


def test_read_handles_empty_lines(tmp_path):
    """Blank lines between entries (could happen with manual log
    inspection followed by re-write) must not break the read."""
    log = tmp_path / "cs.jsonl"
    log.write_text(
        '{"dedupe_hash":"a"}\n'
        "\n"
        "   \n"
        '{"dedupe_hash":"b"}\n',
        encoding="utf-8",
    )
    records = read_observations(log)
    assert len(records) == 2


def test_roundtrip_write_then_read(tmp_path):
    log = tmp_path / "cs.jsonl"
    write_observation(
        log, dedupe_hash="d1", first_source="telegram:a",
        first_observed_at_unix=1000, second_source="telegram:b",
        second_observed_at_unix=1042, headline="event one",
    )
    write_observation(
        log, dedupe_hash="d2", first_source="rss:bloomberg",
        first_observed_at_unix=2000, second_source="telegram:a",
        second_observed_at_unix=2150, headline="event two",
    )
    records = read_observations(log)
    assert len(records) == 2
    assert records[0]["dedupe_hash"] == "d1"
    assert records[0]["latency_seconds"] == 42
    assert records[1]["latency_seconds"] == 150
