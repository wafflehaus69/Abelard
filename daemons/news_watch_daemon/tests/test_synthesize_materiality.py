"""Materiality gate tests — threshold + dedup against recent archive."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from news_watch_daemon.synthesize.archive import write_brief
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    Event,
    SourceHeadline,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)
from news_watch_daemon.synthesize.materiality import (
    MaterialityDecision,
    evaluate_materiality,
    fingerprint_event,
)


# ---------- helpers ----------


def _brief(
    *,
    events: list[Event],
    generated_at: str = "2026-05-13T14:32:08Z",
    brief_id: str = "nwd-2026-05-13T14-32-08Z-aaaaaaaa",
    narrative: str = "n",
) -> Brief:
    return Brief(
        brief_id=brief_id,
        generated_at=generated_at,
        trigger=Trigger(type="event", reason="t",
                        window=TriggerWindow(since="a", until="b")),
        themes_covered=["t1"],
        events=events,
        narrative=narrative,
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-7", theses_doc_available=False,
        ),
    )


def _event(
    score: float,
    summary: str = "Iran tested new ballistic missile system",
    event_id: str = "evt-1",
) -> Event:
    return Event(
        event_id=event_id,
        headline_summary=summary,
        themes=["t1"],
        materiality_score=score,
    )


# ---------- fingerprint stability ----------


def test_fingerprint_stable_for_same_summary():
    fp1 = fingerprint_event("Iran tested new ballistic missile system")
    fp2 = fingerprint_event("Iran tested new ballistic missile system")
    assert fp1 == fp2


def test_fingerprint_normalized_for_case_punctuation():
    """Reuses Pass A dedup_hash normalization; case-insensitive, punctuation-stripped."""
    fp1 = fingerprint_event("Iran tested new ballistic missile system.")
    fp2 = fingerprint_event("IRAN TESTED NEW BALLISTIC MISSILE SYSTEM!")
    assert fp1 == fp2


def test_fingerprint_distinguishes_different_summaries():
    fp1 = fingerprint_event("Iran tested missile")
    fp2 = fingerprint_event("Fed cuts rates")
    assert fp1 != fp2


# ---------- empty / no events ----------


def test_no_events_suppresses(tmp_path):
    brief = _brief(events=[])
    d = evaluate_materiality(
        brief, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "archive",
    )
    assert d.dispatch is False
    assert d.reason == "no_events"


# ---------- threshold ----------


def test_all_events_below_threshold_suppresses(tmp_path):
    brief = _brief(events=[_event(0.3), _event(0.5, event_id="e2", summary="X")])
    d = evaluate_materiality(
        brief, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "archive",
    )
    assert d.dispatch is False
    assert d.reason == "below_materiality_threshold"
    assert d.above_threshold_count == 0


def test_event_exactly_at_threshold_passes(tmp_path):
    brief = _brief(events=[_event(0.55)])
    d = evaluate_materiality(
        brief, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "archive",
    )
    assert d.dispatch is True
    assert d.reason == "above_threshold"
    assert d.above_threshold_count == 1
    assert d.new_events_count == 1


def test_event_above_threshold_dispatches_empty_archive(tmp_path):
    brief = _brief(events=[_event(0.9)])
    d = evaluate_materiality(
        brief, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "archive",
    )
    assert d.dispatch is True
    assert d.new_events_count == 1


# ---------- dedup against recent archive ----------


def test_all_events_dedup_against_recent_suppresses(tmp_path):
    archive = tmp_path / "archive"
    # Plant a recent brief whose event covers the same summary.
    now = int(datetime.now(timezone.utc).timestamp())
    recent = _brief(
        events=[_event(0.9, summary="Iran tested new missile")],
        generated_at=datetime.fromtimestamp(now - 3600, tz=timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%SZ"),
        brief_id="nwd-2026-05-13T13-32-08Z-bbbbbbbb",
    )
    write_brief(archive, recent)

    # New brief covers the same event.
    incoming = _brief(events=[_event(0.9, summary="Iran tested new missile")])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=archive, now_unix=now,
    )
    assert d.dispatch is False
    assert d.reason == "dedup_recent"
    assert d.above_threshold_count == 1
    assert d.new_events_count == 0
    assert "nwd-2026-05-13T13-32-08Z-bbbbbbbb" in d.deduped_against_brief_ids


def test_some_events_dedup_some_new_dispatches(tmp_path):
    """If at least one event is new, dispatch."""
    archive = tmp_path / "archive"
    now = int(datetime.now(timezone.utc).timestamp())
    recent = _brief(
        events=[_event(0.9, summary="Iran tested new missile", event_id="e-old")],
        generated_at=datetime.fromtimestamp(now - 3600, tz=timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%SZ"),
        brief_id="nwd-2026-05-13T13-32-08Z-bbbbbbbb",
    )
    write_brief(archive, recent)

    incoming = _brief(events=[
        _event(0.9, summary="Iran tested new missile", event_id="e1"),  # dupe
        _event(0.9, summary="Fed signals emergency rate cut", event_id="e2"),  # new
    ])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=archive, now_unix=now,
    )
    assert d.dispatch is True
    assert d.reason == "above_threshold"
    assert d.above_threshold_count == 2
    assert d.new_events_count == 1


def test_dedup_window_respects_timestamp_boundary(tmp_path):
    """Briefs older than the window are NOT consulted for dedup."""
    archive = tmp_path / "archive"
    now = int(datetime.now(timezone.utc).timestamp())
    # Brief from 12 hours ago — outside a 6-hour window.
    old = _brief(
        events=[_event(0.9, summary="Iran tested new missile")],
        generated_at=datetime.fromtimestamp(now - 12 * 3600, tz=timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%SZ"),
        brief_id="nwd-2026-05-13T02-32-08Z-cccccccc",
    )
    write_brief(archive, old)

    incoming = _brief(events=[_event(0.9, summary="Iran tested new missile")])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=archive, now_unix=now,
    )
    # Old brief is outside window → not counted as dedup → dispatches.
    assert d.dispatch is True
    assert d.new_events_count == 1


def test_dedup_only_considers_events_above_threshold(tmp_path):
    """Sub-threshold events in this brief don't even reach dedup check.

    Verifies the algorithm order: threshold filter happens before
    dedup scan.
    """
    archive = tmp_path / "archive"
    now = int(datetime.now(timezone.utc).timestamp())
    # Recent brief with the same summary
    recent = _brief(
        events=[_event(0.9, summary="Routine event")],
        generated_at=datetime.fromtimestamp(now - 3600, tz=timezone.utc)
                              .strftime("%Y-%m-%dT%H:%M:%SZ"),
        brief_id="nwd-2026-05-13T13-32-08Z-bbbbbbbb",
    )
    write_brief(archive, recent)

    # Incoming has only a sub-threshold event with that same summary.
    incoming = _brief(events=[_event(0.2, summary="Routine event")])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=archive, now_unix=now,
    )
    # Should suppress as below_threshold, not as dedup_recent
    # (threshold filter happens first).
    assert d.dispatch is False
    assert d.reason == "below_materiality_threshold"


# ---------- archive scan resilience ----------


def test_missing_archive_root_treated_as_empty(tmp_path):
    """No archive directory → dedup scan finds nothing → dispatches."""
    brief = _brief(events=[_event(0.9)])
    d = evaluate_materiality(
        brief, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "nonexistent",
    )
    assert d.dispatch is True


def test_corrupt_brief_in_archive_skipped(tmp_path):
    """Unreadable brief in archive doesn't crash the gate."""
    archive = tmp_path / "archive"
    partition = archive / "2026-05"
    partition.mkdir(parents=True)
    bid = "nwd-2026-05-13T13-32-08Z-bbbbbbbb"
    (partition / f"{bid}.json").write_text("{not valid json", encoding="utf-8")

    incoming = _brief(events=[_event(0.9)])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=archive,
    )
    # Skipped + dispatched.
    assert d.dispatch is True


# ---------- decision payload ----------


def test_decision_carries_above_threshold_count(tmp_path):
    incoming = _brief(events=[
        _event(0.6, summary="event one", event_id="e1"),
        _event(0.7, summary="event two", event_id="e2"),
        _event(0.3, summary="event three", event_id="e3"),  # below
    ])
    d = evaluate_materiality(
        incoming, threshold=0.55, dedup_window_hours=6,
        archive_root=tmp_path / "archive",
    )
    assert d.above_threshold_count == 2
    assert d.new_events_count == 2
