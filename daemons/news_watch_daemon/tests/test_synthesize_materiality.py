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
            model_used="claude-sonnet-4-6", theses_doc_available=False,
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


# ---------- Follow-up #5 (2026-05-27): discriminated-union archive walk ----------
#
# Regression for the bug discovered live: the archive walk in
# _collect_recent_fingerprints crashed with AttributeError on
# AttentionBrief (.events doesn't exist). Cost: one Pass C brief with
# 6 events on tonight's Iran cluster, lost to the crash. The fix skips
# non-Brief variants of the discriminated union.


def _attention_brief(
    *,
    brief_id: str = "nwd-attn-2026-05-27T22-00-00Z-abcdef12",
    generated_at: str = "2026-05-27T22:00:00Z",
    triggering_term: str = "about",
):
    from news_watch_daemon.attention.brief_schema import AttentionBrief
    from news_watch_daemon.synthesize.brief import Dispatch, SynthesisMetadata
    return AttentionBrief(
        brief_id=brief_id,
        generated_at=generated_at,
        triggering_term=triggering_term,
        term_frequency_window=12,
        term_frequency_prior=2,
        cluster_size=12,
        narrative="Generic preposition; cross_topic_recurrence.",
        source_mix={"telegram:CIG_telegram": 10},
        entities_observed=["Trump"],
        attention_shape="cross_topic_recurrence",
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6", theses_doc_available=False,
        ),
    )


def test_collect_recent_fingerprints_skips_attention_brief(tmp_path):
    """An AttentionBrief in the archive must not crash the dedup-window walk.

    Pre-fix: AttributeError: 'AttentionBrief' object has no attribute
    'events'. Locked-behavior regression — if this test fails, the
    fingerprint-collection loop has lost its isinstance discrimination.
    """
    from news_watch_daemon.synthesize.materiality import _collect_recent_fingerprints
    archive_root = tmp_path / "archive"
    # Seed: ONE attention brief in the same archive partition as a Pass C
    # synthesis would land. The collection walk must skip it cleanly.
    attn = _attention_brief()
    write_brief(archive_root, attn)
    # Now call _collect_recent_fingerprints — this used to crash.
    since_unix = 0  # accept all
    fingerprints, brief_ids = _collect_recent_fingerprints(archive_root, since_unix)
    assert fingerprints == set()
    assert brief_ids == []  # attention brief is not counted


def test_collect_recent_fingerprints_still_collects_pass_c_briefs(tmp_path):
    """A Pass C Brief in the archive must continue to contribute fingerprints.

    Confirms the isinstance discrimination doesn't accidentally swallow
    Pass C briefs (the bucket the dedup logic actually cares about).
    """
    from news_watch_daemon.synthesize.materiality import (
        _collect_recent_fingerprints,
        fingerprint_event,
    )
    archive_root = tmp_path / "archive"
    pass_c = _brief(events=[
        _event(0.7, summary="Iran tested new ballistic missile system", event_id="e1"),
        _event(0.8, summary="China announced new chip export controls",  event_id="e2"),
    ])
    write_brief(archive_root, pass_c)
    fingerprints, brief_ids = _collect_recent_fingerprints(archive_root, 0)
    assert len(fingerprints) == 2
    assert fingerprint_event("Iran tested new ballistic missile system") in fingerprints
    assert fingerprint_event("China announced new chip export controls") in fingerprints
    assert pass_c.brief_id in brief_ids


def test_collect_recent_fingerprints_mixed_archive_correct_subset(tmp_path):
    """Mixed archive: Pass C brief AND attention brief in same partition.

    Reproduces tonight's failure shape exactly. The fix's invariant:
    Pass C fingerprints are collected; attention briefs are skipped
    silently; no crash.
    """
    from news_watch_daemon.synthesize.materiality import (
        _collect_recent_fingerprints,
        fingerprint_event,
    )
    archive_root = tmp_path / "archive"
    # Both briefs in the same YYYY-MM partition (2026-05).
    pass_c = _brief(
        events=[_event(0.7, summary="Iran missile test", event_id="e1")],
        generated_at="2026-05-27T22:50:43Z",
        brief_id="nwd-2026-05-27T22-50-43Z-9b3cfa83",
    )
    attn = _attention_brief(
        generated_at="2026-05-27T23:06:06Z",
        brief_id="nwd-attn-2026-05-27T23-06-06Z-1158cc38",
    )
    write_brief(archive_root, pass_c)
    write_brief(archive_root, attn)
    fingerprints, brief_ids = _collect_recent_fingerprints(archive_root, 0)
    # Only the Pass C brief's event contributes:
    assert fingerprints == {fingerprint_event("Iran missile test")}
    assert brief_ids == [pass_c.brief_id]
