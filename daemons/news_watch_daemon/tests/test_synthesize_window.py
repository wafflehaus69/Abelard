"""Pure-callable tests for synthesize_window + SynthesizeResult.

Per Full Brief Stage 2a-i sub-step B (Abelard 2026-05-29). Focused on
shape + discriminator invariants of the pure callable; the CLI envelope
behavior is already covered by test_cli_synthesize.py.
"""

from __future__ import annotations

import sqlite3
from dataclasses import fields
from pathlib import Path

import pytest

from news_watch_daemon.synthesize.synthesize import SynthesizeResult


# ---------- SynthesizeResult shape pins ----------


def test_synthesize_result_is_frozen_dataclass():
    """Frozen so downstream callers can hash + rely on immutability."""
    sr = SynthesizeResult(
        status="no_trigger",
        window_since_unix=100,
        window_until_unix=200,
    )
    with pytest.raises(Exception):
        sr.status = "synthesized"   # type: ignore[misc]


def test_synthesize_result_status_field_present():
    """Discriminator field is named `status` per Stage 2a-i design."""
    field_names = {f.name for f in fields(SynthesizeResult)}
    assert "status" in field_names


def test_synthesize_result_required_fields_have_no_defaults():
    """status + window_since_unix + window_until_unix are required.
    Pins the contract that these are always populated."""
    # Missing window_since_unix should raise (no default)
    with pytest.raises(TypeError, match="window_since_unix"):
        SynthesizeResult(status="dry_run", window_until_unix=200)   # type: ignore[call-arg]


def test_synthesize_result_no_trigger_shape():
    """no_trigger variant: brief/metadata/brief_path None, reason populated."""
    sr = SynthesizeResult(
        status="no_trigger",
        window_since_unix=100,
        window_until_unix=200,
        reason="gate: no themes crossed threshold",
        trigger_decision_fire=False,
        trigger_decision_reason="gate: no themes crossed threshold",
    )
    assert sr.brief is None
    assert sr.metadata is None
    assert sr.brief_path is None
    assert sr.trigger_decision_fire is False


def test_synthesize_result_archive_failed_preserves_metadata():
    """Stage 1 closing flag discipline: archive_failed must still carry
    metadata so cost telemetry isn't lost on disk-write failure."""
    # Construction shape is permissive — actual archive_failed instances
    # come from synthesize_window's internal flow. We just verify the
    # dataclass supports brief/metadata populated + brief_path=None.
    # Using minimal mock objects (duck-typed):
    class _FakeMd:
        model_used = "claude-sonnet-4-6"
        input_tokens = 1000
        output_tokens = 500
        cache_creation_input_tokens = 0
        cache_read_input_tokens = 0

    sr = SynthesizeResult(
        status="archive_failed",
        window_since_unix=100,
        window_until_unix=200,
        brief=None,   # Brief object — minimal mock would need full Pydantic
        metadata=_FakeMd(),   # type: ignore[arg-type]
        brief_path=None,
        reason="disk full",
    )
    assert sr.metadata is not None
    assert sr.brief_path is None


def test_synthesize_result_status_literal_values():
    """All five status values per Stage 2a-i design accepted by construction.
    (Literal enforcement is at type-check time, but the constructor accepts
    all valid strings.)"""
    for status in (
        "synthesized", "no_trigger", "synthesis_failed",
        "archive_failed", "dry_run",
    ):
        sr = SynthesizeResult(
            status=status,    # type: ignore[arg-type]
            window_since_unix=100,
            window_until_unix=200,
        )
        assert sr.status == status


# ---------- synthesize_window pre-flight (no-client) path ----------


def test_synthesize_window_returns_synthesis_failed_when_client_is_none_and_not_dry_run():
    """Pre-flight: anthropic_client=None + dry_run=False -> synthesis_failed
    with diagnostic reason. Doesn't require any other mocks because the
    function exits before touching conn or DB."""
    from news_watch_daemon.synthesize.synthesize import synthesize_window

    class _DummyCfg:
        class trigger_gate: ...
        class synthesis: ...
        class alert_sink: ...

    # We need to pass a real-ish conn since the function signature expects it,
    # but we won't get to the DB query — the no-client guard fires first.
    conn = sqlite3.connect(":memory:")
    try:
        result = synthesize_window(
            conn=conn,
            active_themes=[],
            brief_archive_path=Path("/tmp/never-used"),
            trigger_log_path=Path("/tmp/never-used.log"),
            theses_path=None,
            synth_cfg=_DummyCfg(),    # type: ignore[arg-type]
            anthropic_client=None,
            sink_factory=None,
            window_hours=24,
            dry_run=False,
        )
        assert result.status == "synthesis_failed"
        assert "anthropic_client is None" in (result.reason or "")
        assert result.brief is None
        assert result.metadata is None
    finally:
        conn.close()
