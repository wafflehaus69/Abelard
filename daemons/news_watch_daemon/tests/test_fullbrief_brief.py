"""Pydantic schema tests for FullBriefEnvelope and its nested sub-section models.

Per Full Brief Stage 2a-i (Abelard 2026-05-29). Covers:
- Round-trip via model_dump + model_validate
- extra="forbid" on every sub-section model
- Required-field enforcement
- Bounds (ge/le) on numeric fields
- Literal-type rejection of out-of-set values
- new_brief_id() format pin (matches Brief / AttentionBrief convention)
- Default-value handling for optional fields
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from news_watch_daemon.fullbrief.brief import (
    AttentionCrossing,
    AttentionSynthesisSection,
    ConvergenceInfo,
    CostEnvelope,
    CostPerAttentionBrief,
    CostPerBrief,
    ExecutiveSummary,
    FrequencyDiagnosticCrossingRow,
    FrequencyDiagnosticNearMissRow,
    FrequencyDiagnosticSection,
    FullBriefEnvelope,
    FullBriefEnvelopeHealth,
    PassFFootprint,
    PassFailure,
    StepHealth,
    ThemeEventDigest,
    ThemeSynthesisSection,
    WindowSection,
)


# ---------- factory ----------


def _step_ok(**kwargs) -> StepHealth:
    return StepHealth(status="ok", **kwargs)


def _minimal_envelope_health() -> FullBriefEnvelopeHealth:
    return FullBriefEnvelopeHealth(
        scrape=_step_ok(headlines_inserted=100, sources_failed=0),
        pass_c=_step_ok(),
        pass_e=_step_ok(crossings_count=2),
        convergence_analysis=_step_ok(),
        frequency_diagnostic=_step_ok(),
    )


def _minimal_cost() -> CostEnvelope:
    return CostEnvelope(
        pass_c=None,
        pass_e_briefs=[],
        pass_e_total_usd=0.0,
        total_usd=0.0,
        model="claude-sonnet-4-6",
        rates_as_of="2026-05-28",
    )


def _make_full_brief(
    *,
    brief_id: str | None = None,
    theme_status: str = "ok",
) -> FullBriefEnvelope:
    """Build a minimal-valid FullBriefEnvelope for schema testing."""
    return FullBriefEnvelope(
        brief_id=brief_id or "nwd-fullbrief-2026-05-29T14-32-47Z-abcd1234",
        generated_at="2026-05-29T14:32:47Z",
        window=WindowSection(
            since="2026-05-28T14:32:47Z",
            until="2026-05-29T14:32:47Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative="Test narrative.",
            dominant_themes=["us_iran_escalation"],
            material_event_count=8,
            attention_crossings_count=2,
            orphan_crossings_count=1,
            highest_materiality_score=0.82,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status=theme_status,
            brief_id="nwd-2026-05-29T14-32-47Z-14522f19" if theme_status == "ok" else None,
            narrative="Pass C narrative." if theme_status == "ok" else None,
            themes_covered=["us_iran_escalation"] if theme_status == "ok" else [],
            no_trigger_reason="quiet_window" if theme_status == "no_trigger" else None,
        ),
        attention_synthesis=AttentionSynthesisSection(status="ok"),
        frequency_diagnostic=FrequencyDiagnosticSection(
            diagnostic_note="Standard near-miss table."
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=24,
            cross_language_event_merges=1,
            attention_crossings_enabled_by_pass_f=["putin"],
        ),
        envelope_health=_minimal_envelope_health(),
        cost=_minimal_cost(),
    )


# ---------- WindowSection ----------


def test_window_section_valid():
    w = WindowSection(since="a", until="b", duration_hours=24)
    assert w.duration_hours == 24


def test_window_section_duration_hours_bounds():
    """duration_hours must be in [1, 168] per the synthesize CLI's bound."""
    with pytest.raises(ValidationError):
        WindowSection(since="a", until="b", duration_hours=0)
    with pytest.raises(ValidationError):
        WindowSection(since="a", until="b", duration_hours=169)


def test_window_section_extra_forbid():
    with pytest.raises(ValidationError):
        WindowSection(since="a", until="b", duration_hours=24, extra_field="x")


# ---------- ExecutiveSummary ----------


def test_executive_summary_valid_minimal():
    es = ExecutiveSummary(
        narrative="x",
        material_event_count=0,
        attention_crossings_count=0,
        orphan_crossings_count=0,
    )
    assert es.dominant_themes == []
    assert es.highest_materiality_score is None


def test_executive_summary_materiality_score_bounds():
    with pytest.raises(ValidationError):
        ExecutiveSummary(
            narrative="x", material_event_count=0, attention_crossings_count=0,
            orphan_crossings_count=0, highest_materiality_score=1.1,
        )


def test_executive_summary_counts_nonneg():
    with pytest.raises(ValidationError):
        ExecutiveSummary(
            narrative="x", material_event_count=-1,
            attention_crossings_count=0, orphan_crossings_count=0,
        )


# ---------- ThemeEventDigest ----------


def test_theme_event_digest_direction_literal_set():
    """direction must be one of confirm/break/ambiguous or None."""
    for d in ("confirm", "break", "ambiguous", None):
        ev = ThemeEventDigest(
            event_id="e1", headline_summary="x", themes=["t"],
            materiality_score=0.5, direction=d, source_count=1,
        )
        assert ev.direction == d
    with pytest.raises(ValidationError):
        ThemeEventDigest(
            event_id="e1", headline_summary="x", themes=["t"],
            materiality_score=0.5, direction="confused", source_count=1,
        )


def test_theme_event_digest_materiality_in_range():
    with pytest.raises(ValidationError):
        ThemeEventDigest(
            event_id="e", headline_summary="x", themes=[],
            materiality_score=1.5, source_count=0,
        )


# ---------- ThemeSynthesisSection ----------


def test_theme_synthesis_status_literal_set():
    """status must be one of ok/no_trigger/failed."""
    for s in ("ok", "no_trigger", "failed"):
        ThemeSynthesisSection(status=s)
    with pytest.raises(ValidationError):
        ThemeSynthesisSection(status="weird")


def test_theme_synthesis_no_trigger_shape():
    """no_trigger: brief_id null, no_trigger_reason populated."""
    ts = ThemeSynthesisSection(
        status="no_trigger", no_trigger_reason="quiet_window",
    )
    assert ts.brief_id is None
    assert ts.brief_path is None
    assert ts.events == []
    assert ts.no_trigger_reason == "quiet_window"


def test_theme_synthesis_failed_shape():
    ts = ThemeSynthesisSection(
        status="failed", failure_reason="LLM error: rate limit",
    )
    assert ts.brief_id is None
    assert ts.failure_reason.startswith("LLM error")


# ---------- AttentionSynthesisSection + AttentionCrossing ----------


def test_attention_crossing_full_shape():
    c = AttentionCrossing(
        term="hormuz",
        freq_window=14,
        freq_prior=1,
        delta_ratio=14.0,
        shape="multi_source_convergence",
        attention_brief_id="nwd-attn-2026-05-29T14-31-21Z-x",
        attention_brief_path="/path/to/brief.json",
        convergence=ConvergenceInfo(
            status="convergent",
            converges_with=["evt-1"],
        ),
        llm_read_summary="first ~280 chars",
    )
    assert c.convergence.status == "convergent"


def test_attention_synthesis_empty_crossings_ok():
    """status=ok + zero crossings is valid (very quiet window)."""
    s = AttentionSynthesisSection(status="ok")
    assert s.crossings == []


def test_attention_synthesis_failure_shape():
    s = AttentionSynthesisSection(
        status="failed",
        failure_reason="counter exception: ...",
    )
    assert s.failure_reason is not None


# ---------- ConvergenceInfo ----------


def test_convergence_info_status_literal():
    for s in ("convergent", "orphan", "unknown"):
        ConvergenceInfo(status=s)
    with pytest.raises(ValidationError):
        ConvergenceInfo(status="maybe")


def test_convergence_info_orphan_with_reason():
    ci = ConvergenceInfo(status="orphan", orphan_reason="term not in any event")
    assert ci.converges_with == []
    assert ci.orphan_reason == "term not in any event"


# ---------- FrequencyDiagnosticSection ----------


def test_freq_diagnostic_near_miss_reason_literal():
    """reason_not_crossed Literal set."""
    for r in ("below_window_min", "above_prior_max"):
        FrequencyDiagnosticNearMissRow(
            term="x", freq_window=10, freq_prior=5,
            delta_ratio=2.0, reason_not_crossed=r,
        )
    with pytest.raises(ValidationError):
        FrequencyDiagnosticNearMissRow(
            term="x", freq_window=10, freq_prior=5,
            delta_ratio=2.0, reason_not_crossed="too_loud",
        )


def test_freq_diagnostic_section_defaults():
    fds = FrequencyDiagnosticSection(diagnostic_note="standard")
    assert fds.threshold_note is None
    assert fds.crossings == []
    assert fds.near_misses == []


def test_freq_diagnostic_threshold_note_populated_at_non_24h():
    """Adjustment 2 schema: threshold_note Optional populates at non-24h."""
    fds = FrequencyDiagnosticSection(
        diagnostic_note="standard",
        threshold_note="Pass E thresholds tuned for 24h; window=6h.",
    )
    assert fds.threshold_note is not None


# ---------- PassFFootprint ----------


def test_pass_f_footprint_non_negative_counts():
    pf = PassFFootprint(
        translated_rows_in_window=0,
        cross_language_event_merges=0,
    )
    assert pf.attention_crossings_enabled_by_pass_f == []
    with pytest.raises(ValidationError):
        PassFFootprint(
            translated_rows_in_window=-1,
            cross_language_event_merges=0,
        )


# ---------- StepHealth + EnvelopeHealth ----------


def test_step_health_status_literal_set():
    for s in ("ok", "failed", "skipped"):
        StepHealth(status=s)
    with pytest.raises(ValidationError):
        StepHealth(status="meh")


def test_envelope_health_requires_all_five_steps():
    """All 5 steps are required — missing one is a schema violation."""
    with pytest.raises(ValidationError):
        FullBriefEnvelopeHealth(   # type: ignore[call-arg]
            scrape=StepHealth(status="ok"),
            pass_c=StepHealth(status="ok"),
            pass_e=StepHealth(status="ok"),
            convergence_analysis=StepHealth(status="ok"),
            # missing frequency_diagnostic
        )


# ---------- CostEnvelope ----------


def test_cost_envelope_pass_c_none_valid():
    """Option A discipline: pass_c=None when Pass C didn't run."""
    ce = CostEnvelope(
        pass_c=None, pass_e_briefs=[],
        pass_e_total_usd=0.0, total_usd=0.0,
        model="claude-sonnet-4-6", rates_as_of="2026-05-28",
    )
    assert ce.pass_c is None


def test_cost_envelope_with_populated_pass_c():
    ce = CostEnvelope(
        pass_c=CostPerBrief(
            input_tokens=2991, output_tokens=2125,
            cache_creation_tokens=2060, cache_read_tokens=0,
            usd=0.0518,
        ),
        pass_e_briefs=[CostPerAttentionBrief(
            attention_brief_id="nwd-attn-x",
            input_tokens=3178, output_tokens=804,
            cache_creation_tokens=1834, cache_read_tokens=0,
            usd=0.0293,
        )],
        pass_e_total_usd=0.0293,
        total_usd=0.0811,
        model="claude-sonnet-4-6",
        rates_as_of="2026-05-28",
    )
    assert ce.pass_c.input_tokens == 2991
    assert len(ce.pass_e_briefs) == 1


def test_cost_envelope_usd_nonneg():
    with pytest.raises(ValidationError):
        CostEnvelope(
            pass_c=None, pass_e_briefs=[], pass_e_total_usd=-0.01,
            total_usd=0.0, model="x", rates_as_of="y",
        )


# ---------- PassFailure ----------


def test_pass_failure_shape():
    pf = PassFailure(step="pass_c", reason="LLM 429", recovered=True)
    assert pf.recovered is True


# ---------- FullBriefEnvelope top-level ----------


def test_full_brief_envelope_brief_type_default_full_brief():
    """brief_type defaults to 'full_brief' Literal — discriminator field."""
    fb = _make_full_brief()
    assert fb.brief_type == "full_brief"


def test_full_brief_envelope_brief_type_literal_locked():
    """brief_type Literal is closed — can't be overridden to a different value."""
    with pytest.raises(ValidationError):
        FullBriefEnvelope(
            brief_id="nwd-fullbrief-x",
            brief_type="theme_event",   # type: ignore[arg-type]
            generated_at="t",
            window=WindowSection(since="a", until="b", duration_hours=24),
            executive_summary=ExecutiveSummary(
                narrative="x", material_event_count=0,
                attention_crossings_count=0, orphan_crossings_count=0,
            ),
            theme_synthesis=ThemeSynthesisSection(status="no_trigger"),
            attention_synthesis=AttentionSynthesisSection(status="ok"),
            frequency_diagnostic=FrequencyDiagnosticSection(diagnostic_note="x"),
            pass_f_footprint=PassFFootprint(
                translated_rows_in_window=0, cross_language_event_merges=0,
            ),
            envelope_health=_minimal_envelope_health(),
            cost=_minimal_cost(),
        )


def test_full_brief_envelope_extra_forbid():
    """extra="forbid" — unknown top-level keys rejected."""
    fb_dict = _make_full_brief().model_dump(mode="json")
    fb_dict["unexpected_key"] = "x"
    with pytest.raises(ValidationError):
        FullBriefEnvelope.model_validate(fb_dict)


def test_full_brief_envelope_round_trip():
    """model_dump -> model_validate round-trip preserves all fields."""
    fb = _make_full_brief()
    serialized = fb.model_dump(mode="json")
    restored = FullBriefEnvelope.model_validate(serialized)
    assert restored == fb


def test_full_brief_envelope_round_trip_no_trigger_case():
    """no_trigger variant round-trips with brief_id null."""
    fb = _make_full_brief(theme_status="no_trigger")
    serialized = fb.model_dump(mode="json")
    restored = FullBriefEnvelope.model_validate(serialized)
    assert restored.theme_synthesis.status == "no_trigger"
    assert restored.theme_synthesis.brief_id is None


# ---------- new_brief_id() format ----------


_BRIEF_ID_RE = re.compile(
    r"^nwd-fullbrief-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z-[0-9a-f]{8}$"
)


def test_new_brief_id_format_matches_convention():
    """Format: nwd-fullbrief-{ISO8601-dashed}-{8char_hex}."""
    bid = FullBriefEnvelope.new_brief_id()
    assert _BRIEF_ID_RE.match(bid), f"unexpected format: {bid}"


def test_new_brief_id_explicit_datetime():
    """when= kwarg lets tests pin a known timestamp."""
    when = datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc)
    bid = FullBriefEnvelope.new_brief_id(when=when)
    assert bid.startswith("nwd-fullbrief-2026-05-29T14-32-47Z-")
    # Suffix is 8 hex chars
    suffix = bid.rsplit("-", 1)[1]
    assert len(suffix) == 8
    assert all(c in "0123456789abcdef" for c in suffix)


def test_new_brief_id_naive_datetime_treated_as_utc():
    """Tz-naive datetimes get UTC by convention (matches Brief.new_brief_id)."""
    naive = datetime(2026, 5, 29, 14, 32, 47)
    bid = FullBriefEnvelope.new_brief_id(when=naive)
    assert "2026-05-29T14-32-47Z" in bid


def test_new_brief_id_unique_across_invocations():
    """8-char hex suffix gives enough entropy to disambiguate same-second mints."""
    ids = {FullBriefEnvelope.new_brief_id() for _ in range(20)}
    assert len(ids) == 20   # all unique
