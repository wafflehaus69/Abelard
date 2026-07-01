"""Render-layer tests for FullBriefEnvelope.

Per Mando's Stage 2b-i forward-guidance:
  - Structural assertions over snapshot tests (snapshot churn-fatigue
    erodes test discipline; structural pins survive formatting changes)
  - Three data-shape fixtures (clean / saturated-prior / no-trigger) per
    Adjustment 3 — each exercises a distinct rendering branch
  - Verify orphan-first ordering, theses warning at section header,
    near-miss soft cap + truncation footer, threshold_note in
    frequency_diagnostic header when populated

The three data shapes are not redundant. Each tests a rendering branch
that the others don't reach:
  - Clean: standard path. Orphan section present, theses warning surfaces,
    crossings + events render normally.
  - Saturated-prior: noise crossings classified as orphan, signal lives
    in near-miss long tail (Δ-ratio < 1.0 terms). >50 near-misses to
    trigger soft cap + truncation footer.
  - No-trigger: Pass C did not fire, narrative reads Q2 informational
    text, attention may also be empty. Tests the "quiet day" branch
    that neither other shape reaches.
"""

from __future__ import annotations

import re

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
from news_watch_daemon.fullbrief.render import (
    NEAR_MISS_RENDER_CAP,
    render_full_brief,
)


# ---------- shared mini-fixtures ----------


def _step_ok(**kwargs) -> StepHealth:
    return StepHealth(status="ok", **kwargs)


def _envelope_health(**overrides) -> FullBriefEnvelopeHealth:
    defaults = {
        "scrape": _step_ok(headlines_inserted=100, sources_failed=0),
        "pass_c": _step_ok(),
        "pass_e": _step_ok(crossings_count=5),
        "convergence_analysis": _step_ok(),
        "frequency_diagnostic": _step_ok(),
    }
    defaults.update(overrides)
    return FullBriefEnvelopeHealth(**defaults)


def _basic_cost() -> CostEnvelope:
    return CostEnvelope(
        pass_c=CostPerBrief(
            input_tokens=3000, output_tokens=2000,
            cache_creation_tokens=2000, cache_read_tokens=0,
            usd=0.0518,
        ),
        pass_e_briefs=[CostPerAttentionBrief(
            attention_brief_id="nwd-attn-x",
            input_tokens=3000, output_tokens=800,
            cache_creation_tokens=1834, cache_read_tokens=0,
            usd=0.0290,
        )],
        pass_e_total_usd=0.0290,
        total_usd=0.0808,
        model="claude-sonnet-4-6",
        rates_as_of="2026-05-28",
    )


def _crossing(
    *,
    term: str,
    shape: str = "multi_source_convergence",
    status: str = "convergent",
    converges_with: list[str] | None = None,
    freq_window: int = 12,
    freq_prior: int = 2,
    summary: str = "Sample attention narrative paragraph.",
) -> AttentionCrossing:
    return AttentionCrossing(
        term=term,
        freq_window=freq_window,
        freq_prior=freq_prior,
        delta_ratio=freq_window / max(freq_prior, 1),
        shape=shape,
        attention_brief_id=f"nwd-attn-x-{term}",
        attention_brief_path=f"/fake/{term}.json",
        convergence=ConvergenceInfo(
            status=status,   # type: ignore[arg-type]
            converges_with=converges_with or [],
            orphan_reason=(
                "term crossed threshold but no Pass C event contains it"
                if status == "orphan" else None
            ),
        ),
        llm_read_summary=summary,
    )


def _event(
    *,
    event_id: str,
    materiality: float = 0.7,
    direction: str = "confirm",
    themes: list[str] | None = None,
    headline_summary: str = "Some event happened.",
) -> ThemeEventDigest:
    return ThemeEventDigest(
        event_id=event_id,
        headline_summary=headline_summary,
        themes=themes or ["us_iran_escalation"],
        materiality_score=materiality,
        direction=direction,   # type: ignore[arg-type]
        source_count=2,
        thesis_links=[],
    )


def _near_miss_row(
    *,
    term: str,
    freq_window: int,
    freq_prior: int,
    reason: str = "above_prior_max",
) -> FrequencyDiagnosticNearMissRow:
    return FrequencyDiagnosticNearMissRow(
        term=term,
        freq_window=freq_window,
        freq_prior=freq_prior,
        delta_ratio=freq_window / max(freq_prior, 1),
        reason_not_crossed=reason,   # type: ignore[arg-type]
    )


# ---------- the three data-shape fixtures ----------


def _make_clean_envelope() -> FullBriefEnvelope:
    """Cycle 1 shape per Adjustment 3:
       5 attention crossings, 8 Pass C events, 1 orphan, theses unwired."""
    crossings = [
        _crossing(term="iranian", status="convergent", converges_with=["evt-1", "evt-2"],
                  freq_window=12, freq_prior=2),
        _crossing(term="ceasefire", status="convergent", converges_with=["evt-1"],
                  shape="multi_source_convergence", freq_window=10, freq_prior=0),
        _crossing(term="putin", status="convergent", converges_with=["evt-5"],
                  shape="single_event_dominant", freq_window=10, freq_prior=0),
        _crossing(term="control", status="convergent", converges_with=["evt-1"],
                  shape="single_event_dominant", freq_window=12, freq_prior=0),
        _crossing(term="south", status="orphan", shape="cross_topic_recurrence",
                  freq_window=13, freq_prior=2,
                  summary="Polysemous noise — geographic prefix across many regions."),
    ]
    events = [
        _event(event_id=f"evt-{i}", materiality=0.88 - 0.05 * i, direction="confirm")
        for i in range(1, 9)
    ]
    near_misses = [
        _near_miss_row(term="iran", freq_window=68, freq_prior=28),
        _near_miss_row(term="trump", freq_window=35, freq_prior=21),
        _near_miss_row(term="hormuz", freq_window=23, freq_prior=6),
        _near_miss_row(term="new", freq_window=23, freq_prior=14),
        _near_miss_row(term="deal", freq_window=20, freq_prior=13),
    ]
    crossings_table = [
        FrequencyDiagnosticCrossingRow(
            term=c.term, freq_window=c.freq_window, freq_prior=c.freq_prior,
            shape=c.shape, convergence=c.convergence.status,   # type: ignore[arg-type]
        )
        for c in crossings
    ]
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-05-28T12-55-17Z-aaaaaaaa",
        generated_at="2026-05-28T12:55:17Z",
        window=WindowSection(
            since="2026-05-27T12:55:17Z",
            until="2026-05-28T12:55:17Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative=(
                "The dominant development this cycle is an active and reciprocal "
                "U.S.-Iran kinetic exchange near the Strait of Hormuz."
            ),
            dominant_themes=["us_iran_escalation", "russia_ukraine_war"],
            material_event_count=5,
            attention_crossings_count=5,
            orphan_crossings_count=1,
            highest_materiality_score=0.88,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status="ok",
            brief_id="nwd-2026-05-28T12-55-17Z-cccccccc",
            narrative="Theme-specific narrative.",
            themes_covered=["us_iran_escalation", "russia_ukraine_war"],
            events=events,
            direction_tally={"confirm": 2, "ambiguous": 6, "break": 0},
            theses_doc_available=False,
            theses_doc_warning="NEWS_WATCH_THESES_PATH not set; synthesis ran no-theses variant",
        ),
        attention_synthesis=AttentionSynthesisSection(
            status="ok", crossings=crossings,
        ),
        frequency_diagnostic=FrequencyDiagnosticSection(
            threshold_note=None,
            crossings=crossings_table,
            near_misses=near_misses,
            diagnostic_note="Standard near-miss table.",
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=24,
            cross_language_event_merges=1,
            attention_crossings_enabled_by_pass_f=["putin"],
        ),
        envelope_health=_envelope_health(),
        cost=_basic_cost(),
    )


def _make_saturated_prior_envelope() -> FullBriefEnvelope:
    """Cycle 2 shape per Adjustment 3:
       2 crossings BOTH cross_topic_recurrence noise → both orphan.
       8 Pass C events, ~60 near-misses with top 5-6 saturated (Δ < 1.0).
       Saturated-prior signal lives in near-miss long tail."""
    crossings = [
        _crossing(term="secretary", status="orphan",
                  shape="cross_topic_recurrence", freq_window=11, freq_prior=2,
                  summary="Generic institutional title across 5 cabinet figures."),
        _crossing(term="administration", status="orphan",
                  shape="cross_topic_recurrence", freq_window=10, freq_prior=2,
                  summary="Attribution tag across unrelated Trump admin actions."),
    ]
    events = [_event(event_id=f"evt-{i}", materiality=0.85 - 0.04 * i) for i in range(1, 9)]
    # Saturated near-misses: prior is HIGH, delta_ratio < 1.0 for the top terms.
    saturated = [
        _near_miss_row(term="iran", freq_window=37, freq_prior=75),    # 0.49
        _near_miss_row(term="trump", freq_window=27, freq_prior=40),   # 0.68
        _near_miss_row(term="war", freq_window=14, freq_prior=21),     # 0.67
        _near_miss_row(term="russia", freq_window=13, freq_prior=18),  # 0.72
        _near_miss_row(term="deal", freq_window=16, freq_prior=24),    # 0.67
    ]
    # 55 more near-misses to trigger the 50-row soft cap.
    long_tail = [
        _near_miss_row(term=f"term_{i:03d}", freq_window=10, freq_prior=5)
        for i in range(55)
    ]
    near_misses = saturated + long_tail
    crossings_table = [
        FrequencyDiagnosticCrossingRow(
            term=c.term, freq_window=c.freq_window, freq_prior=c.freq_prior,
            shape=c.shape, convergence=c.convergence.status,   # type: ignore[arg-type]
        )
        for c in crossings
    ]
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-05-29T14-32-47Z-bbbbbbbb",
        generated_at="2026-05-29T14:32:47Z",
        window=WindowSection(
            since="2026-05-28T14:32:47Z",
            until="2026-05-29T14:32:47Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative="Saturated-prior cycle — both crossings classified as noise.",
            dominant_themes=["us_iran_escalation"],
            material_event_count=5,
            attention_crossings_count=2,
            orphan_crossings_count=2,
            highest_materiality_score=0.82,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status="ok",
            brief_id="nwd-2026-05-29T14-32-47Z-dddddddd",
            narrative="Theme narrative.",
            themes_covered=["us_iran_escalation"],
            events=events,
            direction_tally={"confirm": 1, "ambiguous": 7, "break": 0},
            theses_doc_available=False,
            theses_doc_warning="NEWS_WATCH_THESES_PATH not set",
        ),
        attention_synthesis=AttentionSynthesisSection(
            status="ok", crossings=crossings,
        ),
        frequency_diagnostic=FrequencyDiagnosticSection(
            threshold_note=None,
            crossings=crossings_table,
            near_misses=near_misses,
            diagnostic_note="Saturated near-misses — sustained signal in long tail.",
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=56,
            cross_language_event_merges=0,
            attention_crossings_enabled_by_pass_f=[],
        ),
        envelope_health=_envelope_health(),
        cost=_basic_cost(),
    )


def _make_no_trigger_envelope() -> FullBriefEnvelope:
    """Quiet-day shape per Adjustment 3:
       Pass C status=no_trigger, Pass E 0 crossings, ~10 near-misses."""
    near_misses = [
        _near_miss_row(term=f"term_{i:02d}", freq_window=8, freq_prior=3)
        for i in range(10)
    ]
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-05-30T03-00-00Z-cccccccc",
        generated_at="2026-05-30T03:00:00Z",
        window=WindowSection(
            since="2026-05-29T03:00:00Z",
            until="2026-05-30T03:00:00Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative=(
                "Pass C trigger gate did not fire — no theme crossed materiality "
                "threshold this window. This is informational, not an error."
            ),
            dominant_themes=[],
            material_event_count=0,
            attention_crossings_count=0,
            orphan_crossings_count=0,
            highest_materiality_score=None,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status="no_trigger",
            no_trigger_reason="no_theme_crossed_threshold",
        ),
        attention_synthesis=AttentionSynthesisSection(
            status="ok", crossings=[],
        ),
        frequency_diagnostic=FrequencyDiagnosticSection(
            threshold_note=None,
            crossings=[],
            near_misses=near_misses,
            diagnostic_note="Quiet cycle.",
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=2,
            cross_language_event_merges=0,
            attention_crossings_enabled_by_pass_f=[],
        ),
        envelope_health=_envelope_health(pass_e=_step_ok(crossings_count=0)),
        cost=_basic_cost(),
    )


# ---------- T1a: clean cycle rendering ----------


def test_t1a_clean_cycle_renders_all_sections():
    """Clean cycle exercises the standard rendering path.
    Structural assertions over snapshot per Mando's discipline."""
    envelope = _make_clean_envelope()
    out = render_full_brief(envelope)

    # All standard section headers present, in expected order.
    section_order = [
        "FULL BRIEF — ",
        "NARRATIVE",
        "DOMINANT THEMES:",
        "ORPHAN ATTENTION CROSSINGS — review first",
        "THEME-EVENT SYNTHESIS (Pass C)",
        "ATTENTION SYNTHESIS (Pass E)",
        "FREQUENCY DIAGNOSTIC — near-miss terms",
        "PASS F FOOTPRINT",
        "ENVELOPE HEALTH",
    ]
    positions = [out.find(s) for s in section_order]
    assert all(p > -1 for p in positions), \
        f"Missing sections: {[s for s, p in zip(section_order, positions) if p == -1]}"
    # Strictly monotonically increasing positions
    assert positions == sorted(positions), \
        "Section order incorrect"

    # Theses warning at the THEME-EVENT SYNTHESIS section header line.
    theme_header_idx = out.find("THEME-EVENT SYNTHESIS (Pass C)")
    theme_header_line_end = out.find("\n", theme_header_idx)
    theme_header_line = out[theme_header_idx:theme_header_line_end]
    assert "THESES DOC: NOT WIRED" in theme_header_line, \
        f"Theses warning not at section header (line: {theme_header_line!r})"

    # Orphan section contains the orphan term `south`, NOT convergent terms.
    orphan_idx = out.find("ORPHAN ATTENTION CROSSINGS")
    pass_c_idx = out.find("THEME-EVENT SYNTHESIS")
    orphan_block = out[orphan_idx:pass_c_idx]
    assert "south" in orphan_block
    # Convergent terms should NOT be in the orphan block (they appear only
    # in the Pass E section below).
    assert "iranian" not in orphan_block
    assert "putin" not in orphan_block

    # Pass E section has ALL 5 crossings including orphan with ORPHAN tag.
    pass_e_idx = out.find("ATTENTION SYNTHESIS (Pass E)")
    freq_idx = out.find("FREQUENCY DIAGNOSTIC")
    pass_e_block = out[pass_e_idx:freq_idx]
    for term in ("iranian", "ceasefire", "putin", "control", "south"):
        assert term in pass_e_block, f"crossing {term!r} missing from Pass E section"
    assert "ORPHAN" in pass_e_block, "south should be tagged ORPHAN in Pass E section"
    assert "converges with evt-1" in pass_e_block

    # Pass F footprint shows enabled-by-Pass-F crossing
    assert "putin" in out[out.find("PASS F FOOTPRINT"):]
    # No URL match warnings line when url_match_warnings is None
    assert "URL match warnings" not in out

    # Near-miss table present with column headers + rows
    nm_idx = out.find("FREQUENCY DIAGNOSTIC")
    nm_end_idx = out.find("PASS F FOOTPRINT")
    nm_block = out[nm_idx:nm_end_idx]
    assert "Term" in nm_block and "Window" in nm_block and "Prior" in nm_block
    assert "iran" in nm_block
    assert "hormuz" in nm_block
    # NO truncation footer (only 5 rows, well below cap)
    assert "Truncated at" not in nm_block

    # NO threshold_note since window_hours == 24
    assert "thresholds are tuned for 24h" not in out


# ---------- T1b: saturated-prior cycle ----------


def test_t1b_saturated_prior_cycle_orphans_visible_near_miss_long_tail_truncated():
    """Saturated-prior cycle: BOTH crossings classified as orphan (noise).
    Near-miss table exceeds soft cap → truncation footer + full count visible.
    """
    envelope = _make_saturated_prior_envelope()
    out = render_full_brief(envelope)

    # Both noise crossings appear in the orphan section
    orphan_idx = out.find("ORPHAN ATTENTION CROSSINGS")
    pass_c_idx = out.find("THEME-EVENT SYNTHESIS")
    orphan_block = out[orphan_idx:pass_c_idx]
    assert "secretary" in orphan_block
    assert "administration" in orphan_block
    # And both with cross_topic_recurrence shape label
    assert "cross_topic_recurrence" in orphan_block

    # Both classified ORPHAN in the Pass E section
    pass_e_idx = out.find("ATTENTION SYNTHESIS (Pass E)")
    freq_idx = out.find("FREQUENCY DIAGNOSTIC")
    pass_e_block = out[pass_e_idx:freq_idx]
    orphan_count_in_pass_e = pass_e_block.count("ORPHAN")
    assert orphan_count_in_pass_e == 2

    # Near-miss table: saturated terms render with Δ-ratio < 1.0
    nm_idx = out.find("FREQUENCY DIAGNOSTIC")
    nm_end_idx = out.find("PASS F FOOTPRINT")
    nm_block = out[nm_idx:nm_end_idx]
    # All saturated terms present
    for term in ("iran", "trump", "war", "russia", "deal"):
        assert term in nm_block

    # TRUNCATION FOOTER present — > 50 rows
    total_near_misses = len(envelope.frequency_diagnostic.near_misses)
    assert total_near_misses == 60
    assert f"Truncated at {NEAR_MISS_RENDER_CAP} of {total_near_misses}" in nm_block
    assert "frequency_diagnostic.near_misses" in nm_block

    # The 51st row should NOT appear (term_046 indexed from saturated[5:])
    # Saturated has 5 + long_tail of 55 → first 5 saturated + 45 of long_tail
    # render. term_044 is index 44 of long_tail (45th item) = position 50 of
    # the combined list → renders. term_045 (46th) onwards do not.
    assert "term_044" in nm_block
    assert "term_045" not in nm_block


# ---------- T1c: no-trigger quiet cycle ----------


def test_t1c_no_trigger_quiet_cycle_renders_informational_text():
    """No-trigger quiet cycle: Pass C narrative carries Q2 informational text.
    Pass E section shows 0 crossings. Near-miss table still surfaces sustained
    signal even on quiet days."""
    envelope = _make_no_trigger_envelope()
    out = render_full_brief(envelope)

    # NO orphan section (no crossings at all)
    assert "ORPHAN ATTENTION CROSSINGS" not in out

    # Theme synthesis section carries the Q2 informational text
    theme_idx = out.find("THEME-EVENT SYNTHESIS (Pass C)")
    pass_e_idx = out.find("ATTENTION SYNTHESIS (Pass E)")
    theme_block = out[theme_idx:pass_e_idx]
    assert "Pass C trigger gate did not fire" in theme_block
    assert "informational, not an error" in theme_block
    # And the gate reason is surfaced
    assert "no_theme_crossed_threshold" in theme_block

    # Pass E section says no crossings
    freq_idx = out.find("FREQUENCY DIAGNOSTIC")
    pass_e_block = out[pass_e_idx:freq_idx]
    assert "no crossings this cycle" in pass_e_block.lower() or \
           "(no crossings" in pass_e_block

    # Near-miss table still populates even on quiet days
    nm_block = out[freq_idx:out.find("PASS F FOOTPRINT")]
    assert "term_00" in nm_block   # at least one near-miss row
    # No truncation footer at 10 rows
    assert "Truncated at" not in nm_block


# ---------- threshold_note rendering pin (Adjustment 2) ----------


def test_threshold_note_renders_in_freq_diagnostic_header_when_populated():
    """When window_hours != 24, threshold_note populates and renders
    in the FREQUENCY DIAGNOSTIC section header per Adjustment 2."""
    envelope = _make_clean_envelope()
    envelope = envelope.model_copy(update={
        "window": envelope.window.model_copy(update={"duration_hours": 6}),
        "frequency_diagnostic": envelope.frequency_diagnostic.model_copy(update={
            "threshold_note": (
                "Pass E thresholds are tuned for 24h windows; non-24h windows "
                "may produce fewer or more crossings than expected. Window for "
                "this brief: 6h. Review the near-miss table for signal."
            ),
        }),
    })
    out = render_full_brief(envelope)

    # The threshold note text appears between the FREQUENCY DIAGNOSTIC header
    # line and the table header (i.e., at section-header position).
    freq_header_idx = out.find("FREQUENCY DIAGNOSTIC")
    table_header_idx = out.find("Term", freq_header_idx)
    header_zone = out[freq_header_idx:table_header_idx]
    assert "tuned for 24h windows" in header_zone
    assert "6h" in header_zone


# ---------- URL match warnings render pin (Stage 2a-ii-B audit signal) ----------


def test_url_match_warnings_renders_only_when_populated():
    """url_match_warnings is the defensive audit signal added in Stage 2a-ii-B.
    Renders ONLY when non-null (the abnormal case). When null (the normal
    case), the line does NOT appear — avoids noise on every brief."""
    envelope = _make_clean_envelope()

    # Default: null → no line
    out = render_full_brief(envelope)
    assert "URL match warnings" not in out

    # Populated: renders with audit-signal phrasing
    envelope_with_warnings = envelope.model_copy(update={
        "pass_f_footprint": envelope.pass_f_footprint.model_copy(update={
            "url_match_warnings": 3,
        }),
    })
    out_with = render_full_brief(envelope_with_warnings)
    assert "URL match warnings: 3" in out_with
    assert "audit signal" in out_with


# ---------- Cross-cutting structural pins ----------


def test_orphan_section_appears_before_pass_c_section():
    """Orphan-first ordering per Section 6 rendering rules — the highest-
    value diagnostic surfaces above Pass C output for review priority."""
    envelope = _make_clean_envelope()
    out = render_full_brief(envelope)
    orphan_idx = out.find("ORPHAN ATTENTION CROSSINGS")
    pass_c_idx = out.find("THEME-EVENT SYNTHESIS")
    assert orphan_idx > -1
    assert pass_c_idx > -1
    assert orphan_idx < pass_c_idx, \
        "Orphan section must precede Pass C section"


def test_events_render_in_materiality_descending_order():
    """Spec Section 6: Pass C events render in materiality-descending order."""
    envelope = _make_clean_envelope()
    out = render_full_brief(envelope)
    # The clean envelope has events evt-1 (0.83) down to evt-8 (0.48).
    # In materiality-descending order, evt-1 appears before evt-8.
    pass_c_idx = out.find("THEME-EVENT SYNTHESIS")
    pass_e_idx = out.find("ATTENTION SYNTHESIS")
    pass_c_block = out[pass_c_idx:pass_e_idx]
    evt1_pos = pass_c_block.find("evt-1")
    evt8_pos = pass_c_block.find("evt-8")
    assert evt1_pos != -1 and evt8_pos != -1
    assert evt1_pos < evt8_pos


def test_pass_failures_renders_only_when_non_empty():
    """Check 2 pin (Mando 2026-05-29): the renderer must surface
    pass_failures when populated — they're a primary operational
    diagnostic — but stay silent when empty to avoid noise on healthy
    cycles. Section position: right before ENVELOPE HEALTH for proximity
    to the per-step OK/FAILED labels.

    The orchestrator's 'always returns a valid envelope' invariant means
    a brief can assemble while the daemon is in degraded mode. This
    section is the visible signal that 'something failed but the brief
    still came together.' Hiding it (the pre-2b-i gap-fix state) would
    let degraded modes slip past Mando silently."""
    # Default clean envelope: empty pass_failures → no section
    envelope = _make_clean_envelope()
    assert envelope.pass_failures == []
    out = render_full_brief(envelope)
    assert "PASS FAILURES" not in out
    # Operational marker: envelope_health is the only OK/FAILED block visible
    assert "ENVELOPE HEALTH" in out

    # Populated pass_failures → section renders
    envelope_with_failures = envelope.model_copy(update={
        "pass_failures": [
            PassFailure(
                step="pass_c",
                reason="archive write failed: disk full",
                recovered=True,
            ),
            PassFailure(
                step="pass_f_footprint",
                reason="DB query failure: connection reset",
                recovered=True,
            ),
        ],
    })
    out_with = render_full_brief(envelope_with_failures)
    assert "PASS FAILURES" in out_with
    # Each failure entry visible with step + reason + recovered tag
    assert "pass_c:" in out_with
    assert "disk full" in out_with
    assert "pass_f_footprint:" in out_with
    assert "connection reset" in out_with
    assert "[recovered]" in out_with
    # Section positioned BEFORE envelope_health (operational proximity)
    pf_idx = out_with.find("PASS FAILURES")
    eh_idx = out_with.find("ENVELOPE HEALTH")
    assert pf_idx > -1 and eh_idx > -1
    assert pf_idx < eh_idx, "PASS FAILURES must precede ENVELOPE HEALTH"


def test_cost_total_rendered_with_model_and_rates():
    """Cost line per Section 6: $X.XXX (Sonnet 4.6, rates as of YYYY-MM-DD)."""
    envelope = _make_clean_envelope()
    out = render_full_brief(envelope)
    # Verify cost text contains all required components
    assert "Cost:" in out
    assert "$0.081" in out   # 0.0808 rounded for display
    assert "claude-sonnet-4-6" in out
    assert "2026-05-28" in out   # rates_as_of


def test_theme_segments_section_renders_active_quiet_and_hot_dropped():
    """Theme-segments section: active-first ordering, the hot-but-dropped
    flag for an active theme outside Pass C scope, and the quiet tag."""
    from news_watch_daemon.fullbrief.brief import ThemeSegment, ThemeSegmentsSection
    from news_watch_daemon.fullbrief.render import _render_theme_segments

    section = ThemeSegmentsSection(
        status="ok",
        segments=[
            ThemeSegment(
                theme_id="russia_ukraine_war", display_name="Russia Ukraine War",
                status="active", tagged_headline_count=47, in_pass_c_scope=False,
                summary="Heavy strikes on Kyiv dominate the window.",
                convergence_terms=["russian"],
            ),
            ThemeSegment(
                theme_id="china_us_decoupling", display_name="China US Decoupling",
                status="quiet", tagged_headline_count=3, in_pass_c_scope=False,
                summary="Routine chip-policy chatter, nothing new.",
            ),
        ],
    )
    out = _render_theme_segments(section)
    assert "THEME SEGMENTS" in out
    assert "[ACTIVE] Russia Ukraine War" in out
    assert "hot — outside Pass C scope" in out          # the dropped-theme flag
    assert "Heavy strikes on Kyiv" in out
    assert "attention: russian" in out
    assert "[quiet ] China US Decoupling" in out
    # Active ordered before quiet.
    assert out.index("Russia Ukraine War") < out.index("China US Decoupling")


def test_theme_segments_skipped_renders_nothing():
    from news_watch_daemon.fullbrief.brief import ThemeSegmentsSection
    from news_watch_daemon.fullbrief.render import _render_theme_segments
    assert _render_theme_segments(ThemeSegmentsSection(status="skipped")) == ""
