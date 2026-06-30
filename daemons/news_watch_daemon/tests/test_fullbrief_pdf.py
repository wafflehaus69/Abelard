"""Tests for the Full Brief PDF render target (fullbrief/pdf.py + read-brief --pdf).

Smoke (valid PDF produced) + fail-loud (empty brief, wrong type) + Unicode
(Spanish/accented text renders without crashing) + the CLI surface.
"""

from __future__ import annotations

import pytest

from news_watch_daemon.fullbrief.brief import (
    AttentionCrossing,
    AttentionSynthesisSection,
    ConvergenceInfo,
    CostEnvelope,
    CostPerBrief,
    ExecutiveSummary,
    FrequencyDiagnosticNearMissRow,
    FrequencyDiagnosticSection,
    FullBriefEnvelope,
    FullBriefEnvelopeHealth,
    PassFFootprint,
    StepHealth,
    ThemeEventDigest,
    ThemeSynthesisSection,
    WindowSection,
)
from news_watch_daemon.fullbrief.pdf import PdfRenderError, render_full_brief_pdf


def _health() -> FullBriefEnvelopeHealth:
    return FullBriefEnvelopeHealth(
        scrape=StepHealth(status="ok", headlines_inserted=300, sources_failed=0),
        pass_c=StepHealth(status="ok"),
        pass_e=StepHealth(status="ok", crossings_count=2),
        convergence_analysis=StepHealth(status="ok"),
        frequency_diagnostic=StepHealth(status="ok"),
    )


def _cost() -> CostEnvelope:
    return CostEnvelope(
        pass_c=CostPerBrief(input_tokens=14000, output_tokens=4000,
                            cache_creation_tokens=2000, cache_read_tokens=0, usd=0.12),
        pass_e_briefs=[], pass_e_total_usd=0.05, total_usd=0.17,
        model="claude-sonnet-4-6", rates_as_of="2026-05-28",
    )


def _make_envelope(*, narrative="An active US-Iran conflict in an unstable pause.",
                   events=True, crossings=True) -> FullBriefEnvelope:
    evs = []
    if events:
        evs = [
            ThemeEventDigest(event_id="evt-1", headline_summary="SCOTUS preserves Fed independence 6-3.",
                             themes=["fed_policy_path"], materiality_score=0.82,
                             direction="confirm", source_count=3, thesis_links=[]),
            ThemeEventDigest(event_id="evt-2", headline_summary="Hormuz traffic at ~10% of peacetime.",
                             themes=["us_iran_escalation"], materiality_score=0.78,
                             direction="ambiguous", source_count=2, thesis_links=[]),
        ]
    crs = []
    if crossings:
        crs = [
            AttentionCrossing(term="court", freq_window=16, freq_prior=1, delta_ratio=16.0,
                              shape="multi_source_convergence",
                              attention_brief_id="nwd-attn-x-a", attention_brief_path="/f/a.json",
                              convergence=ConvergenceInfo(status="convergent", converges_with=["evt-1"]),
                              llm_read_summary="Convergent on the SCOTUS Fed ruling."),
            AttentionCrossing(term="ministry", freq_window=12, freq_prior=0, delta_ratio=12.0,
                              shape="cross_topic_recurrence",
                              attention_brief_id="nwd-attn-x-b", attention_brief_path="/f/b.json",
                              convergence=ConvergenceInfo(status="orphan",
                                                          orphan_reason="no Pass C event contains it"),
                              llm_read_summary="Generic institutional label across unrelated stories."),
        ]
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-06-30T01-05-01Z-efbfa8f3",
        generated_at="2026-06-30T01:05:01Z",
        window=WindowSection(since="2026-06-29T01:05:01Z", until="2026-06-30T01:05:01Z",
                             duration_hours=24),
        executive_summary=ExecutiveSummary(
            narrative=narrative, dominant_themes=["fed_policy_path", "us_iran_escalation"],
            material_event_count=len(evs), attention_crossings_count=len(crs),
            orphan_crossings_count=sum(1 for c in crs if c.convergence.status == "orphan"),
            highest_materiality_score=(max((e.materiality_score for e in evs), default=None)),
        ),
        theme_synthesis=ThemeSynthesisSection(
            status="ok", brief_id="nwd-2026-06-30T01-05-01Z-b224401d",
            narrative=narrative, themes_covered=["fed_policy_path", "us_iran_escalation"],
            events=evs, direction_tally={"confirm": 1, "ambiguous": 1, "break": 0},
            theses_doc_available=True,
        ),
        attention_synthesis=AttentionSynthesisSection(status="ok", crossings=crs),
        frequency_diagnostic=FrequencyDiagnosticSection(
            threshold_note=None,
            near_misses=[FrequencyDiagnosticNearMissRow(term="russian", freq_window=25, freq_prior=13,
                                                        delta_ratio=1.92, reason_not_crossed="above_prior_max")],
            diagnostic_note="Standard near-miss table.",
        ),
        pass_f_footprint=PassFFootprint(translated_rows_in_window=66, cross_language_event_merges=2,
                                        attention_crossings_enabled_by_pass_f=["ministry"]),
        envelope_health=_health(), cost=_cost(),
    )


def test_render_pdf_valid_file(tmp_path):
    out = tmp_path / "brief.pdf"
    p = render_full_brief_pdf(_make_envelope(), out)
    assert p == out
    assert out.is_file()
    data = out.read_bytes()
    assert data[:5] == b"%PDF-"
    assert len(data) > 1000  # real content, not a stub


def test_render_pdf_unicode_headline(tmp_path):
    """Spanish/accented + punctuation render without crashing (Vera Unicode font)."""
    env = _make_envelope(
        narrative="Colombia: Abelardo de la Espriella lidera; Petro reaccionó — tensión en Cúcuta. "
                  "Niños, café, señor, ¿qué? €93 billion."
    )
    out = tmp_path / "uni.pdf"
    render_full_brief_pdf(env, out)
    assert out.read_bytes()[:5] == b"%PDF-"
    assert out.stat().st_size > 1000


def test_render_pdf_empty_brief_fails_loud(tmp_path):
    env = _make_envelope(narrative="", events=False, crossings=False)
    with pytest.raises(PdfRenderError) as ei:
        render_full_brief_pdf(env, tmp_path / "empty.pdf")
    assert "empty" in str(ei.value).lower()
    assert not (tmp_path / "empty.pdf").exists()


def test_render_pdf_wrong_type_fails_loud(tmp_path):
    with pytest.raises(PdfRenderError) as ei:
        render_full_brief_pdf({"not": "an envelope"}, tmp_path / "x.pdf")
    assert "FullBriefEnvelope" in str(ei.value)


def test_render_pdf_no_trigger_brief(tmp_path):
    """A no-trigger brief (narrative present, no events/crossings) still renders."""
    env = _make_envelope(narrative="Pass C trigger gate did not fire this window.",
                         events=False, crossings=False)
    render_full_brief_pdf(env, tmp_path / "nt.pdf")
    assert (tmp_path / "nt.pdf").read_bytes()[:5] == b"%PDF-"


# ---------- CLI: read-brief --pdf ----------


@pytest.fixture
def env_db(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    yield tmp_path


def test_read_brief_pdf_cli(env_db, capsys):
    from news_watch_daemon.cli import main
    from news_watch_daemon.synthesize.archive import write_brief

    art = write_brief(env_db, _make_envelope())
    out = env_db / "out.pdf"
    rc = main(["read-brief", str(art), "--pdf", str(out)])
    assert rc == 0
    assert out.is_file() and out.read_bytes()[:5] == b"%PDF-"
    assert "Wrote PDF" in capsys.readouterr().out


def test_read_brief_pdf_cli_missing_file(env_db, capsys):
    from news_watch_daemon.cli import main

    rc = main(["read-brief", str(env_db / "nope.json"), "--pdf", str(env_db / "x.pdf")])
    assert rc == 1
    assert not (env_db / "x.pdf").exists()
    assert "does not exist" in capsys.readouterr().err
