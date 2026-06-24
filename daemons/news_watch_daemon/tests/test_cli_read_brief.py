"""Tests for the `read-brief <path>` subcommand and its leaf loader.

Per the task brief (2026-06-09):
  - Valid persisted artifact → renders IDENTICALLY to generation output
    (reuses render_full_brief; no second renderer).
  - Fail-loud: missing file / malformed JSON / wrong brief_type / schema
    mismatch each produce an explicit error naming the path + nonzero exit.
    NEVER render a partial brief.

The loader (fullbrief/loader.py::load_full_brief_from_path) is the LEAF
load+validate function — total over valid inputs, raising
FullBriefLoadError with a specific message. The `read-brief` subcommand
owns the failure cases (error to stderr + exit 1). Both layers are tested:
the leaf directly (unit) and end-to-end via main().
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from news_watch_daemon.cli import main
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
    StepHealth,
    ThemeEventDigest,
    ThemeSynthesisSection,
    WindowSection,
)
from news_watch_daemon.fullbrief.loader import (
    FullBriefLoadError,
    load_full_brief_from_path,
)
from news_watch_daemon.fullbrief.render import render_full_brief
from news_watch_daemon.synthesize.archive import write_brief


# ---------- env fixture (read-brief never touches DB/themes, but main()
# constructs Config.from_env() before dispatch) ----------


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    yield tmp_path


# ---------- complete-valid envelope factory ----------


def _make_envelope() -> FullBriefEnvelope:
    """A representative Full Brief: orphan crossing + events + near-misses +
    theses-unwired warning, exercising the renderer's main branches."""
    crossings = [
        AttentionCrossing(
            term="iranian", freq_window=12, freq_prior=2, delta_ratio=6.0,
            shape="multi_source_convergence",
            attention_brief_id="nwd-attn-2026-06-09T14-37-45Z-aaaaaaaa",
            attention_brief_path="/fake/iranian.json",
            convergence=ConvergenceInfo(status="convergent", converges_with=["evt-1"]),
            llm_read_summary="Convergent attention narrative paragraph.",
        ),
        AttentionCrossing(
            term="world", freq_window=10, freq_prior=1, delta_ratio=10.0,
            shape="cross_topic_recurrence",
            attention_brief_id="nwd-attn-2026-06-09T14-37-45Z-bbbbbbbb",
            attention_brief_path="/fake/world.json",
            convergence=ConvergenceInfo(
                status="orphan",
                orphan_reason="term crossed threshold but no Pass C event contains it",
            ),
            llm_read_summary="Polysemous noise across many unrelated subjects.",
        ),
    ]
    events = [
        ThemeEventDigest(
            event_id="evt-1", headline_summary="Israel halted strikes on Iran.",
            themes=["us_iran_escalation"], materiality_score=0.78,
            direction="ambiguous", source_count=3, thesis_links=[],
        ),
        ThemeEventDigest(
            event_id="evt-2", headline_summary="Tanker disabled breaching blockade.",
            themes=["us_iran_escalation"], materiality_score=0.72,
            direction="confirm", source_count=2, thesis_links=[],
        ),
    ]
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-06-09T14-37-30Z-1b6de8b8",
        generated_at="2026-06-09T14:37:30Z",
        window=WindowSection(
            since="2026-06-08T14:37:30Z",
            until="2026-06-09T14:37:30Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative="An active US-Iran conflict in an unstable operational pause.",
            dominant_themes=["us_iran_escalation", "political_volatility"],
            material_event_count=2,
            attention_crossings_count=2,
            orphan_crossings_count=1,
            highest_materiality_score=0.78,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status="ok",
            brief_id="nwd-2026-06-09T14-37-30Z-ed16b496",
            narrative="Theme-specific narrative.",
            themes_covered=["us_iran_escalation", "political_volatility"],
            events=events,
            direction_tally={"confirm": 1, "ambiguous": 1, "break": 0},
            theses_doc_available=False,
            theses_doc_warning="NEWS_WATCH_THESES_PATH not set; synthesis ran no-theses variant",
        ),
        attention_synthesis=AttentionSynthesisSection(status="ok", crossings=crossings),
        frequency_diagnostic=FrequencyDiagnosticSection(
            threshold_note=None,
            crossings=[
                FrequencyDiagnosticCrossingRow(
                    term="world", freq_window=10, freq_prior=1,
                    shape="cross_topic_recurrence", convergence="orphan",
                ),
            ],
            near_misses=[
                FrequencyDiagnosticNearMissRow(
                    term="iran", freq_window=35, freq_prior=78,
                    delta_ratio=0.449, reason_not_crossed="above_prior_max",
                ),
                FrequencyDiagnosticNearMissRow(
                    term="war", freq_window=20, freq_prior=9,
                    delta_ratio=2.222, reason_not_crossed="above_prior_max",
                ),
            ],
            diagnostic_note="Standard near-miss table.",
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=53,
            cross_language_event_merges=2,
            attention_crossings_enabled_by_pass_f=[],
            url_match_warnings=3,
        ),
        envelope_health=FullBriefEnvelopeHealth(
            scrape=StepHealth(status="ok", headlines_inserted=572, sources_failed=0),
            pass_c=StepHealth(status="ok"),
            pass_e=StepHealth(status="ok", crossings_count=2),
            convergence_analysis=StepHealth(status="ok"),
            frequency_diagnostic=StepHealth(status="ok"),
        ),
        cost=CostEnvelope(
            pass_c=CostPerBrief(
                input_tokens=14892, output_tokens=4621,
                cache_creation_tokens=2060, cache_read_tokens=0, usd=0.1217,
            ),
            pass_e_briefs=[CostPerAttentionBrief(
                attention_brief_id="nwd-attn-2026-06-09T14-37-45Z-aaaaaaaa",
                input_tokens=3614, output_tokens=960,
                cache_creation_tokens=1834, cache_read_tokens=0, usd=0.0321,
            )],
            pass_e_total_usd=0.0544,
            total_usd=0.1761,
            model="claude-sonnet-4-6",
            rates_as_of="2026-05-28",
        ),
    )


# =========================================================================
# Leaf loader unit tests (fail-loud, total-over-valid-inputs)
# =========================================================================


def test_loader_valid_roundtrip(tmp_path):
    """Persisted artifact loads back into an equal envelope."""
    env_obj = _make_envelope()
    path = write_brief(tmp_path, env_obj)
    loaded = load_full_brief_from_path(path)
    assert isinstance(loaded, FullBriefEnvelope)
    assert loaded.model_dump(mode="json") == env_obj.model_dump(mode="json")


def test_loader_missing_file_names_path(tmp_path):
    missing = tmp_path / "does-not-exist.json"
    with pytest.raises(FullBriefLoadError) as ei:
        load_full_brief_from_path(missing)
    assert str(missing) in str(ei.value)
    assert "does not exist" in str(ei.value)


def test_loader_directory_is_not_a_file(tmp_path):
    with pytest.raises(FullBriefLoadError) as ei:
        load_full_brief_from_path(tmp_path)
    assert "not a regular file" in str(ei.value)


def test_loader_malformed_json(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{ this is not valid json", encoding="utf-8")
    with pytest.raises(FullBriefLoadError) as ei:
        load_full_brief_from_path(bad)
    assert "malformed JSON" in str(ei.value)
    assert str(bad) in str(ei.value)


def test_loader_wrong_brief_type(tmp_path):
    """A Pass C / Pass E artifact (different brief_type) is rejected clearly."""
    raw = _make_envelope().model_dump(mode="json")
    raw["brief_type"] = "theme_event"
    p = tmp_path / "wrong-type.json"
    p.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(FullBriefLoadError) as ei:
        load_full_brief_from_path(p)
    assert "not a Full Brief artifact" in str(ei.value)
    assert "theme_event" in str(ei.value)


def test_loader_missing_required_section(tmp_path):
    """Dropping a required composite section → schema-mismatch error."""
    raw = _make_envelope().model_dump(mode="json")
    del raw["cost"]
    p = tmp_path / "missing-cost.json"
    p.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(FullBriefLoadError) as ei:
        load_full_brief_from_path(p)
    assert "schema mismatch" in str(ei.value)
    assert "cost" in str(ei.value)


# =========================================================================
# End-to-end CLI tests (exit codes + stdout/stderr discipline)
# =========================================================================


def test_read_brief_renders_identically_to_generation(env, capsys):
    """`read-brief <path>` stdout == render_full_brief(envelope) + newline.

    This is the contract: the persisted artifact, reloaded, renders
    byte-identically to what `full-brief` emits at generation time —
    proving the renderer is reused, not forked, and round-trips losslessly.
    """
    env_obj = _make_envelope()
    path = write_brief(env, env_obj)

    rc = main(["read-brief", str(path)])
    assert rc == 0

    captured = capsys.readouterr()
    assert captured.out == render_full_brief(env_obj) + "\n"
    # Sanity: the rendered text actually contains the brief's signature lines.
    assert "FULL BRIEF — nwd-fullbrief-2026-06-09T14-37-30Z-1b6de8b8" in captured.out
    assert "ORPHAN ATTENTION CROSSINGS" in captured.out


def test_read_brief_missing_file_exits_nonzero(env, capsys):
    missing = env / "nope.json"
    rc = main(["read-brief", str(missing)])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""  # no partial render
    assert str(missing) in captured.err
    assert "does not exist" in captured.err


def test_read_brief_malformed_json_exits_nonzero(env, capsys):
    bad = env / "bad.json"
    bad.write_text("{ not json", encoding="utf-8")
    rc = main(["read-brief", str(bad)])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "malformed JSON" in captured.err


def test_read_brief_schema_mismatch_exits_nonzero(env, capsys):
    """Missing required section → exit 1, named failure, no partial render."""
    raw = _make_envelope().model_dump(mode="json")
    del raw["frequency_diagnostic"]
    p = env / "missing-section.json"
    p.write_text(json.dumps(raw), encoding="utf-8")

    rc = main(["read-brief", str(p)])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "schema mismatch" in captured.err
    assert "frequency_diagnostic" in captured.err


def test_read_brief_wrong_brief_type_exits_nonzero(env, capsys):
    raw = _make_envelope().model_dump(mode="json")
    raw["brief_type"] = "attention"
    p = env / "attn-as-full.json"
    p.write_text(json.dumps(raw), encoding="utf-8")

    rc = main(["read-brief", str(p)])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not a Full Brief artifact" in captured.err
