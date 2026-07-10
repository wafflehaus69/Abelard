"""CLI integration tests for the `full-brief` subcommand.

Per Mando's Stage 2b-ii forward-guidance:
  - Handler is a thin argparse wrapper + output formatter + exit-code mapper
  - Exit codes per spec Section 3: 0 (success) / 1 (infra error) / 2
    (brief assembled but primary path failed)
  - --quiet vs --json-only mutex per Q7

Tests mock assemble_full_brief at the import location inside cli; the
orchestrator's behavior is already covered by test_fullbrief_orchestrator.py.
This test file pins the CLI wrapper's responsibility: flag handling,
output mode selection, exit code mapping.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from news_watch_daemon.cli import _compute_full_brief_exit_code, main
from news_watch_daemon.fullbrief.brief import (
    AttentionSynthesisSection,
    CostEnvelope,
    ExecutiveSummary,
    FrequencyDiagnosticSection,
    FullBriefEnvelope,
    FullBriefEnvelopeHealth,
    PassFFootprint,
    PassFailure,
    StepHealth,
    ThemeSynthesisSection,
    WindowSection,
)


# ---------- fixtures ----------


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Minimal env: db at tmp_path, ANTHROPIC_API_KEY set so config loads."""
    db_path = tmp_path / "db" / "nwd.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Init minimal headlines table so connect() doesn't fail downstream
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE headlines (headline_id TEXT PRIMARY KEY, source TEXT, "
        "raw_source TEXT, headline TEXT, headline_en TEXT, url TEXT, "
        "language TEXT, published_at_unix INTEGER, fetched_at_unix INTEGER)"
    )
    conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO schema_meta VALUES ('version', '4')")
    conn.commit()
    conn.close()
    archive = tmp_path / "briefs"
    archive.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(db_path))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE_PATH", str(archive))
    return {"db_path": db_path, "archive": archive}


def _make_canned_envelope(
    *,
    pass_c_status: str = "ok",
    pass_e_status: str = "ok",
    scrape_status: str = "ok",
    theme_status: str = "ok",
    pass_failures: list | None = None,
) -> FullBriefEnvelope:
    """Build a minimal valid FullBriefEnvelope with controllable health states."""
    return FullBriefEnvelope(
        brief_id="nwd-fullbrief-2026-05-29T14-32-47Z-deadbeef",
        generated_at="2026-05-29T14:32:47Z",
        window=WindowSection(
            since="2026-05-28T14:32:47Z",
            until="2026-05-29T14:32:47Z",
            duration_hours=24,
        ),
        executive_summary=ExecutiveSummary(
            narrative="Test narrative.",
            dominant_themes=["us_iran_escalation"],
            material_event_count=1,
            attention_crossings_count=0,
            orphan_crossings_count=0,
        ),
        theme_synthesis=ThemeSynthesisSection(
            status=theme_status,    # type: ignore[arg-type]
            narrative="x" if theme_status == "ok" else None,
            no_trigger_reason="quiet" if theme_status == "no_trigger" else None,
            failure_reason="x" if theme_status == "failed" else None,
        ),
        attention_synthesis=AttentionSynthesisSection(status="ok"),
        frequency_diagnostic=FrequencyDiagnosticSection(
            diagnostic_note="Test.",
        ),
        pass_f_footprint=PassFFootprint(
            translated_rows_in_window=0,
            cross_language_event_merges=0,
        ),
        envelope_health=FullBriefEnvelopeHealth(
            scrape=StepHealth(status=scrape_status),  # type: ignore[arg-type]
            pass_c=StepHealth(status=pass_c_status),  # type: ignore[arg-type]
            pass_e=StepHealth(status=pass_e_status),  # type: ignore[arg-type]
            convergence_analysis=StepHealth(status="ok"),
            frequency_diagnostic=StepHealth(status="ok"),
        ),
        pass_failures=pass_failures or [],
        cost=CostEnvelope(
            pass_c=None, pass_e_briefs=[],
            pass_e_total_usd=0.0, total_usd=0.0,
            model="claude-sonnet-4-6", rates_as_of="2026-05-28",
        ),
    )


# ---------- exit code mapper unit tests ----------


def test_exit_code_clean_envelope_returns_0():
    """All health=ok → 0."""
    env_ = _make_canned_envelope()
    assert _compute_full_brief_exit_code(env_) == 0


def test_exit_code_pass_c_failed_returns_2():
    """Pass C primary path failure → 2."""
    env_ = _make_canned_envelope(pass_c_status="failed")
    assert _compute_full_brief_exit_code(env_) == 2


def test_exit_code_pass_e_failed_returns_2():
    """Pass E primary path failure → 2."""
    env_ = _make_canned_envelope(pass_e_status="failed")
    assert _compute_full_brief_exit_code(env_) == 2


def test_exit_code_scrape_failed_returns_2():
    """Scrape primary path failure → 2 (without fresh data, brief is degraded)."""
    env_ = _make_canned_envelope(scrape_status="failed")
    assert _compute_full_brief_exit_code(env_) == 2


def test_exit_code_scrape_skipped_returns_0():
    """no_scrape → scrape.status=skipped → NOT a failure, exit 0."""
    env_ = _make_canned_envelope(scrape_status="skipped")
    assert _compute_full_brief_exit_code(env_) == 0


def test_exit_code_pass_failures_present_but_secondary_returns_0():
    """pass_failures populated for SECONDARY metric failures (pass_f_footprint,
    frequency_diagnostic) → exit 0 per Mando's primary-vs-secondary mapping.
    The brief is healthy enough to consume normally with footnotes."""
    env_ = _make_canned_envelope(pass_failures=[
        PassFailure(step="pass_f_footprint", reason="DB query failed", recovered=True),
    ])
    assert _compute_full_brief_exit_code(env_) == 0


def test_exit_code_no_trigger_returns_0():
    """no_trigger is a valid quiet-day outcome — health.pass_c.status=ok,
    theme_synthesis.status=no_trigger. Exit 0."""
    env_ = _make_canned_envelope(theme_status="no_trigger")
    assert _compute_full_brief_exit_code(env_) == 0


# ---------- CLI flag behavior tests (main() integration) ----------


def test_t12_no_scrape_flag_threads_to_orchestrator(env, capsys):
    """T12: --no-scrape flag passed to assemble_full_brief.
    Mocked orchestrator inspects no_scrape arg."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ) as mock_assemble:
        rc = main(["full-brief", "--no-scrape"])
    assert rc == 0
    assert mock_assemble.call_args.kwargs["no_scrape"] is True


def test_t13_quiet_flag_suppresses_stdout(env, capsys):
    """T13: --quiet → no stdout output. Artifact write inside
    assemble_full_brief is the only side effect."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ):
        rc = main(["full-brief", "--quiet"])
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out == ""


def test_t14_json_only_flag_prints_envelope_as_json(env, capsys):
    """T14: --json-only → stdout is the JSON envelope. No rendered text."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ):
        rc = main(["full-brief", "--json-only"])
    assert rc == 0
    captured = capsys.readouterr()
    # stdout should parse as JSON
    parsed = json.loads(captured.out)
    assert parsed["brief_id"] == canned.brief_id
    assert parsed["brief_type"] == "full_brief"
    # And should NOT contain rendered text markers
    assert "FULL BRIEF —" not in captured.out
    assert "DOMINANT THEMES:" not in captured.out


def test_default_flag_combination_renders_human_readable(env, capsys):
    """Default (no flags) → rendered text to stdout."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ):
        rc = main(["full-brief"])
    assert rc == 0
    captured = capsys.readouterr()
    # Rendered text markers visible
    assert "FULL BRIEF —" in captured.out
    assert "NARRATIVE" in captured.out
    assert canned.brief_id in captured.out


def test_quiet_and_json_only_mutually_exclusive(env, capsys):
    """Q7 + mutex pin: argparse rejects --quiet --json-only combination."""
    with pytest.raises(SystemExit):
        main(["full-brief", "--quiet", "--json-only"])
    # argparse writes to stderr
    captured = capsys.readouterr()
    assert "not allowed with" in captured.err.lower() or "mutually exclusive" in captured.err.lower()


def test_window_hours_flag_threads_to_orchestrator(env, capsys):
    """--window-hours N reaches assemble_full_brief, clamped to [1, 168]."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ) as mock_assemble:
        rc = main(["full-brief", "--window-hours", "48"])
    assert rc == 0
    assert mock_assemble.call_args.kwargs["window_hours"] == 48


def test_window_hours_clamped_to_max_168(env, capsys):
    """Out-of-range values are clamped, not rejected. Stderr surfaces the
    clamp so the user knows their input was adjusted (per Mando's Stage
    2b-ii pre-commit refinement — silent clamping is a UX trap)."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ) as mock_assemble:
        rc = main(["full-brief", "--window-hours", "999"])
    assert rc == 0
    assert mock_assemble.call_args.kwargs["window_hours"] == 168
    captured = capsys.readouterr()
    assert "window_hours clamped to 168 (was 999" in captured.err


def test_window_hours_clamped_to_min_1(env, capsys):
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ) as mock_assemble:
        rc = main(["full-brief", "--window-hours", "0"])
    assert rc == 0
    assert mock_assemble.call_args.kwargs["window_hours"] == 1
    captured = capsys.readouterr()
    assert "window_hours clamped to 1 (was 0" in captured.err


def test_window_hours_within_bounds_no_clamping_message(env, capsys):
    """In-bounds values (1-168) → no stderr message. Silent on the
    expected case so user doesn't see noise for normal invocations."""
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ):
        rc = main(["full-brief", "--window-hours", "24"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "clamped" not in captured.err


# ---------- exit code paths through main() ----------


def test_main_returns_2_when_orchestrator_signals_pass_c_failed(env, capsys):
    """Exit 2 surfaces from main() when envelope has primary-path failure."""
    canned = _make_canned_envelope(pass_c_status="failed")
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        return_value=canned,
    ):
        rc = main(["full-brief", "--quiet"])
    assert rc == 2


def test_main_returns_1_when_orchestrator_raises(env, capsys):
    """assemble_full_brief raising (e.g., DB unreachable mid-orchestration)
    → exit 1 + stderr message."""
    with patch(
        "news_watch_daemon.cli.assemble_full_brief",
        side_effect=RuntimeError("simulated DB connection lost"),
    ):
        rc = main(["full-brief", "--quiet"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "Full Brief assembly failed" in captured.err
    assert "simulated DB connection lost" in captured.err


# ---------- one-pass extras: --pdf / --out / full artifact path (2026-07-10) ----


def test_pdf_flag_renders_pdf_in_one_pass(env, capsys, tmp_path):
    """--pdf renders a real PDF from the just-assembled envelope, no read-brief."""
    canned = _make_canned_envelope()
    out_pdf = tmp_path / "brief.pdf"
    with patch("news_watch_daemon.cli.assemble_full_brief", return_value=canned):
        rc = main(["full-brief", "--quiet", "--pdf", str(out_pdf)])
    assert rc == 0
    assert out_pdf.exists() and out_pdf.stat().st_size > 0
    assert "Wrote PDF:" in capsys.readouterr().err


def test_out_flag_writes_json_copy(env, capsys, tmp_path):
    """--out lands a JSON copy at a predictable path in the same pass."""
    canned = _make_canned_envelope()
    out_json = tmp_path / "copy.json"
    with patch("news_watch_daemon.cli.assemble_full_brief", return_value=canned):
        rc = main(["full-brief", "--quiet", "--out", str(out_json)])
    assert rc == 0
    parsed = json.loads(out_json.read_text(encoding="utf-8"))
    assert parsed["brief_id"] == canned.brief_id


def test_full_artifact_path_reported_on_stderr(env, capsys):
    """The full archive path (not just the bare filename) is surfaced so the
    operator never has to hunt for the just-written JSON."""
    canned = _make_canned_envelope()
    with patch("news_watch_daemon.cli.assemble_full_brief", return_value=canned):
        rc = main(["full-brief", "--quiet"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "Artifact (JSON):" in err
    assert canned.brief_id in err


def test_pdf_flag_does_not_pollute_json_only_stdout(env, capsys, tmp_path):
    """--pdf writes its confirmation to stderr, so --json-only stdout stays
    a single clean JSON document."""
    canned = _make_canned_envelope()
    out_pdf = tmp_path / "brief.pdf"
    with patch("news_watch_daemon.cli.assemble_full_brief", return_value=canned):
        rc = main(["full-brief", "--json-only", "--pdf", str(out_pdf)])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)  # stdout still pure JSON
    assert parsed["brief_id"] == canned.brief_id
    assert out_pdf.exists()


# ---------- run (one-pass operating cycle) ----------

_SEED_THEMES = Path(__file__).resolve().parent.parent / "themes"


def _run_env(tmp_path, monkeypatch):
    """Fresh DB (no schema yet) + real seed themes — a genuine cold start."""
    monkeypatch.setenv("NEWS_WATCH_DB_PATH", str(tmp_path / "state.db"))
    monkeypatch.setenv("NEWS_WATCH_THEMES_DIR", str(_SEED_THEMES))
    monkeypatch.setenv("NEWS_WATCH_BRIEF_ARCHIVE", str(tmp_path / "briefs"))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-fake")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")


def test_run_cold_start_initializes_then_delegates(tmp_path, monkeypatch):
    """`run` on a fresh DB applies schema, loads themes, then assembles —
    the assemble delegate is reached only if both ensure steps succeeded."""
    _run_env(tmp_path, monkeypatch)
    canned = _make_canned_envelope()
    with patch(
        "news_watch_daemon.cli.assemble_full_brief", return_value=canned,
    ) as mock_assemble:
        rc = main(["run", "--quiet"])
    assert rc == 0
    mock_assemble.assert_called_once()
    assert (tmp_path / "state.db").exists()


def test_run_forwards_pdf_flag(tmp_path, monkeypatch):
    """`run --pdf` produces the PDF in the same pass, proving output flags
    thread through the run wrapper to full-brief."""
    _run_env(tmp_path, monkeypatch)
    canned = _make_canned_envelope()
    out_pdf = tmp_path / "run.pdf"
    with patch("news_watch_daemon.cli.assemble_full_brief", return_value=canned):
        rc = main(["run", "--quiet", "--pdf", str(out_pdf)])
    assert rc == 0
    assert out_pdf.exists() and out_pdf.stat().st_size > 0


def test_run_aborts_before_assembly_on_ensure_failure(tmp_path, monkeypatch, capsys):
    """If ensure-themes fails, run exits 1 BEFORE any scrape/LLM spend."""
    _run_env(tmp_path, monkeypatch)
    err_env = {"status": "error", "error_detail": "boom", "data": None}
    with patch(
        "news_watch_daemon.cli._handle_themes_load", return_value=err_env,
    ), patch("news_watch_daemon.cli.assemble_full_brief") as mock_assemble:
        rc = main(["run", "--quiet"])
    assert rc == 1
    mock_assemble.assert_not_called()
    assert "run aborted at ensure-themes" in capsys.readouterr().err
