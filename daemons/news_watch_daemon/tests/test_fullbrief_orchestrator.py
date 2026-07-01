"""Tests for assemble_full_brief — Stage 2a-ii-A orchestrator.

Test coverage:
  T1   - Full happy path: scrape ok, Pass C synthesized, Pass E ok with
         1 crossing, convergence resolves, envelope assembles + writes to disk.
  T2   - Pass C synthesis_failed: pass_failures gets entry, theme_synthesis
         marked failed, attention path still runs.
  T3   - Pass E attention_outcome status=error: envelope_health.pass_e
         flagged, theme_synthesis still populates.
  T4   - Both Pass C + Pass E fail: envelope still assembles with both
         failure flags + pass_failures entries.
  T5   - Scrape failed (no_scrape=True path to simulate): envelope_health.scrape
         shows skipped, Pass C still runs.
  Structural pin: round-trip via model_dump -> model_validate succeeds.
  Sink_factory pin: orchestrator threads sink_factory through to
         synthesize_window unchanged.

All tests mock at the boundary (synthesize_window, scrape_cycle's helpers,
read_brief, count_terms) — orchestrator's own composition logic is what's
exercised.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from news_watch_daemon.attention.brief_schema import AttentionBrief
from news_watch_daemon.attention.counter import TermCounts
from news_watch_daemon.config import Config
from news_watch_daemon.fullbrief.brief import FullBriefEnvelope, StepHealth as _StepHealth
from news_watch_daemon.fullbrief.orchestrator import assemble_full_brief
from news_watch_daemon.synthesize.brief import (
    Brief,
    Dispatch,
    Event,
    SourceHeadline,
    SynthesisMetadata,
    ThesisLink,
    Trigger,
    TriggerWindow,
)
from news_watch_daemon.synthesize.synthesize import SynthesizeResult


# ---------- factories ----------


def _make_cfg(tmp_path: Path) -> Config:
    """Minimal Config with paths under tmp_path. db_path is mkdir'd by connect()."""
    db_path = tmp_path / "db" / "nwd.db"
    archive = tmp_path / "briefs"
    archive.mkdir(parents=True, exist_ok=True)
    # Build a real SQLite file with headlines table — count_terms needs it.
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE headlines (headline_id TEXT PRIMARY KEY, source TEXT, "
        "raw_source TEXT, headline TEXT, headline_en TEXT, url TEXT, language TEXT, "
        "published_at_unix INTEGER, fetched_at_unix INTEGER)"
    )
    conn.execute(
        "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.execute("INSERT INTO schema_meta (key, value) VALUES ('version', '4')")
    conn.commit()
    conn.close()
    return Config(
        db_path=db_path,
        log_level="INFO",
        anthropic_api_key="sk-ant-fake",
        brief_archive_path=archive,
    )


def _make_brief(
    *,
    brief_id: str = "nwd-2026-05-29T14-32-47Z-deadbeef",
    event_headlines: list[str] | None = None,
) -> Brief:
    """Build a minimal Brief with 1 event whose source_headlines text is given.

    Used by tests to control what convergence will see for the triggering_term.
    """
    if event_headlines is None:
        event_headlines = ["Iran ceasefire announced"]
    return Brief(
        brief_id=brief_id,
        generated_at="2026-05-29T14:32:47Z",
        trigger=Trigger(
            type="event",
            reason="cross_theme:us_iran_escalation",
            window=TriggerWindow(since="x", until="y"),
        ),
        themes_covered=["us_iran_escalation"],
        events=[Event(
            event_id="evt-1",
            headline_summary="Iran ceasefire — summary",
            themes=["us_iran_escalation"],
            source_headlines=[
                SourceHeadline(
                    publisher="Reuters",
                    headline=h,
                    url="https://example.com",
                    published_at="2026-05-29T13:00:00Z",
                )
                for h in event_headlines
            ],
            materiality_score=0.85,
            thesis_links=[ThesisLink(
                thesis_id=None, direction="confirm", note="solid signal",
            )],
        )],
        narrative="Test Pass C narrative.",
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6",
            input_tokens=3000,
            output_tokens=2000,
            cache_creation_input_tokens=2000,
            cache_read_input_tokens=0,
            theses_doc_available=False,
            theses_doc_warning="NEWS_WATCH_THESES_PATH not set",
        ),
    )


def _make_synthesize_result(
    *,
    status: str = "synthesized",
    brief: Brief | None = None,
    brief_path: Path | None = None,
    reason: str | None = None,
) -> SynthesizeResult:
    if brief is None and status in ("synthesized", "archive_failed"):
        brief = _make_brief()
    metadata = brief.synthesis_metadata if brief else None
    return SynthesizeResult(
        status=status,    # type: ignore[arg-type]
        window_since_unix=1000,
        window_until_unix=2000,
        brief=brief,
        metadata=metadata,
        brief_path=brief_path,
        reason=reason,
    )


def _make_attention_brief(
    *,
    brief_id: str = "nwd-attn-2026-05-29T14-31-21Z-abcd1234",
    triggering_term: str = "iran",
) -> AttentionBrief:
    return AttentionBrief(
        brief_id=brief_id,
        generated_at="2026-05-29T14:31:21Z",
        triggering_term=triggering_term,
        term_frequency_window=12,
        term_frequency_prior=2,
        cluster_size=12,
        narrative=f"Sample narrative about {triggering_term}." * 5,
        source_mix={"telegram:CIG_telegram": 8, "rss:bloomberg_politics": 4},
        entities_observed=["Iran", "Hormuz"],
        attention_shape="multi_source_convergence",
        dispatch=Dispatch(alerted=False),
        synthesis_metadata=SynthesisMetadata(
            model_used="claude-sonnet-4-6",
            input_tokens=3178,
            output_tokens=804,
            cache_creation_input_tokens=1834,
            cache_read_input_tokens=0,
            theses_doc_available=False,
        ),
    )


def _make_attention_outcome(
    *,
    status: str = "ok",
    brief_ids: list[str] | None = None,
    reason: str | None = None,
) -> dict:
    if brief_ids is None:
        brief_ids = ["nwd-attn-2026-05-29T14-31-21Z-abcd1234"]
    return {
        "status": status,
        "reason": reason,
        "now_unix": 1500,
        "window_since_unix": 1000,
        "window_until_unix": 2000,
        "prior_since_unix": 0,
        "prior_until_unix": 1000,
        "headlines_in_window": 100,
        "distinct_tokens_in_window": 800,
        "crossings_evaluated": len(brief_ids),
        "per_term": [
            {
                "term": "iran" if i == 0 else f"term_{i}",
                "success": True,
                "brief_id": bid,
                "archive_path": f"/fake/path/{bid}.json",
                "dispatch_success": False,
                "dispatch_error": None,
                "error": None,
                "input_tokens": 3178,
                "output_tokens": 804,
                "cache_creation_input_tokens": 1834,
                "cache_read_input_tokens": 0,
            }
            for i, bid in enumerate(brief_ids)
        ],
        "top_candidates": [],
    }


def _make_term_counts() -> TermCounts:
    """Synthetic counter output for frequency_diagnostic."""
    return TermCounts(
        window_counts={"iran": 30, "hormuz": 12, "trump": 25, "fed": 8},
        prior_counts={"iran": 25, "hormuz": 5, "trump": 22, "fed": 6},
        window_since_unix=1000,
        window_until_unix=2000,
        prior_since_unix=0,
        prior_until_unix=1000,
    )


# Common patches needed for the orchestrator's Pass C + freq_diagnostic flow.
class _Patches:
    def __init__(self):
        self.synthesize_window = patch(
            "news_watch_daemon.fullbrief.orchestrator.synthesize_window",
        )
        self.load_synthesis_config = patch(
            "news_watch_daemon.fullbrief.orchestrator.load_synthesis_config",
            return_value=MagicMock(),
        )
        self.load_all_themes = patch(
            "news_watch_daemon.fullbrief.orchestrator.load_all_themes",
            return_value=[MagicMock(theme_id="us_iran_escalation", status="active")],
        )
        self.build_anthropic_client = patch(
            "news_watch_daemon.fullbrief.orchestrator.build_anthropic_client",
            return_value=MagicMock(),
        )
        self.load_stopwords = patch(
            "news_watch_daemon.fullbrief.orchestrator.load_stopwords",
            return_value=frozenset({"the", "a"}),
        )
        self.count_terms_collapsed = patch(
            "news_watch_daemon.fullbrief.orchestrator.count_terms_collapsed",
            return_value=_make_term_counts(),
        )
        self.read_brief = patch(
            "news_watch_daemon.fullbrief.orchestrator.read_brief",
            return_value=_make_attention_brief(),
        )

    def __enter__(self):
        self.synthesize_window_mock = self.synthesize_window.__enter__()
        self.load_synthesis_config.__enter__()
        self.load_all_themes.__enter__()
        self.build_anthropic_client.__enter__()
        self.load_stopwords.__enter__()
        self.count_terms_collapsed.__enter__()
        self.read_brief.__enter__()
        return self

    def __exit__(self, *args):
        self.synthesize_window.__exit__(*args)
        self.load_synthesis_config.__exit__(*args)
        self.load_all_themes.__exit__(*args)
        self.build_anthropic_client.__exit__(*args)
        self.load_stopwords.__exit__(*args)
        self.count_terms_collapsed.__exit__(*args)
        self.read_brief.__exit__(*args)


# ---------- T1: Full happy path ----------


def test_t1_full_happy_path_envelope_assembles_with_all_sections(tmp_path):
    """T1: scrape ok, Pass C synthesized, Pass E ok with 1 crossing.
    Verify envelope has every section populated, brief written to disk."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief(event_headlines=["Iran ceasefire announced today"])
    brief_path = tmp_path / "briefs" / "fake-pass-c.json"

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief, brief_path=brief_path,
        )
        # Use no_scrape=True for T1 simplicity; T3 covers scrape attention path.
        # We still need an attention_outcome to surface a crossing — feed it in
        # via patching _do_scrape_step.
        with patch(
            "news_watch_daemon.fullbrief.orchestrator._do_scrape_step",
            return_value=(
                _StepHealth(status="ok", headlines_inserted=10, sources_failed=0),
                _make_attention_outcome(),
            ),
        ):
            envelope = assemble_full_brief(
                cfg=cfg,
                window_hours=24,
                no_scrape=False,
                sink_factory=None,
                now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
            )

    # Envelope structural completeness
    assert isinstance(envelope, FullBriefEnvelope)
    assert envelope.brief_type == "full_brief"
    assert envelope.brief_id.startswith("nwd-fullbrief-")
    assert envelope.window.duration_hours == 24

    # Pass C populated as ok
    assert envelope.theme_synthesis.status == "ok"
    assert envelope.theme_synthesis.brief_id == brief.brief_id
    assert envelope.theme_synthesis.narrative == "Test Pass C narrative."
    assert len(envelope.theme_synthesis.events) == 1
    assert envelope.theme_synthesis.events[0].event_id == "evt-1"
    assert envelope.theme_synthesis.theses_doc_warning is not None

    # Pass E populated with the convergent crossing
    assert envelope.attention_synthesis.status == "ok"
    assert len(envelope.attention_synthesis.crossings) == 1
    crossing = envelope.attention_synthesis.crossings[0]
    assert crossing.term == "iran"
    assert crossing.convergence.status == "convergent"
    assert crossing.convergence.converges_with == ["evt-1"]

    # Frequency diagnostic populated
    assert len(envelope.frequency_diagnostic.crossings) == 1
    assert len(envelope.frequency_diagnostic.near_misses) >= 1   # hormuz, trump, fed all elevated

    # Cost envelope reflects both passes
    assert envelope.cost.pass_c is not None
    assert envelope.cost.pass_c.input_tokens == 3000
    assert len(envelope.cost.pass_e_briefs) == 1
    assert envelope.cost.total_usd > 0

    # Disk artifact written
    yyyy_mm = envelope.brief_id.split("-")[2] + "-" + envelope.brief_id.split("-")[3]
    assert (cfg.brief_archive_path / yyyy_mm / f"{envelope.brief_id}.json").is_file()


# ---------- T2: Pass C synthesis_failed ----------


def test_t2_pass_c_synthesis_failed_envelope_still_assembles(tmp_path):
    cfg = _make_cfg(tmp_path)
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesis_failed", reason="Sonnet API 429",
        )
        with patch(
            "news_watch_daemon.fullbrief.orchestrator._do_scrape_step",
            return_value=(
                _StepHealth(status="ok", headlines_inserted=0, sources_failed=0),
                _make_attention_outcome(),
            ),
        ):
            envelope = assemble_full_brief(
                cfg=cfg, no_scrape=False,
                now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
            )

    assert envelope.theme_synthesis.status == "failed"
    assert "Sonnet API 429" in envelope.theme_synthesis.failure_reason
    assert envelope.envelope_health.pass_c.status == "failed"
    # pass_failures has pass_c entry
    pass_c_failures = [pf for pf in envelope.pass_failures if pf.step == "pass_c"]
    assert len(pass_c_failures) == 1
    assert pass_c_failures[0].recovered is True
    # Attention still ran
    assert len(envelope.attention_synthesis.crossings) == 1
    # All crossings orphan since no events to converge against
    assert envelope.attention_synthesis.crossings[0].convergence.status == "orphan"


# ---------- T3: Pass E attention_outcome status=error ----------


def test_t3_pass_e_error_envelope_health_flagged_pass_c_still_runs(tmp_path):
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        with patch(
            "news_watch_daemon.fullbrief.orchestrator._do_scrape_step",
            return_value=(
                _StepHealth(status="ok", headlines_inserted=10, sources_failed=0),
                # Attention outcome with status=error
                {"status": "error", "reason": "attention SDK timeout"},
            ),
        ):
            envelope = assemble_full_brief(
                cfg=cfg, no_scrape=False,
                now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
            )

    # Pass C still ok
    assert envelope.theme_synthesis.status == "ok"
    assert envelope.theme_synthesis.brief_id == brief.brief_id
    # Pass E flagged
    assert envelope.envelope_health.pass_e.status == "failed"
    assert "timeout" in envelope.envelope_health.pass_e.reason
    assert envelope.attention_synthesis.status == "failed"
    assert envelope.attention_synthesis.crossings == []
    # pass_failures has pass_e entry
    pass_e_failures = [pf for pf in envelope.pass_failures if pf.step == "pass_e"]
    assert len(pass_e_failures) == 1


# ---------- T4: Both Pass C + Pass E fail ----------


def test_t4_both_passes_fail_envelope_still_assembles(tmp_path):
    cfg = _make_cfg(tmp_path)
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesis_failed", reason="Sonnet API 500",
        )
        with patch(
            "news_watch_daemon.fullbrief.orchestrator._do_scrape_step",
            return_value=(
                _StepHealth(status="ok", headlines_inserted=0, sources_failed=0),
                {"status": "error", "reason": "attention crashed"},
            ),
        ):
            envelope = assemble_full_brief(
                cfg=cfg, no_scrape=False,
                now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
            )

    assert envelope.theme_synthesis.status == "failed"
    assert envelope.attention_synthesis.status == "failed"
    assert envelope.envelope_health.pass_c.status == "failed"
    assert envelope.envelope_health.pass_e.status == "failed"
    steps_failed = {pf.step for pf in envelope.pass_failures}
    assert "pass_c" in steps_failed
    assert "pass_e" in steps_failed
    # Envelope still assembled and writable
    assert isinstance(envelope, FullBriefEnvelope)


# ---------- T5: Scrape failed / no_scrape ----------


def test_t5_no_scrape_skipped_pass_c_still_runs_against_stale_data(tmp_path):
    """T5 variant: no_scrape=True skips scrape step. Pass C runs against
    whatever DB state exists. envelope_health.scrape shows skipped."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )

    assert envelope.envelope_health.scrape.status == "skipped"
    assert envelope.theme_synthesis.status == "ok"   # Pass C ran
    # Pass E was skipped (no attention_outcome since no scrape)
    assert envelope.envelope_health.pass_e.status == "skipped"
    assert envelope.attention_synthesis.crossings == []


# ---------- Structural pin ----------


def test_envelope_round_trip_via_model_dump_validate(tmp_path):
    """Structural pin: assembled envelope round-trips via Pydantic
    serialize -> validate. The orchestrator's primary contract is that
    its output IS a valid FullBriefEnvelope; this test pins that."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )

    # Round-trip: dump -> validate. Catches any unintentional schema drift.
    dumped = envelope.model_dump(mode="json")
    restored = FullBriefEnvelope.model_validate(dumped)
    assert restored == envelope


# ---------- sink_factory threaded through ----------


def test_sink_factory_passed_through_to_synthesize_window(tmp_path):
    """Orchestrator threads sink_factory through to synthesize_window
    unchanged. The deeper 'sink_factory.assert_called_once() across
    multi-event dispatch' pin is deferred to a synthesize_window-level
    integration test per Mando's Stage 2a-i Check 3 acknowledgment;
    here we pin the orchestrator's responsibility, which is to pass
    the factory through."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    mock_sink_factory = MagicMock(return_value=MagicMock())

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        assemble_full_brief(
            cfg=cfg, no_scrape=True,
            sink_factory=mock_sink_factory,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
        # synthesize_window called exactly once with the same sink_factory
        assert p.synthesize_window_mock.call_count == 1
        called_kwargs = p.synthesize_window_mock.call_args.kwargs
        assert called_kwargs["sink_factory"] is mock_sink_factory


# ---------- 4-case discrimination: archive_failed (the subtle case) ----------


def test_archive_failed_theme_synthesis_status_ok_brief_id_null(tmp_path):
    """Pass C archive_failed case: theme_synthesis.status='ok' (Pass C
    analytical work succeeded) but brief_id=None (no disk artifact).
    envelope_health.pass_c.status='failed' carries the disk-write reason.
    Cost envelope still receives the metadata."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="archive_failed",
            brief=brief,
            brief_path=None,
            reason="brief archive write failed: disk full",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )

    # The subtle contradiction: status="ok" + brief_id=None
    assert envelope.theme_synthesis.status == "ok"
    assert envelope.theme_synthesis.brief_id is None
    assert envelope.theme_synthesis.brief_path is None
    # But the brief's content survived to surface in the envelope
    assert envelope.theme_synthesis.narrative == "Test Pass C narrative."
    assert len(envelope.theme_synthesis.events) == 1
    # envelope_health captures the disk failure
    assert envelope.envelope_health.pass_c.status == "failed"
    assert "disk full" in envelope.envelope_health.pass_c.reason
    # Cost envelope still has Pass C metadata (Stage 1 closing flag)
    assert envelope.cost.pass_c is not None
    assert envelope.cost.pass_c.input_tokens == 3000


# ---------- 4-case discrimination: no_trigger ----------


# ============================================================================
# Stage 2a-ii-B tests (2026-05-29):
#   - T11: window_hours plumbing through Pass C, count_terms, cluster
#   - T11b: threshold_note populated when window_hours != 24
#   - T11c: threshold_note null when window_hours == 24
#   - T16: theses-blind warning surfaces in theme_synthesis
#   - pass_f_footprint helpers — direct unit tests
#   - Empirical pin: cycle 1 (cross_language_event_merges=1,
#     attention_crossings_enabled_by_pass_f=["putin"])
#   - Empirical pin: cycle 2 (cross_language_event_merges=0,
#     attention_crossings_enabled_by_pass_f=[])
#   - url_match_warnings audit signal
# ============================================================================


# ---------- helpers for 2a-ii-B tests ----------


def _make_cfg_with_seeded_db(tmp_path: Path, rows: list[tuple]) -> Config:
    """Like _make_cfg but pre-seeds headlines table with given rows.

    Each row is (headline_id, source, raw_source, headline, headline_en,
    url, language, published_at_unix, fetched_at_unix).
    """
    cfg = _make_cfg(tmp_path)
    conn = sqlite3.connect(str(cfg.db_path))
    for row in rows:
        conn.execute(
            "INSERT INTO headlines (headline_id, source, raw_source, headline, "
            "headline_en, url, language, published_at_unix, fetched_at_unix) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    conn.commit()
    conn.close()
    return cfg


def test_pass_f_translated_rows_in_window_counts_correctly(tmp_path):
    """_compute_translated_rows_in_window: SELECT COUNT for language != 'en'
    AND headline_en IS NOT NULL within the window."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_translated_rows_in_window,
    )
    cfg = _make_cfg_with_seeded_db(tmp_path, [
        ("h1", "telegram:Ateobreaking", None, "Russian text 1", "English 1",
         "https://t.me/Ateobreaking/1", "ru", 1500, 1500),
        ("h2", "telegram:Ateobreaking", None, "Russian text 2", "English 2",
         "https://t.me/Ateobreaking/2", "ru", 1600, 1600),
        ("h3", "rss:bloomberg_politics", None, "Bloomberg headline", None,
         "https://bloomberg.com/x", "en", 1700, 1700),
        ("h_old", "telegram:Ateobreaking", None, "Outside window", "English",
         "https://t.me/Ateobreaking/old", "ru", 500, 500),
    ])
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Window: 1000-2000 covers h1, h2, h3; excludes h_old
        count = _compute_translated_rows_in_window(
            conn, window_since_unix=1000, window_until_unix=2000,
        )
        # Only h1 + h2 are translated AND in window. h3 is en-only. h_old is outside.
        assert count == 2
    finally:
        conn.close()


def test_pass_f_cross_language_event_merges_with_mixed_event(tmp_path):
    """_compute_cross_language_event_merges: event with both 'en' and 'ru'
    source_headlines counts as 1 merge. url_match_warnings null on full match."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_cross_language_event_merges,
    )
    cfg = _make_cfg_with_seeded_db(tmp_path, [
        ("h_en", "rss:bloomberg_economics", None, "Bloomberg", None,
         "https://bloomberg.com/a", "en", 1500, 1500),
        ("h_ru1", "telegram:Ateobreaking", None, "Russian", "English1",
         "https://t.me/Ateobreaking/170839", "ru", 1500, 1500),
        ("h_ru2", "telegram:Ateobreaking", None, "Russian2", "English2",
         "https://t.me/Ateobreaking/170838", "ru", 1500, 1500),
    ])
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Build a Pass C brief with evt-5 (Russia-Kazakhstan) shape from cycle 1
        brief = Brief(
            brief_id="nwd-2026-05-28T12-55-17Z-aaaaaaaa",
            generated_at="2026-05-28T12:55:17Z",
            trigger=Trigger(
                type="event", reason="r",
                window=TriggerWindow(since="x", until="y"),
            ),
            themes_covered=["russia_ukraine_war"],
            events=[Event(
                event_id="evt-5",
                headline_summary="Russia-Kazakhstan deals",
                themes=["russia_ukraine_war"],
                source_headlines=[
                    SourceHeadline(publisher="Bloomberg Economics",
                                   headline="Russia-Kazakhstan FX swap",
                                   url="https://bloomberg.com/a",
                                   published_at="2026-05-28T11:36:54Z"),
                    SourceHeadline(publisher=None,
                                   headline="Putin nuclear plant",
                                   url="https://t.me/Ateobreaking/170839",
                                   published_at="2026-05-28T09:07:12Z"),
                    SourceHeadline(publisher=None,
                                   headline="Tokayev statement",
                                   url="https://t.me/Ateobreaking/170838",
                                   published_at="2026-05-28T09:05:28Z"),
                ],
                materiality_score=0.55,
                thesis_links=[],
            )],
            narrative="x",
            dispatch=Dispatch(alerted=False),
            synthesis_metadata=SynthesisMetadata(
                model_used="x", theses_doc_available=False,
            ),
        )
        merges, warnings = _compute_cross_language_event_merges(
            conn, pass_c_brief=brief,
        )
        # evt-5 has Bloomberg (en) + 2 Ateo (ru) → 1 cross-language merge
        assert merges == 1
        assert warnings is None   # all URLs matched
    finally:
        conn.close()


def test_pass_f_cross_language_event_merges_no_mixed_event(tmp_path):
    """Cycle 2 shape: events have en-only source_headlines → 0 merges."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_cross_language_event_merges,
    )
    cfg = _make_cfg_with_seeded_db(tmp_path, [
        ("h1", "rss:bloomberg_economics", None, "Bloomberg A", None,
         "https://bloomberg.com/a", "en", 1500, 1500),
        ("h2", "rss:bloomberg_markets", None, "Bloomberg B", None,
         "https://bloomberg.com/b", "en", 1500, 1500),
    ])
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        brief = _make_brief(event_headlines=["Iran ceasefire"])
        # Override the source_headline URLs to match seeded DB rows
        brief = brief.model_copy(update={"events": [
            brief.events[0].model_copy(update={
                "source_headlines": [
                    SourceHeadline(publisher="Bloomberg", headline="a",
                                   url="https://bloomberg.com/a",
                                   published_at="2026-05-29T13:00:00Z"),
                    SourceHeadline(publisher="Bloomberg", headline="b",
                                   url="https://bloomberg.com/b",
                                   published_at="2026-05-29T13:01:00Z"),
                ],
            }),
        ]})
        merges, warnings = _compute_cross_language_event_merges(
            conn, pass_c_brief=brief,
        )
        assert merges == 0   # all en
        assert warnings is None
    finally:
        conn.close()


def test_pass_f_cross_language_url_match_warnings_populated_on_missing(tmp_path):
    """Defensive audit signal: when a source_headline URL doesn't match any
    DB row, url_match_warnings carries the count. Mando's Stage 2a-ii-B
    refinement — preserves visibility when Sonnet's URL reproduction
    discipline fails."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_cross_language_event_merges,
    )
    cfg = _make_cfg_with_seeded_db(tmp_path, [
        ("h1", "rss:bloomberg", None, "x", None,
         "https://bloomberg.com/exists", "en", 1500, 1500),
    ])
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        brief = _make_brief()
        brief = brief.model_copy(update={"events": [
            brief.events[0].model_copy(update={
                "source_headlines": [
                    SourceHeadline(publisher="Bloomberg", headline="a",
                                   url="https://bloomberg.com/exists",
                                   published_at="2026-05-29T13:00:00Z"),
                    SourceHeadline(publisher="Bloomberg", headline="b",
                                   url="https://bloomberg.com/MISSING",
                                   published_at="2026-05-29T13:01:00Z"),
                    SourceHeadline(publisher="Other", headline="c",
                                   url="https://example.com/also-missing",
                                   published_at="2026-05-29T13:02:00Z"),
                ],
            }),
        ]})
        merges, warnings = _compute_cross_language_event_merges(
            conn, pass_c_brief=brief,
        )
        # 2 of 3 URLs don't match
        assert warnings == 2
        # Only 1 matched URL, language='en' — no cross-language merge possible
        assert merges == 0
    finally:
        conn.close()


def test_pass_f_crossings_enabled_cycle1_putin_majority_translated(tmp_path):
    """Cycle 1 empirical pin: putin crossing with 7/10 cluster rows
    translated returns ["putin"]. Mirrors cycle 1 (2026-05-28) actual data:
    Ateobreaking dominated the putin cluster."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_attention_crossings_enabled_by_pass_f,
    )
    rows = []
    # 7 Russian translated rows mentioning putin
    for i in range(7):
        rows.append((
            f"h_ru_{i}", "telegram:Ateobreaking", None,
            f"Путин делает что-то {i}",
            f"Putin does something {i}",
            f"https://t.me/Ateobreaking/{170839 + i}",
            "ru", 1500 + i, 1500 + i,
        ))
    # 3 English rows mentioning putin
    for i in range(3):
        rows.append((
            f"h_en_{i}", "rss:bloomberg_politics", None,
            f"Putin meets Tokayev {i}", None,
            f"https://bloomberg.com/putin-{i}",
            "en", 1500 + 100 + i, 1500 + 100 + i,
        ))
    cfg = _make_cfg_with_seeded_db(tmp_path, rows)
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        attention_briefs = [_make_attention_brief(triggering_term="putin")]
        enabled = _compute_attention_crossings_enabled_by_pass_f(
            conn,
            attention_briefs=attention_briefs,
            canonical_now_unix=2000,
            window_hours=24,
        )
        # 7/10 = 0.70 > 0.50 → enabled
        assert enabled == ["putin"]
    finally:
        conn.close()


def test_pass_f_crossings_enabled_cycle2_secretary_majority_english(tmp_path):
    """Cycle 2 empirical pin: secretary crossing dominated by English (CIG)
    returns []. Mirrors cycle 2 (2026-05-29) actual: 8/11 from CIG_telegram
    (English), only 3/11 from Ateo. 3/11 = 0.27 < 0.50."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_attention_crossings_enabled_by_pass_f,
    )
    rows = []
    # 8 English rows mentioning secretary (from CIG)
    for i in range(8):
        rows.append((
            f"h_en_{i}", "telegram:CIG_telegram", None,
            f"Treasury Secretary Bessent meets {i}", None,
            f"https://t.me/CIG_telegram/{100 + i}",
            "en", 1500 + i, 1500 + i,
        ))
    # 3 Russian translated rows mentioning secretary
    for i in range(3):
        rows.append((
            f"h_ru_{i}", "telegram:Ateobreaking", None,
            f"Секретарь России {i}",
            f"Russian Secretary {i}",
            f"https://t.me/Ateobreaking/{200 + i}",
            "ru", 1500 + 50 + i, 1500 + 50 + i,
        ))
    cfg = _make_cfg_with_seeded_db(tmp_path, rows)
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        attention_briefs = [_make_attention_brief(
            brief_id="nwd-attn-2026-05-29T14-31-21Z-ee1bc8e0",
            triggering_term="secretary",
        )]
        enabled = _compute_attention_crossings_enabled_by_pass_f(
            conn,
            attention_briefs=attention_briefs,
            canonical_now_unix=2000,
            window_hours=24,
        )
        # 3/11 = 0.27 < 0.50 → not enabled
        assert enabled == []
    finally:
        conn.close()


def test_pass_f_crossings_enabled_iteration_order_preserved(tmp_path):
    """Ordering pin: when multiple crossings are enabled, the output list
    preserves the input AttentionBrief order (same iteration-order discipline
    as convergence)."""
    from news_watch_daemon.fullbrief.orchestrator import (
        _compute_attention_crossings_enabled_by_pass_f,
    )
    rows = []
    # 6 Russian rows for "putin" (majority)
    for i in range(6):
        rows.append((
            f"hp_{i}", "telegram:Ateobreaking", None,
            f"Путин что-то {i}",
            f"Putin something {i}",
            f"https://t.me/Ateobreaking/p{i}",
            "ru", 1500 + i, 1500 + i,
        ))
    # 4 Russian rows for "iranian" (majority)
    for i in range(4):
        rows.append((
            f"hi_{i}", "telegram:Ateobreaking", None,
            f"Иранский что-то {i}",
            f"Iranian something {i}",
            f"https://t.me/Ateobreaking/i{i}",
            "ru", 1500 + 10 + i, 1500 + 10 + i,
        ))
    cfg = _make_cfg_with_seeded_db(tmp_path, rows)
    conn = sqlite3.connect(str(cfg.db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Input order: iranian first, then putin — verify output preserves it
        briefs = [
            _make_attention_brief(brief_id="nwd-attn-x-1", triggering_term="iranian"),
            _make_attention_brief(brief_id="nwd-attn-x-2", triggering_term="putin"),
        ]
        enabled = _compute_attention_crossings_enabled_by_pass_f(
            conn, attention_briefs=briefs,
            canonical_now_unix=2000, window_hours=24,
        )
        assert enabled == ["iranian", "putin"]
    finally:
        conn.close()


def test_t11b_threshold_note_populated_at_non_24h_window(tmp_path):
    """T11b: threshold_note populates with the Adjustment 2 text when
    window_hours != 24."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True, window_hours=6,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
    assert envelope.frequency_diagnostic.threshold_note is not None
    assert "tuned for 24h" in envelope.frequency_diagnostic.threshold_note
    assert "6h" in envelope.frequency_diagnostic.threshold_note


def test_t11c_threshold_note_null_at_default_24h_window(tmp_path):
    """T11c: threshold_note stays null at the default 24h window."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True, window_hours=24,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
    assert envelope.frequency_diagnostic.threshold_note is None


def test_t11_window_hours_threaded_to_synthesize_window(tmp_path):
    """T11: window_hours flows from assemble_full_brief through to
    synthesize_window via the kwarg, preserving the Q-2a-ii-2 plumb-through."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        assemble_full_brief(
            cfg=cfg, no_scrape=True, window_hours=6,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
        called_kwargs = p.synthesize_window_mock.call_args.kwargs
        assert called_kwargs["window_hours"] == 6


def test_t16_theses_blind_warning_surfaces_in_theme_synthesis(tmp_path):
    """T16: when NEWS_WATCH_THESES_PATH is unset (the synthesis_metadata
    carries the warning text), the orchestrator surfaces it in
    theme_synthesis.theses_doc_warning at the top of that section."""
    cfg = _make_cfg(tmp_path)
    # Brief comes with theses_doc_available=False + theses_doc_warning set
    brief = _make_brief()
    assert brief.synthesis_metadata.theses_doc_warning  # sanity: warning set
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
    assert envelope.theme_synthesis.theses_doc_warning is not None
    assert "NEWS_WATCH_THESES_PATH" in envelope.theme_synthesis.theses_doc_warning
    assert envelope.theme_synthesis.theses_doc_available is False


def test_count_terms_re_run_uses_scrape_attention_window_when_available(tmp_path):
    """Check 3 alignment pin: when scrape's auto-attention succeeded, the
    orchestrator's count_terms re-run for the near-miss table MUST use
    the same window_until_unix scrape's count_terms used — otherwise the
    two windows drift by the scrape duration, and a term with exactly
    the threshold count at one boundary but one fewer at the other would
    cross in scrape's view but appear in the orchestrator's near-miss
    table, contradicting itself.

    See the alignment comment block in fullbrief/orchestrator.py Step 6.
    """
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    # Use a scrape window_until_unix that DIFFERS from the orchestrator's
    # now_unix. The orchestrator's `now` is fixed to 2026-05-29T14:32:47Z
    # (unix 1780069967). Scrape's attention window_until_unix is set to
    # 1780069977 (T0 + 10s, simulating scrape duration). The orchestrator's
    # count_terms call must use 1780069977, NOT 1780069967.
    scrape_attention_window_until = 1780069977
    scrape_outcome = _make_attention_outcome()
    scrape_outcome["window_until_unix"] = scrape_attention_window_until
    scrape_outcome["window_since_unix"] = scrape_attention_window_until - 24 * 3600

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        with patch(
            "news_watch_daemon.fullbrief.orchestrator._do_scrape_step",
            return_value=(
                _StepHealth(status="ok", headlines_inserted=10, sources_failed=0),
                scrape_outcome,
            ),
        ):
            assemble_full_brief(
                cfg=cfg, no_scrape=False,
                now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
            )
        # Verify the count_terms re-run received the SCRAPE'S window_until_unix
        # rather than the orchestrator's own now_unix.
        count_terms_mock = p.count_terms_collapsed.target.count_terms_collapsed
        # The mock was registered via patch.object/patch at module level;
        # inspect the most recent call's kwargs.
        assert count_terms_mock.called
        last_call_kwargs = count_terms_mock.call_args.kwargs
        assert last_call_kwargs["now_unix"] == scrape_attention_window_until, (
            f"orchestrator count_terms must align to scrape's attention "
            f"window_until_unix ({scrape_attention_window_until}); got "
            f"{last_call_kwargs['now_unix']}"
        )


def test_count_terms_re_run_falls_back_to_now_unix_when_no_scrape(tmp_path):
    """When no_scrape=True (no scrape attention to align with), the
    orchestrator uses its own now_unix for count_terms — backwards-compat
    pin against accidental break of the no-scrape path."""
    cfg = _make_cfg(tmp_path)
    brief = _make_brief()
    expected_now_unix = int(datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc).timestamp())

    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="synthesized", brief=brief,
            brief_path=tmp_path / "briefs" / "fake.json",
        )
        assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )
        count_terms_mock = p.count_terms_collapsed.target.count_terms_collapsed
        last_call_kwargs = count_terms_mock.call_args.kwargs
        assert last_call_kwargs["now_unix"] == expected_now_unix


def test_no_trigger_narrative_matches_q2_text(tmp_path):
    """Q2 resolution: no_trigger narrative reads the exact Q2 informational text."""
    cfg = _make_cfg(tmp_path)
    with _Patches() as p:
        p.synthesize_window_mock.return_value = _make_synthesize_result(
            status="no_trigger",
            reason="gate: no themes crossed threshold",
        )
        envelope = assemble_full_brief(
            cfg=cfg, no_scrape=True,
            now=datetime(2026, 5, 29, 14, 32, 47, tzinfo=timezone.utc),
        )

    assert envelope.theme_synthesis.status == "no_trigger"
    assert envelope.theme_synthesis.brief_id is None
    assert envelope.theme_synthesis.no_trigger_reason == "gate: no themes crossed threshold"
    assert (
        "Pass C trigger gate did not fire" in envelope.theme_synthesis.narrative
    )
    assert "informational, not an error" in envelope.theme_synthesis.narrative
