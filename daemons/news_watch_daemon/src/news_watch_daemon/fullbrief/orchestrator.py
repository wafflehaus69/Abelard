"""Full Brief composition orchestrator — Steps 1-7 envelope assembly.

Per Abelard's Full Brief spec v1.0 + 2026-05-29 amendments. Composes:
  - scrape_cycle (with run_attention_cycle as the auto-attention callback)
  - synthesize_window (Pass C theme-event synthesis)
  - Pass E extraction from scrape's embedded attention_outcome (Q3 resolution)
  - analyze_convergence (Stage 1 leaf)
  - assemble_near_misses (Stage 1 leaf)
  - assemble_cost_envelope (Stage 1 leaf)
into one FullBriefEnvelope artifact written to disk.

Stage 2a-ii-A scope (this commit):
  - Steps 1-7 happy path
  - 4-case Pass C discrimination on SynthesizeResult.status:
      synthesized       -> standard path
      no_trigger        -> ThemeSynthesisSection.status="no_trigger",
                           brief_id=null, narrative carries the Q2-spec text
      synthesis_failed  -> ThemeSynthesisSection.status="failed",
                           pass_failures gets an entry, attention path still runs
      archive_failed    -> ThemeSynthesisSection.status="ok" (Pass C
                           semantically succeeded), brief content from
                           result.brief, brief_id=null (no disk artifact),
                           envelope_health.pass_c.reason carries the
                           archive failure detail, cost envelope still
                           receives result.metadata
                           ** SEE archive_failed COMMENT IN _build_theme_synthesis **
  - pass_failures array population for each step failure
  - Cost-telemetry-before-write discipline preserved end-to-end

Stage 2a-ii-B (next):
  - pass_f_footprint DB-query computation (currently stubbed with zeros)
  - threshold_note plumbing when window_hours != 24
  - theses-blind warning re-surfacing
  - Tests T11/T11b/T11c/T16
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..alert.sink import AlertSink
from ..attention.brief_schema import AttentionBrief
from ..attention.cluster import cluster_for_term
from ..attention.counter import count_terms_collapsed
from ..attention.orchestrator import run_attention_cycle
from ..attention.stopwords import StopwordsError, load_stopwords
from ..config import Config
from ..db import connect, schema_version
from ..http_client import HttpClient
from ..scrape.factory import build_sources
from ..scrape.orchestrator import ScrapeCycleResult, scrape_cycle
from ..scrape.ticker_extract import TickerExtractError, load_tracked_tickers
from ..synthesize.archive import ArchiveError, read_brief, write_brief
from ..synthesize.brief import SynthesisMetadata
from ..synthesize.config import SynthesisConfigError, load_synthesis_config
from ..synthesize.synthesize import (
    SynthesisError,
    SynthesizeResult,
    build_anthropic_client,
    synthesize_window,
)
from ..theme_config import ThemeLoadError, load_all_themes
from ..translation import load_translation_config
from ..translation.config import TranslationConfigError
from .brief import (
    AttentionCrossing,
    AttentionSynthesisSection,
    ConvergenceInfo,
    CostEnvelope,
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
    ThemeSegment,
    ThemeSegmentsSection,
    ThemeSynthesisSection,
    WindowSection,
)
from .convergence import analyze_convergence
from .cost import assemble_cost_envelope
from .frequency_diagnostic import assemble_near_misses
from .theme_segments import (
    SAMPLE_HEADLINES_PER_THEME,
    ThemeSegmentInput,
    ThemeSegmentsError,
    synthesize_theme_segments,
    template_summary,
)


_LOG = logging.getLogger("news_watch_daemon.fullbrief.orchestrator")


_NO_TRIGGER_NARRATIVE = (
    "Pass C trigger gate did not fire — no theme crossed materiality "
    "threshold this window. This is informational, not an error."
)
_FREQ_DIAGNOSTIC_NOTE = (
    "Near-miss table surfaces dominant-but-non-novel terms. High "
    "delta_ratio with elevated prior may indicate sustained-attention "
    "signal (term has been dominant across multiple cycles)."
)
# Adjustment 2 (2026-05-29): threshold_note populated when window_hours != 24
# so programmatic consumers know Pass E thresholds (absolute counts tuned for
# 24h) may produce different crossing densities at non-default windows. Stage
# 2a-ii-B plumb-through.
_THRESHOLD_NOTE_TEMPLATE = (
    "Pass E thresholds are tuned for 24h windows; non-24h windows may "
    "produce fewer or more crossings than expected. Window for this brief: "
    "{window_hours}h. Review the near-miss table for signal."
)

# Cross-language Pass F threshold per Adjustment 5: a crossing is "enabled
# by Pass F" when STRICTLY MORE THAN this fraction of its cluster rows are
# translated (language != 'en'). 0.50 = "majority translated".
_PASS_F_ENABLED_THRESHOLD = 0.50

# Used ONLY in the orchestrator's exception-handler fallback path when
# _build_pass_f_footprint raises mid-query (DB connection failure, etc.).
# Real metric computation always returns from _build_pass_f_footprint directly.
# This constant exists to make the fallback semantically distinct from a
# successful zero-result (e.g., a quiet window with zero translated rows
# legitimately producing zeros — distinguishable by pass_failures entry).
_PASS_F_FOOTPRINT_UNAVAILABLE = PassFFootprint(
    translated_rows_in_window=0,
    cross_language_event_merges=0,
    attention_crossings_enabled_by_pass_f=[],
)


def _iso_from_unix(ts: int) -> str:
    """Format a unix timestamp as ISO-8601 UTC with `Z` suffix."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Step 2 helper — scrape
# ---------------------------------------------------------------------------


def _do_scrape_step(
    cfg: Config,
    conn: sqlite3.Connection,
) -> tuple[StepHealth, dict[str, Any] | None]:
    """Step 2: execute one scrape sweep via scrape_cycle.

    Returns (scrape_health, attention_outcome). attention_outcome is the
    raw nested dict from scrape's auto-attention follow-on; Step 4 extracts
    AttentionBriefs from it.

    Pre-flight failures (theme load, ticker load, etc.) surface as
    StepHealth(status="failed", reason=...). The Full Brief still
    assembles — degraded data, but no exception propagation.
    """
    try:
        all_themes = load_all_themes(cfg.themes_dir)
    except ThemeLoadError as exc:
        return (
            StepHealth(status="failed", reason=f"theme load failed: {exc}"),
            None,
        )
    active_themes = [t for t in all_themes if t.status == "active"]
    if not active_themes:
        return (
            StepHealth(status="failed", reason="no active themes in registry"),
            None,
        )

    http = HttpClient(
        user_agent=cfg.http_user_agent,
        default_timeout_s=cfg.http_default_timeout_s,
    )
    sources = build_sources(cfg, active_themes, http)

    try:
        tracked_tickers = load_tracked_tickers(cfg.tracked_tickers_path)
    except TickerExtractError as exc:
        return (
            StepHealth(status="failed", reason=f"tickers load failed: {exc}"),
            None,
        )

    # Translation config — failures degrade to "translation disabled" rather
    # than failing the scrape step entirely, mirroring CLI handler discipline.
    translation_credentials: tuple[int, str, str] | None = None
    translation_source = "telegram_native"
    translation_batch_size = 10
    try:
        tx_cfg = load_translation_config(cfg.translation_config_path)
        translation_source = tx_cfg.translation_source
        translation_batch_size = tx_cfg.telegram_native_batch_size
    except TranslationConfigError as exc:
        _LOG.warning(
            "translation config load failed: %s. Scrape continues with "
            "translation disabled.", exc,
        )
    if cfg.telegram_creds_complete and translation_source == "telegram_native":
        translation_credentials = (
            cfg.telegram_api_id,  # type: ignore[arg-type]
            cfg.telegram_api_hash,  # type: ignore[arg-type]
            cfg.telegram_session_string,  # type: ignore[arg-type]
        )

    def _attention_callback() -> dict[str, Any]:
        return run_attention_cycle(cfg=cfg, conn=conn, dry_run=False)

    cycle_result: ScrapeCycleResult = scrape_cycle(
        conn=conn,
        sources=sources,
        themes=active_themes,
        tracked_tickers=tracked_tickers,
        cross_source_log_path=cfg.cross_source_log_path,
        ingest_filter_log_path=cfg.filtered_log_path,
        translation_credentials=translation_credentials,
        translation_source=translation_source,
        translation_batch_size=translation_batch_size,
        attention_callback=_attention_callback,
    )

    if cycle_result.status == "scrape_failed":
        return (
            StepHealth(
                status="failed",
                reason=cycle_result.reason or "scrape_cycle returned scrape_failed",
            ),
            None,
        )

    assert cycle_result.scrape_result is not None
    health = StepHealth(
        status="ok",
        headlines_inserted=cycle_result.scrape_result.headlines_inserted_total,
        sources_failed=cycle_result.scrape_result.sources_failed,
    )
    return health, cycle_result.attention_outcome


# ---------------------------------------------------------------------------
# Step 3 helper — Pass C synthesis + 4-case discrimination
# ---------------------------------------------------------------------------


def _do_pass_c_step(
    cfg: Config,
    conn: sqlite3.Connection,
    *,
    window_hours: int,
    sink_factory: Callable[[], AlertSink] | None,
    when: datetime,
) -> tuple[StepHealth, ThemeSynthesisSection, SynthesisMetadata | None, Any | None]:
    """Step 3: execute Pass C via synthesize_window.

    Returns (pass_c_health, theme_synthesis_section, metadata, brief).

    `brief` is the Brief Pydantic object (or None for failure cases) — needed
    for Step 5 convergence analysis, which reads event.source_headlines[].
    headline (a field absent from the denormalized ThemeEventDigest).
    Production callers (assemble_full_brief) plumb this through to
    analyze_convergence().

    Metadata is populated whenever Sonnet was called — including the
    archive_failed case — so cost telemetry survives disk-write failure
    (Stage 1 closing flag discipline).

    Pre-flight failures (synth_cfg load, theme load, client construction)
    return status="failed" with reason in StepHealth; theme_synthesis_section
    gets status="failed" + failure_reason. brief is None for failures.
    """
    # Load synthesis config.
    try:
        synth_cfg = load_synthesis_config(cfg.synthesis_config_path)
    except SynthesisConfigError as exc:
        reason = f"synthesis_config load failed: {exc}"
        return (
            StepHealth(status="failed", reason=reason),
            ThemeSynthesisSection(status="failed", failure_reason=reason),
            None, None,
        )

    # Load + filter active themes.
    try:
        all_themes = load_all_themes(cfg.themes_dir)
    except ThemeLoadError as exc:
        reason = f"theme load failed: {exc}"
        return (
            StepHealth(status="failed", reason=reason),
            ThemeSynthesisSection(status="failed", failure_reason=reason),
            None, None,
        )
    active_themes = [t for t in all_themes if t.status == "active"]

    # Anthropic client.
    if not cfg.anthropic_api_key:
        reason = "ANTHROPIC_API_KEY not set"
        return (
            StepHealth(status="failed", reason=reason),
            ThemeSynthesisSection(status="failed", failure_reason=reason),
            None, None,
        )
    try:
        anthropic_client = build_anthropic_client(cfg.anthropic_api_key)
    except SynthesisError as exc:
        reason = f"anthropic client construction failed: {exc}"
        return (
            StepHealth(status="failed", reason=reason),
            ThemeSynthesisSection(status="failed", failure_reason=reason),
            None, None,
        )

    # Delegate to pure callable. synthesize_window catches its own
    # (SynthesisError, SynthesisLLMError), but raw Anthropic SDK errors
    # (rate-limit, timeout, connection, 5xx) bubble up untouched per
    # synthesize_brief's contract. Catch them HERE so a transient API hiccup
    # degrades Pass C to a recovered failure and the Full Brief still renders
    # (scrape + attention + theme segments + PDF) instead of aborting the whole
    # run with no output — the same graceful-degradation the orchestrator
    # already applies to scrape / pass_e / theme_segments / pass_f.
    try:
        result: SynthesizeResult = synthesize_window(
            conn=conn,
            active_themes=active_themes,
            brief_archive_path=cfg.brief_archive_path,
            trigger_log_path=cfg.trigger_log_path,
            theses_path=cfg.theses_path,
            synth_cfg=synth_cfg,
            anthropic_client=anthropic_client,
            sink_factory=sink_factory,
            window_hours=window_hours,
            dry_run=False,
            now=when,
        )
    except Exception as exc:  # noqa: BLE001 — anthropic.* SDK errors + defensive
        reason = f"synthesis call raised: {type(exc).__name__}: {exc}"
        _LOG.warning("Pass C degraded (Full Brief still renders): %s", reason)
        return (
            StepHealth(status="failed", reason=reason),
            ThemeSynthesisSection(status="failed", failure_reason=reason),
            None, None,
        )
    return _build_theme_synthesis(result, cfg.brief_archive_path)


def _build_theme_synthesis(
    result: SynthesizeResult,
    brief_archive_path: Path,
) -> tuple[StepHealth, ThemeSynthesisSection, SynthesisMetadata | None, Any | None]:
    """Convert SynthesizeResult to (StepHealth, ThemeSynthesisSection, metadata).

    Implements the 4-case Pass C discrimination per Mando's Stage 2a-ii-A
    forward-guidance. SEE THE archive_failed COMMENT BELOW — it's the
    subtle case where status="ok" coexists with brief_id=null, representing
    "the work was done and we know what it produced, we just couldn't
    persist it to disk."
    """
    if result.status == "synthesized":
        assert result.brief is not None
        assert result.metadata is not None
        assert result.brief_path is not None
        events = _events_to_digests(result.brief.events)
        tally = _direction_tally(result.brief.events)
        return (
            StepHealth(status="ok"),
            ThemeSynthesisSection(
                status="ok",
                brief_id=result.brief.brief_id,
                brief_path=str(result.brief_path),
                narrative=result.brief.narrative,
                themes_covered=list(result.brief.themes_covered),
                events=events,
                direction_tally=tally,
                theses_doc_available=result.metadata.theses_doc_available,
                theses_doc_warning=result.metadata.theses_doc_warning,
            ),
            result.metadata,
            result.brief,
        )

    if result.status == "no_trigger":
        return (
            StepHealth(status="ok"),
            ThemeSynthesisSection(
                status="no_trigger",
                narrative=_NO_TRIGGER_NARRATIVE,
                no_trigger_reason=result.reason,
            ),
            None,
            None,
        )

    if result.status == "synthesis_failed":
        return (
            StepHealth(status="failed", reason=result.reason),
            ThemeSynthesisSection(
                status="failed",
                failure_reason=result.reason,
            ),
            result.metadata,   # may still be populated if validation failed post-SDK call
            None,
        )

    if result.status == "archive_failed":
        # ARCHIVE_FAILED — THE SUBTLE FOURTH CASE (Mando 2026-05-29):
        #
        # Pass C semantically succeeded: Sonnet returned a valid Brief and we
        # validated it. The archive WRITE to disk failed, so the standalone
        # Pass C brief artifact doesn't exist on disk — but we DO know what
        # it would have contained (it's in result.brief).
        #
        # Discrimination choice:
        #   theme_synthesis.status = "ok"   -- Pass C's analytical work succeeded
        #   theme_synthesis.brief_id = None -- no addressable disk artifact
        #   theme_synthesis.brief_path = None -- nothing to link to
        #   envelope_health.pass_c.status = "failed" -- the step's INFRASTRUCTURE
        #                                    failed; reason carries the archive
        #                                    error detail so the operator can
        #                                    diagnose (disk full, perms, etc.)
        #   cost envelope: receives result.metadata, billing the API call we did
        #                  make — Stage 1 closing flag's "telemetry survives
        #                  disk-write failure" discipline.
        #
        # The contradiction (`status="ok"` despite `brief_id=null`) is
        # deliberate: future readers must understand "ok" means "Pass C
        # succeeded analytically," not "we wrote a brief artifact."
        # envelope_health is the place that records the disk-write failure;
        # don't conflate the two.
        assert result.brief is not None
        assert result.metadata is not None
        events = _events_to_digests(result.brief.events)
        tally = _direction_tally(result.brief.events)
        return (
            StepHealth(
                status="failed",
                reason=result.reason or "archive write failed",
            ),
            ThemeSynthesisSection(
                status="ok",
                brief_id=None,
                brief_path=None,
                narrative=result.brief.narrative,
                themes_covered=list(result.brief.themes_covered),
                events=events,
                direction_tally=tally,
                theses_doc_available=result.metadata.theses_doc_available,
                theses_doc_warning=result.metadata.theses_doc_warning,
            ),
            result.metadata,
            result.brief,
        )

    # dry_run is not a valid path for assemble_full_brief — synthesize_window
    # was called with dry_run=False above. Surfacing as failed for safety.
    return (
        StepHealth(status="failed", reason=f"unexpected status: {result.status}"),
        ThemeSynthesisSection(
            status="failed",
            failure_reason=f"unexpected synthesize_window status: {result.status}",
        ),
        result.metadata,
        None,
    )


def _events_to_digests(events: list[Any]) -> list[ThemeEventDigest]:
    """Convert Pass C Event list to ThemeEventDigest list.

    Iteration order preserved — convergence analysis in Step 5 will iterate
    these in the SAME order, so converges_with reflects pass_c_events list
    order deterministically (per Mando's 2026-05-29 forward-guidance).
    """
    digests: list[ThemeEventDigest] = []
    for ev in events:
        direction: str | None = None
        if ev.thesis_links:
            direction = ev.thesis_links[0].direction
        digests.append(ThemeEventDigest(
            event_id=ev.event_id,
            headline_summary=ev.headline_summary,
            themes=list(ev.themes),
            materiality_score=ev.materiality_score,
            direction=direction,  # type: ignore[arg-type]
            source_count=len(ev.source_headlines),
            thesis_links=[tl.model_dump(mode="json") for tl in ev.thesis_links],
        ))
    return digests


def _direction_tally(events: list[Any]) -> dict[str, int]:
    """Build {confirm, ambiguous, break} count tally from event thesis_links."""
    tally = {"confirm": 0, "ambiguous": 0, "break": 0}
    for ev in events:
        for tl in ev.thesis_links:
            if tl.direction in tally:
                tally[tl.direction] += 1
    return tally


# ---------------------------------------------------------------------------
# Step 4 helper — Pass E extraction from scrape's attention_outcome
# ---------------------------------------------------------------------------


def _extract_pass_e_step(
    attention_outcome: dict[str, Any] | None,
    archive_root: Path,
) -> tuple[StepHealth, list[AttentionBrief], list[Path]]:
    """Step 4: extract AttentionBriefs from scrape's attention_outcome.

    Per Q3 resolution: NO re-execution of Pass E. We extract the brief
    IDs scrape's auto-attention already generated, load them from disk,
    and surface their contents.

    Returns (pass_e_health, attention_briefs, brief_paths). The two
    output lists are parallel (same indices).

    Failure modes:
      - attention_outcome is None (scrape didn't run, no_scrape=True OR
        scrape failed): status="skipped" with reason
      - attention_outcome["status"] != "ok": status="failed" with reason
      - Per-brief read failure: skipped silently, logged at WARNING
        (one bad brief doesn't kill the rest)
    """
    if attention_outcome is None:
        return StepHealth(status="skipped", reason="no scrape run"), [], []

    outcome_status = attention_outcome.get("status")
    if outcome_status != "ok":
        return (
            StepHealth(
                status="failed",
                reason=attention_outcome.get("reason") or f"attention status={outcome_status!r}",
            ),
            [],
            [],
        )

    briefs: list[AttentionBrief] = []
    paths: list[Path] = []
    for per_term in attention_outcome.get("per_term", []):
        if not per_term.get("success"):
            continue
        brief_id = per_term.get("brief_id")
        if not brief_id:
            continue
        try:
            loaded = read_brief(archive_root, brief_id)
        except ArchiveError as exc:
            _LOG.warning(
                "Pass E brief load failed for %s: %s. Skipping crossing.",
                brief_id, exc,
            )
            continue
        if not isinstance(loaded, AttentionBrief):
            _LOG.warning(
                "Pass E brief %s loaded as wrong type %s. Skipping.",
                brief_id, type(loaded).__name__,
            )
            continue
        briefs.append(loaded)
        archive_path_str = per_term.get("archive_path")
        paths.append(Path(archive_path_str) if archive_path_str else Path(""))

    return (
        StepHealth(status="ok", crossings_count=len(briefs)),
        briefs,
        paths,
    )


# ---------------------------------------------------------------------------
# Step 5 + 6 helpers — convergence + frequency diagnostic
# ---------------------------------------------------------------------------


def _build_attention_synthesis_with_convergence(
    attention_briefs: list[AttentionBrief],
    brief_paths: list[Path],
    pass_e_health: StepHealth,
    theme_events_for_convergence: list[Any],
) -> tuple[AttentionSynthesisSection, list[AttentionCrossing]]:
    """Step 5: build AttentionSynthesisSection with per-crossing ConvergenceInfo.

    Convergence iteration order is observable (per Mando's 2026-05-29
    forward-guidance): for each AttentionBrief, iterate theme_events
    IN LIST ORDER and emit `converges_with` in the SAME order. Tests
    can rely on the order being stable across runs.

    Returns (attention_synthesis_section, crossings_list). The crossings
    list is also returned separately so Step 6 can use them for the
    frequency_diagnostic.crossings table without re-iterating.
    """
    section_status: str = "ok" if pass_e_health.status != "failed" else "failed"
    crossings: list[AttentionCrossing] = []

    for ab, path in zip(attention_briefs, brief_paths):
        # Convergence per Q6 + Adjustment 5: strict-headline ASCII substring.
        # analyze_convergence iterates theme_events_for_convergence in the
        # passed list order — pinned in Stage 1 leaf module.
        # We pass the original Event list (not the digests) because
        # analyze_convergence reads event.source_headlines[].headline,
        # which exists on the Event Pydantic model.
        cr = analyze_convergence(
            triggering_term=ab.triggering_term,
            pass_c_events=theme_events_for_convergence,
        )
        convergence_info = ConvergenceInfo(
            status=cr.status,
            converges_with=cr.converges_with,
            orphan_reason=cr.orphan_reason,
        )

        delta_ratio = ab.term_frequency_window / max(ab.term_frequency_prior, 1)
        # Full attention-brief narrative (was capped at 280 chars) — the orphan
        # crossings are the "review first" items; the operator wants the whole
        # LLM read, not a teaser (2026-07-08).
        llm_read_summary = ab.narrative or ""
        crossings.append(AttentionCrossing(
            term=ab.triggering_term,
            freq_window=ab.term_frequency_window,
            freq_prior=ab.term_frequency_prior,
            delta_ratio=delta_ratio,
            shape=ab.attention_shape,
            attention_brief_id=ab.brief_id,
            attention_brief_path=str(path),
            convergence=convergence_info,
            llm_read_summary=llm_read_summary,
        ))

    return (
        AttentionSynthesisSection(
            status=section_status,  # type: ignore[arg-type]
            crossings=crossings,
            failure_reason=pass_e_health.reason if section_status == "failed" else None,
        ),
        crossings,
    )


def _build_frequency_diagnostic(
    conn: sqlite3.Connection,
    now_unix: int,
    window_hours: int,
    cfg: Config,
    attention_briefs: list[AttentionBrief],
    crossings: list[AttentionCrossing],
) -> tuple[StepHealth, FrequencyDiagnosticSection]:
    """Step 6: build FrequencyDiagnosticSection.

    Re-runs count_terms (cheap — counter only, no LLM) to obtain fresh
    window/prior counts for the near-miss table. The Pass E attention_
    outcome from scrape only carries the THRESHOLD-CROSSING terms; we
    need the full count map for the unbounded near-miss surfacing per
    Adjustment 1.

    threshold_note is stubbed to None in Stage 2a-ii-A; Stage 2a-ii-B
    populates it when window_hours != 24 per Adjustment 2.
    """
    # Adjustment 2 (Stage 2a-ii-B): threshold_note populated when window_hours
    # != 24 so programmatic consumers know Pass E's absolute thresholds are
    # tuned for 24h. Null at default window keeps the field absent from
    # render output in the dominant case.
    threshold_note: str | None = (
        None if window_hours == 24
        else _THRESHOLD_NOTE_TEMPLATE.format(window_hours=window_hours)
    )

    try:
        stopwords = load_stopwords(cfg.stopwords_path)
    except StopwordsError as exc:
        return (
            StepHealth(status="failed", reason=f"stopwords load failed: {exc}"),
            FrequencyDiagnosticSection(
                threshold_note=threshold_note,
                crossings=[],
                near_misses=[],
                diagnostic_note=_FREQ_DIAGNOSTIC_NOTE,
            ),
        )

    try:
        term_counts = count_terms_collapsed(
            conn,
            now_unix=now_unix,
            stopwords=stopwords,
            window_hours=window_hours,
        )
    except Exception as exc:  # noqa: BLE001 — surface any counter failure
        return (
            StepHealth(status="failed", reason=f"count_terms_collapsed failed: {exc}"),
            FrequencyDiagnosticSection(
                threshold_note=threshold_note,
                crossings=[],
                near_misses=[],
                diagnostic_note=_FREQ_DIAGNOSTIC_NOTE,
            ),
        )

    crossing_terms = [ab.triggering_term for ab in attention_briefs]
    near_miss_terms = assemble_near_misses(
        window_counts=term_counts.window_counts,
        prior_counts=term_counts.prior_counts,
        crossing_terms=crossing_terms,
        stopwords=stopwords,
    )

    # Crossings table — pull from already-built AttentionCrossing list.
    # Convergence status field is copied from the per-crossing analysis.
    crossings_rows: list[FrequencyDiagnosticCrossingRow] = []
    convergence_by_term = {c.term: c.convergence.status for c in crossings}
    for ab in attention_briefs:
        crossings_rows.append(FrequencyDiagnosticCrossingRow(
            term=ab.triggering_term,
            freq_window=ab.term_frequency_window,
            freq_prior=ab.term_frequency_prior,
            shape=ab.attention_shape,
            convergence=convergence_by_term.get(ab.triggering_term, "unknown"),  # type: ignore[arg-type]
        ))

    near_misses_rows: list[FrequencyDiagnosticNearMissRow] = []
    for nm in near_miss_terms:
        near_misses_rows.append(FrequencyDiagnosticNearMissRow(
            term=nm.term,
            freq_window=nm.freq_window,
            freq_prior=nm.freq_prior,
            delta_ratio=nm.delta_ratio,
            reason_not_crossed=nm.reason_not_crossed,  # type: ignore[arg-type]
        ))

    return (
        StepHealth(status="ok"),
        FrequencyDiagnosticSection(
            threshold_note=threshold_note,
            crossings=crossings_rows,
            near_misses=near_misses_rows,
            diagnostic_note=_FREQ_DIAGNOSTIC_NOTE,
        ),
    )


# ---------------------------------------------------------------------------
# Step 6.5 — dedicated theme segments (guaranteed per-theme coverage)
# ---------------------------------------------------------------------------


def _gather_theme_inputs(
    conn: sqlite3.Connection,
    *,
    active_theme_ids: list[str],
    display_names: dict[str, str],
    window_since_unix: int,
    window_until_unix: int,
    themes_in_scope: set[str],
    crossing_terms: list[str],
) -> list[ThemeSegmentInput]:
    """DB-gather per-theme tag counts + sample headlines, derive convergence.

    Per-theme-distinct: one headline_theme_tags row per (headline, theme).
    Sample headlines are the most-recent SAMPLE_HEADLINES_PER_THEME in
    window. Convergence terms = crossing terms that ASCII-substring-appear
    in a theme's sample headlines (bounded, sample-based approximation).
    """
    rows = conn.execute(
        "SELECT t.theme_id, COALESCE(h.headline_en, h.headline) AS headline "
        "FROM headline_theme_tags t "
        "JOIN headlines h ON t.headline_id = h.headline_id "
        "WHERE h.published_at_unix > ? AND h.published_at_unix <= ? "
        "ORDER BY h.published_at_unix DESC",
        (window_since_unix, window_until_unix),
    ).fetchall()

    counts: dict[str, int] = {}
    samples: dict[str, list[str]] = {}
    for theme_id, headline in rows:
        counts[theme_id] = counts.get(theme_id, 0) + 1
        bucket = samples.setdefault(theme_id, [])
        if len(bucket) < SAMPLE_HEADLINES_PER_THEME and headline:
            bucket.append(" ".join(headline.split()))

    lowered_terms = [(t, t.lower()) for t in crossing_terms]
    inputs: list[ThemeSegmentInput] = []
    for tid in active_theme_ids:
        sample = samples.get(tid, [])
        hay = " ".join(sample).lower()
        conv = [orig for orig, low in lowered_terms if low and low in hay]
        inputs.append(ThemeSegmentInput(
            theme_id=tid,
            display_name=display_names.get(tid, tid),
            tag_count=counts.get(tid, 0),
            in_scope=tid in themes_in_scope,
            sample_headlines=sample,
            convergence_terms=conv,
        ))
    return inputs


def _build_theme_segments(
    conn: sqlite3.Connection,
    *,
    cfg: Config,
    window_since_unix: int,
    window_until_unix: int,
    themes_covered: list[str],
    crossings: list[AttentionCrossing],
    model: str = "claude-sonnet-4-6",
    # NW-SRC-4 §6: 1500 -> 2800 to fit the deeper 5-6 sentence active segments
    # across the now-11 tracked themes (2 new themes + longer summaries) without
    # truncating the batched output.
    max_tokens: int = 2800,
) -> tuple[ThemeSegmentsSection, Any | None]:
    """Step 6.5: one guaranteed segment per tracked theme.

    Returns (section, batched_call_metadata_or_None). Never raises —
    every failure mode degrades to template summaries so the section
    always renders (guaranteed-coverage is the whole point). The metadata
    is None unless the batched Sonnet call actually completed.
    """
    try:
        all_themes = load_all_themes(cfg.themes_dir)
    except ThemeLoadError as exc:
        return (
            ThemeSegmentsSection(status="failed", failure_reason=f"theme load failed: {exc}"),
            None,
        )
    active = [t for t in all_themes if t.status == "active"]
    if not active:
        return ThemeSegmentsSection(status="skipped"), None

    display_names = {t.theme_id: t.display_name for t in active}
    try:
        inputs = _gather_theme_inputs(
            conn,
            active_theme_ids=[t.theme_id for t in active],
            display_names=display_names,
            window_since_unix=window_since_unix,
            window_until_unix=window_until_unix,
            themes_in_scope=set(themes_covered),
            crossing_terms=[c.term for c in crossings],
        )
    except Exception as exc:  # noqa: BLE001 — DB gather failure degrades, never aborts the brief
        return (
            ThemeSegmentsSection(status="failed", failure_reason=f"tag-count query failed: {exc}"),
            None,
        )

    # Batched LLM summaries — degrade to templates on any failure.
    summaries: dict[str, str] = {}
    metadata: Any | None = None
    degraded_reason: str | None = None
    if not cfg.anthropic_api_key:
        degraded_reason = "ANTHROPIC_API_KEY not set"
    else:
        try:
            client = build_anthropic_client(cfg.anthropic_api_key)
            summaries, metadata = synthesize_theme_segments(
                client=client, model=model, max_tokens=max_tokens, inputs=inputs,
            )
        except (ThemeSegmentsError, SynthesisError) as exc:
            degraded_reason = f"segment call failed: {exc}"
        except Exception as exc:  # noqa: BLE001 — SDK/network errors degrade, never abort the brief
            degraded_reason = f"segment call errored: {exc}"

    segments: list[ThemeSegment] = []
    missing_summary = False
    for inp in inputs:
        summary = summaries.get(inp.theme_id)
        if not summary:
            summary = template_summary(inp)
            missing_summary = True
        segments.append(ThemeSegment(
            theme_id=inp.theme_id,
            display_name=inp.display_name,
            status=inp.status(),  # type: ignore[arg-type]
            tagged_headline_count=inp.tag_count,
            in_pass_c_scope=inp.in_scope,
            summary=summary,
            convergence_terms=inp.convergence_terms,
        ))

    llm_degraded = degraded_reason is not None or missing_summary
    return (
        ThemeSegmentsSection(
            status="ok",
            segments=segments,
            llm_degraded=llm_degraded,
            failure_reason=degraded_reason,
        ),
        metadata,
    )


# ---------------------------------------------------------------------------
# Step 7 helper — executive summary
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Pass F footprint computation (Stage 2a-ii-B, 2026-05-29)
#
# All three metrics are DB-queried per Mando's Q-2a-ii-1 resolution: re-query
# at orchestrator time, leaves stay pure-logic, DB access lives at the
# orchestrator. Heuristic-by-source-name approach was rejected because the
# metric's value lives in matching its definition, not in shortcuts that
# silently under-report when the source registry grows.
#
# WINDOW-ALIGNMENT DISCIPLINE (per Check 3 doctrine, 2026-05-29):
# All three helpers receive `canonical_now_unix` from the orchestrator's
# Step 6 alignment computation. They never compute their own now_unix.
# This guarantees the orchestrator's pass_f_footprint sees the SAME row-set
# Step 6's count_terms saw — preventing the subtle drift-across-windows
# bug Check 3 surfaced.
# ---------------------------------------------------------------------------


def _compute_translated_rows_in_window(
    conn: sqlite3.Connection,
    *,
    window_since_unix: int,
    window_until_unix: int,
) -> int:
    """Single COUNT query — translated_rows_in_window metric."""
    return conn.execute(
        "SELECT COUNT(*) FROM headlines "
        "WHERE published_at_unix >= ? AND published_at_unix <= ? "
        "AND language != 'en' AND headline_en IS NOT NULL",
        (window_since_unix, window_until_unix),
    ).fetchone()[0]


def _compute_cross_language_event_merges(
    conn: sqlite3.Connection,
    *,
    pass_c_brief: Any | None,
) -> tuple[int, int | None]:
    """Cross-language event merge count + url_match_warnings audit signal.

    For each Pass C event, batch-query the language of its source_headlines
    URLs. An event is "cross-language merged" iff its URLs include at least
    one row with language='en' AND at least one with language!='en'.

    Returns (event_merge_count, url_match_warnings):
      - event_merge_count: number of qualifying events
      - url_match_warnings: count of source_headline URLs that didn't match
        any DB row. None when all URLs matched (the expected case per
        Mando's Q-2a-ii-B-2 reasoning); non-null indicates URL drift
        (prompt change, model update, edge content). Defensive audit
        signal per Mando's Stage 2a-ii-B refinement — preserves visibility
        when Sonnet's URL reproduction discipline fails.
    """
    if pass_c_brief is None or not pass_c_brief.events:
        return (0, None)

    # Collect every (event_idx, url) pair from source_headlines.
    event_urls: list[tuple[int, str]] = []
    for evt_idx, evt in enumerate(pass_c_brief.events):
        for sh in evt.source_headlines:
            if sh.url:
                event_urls.append((evt_idx, sh.url))

    if not event_urls:
        return (0, None)

    # Batch-query language for all URLs at once.
    distinct_urls = sorted({u for _, u in event_urls})
    placeholders = ",".join("?" * len(distinct_urls))
    rows = conn.execute(
        f"SELECT url, language FROM headlines WHERE url IN ({placeholders})",
        distinct_urls,
    ).fetchall()
    language_by_url: dict[str, str] = {r[0]: r[1] for r in rows}

    # url_match_warnings: how many input URLs are NOT in the DB?
    unmatched_count = sum(
        1 for u in distinct_urls if u not in language_by_url
    )
    warnings: int | None = unmatched_count if unmatched_count > 0 else None

    # For each event, classify its URLs' languages.
    events_in_scope: dict[int, dict[str, bool]] = {}
    for evt_idx, url in event_urls:
        lang = language_by_url.get(url)
        if lang is None:
            continue   # URL didn't match a DB row — counted in warnings already
        flags = events_in_scope.setdefault(evt_idx, {"has_en": False, "has_non_en": False})
        if lang == "en":
            flags["has_en"] = True
        else:
            flags["has_non_en"] = True

    merge_count = sum(
        1 for flags in events_in_scope.values()
        if flags["has_en"] and flags["has_non_en"]
    )
    return (merge_count, warnings)


def _compute_attention_crossings_enabled_by_pass_f(
    conn: sqlite3.Connection,
    *,
    attention_briefs: list[AttentionBrief],
    canonical_now_unix: int,
    window_hours: int,
) -> list[str]:
    """List of crossing terms where >50% of the cluster rows are translated.

    Re-derives the cluster via attention.cluster.cluster_for_term() — same
    Stage 1 helper the original attention pass used. Window-alignment
    discipline: uses canonical_now_unix (NOT time.time()) so the cluster
    re-derivation sees the same row-set the original cluster saw.

    For each crossing's cluster, batch-query language for the cluster's
    headline_ids. Count rows with language != 'en'. If translated/total
    > _PASS_F_ENABLED_THRESHOLD (0.50), the crossing is enabled-by-Pass-F.

    Returns the enabled terms in the SAME ORDER as attention_briefs (input
    order preserved for deterministic envelope output — same iteration-order
    discipline as convergence).
    """
    if not attention_briefs:
        return []

    window_since_unix = canonical_now_unix - window_hours * 3600
    enabled: list[str] = []

    for ab in attention_briefs:
        cluster = cluster_for_term(
            conn,
            term=ab.triggering_term,
            window_since_unix=window_since_unix,
            window_until_unix=canonical_now_unix,
        )
        if not cluster:
            continue

        ids = [c.headline_id for c in cluster]
        placeholders = ",".join("?" * len(ids))
        rows = conn.execute(
            f"SELECT headline_id, language FROM headlines "
            f"WHERE headline_id IN ({placeholders})",
            ids,
        ).fetchall()
        language_by_id: dict[str, str] = {r[0]: r[1] for r in rows}

        translated_count = sum(
            1 for ch in cluster
            if language_by_id.get(ch.headline_id, "en") != "en"
        )
        if translated_count / len(cluster) > _PASS_F_ENABLED_THRESHOLD:
            enabled.append(ab.triggering_term)

    return enabled


def _build_pass_f_footprint(
    conn: sqlite3.Connection,
    *,
    pass_c_brief: Any | None,
    attention_briefs: list[AttentionBrief],
    canonical_now_unix: int,
    window_hours: int,
) -> PassFFootprint:
    """Compose the three metrics + url_match_warnings into PassFFootprint.

    All three sub-computations use canonical_now_unix per the
    window-alignment discipline (Check 3 doctrine).
    """
    window_since_unix = canonical_now_unix - window_hours * 3600

    translated = _compute_translated_rows_in_window(
        conn,
        window_since_unix=window_since_unix,
        window_until_unix=canonical_now_unix,
    )
    merges, url_warnings = _compute_cross_language_event_merges(
        conn, pass_c_brief=pass_c_brief,
    )
    enabled = _compute_attention_crossings_enabled_by_pass_f(
        conn,
        attention_briefs=attention_briefs,
        canonical_now_unix=canonical_now_unix,
        window_hours=window_hours,
    )

    return PassFFootprint(
        translated_rows_in_window=translated,
        cross_language_event_merges=merges,
        attention_crossings_enabled_by_pass_f=enabled,
        url_match_warnings=url_warnings,
    )


def _build_executive_summary(
    theme_synthesis: ThemeSynthesisSection,
    crossings: list[AttentionCrossing],
    materiality_threshold: float = 0.5,
) -> ExecutiveSummary:
    """Build the top-of-brief executive summary panel.

    - narrative: theme_synthesis.narrative when populated; the no_trigger
      narrative when status="no_trigger"; otherwise empty.
    - material_event_count: events with materiality_score >= threshold.
    - orphan_crossings_count: crossings whose convergence.status == "orphan".
    - highest_materiality_score: max materiality among events, or None if no events.
    """
    if theme_synthesis.narrative:
        narrative = theme_synthesis.narrative
    elif theme_synthesis.status == "no_trigger":
        narrative = _NO_TRIGGER_NARRATIVE
    else:
        narrative = ""

    material_count = sum(
        1 for ev in theme_synthesis.events
        if ev.materiality_score >= materiality_threshold
    )
    orphan_count = sum(
        1 for c in crossings if c.convergence.status == "orphan"
    )
    highest = max(
        (ev.materiality_score for ev in theme_synthesis.events),
        default=None,
    )

    return ExecutiveSummary(
        narrative=narrative,
        dominant_themes=list(theme_synthesis.themes_covered),
        material_event_count=material_count,
        attention_crossings_count=len(crossings),
        orphan_crossings_count=orphan_count,
        highest_materiality_score=highest,
    )


# ---------------------------------------------------------------------------
# Top-level: assemble_full_brief
# ---------------------------------------------------------------------------


def assemble_full_brief(
    *,
    cfg: Config,
    window_hours: int = 24,
    no_scrape: bool = False,
    sink_factory: Callable[[], AlertSink] | None = None,
    now: datetime | None = None,
    pass_c_brief_events: list[Any] | None = None,  # injected for tests when synthesize_window is mocked
) -> FullBriefEnvelope:
    """Assemble a Full Brief per spec v1.0 Steps 1-7.

    Args:
      cfg: Config with db_path, brief_archive_path, theses_path, etc.
      window_hours: synthesis window length [1, 168]. Default 24.
      no_scrape: if True, skip Step 2 — Pass C + Pass E run against
        existing DB state. Default False.
      sink_factory: optional Callable building an AlertSink for dispatch.
        None disables dispatch. Lazy — only invoked by synthesize_window
        when materiality says dispatch.
      now: datetime override for tests; defaults to UTC now.
      pass_c_brief_events: test injection — when a test mocks
        synthesize_window such that the returned SynthesizeResult.brief
        wouldn't carry the right events for convergence analysis, the
        test can inject the events list directly. None in production.

    Returns:
      FullBriefEnvelope. Writes the artifact to disk at
      cfg.brief_archive_path under the YYYY-MM partition. Always returns
      a valid envelope — step failures surface in pass_failures +
      envelope_health rather than raising.

    Stage 2a-ii-A scope: pass_f_footprint is stubbed with zeros (Stage
    2a-ii-B populates via DB query); threshold_note is None regardless
    of window_hours (Stage 2a-ii-B threads the warning when != 24).
    """
    when = now if now is not None else datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    else:
        when = when.astimezone(timezone.utc)
    now_unix = int(when.timestamp())

    pass_failures: list[PassFailure] = []

    # Step 1: Window definition.
    window_section = WindowSection(
        since=_iso_from_unix(now_unix - window_hours * 3600),
        until=_iso_from_unix(now_unix),
        duration_hours=window_hours,
    )

    conn = connect(cfg.db_path)
    try:
        # Step 2: Scrape (unless no_scrape).
        if no_scrape:
            scrape_health = StepHealth(status="skipped")
            attention_outcome: dict[str, Any] | None = None
        else:
            scrape_health, attention_outcome = _do_scrape_step(cfg, conn)
            if scrape_health.status == "failed":
                pass_failures.append(PassFailure(
                    step="scrape",
                    reason=scrape_health.reason or "unknown",
                    recovered=True,
                ))

        # Step 3: Pass C synthesis (4-case discrimination).
        pass_c_health, theme_synthesis, synth_metadata, pass_c_brief = _do_pass_c_step(
            cfg, conn,
            window_hours=window_hours,
            sink_factory=sink_factory,
            when=when,
        )
        if pass_c_health.status == "failed":
            pass_failures.append(PassFailure(
                step="pass_c",
                reason=pass_c_health.reason or "unknown",
                recovered=True,
            ))

        # Step 4: Pass E extraction from scrape's attention_outcome.
        pass_e_health, attention_briefs, brief_paths = _extract_pass_e_step(
            attention_outcome, cfg.brief_archive_path,
        )
        if pass_e_health.status == "failed":
            pass_failures.append(PassFailure(
                step="pass_e",
                reason=pass_e_health.reason or "unknown",
                recovered=True,
            ))

        # Step 5: Convergence analysis.
        # Use the original Event list from the Brief object when available.
        # Tests injecting via pass_c_brief_events override the source.
        # Iteration order is the Brief's event list order (per Mando's
        # forward-guidance: convergence iteration order is observable).
        if pass_c_brief_events is not None:
            events_for_convergence = pass_c_brief_events
        elif pass_c_brief is not None:
            events_for_convergence = list(pass_c_brief.events)
        else:
            events_for_convergence = []

        attention_synthesis, crossings = _build_attention_synthesis_with_convergence(
            attention_briefs,
            brief_paths,
            pass_e_health,
            events_for_convergence,
        )
        convergence_health = StepHealth(status="ok")

        # Canonical timestamp for Steps 6 + 7's DB-queried metrics
        # (Window-alignment discipline, Check 3 doctrine).
        #
        # scrape's auto-attention computed its own now_unix when its
        # callback fired (T0+Δ where Δ = scrape duration). count_terms
        # there used window [T0+Δ-24h, T0+Δ]. If the orchestrator used
        # its own now_unix (T0) for the count_terms re-run AND for the
        # pass_f_footprint DB queries, the two windows would drift by Δ
        # — and a term with exactly COLD_START_WINDOW_MIN mentions at
        # one boundary but one fewer at the other would cross in scrape's
        # view but appear in the orchestrator's near-miss table,
        # contradicting itself. Same drift applies to cluster
        # re-derivation in pass_f_footprint.
        #
        # Fix: compute canonical_now_unix ONCE and thread it to ALL
        # downstream DB queries. When scrape ran successfully, use
        # attention's window_until_unix. When scrape didn't run
        # (no_scrape or scrape_failed), fall back to orchestrator's
        # now_unix.
        #
        # Stage 2a-ii-B note: this canonical value flows to BOTH
        # _build_frequency_diagnostic (Step 6 count_terms) AND
        # _build_pass_f_footprint (Step 7 cluster_for_term + headline
        # language lookups). All four DB queries that depend on a
        # window route through the same timestamp — uniform alignment.
        if (
            attention_outcome is not None
            and attention_outcome.get("status") == "ok"
            and "window_until_unix" in attention_outcome
        ):
            canonical_now_unix = int(attention_outcome["window_until_unix"])
        else:
            canonical_now_unix = now_unix

        # Step 6: Frequency diagnostic.
        freq_health, freq_diagnostic = _build_frequency_diagnostic(
            conn, canonical_now_unix, window_hours, cfg,
            attention_briefs, crossings,
        )
        if freq_health.status == "failed":
            pass_failures.append(PassFailure(
                step="frequency_diagnostic",
                reason=freq_health.reason or "unknown",
                recovered=True,
            ))

        # Step 6.5: Dedicated theme segments (guaranteed per-theme coverage).
        # Window-aligned to canonical_now_unix (same discipline as Step 6).
        # Never raises — degrades to template summaries internally.
        theme_segments_section, theme_segments_metadata = _build_theme_segments(
            conn,
            cfg=cfg,
            window_since_unix=canonical_now_unix - window_hours * 3600,
            window_until_unix=canonical_now_unix,
            themes_covered=list(theme_synthesis.themes_covered),
            crossings=crossings,
        )
        if theme_segments_section.status == "failed":
            pass_failures.append(PassFailure(
                step="theme_segments",
                reason=theme_segments_section.failure_reason or "unknown",
                recovered=True,
            ))

        # Step 7: Envelope assembly.
        # Stage 2a-ii-B: pass_f_footprint populated via DB queries with
        # window-alignment discipline (same canonical_now_unix as Step 6).
        try:
            pass_f_footprint = _build_pass_f_footprint(
                conn,
                pass_c_brief=pass_c_brief,
                attention_briefs=attention_briefs,
                canonical_now_unix=canonical_now_unix,
                window_hours=window_hours,
            )
        except Exception as exc:  # noqa: BLE001 — DB query failure surfaces here
            _LOG.warning("pass_f_footprint computation failed: %s", exc)
            pass_failures.append(PassFailure(
                step="pass_f_footprint",
                reason=f"DB query failure: {exc}",
                recovered=True,
            ))
            # _PASS_F_FOOTPRINT_UNAVAILABLE makes the degraded-path nature
            # semantically distinct from a legitimate-zero result. Consumers
            # checking pass_failures can discriminate the two.
            pass_f_footprint = _PASS_F_FOOTPRINT_UNAVAILABLE

        # Cost envelope — uses metadata even on archive_failed (Stage 1
        # closing flag discipline).
        pass_e_brief_metadata: list[tuple[str, Any]] = [
            (ab.brief_id, ab.synthesis_metadata) for ab in attention_briefs
        ]
        cost_envelope_dict = assemble_cost_envelope(
            pass_c_metadata=synth_metadata,
            pass_e_brief_metadata=pass_e_brief_metadata,
            model="claude-sonnet-4-6",
            theme_segments_metadata=theme_segments_metadata,
        )
        cost = CostEnvelope.model_validate(cost_envelope_dict)

        # Executive summary.
        exec_summary = _build_executive_summary(theme_synthesis, crossings)

        # Final envelope.
        envelope = FullBriefEnvelope(
            brief_id=FullBriefEnvelope.new_brief_id(when),
            generated_at=when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            window=window_section,
            executive_summary=exec_summary,
            theme_synthesis=theme_synthesis,
            theme_segments=theme_segments_section,
            attention_synthesis=attention_synthesis,
            frequency_diagnostic=freq_diagnostic,
            pass_f_footprint=pass_f_footprint,
            envelope_health=FullBriefEnvelopeHealth(
                scrape=scrape_health,
                pass_c=pass_c_health,
                pass_e=pass_e_health,
                convergence_analysis=convergence_health,
                frequency_diagnostic=freq_health,
            ),
            pass_failures=pass_failures,
            cost=cost,
        )

        # Write artifact to disk.
        try:
            write_brief(cfg.brief_archive_path, envelope)
        except (ArchiveError, OSError) as exc:
            _LOG.warning(
                "Full Brief disk write failed: %s. Envelope returned in-memory.", exc,
            )
            # Append a pass_failures entry but return the envelope anyway.
            envelope = envelope.model_copy(update={
                "pass_failures": list(envelope.pass_failures) + [PassFailure(
                    step="full_brief_archive_write",
                    reason=str(exc),
                    recovered=True,
                )],
            })

        return envelope
    finally:
        conn.close()


__all__ = [
    "assemble_full_brief",
]
