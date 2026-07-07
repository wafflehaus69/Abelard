"""Top-level synthesis orchestration: cluster -> prompt -> LLM -> Brief.

Pass C Step 9. Threads the synthesis pipeline together:

  1. Load Abelard's theses doc (if `NEWS_WATCH_THESES_PATH` is set).
     Absence is not fatal — synthesis falls back to the no-theses
     prompt variant and records a WARN in `synthesis_metadata`.
  2. Build the cached prompt payload (prompt.build_messages_payload).
  3. Call the Anthropic Messages API (llm_client.call_synthesis_llm).
  4. Validate each event dict against the Pydantic `Event` schema —
     enforces materiality_score range, source_headlines shape, etc.
  5. Assemble the full `Brief` by minting brief_id + generated_at,
     attaching trigger + themes_covered + envelope_health, and
     packing the cache + theses telemetry into `synthesis_metadata`.
  6. Return the Brief. The orchestrator's caller (the daemon loop)
     runs the materiality gate, writes the archive, and dispatches.

The synthesis orchestrator does NOT:
  - Write to the archive (that's archive.write_brief).
  - Run the materiality gate (materiality.evaluate_materiality).
  - Dispatch via AlertSink (sink.dispatch).
  - Run the drift watcher (Step 10's haiku call, separate module).

The Anthropic client is INJECTED — tests pass a mock with the right
shape, production code calls `build_anthropic_client(api_key)`. This
keeps the heavy SDK import out of the module's top level.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from pydantic import ValidationError

from ..alert.factory import AlertSinkFactoryError, build_alert_sink
from ..alert.sink import AlertSink
from .archive import ArchiveError, write_brief
from .brief import (
    Brief,
    Dispatch,
    EnvelopeHealth,
    Event,
    SynthesisMetadata,
    Trigger,
    TriggerWindow,
)
from .cluster import Cluster, ClusterInput, cluster_headlines
from .config import SynthesisDaemonConfig
from .magnitude import Magnitude, extract_magnitudes
from .llm_client import (
    SynthesisLLMError,
    SynthesisResponse,
    call_synthesis_llm,
)
from .materiality import MaterialityDecision, evaluate_materiality
from .prompt import build_messages_payload
from .trigger import TriggerDecision, TriggerHeadline, evaluate_gate
from .trigger_log import write_entry as write_trigger_log_entry


_LOG = logging.getLogger("news_watch_daemon.synthesize.synthesize")


class SynthesisError(RuntimeError):
    """Raised when synthesis fails at the orchestration layer.

    Distinct from `SynthesisLLMError` (which fires for LLM-output parse
    issues) — this wraps event-validation failures and other
    orchestration-layer breakage so the caller has one exception type
    to catch above the SDK boundary.
    """


def build_anthropic_client(api_key: str) -> Any:
    """Construct the production Anthropic SDK client.

    Lazy-imports `anthropic` so this module loads in environments
    without the SDK (test fixtures inject mock clients directly).

    The returned client exposes `.messages.create()`.
    """
    if not api_key:
        raise SynthesisError(
            "ANTHROPIC_API_KEY is empty; synthesis cannot run. "
            "Set ANTHROPIC_API_KEY in the environment or pass a "
            "non-empty key explicitly."
        )
    try:
        import anthropic  # noqa: PLC0415 — lazy on purpose
    except ImportError as exc:
        raise SynthesisError(
            "the `anthropic` package is not installed; "
            "synthesis cannot run. `pip install anthropic`."
        ) from exc
    return anthropic.Anthropic(api_key=api_key)


def _load_theses_doc(
    theses_path: Path | None,
) -> tuple[str | None, bool, str | None]:
    """Read the theses document if present.

    Returns:
        (text, available, warning) — text is None when the doc is
        absent/unreadable; available reflects that; warning is the
        non-silent surface for the absent-doc case (recorded in
        synthesis_metadata.theses_doc_warning per Pass C §4).
    """
    if theses_path is None:
        return None, False, (
            "NEWS_WATCH_THESES_PATH not set; synthesis ran no-theses variant"
        )
    if not theses_path.is_file():
        return None, False, (
            f"theses file not found at {theses_path}; "
            "synthesis ran no-theses variant"
        )
    try:
        text = theses_path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, False, (
            f"theses file unreadable at {theses_path}: {exc}; "
            "synthesis ran no-theses variant"
        )
    if not text.strip():
        return None, False, (
            f"theses file empty at {theses_path}; "
            "synthesis ran no-theses variant"
        )
    return text, True, None


def _validate_events(
    events_payload: list[dict[str, Any]],
) -> list[Event]:
    """Validate each raw event dict against the Pydantic Event schema.

    Aggregates errors — reports all bad events in one exception rather
    than failing on the first. Easier to diagnose Sonnet-side schema
    drift when the orchestrator surfaces every offending event in the
    same error message.
    """
    validated: list[Event] = []
    failures: list[str] = []
    for i, raw in enumerate(events_payload):
        try:
            validated.append(Event.model_validate(raw))
        except ValidationError as exc:
            failures.append(f"events[{i}]: {exc}")
    if failures:
        raise SynthesisError(
            "event validation failed for synthesis response:\n  "
            + "\n  ".join(failures)
        )
    return validated


def enrich_clusters_with_magnitudes(clusters: list[Cluster]) -> list[Cluster]:
    """Attach mechanically-extracted stated magnitudes to each cluster.

    Runs the pure `extract_magnitudes` extractor over each member's
    `headline` — the SAME field `_format_cluster` renders — so the prompt's
    magnitude line matches the shown headline. Each member is rebuilt with
    its per-headline magnitudes; the cluster carries the leader-first,
    (value, unit, kind)-deduped aggregate. No LLM, no I/O, no DB write —
    ephemeral enrichment recomputed per brief from the stored verbatim text.
    """
    enriched: list[Cluster] = []
    for cluster in clusters:
        new_members = tuple(
            replace(member, stated_magnitudes=extract_magnitudes(member.headline))
            for member in cluster.members
        )
        aggregate: list[Magnitude] = []
        seen: set[tuple[float, str, str]] = set()
        for member in new_members:  # members are leader-first
            for mag in member.stated_magnitudes:
                key = (mag.value, mag.unit, mag.kind)
                if key not in seen:
                    seen.add(key)
                    aggregate.append(mag)
        enriched.append(
            replace(cluster, members=new_members, stated_magnitudes=tuple(aggregate))
        )
    return enriched


def synthesize_brief(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    trigger: Trigger,
    themes_in_scope: list[str],
    theme_briefs: dict[str, str],
    clusters: list[Cluster],
    max_events_per_brief: int,
    theses_path: Path | None,
    envelope_health: EnvelopeHealth | None = None,
    now: datetime | None = None,
) -> Brief:
    """End-to-end synthesis call: prompt -> LLM -> Brief.

    Args:
        client: Anthropic SDK client (or test double with the same
            `.messages.create()` surface).
        model: Anthropic model ID, e.g. `"claude-sonnet-4-6"`.
        max_tokens: Output cap for the LLM call.
        trigger: What fired this synthesis (event / pull + reason + window).
        themes_in_scope: theme_ids covered by this brief.
        theme_briefs: theme_id -> brief text (the `brief: |` block
            from each theme YAML in scope).
        clusters: clustered headlines for this synthesis window.
        max_events_per_brief: hard cap Sonnet must respect.
        theses_path: optional path to THESES.md. Unset / unreadable
            triggers the no-theses prompt variant + a WARN in metadata.
        envelope_health: optional snapshot of source/heartbeat state.
        now: datetime override for tests; defaults to UTC now.

    Returns:
        Validated `Brief` with `dispatch.alerted=False`. The
        materiality gate (called by the daemon loop) sets dispatch
        fields based on its decision.

    Raises:
        SynthesisError: event validation failed.
        SynthesisLLMError: LLM output unparseable.
        anthropic.* exceptions: SDK-level errors (auth, rate-limit,
            timeout). Bubble up untouched — caller decides retry.
    """
    theses_text, theses_available, theses_warning = _load_theses_doc(theses_path)
    if theses_warning:
        _LOG.warning(theses_warning)

    # Magnitude-awareness (2026-07-07): enrich clusters with mechanically-
    # extracted stated magnitudes just before prompt-building, so the prompt
    # surfaces them explicitly. Scripts-first — no LLM in the extractor.
    clusters = enrich_clusters_with_magnitudes(clusters)

    payload = build_messages_payload(
        trigger=trigger,
        themes_in_scope=themes_in_scope,
        theme_briefs=theme_briefs,
        clusters=clusters,
        max_events_per_brief=max_events_per_brief,
        theses_doc_text=theses_text,
    )

    response: SynthesisResponse = call_synthesis_llm(
        client=client,
        model=model,
        max_tokens=max_tokens,
        payload=payload,
    )

    _LOG.info(
        "synthesis call completed: model=%s input_tokens=%d output_tokens=%d "
        "cache_creation=%d cache_read=%d events=%d",
        response.model_used,
        response.input_tokens,
        response.output_tokens,
        response.cache_creation_input_tokens,
        response.cache_read_input_tokens,
        len(response.events_payload),
    )

    events = _validate_events(response.events_payload)

    metadata = SynthesisMetadata(
        model_used=response.model_used,
        input_tokens=response.input_tokens,
        output_tokens=response.output_tokens,
        cache_creation_input_tokens=response.cache_creation_input_tokens,
        cache_read_input_tokens=response.cache_read_input_tokens,
        theses_doc_available=theses_available,
        theses_doc_path=str(theses_path) if theses_path else None,
        theses_doc_warning=theses_warning,
    )

    when = now if now is not None else datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    else:
        when = when.astimezone(timezone.utc)

    return Brief(
        brief_id=Brief.new_brief_id(when),
        generated_at=when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        trigger=trigger,
        themes_covered=list(themes_in_scope),
        events=events,
        narrative=response.narrative,
        dispatch=Dispatch(alerted=False),
        drift_proposals=[],
        synthesis_metadata=metadata,
        envelope_health=envelope_health if envelope_health is not None else EnvelopeHealth(),
    )


# ---------------------------------------------------------------------------
# Full Brief Stage 2a-i sub-step B (2026-05-29): synthesize_window().
#
# Pure-callable cycle wrapper hoisted from cli._handle_synthesize. Returns
# a structured `SynthesizeResult` discriminated by `status`. The CLI handler
# in cli.py becomes a thin argparse wrapper that:
#
#   1. Resolves config + themes + DB connection from args + cfg
#   2. Builds anthropic_client + sink_factory
#   3. Calls synthesize_window with explicit kwargs
#   4. Formats the CLI envelope dict from the structured result
#   5. Owns exit-code logic (CLI concern, not synthesis concern)
#
# The Full Brief orchestrator at Stage 2a-ii is the second caller — it
# uses the same pure callable to obtain a Brief + cost metadata + status
# discriminator without going through the CLI argparse path.
#
# Cost telemetry discipline (per Stage 1 closing flag): `metadata` is
# populated on SynthesizeResult whenever Sonnet was called, EVEN IF
# archive write fails afterward. The orchestrator can still bill the API
# call so token telemetry isn't lost on disk-write failure.
# ---------------------------------------------------------------------------


def _iso_from_unix(ts: int) -> str:
    """Format a unix timestamp as ISO-8601 UTC with `Z` suffix."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _query_window_headlines(
    conn: sqlite3.Connection,
    *,
    window_since_unix: int,
    window_until_unix: int,
) -> tuple[list[TriggerHeadline], list[ClusterInput]]:
    """Read headlines in the synthesis window. Returns (trigger_input, cluster_input).

    One round trip to headlines + one to headline_theme_tags. Both
    inputs are derived from the same rows so the trigger gate and the
    clustering pass agree on what's in scope.
    """
    # Pass F (Follow-up #8, 2026-05-28): COALESCE(headline_en, headline)
    # so the Pass C trigger gate (phrase match) and the clustering pass
    # (Jaccard token overlap) both see TRANSLATED content for non-English
    # rows. English-content rows have headline_en IS NULL by design and
    # fall through to the original headline via COALESCE — bit-identical
    # to pre-Pass-F behavior for those rows.
    #
    # Without this COALESCE: Russian-source rows (e.g. telegram:Ateobreaking)
    # would tag correctly (theme aggregation runs against translated text
    # at re-tag time) but cluster-match against original Russian text,
    # producing isolated single-row clusters that fall below materiality
    # thresholds — Ateo content would be silently excluded from morning
    # briefs despite being correctly tagged. This is the defect Step 8
    # of the Pass F validation arc exposed.
    #
    # See the FROM-headlines audit (Follow-up #8 commit message) for the
    # complete classification of which reader sites use COALESCE vs
    # intentionally read raw `headline` (e.g., the language classifier and
    # the translation backfill query both deliberately read original text).
    rows = conn.execute(
        "SELECT headline_id, COALESCE(headline_en, headline) AS headline, "
        "       raw_source, url, "
        "       published_at_unix, fetched_at_unix "
        "FROM headlines "
        "WHERE published_at_unix >= ? AND published_at_unix <= ? "
        "ORDER BY published_at_unix DESC",
        (window_since_unix, window_until_unix),
    ).fetchall()
    if not rows:
        return [], []

    ids = [r["headline_id"] for r in rows]
    placeholders = ",".join("?" * len(ids))
    tag_rows = conn.execute(
        f"SELECT headline_id, theme_id FROM headline_theme_tags "
        f"WHERE headline_id IN ({placeholders})",
        ids,
    ).fetchall()
    tags_by_id: dict[str, list[str]] = {hid: [] for hid in ids}
    for tr in tag_rows:
        tags_by_id[tr["headline_id"]].append(tr["theme_id"])

    trigger_inputs: list[TriggerHeadline] = []
    cluster_inputs: list[ClusterInput] = []
    for r in rows:
        themes_for_row = tuple(tags_by_id.get(r["headline_id"], []))
        # Trigger gate is keyed on tagged headlines only.
        if themes_for_row:
            trigger_inputs.append(TriggerHeadline(
                headline_id=r["headline_id"],
                headline=r["headline"],
                themes=themes_for_row,
                fetched_at_unix=r["fetched_at_unix"],
            ))
        # Clustering operates on all headlines (the headline can be
        # untagged and still cluster with a tagged sibling); the
        # orchestrator later filters clusters to the in-scope themes.
        cluster_inputs.append(ClusterInput(
            headline_id=r["headline_id"],
            headline=r["headline"],
            url=r["url"],
            publisher=r["raw_source"],
            published_at_unix=r["published_at_unix"],
        ))
    return trigger_inputs, cluster_inputs


def _filter_clusters_to_scope(
    clusters: list[Cluster],
    *,
    in_scope_headline_ids: set[str],
) -> list[Cluster]:
    """Keep clusters whose leader (or any member) tagged to a scoped theme.

    Cheap defensive filter — Sonnet only sees clusters relevant to the
    themes in scope. A cluster of fully-untagged headlines is dropped.
    """
    if not in_scope_headline_ids:
        return []
    kept: list[Cluster] = []
    for c in clusters:
        if any(m.headline_id in in_scope_headline_ids for m in c.members):
            kept.append(c)
    return kept


@dataclass(frozen=True)
class SynthesizeResult:
    """Structured return from `synthesize_window` — pure callable result.

    Status discriminator covers five end states:

      - "synthesized": Sonnet call + brief assembly + archive write all
        succeeded. `brief`, `metadata`, `brief_path` populated.
        Materiality + dispatch outcomes in `materiality_decision_payload`
        and `dispatch_result_payload`.

      - "no_trigger": Trigger gate didn't fire (Q2 quiet-day case).
        `brief=None`, `metadata=None`, `brief_path=None`. `reason` carries
        the gate's diagnostic. Valid outcome, NOT an error.

      - "synthesis_failed": Pre-flight check failed (e.g., client missing
        in non-dry-run mode) OR Sonnet call / LLM-output parse failed.
        `brief=None`, `brief_path=None`. `metadata` may be partially
        populated if the SDK call returned but event validation failed.
        `reason` carries the error description.

      - "archive_failed": Brief assembled + materiality evaluated but
        archive write to disk failed. `brief` populated, `metadata`
        populated, `brief_path=None`, `reason` carries write-error
        detail. Cost telemetry MUST still be captured per Stage 1
        closing flag — `metadata` is the cost source.

      - "dry_run": Dry-run mode (no Sonnet call). Returns trigger +
        cluster diagnostics for inspection without billing.

    Trigger context (`trigger_obj`, `trigger_decision_fire`,
    `trigger_decision_reason`, `themes_in_scope`) populated whenever a
    trigger was evaluated — even in no_trigger case where the gate ran
    but didn't fire.
    """

    status: Literal[
        "synthesized", "no_trigger", "synthesis_failed", "archive_failed", "dry_run"
    ]

    # Always populated:
    window_since_unix: int
    window_until_unix: int

    # Populated when Sonnet was called (synthesized OR archive_failed):
    brief: Brief | None = None
    metadata: SynthesisMetadata | None = None
    brief_path: Path | None = None

    # Failure detail (no_trigger / synthesis_failed / archive_failed):
    reason: str | None = None

    # Trigger context — populated whenever a trigger was evaluated:
    trigger_obj: Trigger | None = None
    trigger_decision_fire: bool | None = None
    trigger_decision_reason: str | None = None
    themes_in_scope: list[str] = field(default_factory=list)

    # Dry-run + Full Brief diagnostic context:
    headlines_in_window: int = 0
    tagged_headlines_in_window: int = 0
    cluster_count: int = 0

    # Materiality + dispatch (status="synthesized" OR "archive_failed"):
    materiality_decision_payload: dict[str, Any] | None = None
    dispatch_result_payload: dict[str, Any] | None = None


def synthesize_window(
    *,
    conn: sqlite3.Connection,
    active_themes: list[Any],
    brief_archive_path: Path,
    trigger_log_path: Path,
    theses_path: Path | None,
    synth_cfg: SynthesisDaemonConfig,
    anthropic_client: Any | None = None,
    sink_factory: Callable[[], AlertSink] | None = None,
    window_hours: int = 24,
    pull_theme: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> SynthesizeResult:
    """Execute one Pass C synthesis cycle. Pure callable, no stdout/argparse.

    Args:
        conn: SQLite connection with row_factory=Row. Caller owns lifecycle.
        active_themes: pre-filtered list of active ThemeConfig objects
            (each must have `.theme_id` and `.brief` attributes).
        brief_archive_path: directory root for brief archive.
        trigger_log_path: path for trigger-gate append-only log.
        theses_path: optional THESES.md path (None triggers no-theses variant).
        synth_cfg: loaded synthesis config (model, max_tokens, gate,
            materiality, alert_sink, etc.).
        anthropic_client: SDK client (or test double). May be None ONLY
            when dry_run=True; otherwise returns synthesis_failed.
        sink_factory: zero-arg callable returning a built AlertSink for
            dispatch. None disables dispatch entirely (the brief is still
            archived; just no Signal/Telegram delivery attempted). Lazy
            construction lets the function avoid building a sink when
            materiality says don't-dispatch.
        window_hours: synthesis window length (default 24, bounded
            [1, 168] by caller — synthesize_window does NOT re-clamp).
        pull_theme: if set, runs pull-trigger mode against this single
            theme_id, bypassing the event-trigger gate. Caller must
            validate that the theme is in active_themes.
        dry_run: if True, short-circuit before Sonnet call. Returns
            status="dry_run" with diagnostic counts.
        now: datetime override for tests; defaults to UTC now.

    Returns:
        SynthesizeResult with the appropriate `status` discriminator. The
        function never raises for normal control-flow outcomes (no_trigger,
        synthesis_failed, archive_failed) — those surface in the result.
        Unexpected pre-flight failures (e.g., DB connection broken) bubble
        as exceptions per existing exception discipline.
    """
    when = now if now is not None else datetime.now(timezone.utc)

    # Pre-flight: API client required for non-dry-run mode.
    if not dry_run and anthropic_client is None:
        _now_unix = int(when.timestamp())
        return SynthesizeResult(
            status="synthesis_failed",
            window_since_unix=_now_unix - window_hours * 3600,
            window_until_unix=_now_unix,
            reason=(
                "anthropic_client is None but dry_run=False — caller must "
                "construct a client before invoking synthesize_window for a "
                "live synthesis cycle."
            ),
        )

    # Window definition.
    now_unix = int(when.timestamp())
    window_since = now_unix - window_hours * 3600
    window_until = now_unix

    # Query headlines (Pass F COALESCE applies via _query_window_headlines).
    trigger_inputs, cluster_inputs = _query_window_headlines(
        conn,
        window_since_unix=window_since,
        window_until_unix=window_until,
    )

    active_ids = {t.theme_id for t in active_themes}

    # Trigger gate (event mode) vs pull-mode shortcut.
    trigger_obj: Trigger
    trigger_decision: TriggerDecision | None = None
    themes_in_scope: list[str]
    if pull_theme is not None:
        themes_in_scope = [pull_theme]
        trigger_obj = Trigger(
            type="pull",
            reason=f"pull-trigger for {pull_theme}",
            window=TriggerWindow(
                since=_iso_from_unix(window_since),
                until=_iso_from_unix(window_until),
            ),
        )
    else:
        trigger_decision = evaluate_gate(
            trigger_inputs,
            config=synth_cfg.trigger_gate,
            window_since_unix=window_since,
            window_until_unix=window_until,
        )
        # Append to trigger log regardless of fire/suppress decision.
        try:
            write_trigger_log_entry(trigger_log_path, trigger_decision)
        except OSError as exc:
            _LOG.warning("trigger_log append failed: %s", exc)

        if not trigger_decision.fire:
            return SynthesizeResult(
                status="no_trigger",
                window_since_unix=window_since,
                window_until_unix=window_until,
                reason=trigger_decision.reason,
                trigger_decision_fire=False,
                trigger_decision_reason=trigger_decision.reason,
                themes_in_scope=[],
                headlines_in_window=len(cluster_inputs),
                tagged_headlines_in_window=len(trigger_inputs),
            )

        themes_in_scope = list(trigger_decision.themes_in_scope)
        trigger_obj = Trigger(
            type="event",
            reason=trigger_decision.reason,
            window=TriggerWindow(
                since=_iso_from_unix(window_since),
                until=_iso_from_unix(window_until),
            ),
        )

    # Build clusters + filter to in-scope headlines.
    clusters = cluster_headlines(cluster_inputs)
    if pull_theme is not None:
        in_scope_ids = {
            h.headline_id for h in trigger_inputs if pull_theme in h.themes
        }
    else:
        in_scope_ids = {
            h.headline_id for h in trigger_inputs
            if any(t in themes_in_scope for t in h.themes)
        }
    scoped_clusters = _filter_clusters_to_scope(
        clusters, in_scope_headline_ids=in_scope_ids,
    )

    # Dry-run short-circuit.
    if dry_run:
        return SynthesizeResult(
            status="dry_run",
            window_since_unix=window_since,
            window_until_unix=window_until,
            trigger_obj=trigger_obj,
            trigger_decision_fire=(
                trigger_decision.fire if trigger_decision is not None else None
            ),
            trigger_decision_reason=(
                trigger_decision.reason if trigger_decision is not None else None
            ),
            themes_in_scope=themes_in_scope,
            headlines_in_window=len(cluster_inputs),
            tagged_headlines_in_window=len(trigger_inputs),
            cluster_count=len(scoped_clusters),
        )

    # Build theme briefs from active themes in scope.
    theme_briefs: dict[str, str] = {
        t.theme_id: t.brief for t in active_themes if t.theme_id in themes_in_scope
    }

    # Sonnet call.
    try:
        brief = synthesize_brief(
            client=anthropic_client,
            model=synth_cfg.synthesis.default_model,
            max_tokens=synth_cfg.synthesis.default_max_tokens,
            trigger=trigger_obj,
            themes_in_scope=themes_in_scope,
            theme_briefs=theme_briefs,
            clusters=scoped_clusters,
            max_events_per_brief=synth_cfg.synthesis.max_events_per_brief,
            theses_path=theses_path,
            now=when,
        )
    except (SynthesisError, SynthesisLLMError) as exc:
        return SynthesizeResult(
            status="synthesis_failed",
            window_since_unix=window_since,
            window_until_unix=window_until,
            reason=f"synthesis call failed: {exc}",
            trigger_obj=trigger_obj,
            trigger_decision_fire=(
                trigger_decision.fire if trigger_decision is not None else None
            ),
            trigger_decision_reason=(
                trigger_decision.reason if trigger_decision is not None else None
            ),
            themes_in_scope=themes_in_scope,
            headlines_in_window=len(cluster_inputs),
            tagged_headlines_in_window=len(trigger_inputs),
            cluster_count=len(scoped_clusters),
        )

    # Materiality gate.
    decision: MaterialityDecision = evaluate_materiality(
        brief,
        threshold=synth_cfg.synthesis.materiality_threshold,
        dedup_window_hours=synth_cfg.synthesis.dedup_window_hours,
        archive_root=brief_archive_path,
    )

    # Patch brief.dispatch from the gate's verdict.
    if decision.dispatch:
        brief = brief.model_copy(update={"dispatch": Dispatch(alerted=True)})
    else:
        brief = brief.model_copy(update={"dispatch": Dispatch(
            alerted=False, suppressed_reason=decision.reason,
        )})

    materiality_payload: dict[str, Any] = {
        "dispatch": decision.dispatch,
        "reason": decision.reason,
        "above_threshold_count": decision.above_threshold_count,
        "new_events_count": decision.new_events_count,
        "deduped_against_brief_ids": list(decision.deduped_against_brief_ids),
    }

    # Archive write. On failure, return archive_failed but preserve
    # metadata for cost telemetry (Stage 1 closing flag).
    try:
        archive_path = write_brief(brief_archive_path, brief)
    except (ArchiveError, OSError) as exc:
        return SynthesizeResult(
            status="archive_failed",
            window_since_unix=window_since,
            window_until_unix=window_until,
            brief=brief,
            metadata=brief.synthesis_metadata,
            brief_path=None,
            reason=f"brief archive write failed: {exc}",
            trigger_obj=trigger_obj,
            trigger_decision_fire=(
                trigger_decision.fire if trigger_decision is not None else None
            ),
            trigger_decision_reason=(
                trigger_decision.reason if trigger_decision is not None else None
            ),
            themes_in_scope=themes_in_scope,
            headlines_in_window=len(cluster_inputs),
            tagged_headlines_in_window=len(trigger_inputs),
            cluster_count=len(scoped_clusters),
            materiality_decision_payload=materiality_payload,
            dispatch_result_payload=None,
        )

    # Dispatch via configured sink (if provided and material).
    dispatch_result_payload: dict[str, Any] | None = None
    if decision.dispatch and sink_factory is not None:
        try:
            sink = sink_factory()
        except AlertSinkFactoryError as exc:
            dispatch_result_payload = {
                "success": False,
                "channel": None,
                "error": f"sink construction failed: {exc}",
            }
        else:
            result = sink.dispatch(brief)
            dispatch_result_payload = {
                "success": result.success,
                "channel": result.channel,
                "error": result.error,
                "dispatched_at_unix": result.dispatched_at_unix,
            }
            # Re-write brief with channel + actual alerted state.
            brief = brief.model_copy(update={"dispatch": Dispatch(
                alerted=result.success,
                channel=result.channel if result.success and result.channel in ("signal", "telegram_bot") else None,
                suppressed_reason=None if result.success else f"dispatch_failed:{result.error}",
            )})
            try:
                write_brief(brief_archive_path, brief)
            except (ArchiveError, OSError):
                # Keep the success envelope; original brief already on disk.
                pass

    return SynthesizeResult(
        status="synthesized",
        window_since_unix=window_since,
        window_until_unix=window_until,
        brief=brief,
        metadata=brief.synthesis_metadata,
        brief_path=archive_path,
        trigger_obj=trigger_obj,
        trigger_decision_fire=(
            trigger_decision.fire if trigger_decision is not None else None
        ),
        trigger_decision_reason=(
            trigger_decision.reason if trigger_decision is not None else None
        ),
        themes_in_scope=themes_in_scope,
        headlines_in_window=len(cluster_inputs),
        tagged_headlines_in_window=len(trigger_inputs),
        cluster_count=len(scoped_clusters),
        materiality_decision_payload=materiality_payload,
        dispatch_result_payload=dispatch_result_payload,
    )


__all__ = [
    "SynthesisError",
    "SynthesizeResult",
    "build_anthropic_client",
    "synthesize_brief",
    "synthesize_window",
]
