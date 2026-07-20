"""Pass E orchestrator: counter -> threshold -> per-term LLM call -> brief.

Ties the ATTENTION-driven synthesis pipeline together. One call to
`run_attention()` does the full cycle for a single cron tick:

  1. count_terms over the 24h window.
  2. evaluate_threshold against the cold-start rule.
  3. If zero crossings: return an `AttentionRunResult` with the top-K
     near-miss candidates for operator visibility.
  4. For each crossing term: cluster_for_term -> build_messages_payload
     -> Anthropic call -> AttentionBrief -> archive write -> dispatch.

LLM call follows Pass C llm_client conventions: stream the response,
disable adaptive thinking (synthesis is structured-output and thinking
exhausts the budget — see Pass C live smoke #3, 2026-05-14), extract
text from typed content blocks, defensively strip markdown fences if
Sonnet ignored no-fence instructions.

Failure mode discipline:
  - LLM SDK errors (auth, rate-limit, network) bubble up as native
    Anthropic exceptions. Caller (CLI handler) decides retry policy.
  - LLM-output parse failures raise `AttentionLLMError` with diagnostic
    detail (stop_reason, output_tokens, block_types, raw[:500]).
  - Pydantic validation failures (e.g. attention_shape outside the
    closed Literal set) raise `AttentionError` — fail loud per the
    Q5 design decision; do NOT silently coerce to "unclear".
  - Per-term failures inside the for loop are caught and recorded as
    failures in the run result; remaining terms still process.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ..alert.factory import AlertSinkFactoryError, build_alert_sink
from ..alert.sink import AlertSink
from ..config import Config
from ..llm_text import strip_code_fences
from ..synthesize.archive import write_brief
from ..synthesize.brief import Dispatch, SynthesisMetadata
from ..synthesize.config import SynthesisConfigError, load_synthesis_config
from ..synthesize.synthesize import SynthesisError, build_anthropic_client
from .brief_schema import AttentionBrief, AttentionShape
from .cluster import ClusterHeadline, cluster_for_term
from .counter import TermCounts, count_terms_collapsed
from .event_group import group_convergent_crossings
from .prompt import build_messages_payload
from .stopwords import StopwordsError, load_stopwords
from .threshold import CandidateTerm, CrossingTerm, evaluate_threshold, top_candidates


_LOG = logging.getLogger("news_watch_daemon.attention.orchestrator")


class AttentionError(RuntimeError):
    """Raised when the attention pipeline fails at the orchestration layer.

    Covers Pydantic validation failures (attention_shape out-of-set), brief
    construction errors, and other orchestration-level breakage. Distinct
    from AttentionLLMError (LLM-output parse issues) so callers can match
    on the specific failure mode.
    """


class AttentionLLMError(RuntimeError):
    """Raised when the LLM response is unparseable or shape-violating."""


_VALID_ATTENTION_SHAPES: frozenset[str] = frozenset({
    "single_event_dominant",
    "multi_source_convergence",
    "slow_burn",
    "narrow_source_spike",
    "cross_topic_recurrence",
    "unclear",
})


@dataclass(frozen=True)
class PerTermOutcome:
    """One term's outcome within an attention cycle."""

    term: str
    success: bool
    brief_id: str | None = None
    archive_path: str | None = None
    dispatch_success: bool | None = None
    dispatch_error: str | None = None
    error: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    # Other crossing terms whose clusters converged with this one (same event);
    # they were folded into this single synthesis call instead of firing their
    # own. Empty for a solo crossing. See attention/event_group.py.
    merged_terms: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AttentionRunResult:
    """End-to-end result of one attention cycle.

    Both outcomes are valuable per Pass E live-smoke spec:
      - `crossings_evaluated > 0`: per-term outcomes describe what fired.
      - `crossings_evaluated == 0`: `top_candidates` describes what
        almost-fired. Operator can decide if the threshold needs tuning.
    """

    now_unix: int
    window_since_unix: int
    window_until_unix: int
    prior_since_unix: int
    prior_until_unix: int
    headlines_in_window: int
    distinct_tokens_in_window: int
    crossings_evaluated: int
    per_term: list[PerTermOutcome] = field(default_factory=list)
    candidates: list[CandidateTerm] = field(default_factory=list)


def _parse_attention_response(text: str) -> dict[str, Any]:
    """Parse Sonnet's JSON output for an ATTENTION call.

    Defensively strips markdown fences. Validates required top-level keys
    are present with the right Python types; per-field schema validation
    happens at Pydantic construction time downstream.
    """
    text = strip_code_fences(text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AttentionLLMError(
            f"failed to parse attention JSON: {exc}; raw[:500]={text[:500]!r}"
        ) from exc
    if not isinstance(data, dict):
        raise AttentionLLMError(
            f"attention response root must be a JSON object; got {type(data).__name__}"
        )
    # Spot-check the required fields exist with the right Python types
    # before Pydantic validates. Fail-loud on shape drift.
    for key, expected_type in (
        ("narrative", str),
        ("source_mix", dict),
        ("entities_observed", list),
        ("attention_shape", str),
    ):
        if key not in data:
            raise AttentionLLMError(f"attention response missing required key {key!r}")
        if not isinstance(data[key], expected_type):
            raise AttentionLLMError(
                f"attention {key!r} must be {expected_type.__name__}; "
                f"got {type(data[key]).__name__}"
            )
    if data["attention_shape"] not in _VALID_ATTENTION_SHAPES:
        raise AttentionLLMError(
            f"attention_shape {data['attention_shape']!r} not in allowed set "
            f"{sorted(_VALID_ATTENTION_SHAPES)}"
        )
    return data


def _extract_text_from_response(response: Any) -> str:
    """Concatenate text from TextBlock items in `response.content`.

    Mirrors `synthesize.llm_client._extract_text_from_response` — Sonnet
    may emit thinking blocks (though we disable thinking); only
    `type=='text'` blocks contribute.
    """
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        if getattr(block, "type", None) == "text":
            text_value = getattr(block, "text", None)
            if isinstance(text_value, str):
                parts.append(text_value)
    return "".join(parts)


def _call_attention_llm(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Issue one ATTENTION call against the Anthropic API.

    Streaming + thinking-disabled, per Pass C live-smoke lessons. Returns
    (parsed_response_dict, usage_dict). usage_dict has the four token
    counters (input, output, cache_creation, cache_read) plus model_used.
    """
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        thinking={"type": "disabled"},
        system=payload["system"],
        messages=payload["messages"],
    ) as stream:
        response = stream.get_final_message()

    text = _extract_text_from_response(response).strip()
    if not text:
        stop_reason = getattr(response, "stop_reason", "unknown")
        usage = getattr(response, "usage", None)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        block_types = [
            getattr(b, "type", "unknown")
            for b in (getattr(response, "content", None) or [])
        ]
        raise AttentionLLMError(
            "attention response had no text content "
            f"(stop_reason={stop_reason!r}, output_tokens={output_tokens}, "
            f"max_tokens_requested={max_tokens}, block_types={block_types!r})."
        )

    parsed = _parse_attention_response(text)
    usage = getattr(response, "usage", None)
    usage_dict = {
        "model_used": getattr(response, "model", model) or model,
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
        "cache_creation_input_tokens": int(
            getattr(usage, "cache_creation_input_tokens", 0) or 0
        ),
        "cache_read_input_tokens": int(
            getattr(usage, "cache_read_input_tokens", 0) or 0
        ),
    }
    return parsed, usage_dict


def _build_attention_brief(
    *,
    crossing: CrossingTerm,
    cluster: list[ClusterHeadline],
    llm_parsed: dict[str, Any],
    llm_usage: dict[str, Any],
    when: datetime,
) -> AttentionBrief:
    """Assemble the AttentionBrief from LLM output + orchestrator-owned fields.

    Raises AttentionError (wrapping ValidationError) if the LLM output's
    `attention_shape`, source_mix, or entities_observed shape violates the
    Pydantic schema. Pydantic's error message is included verbatim for
    diagnosability.
    """
    metadata = SynthesisMetadata(
        model_used=llm_usage["model_used"],
        input_tokens=llm_usage["input_tokens"],
        output_tokens=llm_usage["output_tokens"],
        cache_creation_input_tokens=llm_usage["cache_creation_input_tokens"],
        cache_read_input_tokens=llm_usage["cache_read_input_tokens"],
        theses_doc_available=False,   # ATTENTION is theme/theses-blind by design
        theses_doc_path=None,
        theses_doc_warning=None,
    )
    try:
        return AttentionBrief(
            brief_id=AttentionBrief.new_brief_id(when),
            generated_at=when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            triggering_term=crossing.term,
            term_frequency_window=crossing.window_count,
            term_frequency_prior=crossing.prior_count,
            cluster_size=len(cluster),
            narrative=llm_parsed["narrative"],
            source_mix=llm_parsed["source_mix"],
            entities_observed=llm_parsed["entities_observed"],
            attention_shape=llm_parsed["attention_shape"],
            dispatch=Dispatch(alerted=False),
            synthesis_metadata=metadata,
        )
    except ValidationError as exc:
        raise AttentionError(
            f"AttentionBrief validation failed for term {crossing.term!r}: {exc}"
        ) from exc


def run_attention(
    *,
    conn: sqlite3.Connection,
    now_unix: int,
    stopwords: frozenset[str],
    anthropic_client: Any,
    model: str,
    max_tokens: int,
    archive_root: Path,
    sink: AlertSink | None,
    when: datetime | None = None,
    top_candidates_limit: int = 5,
    window_hours: int = 24,
) -> AttentionRunResult:
    """End-to-end ATTENTION cycle. Returns the run result; never raises.

    Args:
      conn: SQLite connection; caller owns lifecycle.
      now_unix: anchor time for the two windows.
      stopwords: frozenset from `load_stopwords(path)`.
      anthropic_client: SDK client (or test double).
      model: Anthropic model ID.
      max_tokens: per-call output cap.
      archive_root: path to the brief archive root (typically
                    `~/.openclaw/news_watch/briefs`).
      sink: AlertSink to dispatch through; pass None to skip dispatch
            (useful for tests).
      when: datetime override for brief_id minting (tests).
      top_candidates_limit: how many near-miss candidates to surface
            when zero terms cross (default 5 per Pass E spec).
      window_hours: live + prior window length in hours (each). Default
            24. Bounded [1, 168] inside `count_terms`. Threshold constants
            (`COLD_START_WINDOW_MIN=10`, `COLD_START_PRIOR_MAX=3`) do NOT
            scale with this kwarg — see `count_terms` docstring for the
            Full Brief Commit A Q4 design discussion.

    Per-term outcomes are recorded in PerTermOutcome — failures don't
    abort the cycle; remaining terms still process.
    """
    if when is None:
        when = datetime.now(timezone.utc)
    elif when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    else:
        when = when.astimezone(timezone.utc)

    counts: TermCounts = count_terms_collapsed(
        conn,
        now_unix=now_unix,
        stopwords=stopwords,
        window_hours=window_hours,
    )
    headlines_in_window = conn.execute(
        "SELECT COUNT(*) FROM headlines WHERE published_at_unix >= ? AND published_at_unix <= ?",
        (counts.window_since_unix, counts.window_until_unix),
    ).fetchone()[0]

    crossings = evaluate_threshold(counts)

    _LOG.info(
        "attention cycle: window=%d-%d headlines=%d distinct_tokens=%d crossings=%d",
        counts.window_since_unix, counts.window_until_unix,
        headlines_in_window, len(counts.window_counts), len(crossings),
    )

    if not crossings:
        return AttentionRunResult(
            now_unix=now_unix,
            window_since_unix=counts.window_since_unix,
            window_until_unix=counts.window_until_unix,
            prior_since_unix=counts.prior_since_unix,
            prior_until_unix=counts.prior_until_unix,
            headlines_in_window=headlines_in_window,
            distinct_tokens_in_window=len(counts.window_counts),
            crossings_evaluated=0,
            per_term=[],
            candidates=top_candidates(counts, limit=top_candidates_limit),
        )

    # Pre-compute each crossing's cluster ONCE (scripts-only DB queries), then
    # group crossings whose clusters describe the same event. A single dominant
    # story surfaces under several crossing phrases ("attacks iran" / "hormuz
    # tensions" / "tensions rise"); without this each would fire its own LLM
    # call. Grouping collapses them so we synthesize once per event, not once
    # per phrase — the reused clusters are passed straight into synthesis, so
    # there is no second DB pass.
    clusters_by_term: dict[str, list[ClusterHeadline]] = {
        c.term: cluster_for_term(
            conn,
            term=c.term,
            window_since_unix=counts.window_since_unix,
            window_until_unix=counts.window_until_unix,
        )
        for c in crossings
    }
    id_sets = {t: {h.headline_id for h in cl} for t, cl in clusters_by_term.items()}
    groups = group_convergent_crossings(crossings, id_sets)
    if len(groups) < len(crossings):
        _LOG.info(
            "attention convergence: %d crossings collapsed to %d event-group(s) "
            "(saved %d LLM call(s))",
            len(crossings), len(groups), len(crossings) - len(groups),
        )

    per_term: list[PerTermOutcome] = []
    for group in groups:
        representative = group[0]
        also_surfaced = [c.term for c in group[1:]]
        merged_cluster = _merge_clusters(
            [clusters_by_term[c.term] for c in group]
        )
        outcome = _process_one_term(
            crossing=representative,
            cluster=merged_cluster,
            also_surfaced_terms=also_surfaced,
            counts=counts,
            anthropic_client=anthropic_client,
            model=model,
            max_tokens=max_tokens,
            archive_root=archive_root,
            sink=sink,
            when=when,
        )
        per_term.append(outcome)

    return AttentionRunResult(
        now_unix=now_unix,
        window_since_unix=counts.window_since_unix,
        window_until_unix=counts.window_until_unix,
        prior_since_unix=counts.prior_since_unix,
        prior_until_unix=counts.prior_until_unix,
        headlines_in_window=headlines_in_window,
        distinct_tokens_in_window=len(counts.window_counts),
        crossings_evaluated=len(crossings),
        per_term=per_term,
        candidates=[],
    )


def _merge_clusters(clusters: list[list[ClusterHeadline]]) -> list[ClusterHeadline]:
    """Union of several term clusters, deduped by headline_id, newest-first.

    Convergent crossings share most of their headlines, so the union is close
    to any single member's cluster; the dedup keeps the merged view from
    double-listing the shared rows.
    """
    seen: set[str] = set()
    merged: list[ClusterHeadline] = []
    for cluster in clusters:
        for h in cluster:
            if h.headline_id in seen:
                continue
            seen.add(h.headline_id)
            merged.append(h)
    merged.sort(key=lambda h: -h.published_at_unix)
    return merged


def _process_one_term(
    *,
    crossing: CrossingTerm,
    cluster: list[ClusterHeadline],
    counts: TermCounts,
    anthropic_client: Any,
    model: str,
    max_tokens: int,
    archive_root: Path,
    sink: AlertSink | None,
    when: datetime,
    also_surfaced_terms: list[str] | None = None,
) -> PerTermOutcome:
    """Process one event-group: LLM -> brief -> archive -> dispatch.

    `crossing` is the group's representative term; `cluster` is the merged
    headline set for the group (a solo crossing is just a one-member group).
    `also_surfaced_terms` names the converged phrasings folded into this call.

    Failures (cluster empty, LLM parse error, validation error, archive write
    error) are caught and recorded in the outcome. Remaining groups still get
    processed.
    """
    also_surfaced_terms = also_surfaced_terms or []
    if not cluster:
        # Threshold said the term crossed but the merged cluster is empty: the
        # LIKE pre-filter or word-boundary post-verify dropped everything.
        # Defensive — should be impossible if threshold and counter agree, but
        # log and bail rather than call LLM on empty input.
        return PerTermOutcome(
            term=crossing.term,
            success=False,
            error=(
                "cluster empty after term retrieval; counter and cluster "
                "filters disagree (possible regex/word-boundary edge case)"
            ),
            merged_terms=also_surfaced_terms,
        )

    window_since_iso = datetime.fromtimestamp(
        counts.window_since_unix, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_until_iso = datetime.fromtimestamp(
        counts.window_until_unix, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = build_messages_payload(
        triggering_term=crossing.term,
        term_frequency_window=crossing.window_count,
        term_frequency_prior=crossing.prior_count,
        window_since_iso=window_since_iso,
        window_until_iso=window_until_iso,
        cluster=cluster,
        also_surfaced_terms=also_surfaced_terms,
    )

    try:
        llm_parsed, llm_usage = _call_attention_llm(
            client=anthropic_client,
            model=model,
            max_tokens=max_tokens,
            payload=payload,
        )
    except AttentionLLMError as exc:
        _LOG.warning("attention LLM call failed for term %r: %s", crossing.term, exc)
        return PerTermOutcome(
            term=crossing.term,
            success=False,
            error=f"llm_error: {exc}",
            merged_terms=also_surfaced_terms,
        )
    except Exception as exc:  # noqa: BLE001 — anthropic.* exceptions
        _LOG.warning("attention SDK call raised for term %r: %s", crossing.term, exc)
        return PerTermOutcome(
            term=crossing.term,
            success=False,
            error=f"sdk_error: {type(exc).__name__}: {exc}",
            merged_terms=also_surfaced_terms,
        )

    _LOG.info(
        "attention call completed: term=%s model=%s input_tokens=%d output_tokens=%d "
        "cache_creation=%d cache_read=%d shape=%s",
        crossing.term,
        llm_usage["model_used"],
        llm_usage["input_tokens"],
        llm_usage["output_tokens"],
        llm_usage["cache_creation_input_tokens"],
        llm_usage["cache_read_input_tokens"],
        llm_parsed.get("attention_shape"),
    )

    try:
        brief = _build_attention_brief(
            crossing=crossing,
            cluster=cluster,
            llm_parsed=llm_parsed,
            llm_usage=llm_usage,
            when=when,
        )
    except AttentionError as exc:
        return PerTermOutcome(
            term=crossing.term,
            success=False,
            error=str(exc),
            input_tokens=llm_usage["input_tokens"],
            output_tokens=llm_usage["output_tokens"],
            cache_creation_input_tokens=llm_usage["cache_creation_input_tokens"],
            cache_read_input_tokens=llm_usage["cache_read_input_tokens"],
            merged_terms=also_surfaced_terms,
        )

    try:
        archive_path = write_brief(archive_root, brief)
    except Exception as exc:  # noqa: BLE001 — OSError, ArchiveError
        _LOG.warning(
            "attention archive write failed for term %r (brief_id=%s): %s",
            crossing.term, brief.brief_id, exc,
        )
        return PerTermOutcome(
            term=crossing.term,
            success=False,
            error=f"archive_error: {type(exc).__name__}: {exc}",
            brief_id=brief.brief_id,
            input_tokens=llm_usage["input_tokens"],
            output_tokens=llm_usage["output_tokens"],
            cache_creation_input_tokens=llm_usage["cache_creation_input_tokens"],
            cache_read_input_tokens=llm_usage["cache_read_input_tokens"],
            merged_terms=also_surfaced_terms,
        )

    dispatch_success: bool | None = None
    dispatch_error: str | None = None
    if sink is not None:
        result = sink.dispatch(brief)
        dispatch_success = result.success
        dispatch_error = result.error
        # Patch the brief on-disk with dispatch outcome — mirrors Pass C's
        # post-dispatch re-write pattern.
        if result.success:
            brief = brief.model_copy(update={"dispatch": Dispatch(
                alerted=True,
                channel=result.channel if result.channel in ("signal", "telegram_bot") else None,
            )})
        else:
            brief = brief.model_copy(update={"dispatch": Dispatch(
                alerted=False,
                suppressed_reason=f"dispatch_failed:{result.error}",
            )})
        try:
            write_brief(archive_root, brief)
        except Exception as rewrite_exc:  # noqa: BLE001 — non-fatal second write
            # The alert WAS sent; only the on-disk dispatch-outcome patch
            # failed, so the archived brief's dispatch.alerted may disagree
            # with reality. Surface it rather than silently diverging.
            logging.getLogger("news_watch_daemon.attention.orchestrator").warning(
                "failed to re-write attention brief %s with dispatch outcome "
                "(alert was sent; archived dispatch state may be stale): %s: %s",
                brief.brief_id, type(rewrite_exc).__name__, rewrite_exc,
            )

    return PerTermOutcome(
        term=crossing.term,
        success=True,
        brief_id=brief.brief_id,
        archive_path=str(archive_path),
        dispatch_success=dispatch_success,
        dispatch_error=dispatch_error,
        input_tokens=llm_usage["input_tokens"],
        output_tokens=llm_usage["output_tokens"],
        cache_creation_input_tokens=llm_usage["cache_creation_input_tokens"],
        cache_read_input_tokens=llm_usage["cache_read_input_tokens"],
        merged_terms=also_surfaced_terms,
    )


# ---------------------------------------------------------------------------
# Full Brief Stage 2a-ii-A (2026-05-29): cycle wrapper hoisted from cli.py.
#
# run_attention_cycle and attention_outcome_to_dict were previously
# CLI-private helpers in cli.py. Hoisted here so the Full Brief
# orchestrator at fullbrief/orchestrator.py can use the same plumbing
# (load stopwords -> config -> client -> sink -> run_attention) without
# importing from cli.py — which would create a circular dependency once
# cli.py registers the `full-brief` subcommand in Stage 2b.
#
# Composition glue per Q8: wrapper that ties configuration to
# run_attention, no inner synthesis logic.
# ---------------------------------------------------------------------------


# Per-call output cap for the ATTENTION LLM. Sonnet output is smaller than
# Pass C briefs (one narrative + source_mix + entities — no events list),
# so the per-call cap is set lower. Operator can tune via the env var
# NEWS_WATCH_ATTENTION_MAX_TOKENS if a cycle hits the cap.
DEFAULT_ATTENTION_MAX_TOKENS = 2048


def attention_outcome_to_dict(result: AttentionRunResult) -> dict[str, Any]:
    """Render AttentionRunResult as a JSON-friendly dict.

    Same shape whether the ATTENTION cycle is called via the standalone
    `news-watch-daemon attention` subcommand, chained as a follow-on
    inside the scrape handler, or invoked by the Full Brief orchestrator.
    """
    return {
        "now_unix": result.now_unix,
        "window_since_unix": result.window_since_unix,
        "window_until_unix": result.window_until_unix,
        "prior_since_unix": result.prior_since_unix,
        "prior_until_unix": result.prior_until_unix,
        "headlines_in_window": result.headlines_in_window,
        "distinct_tokens_in_window": result.distinct_tokens_in_window,
        "crossings_evaluated": result.crossings_evaluated,
        "per_term": [
            {
                "term": o.term,
                "success": o.success,
                "brief_id": o.brief_id,
                "archive_path": o.archive_path,
                "dispatch_success": o.dispatch_success,
                "dispatch_error": o.dispatch_error,
                "error": o.error,
                "input_tokens": o.input_tokens,
                "output_tokens": o.output_tokens,
                "cache_creation_input_tokens": o.cache_creation_input_tokens,
                "cache_read_input_tokens": o.cache_read_input_tokens,
                "merged_terms": o.merged_terms,
            }
            for o in result.per_term
        ],
        "top_candidates": [
            {
                "term": c.term,
                "window_count": c.window_count,
                "prior_count": c.prior_count,
                "reason": c.reason,
            }
            for c in result.candidates
        ],
    }


def run_attention_cycle(
    *,
    cfg: Config,
    conn: sqlite3.Connection,
    dry_run: bool = False,
    top_candidates_limit: int = 5,
) -> dict[str, Any]:
    """Shared attention-run path used by the standalone CLI subcommand,
    the scrape follow-on, and the Full Brief orchestrator.

    Returns a JSON-friendly outcome dict. Skip-not-fail discipline: if
    stopwords file is missing or the Anthropic key is unset, returns a
    `{"status": "skipped", "reason": ...}` dict rather than raising.
    Lets the caller's main result envelope stay healthy when ATTENTION
    is unconfigured — the caller's work (scrape, full-brief) succeeded;
    ATTENTION just didn't run.

    Stage 2a-ii-B will add a `window_hours: int = 24` kwarg that plumbs
    through `run_attention` -> `count_terms` per the Commit A
    parameterization. Default 24 preserves backwards compat with the
    standalone CLI subcommand and the scrape follow-on; only the Full
    Brief orchestrator passes a non-default value.
    """
    _log = logging.getLogger("news_watch_daemon.attention.orchestrator")

    # 1. Stopwords — bail with status=skipped if file is unreadable.
    try:
        stopwords = load_stopwords(cfg.stopwords_path)
    except StopwordsError as exc:
        _log.warning("attention skipped: stopwords load failed: %s", exc)
        return {
            "status": "skipped",
            "reason": f"stopwords_load_failed: {exc}",
            "stopwords_path": str(cfg.stopwords_path),
        }

    # 2. Dry-run short-circuits before any LLM construction.
    if dry_run:
        now_unix_dry = int(time.time())
        counts = count_terms_collapsed(conn, now_unix=now_unix_dry, stopwords=stopwords)
        crossings = evaluate_threshold(counts)
        headlines_in_window = conn.execute(
            "SELECT COUNT(*) FROM headlines WHERE published_at_unix >= ? AND published_at_unix <= ?",
            (counts.window_since_unix, counts.window_until_unix),
        ).fetchone()[0]
        return {
            "status": "ok",
            "dry_run": True,
            "now_unix": now_unix_dry,
            "window_since_unix": counts.window_since_unix,
            "window_until_unix": counts.window_until_unix,
            "headlines_in_window": headlines_in_window,
            "distinct_tokens_in_window": len(counts.window_counts),
            "crossings_evaluated": len(crossings),
            "crossings": [
                {"term": c.term, "window_count": c.window_count, "prior_count": c.prior_count}
                for c in crossings
            ],
            "top_candidates": [
                {"term": c.term, "window_count": c.window_count,
                 "prior_count": c.prior_count, "reason": c.reason}
                for c in top_candidates(counts, limit=top_candidates_limit)
            ],
        }

    # 3. Anthropic key required for the live path.
    if not cfg.anthropic_api_key:
        _log.warning("attention skipped: ANTHROPIC_API_KEY not set")
        return {
            "status": "skipped",
            "reason": "ANTHROPIC_API_KEY not set",
        }

    # 4. Load synthesis config to borrow model name; max_tokens is the
    #    attention-specific default constant above.
    try:
        synth_cfg = load_synthesis_config(cfg.synthesis_config_path)
    except SynthesisConfigError as exc:
        _log.warning("attention skipped: synthesis_config load failed: %s", exc)
        return {
            "status": "skipped",
            "reason": f"synthesis_config_load_failed: {exc}",
        }

    # 5. Build Anthropic client + sink. Sink errors are not fatal —
    #    archive write still happens, dispatch outcome surfaces as
    #    per_term.dispatch_error.
    try:
        client = build_anthropic_client(cfg.anthropic_api_key)
    except SynthesisError as exc:
        _log.warning("attention skipped: client construction failed: %s", exc)
        return {
            "status": "skipped",
            "reason": f"client_construction_failed: {exc}",
        }

    try:
        sink = build_alert_sink(synth_cfg.alert_sink)
    except AlertSinkFactoryError as exc:
        _log.warning("attention dispatch sink construction failed: %s", exc)
        sink = None  # archives will still write; dispatch silently skips

    now_unix = int(time.time())
    result = run_attention(
        conn=conn,
        now_unix=now_unix,
        stopwords=stopwords,
        anthropic_client=client,
        model=synth_cfg.synthesis.default_model,
        max_tokens=DEFAULT_ATTENTION_MAX_TOKENS,
        archive_root=cfg.brief_archive_path,
        sink=sink,
        top_candidates_limit=top_candidates_limit,
    )
    outcome = attention_outcome_to_dict(result)
    outcome["status"] = "ok"
    return outcome


__all__ = [
    "AttentionError",
    "AttentionLLMError",
    "AttentionRunResult",
    "DEFAULT_ATTENTION_MAX_TOKENS",
    "PerTermOutcome",
    "attention_outcome_to_dict",
    "run_attention",
    "run_attention_cycle",
]
