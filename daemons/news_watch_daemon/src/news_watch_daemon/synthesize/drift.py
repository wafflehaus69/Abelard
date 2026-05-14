"""Drift-watcher orchestration: untagged headlines + themes -> proposals.

Pass C Step 10. Threads the drift pipeline together:

  1. Build the prompt payload (single cache breakpoint at end of
     system prompt; active themes + untagged headlines in the user
     message).
  2. Call Haiku via drift_client.call_drift_llm.
  3. Mint proposal_id + generated_at for each raw proposal Haiku
     returned (Haiku never generates these — orchestrator-side
     minting prevents proposal_id collisions and spoofing).
  4. Validate each proposal against the Pydantic `DriftProposal`
     schema — enforces theme_id presence, tier enum, evidence_count
     non-negative, etc.
  5. Apply the orchestrator-side `min_evidence_count` floor. The
     prompt also asks for this; the orchestrator enforces it as
     defense-in-depth (a misbehaving response can't slip a singleton
     proposal through).
  6. Drop proposals targeting theme_ids that aren't in the supplied
     theme list — the prompt forbids this but defense-in-depth.
  7. Drop proposals whose proposed_keyword already exists in the
     theme's primary/secondary/exclusion list. Again defense-in-depth.
  8. Return list[DriftProposal].

The drift orchestrator does NOT:
  - Read SQLite (caller pre-fetches untagged headlines).
  - Write proposals to disk (Step 11's proposals CLI handles persistence).
  - Apply approved proposals to theme YAMLs (Step 11 also).
  - Run the synthesis call (separate orchestrator, separate Haiku/Sonnet
    boundary).

The Anthropic client is INJECTED; production callers use
`synthesize.synthesize.build_anthropic_client(api_key)` from the
sibling module (same SDK + same key plumbing).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from ..theme_config import ThemeConfig
from .brief import DriftProposal
from .drift_client import (
    DriftLLMError,
    DriftResponse,
    call_drift_llm,
)
from .drift_prompt import build_messages_payload


_LOG = logging.getLogger("news_watch_daemon.synthesize.drift")


class DriftError(RuntimeError):
    """Raised when drift fails at the orchestration layer.

    Distinct from `DriftLLMError` — wraps validation failures and
    other orchestration-layer breakage. SDK-level exceptions
    (`anthropic.*`) bubble up untouched.
    """


@dataclass(frozen=True)
class DriftRunResult:
    """Aggregate output of `propose_drift()`. Pure data.

    The synthesis side of the daemon returns a `Brief` (which carries
    its own `synthesis_metadata` with token counts). Drift produces N
    proposals with no equivalent envelope — this wrapper holds the
    validated proposals plus the same cache-telemetry fields the
    synthesis call surfaces, so callers (smoke runner, daemon loop)
    can report Checkpoint-style metrics without re-fetching from the
    raw `DriftResponse`.
    """

    proposals: list[DriftProposal] = field(default_factory=list)
    model_used: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0


def _filter_proposals(
    raw_proposals: list[dict[str, Any]],
    themes: list[ThemeConfig],
    min_evidence_count: int,
    max_proposals_per_batch: int,
) -> list[dict[str, Any]]:
    """Apply orchestrator-side defense-in-depth filters.

    Drops:
      - proposals with theme_id not in the supplied themes list
      - proposals whose proposed_keyword already exists in the
        theme's primary/secondary/exclusion list
      - proposals with evidence_count < min_evidence_count
    Then trims to max_proposals_per_batch (highest evidence_count first).

    Returns the surviving raw dicts in (descending evidence_count)
    order.
    """
    theme_index: dict[str, ThemeConfig] = {t.theme_id: t for t in themes}
    kept: list[dict[str, Any]] = []
    for raw in raw_proposals:
        theme_id = raw.get("theme_id")
        if theme_id not in theme_index:
            _LOG.warning(
                "dropping drift proposal: theme_id %r not in active themes; raw=%r",
                theme_id, raw,
            )
            continue
        keyword = raw.get("proposed_keyword")
        if not isinstance(keyword, str) or not keyword.strip():
            _LOG.warning(
                "dropping drift proposal: missing/empty proposed_keyword; raw=%r", raw,
            )
            continue
        evidence = raw.get("evidence_count", 0)
        if not isinstance(evidence, int) or evidence < min_evidence_count:
            _LOG.warning(
                "dropping drift proposal: evidence_count %r < floor %d; raw=%r",
                evidence, min_evidence_count, raw,
            )
            continue
        theme = theme_index[theme_id]
        existing = (
            set(theme.keywords.primary)
            | set(theme.keywords.secondary)
            | set(theme.keywords.exclusions)
        )
        if keyword in existing:
            _LOG.warning(
                "dropping drift proposal: keyword %r already in theme %r keyword set",
                keyword, theme_id,
            )
            continue
        kept.append(raw)

    # Trim to cap by evidence_count desc; stable for ties.
    kept.sort(key=lambda r: r.get("evidence_count", 0), reverse=True)
    return kept[:max_proposals_per_batch]


def _validate_proposals(
    raw_proposals: list[dict[str, Any]],
    when: datetime,
) -> list[DriftProposal]:
    """Mint proposal_id + generated_at, then validate each as DriftProposal.

    Aggregates validation errors (Mando-style — surface ALL bad
    proposals in one exception so Haiku-side schema drift is easier
    to diagnose).
    """
    generated_at_iso = when.strftime("%Y-%m-%dT%H:%M:%SZ")
    validated: list[DriftProposal] = []
    failures: list[str] = []
    for i, raw in enumerate(raw_proposals):
        # Orchestrator mints these — overwrite anything Haiku may have
        # tried to provide (it shouldn't, per the prompt schema).
        merged = {
            **raw,
            "proposal_id": DriftProposal.new_proposal_id(when),
            "generated_at": generated_at_iso,
        }
        try:
            validated.append(DriftProposal.model_validate(merged))
        except ValidationError as exc:
            failures.append(f"proposals[{i}]: {exc}")
    if failures:
        raise DriftError(
            "drift proposal validation failed:\n  " + "\n  ".join(failures)
        )
    return validated


def propose_drift(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    themes: list[ThemeConfig],
    untagged: list[tuple[str | None, str, int]],
    max_proposals_per_batch: int,
    min_evidence_count: int,
    now: datetime | None = None,
) -> DriftRunResult:
    """End-to-end drift call: untagged + themes -> proposals + telemetry.

    Args:
        client: Anthropic SDK client (or test double exposing
            `.messages.create()`).
        model: Anthropic model ID, e.g. `"claude-haiku-4-5"`.
        max_tokens: Output cap for the LLM call.
        themes: active themes — pass the full ThemeConfig set so the
            prompt can render keyword lists Haiku will cross-check.
        untagged: list of `(publisher, headline, published_at_unix)`
            tuples. Caller fetches these from SQLite (headlines with
            `themes_json` empty/null).
        max_proposals_per_batch: hard cap on output proposals.
        min_evidence_count: floor — drop any proposal whose
            evidence_count falls below.
        now: datetime override for tests; defaults to UTC now.

    Returns:
        DriftRunResult with `proposals` (ordered by evidence_count
        descending) plus the cache telemetry fields from the
        underlying DriftResponse.

    Raises:
        DriftError: proposal validation failed.
        DriftLLMError: LLM output unparseable.
        anthropic.* exceptions: SDK-level errors bubble up.
    """
    payload = build_messages_payload(
        themes=themes,
        untagged=untagged,
        max_proposals_per_batch=max_proposals_per_batch,
        min_evidence_count=min_evidence_count,
    )

    response: DriftResponse = call_drift_llm(
        client=client,
        model=model,
        max_tokens=max_tokens,
        payload=payload,
    )

    _LOG.info(
        "drift call completed: model=%s input_tokens=%d output_tokens=%d "
        "cache_creation=%d cache_read=%d raw_proposals=%d",
        response.model_used,
        response.input_tokens,
        response.output_tokens,
        response.cache_creation_input_tokens,
        response.cache_read_input_tokens,
        len(response.proposals_payload),
    )

    filtered = _filter_proposals(
        response.proposals_payload,
        themes,
        min_evidence_count,
        max_proposals_per_batch,
    )

    when = now if now is not None else datetime.now(timezone.utc)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    else:
        when = when.astimezone(timezone.utc)

    return DriftRunResult(
        proposals=_validate_proposals(filtered, when),
        model_used=response.model_used,
        input_tokens=response.input_tokens,
        output_tokens=response.output_tokens,
        cache_creation_input_tokens=response.cache_creation_input_tokens,
        cache_read_input_tokens=response.cache_read_input_tokens,
    )


__all__ = [
    "DriftError",
    "DriftRunResult",
    "propose_drift",
]
