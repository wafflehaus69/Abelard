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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from .brief import (
    Brief,
    Dispatch,
    EnvelopeHealth,
    Event,
    SynthesisMetadata,
    Trigger,
)
from .cluster import Cluster
from .llm_client import (
    SynthesisLLMError,
    SynthesisResponse,
    call_synthesis_llm,
)
from .prompt import build_messages_payload


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


__all__ = [
    "SynthesisError",
    "build_anthropic_client",
    "synthesize_brief",
]
