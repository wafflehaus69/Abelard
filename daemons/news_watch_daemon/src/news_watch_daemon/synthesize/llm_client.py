"""LLM client wrapper for the synthesis call.

Pass C Step 9. Thin shell around the Anthropic Messages API:

  - Constructs the request payload from the prompt module's output.
  - Issues `messages.create()` against the provided client.
  - Extracts text from response content blocks (skipping thinking blocks).
  - Parses the JSON response (defensively — strips markdown fences if
    Sonnet ignored the no-fence instruction).
  - Surfaces full cache telemetry: input_tokens, output_tokens,
    cache_creation_input_tokens, cache_read_input_tokens.

Design choices:

  - Adaptive thinking is ENABLED (`thinking={"type": "adaptive"}`).
    Synthesis is non-trivial — materiality scoring + thesis linking +
    source-headline selection all benefit. Per the claude-api skill,
    adaptive thinking is the modern default for "anything remotely
    complicated."

  - Streaming is OFF. Synthesis output is bounded (~2-4K tokens), well
    under request-timeout risk. Non-streaming keeps the orchestrator
    simpler.

  - Client is INJECTED (caller constructs the `anthropic.Anthropic`
    instance). This module does NOT import the `anthropic` package
    directly — keeps test fixtures lightweight (no real SDK needed
    for unit tests) and lets the orchestrator own API-key plumbing.

  - SDK-level errors (auth, rate-limit, timeout) BUBBLE UP untouched.
    The orchestrator decides retry policy. This module only raises
    `SynthesisLLMError` for parse failures and shape violations in
    the response.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SynthesisResponse:
    """Output of a single synthesis call. Pure data.

    `events_payload` is a list of raw event dicts as Sonnet returned
    them; the orchestrator validates each via `Event.model_validate`
    before assembling the Brief. Validation errors are caught there,
    not here — this module is the LLM boundary, not the schema parser.

    Token counts come straight from `response.usage`; the orchestrator
    records them in `SynthesisMetadata` for Checkpoint 4's cache-
    effectiveness verification and for ongoing cost tracking.
    """

    events_payload: list[dict[str, Any]]
    narrative: str
    model_used: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


class SynthesisLLMError(RuntimeError):
    """Raised when synthesis output is unparseable or shape-violating.

    SDK-level errors (auth, rate-limit, network timeout) are NOT wrapped
    in this — they bubble up as their native Anthropic exceptions so
    the orchestrator can match on them.
    """


# Markdown-fence regexes — defensive strip. Sonnet sometimes ignores
# no-fence instructions despite explicit rules in the prompt.
_FENCE_OPEN = re.compile(r"^```(?:json)?\s*\n?")
_FENCE_CLOSE = re.compile(r"\n?```\s*$")


def parse_synthesis_response(text: str) -> tuple[list[dict[str, Any]], str]:
    """Parse Sonnet's JSON output into (events_payload, narrative).

    Defensively strips markdown fences if present.

    Raises:
        SynthesisLLMError: malformed JSON, non-object root, missing
            `events` list, or missing `narrative` string.
    """
    text = text.strip()
    # Strip a leading ```json or ``` fence + trailing ``` fence if either present.
    if text.startswith("```"):
        text = _FENCE_OPEN.sub("", text, count=1)
        text = _FENCE_CLOSE.sub("", text, count=1)
        text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SynthesisLLMError(
            f"failed to parse synthesis JSON: {exc}; raw[:500]={text[:500]!r}"
        ) from exc

    if not isinstance(data, dict):
        raise SynthesisLLMError(
            f"synthesis response root must be a JSON object; got {type(data).__name__}"
        )

    events = data.get("events")
    narrative = data.get("narrative")

    if not isinstance(events, list):
        raise SynthesisLLMError(
            f"synthesis 'events' must be a list; got {type(events).__name__}"
        )
    if not isinstance(narrative, str):
        raise SynthesisLLMError(
            f"synthesis 'narrative' must be a string; got {type(narrative).__name__}"
        )

    return events, narrative


def _extract_text_from_response(response: Any) -> str:
    """Concatenate text from TextBlock items in `response.content`.

    Sonnet may emit thinking blocks (adaptive thinking is enabled);
    those are silently skipped. Only `type=="text"` blocks contribute.
    Anthropic's SDK returns `content` as a list of typed blocks.
    """
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        if getattr(block, "type", None) == "text":
            text_value = getattr(block, "text", None)
            if isinstance(text_value, str):
                parts.append(text_value)
    return "".join(parts)


def call_synthesis_llm(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    payload: dict[str, Any],
) -> SynthesisResponse:
    """Issue the synthesis call against the Anthropic API.

    Args:
        client: A constructed `anthropic.Anthropic` instance (or a
            test double exposing `.messages.create()`).
        model: Anthropic model ID, e.g. `"claude-sonnet-4-6"`.
        max_tokens: Output cap. The prompt's hard rules cap event count
            via `max_events_per_brief`; this is the API-level ceiling.
        payload: dict with `system` (cached text blocks) and `messages`
            (one user message). Comes from
            `prompt.build_messages_payload()`.

    Returns:
        SynthesisResponse with parsed events + narrative + token counts.

    Raises:
        SynthesisLLMError: parse failure or shape violation.
        anthropic.* exceptions: bubble up untouched.
    """
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        thinking={"type": "adaptive"},
        system=payload["system"],
        messages=payload["messages"],
    )

    text = _extract_text_from_response(response).strip()
    if not text:
        # Diagnostic detail — the first live-smoke failure (2026-05-14)
        # hit this path because max_tokens=2048 was exhausted in
        # adaptive thinking blocks before the model emitted text.
        # Surface stop_reason + output_tokens + block types so future
        # failures of the same shape are diagnosable from the error
        # envelope alone, without needing to instrument live calls.
        stop_reason = getattr(response, "stop_reason", "unknown")
        usage = getattr(response, "usage", None)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        block_types = [
            getattr(b, "type", "unknown")
            for b in (getattr(response, "content", None) or [])
        ]
        raise SynthesisLLMError(
            "synthesis response had no text content "
            f"(stop_reason={stop_reason!r}, output_tokens={output_tokens}, "
            f"max_tokens_requested={max_tokens}, block_types={block_types!r}). "
            "If stop_reason='max_tokens' and block_types is all 'thinking', "
            "increase synthesis.default_max_tokens in synthesis_config.yaml "
            "or pass a higher max_tokens to the orchestrator."
        )

    events, narrative = parse_synthesis_response(text)

    usage = getattr(response, "usage", None)
    return SynthesisResponse(
        events_payload=events,
        narrative=narrative,
        model_used=getattr(response, "model", model) or model,
        input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
        output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
        cache_creation_input_tokens=int(
            getattr(usage, "cache_creation_input_tokens", 0) or 0
        ),
        cache_read_input_tokens=int(
            getattr(usage, "cache_read_input_tokens", 0) or 0
        ),
    )


__all__ = [
    "SynthesisLLMError",
    "SynthesisResponse",
    "call_synthesis_llm",
    "parse_synthesis_response",
]
