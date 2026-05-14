"""Drift LLM client wrapper.

Pass C Step 10. Parallel to `llm_client.py` (synthesis), with a
schema-specific parse — drift returns `{"proposals": [...]}`, not the
synthesis `{"events", "narrative"}` shape.

Design mirrors llm_client.py:
  - Constructs the API call payload from `drift_prompt`'s output.
  - Issues `messages.create()` against the injected client.
  - Extracts text from response content blocks (skips thinking blocks).
  - Defensively strips markdown fences.
  - Surfaces full cache telemetry (creation + read).

Adaptive thinking ENABLED — per claude-api skill defaults for
"anything remotely complicated." Drift detection is lower-judgment
than synthesis but still nontrivial: Haiku has to cross-reference
candidate keywords against existing theme lists and judge tier.

Streaming OFF — drift output is small (~500-1500 tokens).

The Anthropic effort parameter is OMITTED on this call path: per the
claude-api skill, effort is supported on Opus / Sonnet 4.6 but ERRORS
on Haiku 4.5. Don't pass it.

The Anthropic client is INJECTED. This module does NOT import the
`anthropic` SDK directly — keeps tests SDK-free.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DriftResponse:
    """Output of a single drift call. Pure data.

    `proposals_payload` is the list of raw proposal dicts as Haiku
    returned them. The orchestrator (`drift.py`) mints `proposal_id` +
    `generated_at` and validates each against the `DriftProposal`
    Pydantic schema.

    Token counts come straight from `response.usage` for cost tracking
    and cache-effectiveness verification.
    """

    proposals_payload: list[dict[str, Any]]
    model_used: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


class DriftLLMError(RuntimeError):
    """Raised when drift output is unparseable or shape-violating.

    SDK-level errors (auth, rate-limit, network) are NOT wrapped — they
    bubble up as their native Anthropic exceptions.
    """


_FENCE_OPEN = re.compile(r"^```(?:json)?\s*\n?")
_FENCE_CLOSE = re.compile(r"\n?```\s*$")


def parse_drift_response(text: str) -> list[dict[str, Any]]:
    """Parse Haiku's JSON output into a list of raw proposal dicts.

    Defensively strips markdown fences. The drift schema is just
    `{"proposals": [...]}` — narrower than synthesis, so the parse
    surface is correspondingly narrower.

    Raises:
        DriftLLMError: malformed JSON, non-object root, or missing /
            non-list `proposals` key.
    """
    text = text.strip()
    if text.startswith("```"):
        text = _FENCE_OPEN.sub("", text, count=1)
        text = _FENCE_CLOSE.sub("", text, count=1)
        text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DriftLLMError(
            f"failed to parse drift JSON: {exc}; raw[:500]={text[:500]!r}"
        ) from exc

    if not isinstance(data, dict):
        raise DriftLLMError(
            f"drift response root must be a JSON object; got {type(data).__name__}"
        )

    proposals = data.get("proposals")
    if not isinstance(proposals, list):
        raise DriftLLMError(
            f"drift 'proposals' must be a list; got {type(proposals).__name__}"
        )

    return proposals


def _extract_text_from_response(response: Any) -> str:
    """Concatenate text from TextBlock items in `response.content`.

    Mirror of the helper in `llm_client.py` — kept separate to avoid
    cross-module coupling between the synthesis and drift call paths.
    Adaptive thinking emits `type=="thinking"` blocks which are
    silently skipped.
    """
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        if getattr(block, "type", None) == "text":
            text_value = getattr(block, "text", None)
            if isinstance(text_value, str):
                parts.append(text_value)
    return "".join(parts)


def call_drift_llm(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    payload: dict[str, Any],
) -> DriftResponse:
    """Issue the drift call against the Anthropic API.

    Args:
        client: Constructed `anthropic.Anthropic` (or test double).
        model: Anthropic model ID, e.g. `"claude-haiku-4-5"`.
        max_tokens: Output cap.
        payload: dict with `system` (one cached block) + `messages`
            (one user message). From `drift_prompt.build_messages_payload`.

    Returns:
        DriftResponse with raw proposal dicts + token counts.

    Raises:
        DriftLLMError: parse failure or shape violation.
        anthropic.* exceptions: bubble up.
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
        raise DriftLLMError(
            "drift response had no text content "
            "(all blocks were non-text or content was empty)"
        )

    proposals = parse_drift_response(text)

    usage = getattr(response, "usage", None)
    return DriftResponse(
        proposals_payload=proposals,
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
    "DriftLLMError",
    "DriftResponse",
    "call_drift_llm",
    "parse_drift_response",
]
