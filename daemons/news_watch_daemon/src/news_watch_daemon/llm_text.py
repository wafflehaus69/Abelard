"""Shared helpers for handling LLM responses.

The four response paths (synthesis, drift, attention, theme-segments) all had
byte-identical copies of: fence-stripping the JSON, concatenating the text
blocks out of the SDK response, and pulling token usage off it. Those live here
once rather than copied four ways.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


_FENCE_OPEN = re.compile(r"^```(?:json)?\s*\n?")
_FENCE_CLOSE = re.compile(r"\n?```\s*$")


def strip_code_fences(text: str) -> str:
    """Strip a leading ```json/``` fence and trailing ``` fence, plus surrounding
    whitespace, from an LLM response.

    Returns the text stripped of whitespace when no fence is present, so callers
    can feed the result straight to `json.loads`. Idempotent.
    """
    text = text.strip()
    if text.startswith("```"):
        text = _FENCE_OPEN.sub("", text, count=1)
        text = _FENCE_CLOSE.sub("", text, count=1)
        text = text.strip()
    return text


def extract_text_blocks(response: Any) -> str:
    """Concatenate the text of every `type=="text"` block in `response.content`.

    Thinking blocks (adaptive thinking) and any non-text block are skipped.
    Pure function over the duck-typed Anthropic SDK Message shape — the four
    call sites previously copied this verbatim.
    """
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        if getattr(block, "type", None) == "text":
            value = getattr(block, "text", None)
            if isinstance(value, str):
                parts.append(value)
    return "".join(parts)


@dataclass(frozen=True)
class LlmUsage:
    """Token telemetry pulled off an Anthropic SDK response (all fields zero-safe)."""

    model_used: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


def extract_usage(response: Any, fallback_model: str) -> LlmUsage:
    """Pull model + token counts off `response`, defaulting every count to 0.

    Consolidates the identical `int(getattr(usage, "<field>", 0) or 0)` quad the
    four response paths each repeated.
    """
    usage = getattr(response, "usage", None)

    def _int(name: str) -> int:
        return int(getattr(usage, name, 0) or 0)

    return LlmUsage(
        model_used=getattr(response, "model", fallback_model) or fallback_model,
        input_tokens=_int("input_tokens"),
        output_tokens=_int("output_tokens"),
        cache_creation_input_tokens=_int("cache_creation_input_tokens"),
        cache_read_input_tokens=_int("cache_read_input_tokens"),
    )


__all__ = ["strip_code_fences", "extract_text_blocks", "LlmUsage", "extract_usage"]
