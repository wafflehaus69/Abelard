"""Shared text helpers for LLM output.

Sonnet/Haiku occasionally wrap their JSON output in a markdown ```json … ```
fence despite explicit no-fence instructions in the prompt. Every response
parser (synthesis, drift, attention, theme-segments) has to defend against it
identically, so the strip lives here once rather than copied four ways.
"""

from __future__ import annotations

import re


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


__all__ = ["strip_code_fences"]
