"""Dedicated theme segments — guaranteed per-theme coverage every brief.

Pass C only synthesizes themes inside the trigger's scope (the first
firing signal's themes). A theme with many tagged headlines is dropped
from the brief entirely when it isn't the first signal — the
russia(47)/iran(20)/fed(17)-dropped finding (2026-06-30). This module
produces one segment PER TRACKED THEME so every theme surfaces, and does
it with a SINGLE batched Sonnet call (cost-aware — one call covers all
themes, active syntheses and quiet one-liners alike).

Split (Mando 2026-06-30): a theme is "active" (2-3 sentence synthesis) if
it is in Pass C scope OR its tagged-headline count clears
`ACTIVE_TAG_THRESHOLD`; otherwise "quiet" (a single "why it's hot" line).

Design discipline mirrors synthesize/llm_client.py: injected client,
streaming call, thinking disabled, defensive fence-strip on the JSON, full
cache telemetry surfaced for the cost envelope. The DB access (per-theme
tag counts + sample headlines) lives in the orchestrator; this module is
pure logic + the LLM boundary.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any


# A theme with at least this many tagged headlines in-window is treated as
# "active" even when it fell outside Pass C's trigger scope — so a genuinely
# hot-but-dropped theme (russia at 47) gets a real synthesis, not a
# one-liner. Tuned to the 2026-06-30 finding: china(5) stays quiet-tier,
# fed(17)/iran(20)/russia(47) promote. Module constant, not config — revisit
# with empirical per-theme volume once segments have run for a few weeks.
ACTIVE_TAG_THRESHOLD = 8

# Cap on sample headlines fed per theme — bounds the batched call's input
# tokens. The model gets enough to characterize the thread without the full
# tagged set.
SAMPLE_HEADLINES_PER_THEME = 8


@dataclass(frozen=True)
class ThemeSegmentInput:
    """Everything the batched call needs about one tracked theme."""

    theme_id: str
    display_name: str
    tag_count: int
    in_scope: bool
    sample_headlines: list[str] = field(default_factory=list)
    convergence_terms: list[str] = field(default_factory=list)

    def status(self, threshold: int = ACTIVE_TAG_THRESHOLD) -> str:
        """"active" if in Pass C scope or over the activity threshold."""
        return "active" if (self.in_scope or self.tag_count >= threshold) else "quiet"


@dataclass(frozen=True)
class ThemeSegmentsMetadata:
    """Token telemetry for the single batched call (duck-typed for cost.py)."""

    model_used: str
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int


class ThemeSegmentsError(RuntimeError):
    """Raised when the batched segment response is unparseable/shape-violating."""


# Defensive markdown-fence strip — same posture as synthesis (Sonnet
# occasionally fences JSON despite the no-fence instruction).
_FENCE_OPEN = re.compile(r"^```(?:json)?\s*\n?")
_FENCE_CLOSE = re.compile(r"\n?```\s*$")

_SYSTEM_INSTRUCTIONS = (
    "You write the THEME SEGMENTS panel of a markets/geopolitics intelligence "
    "brief. For each theme you are given its tagged-headline count for the "
    "window, whether the deeper event-synthesis pass already covered it, a "
    "sample of its headlines, and any attention-spike terms riding on it.\n\n"
    "For each theme produce a summary keyed by its theme_id:\n"
    "  - status 'active': 2-3 sentences on what is actually happening in this "
    "theme this window — the concrete thread, not meta-commentary.\n"
    "  - status 'quiet': ONE sentence — why (or whether) it is hot; be honest "
    "when it is genuinely quiet ('only N headlines, routine X').\n\n"
    "Ground every line in the supplied headlines. Do not invent specifics. "
    "Do not mention headline counts or the word 'theme' in the prose. Return "
    "ONLY a JSON object of the exact shape:\n"
    '{"segments": {"<theme_id>": "<summary text>", ...}}\n'
    "No markdown, no code fences, no commentary outside the JSON."
)


def build_segment_prompt(
    inputs: list[ThemeSegmentInput],
    *,
    threshold: int = ACTIVE_TAG_THRESHOLD,
) -> dict[str, Any]:
    """Build the `{system, messages}` payload for the batched call.

    The system block is static (cache-friendly); the per-cycle theme data
    goes in the single user message.
    """
    theme_blocks: list[str] = []
    for inp in inputs:
        conv = ", ".join(inp.convergence_terms) if inp.convergence_terms else "none"
        heads = "\n".join(f"    - {h}" for h in inp.sample_headlines) or "    (none)"
        theme_blocks.append(
            f"theme_id: {inp.theme_id}\n"
            f"  display_name: {inp.display_name}\n"
            f"  status: {inp.status(threshold)}\n"
            f"  tagged_headlines_in_window: {inp.tag_count}\n"
            f"  attention_spike_terms: {conv}\n"
            f"  sample_headlines:\n{heads}"
        )
    user_text = (
        "Themes this window:\n\n" + "\n\n".join(theme_blocks)
        + "\n\nReturn the JSON object now."
    )
    return {
        "system": _SYSTEM_INSTRUCTIONS,
        "messages": [{"role": "user", "content": user_text}],
    }


def parse_segment_response(text: str, expected_ids: list[str]) -> dict[str, str]:
    """Parse the batched JSON into {theme_id: summary}.

    Missing themes are tolerated (the orchestrator fills them with a
    template line) — the model occasionally drops a quiet theme. Extra
    theme_ids not in `expected_ids` are ignored. Non-string summaries are
    coerced/skipped defensively.
    """
    text = text.strip()
    if text.startswith("```"):
        text = _FENCE_OPEN.sub("", text, count=1)
        text = _FENCE_CLOSE.sub("", text, count=1)
        text = text.strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ThemeSegmentsError(
            f"failed to parse theme-segments JSON: {exc}; raw[:400]={text[:400]!r}"
        ) from exc
    if not isinstance(data, dict):
        raise ThemeSegmentsError(
            f"theme-segments root must be a JSON object; got {type(data).__name__}"
        )
    segments = data.get("segments")
    if not isinstance(segments, dict):
        raise ThemeSegmentsError(
            f"theme-segments 'segments' must be an object; got {type(segments).__name__}"
        )
    expected = set(expected_ids)
    out: dict[str, str] = {}
    for tid, summary in segments.items():
        if tid in expected and isinstance(summary, str) and summary.strip():
            out[tid] = summary.strip()
    return out


def _extract_text(response: Any) -> str:
    parts: list[str] = []
    for block in getattr(response, "content", None) or []:
        if getattr(block, "type", None) == "text":
            val = getattr(block, "text", None)
            if isinstance(val, str):
                parts.append(val)
    return "".join(parts)


def synthesize_theme_segments(
    *,
    client: Any,
    model: str,
    max_tokens: int,
    inputs: list[ThemeSegmentInput],
    threshold: int = ACTIVE_TAG_THRESHOLD,
) -> tuple[dict[str, str], ThemeSegmentsMetadata]:
    """Issue the single batched segment call; return ({theme_id: summary}, metadata).

    Streams (long-input safety, per synthesis discipline), thinking
    disabled (structured task). Raises ThemeSegmentsError on empty/
    unparseable output; SDK-level exceptions bubble up untouched so the
    orchestrator can degrade to template lines.
    """
    payload = build_segment_prompt(inputs, threshold=threshold)
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        thinking={"type": "disabled"},
        system=payload["system"],
        messages=payload["messages"],
    ) as stream:
        response = stream.get_final_message()

    text = _extract_text(response).strip()
    if not text:
        stop_reason = getattr(response, "stop_reason", "unknown")
        raise ThemeSegmentsError(
            f"theme-segments response had no text (stop_reason={stop_reason!r}, "
            f"max_tokens={max_tokens})"
        )

    summaries = parse_segment_response(text, [i.theme_id for i in inputs])

    usage = getattr(response, "usage", None)
    metadata = ThemeSegmentsMetadata(
        model_used=getattr(response, "model", model) or model,
        input_tokens=int(getattr(usage, "input_tokens", 0) or 0),
        output_tokens=int(getattr(usage, "output_tokens", 0) or 0),
        cache_creation_input_tokens=int(getattr(usage, "cache_creation_input_tokens", 0) or 0),
        cache_read_input_tokens=int(getattr(usage, "cache_read_input_tokens", 0) or 0),
    )
    return summaries, metadata


def template_summary(inp: ThemeSegmentInput, *, threshold: int = ACTIVE_TAG_THRESHOLD) -> str:
    """Deterministic fallback line when the LLM summary is unavailable.

    States WHAT (count + top headline + spike terms) without claiming WHY —
    the honest degraded surface so the section always renders.
    """
    top = inp.sample_headlines[0] if inp.sample_headlines else "no headlines"
    top = " ".join(top.split())
    if len(top) > 140:
        top = top[:140].rstrip() + "..."
    conv = f"; attention terms: {', '.join(inp.convergence_terms)}" if inp.convergence_terms else ""
    return f"{inp.tag_count} tagged headlines this window (summary unavailable). Top: {top}{conv}"


__all__ = [
    "ACTIVE_TAG_THRESHOLD",
    "SAMPLE_HEADLINES_PER_THEME",
    "ThemeSegmentInput",
    "ThemeSegmentsError",
    "ThemeSegmentsMetadata",
    "build_segment_prompt",
    "parse_segment_response",
    "synthesize_theme_segments",
    "template_summary",
]
