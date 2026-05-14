"""Drift-watcher prompt construction — system + user message.

Pass C Step 10. Builds the message payload for the Haiku drift call.
Caching strategy: ONE breakpoint at end of system prompt. Unlike
synthesis (Step 9), drift has no theses doc — the active themes list
is volatile per call (theme YAMLs can be edited between cycles, and
keyword lists drift as Mando approves proposals), so it lives in the
user message and isn't cached.

The drift watcher's job:
  - Input: a batch of UNTAGGED headlines (passed the scrape pipeline
    without matching any theme's keyword regex) + the active themes
    with their current keyword lists.
  - Output: JSON `{"proposals": [...]}` — each entry suggests a new
    keyword to add to an existing theme's primary / secondary /
    exclusion list, with evidence_count and sample headlines.

The orchestrator (`drift.py`) takes the raw proposals, mints
`proposal_id` + `generated_at`, validates each against the Pydantic
`DriftProposal` schema, applies the orchestrator-side
`min_evidence_count` floor, and returns the list.

Hard rules in the prompt:
  - Only propose keywords for theme_ids in the active themes list.
    No new-theme proposals — those require Mando's direct intervention.
  - Don't propose keywords already in the theme's primary/secondary.
  - Avoid single-word proper nouns (Trump, Apple, etc.) — they create
    cross-theme contamination.
  - evidence_count must be >= 3.
  - Cap at max_proposals_per_batch.

Pure module: no I/O, no API calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..theme_config import ThemeConfig


# ---------- the system prompt (cached at the only breakpoint) ----------
#
# Byte-stable: any edit invalidates the cache prefix.

DRIFT_SYSTEM_PROMPT = """\
You are the drift watcher for an institutional research analyst's news daemon.

Your job: scan UNTAGGED headlines (headlines that did not match any
active theme's keyword regex) for repeated patterns that suggest a
new keyword should be added to an existing theme's keyword list.

[ROLE]
You are a pattern detector, not a domain expert. You look for repeated
nouns or phrases that recur across multiple untagged headlines and
could be added to an existing theme. You do NOT speculate about whether
a topic is "important"; you only report what patterns appear in the
data.

[INPUT FORMAT]
You will receive in the user message:
  1. The set of ACTIVE THEMES — for each theme: theme_id, brief
     excerpt, current primary keywords, current secondary keywords,
     and current exclusions. This is your reference for what already
     exists.
  2. A batch of UNTAGGED HEADLINES — headlines that the keyword regex
     pipeline did not match to any theme.
  3. A constraints block (max_proposals_per_batch, min_evidence_count).

[OUTPUT FORMAT]
Return ONE JSON object, no prose, no markdown fence. Schema:

{
  "proposals": [
    {
      "theme_id": "us_iran_escalation",
      "proposed_keyword": "Yemen-flagged tanker",
      "suggested_tier": "secondary",
      "evidence_count": 5,
      "sample_headlines": [
        "Yemen-flagged tanker seized off Hormuz",
        "Yemen-flagged tanker disabled by mine"
      ],
      "notes": "Phrase appears 5 times across untagged batch; geo+vessel pattern matches this theme's maritime signals."
    }
  ]
}

Field details:
- theme_id: must be one of the theme_ids in active themes list.
- proposed_keyword: the exact string to add to the keyword list.
  Should be a real phrase from the untagged batch that would have
  matched if it had been in the theme's keyword list.
- suggested_tier:
    * "primary": the keyword is unambiguous and theme-defining.
    * "secondary": the keyword provides corroboration alongside primary
      matches; alone it may not be sufficient.
    * "exclusion": the keyword appears in untagged headlines that
      SHOULD be excluded from this theme (false-positive suppressor).
- evidence_count: number of untagged headlines this keyword would
  have matched. Must be >= min_evidence_count (from constraints).
- sample_headlines: up to 3 headlines verbatim from the untagged
  batch that exemplify the proposal.
- notes: 1-2 sentences explaining the rationale for suggested_tier
  and the theme assignment. Optional but encouraged.

[HARD RULES]
1. Do NOT propose keywords that are already in the theme's primary,
   secondary, or exclusion lists. Cross-check against the lists you
   receive in the user message.
2. Do NOT propose theme_ids that aren't in the active themes list.
   Drift proposals are for EXISTING themes only — new-theme proposals
   require direct human intervention and are out of scope here.
3. Do NOT propose single-word proper nouns alone (e.g. "Trump",
   "Apple", "Iran"). Single proper nouns create cross-theme
   contamination because they show up across many themes. Prefer
   two-word phrases or longer (e.g. "Apple antitrust", "Iran sanctions
   relief"). Single-word common nouns are fine when domain-specific
   enough (e.g. "yield curve", "cease-fire").
4. Each proposal must have evidence_count >= min_evidence_count.
   One-off appearances are noise, not drift.
5. Cap proposals at max_proposals_per_batch (from constraints). If
   you have more candidates, return the highest-evidence-count ones.
6. IF no patterns rise above the evidence floor: return
   {"proposals": []}.
7. Output JSON only - no preamble, no markdown fence, no commentary
   before or after the JSON object.
"""


# ---------- helpers ----------


def _iso_from_unix(ts: int) -> str:
    """Render Unix-seconds back to ISO-8601 for the prompt's headline list."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_theme_block(theme: ThemeConfig, brief_excerpt_chars: int = 500) -> str:
    """Render one theme as a block Haiku can reference.

    Truncates the long-form brief to keep prompt size reasonable —
    Haiku only needs the gist plus the keyword lists to judge
    candidate-keyword fit.
    """
    brief_text = theme.brief.strip()
    if len(brief_text) > brief_excerpt_chars:
        brief_text = brief_text[:brief_excerpt_chars].rstrip() + " ..."

    primary = ", ".join(theme.keywords.primary) if theme.keywords.primary else "(none)"
    secondary = ", ".join(theme.keywords.secondary) if theme.keywords.secondary else "(none)"
    exclusions = ", ".join(theme.keywords.exclusions) if theme.keywords.exclusions else "(none)"

    return (
        f"## {theme.theme_id}\n"
        f"Brief: {brief_text}\n"
        f"Primary keywords: {primary}\n"
        f"Secondary keywords: {secondary}\n"
        f"Exclusions: {exclusions}"
    )


# ---------- public surface ----------


def build_user_prompt(
    *,
    themes: list[ThemeConfig],
    untagged: list[tuple[str | None, str, int]],
    max_proposals_per_batch: int,
    min_evidence_count: int,
    theme_brief_excerpt_chars: int = 500,
) -> str:
    """Build the per-run user-message text. NOT cached.

    Args:
        themes: active themes (theme_id, brief, keyword lists).
        untagged: list of (publisher, headline, published_at_unix)
            tuples. The drift watcher works on text alone — URL is
            unused. Publisher is rendered for context only.
        max_proposals_per_batch: hard cap on output.
        min_evidence_count: floor (mirrors orchestrator-side filter).
        theme_brief_excerpt_chars: truncation length for each theme's
            brief text in the prompt. Default 500 chars keeps Haiku's
            attention focused without over-quoting.
    """
    sections: list[str] = []

    sections.append("[ACTIVE THEMES]")
    sections.append("")
    for theme in themes:
        sections.append(_format_theme_block(theme, theme_brief_excerpt_chars))
        sections.append("")

    sections.append(
        f"[UNTAGGED HEADLINES ({len(untagged)} total)]"
    )
    if not untagged:
        sections.append("(none — no untagged headlines in this batch)")
    else:
        for i, (publisher, headline, ts_unix) in enumerate(untagged, start=1):
            pub = publisher or "?"
            sections.append(f"{i}. [{pub} | {_iso_from_unix(ts_unix)}] {headline}")
    sections.append("")

    sections.append("[CONSTRAINTS]")
    sections.append(f"max_proposals_per_batch: {max_proposals_per_batch}")
    sections.append(f"min_evidence_count: {min_evidence_count}")
    sections.append("Output JSON only, no preamble.")

    return "\n".join(sections)


def build_system_blocks() -> list[dict[str, Any]]:
    """Build the single cached system block for the drift Messages API call.

    Drift has no theses doc — one cache breakpoint, always. The active
    themes (which drift NEEDS context for) go in the user prompt
    because they're volatile (keyword lists change as Mando approves
    proposals).
    """
    return [
        {
            "type": "text",
            "text": DRIFT_SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        },
    ]


def build_messages_payload(
    *,
    themes: list[ThemeConfig],
    untagged: list[tuple[str | None, str, int]],
    max_proposals_per_batch: int,
    min_evidence_count: int,
) -> dict[str, Any]:
    """Build the `system` + `messages` kwargs for `client.messages.create()`.

    Caller adds `model`, `max_tokens`. Drift uses
    `claude-haiku-4-5` per DriftWatcherConfig default.
    """
    return {
        "system": build_system_blocks(),
        "messages": [
            {
                "role": "user",
                "content": build_user_prompt(
                    themes=themes,
                    untagged=untagged,
                    max_proposals_per_batch=max_proposals_per_batch,
                    min_evidence_count=min_evidence_count,
                ),
            },
        ],
    }


__all__ = [
    "DRIFT_SYSTEM_PROMPT",
    "build_messages_payload",
    "build_system_blocks",
    "build_user_prompt",
]
