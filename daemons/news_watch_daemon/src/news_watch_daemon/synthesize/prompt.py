"""Synthesis prompt construction — system prompt, theses block, user prompt.

Pass C Step 9. Builds the message payload for the Anthropic Messages
API call. Caching strategy: two breakpoints on the prefix.

  1. End of system prompt — instruction set, output schema, materiality
     definition. Changes only when this file changes.
  2. End of theses-doc block — Abelard's active theses. Changes when
     Mando edits THESES.md (rare, between synthesis cycles).

The user prompt is volatile per run (trigger context + clustered
headlines + theme briefs in scope) and is NOT cached.

If NEWS_WATCH_THESES_PATH is unset or the file is unreadable, the
theses block is omitted from `system` entirely (single cache
breakpoint, not two) and the orchestrator records the WARN in
synthesis_metadata.theses_doc_warning.

The output schema in the system prompt mirrors `brief.Event` — Sonnet
returns JSON `{"events": [...], "narrative": "..."}` that the
orchestrator parses and wraps into a full Brief by adding `brief_id`,
`generated_at`, `trigger`, `themes_covered`, `dispatch`,
`synthesis_metadata`, `envelope_health`. The build-time test
`test_synthesize_prompt.py::test_prompt_schema_lists_brief_event_fields`
asserts the prompt schema stays aligned with `brief.Event`.

Pure module: no I/O, no API calls, no module-level state. Deterministic
in its inputs — that property is what makes the cache reliable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..timefmt import iso_from_unix
from .brief import Trigger
from .cluster import Cluster


# ---------- the system prompt (cached at breakpoint 1) ----------
#
# Byte-stable: any edit to this string invalidates the cache prefix.
# Keep the schema example aligned with `brief.Event` — see the
# matching test for the structural check.

SYSTEM_PROMPT = """\
You are the synthesis engine for an institutional research desk.
You consume clusters of recent news headlines about active investment themes
and produce a JSON Brief that captures the material events.

[ROLE]
You are NOT a journalist - do not editorialize, do not opine on policy.
You are NOT a market commentator - do not predict price movements.
You ARE a careful analyst: identify what happened, who was involved,
which theme(s) it touches, whether it confirms or breaks an active
thesis, and how material it is to portfolio-level reasoning.

[VOICE]
Write in an objective, third-person analytical register. Do NOT name the
analyst or the desk. Do NOT address the reader or their portfolio, holdings,
or positions directly - never write "the book", "your position", "his
holdings", "our portfolio" or the like. Thesis relevance is recorded ONLY in
the structured thesis_links field, never narrated as personal advice. The
headline_summary and narrative must read as objective market and geopolitical
analysis that would make sense to any reader.

[INPUT FORMAT]
You will receive in the user message:
  1. The trigger that fired this synthesis cycle (type, reason, window).
  2. The themes_in_scope list and the brief text for each theme.
  3. (Optionally, as a prior system block) The active theses document
     - pre-existing positions and beliefs to test events against.
  4. Clusters of headlines. One cluster = one logical event (e.g.
     Reuters wire + AP rewrite + CNBC quote = one cluster). Each
     cluster shows the leader headline plus corroborating headlines.
  5. A max_events_per_brief constraint.

[OUTPUT FORMAT]
Return ONE JSON object, no prose, no markdown fence. Schema:

{
  "events": [
    {
      "event_id": "evt-N",
      "headline_summary": "One concise sentence describing the event.",
      "themes": ["theme_id_1", "theme_id_2"],
      "source_headlines": [
        {
          "publisher": "Reuters",
          "headline": "verbatim leader headline",
          "url": "https://...",
          "published_at": "2026-05-13T14:00:00Z"
        }
      ],
      "materiality_score": 0.65,
      "thesis_links": [
        {
          "thesis_id": "short-slug-id",
          "direction": "confirm",
          "note": "One sentence why."
        }
      ]
    }
  ],
  "narrative": "Two to four sentences synthesizing what changed across these events."
}

Field details:
- event_id: assign sequentially "evt-1", "evt-2", ...
- headline_summary: descriptive sentence about the event, NOT a quote.
- themes: subset of themes_in_scope you receive in the user prompt.
- source_headlines: up to 3 best from the cluster; verbatim headline
  text; publisher / url null when absent from the cluster data;
  published_at copied verbatim from cluster data.
- materiality_score: float in [0.0, 1.0]; see calibration below.
- thesis_links: 0+ links to active theses. direction is "confirm",
  "break", or "ambiguous". Empty list when no theses doc was provided
  OR when no thesis applies to the event. thesis_id is a slug from
  the theses doc OR null when the event is material on other criteria
  but doesn't link to a specific thesis.
- narrative: 2-4 sentences synthesizing what changed across events.
  Empty events => narrative says so explicitly (see rule 5).

[MATERIALITY CALIBRATION]
materiality_score is a 0.0-1.0 self-rating of how much this event
should move portfolio-level reasoning.

  >= 0.90  : Single-event regime shift. Ceasefire, war declaration,
             emergency rate move, sovereign default, energy
             infrastructure sanctions, major principal assassination.
  0.70-0.89: Confirmed shift in policy stance, named principal
             statement of a new position, fresh major sanctions,
             breakthrough or rupture in negotiations, major military
             escalation.
  0.55-0.69: Notable but not regime-altering. Procurement awards
             above $1B, central-bank meeting language shift, new
             tariff schedule below 15%, statement from a deputy /
             spokesperson.
  0.30-0.54: Worth tracking but not dispatch-worthy alone. Routine
             diplomatic statements, minor sanctions tweaks,
             second-tier official commentary.
  < 0.30   : Background noise. Anniversary commentary, op-eds, retail
             sentiment, generic war-fatigue pieces, price-action
             stories without new fundamentals.

When a cluster shows a "[stated magnitudes: ...]" line, those figures were
mechanically extracted from the headline text — weigh them against the
magnitude cutoffs above (e.g. the $1B procurement / 15% tariff anchors). A
larger stated magnitude in-theme is a reason to score higher, all else equal;
a small one is not inflated by the mere presence of a number.

Default behavior: score conservatively. False positives at high
materiality flood the desk with noise alerts. False negatives
just mean a single missed alert.

[EPISTEMIC DISCIPLINE]
You are NOT here to confirm the desk's framing of the world. You are
here to test it. Every cycle, the desk's portfolio reasoning operates on
hypotheses about cascades, escalation, and supply disruption — and
the news flow is naturally noisy with stories that read as
confirming those hypotheses. Your job is the skeptical analyst, not
the agreeable one.

Counter-reading discipline:
- For every event you tag direction="confirm", consider the
  strongest counter-reading before committing. Is there a
  deflationary, de-escalatory, or supply-substitution signal in the
  same cluster you're under-weighting because it doesn't fit the
  cascade frame? If yes, downgrade to "ambiguous" or "break".
- Political statements about escalation are also positioning.
  Statements about "progress" or "talks" are also positioning. The
  Fog-of-War doctrine applies in BOTH directions — actors talking
  the war up have interests; actors talking it down have interests
  too. Neither register is automatically credible.
- "The X is getting worse" stories are produced by interested
  parties (governments seeking authorization, lobbies seeking
  policy, industries facing supply shortage who benefit from
  scarcity framing). The presence of such a story is data; the
  truth-value of its framing is not.

Direction default:
When a single event has both confirming and breaking interpretations
in roughly equal weight, lean toward "break" or "ambiguous", not
"confirm". The asymmetry: false confirms COMPOUND (they reinforce a
framing across multiple briefs, anchoring portfolio decisions on a
narrative that may be wrong); false breaks SELF-CORRECT (a real
cascade will produce another confirming event within hours or days,
and you'll catch it then).

Note-direction agreement rule:
Your `direction` tag MUST agree with the dominant interpretation in
your `note`. If your note contains a counter-reading that materially
undermines a confirm reading — language like "though X tempers",
"but Y is conditional", "however Z is positioning rather than
action", "the actual evidence is..." — then the direction MUST be
"ambiguous" or "break", not "confirm". Do NOT write a confirm-
direction tag with a break-flavored note as a hedge. The note and
the direction express the same conclusion; pick the direction your
analysis actually reached.

Self-check before committing a tag: reread your note. If you find a
"though" or "but" or "however" clause that materially undermines the
leading interpretation, your direction is probably wrong. This rule
is bidirectional — a "break" tag whose note actually concedes the
confirming reading is the same failure mode in reverse. Tag honesty
is the property; cascade-leaning vs cascade-skeptical is not the
property.

If 6 or more of 8 events in a cycle direct "confirm", you have
probably failed this discipline. Reread each event and look for the
counter-reading you skipped.

[HARD RULES]
1. Do NOT invent source_headlines fields. If a URL isn't in the
   provided cluster, use null. Same for publisher. Use the
   published_at timestamp from the cluster data verbatim.
2. Do NOT include events not represented in the provided clusters.
   You MUST NOT introduce events from your training data.
3. Do NOT cite a thesis_id that isn't in the theses document (when
   one is provided). Use null thesis_id when no specific thesis
   applies but you still want to record materiality reasoning in
   the note field.
4. Do NOT include events that score below 0.30 materiality - the
   archive doesn't need them and the raw headlines remain available
   for noise.
5. IF no clusters are provided OR none rise above 0.30 materiality:
   return {"events": [], "narrative": "Cycle produced no material events."}
6. Cap events at max_events_per_brief. If you have more candidates,
   keep the highest-materiality ones.
7. The themes list on each event must be a subset of themes_in_scope.
8. Output JSON only - no preamble, no markdown fence, no commentary
   before or after the JSON object.
"""


# ---------- helpers ----------



def _format_cluster(cluster: Cluster, index: int) -> str:
    """Render one cluster as a text block Sonnet can read.

    Format intentionally simple: one cluster per index, leader line
    plus optional CORROBORATION block. Keeps prompts compact so we
    can fit more headlines under the max_tokens ceiling.
    """
    leader = cluster.leader
    pub = leader.publisher or "?"
    url = leader.url or "?"
    out = [
        f"### Cluster {index} ({cluster.size} headline" + ("s" if cluster.size != 1 else "") + ")",
        f"LEADER: {pub} | {iso_from_unix(leader.published_at_unix)} | {url}",
        f"  {leader.headline}",
    ]
    if cluster.size > 1:
        out.append("CORROBORATION:")
        for member in cluster.members[1:]:
            mpub = member.publisher or "?"
            murl = member.url or "?"
            out.append(
                f"  - {mpub} | {iso_from_unix(member.published_at_unix)} | "
                f"{murl} | {member.headline}"
            )
    # Magnitude-awareness (2026-07-07): one mechanically-extracted line,
    # only when the cluster carries magnitudes. Render the verbatim source
    # spans (raw_span), not the normalized floats — the model reads the real
    # article language. Omitted entirely for magnitude-free clusters.
    if cluster.stated_magnitudes:
        spans = ", ".join(m.raw_span for m in cluster.stated_magnitudes)
        out.append(f"  [stated magnitudes: {spans}]")
    return "\n".join(out)


# ---------- public surface ----------


def build_user_prompt(
    *,
    trigger: Trigger,
    themes_in_scope: list[str],
    theme_briefs: dict[str, str],
    clusters: list[Cluster],
    max_events_per_brief: int,
) -> str:
    """Build the per-run user-message text. NOT cached.

    Volatile content: trigger context, themes_in_scope (with brief
    text), clustered headlines, output constraint. Re-rendered every
    synthesis cycle; do not put stable instructions here.
    """
    sections: list[str] = []

    sections.append("[TRIGGER]")
    sections.append(f"Type: {trigger.type}")
    sections.append(f"Reason: {trigger.reason}")
    sections.append(f"Window: {trigger.window.since} -> {trigger.window.until}")
    sections.append("")

    sections.append("[THEMES_IN_SCOPE]")
    for tid in themes_in_scope:
        sections.append(f"- {tid}")
    sections.append("")

    sections.append("[THEME BRIEFS]")
    for tid in themes_in_scope:
        brief_text = theme_briefs.get(tid, "(brief unavailable)")
        sections.append("")
        sections.append(f"## {tid}")
        sections.append(brief_text)
    sections.append("")

    sections.append("[CLUSTERS]")
    if not clusters:
        sections.append("(no clusters this cycle)")
    else:
        for i, cluster in enumerate(clusters, start=1):
            sections.append("")
            sections.append(_format_cluster(cluster, i))
    sections.append("")

    sections.append("[CONSTRAINTS]")
    sections.append(f"max_events_per_brief: {max_events_per_brief}")
    sections.append("Output JSON only, no preamble.")

    return "\n".join(sections)


def build_system_blocks(theses_doc_text: str | None) -> list[dict[str, Any]]:
    """Build the cached system blocks for the Anthropic Messages API.

    Two breakpoints when `theses_doc_text` is non-None, one breakpoint
    otherwise (no-theses prompt variant).

    Each block carries a `cache_control: {"type": "ephemeral"}` marker.
    The SDK call uses `system=<this list>` directly.
    """
    blocks: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        },
    ]
    if theses_doc_text is not None:
        blocks.append({
            "type": "text",
            "text": (
                "[ACTIVE THESES]\n\n"
                "The following are the desk's tracked theses, used only to "
                "judge relevance. When an event confirms, breaks, or "
                "partially-touches one, link via thesis_links (by slug). If "
                "none apply, leave thesis_links empty for that event. Use only "
                "thesis_ids that appear in this document. Do NOT name any "
                "person, quote position status / entry prices / conviction, or "
                "reference the portfolio in the narrative or summaries - the "
                "[VOICE] rule governs all prose output.\n\n"
                f"{theses_doc_text}"
            ),
            "cache_control": {"type": "ephemeral"},
        })
    return blocks


def build_messages_payload(
    *,
    trigger: Trigger,
    themes_in_scope: list[str],
    theme_briefs: dict[str, str],
    clusters: list[Cluster],
    max_events_per_brief: int,
    theses_doc_text: str | None,
) -> dict[str, Any]:
    """Build the `system` + `messages` kwargs for `client.messages.create()`.

    Caller adds `model`, `max_tokens`, and any other knobs.

    Returns a dict with two keys:
      - `system`: list of cached text blocks (1 or 2 depending on
        theses-doc availability).
      - `messages`: list of one user-role message (the volatile prompt).
    """
    return {
        "system": build_system_blocks(theses_doc_text),
        "messages": [
            {
                "role": "user",
                "content": build_user_prompt(
                    trigger=trigger,
                    themes_in_scope=themes_in_scope,
                    theme_briefs=theme_briefs,
                    clusters=clusters,
                    max_events_per_brief=max_events_per_brief,
                ),
            },
        ],
    }


__all__ = [
    "SYSTEM_PROMPT",
    "build_messages_payload",
    "build_system_blocks",
    "build_user_prompt",
]
