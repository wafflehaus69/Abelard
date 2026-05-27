"""Pydantic schema for the AttentionBrief — output of Pass E synthesis.

An AttentionBrief is the unit of ATTENTION output. One per crossing term per
cycle. Persisted to the same archive tree as Pass C Briefs (under YYYY-MM/
partitions) and dispatched through the same AlertSink interface, but with
a distinct schema and a distinct filename infix (`nwd-attn-...`) so readers
can route to the right validation model.

Distinct from Brief in synthesize/brief.py:
  - No materiality_score (statistical threshold IS the gate)
  - No thesis_links (Abelard's downstream layer handles thesis intersection)
  - No events list (the entire brief IS one term's attention shape)
  - Descriptive narrative, not evaluative
  - Attention-shape categorical for downstream routing

Schema is rigid (extra="forbid") per the same load-bearing discipline as
Brief; cross-brief-type compatibility is via Union types at dispatch and
archive boundaries, not via base-class sharing.

Brief ID format (per Pass E Q3 decision, 2026-05-26):

    nwd-attn-{ISO timestamp with dashes for colons}-{8-char uuid4 hex}

Example: `nwd-attn-2026-05-26T22-50-43Z-9b3cfa83`

The `attn` infix lets the shared archive partitioner (_month_partition in
synthesize/archive.py) branch on parts[1] to recover YYYY-MM correctly.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..synthesize.brief import Dispatch, SynthesisMetadata


# Closed set of attention-shape categorical labels. Pass E build decision
# (Q5, 2026-05-26): fail-loud on out-of-set values per epistemic discipline.
AttentionShape = Literal[
    "single_event_dominant",
    "multi_source_convergence",
    "slow_burn",
    "narrow_source_spike",
    "cross_topic_recurrence",
    "unclear",
]


class AttentionBrief(BaseModel):
    """One ATTENTION brief — descriptive synthesis on one threshold-crossing term.

    Constructed by the orchestrator from the LLM's JSON response (narrative,
    source_mix, entities_observed, attention_shape) plus orchestrator-owned
    fields (brief_id, generated_at, trigger stats, dispatch, telemetry).
    """

    model_config = ConfigDict(extra="forbid")

    brief_id: str
    generated_at: str
    brief_type: Literal["attention"] = "attention"

    # Trigger context — set by the orchestrator from the threshold result
    triggering_term: str
    term_frequency_window: int = Field(ge=0)
    term_frequency_prior: int = Field(ge=0)
    cluster_size: int = Field(ge=0)

    # LLM-produced fields
    narrative: str
    source_mix: dict[str, int] = Field(default_factory=dict)
    entities_observed: list[str] = Field(default_factory=list)
    attention_shape: AttentionShape

    # Dispatch + telemetry — reused from Pass C schema
    dispatch: Dispatch
    synthesis_metadata: SynthesisMetadata

    @staticmethod
    def new_brief_id(when: datetime | None = None) -> str:
        """Mint a fresh ATTENTION brief_id: `nwd-attn-{iso-dashed}-{8-char-hex}`.

        Filesystem-safe (dashes for colons) and parseable by the shared
        archive partitioner via the `attn` infix at parts[1].
        """
        dt = when if when is not None else datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        iso = dt.strftime("%Y-%m-%dT%H-%M-%SZ")
        suffix = uuid.uuid4().hex[:8]
        return f"nwd-attn-{iso}-{suffix}"


__all__ = ["AttentionBrief", "AttentionShape"]
