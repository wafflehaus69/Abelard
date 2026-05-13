"""Pydantic schema for the Brief — the canonical output of synthesis.

A Brief is the unit of synthesis output. One per synthesis run (event-
triggered or pull-triggered). The schema is identical regardless of
trigger source. Briefs are persisted to the filesystem archive
(synthesize/archive.py) — that archive is the source of truth that
Abelard reads via the `briefs list` / `briefs show` CLI surface.

This module defines shape only — no I/O, no model calls. The synthesis
prompt construction (Step 9) produces a JSON payload that parses into
this schema; the materiality gate (Step 8) reads `materiality_score`
on events to make dispatch decisions.

Brief ID format (per Mando's Step 2 decision):

    nwd-{ISO timestamp with dashes for colons}-{8-char uuid4 hex}

Example: `nwd-2026-05-13T14-32-08Z-a1b2c3d4`

Dashes-for-colons keeps the ID filesystem-safe on Windows; the 8-char
suffix disambiguates briefs generated in the same second.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


# ---------- nested models ----------


class SourceHeadline(BaseModel):
    """One concrete headline cited as evidence for an Event."""

    model_config = ConfigDict(extra="forbid")

    publisher: Optional[str]
    headline: str
    url: Optional[str]
    published_at: str


class ThesisLink(BaseModel):
    """How an Event relates to a thesis in Abelard's THESES.md.

    `thesis_id` is a short slug Sonnet generates from the thesis text
    (e.g. `nat-rate-higher`, `iran-cascade`), or `None` if no specific
    thesis is linked (the event is material on other criteria — see
    the synthesis prompt's materiality definition).
    """

    model_config = ConfigDict(extra="forbid")

    thesis_id: Optional[str]
    direction: Literal["confirm", "break", "ambiguous"]
    note: str


class Event(BaseModel):
    """One material event the synthesis call identified in the cluster set."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    headline_summary: str
    themes: list[str]
    source_headlines: list[SourceHeadline] = Field(default_factory=list)
    materiality_score: float = Field(ge=0.0, le=1.0)
    thesis_links: list[ThesisLink] = Field(default_factory=list)


class TriggerWindow(BaseModel):
    """The time window over which this synthesis read the headline corpus."""

    model_config = ConfigDict(extra="forbid")

    since: str
    until: str


class Trigger(BaseModel):
    """Why synthesis ran this time."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["event", "pull"]
    reason: str
    window: TriggerWindow


class Dispatch(BaseModel):
    """The materiality gate's verdict and the sink's outcome.

    `alerted=False` does NOT mean the Brief is discarded; the Brief is
    always archived. `alerted` says "did the AlertSink fire".
    `suppressed_reason` is populated when the gate chose not to dispatch.
    """

    model_config = ConfigDict(extra="forbid")

    alerted: bool
    channel: Optional[Literal["signal", "telegram_bot"]] = None
    suppressed_reason: Optional[str] = None


class DriftProposal(BaseModel):
    """Drift watcher's proposed keyword addition to a theme config.

    Lands in `proposals/pending.json`; resolved by Mando via the
    `proposals approve|reject <id>` CLI. Never auto-applied.

    `notes` (Pass C Step 0 addition) captures Haiku's rationale for the
    suggested_tier choice — triage convenience when Mando reviews.
    """

    model_config = ConfigDict(extra="forbid")

    proposal_id: str
    theme_id: str
    proposed_keyword: str
    suggested_tier: Literal["primary", "secondary", "exclusion"]
    evidence_count: int = Field(ge=0)
    sample_headlines: list[str] = Field(default_factory=list)
    notes: Optional[str] = None
    generated_at: str


class SynthesisMetadata(BaseModel):
    """Cost/model telemetry + theses-doc availability surface.

    Token counts are recorded for cost tracking and for verifying
    prompt-caching effectiveness in the Step 9 checkpoint.

    `theses_doc_warning` is the non-silent surface for the absent-doc
    case (per Pass C §4): if THESES.md isn't readable, synthesis runs
    with a prompt variant that omits the theses block, and this field
    carries the WARN string explaining why thesis_links is empty.
    """

    model_config = ConfigDict(extra="forbid")

    model_used: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    theses_doc_available: bool
    theses_doc_path: Optional[str] = None
    theses_doc_warning: Optional[str] = None


class EnvelopeHealth(BaseModel):
    """Snapshot of source/heartbeat state at synthesis time.

    Lets Abelard inspect what the underlying scrape layer looked like
    when this Brief was generated — useful for debugging why a Brief
    cited certain headlines or missed others.
    """

    model_config = ConfigDict(extra="forbid")

    source_health: dict[str, Any] = Field(default_factory=dict)
    heartbeats: dict[str, Any] = Field(default_factory=dict)


# ---------- the Brief itself ----------


class Brief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    brief_id: str
    generated_at: str
    trigger: Trigger
    themes_covered: list[str]
    events: list[Event] = Field(default_factory=list)
    narrative: str
    dispatch: Dispatch
    drift_proposals: list[DriftProposal] = Field(default_factory=list)
    synthesis_metadata: SynthesisMetadata
    envelope_health: EnvelopeHealth = Field(default_factory=EnvelopeHealth)

    @staticmethod
    def new_brief_id(when: datetime | None = None) -> str:
        """Mint a fresh brief_id: `nwd-{iso-dashed}-{8-char-hex}`."""
        dt = when if when is not None else datetime.now(timezone.utc)
        # Ensure UTC and strip microseconds for stable format.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        iso = dt.strftime("%Y-%m-%dT%H-%M-%SZ")
        suffix = uuid.uuid4().hex[:8]
        return f"nwd-{iso}-{suffix}"


__all__ = [
    "Brief",
    "Dispatch",
    "DriftProposal",
    "EnvelopeHealth",
    "Event",
    "SourceHeadline",
    "SynthesisMetadata",
    "ThesisLink",
    "Trigger",
    "TriggerWindow",
]
