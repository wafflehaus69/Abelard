"""Pydantic schema for FullBriefEnvelope — the canonical Full Brief artifact.

Per Abelard's Full Brief spec v1.0 + 2026-05-29 amendments. Composes Pass C
theme-event synthesis + Pass E ATTENTION sweep + convergence analysis +
frequency diagnostic into one structured deliverable.

Schema discipline: nested sub-section models, NOT a flat structure with
30+ top-level fields. Each section is independently testable and
reviewable. extra="forbid" everywhere matches the project's existing
posture (Brief, AttentionBrief).

Brief ID + filename convention
------------------------------
  Field name:  `brief_id: str`
  Value:       `nwd-fullbrief-{YYYY-MM-DDTHH-MM-SSZ}-{8-char-hex}`
  Filename:    `{brief_id}.json` in YYYY-MM partition

DEVIATION FROM SPEC EXAMPLE (Abelard-approved 2026-05-29):
Spec Section 5 example block showed `full_brief_id: "fb-..."`. That was
draft-time inconsistency. Canonical form follows the `nwd-` namespace
convention used by Brief and AttentionBrief: archive routing
(_BRIEF_TYPE_INFIXES) discriminates on `parts[1]` of the brief_id, and
all news_watch_daemon artifacts share the `nwd-` prefix family. Spec
text will be corrected in the doctrine integration commit at Stage 2b
close. This deviation is documented here so future readers can
reconstruct the choice without re-litigating it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Window + Executive summary
# ---------------------------------------------------------------------------


class WindowSection(BaseModel):
    """Time window covered by this Full Brief."""

    model_config = ConfigDict(extra="forbid")

    since: str
    until: str
    duration_hours: int = Field(ge=1, le=168)


class ExecutiveSummary(BaseModel):
    """Top-of-brief summary panel — quick-read stats for human review."""

    model_config = ConfigDict(extra="forbid")

    narrative: str
    dominant_themes: list[str] = Field(default_factory=list)
    material_event_count: int = Field(ge=0)
    attention_crossings_count: int = Field(ge=0)
    orphan_crossings_count: int = Field(ge=0)
    highest_materiality_score: Optional[float] = Field(default=None, ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# Theme synthesis section (Pass C)
# ---------------------------------------------------------------------------


class ThemeEventDigest(BaseModel):
    """Compact event reference.

    Full Pass C Event lives in the linked Pass C brief artifact
    (theme_synthesis.brief_path). This digest is the denormalized
    rendering input for the Full Brief's top-level theme synthesis view.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: str
    headline_summary: str
    themes: list[str] = Field(default_factory=list)
    materiality_score: float = Field(ge=0.0, le=1.0)
    direction: Optional[Literal["confirm", "break", "ambiguous"]] = None
    source_count: int = Field(ge=0)
    thesis_links: list[dict[str, Any]] = Field(default_factory=list)


class ThemeSynthesisSection(BaseModel):
    """Pass C theme-event synthesis section.

    Status discriminates the case (per Q2 resolution 2026-05-29):
      - "ok": Pass C ran successfully; brief_id + brief_path populated
      - "no_trigger": Pass C trigger gate didn't fire (quiet day, valid
        outcome — not an error). brief_id + brief_path are null,
        no_trigger_reason populated with the trigger gate's diagnostic.
      - "failed": Pass C attempted but failed; failure_reason populated.

    When status != "ok", `events`, `narrative`, `direction_tally`, and
    `themes_covered` are empty/null (no synthesis output to surface).
    """

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "no_trigger", "failed"]
    brief_id: Optional[str] = None
    brief_path: Optional[str] = None
    narrative: Optional[str] = None
    themes_covered: list[str] = Field(default_factory=list)
    events: list[ThemeEventDigest] = Field(default_factory=list)
    direction_tally: Optional[dict[str, int]] = None
    theses_doc_available: bool = False
    theses_doc_warning: Optional[str] = None
    no_trigger_reason: Optional[str] = None
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Theme segments section — guaranteed per-theme coverage (2026-06-30)
# ---------------------------------------------------------------------------


class ThemeSegment(BaseModel):
    """One tracked theme's guaranteed per-brief segment.

    Pass C only synthesizes themes that land in the trigger's scope (the
    first firing signal's themes), so a theme with many tagged headlines
    can be dropped from the brief entirely if it wasn't the first signal
    (the russia(47)/iran(20)/fed(17)-dropped finding, 2026-06-30). This
    section guarantees EVERY tracked theme surfaces every brief.

    status:
      - "active": in Pass C scope OR tagged-headline count over the
        activity threshold — gets a 2-3 sentence synthesis.
      - "quiet": tracked but below threshold and out of scope — gets a
        single "why it's hot" one-liner.

    `summary` is the batched-LLM line (one Sonnet call covers all themes).
    On LLM failure it degrades to a deterministic template line so the
    segment still renders. `convergence_terms` are attention-crossing
    terms that appear in this theme's window headlines.
    """

    model_config = ConfigDict(extra="forbid")

    theme_id: str
    display_name: str
    status: Literal["active", "quiet"]
    tagged_headline_count: int = Field(ge=0)
    in_pass_c_scope: bool
    summary: str
    convergence_terms: list[str] = Field(default_factory=list)


class ThemeSegmentsSection(BaseModel):
    """Guaranteed-coverage roll-up: one ThemeSegment per tracked theme.

    status:
      - "ok": segments assembled (LLM summaries or degraded templates).
      - "skipped": no themes / feature not run (default at envelope init).
      - "failed": the step itself could not assemble segments.

    `llm_degraded` flags that the batched summary call failed and the
    summaries are deterministic templates — surfaced so the operator
    knows the lines are counts, not interpretation.
    """

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "skipped", "failed"]
    segments: list[ThemeSegment] = Field(default_factory=list)
    llm_degraded: bool = False
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Attention synthesis section (Pass E)
# ---------------------------------------------------------------------------


class ConvergenceInfo(BaseModel):
    """Convergence judgment for one attention crossing.

    Mirrors the leaf module's `ConvergenceResult` but as a Pydantic model
    for serialization. Per Q6 + Adjustment 5: strict-headline ASCII
    substring match against Pass C event source_headlines.
    """

    model_config = ConfigDict(extra="forbid")

    status: Literal["convergent", "orphan", "unknown"]
    converges_with: list[str] = Field(default_factory=list)
    orphan_reason: Optional[str] = None


class AttentionCrossing(BaseModel):
    """One Pass E attention crossing as embedded in the Full Brief envelope.

    Carries denormalized data needed for rendering + the link to the
    underlying AttentionBrief artifact (attention_brief_id + path).
    `llm_read_summary` is the brief's full narrative, denormalized here for
    inline context — avoids forcing the operator to open the linked brief for
    a quick read. (Previously capped at ~280 chars; uncapped 2026-07-08 so the
    orphan "review first" readout is complete in the brief itself.)
    """

    model_config = ConfigDict(extra="forbid")

    term: str
    freq_window: int = Field(ge=0)
    freq_prior: int = Field(ge=0)
    delta_ratio: float = Field(ge=0.0)
    shape: str   # AttentionShape values; kept as str here since validation
                 # happens at AttentionBrief level (and shape comes pre-validated).
    attention_brief_id: str
    attention_brief_path: str
    convergence: ConvergenceInfo
    llm_read_summary: str


class AttentionSynthesisSection(BaseModel):
    """Pass E ATTENTION sweep section.

    When Pass E succeeds with 0 crossings, status="ok" and crossings=[].
    When Pass E itself fails (e.g., counter exception), status="failed"
    and failure_reason populated.
    """

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "failed"]
    crossings: list[AttentionCrossing] = Field(default_factory=list)
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Frequency diagnostic section (Adjustments 1 + 2)
# ---------------------------------------------------------------------------


class FrequencyDiagnosticCrossingRow(BaseModel):
    """Compact crossing-table row inside frequency_diagnostic.

    Distinct from `AttentionCrossing` above (which is the full link).
    This is the diagnostic-table view: term + counts + shape + convergence
    status for at-a-glance scanning.
    """

    model_config = ConfigDict(extra="forbid")

    term: str
    freq_window: int = Field(ge=0)
    freq_prior: int = Field(ge=0)
    shape: str
    convergence: Literal["convergent", "orphan", "unknown"]


class FrequencyDiagnosticNearMissRow(BaseModel):
    """Near-miss table row (Adjustment 1: unbounded in JSON).

    Sort order in the assembled list: freq_window desc, ties by
    delta_ratio desc. Reason classification mirrors threshold module:
    `below_window_min` or `above_prior_max`.
    """

    model_config = ConfigDict(extra="forbid")

    term: str
    freq_window: int = Field(ge=0)
    freq_prior: int = Field(ge=0)
    delta_ratio: float = Field(ge=0.0)
    reason_not_crossed: Literal["below_window_min", "above_prior_max"]


class FrequencyDiagnosticSection(BaseModel):
    """Pass E frequency analysis surface — crossings + sustained-attention near-misses.

    Adjustment 2 (2026-05-29): `threshold_note` is non-null when the
    Full Brief window_hours != 24, informing programmatic consumers
    that the absolute thresholds (COLD_START_WINDOW_MIN=10,
    COLD_START_PRIOR_MAX=3) are tuned for 24h windows and may produce
    fewer/more crossings at non-default windows. When window_hours==24,
    `threshold_note` is null and the field is omitted from rendering.

    Adjustment 1 (2026-05-29): `near_misses` is unbounded in JSON. The
    rendering module applies a soft cap of 50 with an overflow footer;
    JSON consumers always see the full list.
    """

    model_config = ConfigDict(extra="forbid")

    threshold_note: Optional[str] = None
    crossings: list[FrequencyDiagnosticCrossingRow] = Field(default_factory=list)
    near_misses: list[FrequencyDiagnosticNearMissRow] = Field(default_factory=list)
    diagnostic_note: str


# ---------------------------------------------------------------------------
# Pass F footprint
# ---------------------------------------------------------------------------


class PassFFootprint(BaseModel):
    """Per-cycle Pass F translation visibility.

    `cross_language_event_merges`: count of Pass C events whose
    source_headlines mix translated (`language != 'en'`) and en-only
    rows. Empirically 1 in cycle 1 (evt-5 Bloomberg + Ateo), 0 in cycle 2.

    `attention_crossings_enabled_by_pass_f`: list of crossing terms
    where >50% of the cluster items are translated rows — i.e.,
    crossings that would not have fired without translation
    contributing tokens. Empirically `["putin"]` in cycle 1, `[]` in
    cycle 2.

    `url_match_warnings`: defensive audit signal added in Stage 2a-ii-B
    (2026-05-29). When the orchestrator batch-queries headlines by URL
    to look up language for `cross_language_event_merges`, this field
    surfaces the count of source_headline URLs that didn't match any
    DB row. Null when all URLs matched (the expected case — Sonnet
    reproduces URLs verbatim from cluster inputs). Non-null indicates
    URL drift (prompt change, model update, edge content) that would
    silently under-report the metric without this audit signal.
    """

    model_config = ConfigDict(extra="forbid")

    translated_rows_in_window: int = Field(ge=0)
    cross_language_event_merges: int = Field(ge=0)
    attention_crossings_enabled_by_pass_f: list[str] = Field(default_factory=list)
    url_match_warnings: Optional[int] = None


# ---------------------------------------------------------------------------
# Envelope health
# ---------------------------------------------------------------------------


class StepHealth(BaseModel):
    """Per-step status block.

    All optional context fields default to None — populated only where
    they apply (e.g., scrape carries headlines_inserted + sources_failed,
    Pass E carries crossings_count).
    """

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "failed", "skipped"]
    headlines_inserted: Optional[int] = None
    sources_failed: Optional[int] = None
    crossings_count: Optional[int] = None
    reason: Optional[str] = None


class FullBriefEnvelopeHealth(BaseModel):
    """Per-step health snapshot covering all Step 1-7 phases of execution."""

    model_config = ConfigDict(extra="forbid")

    scrape: StepHealth
    pass_c: StepHealth
    pass_e: StepHealth
    convergence_analysis: StepHealth
    frequency_diagnostic: StepHealth


# ---------------------------------------------------------------------------
# Cost envelope (mirrors fullbrief/cost.py output shape)
# ---------------------------------------------------------------------------


class CostPerBrief(BaseModel):
    """Per-Pass-C-brief cost breakdown across the four Anthropic token categories."""

    model_config = ConfigDict(extra="forbid")

    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cache_creation_tokens: int = Field(ge=0)
    cache_read_tokens: int = Field(ge=0)
    usd: float = Field(ge=0.0)


class CostPerAttentionBrief(BaseModel):
    """Per-attention-brief cost breakdown with attention_brief_id linkage."""

    model_config = ConfigDict(extra="forbid")

    attention_brief_id: str
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    cache_creation_tokens: int = Field(ge=0)
    cache_read_tokens: int = Field(ge=0)
    usd: float = Field(ge=0.0)


class CostEnvelope(BaseModel):
    """Cost breakdown for the Full Brief per Adjustment 4.

    Per Option A discipline: `pass_c` is None when Pass C did not run
    (no_trigger or upstream failure), distinguishable from a zero-cost
    object meaning "ran but produced no measurable output." Same applies
    to `pass_e_briefs` — empty list means "Pass E ran, zero crossings,"
    NOT a list of zero-cost objects.

    `pass_e_total_usd` is a convenience sum; downstream readers may
    use it directly rather than summing the per-brief array.
    `rates_as_of` carries the rate-table effective date — historical
    Full Briefs surface this so consumers know which rate generated
    the number.
    """

    model_config = ConfigDict(extra="forbid")

    pass_c: Optional[CostPerBrief] = None
    pass_e_briefs: list[CostPerAttentionBrief] = Field(default_factory=list)
    pass_e_total_usd: float = Field(ge=0.0)
    # Single batched theme-segments call (2026-06-30). None when the step
    # did not run (no key / skipped); a zero-cost object would wrongly imply
    # "ran but free," so None is the honest not-run signal (same discipline
    # as pass_c above).
    theme_segments: Optional[CostPerBrief] = None
    total_usd: float = Field(ge=0.0)
    model: str
    rates_as_of: str


# ---------------------------------------------------------------------------
# Pass failures + top-level envelope
# ---------------------------------------------------------------------------


class PassFailure(BaseModel):
    """One step's failure descriptor.

    Populated in `pass_failures` array on the top-level envelope when a
    step failed but the brief still assembled (per spec exit-code 2
    semantics).
    """

    model_config = ConfigDict(extra="forbid")

    step: str
    reason: str
    recovered: bool


class FullBriefEnvelope(BaseModel):
    """Top-level Full Brief artifact.

    See module docstring for brief_id naming convention deviation from
    spec example. Filename pattern: `{brief_id}.json` in YYYY-MM
    partition; routing via archive._BRIEF_TYPE_INFIXES["fullbrief"].
    """

    model_config = ConfigDict(extra="forbid")

    brief_id: str = Field(
        description=(
            "Canonical brief identifier matching the archive filename. "
            "Format: 'nwd-fullbrief-{ISO8601-dashed}-{8char_hex}'. "
            "Note: Spec §5 example showed 'full_brief_id' and 'fb-' "
            "prefix — that was draft-time inconsistency. Canonical form "
            "follows the 'nwd-' namespace convention used by Brief and "
            "AttentionBrief."
        ),
    )
    brief_type: Literal["full_brief"] = "full_brief"
    generated_at: str
    window: WindowSection
    executive_summary: ExecutiveSummary
    theme_synthesis: ThemeSynthesisSection
    # Defaulted so existing constructors/round-trips stay valid; the
    # orchestrator always populates it explicitly at Step 6.5.
    theme_segments: ThemeSegmentsSection = Field(
        default_factory=lambda: ThemeSegmentsSection(status="skipped"),
    )
    attention_synthesis: AttentionSynthesisSection
    frequency_diagnostic: FrequencyDiagnosticSection
    pass_f_footprint: PassFFootprint
    envelope_health: FullBriefEnvelopeHealth
    pass_failures: list[PassFailure] = Field(default_factory=list)
    cost: CostEnvelope

    @staticmethod
    def new_brief_id(when: datetime | None = None) -> str:
        """Mint a fresh brief_id: `nwd-fullbrief-{iso-dashed}-{8-char-hex}`.

        Matches Brief.new_brief_id() and AttentionBrief.new_brief_id()
        pattern — same filesystem-safe timestamp (dashes for colons) +
        8-char disambiguation suffix.
        """
        dt = when if when is not None else datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        iso = dt.strftime("%Y-%m-%dT%H-%M-%SZ")
        suffix = uuid.uuid4().hex[:8]
        return f"nwd-fullbrief-{iso}-{suffix}"


__all__ = [
    "AttentionCrossing",
    "AttentionSynthesisSection",
    "ConvergenceInfo",
    "CostEnvelope",
    "CostPerAttentionBrief",
    "CostPerBrief",
    "ExecutiveSummary",
    "FrequencyDiagnosticCrossingRow",
    "FrequencyDiagnosticNearMissRow",
    "FrequencyDiagnosticSection",
    "FullBriefEnvelope",
    "FullBriefEnvelopeHealth",
    "PassFFootprint",
    "PassFailure",
    "StepHealth",
    "ThemeEventDigest",
    "ThemeSegment",
    "ThemeSegmentsSection",
    "ThemeSynthesisSection",
    "WindowSection",
]
