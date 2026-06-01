"""Cost estimator for Full Brief composition.

Per Full Brief spec Adjustment 4 (Abelard 2026-05-29): reads ALL FOUR
token categories from each underlying brief's SynthesisMetadata and
applies category-specific rates. Cache reads at 0.10x input rate are
common (per-brief cache discipline) and ignoring them over-estimates
cost by an order of magnitude on cache-hit cycles. This was the error
Abelard self-corrected from the prior session ($0.50-$0.80 estimate
vs $0.16 actual).

Rate constants are module-level with citation comments — historical
Full Briefs surface `rates_as_of` so downstream consumers reading
archived briefs know which rate table generated the number. When
rates change, update BOTH the constants below AND `RATES_AS_OF` in
the same commit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# Sonnet 4.6 token rates per Anthropic public pricing as of 2026-05-28.
# Source: https://www.anthropic.com/pricing
#
# Four-category billing:
#   - Input tokens:           $3.00 per million
#   - Output tokens:          $15.00 per million (5x input)
#   - Cache creation tokens:  $3.75 per million (1.25x input)
#   - Cache read tokens:      $0.30 per million (0.10x input)
#
# Cache discipline is what makes Pass E briefs cheap: the first brief in
# a sweep creates the cache (1834 tokens at 1.25x input), each subsequent
# brief in the same sweep hits cache read (1834 tokens at 0.10x input)
# instead of paying full input rate. Treating cache reads as full input
# over-estimates by ~10x.
#
# `RATES_AS_OF` is the single source of truth for the rates' effective
# date. Historical Full Brief envelopes surface this so consumers reading
# archived briefs know which rate table applied.
# ---------------------------------------------------------------------------

INPUT_RATE_PER_TOKEN = 3.00 / 1_000_000
OUTPUT_RATE_PER_TOKEN = 15.00 / 1_000_000
CACHE_WRITE_RATE_PER_TOKEN = 3.75 / 1_000_000
CACHE_READ_RATE_PER_TOKEN = 0.30 / 1_000_000
RATES_AS_OF = "2026-05-28"


@dataclass(frozen=True)
class CostBreakdown:
    """Per-brief cost breakdown across the four Anthropic token categories.

    All token fields are int (raw counts copied from synthesis_metadata).
    `usd` is the rounded-to-4-decimal-places total cost in USD for the
    single brief this breakdown describes.
    """

    input_tokens: int
    output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int
    usd: float


def estimate_brief_cost(metadata: Any) -> CostBreakdown:
    """Compute per-category cost from a brief's SynthesisMetadata.

    `metadata` may be a `SynthesisMetadata` Pydantic instance OR any
    object with the four token-count attributes (duck-typed for test
    friendliness — Stage 1 unit tests use simple dataclasses; Stage 2
    orchestrator passes real SynthesisMetadata).

    Returns `CostBreakdown` with token counts copied from metadata and
    `usd` computed per Adjustment 4 four-category formula. Defensive
    against missing or None fields — treats as zero.
    """
    input_tokens = int(getattr(metadata, "input_tokens", 0) or 0)
    output_tokens = int(getattr(metadata, "output_tokens", 0) or 0)
    cache_creation = int(getattr(metadata, "cache_creation_input_tokens", 0) or 0)
    cache_read = int(getattr(metadata, "cache_read_input_tokens", 0) or 0)

    usd = round(
        input_tokens * INPUT_RATE_PER_TOKEN
        + output_tokens * OUTPUT_RATE_PER_TOKEN
        + cache_creation * CACHE_WRITE_RATE_PER_TOKEN
        + cache_read * CACHE_READ_RATE_PER_TOKEN,
        4,
    )
    return CostBreakdown(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_tokens=cache_creation,
        cache_read_tokens=cache_read,
        usd=usd,
    )


def assemble_cost_envelope(
    *,
    pass_c_metadata: Any | None,
    pass_e_brief_metadata: list[tuple[str, Any]],
    model: str,
) -> dict[str, Any]:
    """Compose the Full Brief envelope's `cost` section per Adjustment 4.

    Args:
      pass_c_metadata: `SynthesisMetadata` for the Pass C brief; pass
                       `None` if Pass C failed or didn't fire (Q2
                       no_trigger case).
      pass_e_brief_metadata: list of `(brief_id, metadata)` tuples for
                             each Pass E attention brief. Empty list when
                             zero crossings.
      model: model identifier (typically "claude-sonnet-4-6").

    Returns a dict matching the Adjustment 4 schema:

      {
        "pass_c": { input_tokens, output_tokens, cache_creation_tokens,
                    cache_read_tokens, usd }  | null,
        "pass_e_briefs": [
          { attention_brief_id, input_tokens, output_tokens,
            cache_creation_tokens, cache_read_tokens, usd },
          ...
        ],
        "pass_e_total_usd": <computed sum across pass_e_briefs[].usd>,
        "total_usd": <pass_c.usd (or 0) + pass_e_total_usd>,
        "model": <model>,
        "rates_as_of": <RATES_AS_OF>,
      }

    `pass_e_total_usd` is a convenience field so downstream consumers
    don't need to sum the array themselves — supplied in addition to
    the per-brief breakdown, not as a replacement for it.
    """
    pass_c_section: dict[str, Any] | None = None
    pass_c_usd = 0.0
    if pass_c_metadata is not None:
        bc = estimate_brief_cost(pass_c_metadata)
        pass_c_section = {
            "input_tokens": bc.input_tokens,
            "output_tokens": bc.output_tokens,
            "cache_creation_tokens": bc.cache_creation_tokens,
            "cache_read_tokens": bc.cache_read_tokens,
            "usd": bc.usd,
        }
        pass_c_usd = bc.usd

    pass_e_section: list[dict[str, Any]] = []
    pass_e_total = 0.0
    for brief_id, metadata in pass_e_brief_metadata:
        bc = estimate_brief_cost(metadata)
        pass_e_section.append({
            "attention_brief_id": brief_id,
            "input_tokens": bc.input_tokens,
            "output_tokens": bc.output_tokens,
            "cache_creation_tokens": bc.cache_creation_tokens,
            "cache_read_tokens": bc.cache_read_tokens,
            "usd": bc.usd,
        })
        pass_e_total += bc.usd

    return {
        "pass_c": pass_c_section,
        "pass_e_briefs": pass_e_section,
        "pass_e_total_usd": round(pass_e_total, 4),
        "total_usd": round(pass_c_usd + pass_e_total, 4),
        "model": model,
        "rates_as_of": RATES_AS_OF,
    }


__all__ = [
    "CACHE_READ_RATE_PER_TOKEN",
    "CACHE_WRITE_RATE_PER_TOKEN",
    "CostBreakdown",
    "INPUT_RATE_PER_TOKEN",
    "OUTPUT_RATE_PER_TOKEN",
    "RATES_AS_OF",
    "assemble_cost_envelope",
    "estimate_brief_cost",
]
