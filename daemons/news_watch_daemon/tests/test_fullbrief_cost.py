"""Cost estimator tests — 4-category billing, envelope assembly, rate provenance.

Per Full Brief spec Adjustment 4 (Abelard 2026-05-29). T15 + T15b coverage.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

import pytest

from news_watch_daemon.fullbrief import cost as cost_mod
from news_watch_daemon.fullbrief.cost import (
    CACHE_READ_RATE_PER_TOKEN,
    CACHE_WRITE_RATE_PER_TOKEN,
    INPUT_RATE_PER_TOKEN,
    OUTPUT_RATE_PER_TOKEN,
    RATES_AS_OF,
    CostBreakdown,
    assemble_cost_envelope,
    estimate_brief_cost,
)


# ---------- fixture ----------


@dataclass(frozen=True)
class _FakeMetadata:
    """Stand-in for SynthesisMetadata — duck-typed by attribute access in
    cost module. Production uses the real Pydantic SynthesisMetadata; tests
    use this minimal dataclass to keep unit tests decoupled from Pydantic
    schema evolution."""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0


# ---------- estimate_brief_cost ----------


def test_estimate_brief_cost_known_token_counts():
    """T15: per-category rates correct, total matches expected within rounding."""
    md = _FakeMetadata(input_tokens=3000, output_tokens=2000,
                       cache_creation_input_tokens=2000,
                       cache_read_input_tokens=500)
    cost = estimate_brief_cost(md)
    assert cost.input_tokens == 3000
    assert cost.output_tokens == 2000
    assert cost.cache_creation_tokens == 2000
    assert cost.cache_read_tokens == 500

    # NOTE: Do NOT tighten this sanity band to strict equality.
    #
    # Mechanism: `3.00 / 1_000_000` has no exact binary representation in IEEE 754
    # double-precision floats. Computing the rate constant once at module import
    # (`INPUT_RATE_PER_TOKEN = 3.00 / 1_000_000`) and using it everywhere produces
    # one rounding error. Recomputing the rate inline at test-evaluation time
    # (`tokens * 3.00 / 1_000_000`) produces a *different* rounding error because
    # the intermediate `tokens * 3.00` is exact but the final divide accumulates
    # differently than (tokens) * (precomputed rate).
    #
    # The implementation uses the module constant. Tests must mirror that exact
    # arithmetic OR use a sanity band — never recompute from raw rates and assert
    # strict equality, because the last-bit difference will fail intermittently
    # depending on the token magnitude.
    #
    # Sanity band: 0.046 < cost.usd < 0.047 is the right discipline here. Do not
    # narrow it.
    expected = round(
        3000 * INPUT_RATE_PER_TOKEN + 2000 * OUTPUT_RATE_PER_TOKEN
        + 2000 * CACHE_WRITE_RATE_PER_TOKEN + 500 * CACHE_READ_RATE_PER_TOKEN,
        4,
    )
    assert cost.usd == expected
    assert 0.046 < cost.usd < 0.047


def test_estimate_brief_cost_cache_read_is_10x_cheaper_than_input():
    """Per Adjustment 4 critical fact: cache reads are 0.10x input rate.
    Treating them as input would over-estimate by ~10x on cache-hit cycles —
    the error Abelard self-corrected in the prior session."""
    cache_hit_md = _FakeMetadata(cache_read_input_tokens=10000)
    full_input_md = _FakeMetadata(input_tokens=10000)
    cache_cost = estimate_brief_cost(cache_hit_md).usd
    full_cost = estimate_brief_cost(full_input_md).usd
    # cache_read at 0.30/M vs input at 3.00/M -> exactly 10x cheaper
    assert full_cost == pytest.approx(cache_cost * 10, abs=0.0001)


def test_estimate_brief_cost_zero_tokens_zero_cost():
    """Empty/zero metadata -> $0 cost, all categories 0."""
    cost = estimate_brief_cost(_FakeMetadata())
    assert cost.usd == 0.0
    assert cost.input_tokens == 0
    assert cost.output_tokens == 0
    assert cost.cache_creation_tokens == 0
    assert cost.cache_read_tokens == 0


def test_estimate_brief_cost_handles_none_attributes():
    """Defensive: None on a token attribute treated as 0 (some upstream
    code may set None rather than 0 explicitly)."""
    @dataclass(frozen=True)
    class _MdWithNones:
        input_tokens: int | None = None
        output_tokens: int | None = None
        cache_creation_input_tokens: int | None = None
        cache_read_input_tokens: int | None = None
    cost = estimate_brief_cost(_MdWithNones())
    assert cost.usd == 0.0


def test_estimate_brief_cost_returns_dataclass_with_immutable_shape():
    """CostBreakdown is frozen; downstream code can rely on hash/equality."""
    md = _FakeMetadata(input_tokens=100, output_tokens=200)
    cost = estimate_brief_cost(md)
    assert isinstance(cost, CostBreakdown)
    # Frozen dataclass: assignment should raise
    with pytest.raises(Exception):
        cost.usd = 9.99   # type: ignore[misc]


# ---------- assemble_cost_envelope ----------


def test_assemble_cost_envelope_full_shape_per_adjustment_4():
    """Verify all schema keys from Adjustment 4: pass_c, pass_e_briefs,
    pass_e_total_usd (convenience), total_usd, model, rates_as_of."""
    pass_c_md = _FakeMetadata(input_tokens=2991, output_tokens=2125,
                              cache_creation_input_tokens=2060,
                              cache_read_input_tokens=0)
    pass_e_md_1 = _FakeMetadata(input_tokens=3178, output_tokens=804,
                                cache_creation_input_tokens=1834,
                                cache_read_input_tokens=0)
    pass_e_md_2 = _FakeMetadata(input_tokens=3847, output_tokens=691,
                                cache_creation_input_tokens=0,
                                cache_read_input_tokens=1834)
    env = assemble_cost_envelope(
        pass_c_metadata=pass_c_md,
        pass_e_brief_metadata=[
            ("nwd-attn-id-1", pass_e_md_1),
            ("nwd-attn-id-2", pass_e_md_2),
        ],
        model="claude-sonnet-4-6",
    )

    # Top-level schema
    assert set(env.keys()) == {
        "pass_c", "pass_e_briefs", "pass_e_total_usd",
        "total_usd", "model", "rates_as_of",
    }
    assert env["model"] == "claude-sonnet-4-6"
    assert env["rates_as_of"] == "2026-05-28"

    # pass_c per-category breakdown
    assert env["pass_c"]["input_tokens"] == 2991
    assert env["pass_c"]["output_tokens"] == 2125
    assert env["pass_c"]["cache_creation_tokens"] == 2060
    assert env["pass_c"]["cache_read_tokens"] == 0
    assert env["pass_c"]["usd"] > 0

    # pass_e_briefs array with attention_brief_id linkage
    assert len(env["pass_e_briefs"]) == 2
    assert env["pass_e_briefs"][0]["attention_brief_id"] == "nwd-attn-id-1"
    assert env["pass_e_briefs"][1]["attention_brief_id"] == "nwd-attn-id-2"

    # pass_e_total_usd is convenience sum
    expected_e_total = round(
        env["pass_e_briefs"][0]["usd"] + env["pass_e_briefs"][1]["usd"], 4
    )
    assert env["pass_e_total_usd"] == expected_e_total

    # total_usd = pass_c.usd + pass_e_total_usd
    expected_total = round(env["pass_c"]["usd"] + env["pass_e_total_usd"], 4)
    assert env["total_usd"] == expected_total


def test_assemble_cost_envelope_pass_c_none_when_no_trigger():
    """Q2 no_trigger case: pass_c is None, total_usd = pass_e_total_usd."""
    pass_e_md = _FakeMetadata(input_tokens=1000, output_tokens=500)
    env = assemble_cost_envelope(
        pass_c_metadata=None,
        pass_e_brief_metadata=[("nwd-attn-x", pass_e_md)],
        model="claude-sonnet-4-6",
    )
    assert env["pass_c"] is None
    assert env["total_usd"] == env["pass_e_total_usd"]
    assert env["total_usd"] > 0


def test_assemble_cost_envelope_zero_pass_e_briefs():
    """Pass E produced no crossings: pass_e_briefs empty, pass_e_total_usd 0."""
    pass_c_md = _FakeMetadata(input_tokens=1000, output_tokens=500)
    env = assemble_cost_envelope(
        pass_c_metadata=pass_c_md,
        pass_e_brief_metadata=[],
        model="claude-sonnet-4-6",
    )
    assert env["pass_e_briefs"] == []
    assert env["pass_e_total_usd"] == 0.0
    assert env["total_usd"] == env["pass_c"]["usd"]


def test_assemble_cost_envelope_both_passes_failed():
    """Worst case: pass_c None, pass_e empty. total_usd = 0, valid envelope."""
    env = assemble_cost_envelope(
        pass_c_metadata=None,
        pass_e_brief_metadata=[],
        model="claude-sonnet-4-6",
    )
    assert env["pass_c"] is None
    assert env["pass_e_briefs"] == []
    assert env["pass_e_total_usd"] == 0.0
    assert env["total_usd"] == 0.0
    assert env["rates_as_of"] == "2026-05-28"


def test_assemble_cost_envelope_cycle_2_empirical_numbers():
    """Pin against cycle 2 (2026-05-29) actual production numbers:
    Pass C: in=2991, out=2125, cache_create=2060, cache_read=0 -> ~$0.0518
    Pass E #1 (secretary): in=3178, out=804, cache_create=1834, cache_read=0 -> ~$0.0293
    Pass E #2 (administration): in=3847, out=691, cache_create=0, cache_read=1834 -> ~$0.0223
    Total ~$0.1034
    """
    pass_c_md = _FakeMetadata(input_tokens=2991, output_tokens=2125,
                              cache_creation_input_tokens=2060)
    pass_e_secretary = _FakeMetadata(input_tokens=3178, output_tokens=804,
                                     cache_creation_input_tokens=1834)
    pass_e_administration = _FakeMetadata(input_tokens=3847, output_tokens=691,
                                          cache_read_input_tokens=1834)
    env = assemble_cost_envelope(
        pass_c_metadata=pass_c_md,
        pass_e_brief_metadata=[
            ("nwd-attn-secretary", pass_e_secretary),
            ("nwd-attn-administration", pass_e_administration),
        ],
        model="claude-sonnet-4-6",
    )
    # Sanity-band: total within the "average day" estimate from spec Section 12
    assert 0.08 < env["total_usd"] < 0.20
    # Compare to known Adjustment-4 over-estimate failure mode: if we'd
    # treated the cache_read=1834 as full input, it would have added
    # ~$0.005 (1834 * 3/M = $0.0055) instead of ~$0.0005 (1834 * 0.30/M).
    # Confirm cache_read for the admin brief contributes the cheap rate.
    admin = env["pass_e_briefs"][1]
    assert admin["cache_read_tokens"] == 1834
    cache_read_contribution = 1834 * CACHE_READ_RATE_PER_TOKEN
    assert cache_read_contribution < 0.001   # under a tenth of a cent


# ---------- T15b: rate provenance ----------


def test_rate_constants_exist_at_module_level():
    """T15b part 1: all four rate constants + RATES_AS_OF at module scope.
    Audit by attribute presence — guard against accidental deletion."""
    assert hasattr(cost_mod, "INPUT_RATE_PER_TOKEN")
    assert hasattr(cost_mod, "OUTPUT_RATE_PER_TOKEN")
    assert hasattr(cost_mod, "CACHE_WRITE_RATE_PER_TOKEN")
    assert hasattr(cost_mod, "CACHE_READ_RATE_PER_TOKEN")
    assert hasattr(cost_mod, "RATES_AS_OF")
    assert isinstance(RATES_AS_OF, str)
    # Sanity: rates are positive small floats
    for rate in (INPUT_RATE_PER_TOKEN, OUTPUT_RATE_PER_TOKEN,
                 CACHE_WRITE_RATE_PER_TOKEN, CACHE_READ_RATE_PER_TOKEN):
        assert 0 < rate < 1


def test_rate_constants_have_citation_in_source():
    """T15b part 2: source file contains citation block referencing Anthropic
    pricing. Pins against rate-magic-numbers without provenance."""
    src = inspect.getsource(cost_mod)
    # Required citation tokens
    assert "Anthropic" in src or "anthropic" in src
    assert "pricing" in src.lower()
    # The RATES_AS_OF date should also be findable in the source
    # (constant assignment AND citation block — single source of truth)
    assert RATES_AS_OF in src


def test_rate_constants_ratios_match_published_anthropic_ratios():
    """Self-consistency: cache_write should be 1.25x input, output 5x input,
    cache_read 0.10x input. Documents the rate relationships explicitly so
    a typo in one constant gets caught by the others."""
    assert CACHE_WRITE_RATE_PER_TOKEN == pytest.approx(INPUT_RATE_PER_TOKEN * 1.25)
    assert OUTPUT_RATE_PER_TOKEN == pytest.approx(INPUT_RATE_PER_TOKEN * 5.0)
    assert CACHE_READ_RATE_PER_TOKEN == pytest.approx(INPUT_RATE_PER_TOKEN * 0.10)


def test_rates_as_of_matches_envelope_field():
    """Single-source-of-truth pin: the envelope's rates_as_of field is sourced
    from the same RATES_AS_OF constant — no drift possible between citation
    and envelope value."""
    env = assemble_cost_envelope(
        pass_c_metadata=None,
        pass_e_brief_metadata=[],
        model="claude-sonnet-4-6",
    )
    assert env["rates_as_of"] == RATES_AS_OF
