"""Drift prompt tests — system prompt shape, user prompt rendering, cache breakpoint."""

from __future__ import annotations

from datetime import date

import pytest

from news_watch_daemon.synthesize.drift_prompt import (
    DRIFT_SYSTEM_PROMPT,
    build_messages_payload,
    build_system_blocks,
    build_user_prompt,
)
from news_watch_daemon.theme_config import ThemeConfig


# ---------- helpers ----------


def _theme(
    theme_id: str = "t1",
    *,
    primary: list[str] | None = None,
    secondary: list[str] | None = None,
    exclusions: list[str] | None = None,
    brief: str = "Brief text for theme t1.",
) -> ThemeConfig:
    payload = {
        "theme_id": theme_id,
        "display_name": f"Display {theme_id}",
        "status": "active",
        "created_at": date(2026, 5, 1),
        "brief": brief,
        "keywords": {
            "primary": primary or ["thing"],
            "secondary": secondary or [],
            "exclusions": exclusions or [],
        },
        "tracked_entities": {"tickers": ["X"]},
        "alerts": {"velocity_baseline_headlines_per_day": 1.0},
    }
    return ThemeConfig.model_validate(payload)


# ---------- DRIFT_SYSTEM_PROMPT shape ----------


def test_drift_system_prompt_cacheable_size():
    """Sonnet/Haiku cacheable minimum is ~1024 tokens; aim for >4000 chars."""
    assert len(DRIFT_SYSTEM_PROMPT) > 3000, (
        f"DRIFT_SYSTEM_PROMPT too short for caching (len={len(DRIFT_SYSTEM_PROMPT)})"
    )


def test_drift_system_prompt_lists_proposal_fields():
    """Every DriftProposal field that Haiku is asked to emit should
    appear at least once in the prompt text."""
    expected = ["theme_id", "proposed_keyword", "suggested_tier",
                "evidence_count", "sample_headlines", "notes"]
    missing = [f for f in expected if f not in DRIFT_SYSTEM_PROMPT]
    assert not missing, f"DRIFT_SYSTEM_PROMPT missing fields: {missing}"


def test_drift_system_prompt_states_tier_enum():
    for tier in ["primary", "secondary", "exclusion"]:
        assert tier in DRIFT_SYSTEM_PROMPT


def test_drift_system_prompt_forbids_new_theme_proposals():
    """Drift watcher must NOT invent new themes — only propose
    keywords for existing themes."""
    # Look for the hard rule text — drift's contract is theme-bounded.
    assert "existing" in DRIFT_SYSTEM_PROMPT.lower()
    assert "new-theme" in DRIFT_SYSTEM_PROMPT or "new theme" in DRIFT_SYSTEM_PROMPT.lower()


def test_drift_system_prompt_warns_against_single_proper_nouns():
    """Proper-noun contamination rule."""
    assert "Trump" in DRIFT_SYSTEM_PROMPT or "proper noun" in DRIFT_SYSTEM_PROMPT.lower()


# ---------- build_system_blocks: single breakpoint ----------


def test_system_blocks_always_single_breakpoint():
    """Drift has no theses doc — always 1 cache block."""
    blocks = build_system_blocks()
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["cache_control"] == {"type": "ephemeral"}
    assert blocks[0]["text"] == DRIFT_SYSTEM_PROMPT


# ---------- build_user_prompt: shape ----------


def test_build_user_prompt_lists_themes():
    out = build_user_prompt(
        themes=[
            _theme("us_iran_escalation", primary=["Iran", "Tehran"]),
            _theme("fed_policy_path", primary=["Fed", "FOMC"]),
        ],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "[ACTIVE THEMES]" in out
    assert "## us_iran_escalation" in out
    assert "## fed_policy_path" in out
    assert "Iran" in out
    assert "FOMC" in out


def test_build_user_prompt_renders_keyword_lists():
    out = build_user_prompt(
        themes=[_theme(
            "t1",
            primary=["alpha"],
            secondary=["beta", "gamma"],
            exclusions=["delta"],
        )],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "Primary keywords: alpha" in out
    assert "beta" in out and "gamma" in out
    assert "Exclusions: delta" in out


def test_build_user_prompt_empty_keyword_buckets_marked_none():
    out = build_user_prompt(
        themes=[_theme("t1", primary=["a"], secondary=[], exclusions=[])],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "Secondary keywords: (none)" in out
    assert "Exclusions: (none)" in out


def test_build_user_prompt_truncates_long_brief():
    long_brief = "A" * 1000  # 1000 chars > 500 default
    out = build_user_prompt(
        themes=[_theme("t1", brief=long_brief)],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    # Should be truncated with ellipsis.
    assert "..." in out
    # And the full 1000-char block should not appear verbatim.
    assert ("A" * 1000) not in out


def test_build_user_prompt_lists_untagged_headlines():
    out = build_user_prompt(
        themes=[_theme()],
        untagged=[
            ("Reuters", "First untagged headline", 1764100000),
            ("AP", "Second untagged headline", 1764100050),
        ],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "[UNTAGGED HEADLINES (2 total)]" in out
    assert "First untagged headline" in out
    assert "Second untagged headline" in out
    assert "Reuters" in out
    assert "AP" in out


def test_build_user_prompt_empty_untagged_marked():
    out = build_user_prompt(
        themes=[_theme()],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "no untagged headlines" in out


def test_build_user_prompt_null_publisher_rendered():
    out = build_user_prompt(
        themes=[_theme()],
        untagged=[(None, "Mystery source", 1764100000)],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert "[?" in out


def test_build_user_prompt_constraints_block():
    out = build_user_prompt(
        themes=[_theme()],
        untagged=[],
        max_proposals_per_batch=5,
        min_evidence_count=4,
    )
    assert "max_proposals_per_batch: 5" in out
    assert "min_evidence_count: 4" in out


# ---------- build_messages_payload: end-to-end ----------


def test_build_messages_payload_structure():
    payload = build_messages_payload(
        themes=[_theme()],
        untagged=[],
        max_proposals_per_batch=8,
        min_evidence_count=3,
    )
    assert set(payload.keys()) == {"system", "messages"}
    # Single cache breakpoint always.
    assert len(payload["system"]) == 1
    assert payload["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert len(payload["messages"]) == 1
    assert payload["messages"][0]["role"] == "user"
