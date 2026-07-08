"""Prompt-assembly tests — system blocks shape + user prompt content."""

from __future__ import annotations

from news_watch_daemon.attention.cluster import ClusterHeadline
from news_watch_daemon.attention.prompt import (
    SYSTEM_PROMPT,
    build_messages_payload,
    build_system_blocks,
    build_user_prompt,
)


def _cluster_hl(hid: str, source: str, headline: str, ts: int = 1000) -> ClusterHeadline:
    return ClusterHeadline(
        headline_id=hid, source=source, headline=headline,
        url=f"https://example.com/{hid}", publisher="TestWire", published_at_unix=ts,
    )


# ---------- system blocks ----------


def test_system_blocks_one_cache_breakpoint():
    """Pass E is theme-blind — single cache breakpoint on the system prompt."""
    blocks = build_system_blocks()
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["cache_control"] == {"type": "ephemeral"}
    # And it's exactly the SYSTEM_PROMPT constant — byte-stable for caching.
    assert blocks[0]["text"] == SYSTEM_PROMPT


def test_system_prompt_contains_role_section():
    """Sanity-check the system prompt has the required ROLE / OUTPUT / DISCIPLINE
    sections so the schema can't drift without breaking this."""
    assert "[ROLE]" in SYSTEM_PROMPT
    assert "[INPUT FORMAT]" in SYSTEM_PROMPT
    assert "[OUTPUT FORMAT]" in SYSTEM_PROMPT
    assert "[NARRATIVE GUIDANCE]" in SYSTEM_PROMPT
    assert "[EPISTEMIC DISCIPLINE]" in SYSTEM_PROMPT
    assert "[HARD RULES]" in SYSTEM_PROMPT


def test_system_prompt_lists_all_attention_shapes():
    """The 6-value closed Literal must be in the prompt so the model knows
    the rubric. If a label is added/removed, the prompt update is part of
    the schema change."""
    for shape in (
        "single_event_dominant",
        "multi_source_convergence",
        "slow_burn",
        "narrow_source_spike",
        "cross_topic_recurrence",
        "unclear",
    ):
        assert shape in SYSTEM_PROMPT


def test_system_prompt_forbids_materiality_and_thesis():
    """Hard rules — materiality and thesis links must be explicitly forbidden."""
    assert "DO NOT include a materiality score" in SYSTEM_PROMPT
    assert "DO NOT link to theses" in SYSTEM_PROMPT
    assert "DO NOT recommend action" in SYSTEM_PROMPT


def test_system_prompt_info_ops_paragraph_forces_single_event_dominant():
    """Mando-added rule: coordinated verbatim echoes force
    attention_shape = single_event_dominant, not multi_source_convergence.
    (Wording compressed 2026-07-08 in the editorial-reduction pass; the
    single_event_dominant instruction must survive.)"""
    assert "verbatim echoes" in SYSTEM_PROMPT
    assert "single_event_dominant" in SYSTEM_PROMPT
    assert "not multi_source_convergence" in SYSTEM_PROMPT


def test_system_prompt_cig_naming_and_no_ideology_labeling():
    """Mando editorial rules (2026-07-08): the CIG source is named "CIG"
    in narrative, and no source is labeled by political ideology / white
    nationalism."""
    assert 'refer to the source `telegram:CIG_telegram` as' in SYSTEM_PROMPT
    assert '"white nationalist"' in SYSTEM_PROMPT
    assert "political ideology" in SYSTEM_PROMPT


# ---------- user prompt ----------


def test_user_prompt_includes_trigger_section():
    text = build_user_prompt(
        triggering_term="hormuz",
        term_frequency_window=14,
        term_frequency_prior=1,
        window_since_iso="2026-05-26T00:00:00Z",
        window_until_iso="2026-05-27T00:00:00Z",
        cluster=[],
    )
    assert "[TRIGGERING TERM]" in text
    assert "Term: hormuz" in text
    assert "Window count (24h): 14" in text
    assert "Prior count (prior 24h): 1" in text
    assert "2026-05-26T00:00:00Z" in text
    assert "2026-05-27T00:00:00Z" in text


def test_user_prompt_includes_each_cluster_headline():
    cluster = [
        _cluster_hl("h1", "telegram:CIG_telegram", "Iran tests new missile"),
        _cluster_hl("h2", "finnhub:general", "CENTCOM denies WSJ report"),
    ]
    text = build_user_prompt(
        triggering_term="iran",
        term_frequency_window=10, term_frequency_prior=2,
        window_since_iso="a", window_until_iso="b",
        cluster=cluster,
    )
    assert "[CLUSTER]" in text
    assert "telegram:CIG_telegram" in text
    assert "finnhub:general" in text
    assert "Iran tests new missile" in text
    assert "CENTCOM denies WSJ report" in text
    assert "Headline 1" in text
    assert "Headline 2" in text


def test_user_prompt_handles_empty_cluster_marker():
    text = build_user_prompt(
        triggering_term="x",
        term_frequency_window=10, term_frequency_prior=0,
        window_since_iso="a", window_until_iso="b",
        cluster=[],
    )
    assert "no headlines in cluster" in text   # defensive marker


# ---------- build_messages_payload ----------


def test_build_messages_payload_shape():
    payload = build_messages_payload(
        triggering_term="hormuz",
        term_frequency_window=14, term_frequency_prior=1,
        window_since_iso="a", window_until_iso="b",
        cluster=[_cluster_hl("h1", "x", "Iran tests Hormuz patrol")],
    )
    assert set(payload.keys()) == {"system", "messages"}
    assert len(payload["system"]) == 1
    assert payload["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert len(payload["messages"]) == 1
    assert payload["messages"][0]["role"] == "user"
    assert "hormuz" in payload["messages"][0]["content"]
    assert "Iran tests Hormuz patrol" in payload["messages"][0]["content"]
