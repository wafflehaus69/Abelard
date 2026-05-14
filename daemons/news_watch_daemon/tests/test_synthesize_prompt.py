"""Prompt construction tests — cache blocks, user prompt shape, schema alignment."""

from __future__ import annotations

import re

import pytest

from news_watch_daemon.synthesize.brief import Event, Trigger, TriggerWindow
from news_watch_daemon.synthesize.cluster import Cluster, ClusterInput
from news_watch_daemon.synthesize.prompt import (
    SYSTEM_PROMPT,
    build_messages_payload,
    build_system_blocks,
    build_user_prompt,
)


# ---------- helpers ----------


def _trigger(kind: str = "event", reason: str = "delta exceeded", since: str = "A", until: str = "B") -> Trigger:
    return Trigger(type=kind, reason=reason, window=TriggerWindow(since=since, until=until))


def _cluster(headlines: list[tuple[str, str | None, str | None, int]]) -> Cluster:
    """Build a Cluster from (headline, publisher, url, published_at_unix) tuples."""
    members = tuple(
        ClusterInput(
            headline_id=f"h-{i}",
            headline=h,
            url=u,
            publisher=p,
            published_at_unix=ts,
        )
        for i, (h, p, u, ts) in enumerate(headlines)
    )
    return Cluster(headline_ids=tuple(m.headline_id for m in members), members=members)


# ---------- SYSTEM_PROMPT shape ----------


def test_system_prompt_is_non_trivial_size_for_caching():
    """Sonnet 4.6's minimum cacheable prefix is ~1024 tokens. Roughly 4
    chars per token → SYSTEM_PROMPT should be at least 4000 chars."""
    assert len(SYSTEM_PROMPT) > 4000, (
        f"SYSTEM_PROMPT too short to be cacheable (len={len(SYSTEM_PROMPT)})"
    )


def test_system_prompt_lists_brief_event_fields():
    """The schema example in SYSTEM_PROMPT must reference every field
    on brief.Event (drift between prompt and Pydantic model is the
    most likely Step-9 regression). Tests that every Event field name
    appears at least once in the prompt text."""
    event_fields = set(Event.model_fields.keys())
    missing = [f for f in event_fields if f not in SYSTEM_PROMPT]
    assert not missing, (
        f"SYSTEM_PROMPT schema example is missing Event fields: {missing}"
    )


def test_system_prompt_states_materiality_floor():
    """Hard rule: don't include sub-0.30 events. Pinned because the
    materiality gate's default threshold (0.55) sits above this; if
    Sonnet emitted 0.2 events we'd waste tokens archiving them."""
    assert "0.30" in SYSTEM_PROMPT


def test_system_prompt_forbids_invented_events():
    """Pinned: Sonnet must not introduce events from training data."""
    assert "training data" in SYSTEM_PROMPT or "training-data" in SYSTEM_PROMPT


# ---------- [EPISTEMIC DISCIPLINE] section (2026-05-14 architect directive) ----


def test_system_prompt_has_epistemic_discipline_section():
    """First-smoke output (4 confirm + 4 ambiguous + 0 break across 8
    events) revealed that the prompt was confirmation-bias-friendly:
    Sonnet read every event through the cascade frame because nothing
    in the prompt told it to resist Mando's own framing.

    Mando's architect directive: encode counter-reading discipline +
    direction-default-toward-break + compounding-false-confirms
    asymmetry. Pin the section so future prompt edits can't silently
    drop it."""
    assert "[EPISTEMIC DISCIPLINE]" in SYSTEM_PROMPT


def test_system_prompt_warns_against_confirming_mando_framing():
    """The doctrine: 'you are NOT here to confirm Mando's framing of
    the world. You are here to test it.' Pin both halves."""
    assert "NOT here to confirm" in SYSTEM_PROMPT
    assert "test it" in SYSTEM_PROMPT


def test_system_prompt_requires_counter_reading_for_confirm():
    """Every confirm-direction tag must have considered the strongest
    counter-reading."""
    assert "counter-reading" in SYSTEM_PROMPT.lower()
    assert "strongest counter-reading" in SYSTEM_PROMPT.lower()


def test_system_prompt_states_fog_of_war_both_directions():
    """Symmetry: actors talking the war up have interests; actors
    talking it down have interests too."""
    assert "Fog-of-War" in SYSTEM_PROMPT
    assert "BOTH directions" in SYSTEM_PROMPT


def test_system_prompt_states_direction_default_toward_break():
    """When a single event has both readings in equal weight, lean
    toward break/ambiguous, not confirm."""
    # The doctrine itself.
    assert "lean toward" in SYSTEM_PROMPT.lower()
    # The compounding-vs-self-correcting asymmetry.
    assert "compound" in SYSTEM_PROMPT.lower()
    assert "self-correct" in SYSTEM_PROMPT.lower() or "self correct" in SYSTEM_PROMPT.lower()


def test_system_prompt_states_six_of_eight_confirm_audit_rule():
    """The audit floor: if 6+ of 8 events direct 'confirm', reread."""
    # Look for either the exact "6 or more" phrasing or the
    # numeric pattern.
    assert "6 or more" in SYSTEM_PROMPT or "6/8" in SYSTEM_PROMPT or "6 of 8" in SYSTEM_PROMPT


# ---------- build_system_blocks: caching shape ----------


def test_system_blocks_without_theses_single_breakpoint():
    blocks = build_system_blocks(theses_doc_text=None)
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["cache_control"] == {"type": "ephemeral"}
    assert blocks[0]["text"] == SYSTEM_PROMPT


def test_system_blocks_with_theses_two_breakpoints():
    blocks = build_system_blocks(theses_doc_text="THESIS_ID iran-cascade: ...")
    assert len(blocks) == 2
    for block in blocks:
        assert block["type"] == "text"
        assert block["cache_control"] == {"type": "ephemeral"}
    assert blocks[0]["text"] == SYSTEM_PROMPT
    assert "iran-cascade" in blocks[1]["text"]
    assert "ACTIVE THESES" in blocks[1]["text"]


def test_system_blocks_theses_block_instructs_thesis_id_usage():
    blocks = build_system_blocks(theses_doc_text="thesis-x: blah")
    assert "thesis_links" in blocks[1]["text"]


# ---------- build_user_prompt: shape ----------


def test_build_user_prompt_includes_trigger():
    out = build_user_prompt(
        trigger=_trigger(reason="cross-theme fire"),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "brief text"},
        clusters=[],
        max_events_per_brief=8,
    )
    assert "[TRIGGER]" in out
    assert "cross-theme fire" in out
    assert "Type: event" in out


def test_build_user_prompt_lists_themes_in_scope():
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["us_iran_escalation", "fed_policy_path"],
        theme_briefs={"us_iran_escalation": "ix brief", "fed_policy_path": "fp brief"},
        clusters=[],
        max_events_per_brief=8,
    )
    assert "[THEMES_IN_SCOPE]" in out
    assert "- us_iran_escalation" in out
    assert "- fed_policy_path" in out
    assert "ix brief" in out
    assert "fp brief" in out
    assert "## us_iran_escalation" in out
    assert "## fed_policy_path" in out


def test_build_user_prompt_missing_brief_text_marked():
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["unknown_theme"],
        theme_briefs={},
        clusters=[],
        max_events_per_brief=8,
    )
    assert "(brief unavailable)" in out


def test_build_user_prompt_no_clusters_marked():
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[],
        max_events_per_brief=8,
    )
    assert "(no clusters this cycle)" in out


def test_build_user_prompt_renders_cluster_with_corroboration():
    cluster = _cluster([
        ("Iran rejects ceasefire", "Reuters", "https://reuters.com/x", 1764100000),
        ("Tehran says no to U.S. terms", "CNBC", None, 1764100050),
        ("Iranian leadership turns down peace bid", "AP", "https://ap.org/y", 1764100100),
    ])
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["us_iran_escalation"],
        theme_briefs={"us_iran_escalation": "b"},
        clusters=[cluster],
        max_events_per_brief=8,
    )
    assert "Cluster 1 (3 headlines)" in out
    assert "LEADER: Reuters" in out
    assert "Iran rejects ceasefire" in out
    assert "CORROBORATION:" in out
    assert "CNBC" in out
    assert "AP" in out


def test_build_user_prompt_singleton_cluster_no_corroboration():
    cluster = _cluster([("Solo headline", "X", None, 1764100000)])
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[cluster],
        max_events_per_brief=8,
    )
    assert "Cluster 1 (1 headline)" in out
    assert "CORROBORATION:" not in out


def test_build_user_prompt_states_max_events_constraint():
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[],
        max_events_per_brief=5,
    )
    assert "max_events_per_brief: 5" in out


def test_build_user_prompt_null_publisher_url_rendered():
    cluster = _cluster([("Headline", None, None, 1764100000)])
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[cluster],
        max_events_per_brief=8,
    )
    # Null pub/url render as "?" placeholders; Sonnet sees these and
    # emits null in the output per the prompt's hard rules.
    assert "LEADER: ? |" in out
    assert "| ?" in out


def test_build_user_prompt_iso_timestamp_format():
    cluster = _cluster([("X", "Y", "z", 1764100000)])
    out = build_user_prompt(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "b"},
        clusters=[cluster],
        max_events_per_brief=8,
    )
    # ISO-8601 with Z suffix, second precision.
    assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", out)


# ---------- build_messages_payload: end-to-end ----------


def test_build_messages_payload_structure():
    payload = build_messages_payload(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "brief"},
        clusters=[],
        max_events_per_brief=8,
        theses_doc_text=None,
    )
    assert set(payload.keys()) == {"system", "messages"}
    assert isinstance(payload["system"], list)
    assert isinstance(payload["messages"], list)
    assert len(payload["messages"]) == 1
    assert payload["messages"][0]["role"] == "user"
    assert isinstance(payload["messages"][0]["content"], str)


def test_build_messages_payload_with_theses_has_two_system_blocks():
    payload = build_messages_payload(
        trigger=_trigger(),
        themes_in_scope=["t1"],
        theme_briefs={"t1": "brief"},
        clusters=[],
        max_events_per_brief=8,
        theses_doc_text="active thesis xyz",
    )
    assert len(payload["system"]) == 2
    assert "active thesis xyz" in payload["system"][1]["text"]
