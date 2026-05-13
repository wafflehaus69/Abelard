"""Trigger gate tests — pure function, hermetic synthetic cases."""

from __future__ import annotations

import pytest

from news_watch_daemon.synthesize.config import TriggerGateConfig
from news_watch_daemon.synthesize.trigger import (
    TriggerDecision,
    TriggerHeadline,
    evaluate_gate,
)


def _h(hid: str, text: str, themes: tuple[str, ...] = ()) -> TriggerHeadline:
    return TriggerHeadline(
        headline_id=hid,
        headline=text,
        themes=themes,
        fetched_at_unix=1000,
    )


@pytest.fixture
def basic_config() -> TriggerGateConfig:
    return TriggerGateConfig(
        delta_threshold_default=3,
        delta_threshold_overrides={"us_iran_escalation": 5},
        high_signal_phrases=["new sanctions", "ceasefire", "emergency rate cut"],
        cross_theme_always_triggers=True,
    )


# ---------- empty input ----------


def test_empty_headlines_no_fire(basic_config):
    d = evaluate_gate(
        [], config=basic_config,
        window_since_unix=100, window_until_unix=200,
    )
    assert d.fire is False
    assert d.reason == "no_new_headlines"
    assert d.window_since_unix == 100
    assert d.window_until_unix == 200


# ---------- cross-theme signal ----------


def test_cross_theme_fires(basic_config):
    items = [_h("h1", "Iran inflation hits new high", themes=("us_iran_escalation", "fed_policy_path"))]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert d.reason == "cross_theme:fed_policy_path+us_iran_escalation"
    assert d.matched_headline_ids == ("h1",)
    assert d.themes_in_scope == ("fed_policy_path", "us_iran_escalation")


def test_cross_theme_disabled_in_config():
    cfg = TriggerGateConfig(
        cross_theme_always_triggers=False,
        high_signal_phrases=[],
        delta_threshold_default=10,
    )
    items = [_h("h1", "Iran inflation rises", themes=("us_iran_escalation", "fed_policy_path"))]
    d = evaluate_gate(items, config=cfg, window_since_unix=0, window_until_unix=1)
    assert d.fire is False
    assert d.reason == "below_thresholds"


def test_single_theme_headline_does_not_trigger_cross_theme(basic_config):
    """`themes` length 1 — even if other signals also could fire."""
    items = [_h("h1", "Just one theme tag here", themes=("us_iran_escalation",))]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    # No cross-theme; no phrase match; below default threshold of 3.
    assert d.fire is False


# ---------- high-signal phrase ----------


def test_high_signal_phrase_fires(basic_config):
    items = [_h("h1", "EU imposes new sanctions on Russia today", themes=("us_iran_escalation",))]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert d.reason == "high_signal_phrase:new sanctions"


def test_high_signal_phrase_case_insensitive(basic_config):
    items = [_h("h1", "BREAKING: Ceasefire announced in Gaza", themes=("us_iran_escalation",))]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert "ceasefire" in d.reason.lower()


def test_high_signal_phrase_word_boundary():
    """`new sanctions` must not match `anew sanctions` or similar."""
    cfg = TriggerGateConfig(
        cross_theme_always_triggers=False,
        high_signal_phrases=["new sanctions"],
        delta_threshold_default=10,
    )
    items = [_h("h1", "this anew sanctions package", themes=("t1",))]
    d = evaluate_gate(items, config=cfg, window_since_unix=0, window_until_unix=1)
    # `\bnew sanctions\b` does NOT match because `anew` has no left-side boundary
    # before `new`. False positive prevented.
    assert d.fire is False


def test_empty_phrase_list_falls_through(basic_config):
    cfg = TriggerGateConfig(
        cross_theme_always_triggers=False,
        high_signal_phrases=[],
        delta_threshold_default=10,
    )
    items = [_h("h1", "the world ends today", themes=("t",))]
    d = evaluate_gate(items, config=cfg, window_since_unix=0, window_until_unix=1)
    assert d.fire is False


# ---------- delta threshold ----------


def test_delta_default_fires_at_threshold(basic_config):
    items = [
        _h("h1", "story one", themes=("fed_policy_path",)),
        _h("h2", "story two", themes=("fed_policy_path",)),
        _h("h3", "story three", themes=("fed_policy_path",)),
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert d.reason == "delta_threshold:fed_policy_path:3"
    assert d.themes_in_scope == ("fed_policy_path",)
    assert set(d.matched_headline_ids) == {"h1", "h2", "h3"}


def test_delta_below_default_does_not_fire(basic_config):
    items = [
        _h("h1", "story one", themes=("fed_policy_path",)),
        _h("h2", "story two", themes=("fed_policy_path",)),
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is False
    assert d.reason == "below_thresholds"


def test_per_theme_override_lifts_bar(basic_config):
    """us_iran_escalation override is 5; 4 should NOT fire."""
    items = [
        _h(f"h{i}", f"story {i}", themes=("us_iran_escalation",))
        for i in range(4)
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is False


def test_per_theme_override_fires_at_override(basic_config):
    items = [
        _h(f"h{i}", f"story {i}", themes=("us_iran_escalation",))
        for i in range(5)
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert d.reason == "delta_threshold:us_iran_escalation:5"


def test_mixed_themes_threshold_picks_first_hitter(basic_config):
    """Iran 4 (below 5 override), Fed 3 (at default) → Fed fires."""
    items = (
        [_h(f"i{i}", "iran story", themes=("us_iran_escalation",)) for i in range(4)]
        + [_h(f"f{i}", "fed story", themes=("fed_policy_path",)) for i in range(3)]
    )
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is True
    assert d.reason == "delta_threshold:fed_policy_path:3"


# ---------- evaluation order ----------


def test_cross_theme_priority_over_phrase(basic_config):
    """A cross-theme headline that also contains a phrase fires as cross-theme."""
    items = [_h(
        "h1",
        "ceasefire announced amid Iran inflation",
        themes=("us_iran_escalation", "fed_policy_path"),
    )]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.reason.startswith("cross_theme:")


def test_phrase_priority_over_delta(basic_config):
    """Phrase fires even if delta threshold would also be hit."""
    items = [
        _h("h1", "ceasefire breaking news", themes=("fed_policy_path",)),
        _h("h2", "story two", themes=("fed_policy_path",)),
        _h("h3", "story three", themes=("fed_policy_path",)),
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.reason == "high_signal_phrase:ceasefire"


# ---------- determinism ----------


def test_below_thresholds_reports_themes_in_scope(basic_config):
    """Even when nothing fires, themes_in_scope reflects what we saw."""
    items = [
        _h("h1", "x", themes=("us_iran_escalation",)),
        _h("h2", "y", themes=("fed_policy_path",)),
    ]
    d = evaluate_gate(items, config=basic_config, window_since_unix=0, window_until_unix=1)
    assert d.fire is False
    assert d.themes_in_scope == ("fed_policy_path", "us_iran_escalation")
