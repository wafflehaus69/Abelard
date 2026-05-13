"""Theme config tests — schema, validation, seed-theme load, malformations."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from news_watch_daemon.theme_config import (
    RssFeedConfig,
    TelegramChannelConfig,
    ThemeConfig,
    ThemeLoadError,
    load_all_themes,
    load_theme,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_THEME = REPO_ROOT / "themes" / "us_iran_escalation.yaml"


# ---------- seed theme ----------


def test_seed_theme_loads():
    theme = load_theme(SEED_THEME)
    assert theme.theme_id == "us_iran_escalation"
    assert theme.display_name == "U.S.–Iran Escalation"
    assert theme.status == "active"
    assert "Iran" in theme.keywords.primary
    assert "LHX" in theme.tracked_entities.tickers
    assert theme.synthesis.cadence_hours == 4
    assert theme.alerts.velocity_spike_multiplier == 3.0
    assert "telegram" in theme.alerts.alert_channels


def test_seed_theme_keyword_patterns_compile():
    theme = load_theme(SEED_THEME)
    for bucket in (theme.keywords.primary, theme.keywords.secondary, theme.keywords.exclusions):
        for pattern in bucket:
            re.compile(pattern)  # raises if not valid


def test_seed_theme_config_hash_is_stable():
    a = load_theme(SEED_THEME).config_hash()
    b = load_theme(SEED_THEME).config_hash()
    assert a == b
    assert re.fullmatch(r"[0-9a-f]{64}", a)


def test_load_all_themes_picks_up_seed():
    themes = load_all_themes(REPO_ROOT / "themes")
    ids = [t.theme_id for t in themes]
    assert "us_iran_escalation" in ids


# ---------- ThemeConfig direct construction ----------


def _valid_payload() -> dict:
    return {
        "theme_id": "fertilizer_supply",
        "display_name": "Fertilizer Supply",
        "status": "active",
        "created_at": "2026-05-12",
        "brief": "Track fertilizer supply tightness.",
        "keywords": {
            "primary": ["urea", "ammonia"],
            "secondary": [],
            "exclusions": [],
        },
        "tracked_entities": {
            "tickers": ["NTR", "MOS"],
            "companies": [],
            "countries": [],
            "commodities": ["urea"],
            "people": [],
        },
        "synthesis": {},
        "alerts": {
            "velocity_baseline_headlines_per_day": 5.0,
        },
    }


def test_minimal_valid_theme_constructs():
    theme = ThemeConfig.model_validate(_valid_payload())
    assert theme.synthesis.cadence_hours == 4
    assert theme.synthesis.model == "claude-sonnet-4-7"
    assert theme.alerts.velocity_spike_multiplier == 3.0


def test_theme_id_must_be_snake_case():
    payload = _valid_payload()
    payload["theme_id"] = "Fertilizer-Supply"
    with pytest.raises(Exception, match="snake_case"):
        ThemeConfig.model_validate(payload)


def test_status_must_be_in_enum():
    payload = _valid_payload()
    payload["status"] = "draft"
    with pytest.raises(Exception):
        ThemeConfig.model_validate(payload)


def test_invalid_regex_pattern_rejected():
    payload = _valid_payload()
    payload["keywords"]["primary"] = ["[unclosed"]
    with pytest.raises(Exception, match="invalid regex"):
        ThemeConfig.model_validate(payload)


def test_empty_primary_keywords_rejected():
    payload = _valid_payload()
    payload["keywords"]["primary"] = []
    with pytest.raises(Exception, match="primary"):
        ThemeConfig.model_validate(payload)


def test_empty_tracked_entities_rejected():
    payload = _valid_payload()
    payload["tracked_entities"] = {
        "tickers": [],
        "companies": [],
        "countries": [],
        "commodities": [],
        "people": [],
    }
    with pytest.raises(Exception, match="tracked_entities"):
        ThemeConfig.model_validate(payload)


def test_unknown_top_level_field_rejected():
    payload = _valid_payload()
    payload["surprise"] = "boo"
    with pytest.raises(Exception):
        ThemeConfig.model_validate(payload)


def test_negative_cadence_rejected():
    payload = _valid_payload()
    payload["synthesis"] = {"cadence_hours": 0}
    with pytest.raises(Exception):
        ThemeConfig.model_validate(payload)


def test_duplicate_alert_channels_rejected():
    payload = _valid_payload()
    payload["alerts"]["alert_channels"] = ["telegram", "telegram"]
    with pytest.raises(Exception, match="duplicates"):
        ThemeConfig.model_validate(payload)


# ---------- loader-level malformations ----------


def _write_yaml(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def test_filename_mismatch_rejected(tmp_path):
    payload = _valid_payload()
    payload["theme_id"] = "fertilizer_supply"
    path = _write_yaml(tmp_path, "other_name", payload)
    with pytest.raises(ThemeLoadError, match="filename stem"):
        load_theme(path)


def test_missing_file_rejected(tmp_path):
    with pytest.raises(ThemeLoadError, match="not found"):
        load_theme(tmp_path / "missing.yaml")


def test_yaml_root_must_be_mapping(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("- just\n- a list\n", encoding="utf-8")
    with pytest.raises(ThemeLoadError, match="mapping"):
        load_theme(path)


def test_invalid_yaml_rejected(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("not: valid: yaml: : :\n", encoding="utf-8")
    with pytest.raises(ThemeLoadError, match="invalid YAML"):
        load_theme(path)


def test_load_all_themes_missing_dir(tmp_path):
    with pytest.raises(ThemeLoadError, match="themes directory"):
        load_all_themes(tmp_path / "nope")


def test_load_all_themes_empty_dir_returns_empty_list(tmp_path):
    assert load_all_themes(tmp_path) == []


# ---------- rss_feeds extension (Pass A) ----------


def test_seed_theme_has_empty_rss_feeds_by_default():
    theme = load_theme(SEED_THEME)
    assert theme.rss_feeds == []


def test_theme_with_one_rss_feed_loads():
    payload = _valid_payload()
    payload["rss_feeds"] = [
        {"url": "https://example.com/feed.xml", "feed_id": "example_main", "enabled": True}
    ]
    theme = ThemeConfig.model_validate(payload)
    assert len(theme.rss_feeds) == 1
    assert str(theme.rss_feeds[0].url) == "https://example.com/feed.xml"
    assert theme.rss_feeds[0].feed_id == "example_main"
    assert theme.rss_feeds[0].enabled is True


def test_rss_feed_defaults_enabled_true_and_feed_id_none():
    feed = RssFeedConfig.model_validate({"url": "https://example.com/feed.xml"})
    assert feed.enabled is True
    assert feed.feed_id is None


def test_duplicate_rss_urls_rejected():
    payload = _valid_payload()
    payload["rss_feeds"] = [
        {"url": "https://example.com/feed.xml"},
        {"url": "https://example.com/feed.xml"},
    ]
    with pytest.raises(Exception, match="duplicate URLs"):
        ThemeConfig.model_validate(payload)


def test_feed_id_with_whitespace_rejected():
    with pytest.raises(Exception, match="feed_id"):
        RssFeedConfig.model_validate({
            "url": "https://example.com/feed.xml",
            "feed_id": "has spaces",
        })


def test_feed_id_with_uppercase_rejected():
    with pytest.raises(Exception, match="feed_id"):
        RssFeedConfig.model_validate({
            "url": "https://example.com/feed.xml",
            "feed_id": "HasUpper",
        })


def test_feed_id_with_punctuation_rejected():
    with pytest.raises(Exception, match="feed_id"):
        RssFeedConfig.model_validate({
            "url": "https://example.com/feed.xml",
            "feed_id": "ok.but.dots",
        })


def test_rss_feeds_unknown_field_rejected():
    with pytest.raises(Exception):
        RssFeedConfig.model_validate({
            "url": "https://example.com/feed.xml",
            "surprise": "no",
        })


def test_config_hash_changes_when_rss_feeds_change():
    base = ThemeConfig.model_validate(_valid_payload())
    payload = _valid_payload()
    payload["rss_feeds"] = [{"url": "https://example.com/feed.xml"}]
    with_feed = ThemeConfig.model_validate(payload)
    assert base.config_hash() != with_feed.config_hash()


# ---------- telegram_channels extension (Pass B) ----------


def test_seed_theme_registers_core_telegram_channels():
    """The seed theme always declares the core CIG/bloomberg/trading trio.

    Channel-list-agnostic beyond that core set — themes evolve, and the
    formerly-fourth chainlinkbreadcrumbs channel has migrated to the
    tokenized_finance_infrastructure theme where it fits better.
    """
    theme = load_theme(SEED_THEME)
    usernames = {c.username for c in theme.telegram_channels}
    core = {"CIG_telegram", "bloomberg", "trading"}
    assert core.issubset(usernames)
    # Cadences for the core trio are stable
    by_username = {c.username: c for c in theme.telegram_channels}
    assert by_username["CIG_telegram"].cadence_minutes == 15
    assert by_username["bloomberg"].cadence_minutes == 30
    assert by_username["trading"].cadence_minutes == 30


def test_seed_theme_telegram_channels_all_enabled_by_default():
    theme = load_theme(SEED_THEME)
    assert all(c.enabled for c in theme.telegram_channels)


def test_telegram_channel_defaults():
    c = TelegramChannelConfig.model_validate({"username": "valid_name"})
    assert c.cadence_minutes == 15
    assert c.enabled is True


def test_telegram_channel_invalid_username_format_rejected():
    with pytest.raises(Exception, match="username"):
        TelegramChannelConfig.model_validate({"username": "no-hyphens-allowed"})


def test_telegram_channel_too_short_username_rejected():
    with pytest.raises(Exception, match="username"):
        TelegramChannelConfig.model_validate({"username": "abcd"})  # 4 chars


def test_telegram_channel_too_long_username_rejected():
    with pytest.raises(Exception, match="username"):
        TelegramChannelConfig.model_validate({"username": "a" * 33})


def test_telegram_channel_starting_with_digit_rejected():
    with pytest.raises(Exception, match="username"):
        TelegramChannelConfig.model_validate({"username": "1abcde"})


def test_telegram_channel_negative_cadence_rejected():
    with pytest.raises(Exception):
        TelegramChannelConfig.model_validate({"username": "valid_name", "cadence_minutes": -1})


def test_telegram_channel_zero_cadence_rejected():
    with pytest.raises(Exception):
        TelegramChannelConfig.model_validate({"username": "valid_name", "cadence_minutes": 0})


def test_telegram_channel_unknown_field_rejected():
    with pytest.raises(Exception):
        TelegramChannelConfig.model_validate({"username": "valid_name", "surprise": 1})


def test_duplicate_telegram_usernames_in_one_theme_rejected():
    payload = _valid_payload()
    payload["telegram_channels"] = [
        {"username": "cig_telegram"},
        {"username": "cig_telegram"},
    ]
    with pytest.raises(Exception, match="duplicate usernames"):
        ThemeConfig.model_validate(payload)


def test_empty_telegram_channels_is_valid():
    payload = _valid_payload()
    payload["telegram_channels"] = []
    theme = ThemeConfig.model_validate(payload)
    assert theme.telegram_channels == []


def test_config_hash_changes_when_telegram_channels_change():
    base = ThemeConfig.model_validate(_valid_payload())
    payload = _valid_payload()
    payload["telegram_channels"] = [{"username": "valid_name"}]
    with_channel = ThemeConfig.model_validate(payload)
    assert base.config_hash() != with_channel.config_hash()
