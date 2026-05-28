"""Translation config YAML loader tests (Pass F Commit 2)."""

from __future__ import annotations

import pytest

from news_watch_daemon.translation.config import (
    TranslationConfigError,
    load_translation_config,
)


def _write(tmp_path, body: str):
    p = tmp_path / "translation.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def test_load_translation_config_default_shape(tmp_path):
    """Canonical config (matching bundled config/translation.yaml) loads."""
    body = (
        "translation_source: telegram_native\n"
        "telegram_native_batch_size: 10\n"
        "deepl:\n"
        "  api_key_env: DEEPL_API_KEY\n"
        "  free_tier: true\n"
    )
    cfg = load_translation_config(_write(tmp_path, body))
    assert cfg.translation_source == "telegram_native"
    assert cfg.telegram_native_batch_size == 10
    assert cfg.deepl.api_key_env == "DEEPL_API_KEY"
    assert cfg.deepl.free_tier is True


def test_load_translation_config_deepl_source(tmp_path):
    """translation_source='deepl' loads cleanly (stub validation; the
    code-path that calls the deepl_stub module is exercised separately)."""
    body = (
        "translation_source: deepl\n"
        "telegram_native_batch_size: 5\n"
        "deepl:\n"
        "  api_key_env: ALTERNATE_KEY\n"
        "  free_tier: false\n"
    )
    cfg = load_translation_config(_write(tmp_path, body))
    assert cfg.translation_source == "deepl"
    assert cfg.deepl.free_tier is False


def test_load_translation_config_batch_size_override(tmp_path):
    """batch_size can be tuned via YAML without a code change."""
    body = (
        "translation_source: telegram_native\n"
        "telegram_native_batch_size: 25\n"
        "deepl:\n  api_key_env: X\n  free_tier: true\n"
    )
    cfg = load_translation_config(_write(tmp_path, body))
    assert cfg.telegram_native_batch_size == 25


def test_load_translation_config_invalid_source_rejected(tmp_path):
    body = (
        "translation_source: google\n"  # not in {telegram_native, deepl}
        "telegram_native_batch_size: 10\n"
        "deepl:\n  api_key_env: X\n  free_tier: true\n"
    )
    with pytest.raises(TranslationConfigError, match="translation_source"):
        load_translation_config(_write(tmp_path, body))


def test_load_translation_config_out_of_bounds_batch_size_rejected(tmp_path):
    body = (
        "translation_source: telegram_native\n"
        "telegram_native_batch_size: 101\n"  # above max 100
        "deepl:\n  api_key_env: X\n  free_tier: true\n"
    )
    with pytest.raises(TranslationConfigError, match="batch_size"):
        load_translation_config(_write(tmp_path, body))


def test_load_translation_config_missing_file_fails_loud(tmp_path):
    with pytest.raises(TranslationConfigError, match="not found"):
        load_translation_config(tmp_path / "does_not_exist.yaml")


def test_load_translation_config_malformed_yaml_fails_loud(tmp_path):
    body = "translation_source:\n  -\n bad\n indent\n"  # malformed
    with pytest.raises(TranslationConfigError):
        load_translation_config(_write(tmp_path, body))
