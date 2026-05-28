"""Loader for `config/translation.yaml` — the Pass F YAML config.

Mirrors the load-validate-fail-loud pattern used by
`attention/stopwords.py` and `synthesize/config.py`. Validates schema
shape + value bounds; raises on missing/malformed config rather than
silently defaulting.

Module added Pass F Commit 2 (2026-05-28).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml


_LOG = logging.getLogger("news_watch_daemon.translation.config")


TranslationSource = Literal["telegram_native", "deepl"]
_VALID_SOURCES: tuple[str, ...] = ("telegram_native", "deepl")

_BATCH_SIZE_MIN = 1
_BATCH_SIZE_MAX = 100


class TranslationConfigError(RuntimeError):
    """Raised when the translation YAML is missing, malformed, or has
    out-of-bounds values."""


@dataclass(frozen=True)
class DeepLConfig:
    """DeepL fallback configuration (stub fields)."""

    api_key_env: str
    free_tier: bool


@dataclass(frozen=True)
class TranslationConfig:
    """Validated Pass F translation configuration.

    `translation_source` is the active translation path discriminator.
    Orchestrator reads this to route the translation call to
    telegram_native or deepl_stub.

    `telegram_native_batch_size` is the per-batch msg_id count for
    Telegram-native calls. Bounds-enforced in the loader.

    `deepl` is the fallback configuration; populated even when
    `translation_source == 'telegram_native'` so the future flip is
    config-only.
    """

    translation_source: TranslationSource
    telegram_native_batch_size: int
    deepl: DeepLConfig


def load_translation_config(path: Path) -> TranslationConfig:
    """Load + validate the translation YAML.

    Args:
        path: Absolute path to the translation.yaml file.

    Returns:
        TranslationConfig with validated fields.

    Raises:
        TranslationConfigError: file missing, unreadable, malformed
            YAML, missing required fields, invalid values, or
            out-of-bounds batch size.
    """
    if not path.is_file():
        raise TranslationConfigError(f"translation config file not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise TranslationConfigError(
            f"invalid YAML in {path}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise TranslationConfigError(
            f"translation config YAML root must be a mapping in {path}; "
            f"got {type(raw).__name__}"
        )

    # translation_source
    src = raw.get("translation_source")
    if src not in _VALID_SOURCES:
        raise TranslationConfigError(
            f"translation_source must be one of {_VALID_SOURCES}; got {src!r}"
        )

    # telegram_native_batch_size
    batch_size = raw.get("telegram_native_batch_size")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool):
        raise TranslationConfigError(
            f"telegram_native_batch_size must be int; got {type(batch_size).__name__}"
        )
    if not (_BATCH_SIZE_MIN <= batch_size <= _BATCH_SIZE_MAX):
        raise TranslationConfigError(
            f"telegram_native_batch_size must be in [{_BATCH_SIZE_MIN}, "
            f"{_BATCH_SIZE_MAX}]; got {batch_size}"
        )

    # deepl block
    deepl_raw = raw.get("deepl")
    if not isinstance(deepl_raw, dict):
        raise TranslationConfigError(
            f"deepl block must be a mapping; got {type(deepl_raw).__name__}"
        )
    deepl_key_env = deepl_raw.get("api_key_env")
    if not isinstance(deepl_key_env, str) or not deepl_key_env.strip():
        raise TranslationConfigError(
            "deepl.api_key_env must be a non-empty string"
        )
    free_tier = deepl_raw.get("free_tier")
    if not isinstance(free_tier, bool):
        raise TranslationConfigError(
            f"deepl.free_tier must be bool; got {type(free_tier).__name__}"
        )

    return TranslationConfig(
        translation_source=src,
        telegram_native_batch_size=batch_size,
        deepl=DeepLConfig(api_key_env=deepl_key_env, free_tier=free_tier),
    )


__all__ = [
    "DeepLConfig",
    "TranslationConfig",
    "TranslationConfigError",
    "TranslationSource",
    "load_translation_config",
]
