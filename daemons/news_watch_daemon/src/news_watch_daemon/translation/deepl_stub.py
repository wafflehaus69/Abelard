"""DeepL translation path — STUB (raises NotImplementedError on call).

Pass F Commit 2 (2026-05-28). Architecture-of-record per the
2026-05-27 doctrine note: DeepL is the documented fallback for the
failure modes Telegram-native translation cannot recover from
(Premium re-gating of messages.translateText, sustained FloodWait,
quality degradation). The YAML config flag (`translation_source` in
config/translation.yaml) lets a future operator flip from
`telegram_native` to `deepl` without a code change — but this stub
ensures the daemon fails LOUDLY on that flip rather than silently
mis-translating against an unimplemented path.

When DeepL is needed in production:
  1. Provision DEEPL_API_KEY (or whatever deepl.api_key_env points to)
  2. Replace this stub with a real httpx-backed DeepL client that
     mirrors translate_telegram_messages()'s signature and returns
     TranslationResult lists
  3. The orchestrator's translation-source dispatch in
     scrape/orchestrator.py routes calls to this module — no
     orchestrator change needed when the implementation lands

Until then: calling translate_deepl() raises NotImplementedError with
a pointer to the doctrine note and this file's docstring.
"""

from __future__ import annotations

from typing import Any

from .types import TranslationResult


_STUB_MESSAGE = (
    "DeepL translation path is intentionally stubbed (Pass F Commit 2). "
    "Flip translation_source back to 'telegram_native' in "
    "config/translation.yaml, OR implement this module per the "
    "docstring guidance and the 2026-05-27 Pass F doctrine note "
    "(daemons/news_watch_daemon/doctrine/2026-05-27_session_notes.md, "
    "Section 3 — Pass F translation architecture)."
)


async def translate_deepl(
    *,
    channel_username: str,
    msg_ids: list[int],
    original_texts: dict[int, str],
    to_lang: str = "en",
    batch_size: int = 10,
    api_key: str | None = None,
    free_tier: bool = True,
    **_extra: Any,
) -> list[TranslationResult]:
    """Stub — raises NotImplementedError. See module docstring."""
    raise NotImplementedError(_STUB_MESSAGE)


__all__ = ["translate_deepl"]
