"""Build the list of concrete `SourcePlugin` instances for a scrape sweep.

Inputs: validated `Config` + the list of active themes.
Output: a list of `SourcePlugin` instances, ordered deterministically.

Rules:
  - Exactly one `FinnhubGeneralNewsSource` (always included; if no
    `FINNHUB_API_KEY` is set, the plugin itself surfaces the error on
    fetch — the factory does not gatekeep).
  - One `RssSource` per unique feed_id across all *active* themes'
    `rss_feeds`. Entries with `enabled=False` are skipped.
  - One `TelegramSource` per unique `username` across all *active*
    themes' `telegram_channels`. Same channel referenced by multiple
    themes → built once, using the *lowest* cadence across all
    referencing themes (so a high-priority theme can override a
    low-priority one's polite cadence). Entries with `enabled=False`
    are skipped. Built only when all three Telegram credentials are
    present in config; otherwise a WARN is logged once and no Telegram
    sources are built (the daemon runs Finnhub + RSS as in Pass A).
  - Order: Finnhub first, then RSS sources sorted by name, then
    Telegram sources sorted by name. Within a tier, sort is
    deterministic.

Per-source constructor failures (e.g. a malformed username slipping
past Pydantic validation, a corrupt session string) are caught and
logged at WARN, and the rest of the source list is still returned.
A bad Telegram channel must not block Finnhub or RSS.
"""

from __future__ import annotations

import logging

from ..config import Config
from ..http_client import HttpClient
from ..sources.base import SourcePlugin
from ..sources.finnhub_general import FinnhubGeneralNewsSource
from ..sources.rss import RssSource
from ..sources.telegram import TelegramSource
from ..theme_config import ThemeConfig


_LOG = logging.getLogger("news_watch_daemon.scrape.factory")


def build_sources(
    cfg: Config,
    themes: list[ThemeConfig],
    http_client: HttpClient,
) -> list[SourcePlugin]:
    """Assemble the canonical source list for one scrape invocation."""
    sources: list[SourcePlugin] = [
        FinnhubGeneralNewsSource(http_client, cfg.finnhub_api_key),
    ]

    rss_by_id: dict[str, RssSource] = {}
    # Per-username minimum cadence across all active themes that reference it.
    tg_min_cadence: dict[str, int] = {}
    tg_referenced = False

    for theme in themes:
        if theme.status != "active":
            continue
        # RSS
        for feed in theme.rss_feeds:
            if not feed.enabled:
                continue
            url = str(feed.url)
            try:
                src = RssSource(http_client, feed_url=url, feed_id=feed.feed_id)
            except Exception as exc:  # noqa: BLE001 — log and skip per-source
                _LOG.warning(
                    "failed to build RssSource for %s: %s: %s",
                    url, type(exc).__name__, exc,
                )
                continue
            rss_by_id.setdefault(src.feed_id, src)
        # Telegram (collection phase — actual construction below, after creds check)
        for ch in theme.telegram_channels:
            tg_referenced = True
            if not ch.enabled:
                continue
            current = tg_min_cadence.get(ch.username)
            if current is None or ch.cadence_minutes < current:
                tg_min_cadence[ch.username] = ch.cadence_minutes

    sources.extend(sorted(rss_by_id.values(), key=lambda s: s.name))

    # Telegram construction phase.
    if tg_referenced and not cfg.telegram_creds_complete:
        _LOG.warning(
            "Telegram credentials not configured; skipping Telegram sources. "
            "Set TELEGRAM_API_ID, TELEGRAM_API_HASH, and TELEGRAM_SESSION_STRING "
            "to enable. Daemon continues with Finnhub + RSS only."
        )
    elif tg_min_cadence and cfg.telegram_creds_complete:
        tg_sources: list[TelegramSource] = []
        # Type-narrowing: telegram_creds_complete guarantees these are set
        assert cfg.telegram_api_id is not None
        assert cfg.telegram_api_hash is not None
        assert cfg.telegram_session_string is not None
        for username, cadence in tg_min_cadence.items():
            try:
                tg_sources.append(TelegramSource(
                    channel_username=username,
                    api_id=cfg.telegram_api_id,
                    api_hash=cfg.telegram_api_hash,
                    session_string=cfg.telegram_session_string,
                    cadence_minutes=cadence,
                ))
            except Exception as exc:  # noqa: BLE001 — log and skip per-channel
                _LOG.warning(
                    "failed to build TelegramSource for @%s: %s: %s",
                    username, type(exc).__name__, exc,
                )
        sources.extend(sorted(tg_sources, key=lambda s: s.name))

    return sources


__all__ = ["build_sources"]
