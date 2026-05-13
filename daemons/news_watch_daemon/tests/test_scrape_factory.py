"""Source-factory tests — assembly logic for the scrape sweep."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from news_watch_daemon.config import Config
from news_watch_daemon.http_client import HttpClient
from news_watch_daemon.scrape.factory import build_sources
from news_watch_daemon.sources.finnhub_general import FinnhubGeneralNewsSource
from news_watch_daemon.sources.rss import RssSource
from news_watch_daemon.theme_config import ThemeConfig


def _cfg(tmp_path, *, finnhub: str | None = "k") -> Config:
    return Config(
        db_path=tmp_path / "state.db",
        log_level="WARNING",
        finnhub_api_key=finnhub,
    )


def _theme(payload_overrides: dict | None = None) -> ThemeConfig:
    base = {
        "theme_id": "t",
        "display_name": "T",
        "status": "active",
        "created_at": "2026-05-12",
        "brief": "x",
        "keywords": {"primary": ["x"], "secondary": [], "exclusions": []},
        "tracked_entities": {"tickers": ["AAA"], "companies": [], "countries": [], "commodities": [], "people": []},
        "synthesis": {},
        "alerts": {"velocity_baseline_headlines_per_day": 5.0},
    }
    if payload_overrides:
        base.update(payload_overrides)
    return ThemeConfig.model_validate(base)


@pytest.fixture
def http():
    return MagicMock(spec=HttpClient)


def test_finnhub_always_included(tmp_path, http):
    sources = build_sources(_cfg(tmp_path), themes=[], http_client=http)
    assert len(sources) == 1
    assert isinstance(sources[0], FinnhubGeneralNewsSource)


def test_finnhub_first_then_rss(tmp_path, http):
    theme = _theme({"rss_feeds": [{"url": "https://example.com/a"}]})
    sources = build_sources(_cfg(tmp_path), themes=[theme], http_client=http)
    assert isinstance(sources[0], FinnhubGeneralNewsSource)
    assert isinstance(sources[1], RssSource)


def test_rss_sources_dedup_by_url_across_themes(tmp_path, http):
    feed = {"url": "https://example.com/shared"}
    theme_a = _theme({"theme_id": "a", "rss_feeds": [feed]})
    theme_b = _theme({"theme_id": "b", "rss_feeds": [feed]})
    sources = build_sources(_cfg(tmp_path), themes=[theme_a, theme_b], http_client=http)
    rss_sources = [s for s in sources if isinstance(s, RssSource)]
    assert len(rss_sources) == 1


def test_two_different_feeds_yield_two_rss_sources(tmp_path, http):
    theme = _theme({"rss_feeds": [
        {"url": "https://example.com/a"},
        {"url": "https://example.com/b"},
    ]})
    sources = build_sources(_cfg(tmp_path), themes=[theme], http_client=http)
    rss = [s for s in sources if isinstance(s, RssSource)]
    assert len(rss) == 2


def test_disabled_feeds_skipped(tmp_path, http):
    theme = _theme({"rss_feeds": [
        {"url": "https://example.com/on"},
        {"url": "https://example.com/off", "enabled": False},
    ]})
    sources = build_sources(_cfg(tmp_path), themes=[theme], http_client=http)
    rss = [s for s in sources if isinstance(s, RssSource)]
    assert len(rss) == 1
    assert "off" not in rss[0].feed_id


def test_paused_theme_rss_feeds_skipped(tmp_path, http):
    paused = _theme({"theme_id": "p", "status": "paused", "rss_feeds": [{"url": "https://x/y"}]})
    sources = build_sources(_cfg(tmp_path), themes=[paused], http_client=http)
    rss = [s for s in sources if isinstance(s, RssSource)]
    assert rss == []


def test_archived_theme_rss_feeds_skipped(tmp_path, http):
    arch = _theme({"theme_id": "a", "status": "archived", "rss_feeds": [{"url": "https://x/y"}]})
    sources = build_sources(_cfg(tmp_path), themes=[arch], http_client=http)
    rss = [s for s in sources if isinstance(s, RssSource)]
    assert rss == []


def test_rss_sources_sorted_by_name(tmp_path, http):
    theme = _theme({"rss_feeds": [
        {"url": "https://zebra.example.com/feed"},
        {"url": "https://alpha.example.com/feed"},
    ]})
    sources = build_sources(_cfg(tmp_path), themes=[theme], http_client=http)
    rss = [s for s in sources if isinstance(s, RssSource)]
    assert rss[0].name < rss[1].name


def test_factory_passes_no_finnhub_key_through_to_plugin(tmp_path, http):
    """The factory does NOT gatekeep on missing FINNHUB_API_KEY; the plugin errors at fetch."""
    sources = build_sources(_cfg(tmp_path, finnhub=None), themes=[], http_client=http)
    assert len(sources) == 1
    assert isinstance(sources[0], FinnhubGeneralNewsSource)


# ---------- Telegram (Pass B) ----------


def _tg_cfg(tmp_path, *, complete: bool = True) -> Config:
    if complete:
        from tests.test_sources_telegram import VALID_API_HASH, VALID_SESSION
        return Config(
            db_path=tmp_path / "state.db",
            log_level="WARNING",
            finnhub_api_key="k",
            telegram_api_id=12345,
            telegram_api_hash=VALID_API_HASH,
            telegram_session_string=VALID_SESSION,
        )
    return Config(
        db_path=tmp_path / "state.db",
        log_level="WARNING",
        finnhub_api_key="k",
        telegram_api_id=None,
        telegram_api_hash=None,
        telegram_session_string=None,
    )


def test_telegram_built_when_creds_complete(tmp_path, http):
    from news_watch_daemon.sources.telegram import TelegramSource
    theme = _theme({"telegram_channels": [{"username": "valid_name"}]})
    sources = build_sources(_tg_cfg(tmp_path), themes=[theme], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert len(tg) == 1
    assert tg[0].channel_username == "valid_name"


def test_two_themes_same_username_built_once_with_min_cadence(tmp_path, http):
    from news_watch_daemon.sources.telegram import TelegramSource
    theme_a = _theme({"theme_id": "a", "telegram_channels": [
        {"username": "shared_chan", "cadence_minutes": 30},
    ]})
    theme_b = _theme({"theme_id": "b", "telegram_channels": [
        {"username": "shared_chan", "cadence_minutes": 15},
    ]})
    sources = build_sources(_tg_cfg(tmp_path), themes=[theme_a, theme_b], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert len(tg) == 1
    assert tg[0].cadence_minutes == 15  # min of (30, 15)


def test_disabled_telegram_channel_not_built(tmp_path, http, caplog):
    from news_watch_daemon.sources.telegram import TelegramSource
    theme = _theme({"telegram_channels": [
        {"username": "on_chan", "enabled": True},
        {"username": "off_chan", "enabled": False},
    ]})
    sources = build_sources(_tg_cfg(tmp_path), themes=[theme], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert len(tg) == 1
    assert tg[0].channel_username == "on_chan"


def test_missing_telegram_creds_skips_with_warn(tmp_path, http, caplog):
    from news_watch_daemon.sources.telegram import TelegramSource
    theme = _theme({"telegram_channels": [{"username": "valid_name"}]})
    with caplog.at_level("WARNING", logger="news_watch_daemon.scrape.factory"):
        sources = build_sources(_tg_cfg(tmp_path, complete=False), themes=[theme], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert tg == []
    # Finnhub still present; daemon continues.
    assert any(isinstance(s, FinnhubGeneralNewsSource) for s in sources)
    # WARN log emitted, mentioning the required env vars.
    warn_messages = [r.getMessage() for r in caplog.records if r.levelno >= 30]
    assert any("Telegram credentials not configured" in m for m in warn_messages)


def test_no_telegram_channels_no_warn(tmp_path, http, caplog):
    """If no theme references Telegram, missing creds isn't worth a warning."""
    theme = _theme({"telegram_channels": []})
    with caplog.at_level("WARNING", logger="news_watch_daemon.scrape.factory"):
        sources = build_sources(_tg_cfg(tmp_path, complete=False), themes=[theme], http_client=http)
    # No Telegram sources, no warning either.
    warn_messages = [r.getMessage() for r in caplog.records if r.levelno >= 30]
    assert not any("Telegram credentials" in m for m in warn_messages)


def test_paused_theme_telegram_channels_skipped(tmp_path, http):
    from news_watch_daemon.sources.telegram import TelegramSource
    paused = _theme({"theme_id": "p", "status": "paused",
                     "telegram_channels": [{"username": "valid_name"}]})
    sources = build_sources(_tg_cfg(tmp_path), themes=[paused], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert tg == []


def test_seed_theme_telegram_channels_become_sources(tmp_path, http):
    """The factory produces one TelegramSource per channel the seed declares.

    Channel-list-agnostic — derives the expected set from the seed YAML
    itself, so future channel additions/removals don't break this test.
    """
    from news_watch_daemon.theme_config import load_theme
    from news_watch_daemon.sources.telegram import TelegramSource
    from pathlib import Path
    seed = load_theme(Path(__file__).resolve().parent.parent / "themes" / "us_iran_escalation.yaml")
    expected = sorted(c.username for c in seed.telegram_channels if c.enabled)
    sources = build_sources(_tg_cfg(tmp_path), themes=[seed], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert sorted(s.channel_username for s in tg) == expected


def test_constructor_failure_logs_and_continues(tmp_path, http, caplog, monkeypatch):
    """A bad TelegramSource construction must not block other sources."""
    from news_watch_daemon.sources.telegram import TelegramSource

    real_init = TelegramSource.__init__

    def _flaky_init(self, **kwargs):
        if kwargs.get("channel_username") == "valid_name":
            raise ValueError("simulated bad channel")
        return real_init(self, **kwargs)

    monkeypatch.setattr(TelegramSource, "__init__", _flaky_init)

    theme = _theme({"telegram_channels": [
        {"username": "valid_name"},
        {"username": "good_chan"},
    ]})
    with caplog.at_level("WARNING", logger="news_watch_daemon.scrape.factory"):
        sources = build_sources(_tg_cfg(tmp_path), themes=[theme], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    # The good one was still built.
    assert len(tg) == 1
    assert tg[0].channel_username == "good_chan"
    # The bad one was logged.
    warn_messages = [r.getMessage() for r in caplog.records if r.levelno >= 30]
    assert any("valid_name" in m for m in warn_messages)


def test_telegram_sources_sorted_by_name(tmp_path, http):
    from news_watch_daemon.sources.telegram import TelegramSource
    theme = _theme({"telegram_channels": [
        {"username": "zebra_chan"},
        {"username": "alpha_chan"},
    ]})
    sources = build_sources(_tg_cfg(tmp_path), themes=[theme], http_client=http)
    tg = [s for s in sources if isinstance(s, TelegramSource)]
    assert tg[0].name < tg[1].name
