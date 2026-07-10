"""Shared pytest fixtures.

The autouse fixture below resets the `news_watch_daemon` logger state
between tests. `configure_logging()` sets `propagate=False` on the
logger so daemon log records don't double-emit through root handlers.
That's the right production behavior but breaks pytest's caplog in any
test that runs after a test which called `configure_logging` (caplog
captures via root, which won't see records from a non-propagating
logger). Resetting after each test isolates the side effect.
"""

from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _disable_dotenv_autoload(monkeypatch):
    """Keep the suite hermetic: main() auto-loads `<repo_root>/.env` in
    production (see config.load_env_file), and the repo carries a real `.env`
    with the developer's DB path + secrets. Without this guard, any test that
    invokes main() would silently pick up those values — masking, e.g., the
    missing-NEWS_WATCH_DB_PATH config-error path. The NEWS_WATCH_NO_ENV_FILE
    escape hatch disables the auto-load for every test."""
    monkeypatch.setenv("NEWS_WATCH_NO_ENV_FILE", "1")


@pytest.fixture(autouse=True)
def _reset_daemon_logger():
    yield
    logger = logging.getLogger("news_watch_daemon")
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)
    # Also reset any namespaced child loggers we know about.
    for child_name in (
        "news_watch_daemon.http",
        "news_watch_daemon.cli",
        "news_watch_daemon.scrape",
        "news_watch_daemon.scrape.factory",
        "news_watch_daemon.sources.telegram",
    ):
        child = logging.getLogger(child_name)
        child.handlers.clear()
        child.propagate = True
        child.setLevel(logging.NOTSET)
