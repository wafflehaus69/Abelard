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
