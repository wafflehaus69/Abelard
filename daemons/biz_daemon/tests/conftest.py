"""Shared test fixtures."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from biz_daemon import storage
from biz_daemon.config import Config


@pytest.fixture(autouse=True)
def _reset_daemon_logger():
    yield
    logger = logging.getLogger("biz_daemon")
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(
        finnhub_api_key="test_finnhub_key",
        anthropic_api_key="test_anthropic_key",
        log_level="WARNING",
        db_path=tmp_path / "biz.sqlite3",
    )


@pytest.fixture
def conn(cfg: Config):
    c = storage.connect(cfg.db_path)
    storage.init_db(c)
    yield c
    c.close()
