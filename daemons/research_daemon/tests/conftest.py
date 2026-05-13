"""Shared test fixtures."""

from __future__ import annotations

import pytest

from research_daemon.config import Config
from research_daemon.http_client import HttpClient


@pytest.fixture
def cfg() -> Config:
    return Config(
        finnhub_api_key="test_key_xyz",
        edgar_user_agent="ResearchDaemon Test test@example.com",
        log_level="WARNING",
    )


@pytest.fixture
def client() -> HttpClient:
    # Fast retries and no real backoff — tests should not sleep.
    return HttpClient(
        user_agent="ResearchDaemon Test test@example.com",
        max_retries=2,
        base_backoff=0.0,
        timeout=1.0,
    )
