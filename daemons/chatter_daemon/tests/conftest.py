"""Shared hermetic test fixtures (no network)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _reset_daemon_logger():
    yield
    logger = logging.getLogger("chatter_daemon")
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)


def _write(directory: Path, name: str, payload: object) -> Path:
    path = directory / f"{name}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.fixture
def watchlists_dir(tmp_path: Path) -> Path:
    """A directory of two valid watchlists, for valid-load / --all / CLI tests."""
    directory = tmp_path / "watchlists"
    directory.mkdir()
    _write(
        directory,
        "alpha",
        {"name": "alpha", "tickers": [{"symbol": "NVDA"}, {"symbol": "AMD", "name_match": False}]},
    )
    _write(
        directory,
        "beta",
        {
            "name": "beta",
            "tickers": [
                {"symbol": "TSM", "names": [], "notes": "TODO"},
                {"symbol": "P", "enabled": False},
            ],
        },
    )
    return directory
