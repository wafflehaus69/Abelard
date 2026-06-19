"""Source registry — builds the enabled Source plugins for a run.

The CLI calls `build_sources(cfg)`; the orchestrator fans out over whatever it
returns and isolates per-source failure. Finnhub company-news is the first
registered source (Order 2); /smg/, Trends, and Reddit join here as they land.
"""

from __future__ import annotations

from ..config import Config
from .base import Source
from .finnhub_news import FinnhubNewsSource


def build_sources(cfg: Config) -> list[Source]:
    return [
        FinnhubNewsSource(api_key=cfg.finnhub_api_key, user_agent=cfg.user_agent),
    ]
