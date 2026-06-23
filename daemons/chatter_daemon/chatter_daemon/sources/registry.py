"""Source registry — builds the enabled Source plugins for a run.

The CLI calls `build_sources(cfg)`; the orchestrator fans out over whatever it
returns and isolates per-source failure. Finnhub company-news was the first
registered source (Order 2); /smg/, Trends, and StockTwits have since landed.
"""

from __future__ import annotations

from ..config import Config
from .base import Source
from .finnhub_news import FinnhubNewsSource
from .google_trends import GoogleTrendsSource
from .smg import SmgSource
from .stocktwits import StockTwitsSource


def build_sources(cfg: Config) -> list[Source]:
    return [
        FinnhubNewsSource(api_key=cfg.finnhub_api_key, user_agent=cfg.user_agent),
        SmgSource(
            company_names_path=cfg.company_names_path,
            common_words_path=cfg.common_words_path,
            slang_blacklist_path=cfg.slang_blacklist_path,
            word_ticker_allowlist=cfg.word_ticker_allowlist,
            user_agent=cfg.user_agent,
        ),
        GoogleTrendsSource(company_names_path=cfg.company_names_path),
        # StockTwits sentiment (Order 9): native tags + Haiku-on-bodies blend. Browser
        # UA, no key; the Anthropic key (optional) gates Haiku above the sentiment floor.
        StockTwitsSource(
            anthropic_api_key=cfg.anthropic_api_key,
            haiku_model=cfg.haiku_model_id,
            sentiment_min_mentions=cfg.sentiment_min_mentions,
        ),
    ]
