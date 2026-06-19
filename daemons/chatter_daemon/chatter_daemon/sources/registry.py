"""Source registry — builds the enabled Source plugins for a run.

The CLI calls `build_sources(cfg)`; the orchestrator fans out over whatever it
returns and isolates per-source failure. Finnhub company-news is the first
registered source (Order 2); /smg/, Trends, and Reddit join here as they land.
"""

from __future__ import annotations

from ..config import Config
from .base import Source
from .finnhub_news import FinnhubNewsSource
from .google_trends import GoogleTrendsSource
from .reddit import RedditSource
from .smg import SmgSource


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
        RedditSource(
            company_names_path=cfg.company_names_path,
            common_words_path=cfg.common_words_path,
            slang_blacklist_path=cfg.slang_blacklist_path,
            word_ticker_allowlist=cfg.word_ticker_allowlist,
            anthropic_api_key=cfg.anthropic_api_key,
            reddit_client_id=cfg.reddit_client_id,
            reddit_client_secret=cfg.reddit_client_secret,
            reddit_user_agent=cfg.reddit_user_agent,
            subreddits=cfg.reddit_subreddits,
            model=cfg.haiku_model_id,
            min_mentions=cfg.sentiment_min_mentions,
        ),
    ]
