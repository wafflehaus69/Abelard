"""Source registry — builds the enabled Source plugins for a run.

The CLI calls `build_sources(cfg)`; the orchestrator fans out over whatever it
returns and isolates per-source failure. Finnhub company-news was the first
registered source (Order 2); /smg/, Trends, and StockTwits have since landed.
"""

from __future__ import annotations

from ..config import Config
from .base import Source
from .finnhub_news import FinnhubNewsSource
from .smg import SmgSource
from .stocktwits import StockTwitsSource
from .twitter import TwitterSource


def build_sources(cfg: Config) -> list[Source]:
    sources: list[Source] = [
        FinnhubNewsSource(
            api_key=cfg.finnhub_api_key,
            user_agent=cfg.user_agent,
            # Order 15: named-news summary — company aliases for the direct-mention gate,
            # the shared Anthropic key, and the per-scan cost cap.
            company_names_path=cfg.company_names_path,
            anthropic_api_key=cfg.anthropic_api_key,
            haiku_model=cfg.haiku_model_id,
            summary_model=cfg.summary_model,
            summary_cost_cap_usd=cfg.news_summary_cost_cap_usd,
        ),
        SmgSource(
            company_names_path=cfg.company_names_path,
            common_words_path=cfg.common_words_path,
            slang_blacklist_path=cfg.slang_blacklist_path,
            word_ticker_allowlist=cfg.word_ticker_allowlist,
            user_agent=cfg.user_agent,
            # Haiku stance over the matched /smg/ post text (Order 9), gated above the floor.
            anthropic_api_key=cfg.anthropic_api_key,
            haiku_model=cfg.haiku_model_id,
            sentiment_min_mentions=cfg.sentiment_min_mentions,
        ),
        # StockTwits (Order 12): PRIMARY = the sentiment-API aggregate (now-primary live
        # read, real volume, participation); native tags from the stream; Haiku-on-bodies
        # OFF by default (the free aggregate supersedes it).
        StockTwitsSource(
            anthropic_api_key=cfg.anthropic_api_key,
            haiku_model=cfg.haiku_model_id,
            sentiment_min_mentions=cfg.sentiment_min_mentions,
            haiku_enabled=cfg.stocktwits_haiku_enabled,
        ),
    ]
    # Twitter/X cashtag source (Order 17) — the first subprocess source. Gated OFF by
    # default (cfg.twitter_enabled); flip CHATTER_TWITTER_ENABLED=1 after a live cert on
    # the host that has the `twitter` CLI. Absent from the fan-out entirely when off, so
    # zero behavior change to existing scans.
    if cfg.twitter_enabled:
        sources.append(
            TwitterSource(
                binary=cfg.twitter_binary,
                timeout_s=cfg.twitter_timeout_s,
                pace_s=cfg.twitter_pace_s,
                window_hours=cfg.twitter_window_hours,
                max_per_ticker=cfg.twitter_max_per_ticker,
                min_tweets_haiku=cfg.twitter_min_tweets_haiku,
                min_likes=cfg.twitter_min_likes,
                anthropic_api_key=cfg.anthropic_api_key,
                haiku_model=cfg.haiku_model_id,
                summary_model=cfg.summary_model,
                summary_cost_cap_usd=cfg.news_summary_cost_cap_usd,
                drop_promo=cfg.twitter_drop_promo,
            )
        )
    return sources
