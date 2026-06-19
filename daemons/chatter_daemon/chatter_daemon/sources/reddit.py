"""Reddit plugin (Order 6) — the last source, and the daemon's only LLM call.

PRAW (official Reddit API, OAuth script app, free non-commercial tier) over a
configurable set of subreddits. Reuses §B's shared `matching.Matcher` (dual-scan,
`\\b` discipline, name_match gating) — NOT a reimplementation.

**24h activity-counted**, aligned to the canonical 24h window (not a 7d bucket):
the client pulls each subreddit's hot submissions AND their comments; this plugin
keeps only items (submission or comment) whose `created_unix` falls in the last 24h.
That captures engagement on persistently-hot OLD threads — comments made today on a
week-old post still count — while the report reads "24h" consistently across sources.

Per active ticker: count distinct in-window items that mention it, then GATE on the
mention floor:
  - tickers AT/ABOVE `min_mentions` → one batched Haiku call classifies bull/bear/
    neutral stance (`sentiment.method = "haiku"`), flagged `sentiment_classified`;
  - tail tickers (below floor) carry `sentiment.method = "none"` — Haiku never sees
    them, so the LLM never runs on noise.

Cost telemetry is captured before any record is built (doctrine #8); a Haiku failure
is isolated into `SourceResult(error=...)` with the already-spent cost still
attached, and the orchestrator marks the source degraded.

UTF-8 boundary: PRAW keeps its OWN transport (not the shared http_client) and hands
back already-decoded `str`. This plugin still owes the non-ASCII contract at its
boundary — the matcher must still extract tickers wedged against non-ASCII
punctuation (regression test in test_reddit). Creds bind at fetch, not construction:
missing Reddit creds / `praw` raise `RedditAuthError` at fetch and the source
degrades, exactly like Finnhub's missing-key path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from abelard_common import ticker_noise
from abelard_common.company_aliases import load_name_map

from .. import sentiment as _sentiment
from ..config import (
    DEFAULT_SENTIMENT_MIN_MENTIONS,
    DEFAULT_SUBREDDITS,
    DEFAULT_WORD_TICKER_ALLOWLIST,
    HAIKU_MODEL_ID,
)
from ..matching import Matcher
from ..schema import CostTelemetry, Metrics, NormalizedRecord, Sentiment
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

SOURCE_NAME = "reddit"
WINDOW_LABEL = "24h"
_WINDOW_SECONDS = 24 * 60 * 60
DEFAULT_POST_LIMIT = 100


class RedditAuthError(RuntimeError):
    """Missing/invalid Reddit creds, or `praw` not installed. Raised at fetch."""


@dataclass(frozen=True)
class RedditPost:
    """One Reddit item (submission or comment) normalized for the matcher. `text`
    is already UTF-8 `str` (PRAW decodes); `created_unix` is the item's creation
    time, used to keep only activity inside the canonical 24h window."""

    post_id: str
    text: str
    created_unix: int


@runtime_checkable
class RedditClient(Protocol):
    """Injectable Reddit transport — the real one wraps PRAW; tests inject a fake."""

    def posts(
        self, subreddits: tuple[str, ...], *, limit: int, listing: str = "hot"
    ) -> list[RedditPost]:
        ...


class PrawClient:
    """Production Reddit client — lazy-imports PRAW, read-only script-app auth.

    Yields each hot submission AND its comments as timestamped items; the plugin
    does the 24h windowing, so the client stays window-agnostic."""

    def __init__(self, *, client_id: str | None, client_secret: str | None, user_agent: str | None) -> None:
        if not (client_id and client_secret and user_agent):
            raise RedditAuthError(
                "Reddit requires client_id, client_secret and user_agent (script app)"
            )
        try:
            import praw
        except ImportError as exc:
            raise RedditAuthError(
                "the `praw` package is not installed; Reddit cannot run "
                "(`pip install praw`)"
            ) from exc
        self._reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            check_for_async=False,
        )
        self._reddit.read_only = True

    def posts(
        self, subreddits: tuple[str, ...], *, limit: int, listing: str = "hot"
    ) -> list[RedditPost]:
        out: list[RedditPost] = []
        for sub in subreddits:
            sr = self._reddit.subreddit(sub)
            stream = sr.rising(limit=limit) if listing == "rising" else sr.hot(limit=limit)
            for submission in stream:
                out.append(
                    RedditPost(
                        post_id=f"t3_{submission.id}",
                        text=f"{submission.title}\n{submission.selftext or ''}",
                        created_unix=int(submission.created_utc),
                    )
                )
                submission.comments.replace_more(limit=0)  # drop "load more" stubs
                for comment in submission.comments.list():
                    out.append(
                        RedditPost(
                            post_id=f"t1_{comment.id}",
                            text=comment.body or "",
                            created_unix=int(comment.created_utc),
                        )
                    )
        return out


class RedditSource:
    """Source adapter for Reddit. Free-text, dual-scan, gated Haiku stance."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        company_names_path: str | Path,
        common_words_path: str | Path,
        slang_blacklist_path: str | Path,
        anthropic_api_key: str | None,
        reddit_client_id: str | None,
        reddit_client_secret: str | None,
        reddit_user_agent: str | None,
        word_ticker_allowlist: frozenset[str] = DEFAULT_WORD_TICKER_ALLOWLIST,
        subreddits: tuple[str, ...] = DEFAULT_SUBREDDITS,
        model: str = HAIKU_MODEL_ID,
        min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS,
        post_limit: int = DEFAULT_POST_LIMIT,
        reddit_client: RedditClient | None = None,
        anthropic_client: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.reddit")
        self._shared_map = load_name_map(Path(company_names_path))
        self._common_words = ticker_noise.load_common_words(Path(common_words_path))
        self._blacklist = ticker_noise.load_blacklist(Path(slang_blacklist_path))
        self._allowlist = frozenset(word_ticker_allowlist)
        self._anthropic_api_key = anthropic_api_key
        self._reddit_creds = (reddit_client_id, reddit_client_secret, reddit_user_agent)
        self._subreddits = tuple(subreddits)
        self._model = model
        self._min_mentions = min_mentions
        self._post_limit = post_limit
        self._reddit_client = reddit_client  # injected in tests
        self._anthropic_client = anthropic_client  # injected in tests

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        matcher = Matcher.for_watchlist(
            watchlist,
            shared_map=self._shared_map,
            blacklist=self._blacklist,
            common_words=self._common_words,
            allowlist=self._allowlist,
        )
        window = context.windows[WINDOW_LABEL]

        # Loud-fail on missing creds / praw: raises -> orchestrator isolates.
        reddit = self._reddit_client or PrawClient(
            client_id=self._reddit_creds[0],
            client_secret=self._reddit_creds[1],
            user_agent=self._reddit_creds[2],
        )
        posts = reddit.posts(self._subreddits, limit=self._post_limit)

        # 24h activity window, anchored to the single canonical timestamp.
        cutoff = context.canonical_unix - _WINDOW_SECONDS
        counts: dict[str, set[str]] = {}
        kinds: dict[str, set[str]] = {}
        post_hits: dict[str, tuple[str, set[str]]] = {}  # post_id -> (text, {tickers})
        for post in posts:
            if not (cutoff <= post.created_unix <= context.canonical_unix):
                continue  # activity outside the 24h window
            hits = matcher.match(post.text)
            if not hits:
                continue
            post_hits[post.post_id] = (post.text, set(hits))
            for sym, ks in hits.items():
                counts.setdefault(sym, set()).add(post.post_id)
                kinds.setdefault(sym, set()).update(ks)

        # Gate: only tickers AT/ABOVE the floor reach Haiku — the LLM never runs on
        # noise. Cost is captured into this object before any record is built.
        eligible = {t for t, ids in counts.items() if len(ids) >= self._min_mentions}
        cost = CostTelemetry()
        tallies: dict[str, dict[str, int]] = {}
        src_error: str | None = None
        if eligible:
            payload: list[dict[str, Any]] = []
            for post_id, (text, ticks) in post_hits.items():
                ets = sorted(ticks & eligible)
                if ets:
                    payload.append({"post_id": post_id, "text": text, "tickers": ets})
            try:
                client = self._anthropic_client or _sentiment.build_anthropic_client(
                    self._anthropic_api_key or ""
                )
                tallies = _sentiment.classify_stance(
                    posts=payload, client=client, model=self._model, cost=cost
                )
            except _sentiment.SentimentError as exc:
                # Missing key or a Haiku failure: cost already captured in `cost`;
                # record the degradation but keep the mention counts (method="none").
                src_error = str(exc)
                self._log.warning("reddit sentiment failed: %s", exc)

        classified_ok = src_error is None
        records: list[NormalizedRecord] = []
        for spec in watchlist.active_tickers:
            mentions = len(counts.get(spec.symbol, ()))
            tally = tallies.get(spec.symbol)
            did_classify = classified_ok and spec.symbol in eligible and tally is not None
            if did_classify:
                sentiment = Sentiment(
                    method="haiku",
                    bullish=tally.get("bullish", 0),
                    bearish=tally.get("bearish", 0),
                    neutral=tally.get("neutral", 0),
                )
            else:
                sentiment = Sentiment(method="none")
            records.append(
                NormalizedRecord(
                    watchlist=watchlist.name,
                    scan_mode=context.scan_mode,
                    canonical_ts=context.canonical_ts,
                    window=window,
                    source=SOURCE_NAME,
                    ticker=spec.symbol,
                    matched_by=sorted(kinds.get(spec.symbol, set())),
                    metrics=Metrics(mention_count=mentions),
                    sentiment=sentiment,
                    flags=["sentiment_classified"] if did_classify else [],
                )
            )
        return SourceResult(source=SOURCE_NAME, records=records, error=src_error, cost=cost)
