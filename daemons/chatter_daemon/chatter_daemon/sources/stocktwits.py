"""StockTwits source (Order 9) — public API, no key, browser-TLS impersonation.

Two public endpoints back two jobs; this module is the transport + parse layer for
both. NO credentials — StockTwits' public API needs none. It DOES sit behind
CloudFlare, which 403s the Python `requests`/urllib3 TLS signature on sight regardless
of headers, so the default transport uses `curl_cffi` to impersonate a real Chrome
TLS + HTTP2 fingerprint (the live-verified way past the wall).

  - trending  (DISCOVERY, Phase B): GET /api/2/trending/symbols.json
      -> 30 pre-ranked symbol objects. The ranking IS the gate (top-30), so there is
      no mention-floor and no universe-validation: the symbols are cashtag-native and
      exchange-validated at the source. `trending_score`/`rank` are the momentum axis;
      `watchlist_count` is context (too stable to gate on). `fundamentals` and
      `trends.summary` are nullable — guard every access.
  - symbol stream (SENTIMENT, Phase C): GET /api/2/streams/symbol/{T}.json
      -> 30 messages/ticker, each a free-text body + an optional native Bull/Bear tag.

DEGRADE-CLEAN (non-negotiable). Impersonation gets clean JSON today, but CloudFlare can
still re-wall any call (a 403 challenge or a non-JSON body) if it tightens. Every method
is best-effort: a CF wall / non-JSON / transport / rate-limit failure raises
`StockTwitsBlocked`, which the caller turns into a soft per-surface (trending) or
per-ticker (stream) failure and sets `degraded`. The daemon never crashes on StockTwits
and never fabricates — /smg/, Finnhub, and Trends carry the scan when StockTwits is dark.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Callable

from ..config import DEFAULT_SENTIMENT_MIN_MENTIONS, HAIKU_MODEL_ID
from ..schema import CostTelemetry, Metrics, NativeStance, NormalizedRecord, Sentiment
from ..sentiment import AnthropicProvider, SentimentError, classify_stance
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

API_BASE = "https://api.stocktwits.com/api/2"
TRENDING_URL = f"{API_BASE}/trending/symbols.json"
STREAM_URL = API_BASE + "/streams/symbol/{symbol}.json"

# The browser UA curl_cffi presents (informational — the `impersonate` profile sets the
# matching UA + TLS + HTTP2 fingerprint together; a UA alone does NOT pass CloudFlare).
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# curl_cffi impersonation profile — the live-verified one past StockTwits' CloudFlare.
DEFAULT_IMPERSONATE = "chrome"


class StockTwitsBlocked(RuntimeError):
    """A soft, degrade-clean failure: CF wall (403 / non-JSON) or transport.

    Distinct from a crash — the caller logs it, marks the surface / ticker failed, sets
    `degraded`, and carries on. Never raised past a pull/stream boundary.
    """


class _ImpersonatingTransport:
    """Default StockTwits transport: `curl_cffi` impersonating a real browser's TLS +
    HTTP2 fingerprint. StockTwits sits behind CloudFlare, which 403s the Python
    `requests`/urllib3 TLS signature on sight regardless of headers — impersonation is
    what gets clean JSON (live-verified). Exposes the same `get_json(url)` the client
    expects, forces UTF-8 decode, and raises on any non-2xx / non-JSON so the client
    maps it to a degrade-clean StockTwitsBlocked."""

    def __init__(
        self,
        *,
        impersonate: str = DEFAULT_IMPERSONATE,
        timeout: float = 20.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self._impersonate = impersonate
        self._timeout = timeout
        self._log = logger or logging.getLogger("chatter_daemon.stocktwits")

    def get_json(self, url: str) -> Any:
        # Lazy import: only StockTwits pulls in curl_cffi, and tests inject a fake client.
        from curl_cffi import requests as _creq

        resp = _creq.get(url, impersonate=self._impersonate, timeout=self._timeout)
        resp.raise_for_status()  # 403 challenge / 5xx -> HTTPError -> StockTwitsBlocked
        return json.loads(resp.content.decode("utf-8"))  # UTF-8 decode obligation


class StockTwitsClient:
    """Public StockTwits client — no key. The default transport impersonates a browser
    TLS fingerprint (curl_cffi) to clear CloudFlare; a fake `client` is injected in tests.

    `trending()` powers ATTENTION discovery (Phase B); `symbol_stream()` powers the
    sentiment blend (Phase C). Both raise `StockTwitsBlocked` on any soft failure.
    """

    def __init__(
        self,
        *,
        impersonate: str = DEFAULT_IMPERSONATE,
        client: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.stocktwits")
        # Default: TLS-impersonating transport (clears CF). Any object exposing
        # `get_json(url)` can be injected for tests.
        self._client = client or _ImpersonatingTransport(impersonate=impersonate, logger=self._log)

    def trending(self) -> list[dict[str, Any]]:
        """The 30 trending symbol objects (DISCOVERY). Raises StockTwitsBlocked on a CF
        wall / non-JSON / transport failure — a 200 + HTML challenge makes `.json()`
        raise, which we map to a soft block here (never a crash)."""
        data = self._get_json(TRENDING_URL, what="trending")
        symbols = data.get("symbols") if isinstance(data, dict) else None
        if not isinstance(symbols, list) or not symbols:
            raise StockTwitsBlocked(
                "trending response was not JSON-with-symbols (CF challenge?)"
            )
        return [s for s in symbols if isinstance(s, dict)]

    def symbol_stream(self, symbol: str) -> list[dict[str, Any]]:
        """The recent messages for one symbol (SENTIMENT). Each is a dict with at least
        a `body`; an optional `entities.sentiment.basic` in {Bullish, Bearish} is the
        user's native tag (sparse, ~40% coverage). An EMPTY stream is an honest zero,
        not a block. Raises StockTwitsBlocked on a CF wall / non-JSON / transport."""
        data = self._get_json(STREAM_URL.format(symbol=symbol), what=f"stream {symbol}")
        messages = data.get("messages") if isinstance(data, dict) else None
        if not isinstance(messages, list):
            raise StockTwitsBlocked(
                f"stream {symbol} response was not JSON-with-messages (CF challenge?)"
            )
        return [m for m in messages if isinstance(m, dict)]

    def _get_json(self, url: str, *, what: str) -> Any:
        """GET + parse, mapping every failure to StockTwitsBlocked. The shared client
        raises on transport / 404 / 429; `.json()` raises on a non-JSON CF wall. We
        treat all of them as one soft, degrade-clean signal."""
        try:
            return self._client.get_json(url)
        except Exception as exc:  # transport, rate-limit, 404, or non-JSON (CF HTML)
            raise StockTwitsBlocked(
                f"{what} unavailable (CF wall or transport): {exc}"
            ) from exc


SOURCE_NAME = "stocktwits"
WINDOW_LABEL = "24h"  # the stream is recent messages; stamp the 24h window

# Courtesy delay between per-ticker stream pulls — be a polite client (the API tolerates
# ~45/scan, but we space them out). Jittered; injectable as a no-op in tests.
COURTESY_MIN_S = 0.2
COURTESY_MAX_S = 0.5


def _courtesy_sleep() -> None:
    time.sleep(random.uniform(COURTESY_MIN_S, COURTESY_MAX_S))


def native_tag(message: dict[str, Any]) -> str | None:
    """The user's own Bull/Bear self-tag for a message, or None. `entities` and
    `entities.sentiment` are both nullable upstream — guard every hop (a naive chained
    .get crashes on the sparse-tag majority)."""
    entities = message.get("entities")
    sentiment = entities.get("sentiment") if isinstance(entities, dict) else None
    basic = sentiment.get("basic") if isinstance(sentiment, dict) else None
    if isinstance(basic, str):
        b = basic.strip().lower()
        if b in ("bullish", "bearish"):
            return b
    return None


class StockTwitsSource:
    """Watchlist sentiment source (Order 9). For each ACTIVE watchlist ticker, pull its
    StockTwits message stream and blend two stance reads:

      - NATIVE — the users' own Bull/Bear self-tags, tallied at zero LLM cost (~40%
        coverage). ALWAYS carried (in `sentiment.native`) with its tagged/messages
        coverage.
      - HAIKU — Claude classifies the message bodies for FULL coverage, but only when
        the stream has `messages >= sentiment_min_mentions` (the gate) and an Anthropic
        client is available. Above the gate the Haiku tally is the primary read and the
        native tally rides alongside, so a divergence stays visible (Abelard reconciles,
        the daemon does not).

    DEGRADE-CLEAN per ticker: a CF-walled stream (StockTwitsBlocked) yields a warning
    and no record for that ticker (an honest "unread", not a fabricated zero); a Haiku
    failure falls back to the native read with a warning. One ticker never sinks the
    rest, and nothing is fabricated.
    """

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        anthropic_api_key: str | None = None,
        haiku_model: str = HAIKU_MODEL_ID,
        sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS,
        client: StockTwitsClient | None = None,
        anthropic_client: Any | None = None,
        sleep: Callable[[], None] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.stocktwits")
        self._client = client  # StockTwitsClient; built lazily (impersonating) if absent
        self._anthropic = AnthropicProvider(
            api_key=anthropic_api_key, client=anthropic_client, logger=self._log
        )
        self._haiku_model = haiku_model
        self._floor = sentiment_min_mentions
        self._sleep = sleep if sleep is not None else _courtesy_sleep

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        client = self._client or StockTwitsClient(logger=self._log)
        window = context.windows[WINDOW_LABEL]
        records: list[NormalizedRecord] = []
        warnings: list[str] = []
        blocked: list[str] = []
        cost = CostTelemetry()
        actives = watchlist.active_tickers

        for i, spec in enumerate(actives):
            if i:
                self._sleep()  # courtesy delay BETWEEN tickers (not before the first)
            try:
                messages = client.symbol_stream(spec.symbol)
            except StockTwitsBlocked as exc:
                # Per-ticker degrade-clean: log it, emit NO record (an honest "unread",
                # not a fabricated zero), and remember it so the surface is marked
                # degraded below. The unblocked tickers still ship.
                self._log.warning("stocktwits stream blocked for %s: %s", spec.symbol, exc)
                blocked.append(spec.symbol)
                continue
            records.append(
                self._build_record(watchlist, context, window, spec.symbol, messages, warnings, cost)
            )

        # Any CF-walled ticker degrades the surface — `error` flips the envelope
        # `degraded` flag (ok=False) while the unblocked tickers carry on. Haiku
        # soft-fallbacks stay in `warnings` (data present, lower-quality stance) and do
        # NOT flip degraded.
        error = (
            f"{len(blocked)}/{len(actives)} streams walled (CF): {', '.join(blocked)}"
            if blocked
            else None
        )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, error=error, cost=cost
        )

    def _build_record(self, watchlist, context, window, symbol, messages, warnings, cost):
        nat_bull = nat_bear = nat_tagged = 0
        posts: list[dict[str, Any]] = []
        for idx, m in enumerate(messages):
            tag = native_tag(m)
            if tag == "bullish":
                nat_bull += 1
                nat_tagged += 1
            elif tag == "bearish":
                nat_bear += 1
                nat_tagged += 1
            body = m.get("body")
            if isinstance(body, str) and body.strip():
                posts.append(
                    {
                        "post_id": str(m.get("id", f"st-{symbol}-{idx}")),
                        "text": body,
                        "tickers": [symbol],
                    }
                )

        n = len(messages)
        native = NativeStance(bullish=nat_bull, bearish=nat_bear, tagged=nat_tagged, messages=n)

        sentiment: Sentiment | None = None
        # Haiku gated ABOVE the sentiment floor, and only with an Anthropic client + bodies.
        if n >= self._floor and posts:
            anthropic = self._anthropic.get()
            if anthropic is not None:
                try:
                    tallies = classify_stance(
                        posts=posts, client=anthropic, model=self._haiku_model, cost=cost
                    )
                except SentimentError as exc:
                    self._log.warning("stocktwits Haiku failed for %s: %s", symbol, exc)
                    warnings.append(f"{symbol}: Haiku failed, native fallback ({exc})")
                else:
                    t = tallies.get(symbol, {})
                    sentiment = Sentiment(
                        method="haiku",
                        bullish=int(t.get("bullish", 0)),
                        bearish=int(t.get("bearish", 0)),
                        neutral=int(t.get("neutral", 0)),
                        native=native,
                    )
        if sentiment is None:
            # Below the gate, no Anthropic client, or Haiku failed -> native is primary.
            sentiment = Sentiment(
                method="native", bullish=nat_bull, bearish=nat_bear, native=native
            )

        return NormalizedRecord(
            watchlist=watchlist.name,
            scan_mode=context.scan_mode,
            canonical_ts=context.canonical_ts,
            window=window,
            source=SOURCE_NAME,
            ticker=symbol,
            matched_by=["cashtag"],  # StockTwits is cashtag-native
            metrics=Metrics(mention_count=n),
            sentiment=sentiment,
            flags=[],
        )

__all__ = [
    "BROWSER_UA",
    "SOURCE_NAME",
    "STREAM_URL",
    "TRENDING_URL",
    "StockTwitsBlocked",
    "StockTwitsClient",
    "StockTwitsSource",
    "native_tag",
]
