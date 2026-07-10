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
from ..schema import (
    CostTelemetry,
    Metrics,
    NativeStance,
    NormalizedRecord,
    Sentiment,
    StockTwitsAggregate,
)
from ..sentiment import AnthropicProvider, SentimentError, classify_stance
from ..watchlist import WatchlistConfig
from .base import ScanContext, SourceResult

API_BASE = "https://api.stocktwits.com/api/2"
TRENDING_URL = f"{API_BASE}/trending/symbols.json"
STREAM_URL = API_BASE + "/streams/symbol/{symbol}.json"
# Undocumented internal sentiment gateway (Order 12) — different host, but the same TLS
# impersonation clears it. StockTwits' OWN full-stream aggregate (now-primary).
SENTIMENT_URL = "https://api-gw-prd.stocktwits.com/sentiment-api/v2/{symbol}/detail"

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

    def sentiment_detail(self, symbol: str) -> dict[str, Any]:
        """StockTwits' OWN computed sentiment aggregate (gateway, Order 12). Returns the
        raw `{data: {...}}` payload; `parse_sentiment_aggregate` pulls now/24h + the 1D
        participation. Raises StockTwitsBlocked on a CF wall / non-JSON / missing `data`.
        UNDOCUMENTED internal gateway (highest change-risk source) — callers degrade per
        ticker so the gateway is never a single point of failure."""
        data = self._get_json(SENTIMENT_URL.format(symbol=symbol), what=f"sentiment {symbol}")
        if not isinstance(data, dict) or not isinstance(data.get("data"), dict):
            raise StockTwitsBlocked(f"sentiment {symbol}: no `data` block (CF challenge?)")
        return data

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


# --- sentiment-API aggregate parsing (Order 12) -----------------------------------

_CONF_HIGH = 50  # valueNormalized split for the volume x participation confidence quadrants


def _agg_metric(block: Any) -> tuple[int, str | None] | None:
    """(valueNormalized:int, labelNormalized) for a LOADED metric block, else None. NEVER
    reads raw `value`/`label` — those are proven invertible (PLTR label EXTREMELY_BULLISH
    vs labelNormalized NEUTRAL; XOVR vol label EXTREMELY_HIGH vs labelNormalized LOW)."""
    if not isinstance(block, dict) or not block.get("loaded"):
        return None
    vn = block.get("valueNormalized")
    if not isinstance(vn, (int, float)):
        return None
    label = block.get("labelNormalized")
    return int(round(vn)), (label if isinstance(label, str) and label else None)


def _confidence(vol_norm: int | None, part_norm: int | None) -> str | None:
    """Volume x participation trust gate. Sentiment confidence rides on PARTICIPATION,
    not volume: a quiet name with high participation is a real small-crowd consensus; a
    loud name with low participation is a possible pump."""
    if vol_norm is None or part_norm is None:
        return None
    vhigh, phigh = vol_norm >= _CONF_HIGH, part_norm >= _CONF_HIGH
    if vhigh and phigh:
        return "high"           # genuine surge (BLZE: vol 98, part 74)
    if not vhigh and phigh:
        return "quiet"          # real but quiet — trustworthy small crowd (XOVR: vol 6, part 66)
    if not vhigh and not phigh:
        return "low"            # thin noise
    return "pump_suspect"       # high volume, low participation — possible bot/spam


def parse_sentiment_aggregate(raw: dict[str, Any]) -> StockTwitsAggregate | None:
    """Parse the sentiment-API payload into a StockTwitsAggregate. now-primary, 24h as
    baseline, `gap = now - 24h`. Consumes ONLY `now`/`24h` (sentiment + volume) and the
    `timeframes.1D` participation; IGNORES timeframes >= 1W (paywalled/stale) and the raw
    `value`/`label`. Returns None when nothing usable loaded (degrade-clean)."""
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return None
    sent = data.get("sentiment") if isinstance(data.get("sentiment"), dict) else {}
    vol = data.get("messageVolume") if isinstance(data.get("messageVolume"), dict) else {}

    s_now = _agg_metric(sent.get("now"))
    s_24h = _agg_metric(sent.get("24h"))
    sent_now_norm = s_now[0] if s_now else None
    sent_24h_norm = s_24h[0] if s_24h else None
    gap = (
        sent_now_norm - sent_24h_norm
        if sent_now_norm is not None and sent_24h_norm is not None
        else None
    )

    v_now = vol.get("now")
    vol_now_norm = vol_now_raw = vol_change = None
    if isinstance(v_now, dict) and v_now.get("loaded"):
        vn, vr, vc = v_now.get("valueNormalized"), v_now.get("value"), v_now.get("change")
        vol_now_norm = int(round(vn)) if isinstance(vn, (int, float)) else None
        vol_now_raw = int(vr) if isinstance(vr, (int, float)) else None  # raw count IS the real volume
        vol_change = float(vc) if isinstance(vc, (int, float)) else None

    # Participation lives in timeframes.1D (NOT a `now` block — the order's literal field
    # was off; 1D is < 1W so trustworthy, and the value matches the live page: BLZE 74,
    # XOVR 66).
    part_norm = None
    tfs = data.get("timeframes")
    tf1d = tfs.get("1D") if isinstance(tfs, dict) else None
    if isinstance(tf1d, dict):
        ps = tf1d.get("participationScore")
        if isinstance(ps, dict) and ps.get("loaded"):
            pv = ps.get("valueNormalized")
            part_norm = int(round(pv)) if isinstance(pv, (int, float)) else None

    if sent_now_norm is None and vol_now_norm is None:
        return None  # nothing usable -> degrade

    return StockTwitsAggregate(
        sent_now_norm=sent_now_norm,
        sent_now_label=s_now[1] if s_now else None,
        sent_24h_norm=sent_24h_norm,
        sent_24h_label=s_24h[1] if s_24h else None,
        sent_gap=gap,
        vol_now_norm=vol_now_norm,
        vol_now_raw=vol_now_raw,
        vol_change=vol_change,
        participation_norm=part_norm,
        confidence=_confidence(vol_now_norm, part_norm),
    )


class StockTwitsSource:
    """Watchlist StockTwits source (Order 12). PRIMARY = StockTwits' OWN sentiment-API
    aggregate (the gateway, parsed now-primary): the live `now` sentiment, the 24h
    baseline, the now-vs-24h gap (the spike signal), the REAL message volume (retiring
    the page-size 30), and a participation-gated confidence. Carried alongside:

      - NATIVE tags from the documented symbol-stream — free explicit-user-stance, AND
        the degrade-clean FALLBACK: the gateway dies, native tags still yield a read so
        the gateway is never a single point of failure.
      - HAIKU-on-bodies — DEMOTED (off by default). The free full-stream aggregate
        supersedes paying Haiku for a 30-message window; the gated path stays for opt-in
        corroboration only. The default scan spends ZERO Haiku on StockTwits.

    The reads are carried DISTINCTLY (`st_aggregate`, `sentiment.native`, optional
    Haiku); the daemon reconciles nothing (Abelard's job).

    DEGRADE-CLEAN per ticker: gateway 403/non-JSON -> aggregate null; stream walled ->
    native null; only when BOTH die is there no record (and the surface degrades).
    """

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        anthropic_api_key: str | None = None,
        haiku_model: str = HAIKU_MODEL_ID,
        sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS,
        haiku_enabled: bool = False,
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
        self._haiku_enabled = haiku_enabled  # Order 12: off by default (aggregate supersedes)
        self._sleep = sleep if sleep is not None else _courtesy_sleep

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext, **_: object) -> SourceResult:
        client = self._client or StockTwitsClient(logger=self._log)
        window = context.windows[WINDOW_LABEL]
        records: list[NormalizedRecord] = []
        warnings: list[str] = []
        blocked: list[str] = []
        raw_items: list[str] = []  # Order 19: message bodies for the history dump
        cost = CostTelemetry()
        actives = watchlist.active_tickers

        for i, spec in enumerate(actives):
            sym = spec.symbol
            if i:
                self._sleep()  # courtesy delay between tickers
            # 1. PRIMARY: StockTwits' own aggregate (the gateway).
            agg = None
            try:
                agg = parse_sentiment_aggregate(client.sentiment_detail(sym))
            except StockTwitsBlocked as exc:
                self._log.warning("stocktwits gateway blocked for %s: %s", sym, exc)
            # 2. FALLBACK + native tags: the documented symbol-stream.
            self._sleep()  # courtesy delay between the two calls
            messages = None
            try:
                messages = client.symbol_stream(sym)
            except StockTwitsBlocked as exc:
                self._log.warning("stocktwits stream blocked for %s: %s", sym, exc)
            if messages:
                raw_items.extend(
                    f"{sym}\t{m['body']}"
                    for m in messages
                    if isinstance(m.get("body"), str) and m["body"].strip()
                )
            if agg is None and messages is None:
                # BOTH paths dead -> honest "unread"; the surface degrades, others ship.
                blocked.append(sym)
                continue
            records.append(
                self._build_record(watchlist, context, window, sym, agg, messages, warnings, cost)
            )

        # A ticker with BOTH paths dead degrades the surface (ok=False flips the envelope
        # `degraded`); the rest carry on. Haiku soft-fallbacks (when enabled) stay in
        # `warnings` and do NOT flip degraded.
        error = (
            f"{len(blocked)}/{len(actives)} StockTwits unavailable (gateway+stream): "
            f"{', '.join(blocked)}"
            if blocked
            else None
        )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, error=error, cost=cost,
            raw_items=raw_items,
        )

    def _build_record(self, watchlist, context, window, symbol, agg, messages, warnings, cost):
        # Native tags + (optional) Haiku from the stream, when we got it.
        native = None
        nat_bull = nat_bear = nat_tagged = 0
        posts: list[dict[str, Any]] = []
        n_msgs = 0
        if messages is not None:
            n_msgs = len(messages)
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
                        {"post_id": str(m.get("id", f"st-{symbol}-{idx}")), "text": body, "tickers": [symbol]}
                    )
            native = NativeStance(bullish=nat_bull, bearish=nat_bear, tagged=nat_tagged, messages=n_msgs)

        sentiment: Sentiment | None = None
        # Haiku DEMOTED: only when explicitly enabled, above the floor, with bodies + a client.
        if self._haiku_enabled and n_msgs >= self._floor and posts:
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
            # Native tags primary when the stream worked; else no per-message stance
            # (the gateway aggregate is the read, carried in st_aggregate).
            if native is not None:
                sentiment = Sentiment(method="native", bullish=nat_bull, bearish=nat_bear, native=native)
            else:
                sentiment = Sentiment(method="none")

        # mention_count = the REAL volume (aggregate's raw count), retiring the page-size
        # 30. Fall back to the stream page size only when the gateway is dark.
        if agg is not None and agg.vol_now_raw is not None:
            mention_count = agg.vol_now_raw
        else:
            mention_count = n_msgs

        return NormalizedRecord(
            watchlist=watchlist.name,
            scan_mode=context.scan_mode,
            canonical_ts=context.canonical_ts,
            window=window,
            source=SOURCE_NAME,
            ticker=symbol,
            matched_by=["cashtag"],  # StockTwits is cashtag-native
            metrics=Metrics(mention_count=mention_count),
            sentiment=sentiment,
            st_aggregate=agg,
            flags=[],
        )


__all__ = [
    "BROWSER_UA",
    "SENTIMENT_URL",
    "SOURCE_NAME",
    "STREAM_URL",
    "TRENDING_URL",
    "StockTwitsBlocked",
    "StockTwitsClient",
    "StockTwitsSource",
    "native_tag",
    "parse_sentiment_aggregate",
]
