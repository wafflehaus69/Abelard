"""Twitter/X cashtag source (Order 17) — the FIRST subprocess-based source.

Per active watchlist ticker, search recent cashtag tweets via the external `twitter`
CLI (a subprocess), filter the noise, and classify stance with Haiku. Structurally it
mirrors StockTwits (its own per-ticker loop + its own degrade policy) and /smg/ for
SENTIMENT (no native stance — `method` is `haiku` or `none`, never `native`).

Transport is the `twitter` CLI invoked as a subprocess. The runner is INJECTED (real
`subprocess.run` in production, a fake in tests) so the whole source is hermetic — no
real subprocess/network in the suite.

  - AUTH IS AMBIENT. The CLI reads `TWITTER_AUTH_TOKEN` / `TWITTER_CT0` from the
    environment ITSELF. This module never reads, handles, or logs those values, and
    never logs the argv (which could otherwise leak query context). The child inherits
    this process's env (`env=None`), so the cookies flow to the CLI untouched.
  - DEGRADE-CLEAN per ticker. A non-zero exit / timeout / empty / non-JSON stdout raises
    `TwitterBlocked`; the source logs a warning and moves on. A ticker with no readable
    result is dropped and the surface degrades (`ok=False` → `degraded`); the other
    sources carry the scan. A hard config failure (binary absent / wrong major version)
    is a loud `TwitterCliError` from the one-time startup smoke — the orchestrator
    isolates it (never a silent skip, never a crash of the whole scan).

TODO(cert): the `twitter` CLI is ABSENT on the build host, so every flag spelling and
every `--json` field name below is UNVERIFIED — taken from the build order's
transcription. They are ALL isolated in the `_CLI` / `_FIELDS_*` constants + a defensive
multi-name parser, so the first live certification (on the host that HAS the CLI) is a
one-constant fix, not a rewrite. Confirm against `twitter search --help` and one real
`--json` object before flipping `CHATTER_TWITTER_ENABLED=1`.
"""

from __future__ import annotations

import json
import logging
import random
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Callable

from ..config import (
    DEFAULT_SENTIMENT_MIN_MENTIONS,
    DEFAULT_TWITTER_BINARY,
    DEFAULT_TWITTER_MAX_PER_TICKER,
    DEFAULT_TWITTER_MIN_LIKES,
    DEFAULT_TWITTER_TIMEOUT_S,
    DEFAULT_TWITTER_WINDOW_HOURS,
    HAIKU_MODEL_ID,
)
from ..schema import CostTelemetry, Metrics, NormalizedRecord, ObservedWindow, Sentiment
from ..sentiment import AnthropicProvider, SentimentError, classify_stance
from ..watchlist import WatchlistConfig
from ..windows import iso_z
from .base import ScanContext, SourceResult

SOURCE_NAME = "twitter"
WINDOW_LABEL = "24h"  # the record's nominal window; observed_window carries the real span

# --- CLI CONTRACT (TODO(cert): UNVERIFIED — isolated for a one-line fix) ---------------
# Flags, per the build order's transcription of:
#   twitter search "$SYM" -t latest --since <YYYY-MM-DD> --exclude links
#     --min-likes <N> -n <MAX> --json
_SEARCH_SUBCMD = "search"
_FLAG_TWEET_TYPE = ("-t", "latest")
_FLAG_SINCE = "--since"  # DATE granularity (YYYY-MM-DD) — precise window done in-process
_FLAG_EXCLUDE_LINKS = ("--exclude", "links")
_FLAG_MIN_LIKES = "--min-likes"
_FLAG_MAX = "-n"
_FLAG_JSON = "--json"
_FLAG_VERSION = "--version"
# Per-tweet `--json` field names — tried in order, first present wins (defensive).
_FIELDS_ID = ("id", "id_str", "tweetId", "rest_id")
_FIELDS_TEXT = ("text", "fullText", "full_text", "rawContent", "content")
_FIELDS_CREATED = ("createdAtISO", "createdAt", "created_at", "date", "timestamp")
_FIELDS_LIKES = ("likes", "likeCount", "favoriteCount", "favorite_count", "favorites")
# Startup-smoke version gate. None = lenient (must run + emit a parseable version, which
# is logged); set to an int to hard-require that major. TODO(cert): set once known.
_EXPECTED_MAJOR: int | None = None

# Courtesy delay between per-ticker subprocess calls (jittered; injected as a no-op in
# tests). A subprocess search is heavier than an HTTP GET — be a polite client.
COURTESY_MIN_S = 0.3
COURTESY_MAX_S = 0.8

_URL_RE = re.compile(r"https?://\S+")
_NONWORD_RE = re.compile(r"[^a-z0-9]+")
_VERSION_RE = re.compile(r"(\d+)(?:\.\d+)*")


class TwitterBlocked(RuntimeError):
    """Soft, degrade-clean per-ticker failure: non-zero exit / timeout / empty / non-JSON
    stdout. The source logs it, drops that ticker, and carries on (mirror of
    StockTwitsBlocked)."""


class TwitterCliError(RuntimeError):
    """Hard startup failure: the `twitter` binary is absent or reports a wrong/unparseable
    version. Raised loudly by the one-time smoke — NOT a silent skip. The orchestrator
    isolates it (source ok=False + degraded), so it never crashes the whole scan."""


def _courtesy_sleep() -> None:
    time.sleep(random.uniform(COURTESY_MIN_S, COURTESY_MAX_S))


def _subprocess_runner(argv: list[str], timeout: float) -> tuple[int, bytes]:
    """Default runner: invoke the CLI, return (returncode, stdout_bytes). Inherits this
    process's env (`env=None`) so the CLI reads the ambient TWITTER_* cookies itself —
    this function never touches or logs them, and never logs argv. Raises
    FileNotFoundError if the binary is absent, subprocess.TimeoutExpired on timeout."""
    proc = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
    return proc.returncode, proc.stdout


def _first_field(obj: dict[str, Any], names: tuple[str, ...]) -> Any:
    """First present, non-None value among candidate field names (defensive parse over an
    unverified `--json` shape)."""
    for n in names:
        v = obj.get(n)
        if v is not None:
            return v
    return None


def _parse_iso_unix(s: Any) -> int | None:
    """ISO-8601 -> unix seconds, or None if absent/unparseable. Treats a naive stamp as
    UTC. Used both for the precise window filter and observed_window ordering."""
    if not isinstance(s, str) or not s.strip():
        return None
    txt = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _dedupe_key(text: str) -> str:
    """Normalized text key for near-identical dedupe: lowercase, strip URLs + punctuation,
    collapse whitespace. Copypasta / RT-of-same-text collapse to one survivor."""
    t = _URL_RE.sub(" ", text.lower())
    t = _NONWORD_RE.sub(" ", t)
    return " ".join(t.split())


def _parse_major(version_text: str) -> int | None:
    m = _VERSION_RE.search(version_text or "")
    return int(m.group(1)) if m else None


def _parse_tweets(stdout: bytes) -> list[dict[str, Any]]:
    """Decode subprocess stdout (UTF-8 obligation) and parse `--json` into a list of tweet
    dicts. Handles a JSON array, an NDJSON stream (one object per line), or a wrapper
    object ({tweets|data|results: [...]}). Empty or non-JSON -> TwitterBlocked."""
    text = stdout.decode("utf-8", "strict").strip()
    if not text:
        raise TwitterBlocked("empty stdout")
    try:
        obj: Any = json.loads(text)
    except ValueError:
        # NDJSON fallback: one JSON object per line.
        out: list[dict[str, Any]] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except ValueError as exc:
                raise TwitterBlocked(f"non-JSON stdout: {exc}") from exc
            if isinstance(item, dict):
                out.append(item)
        return out
    if isinstance(obj, list):
        return [t for t in obj if isinstance(t, dict)]
    if isinstance(obj, dict):
        for key in ("tweets", "data", "results"):
            v = obj.get(key)
            if isinstance(v, list):
                return [t for t in v if isinstance(t, dict)]
    raise TwitterBlocked("unexpected --json shape (not a list / NDJSON / wrapped list)")


class TwitterClient:
    """Wraps subprocess calls to the `twitter` CLI. One public search method + a startup
    smoke. The runner is injected (fake in tests) exactly as StockTwitsClient injects its
    transport, so the parse/smoke/blocked-mapping logic is exercised with zero real
    subprocess."""

    def __init__(
        self,
        *,
        binary: str = DEFAULT_TWITTER_BINARY,
        timeout: float = DEFAULT_TWITTER_TIMEOUT_S,
        runner: Callable[[list[str], float], tuple[int, bytes]] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._binary = binary
        self._timeout = timeout
        self._runner = runner or _subprocess_runner
        self._log = logger or logging.getLogger("chatter_daemon.twitter")

    def smoke(self) -> str:
        """Startup smoke: the binary exists and emits a parseable version. Fail-loud
        (`TwitterCliError`) if absent / non-zero / unparseable / wrong major — never a
        silent skip. Returns the version string (logged by the caller)."""
        try:
            rc, out = self._runner([self._binary, _FLAG_VERSION], self._timeout)
        except FileNotFoundError as exc:
            raise TwitterCliError(
                f"`{self._binary}` not found — install the twitter CLI or set "
                f"CHATTER_TWITTER_BINARY"
            ) from exc
        except Exception as exc:  # timeout / OSError — the smoke could not run
            raise TwitterCliError(f"`{self._binary} {_FLAG_VERSION}` failed: {exc}") from exc
        if rc != 0:
            raise TwitterCliError(f"`{self._binary} {_FLAG_VERSION}` exited {rc}")
        version = out.decode("utf-8", "replace").strip()
        major = _parse_major(version)
        if major is None:
            raise TwitterCliError(f"could not parse a version from {version!r}")
        if _EXPECTED_MAJOR is not None and major != _EXPECTED_MAJOR:
            raise TwitterCliError(
                f"twitter CLI major {major} != expected {_EXPECTED_MAJOR} ({version!r})"
            )
        return version

    def search(
        self, cashtag: str, *, since_iso: str, max_n: int, min_likes: int
    ) -> list[dict[str, Any]]:
        """Recent cashtag tweets for one symbol. `since_iso` is trimmed to a DATE for the
        CLI's date-granular --since; the source enforces the exact window in-process.
        Raises TwitterBlocked on non-zero exit / timeout / empty / non-JSON stdout."""
        argv = self._build_argv(cashtag, since_iso, max_n, min_likes)
        try:
            rc, out = self._runner(argv, self._timeout)
        except Exception as exc:  # timeout / OSError -> soft, degrade-clean block
            raise TwitterBlocked(
                f"search ${cashtag} failed (subprocess {type(exc).__name__})"
            ) from exc
        if rc != 0:
            raise TwitterBlocked(f"search ${cashtag} exited {rc}")
        return _parse_tweets(out)

    def _build_argv(
        self, cashtag: str, since_iso: str, max_n: int, min_likes: int
    ) -> list[str]:
        since_date = since_iso[:10]  # YYYY-MM-DD (the CLI --since is date-granular)
        return [
            self._binary,
            _SEARCH_SUBCMD,
            f"${cashtag}",
            *_FLAG_TWEET_TYPE,
            _FLAG_SINCE,
            since_date,
            *_FLAG_EXCLUDE_LINKS,
            _FLAG_MIN_LIKES,
            str(min_likes),
            _FLAG_MAX,
            str(max_n),
            _FLAG_JSON,
        ]


class TwitterSource:
    """Watchlist Twitter/X cashtag source (Order 17). Own per-ticker loop; per-ticker
    degrade; Haiku-or-none sentiment (no native stance — mirrors /smg/). ENABLED is gated
    OFF by default in config until a live cert."""

    name = SOURCE_NAME

    def __init__(
        self,
        *,
        binary: str = DEFAULT_TWITTER_BINARY,
        timeout_s: float = DEFAULT_TWITTER_TIMEOUT_S,
        window_hours: int = DEFAULT_TWITTER_WINDOW_HOURS,
        max_per_ticker: int = DEFAULT_TWITTER_MAX_PER_TICKER,
        min_tweets_haiku: int = DEFAULT_SENTIMENT_MIN_MENTIONS,
        min_likes: int = DEFAULT_TWITTER_MIN_LIKES,
        anthropic_api_key: str | None = None,
        haiku_model: str = HAIKU_MODEL_ID,
        client: TwitterClient | None = None,
        anthropic_client: Any | None = None,
        sleep: Callable[[], None] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("chatter_daemon.twitter")
        self._binary = binary
        self._timeout = timeout_s
        self._window_hours = window_hours
        self._max_per_ticker = max_per_ticker
        self._min_tweets_haiku = min_tweets_haiku
        self._min_likes = min_likes
        self._client = client  # TwitterClient; built (real subprocess) if absent
        self._anthropic = AnthropicProvider(
            api_key=anthropic_api_key, client=anthropic_client, logger=self._log
        )
        self._haiku_model = haiku_model
        self._sleep = sleep if sleep is not None else _courtesy_sleep

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        client = self._client or TwitterClient(
            binary=self._binary, timeout=self._timeout, logger=self._log
        )
        # Startup smoke ONCE — fail-loud if the binary is absent/wrong (orchestrator
        # isolates the raise; the scan's other sources carry on).
        version = client.smoke()
        self._log.info("twitter CLI ok: %s", version)

        window = context.windows[WINDOW_LABEL]
        # since is derived from the run's canonical clock (already stamped by the
        # orchestrator) — the source NEVER reads the clock itself.
        cutoff_unix = context.canonical_unix - self._window_hours * 3600
        since_iso = iso_z(cutoff_unix)

        records: list[NormalizedRecord] = []
        warnings: list[str] = []
        blocked: list[str] = []
        cost = CostTelemetry()
        actives = watchlist.active_tickers

        for i, spec in enumerate(actives):
            sym = spec.symbol
            if i:
                self._sleep()  # courtesy delay between per-ticker subprocess calls
            try:
                raw = client.search(
                    sym,
                    since_iso=since_iso,
                    max_n=self._max_per_ticker,
                    min_likes=self._min_likes,
                )
            except TwitterBlocked as exc:
                self._log.warning("twitter search blocked for %s: %s", sym, exc)
                blocked.append(sym)
                continue
            records.append(
                self._build_record(watchlist, context, window, sym, cutoff_unix, raw, cost, warnings)
            )

        # A ticker whose search was unreadable degrades the surface (ok=False flips the
        # envelope `degraded`); the rest ship. An EMPTY (but readable) search is an honest
        # zero record, not a block — mirrors StockTwits' empty-stream distinction.
        error = (
            f"{len(blocked)}/{len(actives)} Twitter unavailable (subprocess): "
            f"{', '.join(blocked)}"
            if blocked
            else None
        )
        return SourceResult(
            source=SOURCE_NAME, records=records, warnings=warnings, error=error, cost=cost
        )

    def _build_record(self, watchlist, context, window, symbol, cutoff_unix, raw, cost, warnings):
        survivors = self._filter(raw, cutoff_unix)
        if len(raw) >= 5 and not survivors:
            # Loud hint: a substantial result that ALL got filtered often means a cert
            # field-name mismatch (createdAt/likes), not a genuinely-empty ticker.
            warnings.append(
                f"{symbol}: {len(raw)} tweets returned but 0 survived filtering "
                f"(check cert field names / filters)"
            )

        observed = None
        if survivors:
            earliest = min(survivors, key=lambda s: s["created_unix"])["created_iso"]
            latest = max(survivors, key=lambda s: s["created_unix"])["created_iso"]
            observed = ObservedWindow(earliest=earliest, latest=latest)

        posts = [
            {"post_id": s["id"], "text": s["text"], "tickers": [symbol]}
            for s in survivors
            if s["text"].strip()
        ]
        sentiment = self._classify(symbol, posts, cost, warnings)

        return NormalizedRecord(
            watchlist=watchlist.name,
            scan_mode=context.scan_mode,
            canonical_ts=context.canonical_ts,
            window=window,
            source=SOURCE_NAME,
            ticker=symbol,
            matched_by=["cashtag"],  # Twitter is cashtag-native
            metrics=Metrics(mention_count=len(survivors)),
            sentiment=sentiment,
            observed_window=observed,
            flags=[],
        )

    def _filter(self, raw: list[dict[str, Any]], cutoff_unix: int) -> list[dict[str, Any]]:
        """Filter stack ON TOP of the CLI-side --exclude links / --min-likes:
          1. PRECISE window — drop tweets older than cutoff (the CLI --since is only a
             date, so a ~24h window can otherwise leak up to ~48h). Unparseable stamp ->
             drop (cannot confirm in-window).
          2. min-likes re-enforce (belt-and-suspenders). A MISSING likes field (unverified
             name) -> keep (trust the CLI's own --min-likes rather than nuke everything).
          3. near-identical text dedupe.
        """
        survivors: list[dict[str, Any]] = []
        seen: set[str] = set()
        for tw in raw:
            if not isinstance(tw, dict):
                continue
            created_iso = _first_field(tw, _FIELDS_CREATED)
            created_unix = _parse_iso_unix(created_iso)
            if created_unix is None or created_unix < cutoff_unix:
                continue  # out of window or unparseable stamp
            likes = _first_field(tw, _FIELDS_LIKES)
            if isinstance(likes, (int, float)) and likes < self._min_likes:
                continue  # below floor (None = unknown -> keep, trust the CLI)
            text = _first_field(tw, _FIELDS_TEXT) or ""
            if not isinstance(text, str):
                text = str(text)
            key = _dedupe_key(text)
            if key and key in seen:
                continue  # near-identical duplicate
            seen.add(key)
            tid = _first_field(tw, _FIELDS_ID)
            survivors.append(
                {
                    "id": str(tid) if tid is not None else f"tw-{len(survivors)}",
                    "text": text,
                    "created_iso": created_iso,
                    "created_unix": created_unix,
                }
            )
        return survivors

    def _classify(self, symbol, posts, cost, warnings):
        """Haiku stance over the surviving tweet bodies, gated above MIN_TWEETS_HAIKU.
        Twitter has no native tags, so it is Haiku-or-none (mirror of /smg/): below the
        floor, with no Anthropic key, or on a Haiku failure, method stays "none" (the
        count still ships). Gating is the SOURCE's job."""
        if len(posts) < self._min_tweets_haiku:
            return Sentiment(method="none")
        anthropic = self._anthropic.get()
        if anthropic is None:
            return Sentiment(method="none")
        try:
            tallies = classify_stance(
                posts=posts, client=anthropic, model=self._haiku_model, cost=cost
            )
        except SentimentError as exc:
            self._log.warning("twitter Haiku failed for %s: %s", symbol, exc)
            warnings.append(f"{symbol}: twitter Haiku failed ({exc})")
            return Sentiment(method="none")
        t = tallies.get(symbol.upper(), {})
        return Sentiment(
            method="haiku",
            bullish=int(t.get("bullish", 0)),
            bearish=int(t.get("bearish", 0)),
            neutral=int(t.get("neutral", 0)),
        )


__all__ = [
    "SOURCE_NAME",
    "TwitterBlocked",
    "TwitterClient",
    "TwitterCliError",
    "TwitterSource",
]
