"""Configuration: .env auto-load plus resolved paths / log level.

Order 1 requires NO credentials — the spine loads watchlists and derives windows
offline. Source keys (Finnhub, Anthropic) become required at their plugin's
invocation, never at spine startup. A missing .env no-ops; the
daemon falls through to a loud `ConfigError` only if a genuinely-required value is
ever absent.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# The daemon's .env and watchlists/ sit next to pyproject.toml, one level above
# the package directory. Resolved from the module location so they load
# regardless of cwd.
_PACKAGE_DIR = Path(__file__).resolve().parent
_DAEMON_ROOT = _PACKAGE_DIR.parent
_DOTENV_PATH = _DAEMON_ROOT / ".env"

REDACTED = "***REDACTED***"


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _load_dotenv(path: Path | None = None) -> None:
    """Load the daemon's .env, filling only the gaps (shell vars win).

    override=False means a key already set in the shell wins over the .env. A
    missing file silently no-ops — the desired graceful behavior.
    """
    load_dotenv(path if path is not None else _DOTENV_PATH, override=False)


def _default_watchlists_dir() -> Path:
    return _DAEMON_ROOT / "watchlists"


def _package_data_dir() -> Path:
    return _PACKAGE_DIR / "data"


def _default_company_names_path() -> Path:
    return _package_data_dir() / "company_names.txt"


def _default_common_words_path() -> Path:
    return _package_data_dir() / "common_words.txt"


def _default_slang_blacklist_path() -> Path:
    return _package_data_dir() / "slang_blacklist.txt"


def _default_state_dir() -> Path:
    # Mutable run state (the baseline DB) lives outside the package, next to .env.
    return _DAEMON_ROOT / "state"


def _default_baseline_db_path() -> Path:
    return _default_state_dir() / "baseline.sqlite3"


def _default_archive_root() -> Path:
    return _DAEMON_ROOT / "archive"


def _default_history_root() -> Path:
    # Raw-scrape text dumps (Order 19) — headlines / StockTwits / Twitter per scan, gitignored.
    return _DAEMON_ROOT / "history"


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """Comma-separated env list -> uppercased tuple (e.g. CHATTER_TWITTER_PRIORITY=NVDA,MU).
    Blank/absent -> the default. Empty items are dropped."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return tuple(s.strip().upper() for s in raw.split(",") if s.strip())


DEFAULT_USER_AGENT = "chatter-daemon/0.1"

# Real tickers that collide with common words — the wordlist filter's exception
# list (a bare token here survives common-word rejection). Lifted from BizDaemon.
DEFAULT_WORD_TICKER_ALLOWLIST = frozenset({"NOW", "META", "CORN"})

# Haiku sentiment (repointed from Reddit comments to StockTwits message bodies,
# Order 9). Model id pinned here, verified live via the claude-api skill at build time.
HAIKU_MODEL_ID = "claude-haiku-4-5"
# Order 19: the prose SUMMARIES (Finnhub named-news + Twitter commentary) run on Sonnet for
# sharper synthesis of specific figures/catalysts; stance/classification stays on Haiku
# (structured tally, cheaper). ~3x the summary cost (~+$0.18/scan). Model id via claude-api.
DEFAULT_SUMMARY_MODEL = "claude-sonnet-4-6"
DEFAULT_SENTIMENT_MIN_MENTIONS = 3
# Order 12: Haiku-on-StockTwits-bodies DEMOTED — OFF by default (the free sentiment-API
# aggregate supersedes it). Opt in for corroboration only; Haiku stays ON for /smg/.
DEFAULT_STOCKTWITS_HAIKU = False
# Per-source summary cost backstop (Finnhub named-news + Twitter commentary). Set to $2 on
# 2026-07-09 (Mando: $5 a little high, $2 an acceptable ceiling) — typical spend is
# ~$0.10-0.30/source (higher on Sonnet), so this stays pure headroom: a runaway-bug guard,
# NOT a routine limiter. Env-tunable via CHATTER_SUMMARY_COST_CAP.
DEFAULT_SUMMARY_COST_CAP_USD = 2.0

# Order 17 — Twitter/X cashtag source (subprocess `twitter` CLI). OFF by default until a
# live cert on the host that HAS the CLI (the build host does not). MIN_TWEETS_HAIKU=3 for
# parity with DEFAULT_SENTIMENT_MIN_MENTIONS; MIN_LIKES is a low positive floor (raw
# `latest` search is ~80% zero-engagement spam). WINDOW_HOURS is enforced PRECISELY
# in-process — the CLI's --since is only date-granular.
DEFAULT_TWITTER_ENABLED = False
DEFAULT_TWITTER_WINDOW_HOURS = 24
DEFAULT_TWITTER_MAX_PER_TICKER = 50
DEFAULT_TWITTER_MIN_TWEETS_HAIKU = 3  # parity with DEFAULT_SENTIMENT_MIN_MENTIONS
DEFAULT_TWITTER_MIN_LIKES = 2  # low positive floor — drops zero-engagement spam
DEFAULT_TWITTER_BINARY = "twitter"
# Per-search subprocess timeout. 8s fail-fasts a throttled search (a real one returns in ~2-5s)
# instead of hanging — the priority+cap (Order 21) keeps the scan under quota so throttling is
# rare, but a boundary search that slow-walks still fails fast. Env-tunable (CHATTER_TWITTER_TIMEOUT).
DEFAULT_TWITTER_TIMEOUT_S = 8.0
# Seconds between per-ticker searches — X rate-limits fast bursts. CALIBRATED 2026-07-09:
# 12/12 clean at 5s vs ~25/45 unpaced; 45 tickers ~= a 4-min Twitter phase. Env-tunable.
DEFAULT_TWITTER_PACE_S = 5.0
# Order 19: drop promotional / follow-bait / cashtag-stuffed tweets before stance + summary
# (raw `latest` is ~80% promo). ON by default; env CHATTER_TWITTER_DROP_PROMO=0 to keep raw.
DEFAULT_TWITTER_DROP_PROMO = True

# Order 21 — priority + cap. X meters authenticated search per-account (~25 requests/rolling
# window), so 45 per-ticker searches always throttled the alphabetical tail. OR-batching was
# tried and REJECTED (live cert 2026-07-10): a `$A OR $B OR ...` latest search returns a small
# low-engagement shared page — ~1 usable tweet/name vs ~15-34 for a solo search. So SOLO
# per-ticker searches stay (quality), but the queue is reordered PRIORITY-first (must-have names
# always land within the budget) and capped to the top-N that fit the quota (MAX_TICKERS). Names
# beyond the cap get no Twitter that scan (logged, never silent). For the barber_growth deploy:
# CHATTER_TWITTER_PRIORITY=NVDA,MU,... and CHATTER_TWITTER_MAX_TICKERS=25.
DEFAULT_TWITTER_PRIORITY: tuple[str, ...] = ()  # symbols searched first (CHATTER_TWITTER_PRIORITY)
DEFAULT_TWITTER_MAX_TICKERS = 0  # 0 = cover all; >0 = only the top-N in priority order

# CH-SRC-1 — Yahoo Finance per-ticker RSS (fresh headline supplement, keyless). Yahoo's ?s= feed
# is a MIXED market feed (recon 2026-07-15: ~3-9/20 items on-ticker), so the source relevance-
# filters (title+desc) and dedups vs Finnhub via prior_records — its value is the ~0.2-1h latency
# edge, not volume. Yahoo deprecates SILENTLY (200 + stale/empty): zero items scan-wide -> source
# error; a freshest item older than STALE_AFTER_H -> a loud staleness warning.
DEFAULT_YAHOO_ENABLED = True
DEFAULT_YAHOO_MAX_ITEMS = 20        # Yahoo serves ~20 items/feed — the per-ticker relevance pool
DEFAULT_YAHOO_STALE_AFTER_H = 48    # freshest item older than this scan-wide -> stale warning
# Relevance matches the TITLE only (not the blurb — a MU headline whose description lists 8 tickers
# would otherwise attribute to all 8, the main cross-ticker duplicate source). ROUNDUP_MAX then
# drops a headline whose title itself names >= this many watchlist tickers (a "Dow movers" roundup,
# low per-ticker signal) — 0 disables it. Mirrors the Twitter >=5-cashtag promo filter.
DEFAULT_YAHOO_ROUNDUP_MAX = 4

# CH-SRC-1 — Finnhub relevance gate. Finnhub's company-news API cross-tags peer/macro stories onto
# every large-cap: only ~23% of the heads it returns for symbol=T actually NAME T in the title, and
# 35% of headline slots are cross-ticker duplicates (measured live 2026-07-15). A count-threshold
# roundup filter is inert here — the dupes name <=3 tickers, not many. The fix is a per-ticker title
# gate: keep a head under T only if its title names T (full alias map, incl name_match:false names
# like "micron" that news headlines can trust). Live: dupes 35%->8%, ~67% of heads dropped (verified
# cross-tag noise, no ticker zeroed), MU coverage restored. This also trims mention_count (feeds
# ranking + the anomaly baseline) — a one-time, more-honest baseline shift. 0/False disables it.
DEFAULT_FINNHUB_RELEVANCE_GATE = True

# CH-SRC-1 — Alpha Vantage NEWS_SENTIMENT (per-ticker news-sentiment axis, KEYED). OFF unless a
# key is present (claim a free one at alphavantage.co; put it in .env as ALPHAVANTAGE_API_KEY or
# AV_KEY). One call/scan with limit=1000 covers the watchlist under the 25/day free cap. The
# IN-BAND ERROR GUARD is mandatory — AV returns errors as HTTP 200 with an Information / Note /
# Error Message body. RELEVANCE_MIN gates low-relevance ticker mentions out as noise pre-aggregate.
DEFAULT_AV_LIMIT = 1000
DEFAULT_AV_RELEVANCE_MIN = 0.1     # drop AV ticker mentions below this relevance_score (tune live)
DEFAULT_AV_SORT = "LATEST"

# Order 7 — baseline store, archive, anomaly tunables.
DEFAULT_BASELINE_WINDOW = 20  # K trailing observations in a baseline
DEFAULT_BASELINE_MIN_OBS = 5  # N_min before a z-score is meaningful (else `building`)
DEFAULT_SPIKE_Z = 2.0  # count-source spike threshold (z-score)
# Per-source min-volume floors: low-magnitude sources (Finnhub headlines, /smg/)
# need low floors or a 2->8 jump on a quiet name z-scores huge off noise.
# All tunable at live smoke.
DEFAULT_SOURCE_FLOORS: dict[str, int] = {
    "finnhub_news": 3,
    "smg": 3,
    "stocktwits": 10,
}

# Order 8 — ATTENTION discovery (Phase 1 calibration).
DEFAULT_UNIVERSE_TTL_S = 86_400  # 24h Finnhub symbol cache

# Order 8 Phase 2 — per-surface admit floors. /smg/ = 3 is CALIBRATED from the live
# pull (150 tickers / 341 mentions; floor 3 keeps the ~25-name head, drops the junk
# tail). WSB / StockTwits floors are deferred placeholders — set on first live pull.
DEFAULT_ATTENTION_FLOORS: dict[str, int] = {
    "smg_freq": 3,  # CALIBRATED
    "stocktwits_trending": 0,  # NO floor — the API's top-30 ranking IS the gate (Order 9)
}


@dataclass(frozen=True)
class Config:
    watchlists_dir: Path = field(default_factory=_default_watchlists_dir)
    log_level: str = "INFO"
    # Source credentials — optional at load, required at the owning plugin's fetch
    # (creds bind at invocation, not spine startup). From env only, never logged.
    finnhub_api_key: str | None = None
    user_agent: str = DEFAULT_USER_AGENT
    # Bundled seed data — the /smg/ matcher's company-name map + collision lists.
    company_names_path: Path = field(default_factory=_default_company_names_path)
    common_words_path: Path = field(default_factory=_default_common_words_path)
    slang_blacklist_path: Path = field(default_factory=_default_slang_blacklist_path)
    word_ticker_allowlist: frozenset[str] = DEFAULT_WORD_TICKER_ALLOWLIST
    # Haiku sentiment (StockTwits bodies, Order 9). Key optional at load, required at fetch.
    anthropic_api_key: str | None = None
    haiku_model_id: str = HAIKU_MODEL_ID
    summary_model: str = DEFAULT_SUMMARY_MODEL  # Order 19: Sonnet for the prose summaries
    sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS
    stocktwits_haiku_enabled: bool = DEFAULT_STOCKTWITS_HAIKU
    news_summary_cost_cap_usd: float = DEFAULT_SUMMARY_COST_CAP_USD
    # Order 17 — Twitter/X cashtag source (gated OFF by default).
    twitter_enabled: bool = DEFAULT_TWITTER_ENABLED
    twitter_window_hours: int = DEFAULT_TWITTER_WINDOW_HOURS
    twitter_max_per_ticker: int = DEFAULT_TWITTER_MAX_PER_TICKER
    twitter_min_tweets_haiku: int = DEFAULT_TWITTER_MIN_TWEETS_HAIKU
    twitter_min_likes: int = DEFAULT_TWITTER_MIN_LIKES
    twitter_binary: str = DEFAULT_TWITTER_BINARY
    twitter_timeout_s: float = DEFAULT_TWITTER_TIMEOUT_S
    twitter_pace_s: float = DEFAULT_TWITTER_PACE_S
    twitter_drop_promo: bool = DEFAULT_TWITTER_DROP_PROMO  # Order 19: strip promo/spam
    # Order 21 — priority-first queue + top-N cap (beat X's per-account quota on solo searches).
    twitter_priority: tuple[str, ...] = DEFAULT_TWITTER_PRIORITY
    twitter_max_tickers: int = DEFAULT_TWITTER_MAX_TICKERS
    # CH-SRC-1 — Yahoo per-ticker RSS (fresh supplement, keyless) + Alpha Vantage news-sentiment.
    yahoo_enabled: bool = DEFAULT_YAHOO_ENABLED
    yahoo_max_items: int = DEFAULT_YAHOO_MAX_ITEMS
    yahoo_stale_after_h: int = DEFAULT_YAHOO_STALE_AFTER_H
    yahoo_roundup_max: int = DEFAULT_YAHOO_ROUNDUP_MAX
    finnhub_relevance_gate: bool = DEFAULT_FINNHUB_RELEVANCE_GATE
    alphavantage_api_key: str | None = None  # from env only, never logged (see secrets())
    av_relevance_min: float = DEFAULT_AV_RELEVANCE_MIN
    av_limit: int = DEFAULT_AV_LIMIT
    av_sort: str = DEFAULT_AV_SORT
    # Order 7 — baseline store, run archive, anomaly tunables.
    history_root: Path = field(default_factory=_default_history_root)  # Order 19: raw dumps
    baseline_db_path: Path = field(default_factory=_default_baseline_db_path)
    archive_root: Path = field(default_factory=_default_archive_root)
    baseline_window: int = DEFAULT_BASELINE_WINDOW
    baseline_min_obs: int = DEFAULT_BASELINE_MIN_OBS
    spike_z_threshold: float = DEFAULT_SPIKE_Z
    source_floors: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_SOURCE_FLOORS)
    )
    # Order 8 — ATTENTION discovery (Phase 1).
    universe_cache_ttl_s: int = DEFAULT_UNIVERSE_TTL_S
    symbol_fallback_path: Path | None = None  # optional static US-symbol fallback
    attention_floors: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_ATTENTION_FLOORS)
    )

    def secrets(self) -> tuple[str, ...]:
        """Values to scrub from log output."""
        return tuple(
            s
            for s in (
                self.finnhub_api_key,
                self.anthropic_api_key,
                self.alphavantage_api_key,
            )
            if s
        )

    @classmethod
    def from_env(cls, *, dotenv_path: Path | None = None) -> "Config":
        # Load .env first so it fills keys absent from the shell; shell wins.
        _load_dotenv(dotenv_path)
        raw_dir = os.environ.get("CHATTER_WATCHLISTS_DIR", "").strip()
        watchlists_dir = Path(raw_dir) if raw_dir else _default_watchlists_dir()
        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        finnhub = os.environ.get("FINNHUB_API_KEY", "").strip() or None
        user_agent = os.environ.get("CHATTER_USER_AGENT", "").strip() or DEFAULT_USER_AGENT
        return cls(
            watchlists_dir=watchlists_dir,
            log_level=log_level,
            finnhub_api_key=finnhub,
            user_agent=user_agent,
            company_names_path=_env_path("CHATTER_COMPANY_NAMES", _default_company_names_path()),
            common_words_path=_env_path("CHATTER_COMMON_WORDS", _default_common_words_path()),
            slang_blacklist_path=_env_path("CHATTER_SLANG_BLACKLIST", _default_slang_blacklist_path()),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", "").strip() or None,
            sentiment_min_mentions=_env_int(
                "CHATTER_SENTIMENT_MIN", DEFAULT_SENTIMENT_MIN_MENTIONS
            ),
            stocktwits_haiku_enabled=_env_bool(
                "CHATTER_STOCKTWITS_HAIKU", DEFAULT_STOCKTWITS_HAIKU
            ),
            news_summary_cost_cap_usd=_env_float(
                "CHATTER_SUMMARY_COST_CAP", DEFAULT_SUMMARY_COST_CAP_USD
            ),
            twitter_enabled=_env_bool("CHATTER_TWITTER_ENABLED", DEFAULT_TWITTER_ENABLED),
            twitter_window_hours=_env_int(
                "CHATTER_TWITTER_WINDOW_HOURS", DEFAULT_TWITTER_WINDOW_HOURS
            ),
            twitter_max_per_ticker=_env_int(
                "CHATTER_TWITTER_MAX_PER_TICKER", DEFAULT_TWITTER_MAX_PER_TICKER
            ),
            twitter_min_tweets_haiku=_env_int(
                "CHATTER_TWITTER_MIN_TWEETS_HAIKU", DEFAULT_TWITTER_MIN_TWEETS_HAIKU
            ),
            twitter_min_likes=_env_int("CHATTER_TWITTER_MIN_LIKES", DEFAULT_TWITTER_MIN_LIKES),
            twitter_binary=os.environ.get("CHATTER_TWITTER_BINARY", "").strip()
            or DEFAULT_TWITTER_BINARY,
            twitter_timeout_s=_env_float("CHATTER_TWITTER_TIMEOUT", DEFAULT_TWITTER_TIMEOUT_S),
            twitter_pace_s=_env_float("CHATTER_TWITTER_PACE", DEFAULT_TWITTER_PACE_S),
            twitter_drop_promo=_env_bool("CHATTER_TWITTER_DROP_PROMO", DEFAULT_TWITTER_DROP_PROMO),
            twitter_priority=_env_list("CHATTER_TWITTER_PRIORITY", DEFAULT_TWITTER_PRIORITY),
            twitter_max_tickers=_env_int("CHATTER_TWITTER_MAX_TICKERS", DEFAULT_TWITTER_MAX_TICKERS),
            yahoo_enabled=_env_bool("CHATTER_YAHOO_ENABLED", DEFAULT_YAHOO_ENABLED),
            yahoo_max_items=_env_int("CHATTER_YAHOO_MAX_ITEMS", DEFAULT_YAHOO_MAX_ITEMS),
            yahoo_stale_after_h=_env_int("CHATTER_YAHOO_STALE_AFTER_H", DEFAULT_YAHOO_STALE_AFTER_H),
            yahoo_roundup_max=_env_int("CHATTER_YAHOO_ROUNDUP_MAX", DEFAULT_YAHOO_ROUNDUP_MAX),
            finnhub_relevance_gate=_env_bool(
                "CHATTER_FINNHUB_RELEVANCE_GATE", DEFAULT_FINNHUB_RELEVANCE_GATE
            ),
            alphavantage_api_key=(
                os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
                or os.environ.get("AV_KEY", "").strip()
                or None
            ),
            av_relevance_min=_env_float("CHATTER_AV_RELEVANCE_MIN", DEFAULT_AV_RELEVANCE_MIN),
            av_limit=_env_int("CHATTER_AV_LIMIT", DEFAULT_AV_LIMIT),
            av_sort=os.environ.get("CHATTER_AV_SORT", "").strip() or DEFAULT_AV_SORT,
            summary_model=os.environ.get("CHATTER_SUMMARY_MODEL", "").strip() or DEFAULT_SUMMARY_MODEL,
            baseline_db_path=_env_path("CHATTER_BASELINE_DB", _default_baseline_db_path()),
            archive_root=_env_path("CHATTER_ARCHIVE_ROOT", _default_archive_root()),
            history_root=_env_path("CHATTER_HISTORY_ROOT", _default_history_root()),
            baseline_window=_env_int("CHATTER_BASELINE_WINDOW", DEFAULT_BASELINE_WINDOW),
            baseline_min_obs=_env_int("CHATTER_BASELINE_MIN_OBS", DEFAULT_BASELINE_MIN_OBS),
            spike_z_threshold=_env_float("CHATTER_SPIKE_Z", DEFAULT_SPIKE_Z),
            universe_cache_ttl_s=_env_int("CHATTER_UNIVERSE_TTL", DEFAULT_UNIVERSE_TTL_S),
            symbol_fallback_path=(
                Path(os.environ["CHATTER_SYMBOL_FALLBACK"].strip())
                if os.environ.get("CHATTER_SYMBOL_FALLBACK", "").strip()
                else None
            ),
        )


class _RedactingFilter(logging.Filter):
    """Replace any occurrence of a known secret value with REDACTED.

    Defense-in-depth: the shared http_client already redacts token= in URLs at
    emit time, but this catches any secret that slips into another log record.
    The §A logger-DI fix routes shared-module logs (fourchan_fetch) under this
    `chatter_daemon` logger so this filter covers them too.
    """

    def __init__(self, secrets: tuple[str, ...]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s)

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        redacted = msg
        for secret in self._secrets:
            if secret in redacted:
                redacted = redacted.replace(secret, REDACTED)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


def configure_logging(config: Config) -> logging.Logger:
    """Configure the daemon logger once, to stderr, with secret redaction. Idempotent."""
    logger = logging.getLogger("chatter_daemon")
    logger.setLevel(config.log_level)
    if not logger.handlers:
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        handler.addFilter(_RedactingFilter(config.secrets()))
        logger.addHandler(handler)
    logger.propagate = False
    return logger
