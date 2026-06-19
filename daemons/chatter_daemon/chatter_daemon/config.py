"""Configuration: .env auto-load plus resolved paths / log level.

Order 1 requires NO credentials — the spine loads watchlists and derives windows
offline. Source keys (Finnhub, Anthropic, Reddit) become required at their plugin's
invocation (Orders 4 / 6), never at spine startup. A missing .env no-ops; the
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


DEFAULT_USER_AGENT = "chatter-daemon/0.1"

# Real tickers that collide with common words — the wordlist filter's exception
# list (a bare token here survives common-word rejection). Lifted from BizDaemon.
DEFAULT_WORD_TICKER_ALLOWLIST = frozenset({"NOW", "META", "CORN"})

# Reddit (Order 6). Subreddits configurable; the Haiku model id is pinned here,
# verified live via the claude-api skill at build time (not from memory).
DEFAULT_SUBREDDITS = ("wallstreetbets", "stocks", "investing", "options")
HAIKU_MODEL_ID = "claude-haiku-4-5"
DEFAULT_SENTIMENT_MIN_MENTIONS = 3

# Order 7 — baseline store, archive, anomaly tunables.
DEFAULT_BASELINE_WINDOW = 20  # K trailing observations in a baseline
DEFAULT_BASELINE_MIN_OBS = 5  # N_min before a z-score is meaningful (else `building`)
DEFAULT_SPIKE_Z = 2.0  # count-source spike threshold (z-score)
DEFAULT_TREND_SPIKE_RATIO = 1.5  # Trends: interest_24h vs its trailing windows
# Per-source min-volume floors: low-magnitude sources (Finnhub headlines, /smg/)
# need low floors or a 2->8 jump on a quiet name z-scores huge off noise; Reddit
# runs high. All tunable at live smoke.
DEFAULT_SOURCE_FLOORS: dict[str, int] = {
    "finnhub_news": 3,
    "smg": 3,
    "reddit": 12,
    "stocktwits": 10,
}

# Order 8 — ATTENTION discovery (Phase 1 calibration).
DEFAULT_UNIVERSE_TTL_S = 86_400  # 24h Finnhub symbol cache
DEFAULT_ATTENTION_SUBREDDITS = ("wallstreetbets",)
DEFAULT_ATTENTION_POST_LIMIT = 100


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
    # Reddit + Haiku (Order 6). Creds optional at load, required at the plugin's fetch.
    anthropic_api_key: str | None = None
    reddit_client_id: str | None = None
    reddit_client_secret: str | None = None
    reddit_user_agent: str | None = None
    reddit_subreddits: tuple[str, ...] = DEFAULT_SUBREDDITS
    haiku_model_id: str = HAIKU_MODEL_ID
    sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS
    # Order 7 — baseline store, run archive, anomaly tunables.
    baseline_db_path: Path = field(default_factory=_default_baseline_db_path)
    archive_root: Path = field(default_factory=_default_archive_root)
    baseline_window: int = DEFAULT_BASELINE_WINDOW
    baseline_min_obs: int = DEFAULT_BASELINE_MIN_OBS
    spike_z_threshold: float = DEFAULT_SPIKE_Z
    trend_spike_ratio: float = DEFAULT_TREND_SPIKE_RATIO
    source_floors: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_SOURCE_FLOORS)
    )
    # Order 8 — ATTENTION discovery (Phase 1).
    universe_cache_ttl_s: int = DEFAULT_UNIVERSE_TTL_S
    symbol_fallback_path: Path | None = None  # optional static US-symbol fallback
    attention_subreddits: tuple[str, ...] = DEFAULT_ATTENTION_SUBREDDITS
    attention_post_limit: int = DEFAULT_ATTENTION_POST_LIMIT

    def secrets(self) -> tuple[str, ...]:
        """Values to scrub from log output."""
        return tuple(
            s
            for s in (
                self.finnhub_api_key,
                self.anthropic_api_key,
                self.reddit_client_secret,
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
        subreddits_raw = os.environ.get("CHATTER_SUBREDDITS", "").strip()
        subreddits = (
            tuple(s.strip() for s in subreddits_raw.split(",") if s.strip())
            if subreddits_raw
            else DEFAULT_SUBREDDITS
        )
        return cls(
            watchlists_dir=watchlists_dir,
            log_level=log_level,
            finnhub_api_key=finnhub,
            user_agent=user_agent,
            company_names_path=_env_path("CHATTER_COMPANY_NAMES", _default_company_names_path()),
            common_words_path=_env_path("CHATTER_COMMON_WORDS", _default_common_words_path()),
            slang_blacklist_path=_env_path("CHATTER_SLANG_BLACKLIST", _default_slang_blacklist_path()),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", "").strip() or None,
            reddit_client_id=os.environ.get("REDDIT_CLIENT_ID", "").strip() or None,
            reddit_client_secret=os.environ.get("REDDIT_CLIENT_SECRET", "").strip() or None,
            reddit_user_agent=os.environ.get("REDDIT_USER_AGENT", "").strip() or None,
            reddit_subreddits=subreddits,
            sentiment_min_mentions=_env_int(
                "CHATTER_SENTIMENT_MIN", DEFAULT_SENTIMENT_MIN_MENTIONS
            ),
            baseline_db_path=_env_path("CHATTER_BASELINE_DB", _default_baseline_db_path()),
            archive_root=_env_path("CHATTER_ARCHIVE_ROOT", _default_archive_root()),
            baseline_window=_env_int("CHATTER_BASELINE_WINDOW", DEFAULT_BASELINE_WINDOW),
            baseline_min_obs=_env_int("CHATTER_BASELINE_MIN_OBS", DEFAULT_BASELINE_MIN_OBS),
            spike_z_threshold=_env_float("CHATTER_SPIKE_Z", DEFAULT_SPIKE_Z),
            trend_spike_ratio=_env_float("CHATTER_TREND_RATIO", DEFAULT_TREND_SPIKE_RATIO),
            universe_cache_ttl_s=_env_int("CHATTER_UNIVERSE_TTL", DEFAULT_UNIVERSE_TTL_S),
            symbol_fallback_path=(
                Path(os.environ["CHATTER_SYMBOL_FALLBACK"].strip())
                if os.environ.get("CHATTER_SYMBOL_FALLBACK", "").strip()
                else None
            ),
            attention_subreddits=(
                tuple(s.strip() for s in _attn_raw.split(",") if s.strip())
                if (_attn_raw := os.environ.get("CHATTER_ATTENTION_SUBREDDITS", "").strip())
                else DEFAULT_ATTENTION_SUBREDDITS
            ),
            attention_post_limit=_env_int(
                "CHATTER_ATTENTION_LIMIT", DEFAULT_ATTENTION_POST_LIMIT
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
