"""Configuration loaded once from environment, plus the structured error type.

Required:
- FINNHUB_API_KEY      — Finnhub key for the US symbol universe. Read from env
                         only; never logged.

Optional (sensible defaults):
- ANTHROPIC_API_KEY    — required at sentiment time, not at load. A missing key
                         surfaces as a loud, structured error when the Haiku
                         pass actually runs, not at startup (a scrape with zero
                         attention-tier tickers needs no key).
- BIZ_DAEMON_DB_PATH   — SQLite state (universe cache + snapshots).
- BIZ_DAEMON_BLACKLIST — curated /biz/ slang blacklist file.
- BIZ_DAEMON_SYMBOL_FALLBACK — static US symbol list, used iff Finnhub
                         /stock/symbol is unavailable.
- BIZ_DAEMON_ATTENTION_N — mention threshold for ATTENTION tier (default 5).
- BIZ_DAEMON_READ_BULL_PCT / _READ_BEAR_PCT — sentiment read thresholds.
- LOG_LEVEL            — default INFO.

Fails loudly on a missing required value — the daemon never starts on half a
configuration.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# The daemon's .env sits next to pyproject.toml, one level above the package.
# Resolved from the module location so it loads regardless of cwd.
_DOTENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _load_dotenv(path: Path | None = None) -> None:
    """Load the daemon's .env into the environment, filling only the gaps.

    override=False means a key already set in the shell wins over the .env
    (session override wins; .env fills the gap). A missing file silently
    no-ops — that is the desired graceful behavior.
    """
    load_dotenv(path if path is not None else _DOTENV_PATH, override=False)

REDACTED = "***REDACTED***"

# Live Haiku model ID (confirmed via the claude-api skill at build time,
# 2026-06-03). Pinned here rather than recomputed from memory.
HAIKU_MODEL_ID = "claude-haiku-4-5"

DEFAULT_ATTENTION_N = 5
# Sentiment runs on any ticker at/above this mention count, decoupled from the
# ATTENTION flag (which stays at N=5 and only drives the ● marker). A ticker
# with 3-4 mentions gets a sentiment read but attention=false; <3 stays
# count-only with sentiment=null.
DEFAULT_SENTIMENT_MIN_MENTIONS = 3
DEFAULT_READ_BULL_PCT = 55
DEFAULT_READ_BEAR_PCT = 55

# "Real tickers that collide with common words" — the wordlist filter's
# exception list. A symbol here survives the common-word rejection (e.g. NOW =
# ServiceNow, META = Meta Platforms, CORN = the Teucrium corn ETF). Grows as
# live scrapes surface more legitimate collisions.
DEFAULT_WORD_TICKER_ALLOWLIST = frozenset({"NOW", "META", "CORN"})
DEFAULT_UNIVERSE_TTL_S = 24 * 60 * 60  # 24h — one Finnhub pull per day max.
DEFAULT_USER_AGENT = "biz-daemon/0.1.0"
DEFAULT_HTTP_TIMEOUT_S = 10.0


def _default_db_path() -> Path:
    return Path.home() / ".openclaw" / "biz_daemon" / "biz_daemon.sqlite3"


def _package_data_dir() -> Path:
    return Path(__file__).resolve().parent / "data"


def _default_blacklist_path() -> Path:
    return _package_data_dir() / "biz_slang_blacklist.txt"


def _default_common_words_path() -> Path:
    return _package_data_dir() / "common_words.txt"


def _default_sp500_names_path() -> Path:
    return _package_data_dir() / "sp500_names.txt"


def resolve_blacklist_path() -> Path:
    """Denylist path from env or default, without requiring full config.

    Lets `blacklist` maintenance commands run without a Finnhub key.
    """
    import os

    raw = os.environ.get("BIZ_DAEMON_BLACKLIST", "").strip()
    return Path(raw) if raw else _default_blacklist_path()


def _default_symbol_fallback_path() -> Path:
    return _package_data_dir() / "us_symbols_fallback.txt"


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


class BizDaemonError(RuntimeError):
    """Base for loud, structured daemon failures.

    Carries a `to_error()` rendering so leaf modules can fail loudly and the
    orchestrator can fold the failure into the `errors` array of the output
    contract without fabricating data.
    """

    def __init__(self, message: str, *, stage: str) -> None:
        super().__init__(message)
        self.stage = stage

    def to_error(self) -> str:
        return f"{self.stage}: {self}"


@dataclass(frozen=True)
class Config:
    finnhub_api_key: str
    anthropic_api_key: str | None
    log_level: str
    db_path: Path = field(default_factory=_default_db_path)
    blacklist_path: Path = field(default_factory=_default_blacklist_path)
    common_words_path: Path = field(default_factory=_default_common_words_path)
    sp500_names_path: Path = field(default_factory=_default_sp500_names_path)
    symbol_fallback_path: Path = field(default_factory=_default_symbol_fallback_path)
    word_ticker_allowlist: frozenset[str] = DEFAULT_WORD_TICKER_ALLOWLIST
    attention_n: int = DEFAULT_ATTENTION_N
    sentiment_min_mentions: int = DEFAULT_SENTIMENT_MIN_MENTIONS
    read_bull_pct: int = DEFAULT_READ_BULL_PCT
    read_bear_pct: int = DEFAULT_READ_BEAR_PCT
    universe_ttl_s: int = DEFAULT_UNIVERSE_TTL_S
    haiku_model_id: str = HAIKU_MODEL_ID
    user_agent: str = DEFAULT_USER_AGENT
    http_timeout_s: float = DEFAULT_HTTP_TIMEOUT_S

    def secrets(self) -> tuple[str, ...]:
        """Values to scrub from log output."""
        return tuple(s for s in (self.finnhub_api_key, self.anthropic_api_key) if s)

    @classmethod
    def from_env(cls, *, dotenv_path: Path | None = None) -> "Config":
        # Load the .env first so it fills any keys absent from the shell env.
        # Shell vars already set win (override=False); a missing .env no-ops.
        _load_dotenv(dotenv_path)

        finnhub = os.environ.get("FINNHUB_API_KEY", "").strip()
        if not finnhub:
            raise ConfigError("FINNHUB_API_KEY is not set")

        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip() or None
        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

        db_path = _env_path("BIZ_DAEMON_DB_PATH", _default_db_path())
        blacklist_path = _env_path("BIZ_DAEMON_BLACKLIST", _default_blacklist_path())
        common_words_path = _env_path(
            "BIZ_DAEMON_COMMON_WORDS", _default_common_words_path()
        )
        sp500_names_path = _env_path(
            "BIZ_DAEMON_SP500_NAMES", _default_sp500_names_path()
        )
        symbol_fallback_path = _env_path(
            "BIZ_DAEMON_SYMBOL_FALLBACK", _default_symbol_fallback_path()
        )
        allowlist = _env_token_set(
            "BIZ_DAEMON_TICKER_ALLOWLIST", DEFAULT_WORD_TICKER_ALLOWLIST
        )

        return cls(
            finnhub_api_key=finnhub,
            anthropic_api_key=anthropic_key,
            log_level=log_level,
            db_path=db_path,
            blacklist_path=blacklist_path,
            common_words_path=common_words_path,
            sp500_names_path=sp500_names_path,
            symbol_fallback_path=symbol_fallback_path,
            word_ticker_allowlist=allowlist,
            attention_n=_env_int("BIZ_DAEMON_ATTENTION_N", DEFAULT_ATTENTION_N),
            sentiment_min_mentions=_env_int(
                "BIZ_DAEMON_SENTIMENT_MIN", DEFAULT_SENTIMENT_MIN_MENTIONS
            ),
            read_bull_pct=_env_int("BIZ_DAEMON_READ_BULL_PCT", DEFAULT_READ_BULL_PCT),
            read_bear_pct=_env_int("BIZ_DAEMON_READ_BEAR_PCT", DEFAULT_READ_BEAR_PCT),
        )


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else default


def _env_token_set(name: str, default: frozenset[str]) -> frozenset[str]:
    """Comma-separated uppercase token override; falls back to default."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return frozenset(tok.strip().upper() for tok in raw.split(",") if tok.strip())


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


class _RedactingFilter(logging.Filter):
    """Replace any occurrence of a known secret with REDACTED.

    Belt-and-suspenders: leaf modules redact query params at emit time, but
    this catches anything else that slips into a log record.
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
    """Configure the root daemon logger once, to stderr. Idempotent."""
    logger = logging.getLogger("biz_daemon")
    logger.setLevel(config.log_level)
    if not any(
        isinstance(f, _RedactingFilter) for h in logger.handlers for f in h.filters
    ):
        import sys

        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        handler.addFilter(_RedactingFilter(config.secrets()))
        logger.addHandler(handler)
    logger.propagate = False
    return logger
