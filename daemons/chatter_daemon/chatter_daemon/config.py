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


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else default


DEFAULT_USER_AGENT = "chatter-daemon/0.1"

# Real tickers that collide with common words — the wordlist filter's exception
# list (a bare token here survives common-word rejection). Lifted from BizDaemon.
DEFAULT_WORD_TICKER_ALLOWLIST = frozenset({"NOW", "META", "CORN"})


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

    def secrets(self) -> tuple[str, ...]:
        """Values to scrub from log output."""
        return tuple(s for s in (self.finnhub_api_key,) if s)

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
