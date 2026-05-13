"""Configuration loaded once from environment variables.

- FINNHUB_API_KEY      — required, Finnhub free-tier key
- EDGAR_USER_AGENT     — required, descriptive string per SEC fair-access policy
                         (e.g. "ResearchDaemon contact@example.com")
- LOG_LEVEL            — optional, default INFO

Fails loudly on missing required values — the daemon should never start
with half-configuration.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass


REDACTED = "***REDACTED***"


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    finnhub_api_key: str
    edgar_user_agent: str
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        finnhub = os.environ.get("FINNHUB_API_KEY", "").strip()
        if not finnhub:
            raise ConfigError("FINNHUB_API_KEY is not set")

        ua = os.environ.get("EDGAR_USER_AGENT", "").strip()
        if not ua:
            raise ConfigError(
                "EDGAR_USER_AGENT is not set. SEC requires a descriptive "
                "User-Agent, e.g. 'ResearchDaemon contact@example.com'."
            )

        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        return cls(finnhub_api_key=finnhub, edgar_user_agent=ua, log_level=log_level)


class _RedactingFilter(logging.Filter):
    """Replace any occurrence of a known secret value with REDACTED.

    Belt-and-suspenders: http_client redacts URL query params at emit time,
    but this catches anything else that slips into a log record.
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
    """Configure the root daemon logger once. Idempotent."""
    logger = logging.getLogger("research_daemon")
    logger.setLevel(config.log_level)
    if not any(isinstance(f, _RedactingFilter) for h in logger.handlers for f in h.filters):
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        handler.addFilter(_RedactingFilter((config.finnhub_api_key,)))
        logger.addHandler(handler)
    logger.propagate = False
    return logger
