"""Configuration — one validated source of truth (spec §5, §6).

Algorithm/behavior parameters come from ``config.yaml`` (pydantic-validated).
Secrets come from the environment (never the yaml): ``ETHERSCAN_API_KEY`` and
an optional ``LOG_LEVEL`` override. Loading fails loudly — the system must never
start half-configured.

The pydantic models forbid unknown keys, so a typo'd knob is a startup error,
not a silently ignored setting.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .errors import ConfigError

REDACTED = "***REDACTED***"

# Default config path: <repo>/consensus/config.yaml (sibling of this package
# directory). Overridable via the CONSENSUS_CONFIG env var or an explicit arg.
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


class _Strict(BaseModel):
    """Base model: reject unknown keys so config typos fail loudly."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class HttpConfig(_Strict):
    user_agent: str
    timeout: float = Field(gt=0)
    max_retries: int = Field(ge=1, le=10)
    base_backoff: float = Field(ge=0)


class EndpointsConfig(_Strict):
    polymarket_data_api: str
    polymarket_gamma_api: str
    polymarket_clob_api: str
    kalshi_api: str
    etherscan_v2_api: str


class SmokeConfig(_Strict):
    market_condition_id: str
    wallet_proxy: str
    kalshi_markets_limit: int = Field(ge=1, le=100)


class DataLayerConfig(_Strict):
    cache_path: str
    http: HttpConfig
    endpoints: EndpointsConfig
    smoke: SmokeConfig


class CategoriesConfig(_Strict):
    targets: list[str] = Field(min_length=1)


class LoggingConfig(_Strict):
    level: str = "INFO"


class MetaConfig(_Strict):
    regime_floor_date: str


class Secrets:
    """Secrets read from the environment, kept out of the validated yaml model."""

    def __init__(self, *, etherscan_api_key: str | None, log_level_override: str | None) -> None:
        self.etherscan_api_key = etherscan_api_key or None
        self.log_level_override = log_level_override or None

    def secret_values(self) -> tuple[str, ...]:
        """Non-empty secret strings, for the logging redaction filter."""
        return tuple(v for v in (self.etherscan_api_key,) if v)


class Config(_Strict):
    meta: MetaConfig
    logging: LoggingConfig
    categories: CategoriesConfig
    data_layer: DataLayerConfig

    # Populated by the loader, not the yaml. Excluded from the strict model to
    # keep validation of the file itself clean.
    model_config = ConfigDict(extra="forbid", frozen=True)

    def resolved_cache_path(self, config_dir: Path) -> Path:
        """Absolute cache DB path (``data_layer.cache_path`` resolved relative to
        the config file's directory when not already absolute)."""
        p = Path(self.data_layer.cache_path)
        return p if p.is_absolute() else (config_dir / p).resolve()

    @property
    def effective_log_level(self) -> str:
        return self.logging.level.upper()


class LoadedConfig:
    """A validated :class:`Config` plus its on-disk context (config dir, resolved
    cache path) and the environment secrets. This is what the rest of the system
    receives."""

    def __init__(self, config: Config, *, config_dir: Path, secrets: Secrets) -> None:
        self.config = config
        self.config_dir = config_dir
        self.secrets = secrets
        self.cache_path = config.resolved_cache_path(config_dir)

    @property
    def log_level(self) -> str:
        return (self.secrets.log_level_override or self.config.effective_log_level).upper()


def load_config(path: str | os.PathLike[str] | None = None) -> LoadedConfig:
    """Load and validate configuration. Raises :class:`ConfigError` on any
    problem (missing file, malformed yaml, failed validation)."""
    config_path = Path(path) if path is not None else Path(
        os.environ.get("CONSENSUS_CONFIG", _DEFAULT_CONFIG_PATH)
    )
    if not config_path.is_file():
        raise ConfigError(f"config file not found: {config_path}")

    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {config_path}: {exc}") from exc

    try:
        data: Any = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"malformed YAML in {config_path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"config root must be a mapping, got {type(data).__name__}")

    try:
        config = Config.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid config in {config_path}:\n{exc}") from exc

    secrets = Secrets(
        etherscan_api_key=os.environ.get("ETHERSCAN_API_KEY", "").strip(),
        log_level_override=os.environ.get("LOG_LEVEL", "").strip(),
    )
    return LoadedConfig(config, config_dir=config_path.parent.resolve(), secrets=secrets)


class _RedactingFilter(logging.Filter):
    """Replace any known secret value in a log record with REDACTED.

    Belt-and-suspenders alongside http_client's URL-query redaction: this
    catches a secret that reaches a log record by any other path.
    """

    def __init__(self, secrets: tuple[str, ...]) -> None:
        super().__init__()
        self._secrets = tuple(s for s in secrets if s)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True
        msg = record.getMessage()
        redacted = msg
        for secret in self._secrets:
            if secret in redacted:
                redacted = redacted.replace(secret, REDACTED)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


def configure_logging(loaded: LoadedConfig) -> logging.Logger:
    """Configure the ``consensus`` logger once (idempotent). Logs go to stderr so
    a CLI can keep stdout clean for structured/report output."""
    import sys

    logger = logging.getLogger("consensus")
    logger.setLevel(loaded.log_level)
    secrets = loaded.secrets.secret_values()
    if not any(isinstance(f, _RedactingFilter) for h in logger.handlers for f in h.filters):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        handler.addFilter(_RedactingFilter(secrets))
        logger.addHandler(handler)
    logger.propagate = False
    return logger
