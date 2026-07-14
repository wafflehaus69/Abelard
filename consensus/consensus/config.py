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
    goldsky_subgraph: str


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


class CollectorTiersConfig(_Strict):
    """Adaptive per-market poll cadence (M1.5). Intervals sized from the
    2026-07-12 measurements: worst observed per-market burst rolls a 4k window
    in ~13.7 min, so the hot tier's default gives >5x margin."""

    hot_interval_minutes: int = Field(ge=1, default=2)
    quiet_interval_minutes: int = Field(ge=1, default=15)
    dormant_interval_minutes: int = Field(ge=1, default=360)
    hot_threshold_new_fills: int = Field(ge=1, default=50)
    hot_ttl_minutes: int = Field(ge=1, default=30)
    quiet_if_fill_within_hours: int = Field(ge=1, default=24)


class CollectorConfig(_Strict):
    """M1.5 forward collector (L2). ``tags`` are gamma tag_slugs — the
    enumeration universe; the market lane is the coverage guarantee."""

    tape_path: str
    tags: list[str] = Field(min_length=1)
    enumeration_interval_minutes: int = Field(ge=1, default=30)
    gamma_page_limit: int = Field(ge=1, le=500, default=100)
    request_spacing_ms: int = Field(ge=0, default=100)
    page_size: int = Field(ge=1, le=1000, default=1000)
    max_pages: int = Field(ge=1, le=4, default=4)
    global_lane_enabled: bool = True
    envelope_log: str | None = None
    # Per-invocation market-poll budget: bounds a pass under ANY universe size
    # (measured: the three tags enumerate ~15k markets). Oldest-polled-first
    # rotation drains the backlog fairly across invocations.
    max_markets_per_run: int = Field(ge=1, default=500)
    # After a market is first seen closed, keep polling it this long before
    # deactivation, so the fills around resolution are captured (drain window).
    drain_minutes: int = Field(ge=1, default=360)
    # A lock older than this is considered abandoned (crashed run).
    lock_stale_minutes: int = Field(ge=1, default=30)
    tiers: CollectorTiersConfig = CollectorTiersConfig()


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
    collector: CollectorConfig

    # Populated by the loader, not the yaml. Excluded from the strict model to
    # keep validation of the file itself clean.
    model_config = ConfigDict(extra="forbid", frozen=True)

    def resolved_cache_path(self, config_dir: Path) -> Path:
        """Absolute cache DB path (``data_layer.cache_path`` resolved relative to
        the config file's directory when not already absolute)."""
        p = Path(self.data_layer.cache_path)
        return p if p.is_absolute() else (config_dir / p).resolve()

    def resolved_tape_path(self, config_dir: Path) -> Path:
        """Absolute L2 tape DB path, same resolution rule as the cache."""
        p = Path(self.collector.tape_path)
        return p if p.is_absolute() else (config_dir / p).resolve()

    def resolved_envelope_log(self, config_dir: Path) -> Path | None:
        if self.collector.envelope_log is None:
            return None
        p = Path(self.collector.envelope_log)
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
        self.tape_path = config.resolved_tape_path(config_dir)
        self.envelope_log = config.resolved_envelope_log(config_dir)

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
