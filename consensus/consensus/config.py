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
    """Base model: reject unknown keys so config typos fail loudly.

    Note: frozen=True blocks attribute rebinding, but dict/list field VALUES
    (factor_weights, tier_thresholds, breakpoint lists) remain mutable in place.
    Config is the single source of truth and must be treated as read-only; the
    determinism contract relies on callers never mutating these in place."""

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
    # Dedup skip guard (L2 throughput). A market-lane walk re-fetches the newest
    # page every poll; records below the market's stored frontier are already on
    # the tape (contiguity invariant) and re-inserting them is pure waste. Skip
    # them — but only those older than (frontier − this margin), so a fill that
    # data-api surfaces LATE (indexed after its trade time) still lands in the
    # re-processed near-frontier band and is never silently dropped. The margin
    # must exceed data-api's worst indexing lag. No lag figure has been directly
    # measured (the 2026-07-12 memo measured fill RATE/window-roll, not lag); 60
    # min is a conservative assumption, well beyond the seconds-to-minutes lag
    # those rates imply — a per-fill trade-time vs first_seen_poll probe would
    # confirm it. A finite margin cannot cover a multi-hour indexer outage +
    # backfill; that needs a periodic margin=0 reconciliation pass, not a bigger
    # routine margin. Set to 0 to disable the skip entirely (full INSERT-OR-IGNORE).
    late_arrival_margin_minutes: int = Field(ge=0, default=60)
    # Stray adjudication is O(unresolved strays) per enumeration — each pending
    # stray costs up to 2 gamma /markets lookups. Strays the global feed shows
    # but gamma never recognises (frequently malformed condition ids) are left
    # unresolved and re-fetched EVERY pass, accumulating without bound; on Basilic
    # (2026-07-20) 5.7k such strays made enumeration ≈31 min and dominated pass
    # duration. Bound it two ways: adjudicate at most this many per run (least-
    # attempted first, so a backlog rotates through)...
    stray_adjudication_max_per_run: int = Field(ge=1, default=500)
    # ...and abandon a stray after this many "unknown to gamma" lookups (transient
    # lookup errors do NOT count), marking it resolved so it stops being re-fetched.
    # Safe: a real target market is adopted by the normal tag-page enumeration
    # walk, not only via the stray path, so abandoning an unknown cid loses nothing.
    stray_max_attempts: int = Field(ge=1, default=3)
    # Resolution sweep (bounded, most-stale-first, mirrors the stray cap). Each
    # enumeration confirms closure of up to this many tracked markets that fell
    # out of the open (closed=false) enumeration — catching whole-event closures
    # the open enumeration never sees — via one gamma closed=true lookup each,
    # persisting the winning outcome for the Detector-A confirmation pass.
    resolution_sweep_max_per_run: int = Field(ge=1, default=200)
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


class LabeledHypothesis(_Strict):
    """One reported insider-wallet hypothesis (press-derived; confirmed only
    by matching against the on-chain tape — spec §7)."""

    name: str
    address: str | None = None
    approx_shares: float | None = None
    approx_price: float | None = None


class M0FConfig(_Strict):
    """M0-F Feb-28 footprint backtest (v1.2 §3) — calibration parameters.
    A historical study on L1 data: no live scanning, no alerting."""

    news_break_ts: int
    window_start_ts: int
    window_end_ts: int
    baseline_days: int = Field(ge=0, default=7)
    search_terms: list[str] = Field(min_length=1)
    size_floor_usdc: float = Field(gt=0, default=5000)
    directional_min: float = Field(ge=0, le=1, default=0.8)
    fresh_day_breakpoints: list[int] = Field(default=[7, 30, 90])
    fresh_scores: list[float] = Field(default=[1.0, 0.6, 0.2, 0.02])
    prior_fills_discount_threshold: int = Field(ge=1, default=50)
    prior_fills_discount: float = Field(gt=0, le=1, default=0.3)
    s_full_scale_frac: float = Field(gt=0, default=0.05)
    t_latency_breakpoints_min: list[int] = Field(default=[60, 1440, 10080])
    t_scores: list[float] = Field(default=[1.0, 0.7, 0.3, 0.1])
    factor_weights: dict[str, float]
    cluster_min: int = Field(ge=2, default=3)
    cluster_window_hours: int = Field(ge=1, default=12)
    cluster_boost: float = Field(ge=1, default=1.5)
    cross_market_enabled: bool = True
    # v1.3 §3.2: cluster membership is dossier evidence, not a score multiplier.
    # In a saturated-attention regime everything clusters, so boosting the
    # composite is the dominant FP driver. Membership is still computed and
    # reported; when False it does not move the composite or the tier.
    cluster_boosts_score: bool = False
    tier_thresholds: dict[str, float]
    as_of_ladder: list[int] = Field(min_length=1)
    labeled_hypotheses: list[LabeledHypothesis] = Field(default_factory=list)


class M0CRegimeSlice(_Strict):
    name: str
    start: int
    end: int


class M0CSweep(_Strict):
    participation_floor: list[int] = Field(min_length=1)
    agreement_threshold: list[float] = Field(min_length=1)
    circle_size_k: list[int] = Field(min_length=1)
    max_edge_paid: list[float] = Field(min_length=1)


class M0CConfig(_Strict):
    """M0-C consensus replay (v1.0 §M0-C + v1.2 §4). Zero-lookahead backtest of
    the CONSENSUS mechanic with a parameter sweep and GO/NO-GO decision."""

    replay_start_ts: int
    replay_end_ts: int
    categories: list[str] = Field(min_length=1)
    min_resolved_trades: int = Field(ge=1, default=10)
    decay_half_life_days: int = Field(ge=1, default=90)
    mm_two_sided_frac: float = Field(ge=0, le=1, default=0.35)
    circle_size_k: int = Field(ge=1, default=15)
    participation_floor: int = Field(ge=1, default=4)
    agreement_threshold: float = Field(ge=0.5, le=1, default=0.75)
    max_edge_paid: float = Field(ge=0, default=0.10)
    entry_lag_minutes: int = Field(ge=0, default=30)
    rescan_cadence_days: int = Field(ge=1, default=7)
    min_position_usdc: float = Field(ge=0, default=500)
    # M6 freshness: the consensus must have COMPLETED within this window of the
    # scan (a consensus formed weeks ago at much lower prices is stale by
    # construction — pilot finding: 6-week-old consensus on 99.9c favorites).
    freshness_window_days: int = Field(ge=1, default=14)
    # Absolute signal-price ceiling: above this there is no meaningful payoff
    # room left to buy (pilot finding: 0.999-band "signals" earn millicents).
    # DEVIATION-FLAG: not in v1.0 §M6 explicitly; supported by its remaining-
    # edge rationale; architect to ratify.
    price_ceiling: float = Field(gt=0, le=1, default=0.95)
    sweep: M0CSweep
    regime_slices: list[M0CRegimeSlice] = Field(default_factory=list)


class M5Config(_Strict):
    """M5 funded→bet latency (addendum v1.4). The deliverable is the
    false-positive curve of the latency factor across all M0-F candidates."""

    cex_fanout_threshold: int = Field(ge=1, default=400)
    latency_breakpoints_min: list[int] = Field(default=[5, 60, 1440])
    latency_scores: list[float] = Field(default=[1.0, 0.6, 0.2, 0.02])
    # Latency thresholds (minutes) swept for the FP curve; a wallet "fires" if
    # its funded→bet latency is at/under the threshold.
    fp_curve_thresholds_min: list[int] = Field(default=[1, 5, 15, 60, 240, 1440])
    request_spacing_ms: int = Field(ge=0, default=250)  # Etherscan free tier ~3-5/s


class M10Config(_Strict):
    """M10 live UNUSUAL_ACTIVITY scan (Detector B, spec §M10 + docs/m10_build_plan).
    An on-command scan over the L2 tape that surfaces fresh-wallet informed-money
    footprints as DOSSIERS. NO EV, never a trade signal, permanently excluded from
    M9 staging. All fields default -> the yaml block is optional (tuning knobs)."""

    unusual_lookback_hours: int = Field(ge=1, default=48)
    size_floor_usdc: float = Field(gt=0, default=10_000)
    # Fill-factor weights: empty -> reuse M0-F's factor_weights at runtime.
    factor_weights: dict[str, float] = Field(default_factory=dict)
    tier_thresholds: dict[str, float] = Field(
        default_factory=lambda: {"WATCH": 0.30, "ELEVATED": 0.50, "CRITICAL": 0.70})
    # Latency elevator (v1.5 §3): a wallet already past the fill-factor bar whose
    # funded->bet latency is at/under latency_tight_minutes gets a multiplicative
    # boost (discounted for CEX funders); loose/absent/errored latency -> no lift.
    latency_tight_minutes: int = Field(ge=1, default=60)
    latency_elevator_boost: float = Field(ge=1.0, default=1.5)
    # v1.6 §3.3 enrichment gate: chain-enrich at most this many fill-bar-passing
    # wallets per scan (a few dozen Etherscan calls, never hundreds).
    enrichment_max_wallets_per_scan: int = Field(ge=1, default=40)
    cluster_window_hours: int = Field(ge=1, default=12)
    cross_market_record_only: bool = True   # v1.3: cluster records, never scores
    excluded_categories: list[str] = Field(default_factory=list)


class Config(_Strict):
    meta: MetaConfig
    logging: LoggingConfig
    categories: CategoriesConfig
    data_layer: DataLayerConfig
    collector: CollectorConfig
    m0f: M0FConfig
    m5: M5Config = M5Config()
    m0c: M0CConfig
    m10: M10Config = M10Config()

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
