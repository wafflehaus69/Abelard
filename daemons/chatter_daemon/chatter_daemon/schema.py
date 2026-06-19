"""The normalized output schema — the binding contract for every plugin and the aggregator.

Appendix B of the build orders. **LOCK THIS DELIBERATELY.** All five source plugins
(Orders 2-6) emit `NormalizedRecord`s against it, and the Order-7 aggregator binds
to it; post-plugin schema churn is the expensive failure, so any change here ripples
across the whole daemon.

Layers:
  - `NormalizedRecord` — one per (ticker, source, window): the plugin output
    *before* the Order-7 anomaly block (the aggregator adds `sources[]`,
    `source_diversity`, and the `anomaly` block downstream). Plugins emit these.
  - `ScanEnvelope` — the run-level wrapper the CLI emits: the single canonical
    timestamp, the derived windows, the loaded watchlist summaries, the flat
    record list, and the always-present `errors` array.

Closed enums (SourceName / ScanMode / MatchedBy / SentimentMethod) make a typo a
validation error rather than a silent contract drift. `flags` is an open `list[str]`
on purpose — the plugin/aggregator flag vocabulary (rarity_hit, noisy_query, thin,
spike) grows without forcing a schema bump.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "1"

# Closed contract vocabularies. A value outside these is a validation error.
SourceName = Literal["stocktwits", "smg", "finnhub_news", "google_trends", "reddit"]
ScanMode = Literal["watchlist", "attention"]
MatchedBy = Literal["symbol", "cashtag", "name"]
SentimentMethod = Literal["native", "haiku", "none"]


class Window(BaseModel):
    """A time window, anchored to the single canonical timestamp. ISO-8601 Z."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: str
    end: str
    label: str  # "24h" | "7d" | "monthly"


class Headline(BaseModel):
    """One company-news headline (Finnhub plugin, Order 4) — count plus raw head."""

    model_config = ConfigDict(extra="forbid")

    title: str
    url: str


class Metrics(BaseModel):
    """Per-record metrics. Sources populate the fields they produce; the rest stay
    null (an honest absence, not a fabricated zero)."""

    model_config = ConfigDict(extra="forbid")

    mention_count: int = Field(default=0, ge=0)
    interest_24h: float | None = None
    interest_7d: float | None = None
    interest_monthly: float | None = None
    headlines: list[Headline] | None = None


class Sentiment(BaseModel):
    """Stance aggregation. `method` records HOW it was produced: native tags
    (StockTwits), Haiku (Reddit), or none (sources that carry no stance)."""

    model_config = ConfigDict(extra="forbid")

    method: SentimentMethod
    bullish: int = Field(default=0, ge=0)
    bearish: int = Field(default=0, ge=0)
    neutral: int = Field(default=0, ge=0)


class NormalizedRecord(BaseModel):
    """One (ticker, source, window) observation — plugin output, pre-anomaly."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    watchlist: str
    scan_mode: ScanMode
    canonical_ts: str
    window: Window
    source: SourceName
    ticker: str
    matched_by: list[MatchedBy] = Field(default_factory=list)
    metrics: Metrics = Field(default_factory=Metrics)
    # Always present (never null): `method` is the stance discriminator. A
    # no-stance source (Finnhub / Trends / /smg/) sets method="none"; the
    # aggregator switches on method, so a null here would fork it.
    sentiment: Sentiment
    flags: list[str] = Field(default_factory=list)


class WatchlistSummary(BaseModel):
    """Per-watchlist summary in the run envelope."""

    model_config = ConfigDict(extra="forbid")

    name: str
    tickers: int  # total ticker specs in the list
    active: int  # enabled tickers (placeholders like an unverified `P` excluded)


class SourceStatus(BaseModel):
    """Per-source outcome in the run envelope — the machine-readable degradation
    state. `ok=False` with an `error` means that source failed and was isolated
    into the top-level `errors`; the other sources still produced output."""

    model_config = ConfigDict(extra="forbid")

    source: SourceName
    ok: bool
    record_count: int = Field(default=0, ge=0)
    error: str | None = None


class CostTelemetry(BaseModel):
    """LLM cost telemetry — captured before any persistence so a downstream write
    failure can't lose the record of what the Haiku batch cost (doctrine #8). Only
    the Reddit plugin (Order 6) populates it; every other source is LLM-free."""

    model_config = ConfigDict(extra="forbid")

    haiku_calls: int = Field(default=0, ge=0)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cache_read_input_tokens: int = Field(default=0, ge=0)
    cache_creation_input_tokens: int = Field(default=0, ge=0)


class ScanEnvelope(BaseModel):
    """Run-level output wrapper — one JSON object per invocation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    scan_mode: ScanMode
    canonical_ts: str
    windows: list[Window] = Field(default_factory=list)
    watchlists: list[WatchlistSummary] = Field(default_factory=list)
    sources: list[SourceStatus] = Field(default_factory=list)
    records: list[NormalizedRecord] = Field(default_factory=list)
    cost: CostTelemetry = Field(default_factory=CostTelemetry)
    # True iff at least one source failed (partial or total). The exit code
    # disambiguates: total source failure (zero records, all failed) -> exit 1.
    degraded: bool = False
    errors: list[str] = Field(default_factory=list)


# --- Order 7: aggregation layer (separate from the plugin NormalizedRecord) -------

AnomalyState = Literal["building", "thin", "ok", "spike", "none"]


class Anomaly(BaseModel):
    """Mechanical anomaly read for one (ticker, source) — Abelard interprets it.

    `kind="count"` (Finnhub / Reddit / /smg/ / StockTwits): z-score vs the trailing
    baseline, gated by a min-volume floor + history depth. `kind="trend"` (Trends):
    within-record elevation of interest_24h over its trailing windows. States:
    building (history < N_min) | thin (count < floor) | ok | spike | none (no signal
    to score). A sigma=0 baseline yields no z — flagged in `note`, never fabricated.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["count", "trend"]
    state: AnomalyState
    z: float | None = None  # count: z-score (None when building/thin/sigma=0)
    mean: float | None = None  # count: baseline mu
    std: float | None = None  # count: baseline sigma
    observations: int = 0  # count: prior history depth
    ratio: float | None = None  # trend: interest_24h / trailing-max
    discounted: bool = False  # trend: noisy_query -> Abelard discounts
    note: str | None = None


class SourceSignal(BaseModel):
    """One source's observation of a ticker plus its anomaly read. Copies the plugin
    record's payload — the NormalizedRecord itself stays untouched."""

    model_config = ConfigDict(extra="forbid")

    source: SourceName
    metrics: Metrics
    sentiment: Sentiment
    matched_by: list[MatchedBy] = Field(default_factory=list)
    flags: list[str] = Field(default_factory=list)
    anomaly: Anomaly


class AggregatedTicker(BaseModel):
    """Per-ticker cross-source view. `source_diversity` = how many sources show
    nonzero signal (higher = corroborated across more surfaces)."""

    model_config = ConfigDict(extra="forbid")

    watchlist: str
    ticker: str
    sources: list[SourceSignal] = Field(default_factory=list)
    source_diversity: int = Field(default=0, ge=0)


class AggregatedScanResult(BaseModel):
    """The persisted Order-7 artifact: the per-ticker anomaly view + run provenance
    (sources / degraded / cost), keyed by a stable `scan_id`."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    scan_id: str
    scan_mode: ScanMode
    canonical_ts: str
    windows: list[Window] = Field(default_factory=list)
    watchlists: list[WatchlistSummary] = Field(default_factory=list)
    tickers: list[AggregatedTicker] = Field(default_factory=list)
    sources: list[SourceStatus] = Field(default_factory=list)
    degraded: bool = False
    cost: CostTelemetry = Field(default_factory=CostTelemetry)
    errors: list[str] = Field(default_factory=list)
