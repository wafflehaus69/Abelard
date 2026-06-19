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
