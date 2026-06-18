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
    sentiment: Sentiment | None = None
    flags: list[str] = Field(default_factory=list)


class WatchlistSummary(BaseModel):
    """Per-watchlist summary in the run envelope."""

    model_config = ConfigDict(extra="forbid")

    name: str
    tickers: int  # total ticker specs in the list
    active: int  # enabled tickers (placeholders like an unverified `P` excluded)


class ScanEnvelope(BaseModel):
    """Run-level output wrapper — one JSON object per invocation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    scan_mode: ScanMode
    canonical_ts: str
    windows: list[Window] = Field(default_factory=list)
    watchlists: list[WatchlistSummary] = Field(default_factory=list)
    records: list[NormalizedRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
