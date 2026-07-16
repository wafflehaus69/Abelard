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
SourceName = Literal[
    "stocktwits", "smg", "finnhub_news", "google_trends", "twitter", "yahoo_rss", "alpha_vantage"
]
ScanMode = Literal["watchlist", "attention"]
MatchedBy = Literal["symbol", "cashtag", "name"]
SentimentMethod = Literal["native", "haiku", "none"]


class Window(BaseModel):
    """A time window, anchored to the single canonical timestamp. ISO-8601 Z."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: str
    end: str
    label: str  # "24h" | "7d" | "monthly"


class ObservedWindow(BaseModel):
    """The ACTUAL span of the tweets that survived filtering for a ticker (Twitter
    source, Order 17): `earliest`/`latest` = min/max of their createdAt. Distinct from
    `Window` (the scan's nominal 24h/7d/monthly window) — this is the real observed
    range of the surviving evidence. A record's `observed_window` is None when zero
    tweets survived (an honest absence, never a fabricated span)."""

    model_config = ConfigDict(extra="forbid")

    earliest: str  # ISO-8601 — min createdAt of survivors
    latest: str  # ISO-8601 — max createdAt of survivors


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


class NativeStance(BaseModel):
    """StockTwits users' own Bull/Bear self-tags — zero-cost, sparse (~40% of messages
    carry one). Carried ALONGSIDE the Haiku body-read on a StockTwits record so the two
    reads sit side by side; `tagged`/`messages` give the native read's coverage.
    Divergence between this and the Haiku tally is signal Abelard reconciles — the
    daemon never collapses them."""

    model_config = ConfigDict(extra="forbid")

    bullish: int = Field(default=0, ge=0)
    bearish: int = Field(default=0, ge=0)
    tagged: int = Field(default=0, ge=0)    # messages carrying a native tag
    messages: int = Field(default=0, ge=0)  # total messages seen (coverage = tagged/messages)


class Sentiment(BaseModel):
    """Stance aggregation. `method` records how the PRIMARY tally was produced: native
    tags (StockTwits self-tags), Haiku (StockTwits bodies), or none (sources with no
    stance). For a StockTwits record the users' own native tally rides in `native`
    alongside the primary — when Haiku ran (method="haiku") the two are distinct reads,
    and the daemon does NOT reconcile a divergence (that is Abelard's call)."""

    model_config = ConfigDict(extra="forbid")

    method: SentimentMethod
    bullish: int = Field(default=0, ge=0)
    bearish: int = Field(default=0, ge=0)
    neutral: int = Field(default=0, ge=0)
    native: NativeStance | None = None


class StockTwitsAggregate(BaseModel):
    """StockTwits' OWN computed aggregate over the full stream (sentiment-API gateway,
    Order 12) — the now-primary read that supersedes the 30-message window. Everything
    is normalized (0-100) + a 5-band categorical; the raw `value`/`label` are NEVER
    consumed (proven invertible in live data). `*_now` is the live gauge, `*_24h` the
    trailing baseline, and `sent_gap = now - 24h` is the spike / regime-shift signal.
    `confidence` is the volume x participation trust gate. All nullable — a missing or
    `loaded:false` metric stays None."""

    model_config = ConfigDict(extra="forbid")

    sent_now_norm: int | None = None       # 0-100 (50=neutral, <25 extreme-bear, >75 extreme-bull)
    sent_now_label: str | None = None      # labelNormalized, 5-band
    sent_24h_norm: int | None = None       # trailing baseline
    sent_24h_label: str | None = None
    sent_gap: int | None = None            # signed now - 24h (the spike signal)
    vol_now_norm: int | None = None        # 0-100 volume band
    vol_now_raw: int | None = None         # real message count (retires the page-size 30)
    vol_change: float | None = None
    participation_norm: int | None = None  # 0-100 (timeframes.1D) — the trust gate
    confidence: str | None = None          # high | quiet | low | pump_suspect


class NewsSentiment(BaseModel):
    """Alpha Vantage NEWS_SENTIMENT per-ticker aggregate (CH-SRC-1) — the news-sentiment axis,
    distinct from StockTwits crowd mood and Finnhub's factual count. Aggregated over the
    articles whose `ticker_sentiment[]` names this ticker ABOVE the relevance gate: `score` =
    relevance-weighted mean of AV's `ticker_sentiment_score` ([-1..+1], + = bullish); `label` =
    the AV band derived from `score`; `articles` = the count above the gate; `mean_relevance` =
    their mean `relevance_score`. All nullable — no qualifying article -> None/0, an honest
    absence. Chatter emits the axis; Abelard joins the three reads (never the daemon)."""

    model_config = ConfigDict(extra="forbid")

    score: float | None = None              # relevance-weighted mean ticker_sentiment_score
    label: str | None = None                # Bearish | Somewhat-Bearish | Neutral | Somewhat-Bullish | Bullish
    articles: int = Field(default=0, ge=0)  # AV articles above the relevance gate
    mean_relevance: float | None = None     # mean relevance_score of the counted articles


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
    # StockTwits sentiment-API aggregate (Order 12) — present only on stocktwits records.
    st_aggregate: StockTwitsAggregate | None = None
    # Haiku one-paragraph summary of the NAMED news (Order 15) — Finnhub records only; the
    # factual "why", distinct from the source's method=none count contract.
    news_summary: str | None = None
    # The actual span of surviving tweets (Twitter source, Order 17) — None when zero
    # tweets survived filtering. Round-trips to stdout via SourceSignal.
    observed_window: ObservedWindow | None = None
    # Haiku <=3-sentence summary of the Twitter commentary (Order 18) — Twitter records
    # only; the crowd's "what they're saying", distinct from the bull/bear stance tally.
    twitter_summary: str | None = None
    # Alpha Vantage NEWS_SENTIMENT per-ticker aggregate (CH-SRC-1) — alpha_vantage records only;
    # the news-sentiment axis, distinct from StockTwits crowd mood + Finnhub's method=none count.
    news_sentiment: NewsSentiment | None = None
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
    the StockTwits plugin populates it; every other source is LLM-free."""

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
    # Order 19: raw scraped text ("source\tTICKER\ttext"), for the history dump only —
    # NOT carried into the persisted AggregatedScanResult.
    raw_items: list[str] = Field(default_factory=list)


# --- Order 7: aggregation layer (separate from the plugin NormalizedRecord) -------

AnomalyState = Literal["building", "thin", "ok", "spike", "none"]


class Anomaly(BaseModel):
    """Mechanical anomaly read for one (ticker, source) — Abelard interprets it.

    `kind="count"` (Finnhub / /smg/ / StockTwits): z-score vs the trailing
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
    st_aggregate: StockTwitsAggregate | None = None  # StockTwits sentiment-API (Order 12)
    news_summary: str | None = None  # Finnhub named-news Haiku summary (Order 15)
    observed_window: ObservedWindow | None = None  # Twitter survivor span (Order 17)
    twitter_summary: str | None = None  # Twitter commentary Haiku summary (Order 18)
    news_sentiment: NewsSentiment | None = None  # Alpha Vantage news-sentiment axis (CH-SRC-1)
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


# --- Order 8: ATTENTION cold archive (prune roll-up) ------------------------------


class DayRollup(BaseModel):
    """One pruned (day, ticker, source) summary — what survives the 14-day hot
    window as compact long-term memory."""

    model_config = ConfigDict(extra="forbid")

    day: str  # YYYY-MM-DD (UTC)
    ticker: str
    source: str
    scans: int = Field(ge=0)
    total_count: int = Field(ge=0)
    max_count: int = Field(ge=0)


class ColdRollup(BaseModel):
    """A prune batch: hot events aged past 14 days, aggregated per day/ticker/source
    and archived to cold storage BEFORE deletion. Indefinite; never feeds live
    velocity."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    rollup_id: str
    generated_ts: int
    cutoff_ts: int
    rollups: list[DayRollup] = Field(default_factory=list)


# --- Order 8: ATTENTION scan result (off-watchlist discovery) ---------------------

# Discovery surfaces are their own labels (rising / frequency / trending) — a parallel
# vocabulary to the watchlist SourceName, not the same enum.
AttentionSource = Literal["smg_freq", "stocktwits_trending"]


class AttentionSignal(BaseModel):
    """One surface's observation of a discovered ticker. `anomaly` is the VELOCITY
    read — the count z-score vs the rolling baseline. For /smg/ the count is distinct-
    post mentions; for StockTwits trending it's the (rounded) trending_score — the
    momentum axis (rank/score), never the watchlist_count (too stable to gate on).

    The StockTwits carry-through fields are null-guarded: absent upstream (an ETF with
    no fundamentals, a `trends: null` with no blurb) or on a non-StockTwits surface,
    they stay None."""

    model_config = ConfigDict(extra="forbid")

    source: AttentionSource
    semantics: str
    count: int = Field(ge=0)
    anomaly: Anomaly | None = None
    # StockTwits trending carry-through (Order 9 Phase B) — null-guarded, see above.
    rank: int | None = None
    trending_score: float | None = None
    watchlist_count: int | None = None
    sector: str | None = None
    summary: str | None = None


class AttentionTicker(BaseModel):
    """One discovered ticker: its per-surface signals, current salience, and whether
    the crowd found one of the operator's watchlist names on its own (amplified)."""

    model_config = ConfigDict(extra="forbid")

    ticker: str
    signals: list[AttentionSignal] = Field(default_factory=list)
    salience: int = Field(default=0, ge=0)  # "loud right now" — sum across surfaces
    on_watchlists: list[str] = Field(default_factory=list)
    amplified: bool = False
    flags: list[str] = Field(default_factory=list)  # cold_start / spike


class AttentionSurfaceStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: AttentionSource
    ok: bool
    candidates: int = Field(default=0, ge=0)  # tickers admitted (count >= floor)
    floor: int = Field(default=0, ge=0)
    warning: str | None = None


class AttentionResult(BaseModel):
    """The persisted ATTENTION artifact — salience + velocity + amplified intersections
    over the discovered universe. Descriptive; Abelard judges materiality."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    scan_id: str
    scan_mode: Literal["attention"] = "attention"
    canonical_ts: str
    surfaces: list[AttentionSurfaceStatus] = Field(default_factory=list)
    tickers: list[AttentionTicker] = Field(default_factory=list)
    pruned: int = Field(default=0, ge=0)  # hot rows rolled to cold this run
    degraded: bool = False
    cost: CostTelemetry = Field(default_factory=CostTelemetry)
    errors: list[str] = Field(default_factory=list)
