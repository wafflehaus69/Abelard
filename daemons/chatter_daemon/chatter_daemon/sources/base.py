"""The Source adapter protocol — the contract every plugin (Order 2+) implements.

A `Source` is a per-surface adapter. Given a watchlist and the run's `ScanContext`
(the single canonical timestamp + the derived windows), it fetches its surface and
returns a `SourceResult` carrying one `NormalizedRecord` per ticker it observed.

Failure-isolation rules (mirrors BizDaemon's per-batch loud-fail):
  - A source failure is ISOLATED: the adapter returns `SourceResult(error=...)`, or
    raises (the orchestrator catches it). Either way it folds into the top-level
    `errors` array and the OTHER sources still produce output. One dead source
    never sinks the whole scan.
  - An honest zero is data, not failure: a ticker with no mentions still yields a
    record with `mention_count=0`. Only REAL failures populate `error`.

UTF-8 DECODE — A PROTOCOL OBLIGATION, NOT A PER-PLUGIN AFTERTHOUGHT
------------------------------------------------------------------
Every adapter that performs HTTP MUST force ``resp.encoding = "utf-8"`` (or
``resp.content.decode("utf-8")``) BEFORE reading ``.json()`` / ``.text``, and MUST
ship a non-ASCII regression test asserting that tickers wedged against non-ASCII
punctuation (em-dashes, smart quotes, accents) still extract. `requests` infers
encoding from headers/chardet and falls back to a platform default (cp1252 on
Windows) that mis-decodes UTF-8 into mojibake; corrupted bytes adjacent to a ticker
eat its ``\\b`` word boundary and it silently fails to extract — this cost ~60% of
tickers in BizDaemon before the fix. Enforced from Order 2 onward. `ChatterPost.text`
is defined as ALREADY UTF-8-clean, so the shared matcher downstream can trust it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from ..schema import CostTelemetry, NormalizedRecord, ScanMode, SourceName, Window
from ..watchlist import WatchlistConfig


@dataclass(frozen=True)
class ScanContext:
    """Per-run timing context, stamped ONCE by the orchestrator and threaded to
    every adapter. No plugin recomputes "now" — it reads `canonical_ts` / the
    `windows` it was handed. `scan_mode` is the run mode the plugin stamps onto
    each NormalizedRecord.
    """

    scan_mode: ScanMode
    canonical_unix: int
    canonical_ts: str  # ISO-8601 Z
    windows: dict[str, Window]  # label -> Window ("24h" / "7d" / "monthly")


@dataclass(frozen=True)
class ChatterPost:
    """One raw post from a FREE-TEXT source (/smg/ / StockTwits), normalized for the
    shared matcher. `text` is already cleaned and UTF-8-decoded (see the module
    docstring's decode obligation). Symbol-keyed sources (StockTwits / Finnhub /
    Trends) do not produce these — they count per queried ticker directly and emit
    `NormalizedRecord`s without an intermediate post stream.
    """

    source: SourceName
    post_id: str  # stable per-source id (string — ids are not all ints)
    text: str
    author: str | None = None
    created_unix: int | None = None
    explicit_symbols: tuple[str, ...] = ()  # source-tagged cashtags/symbols, if any
    meta: dict = field(default_factory=dict)  # source-specific extras (score, etc.)


@dataclass(frozen=True)
class SourceResult:
    """What a `Source.fetch()` returns: the per-ticker normalized records, plus
    non-fatal `warnings` and an optional fatal `error` (both folded into the run's
    `errors` array by the orchestrator).
    """

    source: SourceName
    records: list[NormalizedRecord] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    cost: CostTelemetry | None = None  # LLM cost (StockTwits/Haiku only); folded into the envelope


@runtime_checkable
class Source(Protocol):
    """Every plugin implements this.

    `name` is the stable source identifier (a `SourceName`). `fetch` is total over
    valid input: given a watchlist and the run context it returns a `SourceResult`
    or raises — the orchestrator owns the degraded / partial / missing-source
    handling, never the leaf adapter.
    """

    name: str

    def fetch(self, watchlist: WatchlistConfig, *, context: ScanContext) -> SourceResult:
        ...
