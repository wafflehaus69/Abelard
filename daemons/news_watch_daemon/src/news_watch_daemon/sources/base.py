"""Source-plugin contract.

Concrete plugins (Finnhub, Telegram, RSS) land in the source-plugins
brief. This module fixes the interface they must implement so the scrape
layer can be written against the abstraction.

Design rules carried into every implementation:

  - `fetch()` must NOT raise. Network or parse errors are returned as a
    `FetchResult` with `status != "ok"` and an `error_detail`. This is
    the "fail loudly, not silently" principle in concrete form.
  - `FetchedItem` carries only what a source can supply cheaply. Entity
    extraction (countries, commodities, people) happens downstream in
    the scrape layer using theme keyword/entity matches — not here.
  - `raw_body` is always None for now. Body fetching is on-demand, not
    part of the scrape sweep.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Optional


FetchStatus = Literal["ok", "rate_limited", "error", "partial"]


@dataclass(frozen=True)
class FetchedItem:
    """A single item returned by a source plugin's `fetch()` call."""

    source_item_id: str           # source's native ID
    headline: str
    url: Optional[str]
    published_at_unix: int
    raw_source: Optional[str]     # original publisher if syndicated (e.g. "Reuters")
    tickers: list[str] = field(default_factory=list)  # pre-tagged by source if available
    raw_body: Optional[str] = None  # always None for now; on-demand fetches come later


@dataclass(frozen=True)
class FetchResult:
    """The full return value of a source plugin's `fetch()` call."""

    source: str
    fetched_at_unix: int
    items: list[FetchedItem]
    status: FetchStatus
    error_detail: Optional[str] = None


class SourcePlugin(ABC):
    """Abstract source plugin. Concrete implementations land in a later brief."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable source identifier, e.g. 'finnhub_general_news'.

        Used as the value stored in the `headlines.source` column and the
        `source_health.source` key. Must be unique per concrete plugin
        instance and stable across daemon restarts.
        """

    @abstractmethod
    def fetch(self, since_unix: int) -> FetchResult:
        """Fetch items published since `since_unix`.

        MUST NOT raise. Network errors, parse errors, rate limits — all
        surface in the returned FetchResult.status / error_detail. A
        plugin that lets an exception escape this method violates the
        scrape layer's contract.

        Status semantics:
          - "ok":           clean fetch, items may be empty
          - "partial":      some items parsed, some dropped (see error_detail)
          - "rate_limited": HTTP 429 or equivalent
          - "error":        nothing usable; details in error_detail
        """

    @abstractmethod
    def rate_limit_budget_remaining(self) -> float:
        """0.0–1.0; scraper uses this to skip optional sources when budget is low.

        Implementations should return 1.0 when the budget is unknown or
        not tracked (i.e. the optimistic default).
        """

    @property
    def cadence_minutes(self) -> int | None:
        """Optional minimum cadence between fetches, in minutes.

        Returns None (the default) to mean "run every scrape cycle". A
        positive int tells the orchestrator to skip this source if its
        `source_health.last_attempt_unix` was more recent than
        `cadence_minutes` minutes ago. Brand-new sources (no
        source_health row yet) are never throttled.

        This is intentionally non-abstract — plugins that don't need
        per-source throttling inherit the default and behave exactly
        as they did before Pass B.
        """
        return None


__all__ = ["FetchedItem", "FetchResult", "FetchStatus", "SourcePlugin"]
