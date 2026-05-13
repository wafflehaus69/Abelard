"""Finnhub general-news source plugin.

Endpoint: `GET https://finnhub.io/api/v1/news?category=general&token=<key>`

Verified (live call): the endpoint returns a JSON array of objects with
keys `category`, `datetime` (Unix seconds), `headline`, `id` (numeric),
`image`, `related` (CSV tickers, but in practice always empty on this
endpoint), `source` (publisher name), `summary`, `url`.

Ticker association on the general feed is effectively never set — the
parser preserves it for schema correctness, but `FetchedItem.tickers`
will be `[]` for every item in normal operation. Theme association is
the orchestrator's keyword-match step's job, not this plugin's.
"""

from __future__ import annotations

import time
from typing import Any

from ..http_client import HttpClient
from .base import FetchedItem, FetchResult, SourcePlugin


FINNHUB_BASE_URL = "https://finnhub.io/api/v1/news"
PLUGIN_NAME = "finnhub:general"


class FinnhubGeneralNewsSource(SourcePlugin):
    """Pulls the Finnhub general-news feed and emits `FetchedItem`s."""

    def __init__(self, http_client: HttpClient, api_key: str | None) -> None:
        self._http = http_client
        self._api_key = api_key

    @property
    def name(self) -> str:
        return PLUGIN_NAME

    def fetch(self, since_unix: int) -> FetchResult:
        """Fetch general-news items published since `since_unix`.

        Server-side filter is not available on `/news?category=general`
        (verified with a live call); filtering is client-side on the
        `datetime` field.

        Defense in depth: a top-level try/except wraps the whole method
        so even an unexpected exception (e.g. a future urllib change)
        surfaces as `status="error"` rather than propagating up to the
        scrape orchestrator.
        """
        fetched_at = int(time.time())
        try:
            if not self._api_key:
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail="FINNHUB_API_KEY not set",
                )

            resp = self._http.get_json(
                FINNHUB_BASE_URL,
                params={"category": "general", "token": self._api_key},
            )

            if resp.status == "rate_limited":
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="rate_limited",
                    error_detail=resp.error_detail,
                )
            if resp.status != "ok":
                detail = resp.error_detail or f"http_status={resp.http_status_code}"
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail=detail,
                )

            payload = resp.json
            if not isinstance(payload, list):
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail=f"unexpected response shape: expected list, got {type(payload).__name__}",
                )

            items, drops = self._parse_items(payload, since_unix)
            if drops > 0:
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=items,
                    status="partial",
                    error_detail=f"dropped {drops} malformed item(s)",
                )
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=items,
                status="ok",
            )
        except Exception as exc:  # noqa: BLE001 — contract is "never raise"
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=[],
                status="error",
                error_detail=f"unexpected exception: {type(exc).__name__}: {exc}",
            )

    def rate_limit_budget_remaining(self) -> float:
        """Optimistic. Local Finnhub call accounting is not tracked.

        If we hit 429, the scrape layer marks the source unhealthy
        (`source_health.consecutive_failure_count` ticks up) and the
        next cycle skips or surfaces the issue.
        """
        return 1.0

    # ---- parsing helpers ----

    def _parse_items(
        self,
        payload: list[Any],
        since_unix: int,
    ) -> tuple[list[FetchedItem], int]:
        items: list[FetchedItem] = []
        drops = 0
        for raw in payload:
            parsed = self._parse_one(raw)
            if parsed is None:
                drops += 1
                continue
            if parsed.published_at_unix < since_unix:
                # Outside the time window: not a parse drop, just filtered.
                continue
            items.append(parsed)
        return items, drops

    def _parse_one(self, raw: Any) -> FetchedItem | None:
        if not isinstance(raw, dict):
            return None
        try:
            # Required: id, datetime, headline. Others are optional.
            raw_id = raw["id"]
            published = int(raw["datetime"])
            headline = raw["headline"]
        except (KeyError, TypeError, ValueError):
            return None
        if not isinstance(headline, str) or not headline.strip():
            return None
        return FetchedItem(
            source_item_id=str(raw_id),
            headline=headline,
            url=_str_or_none(raw.get("url")),
            published_at_unix=published,
            raw_source=_str_or_none(raw.get("source")),
            tickers=_parse_tickers(raw.get("related", "")),
            raw_body=None,
        )


def _parse_tickers(related: Any) -> list[str]:
    """Parse `related` CSV: split on comma, strip, drop empties, uppercase.

    Per the live-call verification, `related` is almost always an empty
    string on `/news?category=general`. The function still handles the
    documented multi-ticker shape (e.g. `"AAPL,GOOGL,MSFT"`) so that
    Pass B sources sharing this normalization can reuse the helper.
    Dots and hyphens in tickers (e.g. `MOG.A`, `BRK.B`) are preserved.
    """
    if not related or not isinstance(related, str):
        return []
    out: list[str] = []
    for token in related.split(","):
        cleaned = token.strip().upper()
        if cleaned:
            out.append(cleaned)
    return out


def _str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v if v else None
    return str(v)


__all__ = ["FINNHUB_BASE_URL", "PLUGIN_NAME", "FinnhubGeneralNewsSource"]
