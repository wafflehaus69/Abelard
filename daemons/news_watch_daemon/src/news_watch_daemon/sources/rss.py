"""RSS / Atom source plugin.

One concrete `SourcePlugin` per unique feed URL across active themes.
HTTP transport goes through our shared `HttpClient` so retry / timeout /
URL-redaction semantics are uniform across daemons; parsing is handled
by `feedparser` (handles RSS 2.0, Atom, and the malformed-feed edge
cases we don't want to reimplement).

Theme association is downstream — RSS entries carry no ticker tagging,
so `FetchedItem.tickers` is always empty. The scrape orchestrator's
keyword-regex match step attaches headlines to themes.
"""

from __future__ import annotations

import calendar
import hashlib
import re
import time
from typing import Any

import feedparser  # type: ignore[import-untyped]

from ..http_client import HttpClient
from .base import FetchedItem, FetchResult, SourcePlugin


# ---------- feed ID derivation ----------

_FEED_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_-]")
_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.-]*://", re.IGNORECASE)
_FEED_ID_SLUG_MAX = 64
_FEED_ID_HASH_LEN = 6


def derive_feed_id(feed_url: str) -> str:
    """Stable, filesystem-safe slug from a feed URL.

    Rules (per Pass A brief):
      1. Strip scheme (http://, https://, etc.)
      2. Replace any character not in [a-zA-Z0-9_-] with `_`
      3. Lowercase
      4. Truncate to 64 chars
      5. Append a 6-char hex hash of the original URL

    Two different URLs that slug to the same value disambiguate via the
    hash suffix. Same URL always produces the same feed_id.
    """
    if not feed_url:
        raise ValueError("feed_url must be a non-empty string")
    no_scheme = _SCHEME_RE.sub("", feed_url)
    slug = _FEED_ID_SAFE_RE.sub("_", no_scheme).lower()[:_FEED_ID_SLUG_MAX]
    digest = hashlib.sha256(feed_url.encode("utf-8")).hexdigest()[:_FEED_ID_HASH_LEN]
    return f"{slug}_{digest}"


# ---------- plugin ----------


class RssSource(SourcePlugin):
    """RSS 2.0 / Atom feed reader."""

    def __init__(
        self,
        http_client: HttpClient,
        *,
        feed_url: str,
        feed_id: str | None = None,
    ) -> None:
        if not feed_url:
            raise ValueError("feed_url must be a non-empty string")
        self._http = http_client
        self._feed_url = feed_url
        self._feed_id = feed_id or derive_feed_id(feed_url)

    @property
    def name(self) -> str:
        return f"rss:{self._feed_id}"

    @property
    def feed_url(self) -> str:
        return self._feed_url

    @property
    def feed_id(self) -> str:
        return self._feed_id

    def fetch(self, since_unix: int) -> FetchResult:
        fetched_at = int(time.time())
        try:
            resp = self._http.get_text(self._feed_url)
            if resp.status == "rate_limited":
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="rate_limited",
                    error_detail=resp.error_detail,
                )
            if resp.status == "not_found":
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail=resp.error_detail or "http_404",
                )
            if resp.status != "ok":
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail=resp.error_detail
                    or f"http_status={resp.http_status_code}",
                )
            if resp.body is None:
                return FetchResult(
                    source=self.name,
                    fetched_at_unix=fetched_at,
                    items=[],
                    status="error",
                    error_detail="empty response body",
                )
            return self._parse_feed(resp.body, since_unix, fetched_at)
        except Exception as exc:  # noqa: BLE001 — never raise contract
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=[],
                status="error",
                error_detail=f"unexpected exception: {type(exc).__name__}: {exc}",
            )

    def rate_limit_budget_remaining(self) -> float:
        # RSS feeds aren't rate-limited in the API sense; polite scrape
        # cadence is the orchestrator's job (one fetch per cycle).
        return 1.0

    # ---- parsing ----

    def _parse_feed(self, body: str, since_unix: int, fetched_at: int) -> FetchResult:
        parsed = feedparser.parse(body)
        feed_title = parsed.feed.get("title") if hasattr(parsed, "feed") else None
        raw_source = feed_title if isinstance(feed_title, str) and feed_title.strip() else None

        items: list[FetchedItem] = []
        drops = 0
        for entry in parsed.entries:
            item = self._parse_entry(entry, raw_source=raw_source)
            if item is None:
                drops += 1
                continue
            if item.published_at_unix < since_unix:
                continue
            items.append(item)

        bozo = bool(getattr(parsed, "bozo", False)) and not items and not parsed.entries
        # If feedparser flagged bozo and yielded zero entries → error
        if bozo:
            exc = getattr(parsed, "bozo_exception", None)
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=[],
                status="error",
                error_detail=f"feed parse error: {type(exc).__name__ if exc else 'unknown'}: {exc}",
            )

        # If feedparser flagged bozo BUT we still got entries, surface partial
        if getattr(parsed, "bozo", False) and parsed.entries:
            exc = getattr(parsed, "bozo_exception", None)
            detail = f"bozo: {type(exc).__name__ if exc else 'unknown'}: {exc}"
            if drops:
                detail = f"{detail}; dropped {drops} entr{'y' if drops == 1 else 'ies'}"
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=items,
                status="partial",
                error_detail=detail,
            )

        if drops > 0:
            return FetchResult(
                source=self.name,
                fetched_at_unix=fetched_at,
                items=items,
                status="partial",
                error_detail=f"dropped {drops} entr{'y' if drops == 1 else 'ies'} with no timestamp",
            )
        return FetchResult(
            source=self.name,
            fetched_at_unix=fetched_at,
            items=items,
            status="ok",
        )

    def _parse_entry(self, entry: Any, *, raw_source: str | None) -> FetchedItem | None:
        published_struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if published_struct is None:
            return None
        try:
            published_unix = calendar.timegm(published_struct)
        except (TypeError, ValueError):
            return None

        title = entry.get("title")
        if not isinstance(title, str) or not title.strip():
            return None
        link = entry.get("link")
        url = link if isinstance(link, str) and link else None

        entry_id = entry.get("id")
        if isinstance(entry_id, str) and entry_id:
            source_item_id = entry_id
        else:
            # Fallback: deterministic hash of (link, published_unix). Stable
            # across runs for the same entry.
            basis = f"{url or ''}|{published_unix}"
            source_item_id = hashlib.sha256(basis.encode("utf-8")).hexdigest()[:32]

        return FetchedItem(
            source_item_id=source_item_id,
            headline=title,
            url=url,
            published_at_unix=published_unix,
            raw_source=raw_source,
            tickers=[],
            raw_body=None,
        )


__all__ = ["RssSource", "derive_feed_id"]
