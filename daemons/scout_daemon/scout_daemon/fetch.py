"""Transport: a thin fail-loud layer over `abelard_common.http_client`.

Reuse by import, not by copy. `abelard_common.HttpClient` already carries
retry/backoff, 429 + `Retry-After` handling, forced UTF-8 (`http_client.py:180-181`),
secret redaction in logs and exception messages, and an injectable logger.
SC-R1 certified it and the SC-1 order forbids a fourth copy.

What this module adds is the one thing the shared client cannot know: what
counts as a *provider error dressed as success* for these particular sources.
An HTTP 200 carrying `{"errors": [...]}` from a GraphQL endpoint is a failure,
and the distinction is load-bearing rather than cosmetic -- "ok with zero
items" preserves the source's watermark, while "error" must not. Getting that
backwards silently advances the watermark past a window that had real content,
which is permanent data loss with no error surfaced.

READ-ONLY BY CONSTRUCTION. The only verbs here are GET and the POST that
GraphQL requires for a read query. Nothing in this module can create, submit,
authenticate, or mutate anything on a remote surface.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from abelard_common.http_client import (
    HttpClient,
    NotFound,
    RateLimited,
    TransportError,
)

from . import config
from .errors import FetchError

# Sentinels for provider-error-in-text. Checked on payloads that arrived with a
# 2xx, because that is precisely the case the transport layer cannot catch.
_GRAPHQL_ERROR_KEY = "errors"
_HTML_ERROR_MARKERS = (
    "just a moment...",          # Cloudflare interstitial
    "attention required!",       # Cloudflare block page
    "access denied",
    "please enable javascript",  # JS-shell served instead of content
)


@dataclass
class FetchResult:
    """Outcome of one source read.

    `ok_with_items` vs `ok_empty` is the watermark decision and is kept
    explicit rather than derived from `len(items)` at the call site, so the
    rule lives in one place.
    """

    status: str                    # 'ok' | 'empty' | 'error'
    payload: Any = None
    detail: str = ""
    resolved_url: str = ""

    @property
    def is_ok(self) -> bool:
        return self.status == "ok"

    @property
    def is_error(self) -> bool:
        return self.status == "error"


def build_client(logger: logging.Logger | None = None) -> HttpClient:
    """The daemon's single HTTP client.

    Bounded on purpose: the SDK/library defaults are generous enough to hang an
    unattended scan, and a scout that wedges overnight is worse than one that
    fails loudly at 08:00.
    """
    return HttpClient(
        user_agent=config.USER_AGENT,
        timeout=30.0,
        max_retries=3,
        logger=logger or logging.getLogger("scout_daemon.http"),
    )


def _check_json_payload(payload: Any, url: str) -> None:
    """Raise if a 2xx JSON body is actually an error envelope."""
    if isinstance(payload, dict) and payload.get(_GRAPHQL_ERROR_KEY):
        messages = payload[_GRAPHQL_ERROR_KEY]
        first = ""
        if isinstance(messages, list) and messages:
            first = str(messages[0].get("message", messages[0]))[:200]
        raise FetchError(f"provider error in 200 body from {url}: {first}")


def _check_text_payload(text: str, url: str) -> None:
    """Raise if a 2xx HTML body is a challenge/interstitial rather than content."""
    head = text[:4000].lower()
    for marker in _HTML_ERROR_MARKERS:
        if marker in head:
            raise FetchError(
                f"provider error in 200 body from {url}: matched {marker!r} "
                "(challenge or JS shell served instead of content)"
            )


def get_json(client: HttpClient, url: str, **params: Any) -> FetchResult:
    """GET a JSON endpoint. Fail-loud on transport and on error-in-200."""
    try:
        payload = client.get_json(url, params=params or None)
    except NotFound as exc:
        return FetchResult("error", detail=f"404: {exc}", resolved_url=url)
    except RateLimited as exc:
        return FetchResult("error", detail=f"rate limited: {exc}", resolved_url=url)
    except (TransportError, ValueError, json.JSONDecodeError) as exc:
        return FetchResult("error", detail=f"transport: {exc}", resolved_url=url)

    try:
        _check_json_payload(payload, url)
    except FetchError as exc:
        return FetchResult("error", detail=str(exc), resolved_url=url)

    return FetchResult("ok", payload=payload, resolved_url=url)


def post_graphql(
    client: HttpClient, url: str, query: str, variables: dict | None = None
) -> FetchResult:
    """POST a GraphQL read query.

    POST is the transport GraphQL mandates for queries; this is still a read.
    No mutation is ever constructed here, and the query strings live in the
    adapters as literals so a mutation cannot be assembled dynamically.
    """
    body: dict[str, Any] = {"query": query}
    if variables:
        body["variables"] = variables
    try:
        payload = client.post_json(url, json_body=body)
    except (TransportError, RateLimited, NotFound, ValueError) as exc:
        return FetchResult("error", detail=f"transport: {exc}", resolved_url=url)

    try:
        _check_json_payload(payload, url)
    except FetchError as exc:
        return FetchResult("error", detail=str(exc), resolved_url=url)

    data = payload.get("data") if isinstance(payload, dict) else None
    if data is None:
        return FetchResult(
            "error", detail=f"graphql 200 with no data key from {url}", resolved_url=url
        )
    return FetchResult("ok", payload=data, resolved_url=url)


def get_text(client: HttpClient, url: str) -> FetchResult:
    """GET an HTML page. Fail-loud on challenge pages served with a 200."""
    try:
        text = client.get_text(url)
    except NotFound as exc:
        return FetchResult("error", detail=f"404: {exc}", resolved_url=url)
    except RateLimited as exc:
        return FetchResult("error", detail=f"rate limited: {exc}", resolved_url=url)
    except TransportError as exc:
        return FetchResult("error", detail=f"transport: {exc}", resolved_url=url)

    try:
        _check_text_payload(text, url)
    except FetchError as exc:
        return FetchResult("error", detail=str(exc), resolved_url=url)

    return FetchResult("ok", payload=text, resolved_url=url)


__all__ = ["FetchResult", "build_client", "get_json", "post_graphql", "get_text"]
