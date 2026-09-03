"""Transport. GET only, and that is the whole of it.

Reuse by import, not by copy: `abelard_common.HttpClient` already carries
retry/backoff, 429 handling, forced UTF-8 and secret redaction. Scout's fetch
layer wraps the same client and additionally exposes `post_graphql`, because a
GraphQL read needs a POST.

**This module exposes no POST.** Not a disabled one, not a private one. The
Builder reads repositories and nothing else, so a write verb here would have no
legitimate caller and its mere presence would put invariant 2 one argument away
from being false. `tests/test_soul.py` asserts that `post_json` appears nowhere
in this package.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import re

from abelard_common.http_client import HttpClient, NotFound, RateLimited, TransportError

from . import config
from .errors import FetchError, GoneError

#: The shared client types 404 and 429 but folds every other non-ok status into
#: a generic `TransportError` whose message begins "<status> from <url>". A 410
#: is a determinate answer and must not be retried or treated as a network
#: fault, so the status is recovered here -- in ONE place, deliberately, rather
#: than by widening a client four other daemons depend on.
_STATUS_PREFIX = re.compile(r"^(\d{3}) from ")


def _status_of(exc: Exception) -> int | None:
    m = _STATUS_PREFIX.match(str(exc))
    return int(m.group(1)) if m else None

#: Served with a 200 but carrying no content. Inherited from scout: an
#: interstitial is a failure, not an empty document.
_BLOCK_MARKERS = (
    "just a moment...",
    "attention required!",
    "access denied",
    "please enable javascript",
)


@dataclass(frozen=True)
class Document:
    """One fetched artifact, with the URL that produced it.

    `url` is carried because the provenance packet must name every source read,
    and a body without its origin cannot be cited.
    """

    url: str
    text: str
    found: bool

    def __bool__(self) -> bool:
        return self.found


def build_client(logger: logging.Logger | None = None) -> HttpClient:
    return HttpClient(
        user_agent=config.USER_AGENT,
        timeout=config.HTTP_TIMEOUT_S,
        logger=logger,
    )


def get_text(client: HttpClient, url: str, *, optional: bool = False) -> Document:
    """GET a document.

    `optional=True` turns a 404 into a `found=False` Document rather than an
    error, because "this project has no CONTRIBUTING.md" is a finding, not a
    failure. Every other error still raises: a timeout is not evidence of
    absence, and treating it as such would let a flaky network silently clear a
    policy gate.
    """
    try:
        text = client.get_text(url)
    except NotFound:
        if optional:
            return Document(url=url, text="", found=False)
        raise FetchError(f"not found: {url}") from None
    except (RateLimited, TransportError) as exc:
        if _status_of(exc) == 410:
            raise GoneError(f"{url}: gone (410)") from exc
        raise FetchError(f"{url}: {exc}") from exc

    low = text[:4000].lower()
    for marker in _BLOCK_MARKERS:
        if marker in low:
            raise FetchError(f"{url}: blocked or challenged (matched {marker!r})")
    return Document(url=url, text=text, found=True)


def get_json(client: HttpClient, url: str, *, optional: bool = False):
    """GET a JSON endpoint. Same absence-vs-failure rule as `get_text`."""
    try:
        return client.get_json(url)
    except NotFound:
        if optional:
            return None
        raise FetchError(f"not found: {url}") from None
    except (RateLimited, TransportError) as exc:
        if _status_of(exc) == 410:
            raise GoneError(f"{url}: gone (410)") from exc
        raise FetchError(f"{url}: {exc}") from exc


__all__ = ["Document", "build_client", "get_text", "get_json"]
