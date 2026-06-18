"""4chan /biz/ client: catalog discovery, /smg/ thread scrape, HTML cleaning.

Read-only access to a public JSON API. Respects the API contract:
  - >= 1.0s between any two requests to a.4cdn.org (throttle on the fetcher).
  - Sends `If-Modified-Since` on re-fetch within a session; honors 304 as a
    no-change state, not an error.

Fail loud: a non-200/304 response, malformed JSON, or zero live /smg/ threads
each surface as a structured error — never as an empty success.
"""

from __future__ import annotations

import html
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import requests

from .errors import DaemonError

BASE = "https://a.4cdn.org/biz"
CATALOG_URL = f"{BASE}/catalog.json"
SMG_MARKER = "/smg/"
MIN_REQUEST_INTERVAL_S = 1.0

# NOTE: logger name preserved as "biz_daemon.fourchan" (not __name__) so child
# records keep propagating to the "biz_daemon" root logger and its redacting
# filter — renaming it here would change logging/redaction routing, a behavior
# change out of scope for the Order 0 extraction. Revisit when ChatterDaemon
# wires its own logging.
_log = logging.getLogger("biz_daemon.fourchan")

# Order matters in clean_com: anchors (quotelinks) carry text we want to drop
# entirely, so they must be removed before the generic tag strip.
_QUOTELINK_RE = re.compile(r"<a\b[^>]*>.*?</a>", re.IGNORECASE | re.DOTALL)
_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_WBR_RE = re.compile(r"<wbr\s*/?>", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


class FourchanError(DaemonError):
    """A fetch or parse failure against a.4cdn.org."""

    def __init__(self, message: str) -> None:
        super().__init__(message, stage="fourchan")


class NoSmgThreadError(DaemonError):
    """No live /smg/ thread found — a legitimate state, surfaced loudly.

    This is NOT an empty success. The caller folds it into the `errors` array
    as 'no live /smg/ thread found'.
    """

    def __init__(self) -> None:
        super().__init__("no live /smg/ thread found", stage="fourchan")


def clean_com(raw: str | None) -> str:
    """Strip 4chan comment HTML to plain text.

    - <br> becomes a newline.
    - Quotelinks (`<a ...>&gt;&gt;123</a>`) are reply pointers — dropped whole.
    - Greentext `<span class="quote">` wrappers are removed, inner text kept.
    - <wbr> word-break hints are removed (they fragment long tokens).
    - HTML entities are decoded last.
    Image-only posts (no com) become "".
    """
    if not raw:
        return ""
    text = _BR_RE.sub("\n", raw)
    text = _QUOTELINK_RE.sub("", text)
    text = _WBR_RE.sub("", text)
    text = _TAG_RE.sub("", text)  # drops span wrappers, keeps greentext content
    text = html.unescape(text)
    return text.strip()


@dataclass
class Thread:
    no: int
    subject: str
    posts: list[dict[str, Any]] = field(default_factory=list)

    @property
    def post_count(self) -> int:
        return len(self.posts)


@dataclass
class Fetcher:
    """Throttled, conditional-request HTTP fetcher for a.4cdn.org.

    One Fetcher per scrape. The throttle and the per-URL Last-Modified cache
    live on the instance; an on-demand invocation builds exactly one Fetcher,
    so this is the module-level throttle in practice.
    """

    user_agent: str
    timeout: float = 10.0
    session: requests.Session | None = None
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    _last_request_at: float | None = field(default=None, init=False)
    _last_modified: dict[str, str] = field(default_factory=dict, init=False)
    _cached_json: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()

    def _throttle(self) -> None:
        if self._last_request_at is not None:
            elapsed = self.clock() - self._last_request_at
            wait = MIN_REQUEST_INTERVAL_S - elapsed
            if wait > 0:
                self.sleep(wait)
        self._last_request_at = self.clock()

    def get_json(self, url: str) -> Any:
        """GET JSON with throttle + conditional request. 304 returns cached.

        Raises FourchanError on a non-200/304 status or malformed JSON.
        """
        assert self.session is not None
        self._throttle()
        headers = {"User-Agent": self.user_agent}
        prior = self._last_modified.get(url)
        if prior is not None:
            headers["If-Modified-Since"] = prior

        try:
            resp = self.session.get(url, headers=headers, timeout=self.timeout)
        except requests.RequestException as exc:
            raise FourchanError(f"request failed for {url}: {exc}") from exc

        if resp.status_code == 304:
            if url not in self._cached_json:
                raise FourchanError(f"304 for {url} with no cached payload")
            _log.debug("304 not-modified for %s", url)
            return self._cached_json[url]

        if resp.status_code != 200:
            raise FourchanError(f"{resp.status_code} from {url}")

        # a.4cdn.org serves UTF-8 JSON. Force it — never let requests infer the
        # encoding from headers/chardet or fall back to a platform default
        # (cp1252 on Windows), which mis-decodes non-ASCII (en-dashes, smart
        # quotes, accents) into mojibake. Corrupted bytes adjacent to a ticker
        # eat its \b word boundary, so the ticker silently fails to extract.
        resp.encoding = "utf-8"
        try:
            data = resp.json()
        except ValueError as exc:
            raise FourchanError(f"malformed JSON from {url}: {exc}") from exc

        last_mod = resp.headers.get("Last-Modified")
        if last_mod:
            self._last_modified[url] = last_mod
        self._cached_json[url] = data
        return data


def discover_smg_thread_nos(fetcher: Fetcher) -> list[tuple[int, str]]:
    """Return (no, subject) for every thread whose OP subject contains /smg/.

    Subject only (not the comment body), case-insensitive substring match.
    """
    catalog = fetcher.get_json(CATALOG_URL)
    if not isinstance(catalog, list):
        raise FourchanError("catalog.json was not a list of pages")

    matches: list[tuple[int, str]] = []
    for page in catalog:
        for op in page.get("threads", []):
            subject = op.get("sub") or ""
            if SMG_MARKER in subject.lower():
                matches.append((int(op["no"]), subject))
    return matches


def scrape_thread(fetcher: Fetcher, no: int) -> Thread:
    """Fetch one thread; return all posts with their `com` cleaned in place."""
    data = fetcher.get_json(f"{BASE}/thread/{no}.json")
    posts = data.get("posts")
    if not isinstance(posts, list) or not posts:
        raise FourchanError(f"thread {no} returned no posts")

    subject = posts[0].get("sub") or ""
    cleaned: list[dict[str, Any]] = []
    for post in posts:
        cleaned.append(
            {
                "no": int(post["no"]),
                "com": clean_com(post.get("com")),
            }
        )
    return Thread(no=no, subject=subject, posts=cleaned)


def scrape_smg(fetcher: Fetcher) -> list[Thread]:
    """Discover and scrape every live /smg/ thread. Loud-fails on none found."""
    nos = discover_smg_thread_nos(fetcher)
    if not nos:
        raise NoSmgThreadError()
    threads = [scrape_thread(fetcher, no) for no, _subject in nos]
    return threads
