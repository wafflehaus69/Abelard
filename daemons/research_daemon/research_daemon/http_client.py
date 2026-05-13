"""Shared HTTP client: retry with exponential backoff, rate-limit awareness,
and query-string redaction for logs.

Callers map the raised exceptions to envelope statuses:
    NotFound     -> "not_found"
    RateLimited  -> "rate_limited"
    TransportError -> "error"
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests


DEFAULT_TIMEOUT = 10.0
DEFAULT_RETRIES = 3
DEFAULT_BASE_BACKOFF = 0.5

_SECRET_QUERY_RE = re.compile(
    r"([?&](?:token|api[_-]?key|apikey)=)[^&#\s]+",
    re.IGNORECASE,
)


def redact_url(url: str) -> str:
    return _SECRET_QUERY_RE.sub(r"\1***", url)


class TransportError(RuntimeError):
    """Network error or non-retryable HTTP failure after retries exhausted."""


class RateLimited(RuntimeError):
    """HTTP 429. Caller should return envelope status='rate_limited'."""


class NotFound(RuntimeError):
    """HTTP 404. Caller should return envelope status='not_found'."""


@dataclass
class HttpClient:
    user_agent: str
    default_headers: dict[str, str] = field(default_factory=dict)
    session: requests.Session | None = None
    max_retries: int = DEFAULT_RETRIES
    base_backoff: float = DEFAULT_BASE_BACKOFF
    timeout: float = DEFAULT_TIMEOUT
    logger: logging.Logger | None = None

    def __post_init__(self) -> None:
        if self.session is None:
            self.session = requests.Session()
        if self.logger is None:
            self.logger = logging.getLogger("research_daemon.http")

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> Any:
        return self._request(url, params=params, headers=headers, timeout=timeout).json()

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        return self._request(url, params=params, headers=headers, timeout=timeout).text

    def _request(
        self,
        url: str,
        *,
        params: dict[str, Any] | None,
        headers: dict[str, str] | None,
        timeout: float | None = None,
    ) -> requests.Response:
        assert self.session is not None
        assert self.logger is not None
        merged = {"User-Agent": self.user_agent, **self.default_headers, **(headers or {})}
        effective_timeout = timeout if timeout is not None else self.timeout
        last_exc: Exception | None = None
        safe_url = redact_url(url)

        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, headers=merged, timeout=effective_timeout)
            except requests.RequestException as exc:
                last_exc = exc
                self.logger.warning(
                    "http attempt %d/%d failed for %s: %s",
                    attempt + 1, self.max_retries, safe_url, exc,
                )
                self._sleep_before_retry(attempt)
                continue

            status = resp.status_code

            if status == 404:
                raise NotFound(f"{safe_url} -> 404")

            if status == 429:
                if attempt < self.max_retries - 1:
                    wait = self._retry_after_seconds(resp) or self._backoff(attempt)
                    self.logger.warning("http 429 for %s; sleeping %.2fs", safe_url, wait)
                    time.sleep(wait)
                    continue
                raise RateLimited(f"{safe_url} -> 429")

            if 500 <= status < 600:
                last_exc = TransportError(f"{status} from {safe_url}")
                self.logger.warning(
                    "http attempt %d/%d got %d for %s",
                    attempt + 1, self.max_retries, status, safe_url,
                )
                self._sleep_before_retry(attempt)
                continue

            if not resp.ok:
                body = (resp.text or "")[:200]
                raise TransportError(f"{status} from {safe_url}: {body}")

            return resp

        raise TransportError(f"exhausted retries for {safe_url}: {last_exc}")

    def _sleep_before_retry(self, attempt: int) -> None:
        if attempt < self.max_retries - 1:
            time.sleep(self._backoff(attempt))

    def _backoff(self, attempt: int) -> float:
        return self.base_backoff * (2 ** attempt) + random.uniform(0, 0.1)

    @staticmethod
    def _retry_after_seconds(resp: requests.Response) -> float | None:
        raw = resp.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
