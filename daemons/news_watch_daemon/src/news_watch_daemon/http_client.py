"""Small stdlib HTTP client — no retries, never raises.

News Watch owns this client independently of Research Daemon. Both
daemons mirror the same disciplines but share zero code so they can be
deployed independently.

Contract:

  - Every call returns an `HttpResponse` dataclass. Network failures,
    parse errors, and unexpected HTTP statuses surface as
    `status != "ok"` with `error_detail` populated. Callers must check
    `status` before reading `body`/`json`.
  - No internal retries. Retry policy lives in source plugins or the
    scrape orchestrator, where it can be combined with per-source
    failure counters and rate-limit budgets.
  - Built on `urllib.request` (stdlib). The brief deliberately rules
    out `requests` and `httpx`; the only Pass A external dependency is
    `feedparser` (for RSS).
  - URLs containing secret query params (`token`, `api[_-]?key`,
    `apikey`) are redacted in any log emission.
"""

from __future__ import annotations

import json
import logging
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from typing import Any, Literal


HttpStatus = Literal["ok", "rate_limited", "not_found", "error"]


# Matches `?token=…`, `&token=…`, `?api_key=…`, etc. for log output only.
_SECRET_QUERY_RE = re.compile(
    r"([?&](?:token|api[_-]?key|apikey)=)[^&#\s]+",
    re.IGNORECASE,
)


def redact_url(url: str) -> str:
    """Replace secret query-param values with `***` for safe logging."""
    return _SECRET_QUERY_RE.sub(r"\1***", url)


@dataclass(frozen=True)
class HttpResponse:
    """Outcome of a single HTTP GET. Never raised — always returned."""

    status: HttpStatus
    http_status_code: int | None
    body: str | None
    json: Any | None
    error_detail: str | None
    elapsed_ms: int


def _is_json_content_type(content_type: str | None) -> bool:
    if not content_type:
        return False
    main = content_type.split(";", 1)[0].strip().lower()
    return main == "application/json" or main.endswith("+json")


def _parse_retry_after(raw: str | None) -> str | None:
    """Format the Retry-After value into a stable error_detail suffix.

    RFC 7231 allows both integer seconds and HTTP-date forms. Try int
    first; on parse failure surface the raw string. Either form is
    useful signal to the caller — don't reject non-numeric values.
    """
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        seconds = int(raw)
        return f"retry_after_seconds={seconds}"
    except ValueError:
        return f"retry_after={raw}"


class HttpClient:
    """Small HTTP client with timeouts and rate-limit awareness."""

    def __init__(
        self,
        *,
        user_agent: str,
        default_timeout_s: float = 10.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if not user_agent:
            raise ValueError("user_agent must be a non-empty string")
        if default_timeout_s <= 0:
            raise ValueError("default_timeout_s must be positive")
        self._user_agent = user_agent
        self._default_timeout_s = default_timeout_s
        self._log = logger or logging.getLogger("news_watch_daemon.http")

    @property
    def user_agent(self) -> str:
        return self._user_agent

    @property
    def default_timeout_s(self) -> float:
        return self._default_timeout_s

    # ---- public methods ----

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout_s: float | None = None,
    ) -> HttpResponse:
        """GET expecting JSON. On 2xx with parseable JSON: status='ok'."""
        raw = self._perform(
            url,
            params=params,
            headers=headers,
            timeout_s=timeout_s,
            accept="application/json",
        )
        if raw.status != "ok":
            return raw
        if raw.body is None:
            return replace(
                raw,
                status="error",
                json=None,
                error_detail="json_parse_error: empty body",
            )
        try:
            parsed = json.loads(raw.body)
        except json.JSONDecodeError as exc:
            return replace(
                raw,
                status="error",
                json=None,
                error_detail=f"json_parse_error: {exc}",
            )
        return replace(raw, json=parsed)

    def get_text(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout_s: float | None = None,
    ) -> HttpResponse:
        """GET expecting text or bytes-as-text. JSON is parsed opportunistically."""
        raw = self._perform(
            url,
            params=params,
            headers=headers,
            timeout_s=timeout_s,
            accept="*/*",
        )
        # For get_text we don't error on missing JSON — it's a courtesy parse.
        return raw

    # ---- internals ----

    def _build_url(self, url: str, params: dict[str, str] | None) -> str:
        if not params:
            return url
        sep = "&" if ("?" in url) else "?"
        return f"{url}{sep}{urllib.parse.urlencode(params)}"

    def _build_headers(
        self,
        *,
        accept: str,
        headers: dict[str, str] | None,
    ) -> dict[str, str]:
        merged: dict[str, str] = {"User-Agent": self._user_agent, "Accept": accept}
        if headers:
            merged.update(headers)
        return merged

    def _perform(
        self,
        url: str,
        *,
        params: dict[str, str] | None,
        headers: dict[str, str] | None,
        timeout_s: float | None,
        accept: str,
    ) -> HttpResponse:
        full_url = self._build_url(url, params)
        merged_headers = self._build_headers(accept=accept, headers=headers)
        effective_timeout = timeout_s if timeout_s is not None else self._default_timeout_s
        safe_url = redact_url(full_url)

        req = urllib.request.Request(full_url, headers=merged_headers, method="GET")
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                body_bytes = resp.read()
                http_status = getattr(resp, "status", None) or resp.getcode()
                content_type = resp.headers.get("Content-Type")
                elapsed_ms = self._elapsed_ms(start)
            body_text = body_bytes.decode("utf-8", errors="replace")
            parsed_json: Any | None = None
            if _is_json_content_type(content_type):
                try:
                    parsed_json = json.loads(body_text)
                except json.JSONDecodeError:
                    # Don't promote to error here — get_json owns that
                    # decision. get_text is best-effort.
                    parsed_json = None
            return HttpResponse(
                status="ok",
                http_status_code=int(http_status),
                body=body_text,
                json=parsed_json,
                error_detail=None,
                elapsed_ms=elapsed_ms,
            )
        except urllib.error.HTTPError as exc:
            elapsed_ms = self._elapsed_ms(start)
            try:
                body_bytes = exc.read() or b""
                body_text = body_bytes.decode("utf-8", errors="replace") or None
            except Exception:  # noqa: BLE001 — diagnostic read; never propagate
                body_text = None
            return self._classify_http_error(exc, body_text, elapsed_ms, safe_url)
        except urllib.error.URLError as exc:
            elapsed_ms = self._elapsed_ms(start)
            reason = exc.reason
            reason_text = (
                f"{type(reason).__name__}: {reason}"
                if isinstance(reason, BaseException)
                else str(reason)
            )
            self._log.warning("http request failed for %s: %s", safe_url, reason_text)
            return HttpResponse(
                status="error",
                http_status_code=None,
                body=None,
                json=None,
                error_detail=f"URLError: {reason_text}",
                elapsed_ms=elapsed_ms,
            )
        except (TimeoutError, socket.timeout) as exc:
            # On Python 3.10+ socket.timeout IS TimeoutError; keep both for
            # explicitness against older subclassing.
            elapsed_ms = self._elapsed_ms(start)
            self._log.warning("http timeout for %s: %s", safe_url, exc)
            return HttpResponse(
                status="error",
                http_status_code=None,
                body=None,
                json=None,
                error_detail=f"{type(exc).__name__}: {exc}",
                elapsed_ms=elapsed_ms,
            )
        except OSError as exc:
            elapsed_ms = self._elapsed_ms(start)
            self._log.warning("http OS error for %s: %s", safe_url, exc)
            return HttpResponse(
                status="error",
                http_status_code=None,
                body=None,
                json=None,
                error_detail=f"{type(exc).__name__}: {exc}",
                elapsed_ms=elapsed_ms,
            )

    def _classify_http_error(
        self,
        exc: urllib.error.HTTPError,
        body_text: str | None,
        elapsed_ms: int,
        safe_url: str,
    ) -> HttpResponse:
        code = exc.code
        if code == 429:
            retry_after = _parse_retry_after(exc.headers.get("Retry-After"))
            self._log.warning("http 429 for %s (%s)", safe_url, retry_after or "no retry-after")
            return HttpResponse(
                status="rate_limited",
                http_status_code=429,
                body=body_text,
                json=None,
                error_detail=retry_after,
                elapsed_ms=elapsed_ms,
            )
        if code == 404:
            return HttpResponse(
                status="not_found",
                http_status_code=404,
                body=body_text,
                json=None,
                error_detail=f"http_404: {safe_url}",
                elapsed_ms=elapsed_ms,
            )
        if 500 <= code < 600:
            self._log.warning("http %d for %s", code, safe_url)
            return HttpResponse(
                status="error",
                http_status_code=code,
                body=body_text,
                json=None,
                error_detail=f"http_5xx: {code}",
                elapsed_ms=elapsed_ms,
            )
        # Other 4xx
        self._log.warning("http %d for %s", code, safe_url)
        return HttpResponse(
            status="error",
            http_status_code=code,
            body=body_text,
            json=None,
            error_detail=f"http_{code}",
            elapsed_ms=elapsed_ms,
        )

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        return max(0, int((time.perf_counter() - start) * 1000))


__all__ = [
    "HttpClient",
    "HttpResponse",
    "HttpStatus",
    "redact_url",
]
