"""HTTP client tests — patched at the urllib.request.urlopen layer.

Every test is hermetic: no real network, no real sockets. The client's
contract is "never raise, surface everything via HttpResponse"; the
tests assert that contract for every error path.
"""

from __future__ import annotations

import email.message
import io
import json
import socket
import urllib.error
from typing import Any
from unittest.mock import patch

import pytest

from news_watch_daemon.http_client import (
    HttpClient,
    HttpResponse,
    redact_url,
)


# ---------- fake response plumbing ----------


class _FakeResponse:
    """Minimal stand-in for the object urllib.request.urlopen yields."""

    def __init__(
        self,
        body: bytes = b"",
        status: int = 200,
        content_type: str | None = "application/json",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._body = body
        self.status = status
        headers = email.message.Message()
        if content_type is not None:
            headers["Content-Type"] = content_type
        for k, v in (extra_headers or {}).items():
            headers[k] = v
        self.headers = headers

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *args: Any) -> bool:
        return False


def _http_error(code: int, *, retry_after: str | None = None, body: bytes = b"") -> urllib.error.HTTPError:
    hdrs = email.message.Message()
    if retry_after is not None:
        hdrs["Retry-After"] = retry_after
    return urllib.error.HTTPError(
        url="http://example.invalid/",
        code=code,
        msg=f"HTTP {code}",
        hdrs=hdrs,  # type: ignore[arg-type]
        fp=io.BytesIO(body),
    )


@pytest.fixture
def client():
    return HttpClient(user_agent="news-watch-daemon/test", default_timeout_s=5.0)


# ---------- constructor ----------


def test_client_requires_non_empty_user_agent():
    with pytest.raises(ValueError):
        HttpClient(user_agent="", default_timeout_s=5.0)


def test_client_requires_positive_timeout():
    with pytest.raises(ValueError):
        HttpClient(user_agent="ua", default_timeout_s=0)
    with pytest.raises(ValueError):
        HttpClient(user_agent="ua", default_timeout_s=-1.0)


def test_client_exposes_user_agent_and_timeout(client):
    assert client.user_agent == "news-watch-daemon/test"
    assert client.default_timeout_s == 5.0


# ---------- happy paths ----------


def test_get_json_happy_path(client):
    payload = {"a": 1, "items": [1, 2, 3]}
    fake = _FakeResponse(body=json.dumps(payload).encode(), status=200)
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_json("http://example.invalid/api")
    assert resp.status == "ok"
    assert resp.http_status_code == 200
    assert resp.json == payload
    assert resp.body == json.dumps(payload)
    assert resp.error_detail is None
    assert resp.elapsed_ms >= 0


def test_get_text_happy_path_with_non_json_content(client):
    fake = _FakeResponse(body=b"plain hello", status=200, content_type="text/plain")
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_text("http://example.invalid/")
    assert resp.status == "ok"
    assert resp.body == "plain hello"
    assert resp.json is None


def test_get_text_opportunistically_parses_json_when_content_type_says_so(client):
    fake = _FakeResponse(body=b'{"k": "v"}', status=200, content_type="application/json")
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_text("http://example.invalid/")
    assert resp.status == "ok"
    assert resp.json == {"k": "v"}


def test_get_text_json_parse_failure_does_not_error(client):
    fake = _FakeResponse(body=b"not json", status=200, content_type="application/json")
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_text("http://example.invalid/")
    assert resp.status == "ok"  # get_text is best-effort
    assert resp.json is None


# ---------- request shape ----------


def test_get_json_sets_user_agent_and_accept_json(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json("http://example.invalid/api")
    headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers["user-agent"] == "news-watch-daemon/test"
    assert headers["accept"] == "application/json"


def test_get_text_uses_wildcard_accept(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["headers"] = dict(req.header_items())
        return _FakeResponse(body=b"", content_type="text/plain")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_text("http://example.invalid/")
    headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers["accept"] == "*/*"


def test_caller_headers_override_defaults(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["headers"] = dict(req.header_items())
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json("http://example.invalid/", headers={"X-Custom": "yes", "Accept": "application/vnd+json"})
    headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers["x-custom"] == "yes"
    assert headers["accept"] == "application/vnd+json"


def test_params_are_url_encoded_and_appended(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["url"] = req.full_url
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json(
            "http://example.invalid/api",
            params={"category": "general", "token": "abc&xyz"},
        )
    # urlencode escapes the ampersand inside the token value
    assert captured["url"] == "http://example.invalid/api?category=general&token=abc%26xyz"


def test_params_merge_with_existing_query_string(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["url"] = req.full_url
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json("http://example.invalid/api?already=here", params={"more": "yes"})
    assert captured["url"] == "http://example.invalid/api?already=here&more=yes"


def test_timeout_override_per_request(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["timeout"] = timeout
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json("http://example.invalid/", timeout_s=0.5)
    assert captured["timeout"] == 0.5


def test_default_timeout_used_when_not_overridden(client):
    captured = {}

    def _capture(req, timeout):  # noqa: ARG001
        captured["timeout"] = timeout
        return _FakeResponse(body=b"{}")

    with patch("urllib.request.urlopen", side_effect=_capture):
        client.get_json("http://example.invalid/")
    assert captured["timeout"] == 5.0


# ---------- 4xx / 5xx ----------


def test_429_with_integer_retry_after(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(429, retry_after="30")):
        resp = client.get_json("http://example.invalid/?token=secret")
    assert resp.status == "rate_limited"
    assert resp.http_status_code == 429
    assert resp.error_detail == "retry_after_seconds=30"


def test_429_with_no_retry_after(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(429)):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "rate_limited"
    assert resp.error_detail is None


def test_429_with_http_date_retry_after_preserves_raw(client):
    raw = "Wed, 21 Oct 2026 07:28:00 GMT"
    with patch("urllib.request.urlopen", side_effect=_http_error(429, retry_after=raw)):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "rate_limited"
    assert resp.error_detail == f"retry_after={raw}"


def test_404_returns_not_found(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(404)):
        resp = client.get_json("http://example.invalid/missing")
    assert resp.status == "not_found"
    assert resp.http_status_code == 404
    assert resp.error_detail.startswith("http_404:")


def test_500_returns_error(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(500)):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert resp.http_status_code == 500
    assert resp.error_detail == "http_5xx: 500"


def test_503_returns_error(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(503)):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert resp.http_status_code == 503
    assert resp.error_detail == "http_5xx: 503"


def test_other_4xx_returns_error(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(401)):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert resp.http_status_code == 401
    assert resp.error_detail == "http_401"


# ---------- network failure paths ----------


def test_url_error_returns_error_with_no_status_code(client):
    err = urllib.error.URLError("name resolution failed")
    with patch("urllib.request.urlopen", side_effect=err):
        resp = client.get_json("http://nowhere.invalid/")
    assert resp.status == "error"
    assert resp.http_status_code is None
    assert "URLError" in resp.error_detail


def test_socket_timeout_returns_error(client):
    with patch("urllib.request.urlopen", side_effect=socket.timeout("timed out")):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert resp.http_status_code is None
    assert "timed out" in resp.error_detail


def test_os_error_returns_error(client):
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert "OSError" in resp.error_detail or "connection refused" in resp.error_detail


# ---------- JSON parse failure (get_json only) ----------


def test_get_json_parse_failure_returns_error(client):
    fake = _FakeResponse(body=b"<html>not json</html>", status=200, content_type="application/json")
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert "json_parse_error" in resp.error_detail
    # body is preserved even when JSON parse fails
    assert resp.body == "<html>not json</html>"


def test_get_json_empty_body_treated_as_parse_error(client):
    fake = _FakeResponse(body=b"", status=200, content_type="application/json")
    with patch("urllib.request.urlopen", return_value=fake):
        resp = client.get_json("http://example.invalid/")
    assert resp.status == "error"
    assert "json_parse_error" in resp.error_detail


# ---------- elapsed_ms ----------


def test_elapsed_ms_non_negative_on_success(client):
    with patch("urllib.request.urlopen", return_value=_FakeResponse(body=b"{}")):
        resp = client.get_json("http://example.invalid/")
    assert isinstance(resp.elapsed_ms, int)
    assert resp.elapsed_ms >= 0


def test_elapsed_ms_non_negative_on_error(client):
    with patch("urllib.request.urlopen", side_effect=_http_error(500)):
        resp = client.get_json("http://example.invalid/")
    assert isinstance(resp.elapsed_ms, int)
    assert resp.elapsed_ms >= 0


# ---------- redact_url ----------


def test_redact_url_strips_token():
    assert redact_url("https://api.example.com/v1?token=secret123&x=1") == (
        "https://api.example.com/v1?token=***&x=1"
    )


def test_redact_url_strips_api_key_variants():
    assert "***" in redact_url("https://x?api_key=ABC")
    assert "***" in redact_url("https://x?api-key=ABC")
    assert "***" in redact_url("https://x?APIKEY=abc")


def test_redact_url_preserves_other_params():
    out = redact_url("https://api.example.com/v1?category=general&token=xyz")
    assert "category=general" in out
    assert "xyz" not in out


def test_redact_url_no_change_when_no_secrets():
    url = "https://api.example.com/v1?category=general"
    assert redact_url(url) == url
