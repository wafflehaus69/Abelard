"""HttpClient behaviour — retries, redaction, status mapping."""

from __future__ import annotations

import logging

import pytest
import requests_mock

from research_daemon.http_client import (
    HttpClient,
    NotFound,
    RateLimited,
    TransportError,
    redact_url,
)


URL = "https://example.test/api"


def test_redact_url_removes_token_query_param():
    assert "secret" not in redact_url("https://x/y?token=secret&foo=bar")
    assert "token=***" in redact_url("https://x/y?token=secret&foo=bar")


def test_redact_url_preserves_non_secret_params():
    assert "foo=bar" in redact_url("https://x/y?foo=bar&token=secret")


def test_redact_url_handles_apikey_variants():
    for param in ("api_key", "api-key", "apikey", "APIKEY"):
        redacted = redact_url(f"https://x?{param}=secret")
        assert "secret" not in redacted


def test_get_json_success(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, json={"hello": "world"})
        assert client.get_json(URL) == {"hello": "world"}


def test_get_json_404_raises_not_found(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, status_code=404)
        with pytest.raises(NotFound):
            client.get_json(URL)


def test_get_json_429_raises_rate_limited_after_retries(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, status_code=429)
        with pytest.raises(RateLimited):
            client.get_json(URL)


def test_get_json_500_retries_then_raises(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, status_code=500)
        with pytest.raises(TransportError):
            client.get_json(URL)
        # client.max_retries=2 per conftest, so exactly two attempts.
        assert m.call_count == 2


def test_get_json_recovers_after_transient_500(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, [{"status_code": 500}, {"status_code": 200, "json": {"ok": True}}])
        assert client.get_json(URL) == {"ok": True}


def test_user_agent_header_is_sent(client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(URL, json={})
        client.get_json(URL)
        assert m.last_request.headers["User-Agent"] == client.user_agent


def test_secret_is_redacted_in_log_output(client: HttpClient, caplog):
    client.logger = logging.getLogger("test_http_redact")
    caplog.set_level(logging.WARNING, logger="test_http_redact")
    with requests_mock.Mocker() as m:
        m.get(URL, status_code=500)
        with pytest.raises(TransportError):
            client.get_json(URL, params={"token": "SUPERSECRET"})
    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "SUPERSECRET" not in joined
