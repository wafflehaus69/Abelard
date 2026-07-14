"""Shared HttpClient — redaction, utf-8 forcing, logger DI, status mapping."""

from __future__ import annotations

import json as _json
import logging

import pytest
import requests

from abelard_common.http_client import (
    HttpClient,
    NotFound,
    RateLimited,
    TransportError,
    redact_url,
)


class FakeResponse:
    def __init__(self, status_code, *, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data
        self.headers = headers or {}
        self.ok = 200 <= status_code < 400
        self.text = text
        self.encoding = None

    def json(self):
        return self._json


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.post_calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers))
        r = self._responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    def post(self, url, json=None, params=None, headers=None, timeout=None):
        self.post_calls.append((url, json, headers))
        r = self._responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _client(responses, **kw):
    return HttpClient(user_agent="t", session=FakeSession(responses), base_backoff=0.0, **kw)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("abelard_common.http_client.time.sleep", lambda *_a, **_k: None)


def test_redact_url():
    assert redact_url("https://x/y?symbol=A&token=SECRET&from=1") == (
        "https://x/y?symbol=A&token=***&from=1"
    )
    assert "SECRET" not in redact_url("https://x?api_key=SECRET")
    assert "SECRET" not in redact_url("https://x?apikey=SECRET")


def test_default_logger_is_module_name():
    c = HttpClient(user_agent="t", session=FakeSession([]))
    assert c.logger.name == "abelard_common.http_client"


def test_injected_logger_is_used_for_emission():
    log = logging.getLogger("test.injected.http")
    log.setLevel(logging.WARNING)
    seen: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    log.addHandler(_Capture())
    c = _client([FakeResponse(500, text="e")] * 3, logger=log, max_retries=3)
    with pytest.raises(TransportError):
        c.get_json("https://x/y")
    assert any("got 500" in m for m in seen)  # emitted through the injected logger


def test_get_json_forces_utf8_regardless_of_header():
    # A real Response holding UTF-8 bytes but a MIS-inferred encoding; the client
    # must force utf-8 so non-ASCII survives (em-dash, accents).
    r = requests.Response()
    r.status_code = 200
    r._content = _json.dumps({"h": "café — déjà vu"}, ensure_ascii=False).encode("utf-8")
    r.encoding = "ISO-8859-1"
    data = _client([r]).get_json("https://x/news")
    assert data["h"] == "café — déjà vu"


def test_404_raises_not_found():
    with pytest.raises(NotFound):
        _client([FakeResponse(404)]).get_json("https://x/y")


def test_429_after_retries_raises_rate_limited():
    with pytest.raises(RateLimited):
        _client([FakeResponse(429), FakeResponse(429)], max_retries=2).get_json("https://x/y")


def test_500_exhausted_raises_transport_error():
    with pytest.raises(TransportError):
        _client([FakeResponse(500, text="e"), FakeResponse(500, text="e")], max_retries=2).get_json(
            "https://x/y"
        )


def test_post_json_sends_body_and_parses_response():
    r = FakeResponse(200, json_data={"data": {"ok": 1}})
    client = _client([r])
    out = client.post_json("https://x/graphql", json_body={"query": "{ a }"})
    assert out == {"data": {"ok": 1}}
    assert client.session.post_calls[0][1] == {"query": "{ a }"}
    assert client.session.calls == []  # never fell back to GET


def test_post_json_retries_and_maps_statuses():
    with pytest.raises(RateLimited):
        _client([FakeResponse(429), FakeResponse(429)], max_retries=2).post_json(
            "https://x/graphql", json_body={"query": "q"}
        )
    with pytest.raises(TransportError):
        _client([FakeResponse(500, text="e"), FakeResponse(500, text="e")], max_retries=2).post_json(
            "https://x/graphql", json_body={"query": "q"}
        )


def test_token_not_leaked_in_raised_message():
    # The redacted URL (not the raw token) appears in error text.
    with pytest.raises(NotFound) as ei:
        _client([FakeResponse(404)]).get_json("https://x/news?token=SECRET")
    assert "SECRET" not in str(ei.value)
    assert "token=***" in str(ei.value)
