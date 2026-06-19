"""Shared fourchan_fetch — logger DI + forced UTF-8 (the §A hardening).

UTF-8 forcing was already present (inherited from BizDaemon's fourchan_client in
the Order-0 carve); these tests pin it, and add the injectable-logger contract so
a consuming daemon's redaction filter can catch the 4chan transport's records.
"""

from __future__ import annotations

import json as _json
import logging

import requests

from abelard_common.fourchan_fetch import Fetcher


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, dict(headers or {})))
        return self._responses.pop(0)


class _FakeResp:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.encoding = None

    def json(self):
        return self._payload


def _fetcher(responses, **kw):
    return Fetcher(
        user_agent="t",
        session=_FakeSession(responses),
        sleep=lambda _s: None,
        clock=lambda: 0.0,
        **kw,
    )


def test_default_logger_is_module_name():
    f = Fetcher(user_agent="t", session=_FakeSession([]))
    assert f.logger.name == "abelard_common.fourchan_fetch"


def test_injected_logger_used_on_304():
    log = logging.getLogger("test.injected.fourchan")
    log.setLevel(logging.DEBUG)
    seen: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    log.addHandler(_Capture())
    url = "https://a.4cdn.org/biz/thread/1.json"
    f = _fetcher(
        [
            _FakeResp(200, {"posts": [{"no": 1}]}, headers={"Last-Modified": "GMT"}),
            _FakeResp(304),
        ],
        logger=log,
    )
    f.get_json(url)
    f.get_json(url)  # 304 -> debug routed through the injected logger
    assert any("304 not-modified" in m for m in seen)


def test_forces_utf8_regardless_of_header():
    # A real Response holding UTF-8 bytes with a MIS-set encoding; the fetcher
    # must force utf-8 so non-ASCII survives (smart quotes, em-dash, accents).
    r = requests.Response()
    r.status_code = 200
    r._content = _json.dumps(
        {"com": "buying “GME” — café"}, ensure_ascii=False
    ).encode("utf-8")
    r.encoding = "ISO-8859-1"
    data = _fetcher([r]).get_json("https://a.4cdn.org/biz/thread/1.json")
    assert data["com"] == "buying “GME” — café"
